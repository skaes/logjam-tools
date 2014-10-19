#include "importer-subscriber.h"
#include "importer-streaminfo.h"

/*
 * connections: n_w = NUM_WRITERS, n_p = NUM_PARSERS, "[<>^v]" = connect, "o" = bind
 *
 *                               controller
 *                                   |
 *                                  PIPE
 *                 PUB      SUB      |       PULL    PUSH
 *  logjam device  o----------<  subscriber  o----------<   direct connections
 *                                   o PUSH
 *                                   |
 *                                   |
 *                                   ^ PULL
 *                               parser(n_p)
*/

typedef struct {
    zsock_t *controller_socket;
    zsock_t *sub_socket;
    zsock_t *push_socket;
    zsock_t *pull_socket;
    zsock_t *pub_socket;
} subscriber_state_t;


static
zsock_t* subscriber_sub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);
    zsock_set_rcvhwm(socket, 10000);
    zsock_set_linger(socket, 0);
    zsock_set_reconnect_ivl(socket, 100); // 100 ms
    zsock_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    // connect socket to endpoints
    zconfig_t *endpoints = zconfig_locate(config, "frontend/endpoints");
    assert(endpoints);
    zconfig_t *endpoint = zconfig_child(endpoints);
    assert(endpoint);
    do {
        zconfig_t *binding = zconfig_child(endpoint);
        assert(binding);
        do {
            char *spec = zconfig_value(binding);
            int rc = zsock_connect(socket, "%s", spec);
            log_zmq_error(rc);
            assert(rc == 0);
            binding = zconfig_next(binding);
        } while (binding);
        endpoint = zconfig_next(endpoint);
    } while (endpoint);

    return socket;
}

static char *direct_bind_ip = "*";
static int direct_bind_port = 9605;

static
zsock_t* subscriber_pull_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    zsock_set_rcvhwm(socket, 10000);
    zsock_set_linger(socket, 0);
    zsock_set_reconnect_ivl(socket, 100); // 100 ms
    zsock_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    // connect socket to endpoints
    // TODO: read bind_ip and port from config
    int rc = zsock_bind(socket, "tcp://%s:%d", direct_bind_ip, direct_bind_port);
    assert(rc == direct_bind_port);

    return socket;
}

static
zsock_t* subscriber_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://subscriber");
    assert(rc == 0);
    return socket;
}

static
zsock_t* subscriber_pub_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUB);
    assert(socket);
    zsock_set_sndhwm(socket, 200000);
    int rc = zsock_bind(socket, "tcp://*:%d", 9651);
    assert(rc == 9651);
    return socket;
}

#ifdef PUBLISH_DUPLICATES
#undef PUBLISH_DUPLICATES
#define PUBLISH_DUPLICATES 1
#else
#define PUBLISH_DUPLICATES 0
#endif

static
void subscriber_publish_duplicate(zmsg_t *msg, void *socket)
{
    static size_t seq = 0;
    // zmsg_send(&msg_copy, state->pub_socket);
    zmsg_t *msg_copy = zmsg_dup(msg);
    zmsg_addstrf(msg_copy, "%zu", ++seq);
    zframe_t *frame = zmsg_pop(msg_copy);
    while (frame != NULL) {
        zframe_t *next_frame = zmsg_pop(msg_copy);
        int more = next_frame ? ZFRAME_MORE : 0;
        // zframe_print(frame, "DUP");
        if (zframe_send(&frame, socket, ZFRAME_DONTWAIT|more) == -1)
            break;
        frame = next_frame;
    }
    zmsg_destroy(&msg_copy);
}


static
int read_request_and_forward(zloop_t *loop, zmq_pollitem_t *item, void *callback_data)
{
    subscriber_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(item->socket);
    if (msg != NULL) {
        if (PUBLISH_DUPLICATES) {
            // zmsg_dump(msg);
            subscriber_publish_duplicate(msg, state->pub_socket);
        }
        zmsg_send(&msg, state->push_socket);
    }
    return 0;
}

static
int terminate(zloop_t *loop, zmq_pollitem_t *item, void *callback_data)
{
    zmsg_t *msg = zmsg_recv(item->socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] subscriber: received $TERM command\n");
            return -1;
        } else {
            fprintf(stderr, "[E] subscriber: received unknown actor command: %s\n", cmd);
        }
    }
    return 0;
}

void subscriber(zsock_t *pipe, void *args)
{
    int rc;
    subscriber_state_t state;
    zconfig_t* config = args;
    state.controller_socket = pipe;
    state.sub_socket  = subscriber_sub_socket_new(config);
    state.pull_socket = subscriber_pull_socket_new();
    state.push_socket = subscriber_push_socket_new();
    state.pub_socket  = subscriber_pub_socket_new();

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    if (zhash_size(stream_subscriptions) == 0) {
        // subscribe to all messages
        zsock_set_subscribe(state.sub_socket, "");
    } else {
        // setup subscriptions for only a subset
        zlist_t *subscriptions = zhash_keys(stream_subscriptions);
        char *stream = zlist_first(subscriptions);
        while (stream != NULL)  {
            printf("[I] subscriber: subscribing to stream: %s\n", stream);
            zsock_set_subscribe(state.sub_socket, stream);
            size_t n = strlen(stream);
            if (n > 15 && !strncmp(stream, "request-stream-", 15)) {
                zsock_set_subscribe(state.sub_socket, stream+15);
            } else {
                char old_stream[n+15+1];
                sprintf(old_stream, "request-stream-%s", stream);
                zsock_set_subscribe(state.sub_socket, old_stream);
            }
            stream = zlist_next(subscriptions);
        }
        zlist_destroy(&subscriptions);
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup handler for actor messages
    zmq_pollitem_t pipe_item;
    pipe_item.socket = zsock_resolve(state.controller_socket);
    pipe_item.events = ZMQ_POLLIN;
    rc = zloop_poller(loop, &pipe_item, terminate, &state);
    assert(rc == 0);

     // setup handler for the sub socket
    zmq_pollitem_t sub_item;
    sub_item.socket = zsock_resolve(state.sub_socket);
    sub_item.events = ZMQ_POLLIN;
    rc = zloop_poller(loop, &sub_item, read_request_and_forward, &state);
    assert(rc == 0);

    // setup handler for the pull socket
    zmq_pollitem_t pull_item;
    pull_item.socket = zsock_resolve(state.pull_socket);
    pull_item.events = ZMQ_POLLIN;
    rc = zloop_poller(loop, &pull_item, read_request_and_forward, &state);
    assert(rc == 0);

    // run the loop
    fprintf(stdout, "[I] subscriber: listening\n");
    rc = zloop_start(loop);
    log_zmq_error(rc);
    fprintf(stdout, "[I] subscriber: shutting down\n");

    // shutdown
    zsock_destroy(&state.sub_socket);
    zsock_destroy(&state.pull_socket);
    zsock_destroy(&state.push_socket);
    zsock_destroy(&state.pub_socket);

    zloop_destroy(&loop);
    assert(loop == NULL);

    fprintf(stdout, "[I] subscriber: terminated\n");
}
