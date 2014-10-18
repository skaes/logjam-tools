#include "importer-subscriber.h"
#include "importer-streaminfo.h"

typedef struct {
    void *controller_socket;
    void *sub_socket;
    void *push_socket;
    void *pull_socket;
    void *pub_socket;
} subscriber_state_t;


static
void* subscriber_sub_socket_new(zconfig_t* config, zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_SUB);
    assert(socket);
    zsocket_set_rcvhwm(socket, 10000);
    zsocket_set_linger(socket, 0);
    zsocket_set_reconnect_ivl(socket, 100); // 100 ms
    zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

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
            int rc = zsocket_connect(socket, "%s", spec);
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
void* subscriber_pull_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_PULL);
    assert(socket);
    zsocket_set_rcvhwm(socket, 10000);
    zsocket_set_linger(socket, 0);
    zsocket_set_reconnect_ivl(socket, 100); // 100 ms
    zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    // connect socket to endpoints
    // TODO: read bind_ip and port from config
    int rc = zsocket_bind(socket, "tcp://%s:%d", direct_bind_ip, direct_bind_port);
    assert(rc == direct_bind_port);

    return socket;
}

static
void* subscriber_push_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_PUSH);
    assert(socket);
    int rc = zsocket_bind(socket, "inproc://subscriber");
    assert(rc == 0);
    return socket;
}

static
void* subscriber_pub_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_PUB);  /* testing */
    assert(socket);
    zsocket_set_sndhwm(socket, 200000);
    int rc = zsocket_bind(socket, "tcp://*:%d", 9651);
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

void subscriber(void *args, zctx_t *ctx, void *pipe)
{
    int rc;
    subscriber_state_t state;
    zconfig_t* config = args;
    state.controller_socket = pipe;
    state.sub_socket = subscriber_sub_socket_new(config, ctx);
    state.pull_socket = subscriber_pull_socket_new(ctx);
    state.push_socket = subscriber_push_socket_new(ctx);
    state.pub_socket = subscriber_pub_socket_new(ctx);

    if (zhash_size(stream_subscriptions) == 0) {
        // subscribe to all messages
        zsocket_set_subscribe(state.sub_socket, "");
    } else {
        // setup subscriptions for only a subset
        zlist_t *subscriptions = zhash_keys(stream_subscriptions);
        char *stream = zlist_first(subscriptions);
        while (stream != NULL)  {
            printf("[I] controller: subscribing to stream: %s\n", stream);
            zsocket_set_subscribe(state.sub_socket, stream);
            size_t n = strlen(stream);
            if (n > 15 && !strncmp(stream, "request-stream-", 15)) {
                zsocket_set_subscribe(state.sub_socket, stream+15);
            } else {
                char old_stream[n+15+1];
                sprintf(old_stream, "request-stream-%s", stream);
                zsocket_set_subscribe(state.sub_socket, old_stream);
            }
            stream = zlist_next(subscriptions);
        }
        zlist_destroy(&subscriptions);
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

     // setup handler for the sub socket
    zmq_pollitem_t sub_item;
    sub_item.socket = state.sub_socket;
    sub_item.events = ZMQ_POLLIN;
    rc = zloop_poller(loop, &sub_item, read_request_and_forward, &state);
    assert(rc == 0);

    // setup handler for the pull socket
    zmq_pollitem_t pull_item;
    pull_item.socket = state.pull_socket;
    pull_item.events = ZMQ_POLLIN;
    rc = zloop_poller(loop, &pull_item, read_request_and_forward, &state);
    assert(rc == 0);

    // run the loop
    rc = zloop_start(loop);
    // printf("[D] zloop return: %d", rc);

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);
}
