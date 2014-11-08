#include "importer-subscriber.h"
#include "importer-streaminfo.h"
#include "logjam-util.h"

/*
 * connections: n_w = NUM_WRITERS, n_p = NUM_PARSERS, "[<>^v]" = connect, "o" = bind
 *
 *                               controller
 *                                   |
 *                                  PIPE
 *                 PUB      SUB      |       PULL    PUSH
 *  logjam device  o----------<  subscriber  o----------<   direct connections
 *                              PUSH o        \
 *                                  /          \
 *                                 /            ^ PUSH
 *                           PULL ^             tracker
 *                         parser(n_p)
*/

#define MAX_DEVICES 256

// actor state
typedef struct {
    zsock_t *pipe;                                // actor commands
    zsock_t *sub_socket;                          // incoming data from logjam devices
    zsock_t *push_socket;                         // outgoing data for parsers
    zsock_t *pull_socket;                         // pull for direct connections (apps)
    zsock_t *pub_socket;                          // republish all incoming messages (optional)
    uint64_t sequence_numbers[MAX_DEVICES];       // last seen message sequence number for given device
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
    zconfig_t *bindings = zconfig_locate(config, "frontend/endpoints/bindings");
    assert(bindings);
    zconfig_t *binding = zconfig_child(bindings);
    while (binding) {
        char *spec = zconfig_value(binding);
        printf("[I] subscriber: connecting SUB socket to %s\n", spec);
        int rc = zsock_connect(socket, "%s", spec);
        log_zmq_error(rc);
        assert(rc == 0);
        binding = zconfig_next(binding);
    }

    return socket;
}

static
zsock_t* subscriber_pull_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    zsock_set_rcvhwm(socket, 10000);
    zsock_set_linger(socket, 0);
    zsock_set_reconnect_ivl(socket, 100); // 100 ms
    zsock_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    char *pull_spec = zconfig_resolve(config, "frontend/endpoints/subscriber/pull", "tcp://127.0.0.1:9605");
    printf("[I] subscriber: binding PULL socket to %s\n", pull_spec);
    int rc = zsock_bind(socket, "%s", pull_spec);
    assert(rc != -1);

    const char *inproc_binding = "inproc://subscriber-pull";
    printf("[I] subscriber: binding PULL socket to %s\n", inproc_binding);
    rc = zsock_bind(socket, "%s", inproc_binding);
    assert(rc != -1);

    return socket;
}

static
zsock_t* subscriber_push_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://subscriber");
    assert(rc == 0);
    return socket;
}

static
zsock_t* subscriber_pub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUB);
    assert(socket);
    zsock_set_sndhwm(socket, 200000);
    char *pub_spec = zconfig_resolve(config, "frontend/endpoints/subscriber/pub", "tcp://127.0.0.1:9651");
    printf("[I] subscriber: binding PUB socket to %s\n", pub_spec);
    int rc = zsock_bind(socket, "%s", pub_spec);
    assert(rc != -1);
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
void extract_meta(zmsg_t *msg, msg_meta_t *meta)
{
    zframe_t *app_env_f = zmsg_pop(msg);
    zframe_t *routing_key_f = zmsg_pop(msg);
    zframe_t *body_f = zmsg_pop(msg);
    zframe_t *meta_f = zmsg_pop(msg);
    zmsg_append(msg, &app_env_f);
    zmsg_append(msg, &routing_key_f);
    zmsg_append(msg, &body_f);
    memcpy(meta, zframe_data(meta_f), sizeof(*meta));
    meta_network_2_native(meta);
    zframe_destroy(&meta_f);
}

static
void check_and_update_sequence_number(subscriber_state_t *state, zmsg_t* msg)
{
    msg_meta_t meta;
    extract_meta(msg, &meta);
    if (meta.device_number > MAX_DEVICES) {
        fprintf(stderr, "[E] subscriber: received illegal device number\n");
        return;
    }
    int64_t old_sequence_number = state->sequence_numbers[meta.device_number];
    int64_t gap = meta.sequence_number - old_sequence_number - 1;
    if (gap > 0 && old_sequence_number) {
        fprintf(stderr, "[E] subscriber: lost %llu messages from device %d\n", gap, meta.device_number);
    } else {
        // printf("[D] subscriber: msg(device %d, sequence %llu)\n", meta.device_number, meta.sequence_number);
    }
    state->sequence_numbers[meta.device_number] = meta.sequence_number;
}

static
int read_request_and_forward(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    subscriber_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        int n = zmsg_size(msg);
        if (n < 3 || n > 4) {
            fprintf(stderr, "[E] subscriber: dropped invalid message\n");
            my_zmsg_fprint(msg, "[E] FRAME= ", stderr);
            return 0;
        }
        if (n == 4) {
            check_and_update_sequence_number(state, msg);
        }
        if (PUBLISH_DUPLICATES) {
            subscriber_publish_duplicate(msg, state->pub_socket);
        }
        if (!output_socket_ready(state->push_socket, 0)) {
            fprintf(stderr, "[W] subscriber: push socket not ready. blocking!\n");
        }
        zmsg_send(&msg, state->push_socket);
    }
    return 0;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    int rc = 0;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] subscriber: received $TERM command\n");
            rc = -1;
        } else {
            fprintf(stderr, "[E] subscriber: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
    }
    return rc;
}


static
subscriber_state_t* subscriber_state_new(zsock_t *pipe, zconfig_t* config)
{
    subscriber_state_t *state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->sub_socket = subscriber_sub_socket_new(config);
    state->pull_socket = subscriber_pull_socket_new(config);
    state->push_socket = subscriber_push_socket_new(config);
    if (PUBLISH_DUPLICATES)
        state->pub_socket  = subscriber_pub_socket_new(config);
    return state;
}

static
void subscriber_state_destroy(subscriber_state_t **state_p)
{
    subscriber_state_t *state = *state_p;
    zsock_destroy(&state->sub_socket);
    zsock_destroy(&state->pull_socket);
    zsock_destroy(&state->push_socket);
    if (PUBLISH_DUPLICATES)
        zsock_destroy(&state->pub_socket);
    *state_p = NULL;
}


static
void setup_subscriptions(subscriber_state_t *state)
{
    if (zhash_size(stream_subscriptions) == 0) {
        // subscribe to all messages
        zsock_set_subscribe(state->sub_socket, "");
    } else {
        // setup subscriptions for only a subset
        zlist_t *subscriptions = zhash_keys(stream_subscriptions);
        char *stream = zlist_first(subscriptions);
        while (stream != NULL)  {
            printf("[I] subscriber: subscribing to stream: %s\n", stream);
            zsock_set_subscribe(state->sub_socket, stream);
            size_t n = strlen(stream);
            if (n > 15 && !strncmp(stream, "request-stream-", 15)) {
                zsock_set_subscribe(state->sub_socket, stream+15);
            } else {
                char old_stream[n+15+1];
                sprintf(old_stream, "request-stream-%s", stream);
                zsock_set_subscribe(state->sub_socket, old_stream);
            }
            stream = zlist_next(subscriptions);
        }
        zlist_destroy(&subscriptions);
    }
}

void subscriber(zsock_t *pipe, void *args)
{
    set_thread_name("subscriber[0]");

    int rc;
    zconfig_t* config = args;
    subscriber_state_t *state = subscriber_state_new(pipe, config);

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    // subscribe to either all messages, or a subset
    setup_subscriptions(state);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup handler for actor messages
    rc = zloop_reader(loop, state->pipe, actor_command, state);
    assert(rc == 0);

     // setup handler for the sub socket
    rc = zloop_reader(loop, state->sub_socket, read_request_and_forward, state);
    assert(rc == 0);

    // setup handler for the pull socket
    rc = zloop_reader(loop, state->pull_socket, read_request_and_forward, state);
    assert(rc == 0);

    // run the loop
    fprintf(stdout, "[I] subscriber: listening\n");
    zloop_start(loop);
    fprintf(stdout, "[I] subscriber: shutting down\n");

    // shutdown
    subscriber_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    fprintf(stdout, "[I] subscriber: terminated\n");
}
