#include "importer-subscriber.h"
#include "importer-streaminfo.h"
#include "logjam-util.h"

/*
 * connections: n_w = num_writers, n_p = num_parsers, "[<>^v]" = connect, "o" = bind
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

#define MAX_DEVICES 4096

// actor state
typedef struct {
    zsock_t *pipe;                            // actor commands
    zsock_t *sub_socket;                      // incoming data from logjam devices
    zsock_t *push_socket;                     // outgoing data for parsers
    zsock_t *pull_socket;                     // pull for direct connections (apps)
    zsock_t *pub_socket;                      // republish all incoming messages (optional)
    uint64_t sequence_numbers[MAX_DEVICES];   // last seen message sequence number for given device
    size_t message_count;                     // messages processed (since last tick)
    size_t messages_dev_zero;                 // messages arrived from device 0 (since last tick)
    size_t meta_info_failures;                // messages with invalid meta info (since last tick)
    size_t message_gap_size;                  // messages missed due to gaps in the stream (since last tick)
    size_t message_drops;                     // messages dropped because push_socket wasn't ready (since last tick)
    size_t message_blocks;                    // how often the subscriber blocked on the push_socket (since last tick)
} subscriber_state_t;


static
void extract_bindings_from_config(zconfig_t* config)
{
    zconfig_t *bindings = zconfig_locate(config, "frontend/endpoints/bindings");
    assert(bindings);
    zconfig_t *binding = zconfig_child(bindings);
    while (binding) {
        char *spec = zconfig_value(binding);
        if (streq(spec, "")) {
            if (verbose)
                printf("[I] subscriber: ignoring empty SUB socket binding\n");
        } else {
            zlist_append(hosts, spec);
        }
        binding = zconfig_next(binding);
    }
}

static
zsock_t* subscriber_sub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);
    zsock_set_rcvhwm(socket, rcv_hwm);
    zsock_set_linger(socket, 0);
    zsock_set_reconnect_ivl(socket, 100); // 100 ms
    zsock_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    if (hosts == NULL) {
        hosts = zlist_new();
        extract_bindings_from_config(config);
    }
    if (zlist_size(hosts) == 0)
        zlist_append(hosts, augment_zmq_connection_spec("localhost", sub_port));

    // connect socket to endpoints
    char* host = zlist_first(hosts);
    while (host) {
        if (!quiet)
            printf("[I] subscriber: connecting SUB socket to: %s\n", host);
        int rc = zsock_connect(socket, "%s", host);
        log_zmq_error(rc, __FILE__, __LINE__);
        assert(rc == 0);
        host = zlist_next(hosts);
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

    char *pull_spec = zconfig_resolve(config, "frontend/endpoints/subscriber/pull", "tcp://*");
    char *full_spec = augment_zmq_connection_spec(pull_spec, pull_port);
    if (!quiet)
        printf("[I] subscriber: binding PULL socket to %s\n", full_spec);
    int rc = zsock_bind(socket, "%s", full_spec);
    assert(rc != -1);
    free(full_spec);

    const char *inproc_binding = "inproc://subscriber-pull";
    if (!quiet)
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
    zsock_set_sndtimeo(socket, 10);
    int rc = zsock_bind(socket, "inproc://subscriber");
    assert(rc == 0);
    return socket;
}

static
zsock_t* subscriber_pub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUB);
    assert(socket);
    zsock_set_sndhwm(socket, 10000);
    char *pub_spec = zconfig_resolve(config, "frontend/endpoints/subscriber/pub", "tcp://127.0.0.1:9651");
    if (!quiet)
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
    zmsg_t *msg_copy = zmsg_dup(msg);
    zmsg_addstrf(msg_copy, "%zu", ++seq);
    zframe_t *frame = zmsg_pop(msg_copy);
    while (frame != NULL) {
        zframe_t *next_frame = zmsg_pop(msg_copy);
        int more = next_frame ? ZFRAME_MORE : 0;
        if (zframe_send(&frame, socket, ZFRAME_DONTWAIT|more) == -1)
            break;
        frame = next_frame;
    }
    zmsg_destroy(&msg_copy);
}

static
void check_and_update_sequence_number(subscriber_state_t *state, zmsg_t* msg)
{
    msg_meta_t meta;
    int rc = msg_extract_meta_info(msg, &meta);
    if (!rc) {
        // dump_meta_info(&meta);
        if (!state->meta_info_failures++) {
            fprintf(stderr, "[E] subscriber: received invalid meta info\n");
        }
        return;
    }
    if (meta.device_number == 0) {
        // ignore device number 0
        state->messages_dev_zero++;
        return;
    }
    if (meta.device_number > MAX_DEVICES && !state->meta_info_failures++) {
        fprintf(stderr, "[E] subscriber: received illegal device number: %d\n", meta.device_number);
        return;
    }
    int64_t old_sequence_number = state->sequence_numbers[meta.device_number];
    int64_t gap = meta.sequence_number - old_sequence_number - 1;
    if (gap > 0 && old_sequence_number) {
        fprintf(stderr, "[W] subscriber: lost %" PRIi64 " messages from device %" PRIu32 " (%" PRIu64 "-%" PRIu64 ")\n",
                gap, meta.device_number, old_sequence_number + 1, meta.sequence_number - 1);
        state->message_gap_size += gap;
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
        state->message_count++;
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
        if (!output_socket_ready(state->push_socket, 0) && !state->message_blocks++) {
            fprintf(stderr, "[W] subscriber: push socket not ready. blocking!\n");
        }
        int rc = zmsg_send_and_destroy(&msg, state->push_socket);
        if (rc) {
            if (!state->message_drops++)
                fprintf(stderr, "[E] subscriber: dropped message on push socket (%d: %s)\n", errno, zmq_strerror(errno));
        }
    }
    return 0;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    int rc = 0;
    subscriber_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] subscriber: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            printf("[I] subscriber: %5zu messages "
                   "(gap_size: %zu, no_info: %zu, dev_zero: %zu, blocks: %zu, drops: %zu)\n",
                   state->message_count, state->message_gap_size, state->meta_info_failures,
                   state->messages_dev_zero, state->message_blocks, state->message_drops);
            state->message_count = 0;
            state->message_gap_size = 0;
            state->meta_info_failures = 0;
            state->messages_dev_zero = 0;
            state->message_drops = 0;
            state->message_blocks = 0;
        } else {
            fprintf(stderr, "[E] subscriber: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
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
    zlist_t *subscriptions = zhash_keys(stream_subscriptions);
    setup_subscriptions_for_sub_socket(subscriptions, state->sub_socket);
    zlist_destroy(&subscriptions);
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
    if (!quiet)
        fprintf(stdout, "[I] subscriber: listening\n");

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        fprintf(stdout, "[I] subscriber: shutting down\n");

    // shutdown
    subscriber_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet)
        fprintf(stdout, "[I] subscriber: terminated\n");
}
