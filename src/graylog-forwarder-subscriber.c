#include "graylog-forwarder-subscriber.h"
#include "graylog-forwarder-prometheus-client.h"
#include "logjam-message.h"
#include "device-tracker.h"
#include "logjam-streaminfo.h"

// actor state
typedef struct {
    zconfig_t *config;          // config
    zsock_t *pipe;              // actor commands
    zlist_t *devices;           // list of devices to connect to (overrides config)
    device_tracker_t *tracker;  // tracks gaps and heartbeats
    zsock_t *sub_socket;        // incoming data from logjam devices
    zsock_t *push_socket;       // outgoing data for parsers
    size_t message_count;       // how many messages we have received since last tick
    size_t message_bytes;       // how many message bytes we have received since last tick
    size_t messages_dev_zero;   // messages arrived from device 0 (since last tick)
    size_t meta_info_failures;  // messages with invalid meta info (since last tick)
    size_t message_gap_size;    // messages missed due to gaps in the stream (since last tick)
    size_t message_drops;       // messages dropped because push_socket wasn't ready (since last tick)
    size_t message_blocks;      // how often the subscriber blocked on the push_socket (since last tick)
    zlist_t *subscriptions;     // streams to subscribe to
    int rcv_hwm;
    int snd_hwm;
} subscriber_state_t;

typedef struct {
    zconfig_t *config;
    zlist_t *devices;
    int rcv_hwm;
    int snd_hwm;
} subscriber_args_t;


static subscriber_args_t* subscriber_args_new(zconfig_t *config, zlist_t *devices, int rcv_hwm, int snd_hwm)
{
    subscriber_args_t *args = zmalloc(sizeof(*args));
    args->config = config;
    args->devices = devices;
    args->rcv_hwm = rcv_hwm;
    args->snd_hwm = snd_hwm;
    return args;
}

static
void update_subscriptions(subscriber_state_t *state, zlist_t *subscriptions)
{
    const char* pattern = get_subscription_pattern();
    if (streq(pattern, "")) {
        // no pattern set, we only need to subscribe once at startup
        if (state->subscriptions == NULL) {
            printf("[I] subscriber: subscribing to all streams\n");
            zsock_set_subscribe(state->sub_socket, "");
            state->subscriptions = zlist_new();
        }
        return;
    }

    // only set heartbeat on the first call
    if (state->subscriptions == NULL) {
        zsock_set_subscribe(state->sub_socket, "heartbeat");
        state->subscriptions = zlist_new();
    }

    // subscribe to added streams
    zlist_t *added = zlist_added(state->subscriptions, subscriptions);
    char *new_stream = zlist_first(added);
    while (new_stream) {
        printf("[I] subscriber: subscribing to stream: %s\n", new_stream);
        zsock_set_subscribe(state->sub_socket, new_stream);
        new_stream = zlist_next(added);
    }

    // unsubscribe from deleted streams
    zlist_t *deleted = zlist_deleted(state->subscriptions, subscriptions);
    char *old_stream = zlist_first(deleted);
    while (old_stream) {
        printf("[I] subscriber: unsubscribing from stream: %s\n", old_stream);
        zsock_set_unsubscribe(state->sub_socket, old_stream);
        old_stream = zlist_next(deleted);
    }
    zlist_destroy(&added);
    zlist_destroy(&deleted);
}

static
void setup_subscriptions(subscriber_state_t *state)
{
    zlist_t *new_subscriptions = get_stream_subscriptions();
    update_subscriptions(state, new_subscriptions);
    zlist_destroy(&state->subscriptions);
    state->subscriptions = new_subscriptions;
}

static
int timer_function(zloop_t *loop, int timer_id, void* arg)
{
    subscriber_state_t *state = arg;
    printf("[I] subscriber: updating subscriptions\n");
    setup_subscriptions(state);
    printf("[I] subscriber: subscriptions updated\n");
    return 0;
}

static
zsock_t* subscriber_sub_socket_new(subscriber_state_t *state)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);
    zsock_set_rcvhwm(socket, state->rcv_hwm);

    char* device = zlist_first(state->devices);
    while (device) {
        printf("[I] subscriber: connecting SUB socket to logjam-device via %s\n", device);
        int rc = zsock_connect(socket, "%s", device);
        log_zmq_error(rc, __FILE__, __LINE__);
        assert(rc == 0);
        device = zlist_next(state->devices);
    }

    return socket;
}

static
zsock_t* subscriber_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    zsock_set_sndtimeo(socket, 10);
    int rc = zsock_bind(socket, "inproc://graylog-forwarder-subscriber");
    assert(rc == 0);
    return socket;
}

static
subscriber_state_t* subscriber_state_new(zsock_t* pipe, subscriber_args_t* args)
{
    subscriber_state_t *state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->config = args->config;
    state->devices = args->devices;
    state->rcv_hwm = args->rcv_hwm;
    state->snd_hwm = args->snd_hwm;
    state->sub_socket = subscriber_sub_socket_new(state);
    state->push_socket = subscriber_push_socket_new();
    state->tracker = device_tracker_new(state->devices, state->sub_socket);
    free(args);
    return state;
}

static
void subscriber_state_destroy(subscriber_state_t **state_p)
{
    subscriber_state_t *state = *state_p;
    zsock_destroy(&state->sub_socket);
    zsock_destroy(&state->push_socket);
    device_tracker_destroy(&state->tracker);
    *state_p = NULL;
}

static
int process_meta_information_and_handle_heartbeat(subscriber_state_t *state, zmsg_t* msg)
{
    zframe_t *first = zmsg_first(msg);
    char *pub_spec = NULL;
    bool is_heartbeat = zframe_streq(first, "heartbeat");

    msg_meta_t meta;
    int rc = msg_extract_meta_info(msg, &meta);
    if (!rc) {
        // dump_meta_info(&meta);
        if (!state->meta_info_failures++)
            fprintf(stderr, "[E] subscriber: received invalid meta info\n");
        return is_heartbeat;
    }
    if (meta.device_number == 0) {
        // ignore device number 0
        state->messages_dev_zero++;
        return is_heartbeat;
    }
    if (is_heartbeat) {
        if (debug)
            printf("received heartbeat from device %d\n", meta.device_number);
        zmsg_first(msg); // msg_extract_meta_info repositions the pointer, so reset
        zframe_t *spec_frame = zmsg_next(msg);
        pub_spec = zframe_strdup(spec_frame);
    }
    state->message_gap_size += device_tracker_calculate_gap(state->tracker, &meta, pub_spec);
    return is_heartbeat;
}

static
int read_request_and_forward(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    subscriber_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);

    if (msg) {
        if (debug)
            my_zmsg_fprint(msg, "[D] ", stderr);

        state->message_count++;
        state->message_bytes += zmsg_content_size(msg);
        int n = zmsg_size(msg);
        if (n < 3 || n > 4) {
            fprintf(stderr, "[E] subscriber: dropped invalid message\n");
            my_zmsg_fprint(msg, "[E] MSG", stderr);
            zmsg_destroy(&msg);
            return 0;
        }
        if (n==4) {
            int is_heartbeat = process_meta_information_and_handle_heartbeat(state, msg);
            if (is_heartbeat) {
                zmsg_destroy(&msg);
                return 0;
            }
        }

        if (!output_socket_ready(state->push_socket, 0) && !state->message_blocks++)
            fprintf(stderr, "[W] subscriber: push socket not ready. blocking!\n");

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
    static size_t ticks = 0;
    int rc = 0;
    subscriber_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            fprintf(stderr, "[D] subscriber: received $TERM command\n");
            rc = -1;
        } else if (streq(cmd, "tick")) {
            printf("[I] subscriber: %5zu messages "
                   "(gap_size: %zu, no_info: %zu, dev_zero: %zu, blocks: %zu, drops: %zu)\n",
                   state->message_count, state->message_gap_size, state->meta_info_failures,
                   state->messages_dev_zero, state->message_blocks, state->message_drops);
            graylog_forwarder_prometheus_client_record_rusage_subscriber();
            graylog_forwarder_prometheus_client_count_msgs_received(state->message_count);
            graylog_forwarder_prometheus_client_count_bytes_received(state->message_bytes);
            zmsg_t* response = zmsg_new();
            zmsg_addmem(response, &state->message_count, sizeof(state->message_count));
            zmsg_send(&response, socket);
            state->message_count = 0;
            state->message_bytes = 0;
            state->message_gap_size = 0;
            state->meta_info_failures = 0;
            state->messages_dev_zero = 0;
            state->message_drops = 0;
            if (++ticks % HEART_BEAT_INTERVAL == 0)
                device_tracker_reconnect_stale_devices(state->tracker);
        } else {
            fprintf(stderr, "[E] subscriber: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}

static
void graylog_forwarder_subscriber(zsock_t *pipe, void *args)
{
    set_thread_name("graylog-forwarder-subscriber");

    int rc;
    subscriber_state_t *state = subscriber_state_new(pipe, args);

    // subscribe to either all messages, or a subset
    setup_subscriptions(state);

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);
    // we rely on the controller shutting us down
    zloop_ignore_interrupts(loop);

    // setup handler for actor messages
    rc = zloop_reader(loop, state->pipe, actor_command, state);
    assert(rc == 0);

    // setup handler for the sub socket
    rc = zloop_reader(loop, state->sub_socket, read_request_and_forward, state);
    assert(rc == 0);

    // set up timer for adapting socket subscriptions, if we have a subscription pattern
    if (!streq(get_subscription_pattern(), ""))
        zloop_timer(loop, 60000, 0, timer_function, state);

    // run the loop
    fprintf(stdout, "[I] subscriber: listening\n");

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        if (!zsys_interrupted)
            log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    fprintf(stdout, "[I] subscriber: shutting down\n");

    // shutdown
    subscriber_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    fprintf(stdout, "[I] subscriber: terminated\n");
}

zactor_t* graylog_forwarder_subscriber_new(zconfig_t *config, zlist_t *devices, int rcv_hwm, int snd_hwm)
{
    subscriber_args_t *args = subscriber_args_new(config, devices, rcv_hwm, snd_hwm);
    return zactor_new(graylog_forwarder_subscriber, args);
}
