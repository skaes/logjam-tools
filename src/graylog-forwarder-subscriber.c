#include "graylog-forwarder-subscriber.h"
#include "logjam-message.h"

// actor state
typedef struct {
    zconfig_t *config;    // config
    zlist_t *devices;     // list of devices to connect to (overrides config)
    zsock_t *pipe;        // actor commands
    zsock_t *sub_socket;  // incoming data from logjam devices
    zsock_t *push_socket; // outgoing data for parsers
    size_t message_count; // how many messages we have received since last tick
    zlist_t *subscriptions;
    int rcv_hwm;
    int snd_hwm;
} subscriber_state_t;

typedef struct {
    zconfig_t *config;
    zlist_t *devices;
    zlist_t *subscriptions;
    int rcv_hwm;
    int snd_hwm;
} subscriber_args_t;


static subscriber_args_t* subscriber_args_new(zconfig_t *config, zlist_t *devices, zlist_t *subscriptions, int rcv_hwm, int snd_hwm)
{
    subscriber_args_t *args = zmalloc(sizeof(*args));
    args->config = config;
    args->devices = devices;
    args->subscriptions = subscriptions;
    args->rcv_hwm = rcv_hwm;
    args->snd_hwm = snd_hwm;
    return args;
}

static
zsock_t* subscriber_sub_socket_new(subscriber_state_t *state)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);
    zsock_set_rcvhwm(socket, state->rcv_hwm);

    // set subscription
    if (!state->subscriptions || zlist_size(state->subscriptions) == 0) {
        if (!state->subscriptions)
            state->subscriptions = zlist_new();
        zlist_append(state->subscriptions, zconfig_resolve(state->config, "/logjam/subscription", ""));
    }

    char *subscription = zlist_first(state->subscriptions);
    while (subscription) {
        printf("[I] subscriber: subscribing to '%s'\n", subscription);
        zsock_set_subscribe(socket, subscription);
        subscription = zlist_next(state->subscriptions);
    }

    if (!state->devices || zlist_size(state->devices) == 0) {
        // convert config file to list of devices
        if (!state->devices)
            state->devices = zlist_new();
        zconfig_t *endpoints = zconfig_locate(state->config, "/logjam/endpoints");
        if (!endpoints) {
            zlist_append(state->devices, "tcp://localhost:9606");
        } else {
            zconfig_t *endpoint = zconfig_child(endpoints);
            while (endpoint) {
                char *spec = zconfig_value(endpoint);
                char *new_spec = augment_zmq_connection_spec(spec, 9606);
                zlist_append(state->devices, new_spec);
                endpoint = zconfig_next(endpoint);
            }
        }
    }

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
    state->subscriptions = args->subscriptions;
    state->rcv_hwm = args->rcv_hwm;
    state->snd_hwm = args->snd_hwm;
    state->sub_socket = subscriber_sub_socket_new(state);
    state->push_socket = subscriber_push_socket_new();
    free(args);
    return state;
}

static
void subscriber_state_destroy(subscriber_state_t **state_p)
{
    subscriber_state_t *state = *state_p;
    zsock_destroy(&state->sub_socket);
    zsock_destroy(&state->push_socket);
    *state_p = NULL;
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
            zmsg_destroy(&msg);
            return 0;
        }

        while (!zsys_interrupted && !output_socket_ready(state->push_socket, 1000)) {
            fprintf(stderr, "[W] subscriber: push socket not ready (parser queues are full). blocking!\n");
        }

        if (!zsys_interrupted) {
            zmsg_send(&msg, state->push_socket);
        } else {
            zmsg_destroy(&msg);
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
            fprintf(stderr, "[D] subscriber: received $TERM command\n");
            rc = -1;
        } else if (streq(cmd, "tick")) {
            printf("[I] subscriber: received %zu messages\n",
                    state->message_count);
            state->message_count = 0;
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

zactor_t* graylog_forwarder_subscriber_new(zconfig_t *config, zlist_t *devices, zlist_t *subscriptions, int rcv_hwm, int snd_hwm)
{
    subscriber_args_t *args = subscriber_args_new(config, devices, subscriptions, rcv_hwm, snd_hwm);
    return zactor_new(graylog_forwarder_subscriber, args);
}
