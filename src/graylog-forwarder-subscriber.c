#include "graylog-forwarder-subscriber.h"
#include "logjam-message.h"

// actor state
typedef struct {
    zsock_t *pipe;        // actor commands
    zsock_t *sub_socket;  // incoming data from logjam devices
    zsock_t *push_socket; // outgoing data for parsers
    size_t message_count; // how many messages we have received since last tick
} subscriber_state_t;

static
zsock_t* subscriber_sub_socket_new(zconfig_t * config)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);

    // set inbound high-water-mark
    int high_water_mark = atoi(zconfig_resolve(config, "/logjam/high_water_mark", "10000"));
    printf("[I] graylog-forwarder-subscriber: setting high-water-mark for inbound messages to %d\n", high_water_mark);
    zsock_set_rcvhwm(socket, high_water_mark);

    // set subscription
    char* logjam_subscription = zconfig_resolve(config, "/logjam/subscription", "");
    printf("[I] graylog-forwarder-subscriber: subscribing to %s\n", logjam_subscription);
    zsock_set_subscribe(socket, logjam_subscription);

    // connect socket to endpoints
    zconfig_t *endpoints = zconfig_locate(config, "/logjam/endpoints");
    assert(endpoints);
    zconfig_t *endpoint = zconfig_child(endpoints);
    while (endpoint) {
        char *spec = zconfig_value(endpoint);
        printf("[I] graylog-forwarder-subscriber: connecting SUB socket to logjam-device via %s\n", spec);
        int rc = zsock_connect(socket, "%s", spec);
        log_zmq_error(rc);
        assert(rc == 0);
        endpoint = zconfig_next(endpoint);
    }

    return socket;
}

static
zsock_t* subscriber_push_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://graylog-forwarder-subscriber");
    assert(rc == 0);
    return socket;
}

static
subscriber_state_t* subscriber_state_new(zsock_t *pipe, zconfig_t* config)
{
    subscriber_state_t *state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->sub_socket = subscriber_sub_socket_new(config);
    state->push_socket = subscriber_push_socket_new(config);
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
            fprintf(stderr, "[E] graylog-forwarder-subscriber: dropped invalid message\n");
            my_zmsg_fprint(msg, "[E] FRAME= ", stderr);
            zmsg_destroy(&msg);
            return 0;
        }

        while (!zsys_interrupted && !output_socket_ready(state->push_socket, 1000)) {
            fprintf(stderr, "[W] graylog-forwarder-subscriber: push socket not ready (parser queues are full). blocking!\n");
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
            fprintf(stderr, "[D] graylog-forwarder-subscriber: received $TERM command\n");
            rc = -1;
        } else if (streq(cmd, "tick")) {
            printf("[I] graylog-forwarder-subscriber: received %zu messages\n",
                    state->message_count);
            state->message_count = 0;
        } else {
            fprintf(stderr, "[E] graylog-forwarder-subscriber: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}

void graylog_forwarder_subscriber(zsock_t *pipe, void *args)
{
    set_thread_name("graylog-forwarder-subscriber");

    int rc;
    zconfig_t* config = args;
    subscriber_state_t *state = subscriber_state_new(pipe, config);

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

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

    // run the loop
    fprintf(stdout, "[I] graylog-forwarder-subscriber: listening\n");
    zloop_start(loop);
    fprintf(stdout, "[I] graylog-forwarder-subscriber: shutting down\n");

    // shutdown
    subscriber_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    fprintf(stdout, "[I] graylog-forwarder-subscriber: terminated\n");
}
