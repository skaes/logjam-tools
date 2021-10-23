#include "importer-common.h"
#include "unknown-streams-collector.h"

typedef struct {
    zsock_t* pipe;
    zsock_t* pull_socket;
    zsock_t* pub_socket;
    zhashx_t *unknown_streams;
    size_t message_count;
    size_t message_drops;
    zchunk_t *decompression_buffer;
} unknown_streams_collector_state_t;


static
zsock_t* unknown_streams_collector_pull_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://unknown-streams-collector");
    assert(rc != -1);
    return socket;
}

static
zsock_t* unknown_streams_collector_pub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUB);
    assert(socket);
    zsock_set_sndhwm(socket, snd_hwm);
    if (!quiet)
        printf("[I] unknown_streams_collector: binding pub socket to: %s\n", unknown_streams_collector_connection_spec);
    int rc = zsock_bind(socket, "%s", unknown_streams_collector_connection_spec);
    assert(rc != -1);
    return socket;
}

static
unknown_streams_collector_state_t* unknown_streams_collector_state_new(zsock_t *pipe, zconfig_t* config)
{
    unknown_streams_collector_state_t* state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->pub_socket = unknown_streams_collector_pub_socket_new(config);
    state->pull_socket = unknown_streams_collector_pull_socket_new(config);
    state->unknown_streams = zhashx_new();
    state->decompression_buffer = zchunk_new(NULL, INITIAL_DECOMPRESSION_BUFFER_SIZE);
    return state;
}

static
void unknown_streams_collector_state_destroy(unknown_streams_collector_state_t** state_p)
{
    unknown_streams_collector_state_t* state = *state_p;
    zsock_destroy(&state->pub_socket);
    zsock_destroy(&state->pull_socket);
    zhashx_destroy(&state->unknown_streams);
    zchunk_destroy(&state->decompression_buffer);
    free(state);
    *state_p = NULL;
}

zsock_t* unknown_streams_collector_client_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://unknown-streams-collector");
    assert(rc != -1);
    return socket;
}

static
void log_unknown_streams(unknown_streams_collector_state_t* state) {
    void* elem = zhashx_first(state->unknown_streams);
    if (elem == NULL)
        return;
    int size = 0;
    char streams[1024] = {'\0'};
    while (elem) {
        const char *stream = zhashx_cursor(state->unknown_streams);
        if (size > 0) {
            strcat(streams, ",");
        }
        size += strlen(stream);
        strcat(streams, stream);
        elem = zhashx_next(state->unknown_streams);
    }
    fprintf(stderr,
            "[W] unknown_streams_collector: unknown streams: %s\n"
            "[W] unknown_streams_collector: %5zu messages\n",
            streams, state->message_count);
}

static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    unknown_streams_collector_state_t *state = arg;
    log_unknown_streams(state);
    zhashx_destroy(&state->unknown_streams);
    state->unknown_streams = zhashx_new();
    assert(state->unknown_streams);
    state->message_count = 0;
    state->message_drops = 0;
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
            // fprintf(stderr, "[D] unknown_streams_collector: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            // do nothing
        } else {
            fprintf(stderr, "[E] subscriber: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}

static
int read_msg_and_forward(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    unknown_streams_collector_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        zframe_t *stream_frame = zmsg_first(msg);
        char* stream_name = zframe_strdup(stream_frame);
        zhashx_insert(state->unknown_streams, stream_name, (void*)1);

        // fprintf(stderr, "[E] unknown_streams_collector: received message for unknown stream: %s\n", stream_name);
        // dump_message_payload(msg, stderr, state->decompression_buffer);

        free(stream_name);
        state->message_count++;
        zmsg_set_device_and_sequence_number(msg, 0, 0);

        int rc = zmsg_send_and_destroy(&msg, state->pub_socket);
        if (rc) {
            if (!state->message_drops++)
                fprintf(stderr, "[E] unknown_streams_collector: dropped message on pub socket (%d: %s)\n", errno, zmq_strerror(errno));
        }
    }
    return 0;
}

void unknown_streams_collector_actor_fn(zsock_t *pipe, void *args)
{
    set_thread_name("unknown_streams_collector[0]");

    int rc;
    zconfig_t* config = args;
    unknown_streams_collector_state_t *state = unknown_streams_collector_state_new(pipe, config);

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup handler for actor messages
    rc = zloop_reader(loop, state->pipe, actor_command, state);
    assert(rc == 0);

    // setup handler for the pull socket
    rc = zloop_reader(loop, state->pull_socket, read_msg_and_forward, state);
    assert(rc == 0);

    // log and reset unknown streams every minute
    rc = zloop_timer(loop, 60000, 0, timer_event, state);
    assert(rc != -1);

    // run the loop
    if (!quiet)
        fprintf(stdout, "[I] unknown_streams_collector: listening\n");

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        fprintf(stdout, "[I] unknown_streams_collector: shutting down\n");

    // shutdown
    unknown_streams_collector_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet)
        fprintf(stdout, "[I] unknown_streams_collector: terminated\n");
}
