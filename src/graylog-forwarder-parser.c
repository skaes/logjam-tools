#include "graylog-forwarder-parser.h"
#include "graylog-forwarder-prometheus-client.h"
#include "gelf-message.h"
#include "logjam-message.h"

typedef struct {
    size_t id;
    char me[16];
    zconfig_t *config;
    zsock_t *pipe;                          // actor commands
    zsock_t *pull_socket;                   // incoming messages from subscriber
    zsock_t *push_socket;                   // outgoing messages to writer
    zchunk_t *decompression_buffer;         // grows dynamically on demand
    zchunk_t *scratch_buffer;               // scratch buffer for string operations
    json_tokener *tokener;                  // json tokener instance
    zhash_t *stream_info_cache;             // thread local stream info cache
    size_t gelf_bytes;                      // size of uncompressed GELF messages
    size_t ticks;
    bool received_term_cmd;
} parser_state_t;


static
int process_message(zloop_t *loop, zsock_t *socket, void *arg)
{
    // printf("[I] graylog-forwarder-parser [%zu]: process_logjam_message\n", state->id);
    parser_state_t *state = arg;
    logjam_message *logjam_msg = logjam_message_read(socket);

    if (logjam_msg && !zsys_interrupted) {
        gelf_message *gelf_msg = logjam_message_to_gelf (logjam_msg, state->tokener, state->stream_info_cache, state->decompression_buffer, state->scratch_buffer);
        const char *gelf_data = gelf_message_to_string (gelf_msg);
        state->gelf_bytes += logjam_message_size(logjam_msg);

        if (debug)
            printf("[D] GELF message: %s\n", gelf_data);

        zmsg_t *msg = zmsg_new();
        assert(msg);

        if (compress_gelf) {
            const Bytef *raw_data = (Bytef *)gelf_data;
            uLong raw_len = strlen(gelf_data);
            uLongf compressed_len = compressBound(raw_len);
            Bytef *compressed_data = zmalloc(compressed_len);
            int rc = compress(compressed_data, &compressed_len, raw_data, raw_len);
            assert(rc == Z_OK);

            // printf("[D] GELF bytes uncompressed/compressed: %ld/%ld\n", raw_len, compressed_len);

            compressed_gelf_t *compressed_gelf = compressed_gelf_new(compressed_data, compressed_len);
            zmsg_addptr(msg, compressed_gelf);
        } else {
            zmsg_addstr(msg, gelf_data);
        }

        while (!zsys_interrupted && !output_socket_ready(state->push_socket, 1000)) {
            fprintf(stderr, "[W] parser [%zu]: push socket not ready (writer queue is full). blocking!\n", state->id);
        }

        if (!zsys_interrupted) {
            zmsg_send(&msg, state->push_socket);
        } else {
            zmsg_destroy(&msg);
        }

        gelf_message_destroy(&gelf_msg);
        logjam_message_destroy (&logjam_msg);
        // we don't free gelf_data because it's owned by the json library
    }

    return 0;
}

static
zsock_t* parser_pull_socket_new()
{
    int rc;
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    // connect socket, taking thread startup time into account
    // TODO: this is a hack. better let controller coordinate this
    for (int i=0; i<10; i++) {
        rc = zsock_connect(socket, "inproc://graylog-forwarder-subscriber");
        if (rc == 0) break;
        zclock_sleep(100);
    }
    log_zmq_error(rc, __FILE__, __LINE__);
    assert(rc == 0);
    return socket;
}

static
zsock_t* parser_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc;
    // connect socket, taking thread startup time into account
    // TODO: this is a hack. better let controller coordinate this
    for (int i=0; i<10; i++) {
        rc = zsock_connect(socket, "inproc://graylog-forwarder-writer");
        if (rc == 0) break;
        zclock_sleep(100);
    }
    return socket;
}

static
parser_state_t* parser_state_new(zconfig_t* config, size_t id)
{
    parser_state_t *state = zmalloc(sizeof(*state));
    state->config = config;
    state->id = id;
    snprintf(state->me, 16, "parser[%zu]", id);
    state->pull_socket = parser_pull_socket_new();
    state->push_socket = parser_push_socket_new();
    state->decompression_buffer = zchunk_new(NULL, INITIAL_DECOMPRESSION_BUFFER_SIZE);
    state->scratch_buffer = zchunk_new(NULL, 4096);
    state->tokener = json_tokener_new();
    state->stream_info_cache = zhash_new();
    return state;
}

static
void parser_state_destroy(parser_state_t **state_p)
{
    parser_state_t *state = *state_p;
    // must not destroy the pipe, as it's owned by the actor
    zsock_destroy(&state->pull_socket);
    zsock_destroy(&state->push_socket);
    zchunk_destroy(&state->decompression_buffer);
    zchunk_destroy(&state->scratch_buffer);
    json_tokener_free(state->tokener);
    zhash_destroy(&state->stream_info_cache);
    free(state);
    *state_p = NULL;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *arg)
{
    int rc = 0;
    parser_state_t* state = arg;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        zmsg_destroy(&msg);
        if (streq(cmd, "$TERM")) {
            fprintf(stderr, "[D] parser [%zu]: received $TERM command\n", state->id);
            free(cmd);
            state->received_term_cmd = true;
            rc = -1;
        } else {
            fprintf(stderr, "[E] parser [%zu]: received unknown command: %s\n", state->id, cmd);
            free(cmd);
        }
    }
    return rc;
}

static
int timer_event(zloop_t *loop, int timer_id, void *args)
{
    parser_state_t* state = (parser_state_t*)args;

    // record cpu usage and gelf bytes every second
    graylog_forwarder_prometheus_client_record_rusage_parser(state->id);
    graylog_forwarder_prometheus_client_count_gelf_bytes(state->gelf_bytes);
    state->gelf_bytes = 0;

    // throw away stream_info cache every minute
    if (++state->ticks % 60 == 0) {
        zhash_destroy(&state->stream_info_cache);
        state->stream_info_cache = zhash_new();
    }
    return 0;
}

static
void parser(zsock_t *pipe, void *args)
{
    parser_state_t *state = (parser_state_t*)args;
    state->pipe = pipe;
    set_thread_name(state->me);
    size_t id = state->id;

    // signal readiness after sockets have been created
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup handler for actor messages
    int rc = zloop_reader(loop, pipe, actor_command, state);
    assert(rc == 0);

    // setup handler for the pull socket
    rc = zloop_reader(loop, state->pull_socket, process_message, state);
    assert(rc == 0);

    // setup timer
    int timer_id = zloop_timer(loop, 1000, 0, timer_event, state);
    assert(timer_id != -1);

    printf("[I] parser [%zu]: starting\n", id);

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        if (!state->received_term_cmd)
            log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    printf("[I] parser [%zu]: shutting down\n", id);
    parser_state_destroy(&state);
    printf("[I] parser [%zu]: terminated\n", id);
}

zactor_t* graylog_forwarder_parser_new(zconfig_t *config, size_t id)
{
    parser_state_t *state = parser_state_new(config, id);
    return zactor_new(parser, state);
}

void graylog_forwarder_parser_destroy(zactor_t **parser_p)
{
    zactor_destroy(parser_p);
}
