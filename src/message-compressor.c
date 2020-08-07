#include "message-compressor.h"

/*
 * connections: "o" = bind, "[<>v^]" = connect
 *
 *                            controller
 *                                |
 *                               PIPE
 *              PUSH    PULL      |
 *    producer  o----------<  compressor  >----------o  consumer
 *
 */

// Message compressor takes logjam messages and compresses the body part. One
// could envision a generalisation to doing decompression as well.

extern bool verbose;
extern bool quiet;

typedef struct {
    size_t id;
    zsock_t *pipe;
    zsock_t *pull_socket;
    zsock_t *push_socket;
    int compression_method;
    zchunk_t *compression_buffer;
    bool decompress;
    compressor_callback_fn *cb;
} compressor_state_t;

#define COMPRESS false
#define DECOMPRESS true

static
zsock_t *compressor_pull_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://compressor-input");
    assert(rc == 0);
    return socket;
}

static
zsock_t *compressor_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://compressor-output");
    assert(rc == 0);
    return socket;
}

static
compressor_state_t* compressor_state_new(size_t id, int compression_method, bool decompress)
{
    compressor_state_t *state = zmalloc(sizeof(*state));
    state->id = id;
    state->pull_socket = compressor_pull_socket_new();
    state->push_socket = compressor_push_socket_new();
    state->compression_method = compression_method;
    state->compression_buffer = zchunk_new(NULL, INITIAL_COMPRESSION_BUFFER_SIZE);
    state->decompress = decompress;
    return state;
}

static
void compressor_state_destroy(compressor_state_t **state_p)
{
    compressor_state_t *state = *state_p;
    zsock_destroy(&state->pull_socket);
    zsock_destroy(&state->push_socket);
    zchunk_destroy(&state->compression_buffer);
    free(state);
    *state_p = NULL;
}

static
void handle_compressor_request(zmsg_t *msg, compressor_state_t *state)
{
    // get body frame
    zframe_t *stream_frame = zmsg_first(msg);
    zmsg_next(msg);
    zframe_t *body_frame = zmsg_next(msg);
    zframe_t *meta_frame = zmsg_next(msg);
    msg_meta_t *meta = (msg_meta_t*) zframe_data(meta_frame);

    void *data = zframe_data(body_frame);
    size_t data_len = zframe_size(body_frame);

    // my_zmsg_fprint(msg, "COMPRESSED", stdout);
    // dump_meta_info_network_format(meta);

    if (state->decompress) {
        size_t new_body_len;
        char* new_body;
        int rc = decompress_frame(body_frame, meta->compression_method, state->compression_buffer, &new_body, &new_body_len);
        if (!rc) {
            char *app_env = (char*) zframe_data(stream_frame);
            int n = zframe_size(stream_frame);
            const char *method_name = compression_method_to_string(meta->compression_method);
            fprintf(stderr, "[E] decompressor: could not decompress payload from %.*s (%s)\n", n, app_env, method_name);
            dump_meta_info("[E]", meta);
            my_zmsg_fprint(msg, "[E] FRAME=", stderr);

        } else {
            // printf("UNCOMPRESSED[%zu] %.*s\n", new_body_len, (int)new_body_len, new_body);
            zframe_reset(body_frame, new_body, new_body_len);
            meta->compression_method = NO_COMPRESSION;
        }
    } else {
        zmq_msg_t new_body;
        zmq_msg_init(&new_body);
        compress_message_data(state->compression_method, state->compression_buffer, &new_body, data, data_len);
        zframe_reset(body_frame, zmq_msg_data(&new_body), zmq_msg_size(&new_body));
        zmq_msg_close(&new_body);
        meta->compression_method = state->compression_method;
    }

    zmsg_send(&msg, state->push_socket);
}

static
void message_compressor(zsock_t *pipe, void *args)
{
    compressor_state_t *state = args;
    state->pipe = pipe;
    size_t id = state->id;

    char thread_name[16];
    memset(thread_name, 0, 16);
    snprintf(thread_name, 16, "compressor[%zu]", id);
    set_thread_name(thread_name);

    if (!quiet)
        printf("[I] compressor[%zu]: starting\n", id);

    // signal readyiness
    zsock_signal(pipe, 0);

    zpoller_t *poller = zpoller_new(state->pipe, state->pull_socket, NULL);
    assert(poller);

    while (!zsys_interrupted) {
        // printf("compressor[%zu]: polling\n", id);
        // wait at most one second
        void *socket = zpoller_wait(poller, 1000);
        zmsg_t *msg = NULL;
        if (socket == state->pipe) {
            msg = zmsg_recv(state->pipe);
            if (!msg) continue;
            char *cmd = zmsg_popstr(msg);
            zmsg_destroy(&msg);
            if (streq(cmd, "tick")) {
                if (verbose)
                    printf("[D] compressor[%zu]: tick\n", id);
                if (state->cb) {
                    state->cb(id);
                }
                free(cmd);
            } else if (streq(cmd, "$TERM")) {
                if (verbose)
                    printf("[D] compressor[%zu]: received $TERM command\n", id);
                free(cmd);
                break;
            } else {
                printf("[E] compressor[%zu]: received unknown command: %s\n", id, cmd);
                assert(false);
            }
        } else if (socket == state->pull_socket) {
            msg = zmsg_recv(state->pull_socket);
            if (msg != NULL) {
                handle_compressor_request(msg, state);
            }
        } else if (socket) {
            // if socket is not null, something is horribly broken
            printf("[E] compressor[%zu]: broken poller. committing suicide.\n", id);
            assert(false);
        }
        else {
            // probably interrupted by signal handler
            // if so, loop will terminate on condition !zsys_interrupted
        }
    }

    if (!quiet)
        printf("[I] compressor[%zu]: shutting down\n", id);

    compressor_state_destroy(&state);

    if (!quiet)
        printf("[I] compressor[%zu]: terminated\n", id);
}

zactor_t* message_compressor_new(size_t id, int compression_method, compressor_callback_fn cb)
{
    compressor_state_t *state = compressor_state_new(id, compression_method, COMPRESS);
    state->cb = cb;
    return zactor_new(message_compressor, state);
}

zactor_t* message_decompressor_new(size_t id, compressor_callback_fn cb)
{
    compressor_state_t *state = compressor_state_new(id, NO_COMPRESSION, DECOMPRESS);
    state->cb = cb;
    return zactor_new(message_compressor, state);
}
