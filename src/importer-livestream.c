#include "importer-common.h"
#include "importer-livestream.h"

typedef struct {
    zsock_t* pipe;
    zsock_t* pull_socket;
    zsock_t* pub_socket;
    size_t message_count;
    size_t message_drops;
} live_stream_state_t;


static
zsock_t* live_stream_pull_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://live_stream");
    assert(rc != -1);
    return socket;
}

static
zsock_t* live_stream_pub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_SUB);
    assert(socket);
    zsock_set_sndhwm(socket, snd_hwm);
    if (!quiet)
        printf("[I] live_stream: binding pub socket to: %s\n", live_stream_connection_spec);
    int rc = zsock_bind(socket, "%s", live_stream_connection_spec);
    assert(rc != -1);
    return socket;
}

static
live_stream_state_t* live_stream_state_new(zsock_t *pipe, zconfig_t* config)
{
    live_stream_state_t* state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->pub_socket = live_stream_pub_socket_new(config);
    state->pull_socket = live_stream_pull_socket_new(config);
    return state;
}

static
void live_stream_state_destroy(live_stream_state_t** state_p)
{
    live_stream_state_t* state = *state_p;
    zsock_destroy(&state->pub_socket);
    zsock_destroy(&state->pull_socket);
    free(state);
    *state_p = NULL;
}


zsock_t* live_stream_client_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://live_stream");
    assert(rc != -1);
    return socket;
}

void live_stream_publish(zsock_t *live_stream_socket, const char* key, const char* json_str)
{
    if (dryrun) return;

    zstr_sendx(live_stream_socket, key, json_str, NULL);
}

void publish_error_for_module(stream_info_t *stream_info, const char* module, const char* json_str, zsock_t* live_stream_socket)
{
    size_t n = stream_info->app_len + 1 + stream_info->env_len + 1;
    // skip :: at the beginning of module
    while (*module == ':') module++;
    size_t m = strlen(module) + 1;
    char key[n + m + 3];
    sprintf(key, "%s-%s,%s", stream_info->app, stream_info->env, module);
    // TODO: change this crap in the live stream publisher
    // tolower is unsafe and not really necessary
    for (char *p = key; *p; ++p) *p = tolower(*p);

    live_stream_publish(live_stream_socket, key, json_str);
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    int rc = 0;
    live_stream_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] live_stream: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            printf("[I] live_stream: %5zu messages\n", state->message_count);
            state->message_count = 0;
            state->message_drops = 0;
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
    live_stream_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        state->message_count++;
        int rc = zmsg_send_and_destroy(&msg, state->pub_socket);
        if (rc) {
            if (!state->message_drops++)
                fprintf(stderr, "[E] live_stream: dropped message on pub socket (%d: %s)\n", errno, zmq_strerror(errno));
        }
    }
    return 0;
}

void live_stream_actor_fn(zsock_t *pipe, void *args)
{
    set_thread_name("live_stream[0]");

    int rc;
    zconfig_t* config = args;
    live_stream_state_t *state = live_stream_state_new(pipe, config);

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

    // run the loop
    if (!quiet)
        fprintf(stdout, "[I] live_stream: listening\n");

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        fprintf(stdout, "[I] live_stream: shutting down\n");

    // shutdown
    live_stream_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet)
        fprintf(stdout, "[I] live_stream: terminated\n");
}
