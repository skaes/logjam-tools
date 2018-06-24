#include "importer-common.h"
#include "prom-collector.h"

typedef struct {
    zsock_t* pipe;
    zsock_t* pull_socket;
    zsock_t* pub_socket;
    size_t message_count;
    size_t message_drops;
    zhash_t* sequence_numbers;  // sequeence numbers are per environment
} prom_collector_state_t;


static
zsock_t* prom_collector_pull_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://prom-collector");
    assert(rc != -1);
    return socket;
}

static
zsock_t* prom_collector_pub_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUB);
    assert(socket);
    zsock_set_sndhwm(socket, snd_hwm);
    if (!quiet)
        printf("[I] promcollector: binding pub socket to: %s\n", prom_collector_connection_spec);
    int rc = zsock_bind(socket, "%s", prom_collector_connection_spec);
    assert(rc != -1);
    return socket;
}

static
prom_collector_state_t* prom_collector_state_new(zsock_t *pipe, zconfig_t* config)
{
    prom_collector_state_t* state = zmalloc(sizeof(*state));
    state->pipe = pipe;
    state->pub_socket = prom_collector_pub_socket_new(config);
    state->pull_socket = prom_collector_pull_socket_new(config);
    state->sequence_numbers = zhash_new();
    return state;
}

static
void prom_collector_state_destroy(prom_collector_state_t** state_p)
{
    prom_collector_state_t* state = *state_p;
    zsock_destroy(&state->pub_socket);
    zsock_destroy(&state->pull_socket);
    zhash_destroy(&state->sequence_numbers);
    free(state);
    *state_p = NULL;
}

zsock_t* prom_collector_client_socket_new(zconfig_t* config)
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://prom-collector");
    assert(rc != -1);
    return socket;
}

void prom_collector_publish(zsock_t *prom_collector_socket, const char* env, const char* body)
{
    zstr_sendx(prom_collector_socket, env, body, NULL);
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    int rc = 0;
    prom_collector_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] prom_collector: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            printf("[I] promcollector: %5zu messages\n", state->message_count);
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
void add_sequence_number(prom_collector_state_t *state, zmsg_t* msg)
{
    zframe_t* env_frame = zmsg_first(msg);
    char env[1024] = {};
    int n = zframe_size(env_frame);
    assert(n < 1024);
    memcpy(env, zframe_data(env_frame), n);
    env[n] = '\0';
    size_t *sequence_number = zhash_lookup(state->sequence_numbers, env);
    if (!sequence_number) {
        sequence_number = malloc(sizeof(*sequence_number));
        assert(sequence_number != NULL);
        *sequence_number = 0;
        zhash_insert(state->sequence_numbers, env, sequence_number);
        zhash_freefn(state->sequence_numbers, env, free);
    }
    (*sequence_number)++;
    uint64_t encoded = htonll(*sequence_number);
    zmsg_addmem(msg, &encoded, sizeof(encoded));
}

static
int read_msg_and_forward(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    prom_collector_state_t *state = callback_data;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        // my_zmsg_fprint(msg, "[D] prom-collector", stdout);
        state->message_count++;
        add_sequence_number(state, msg);
        int rc = zmsg_send_and_destroy(&msg, state->pub_socket);
        if (rc) {
            if (!state->message_drops++)
                fprintf(stderr, "[E] promcollector: dropped message on pub socket (%d: %s)\n", errno, zmq_strerror(errno));
        }
    }
    return 0;
}

void prom_collector_actor_fn(zsock_t *pipe, void *args)
{
    set_thread_name("prom-collector");

    int rc;
    zconfig_t* config = args;
    prom_collector_state_t *state = prom_collector_state_new(pipe, config);

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
        fprintf(stdout, "[I] promcollector: listening\n");

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        fprintf(stdout, "[I] promcollector: shutting down\n");

    // shutdown
    prom_collector_state_destroy(&state);
    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet)
        fprintf(stdout, "[I] promcollector: terminated\n");
}
