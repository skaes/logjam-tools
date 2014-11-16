#include "statsd-client.h"

struct _statsd_client_t {
    const char *owner;              // owner log identification
    char *namespace;                // statsd namespace
    zsock_t *updates;               // socket to send updates to the statsd actor
};

#define BUFFER_SIZE 4096

typedef struct {
    size_t id;                      // server id
    zsock_t *pipe;                  // actor pipe
    zsock_t *updates;               // socket for icoming updates
    size_t update_count;            // updates sent since last tick
    size_t update_bytes;            // size of updates sent since last tick
    char buffer[BUFFER_SIZE];       // buffer up to BUFFER_SIZE bytes before sending to statsd (flushed on ticking)
    int buffer_used;                // buffer fullness
    int statsd_socket;              // udp socket for statsd
    struct sockaddr_in servaddr;    // statsd server address
} statsd_server_state_t;


statsd_client_t* statsd_client_new(zconfig_t *config, const char* owner)
{
    assert(owner);
    assert(config);

    statsd_client_t *self = zmalloc(sizeof(*self));

    self->owner = owner;
    const char *namespace = zconfig_resolve(config, "statsd/namespace", "");
    if (strlen(namespace) > 0) {
        int n = asprintf(&self->namespace, "%s.", namespace);
        assert(n > 0 && self->namespace);
    } else {
        self->namespace = strdup(namespace);
    }

    self->updates = zsock_new(ZMQ_PUSH);
    assert(self->updates);

    int rc = zsock_connect(self->updates, "inproc://statsd-updates");
    assert(rc == 0);

    return self;
}

void statsd_client_destroy(statsd_client_t **self_p)
{
    statsd_client_t *self = *self_p;
    free(self->namespace);
    zsock_destroy(&self->updates);
    free(self);
    *self_p = NULL;
}

static inline
int send_update(statsd_client_t *self, const char *name, const char *stats_type, size_t value)
{
    if (output_socket_ready(self->updates, 0)) {
        return zstr_sendf(self->updates, "%s%s:%zu|%s", self->namespace, name, value, stats_type);
    } else {
        fprintf(stderr, "[E] %s: dropped statsd packet\n", self->owner);
        return 0;
    }
}

int statsd_client_increment(statsd_client_t *self, char *name)
{
    return send_update(self, name, "c", 1);
}

int statsd_client_decrement(statsd_client_t *self, char *name)
{
    return send_update(self, name, "c", -1);
}

int statsd_client_count(statsd_client_t *self, char *name, size_t count)
{
    return send_update(self, name, "c", count);
}

int statsd_client_gauge(statsd_client_t *self, char *name, size_t val)
{
    return send_update(self, name, "g", val);
}

int statsd_client_timing(statsd_client_t *self, char *name, size_t ms)
{
    return send_update(self, name, "ms", ms);
}


// TODO: ipv6!!!
static
int setup_statsd_udp_socket_and_sever_address(statsd_server_state_t* state,  zconfig_t *config)
{
    if ((state->statsd_socket = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        fprintf(stderr, "[E] statsd[0]: cannot create statsd UDP socket");
        return 0;
    }

    // parse specification
    const char* connection_spec = zconfig_resolve(config, "statsd/endpoint", "localhost");
    assert(strlen(connection_spec) < 256);

    char host_name[256];
    unsigned int port;
    int n = sscanf(connection_spec, "udp://%[^:]:%u", host_name, &port);
    if (n != 2) {
        fprintf(stderr, "[E] statsd[0]: could not parse connection spec: %s\n", connection_spec);
        return(0);
    }

    // get the host entry
    struct hostent *hostp = gethostbyname(host_name);
    if (!hostp || !hostp->h_addr_list[0]) {
        fprintf(stderr, "[E] statsd[0]: could not obtain address of %s\n", host_name);
        return 0;
    }
    char *ip = hostp->h_addr_list[0];
    printf("[I] statsd[0]: statsd host = %s, ip = %d.%d.%d.%d\n", host_name, ip[0], ip[1], ip[2], ip[3]);

    // fill in the server's address and data
    state->servaddr.sin_family = AF_INET;
    state->servaddr.sin_port = htons(port);
    // put the host's address into the server address structure
    memcpy(&state->servaddr.sin_addr, hostp->h_addr_list[0], hostp->h_length);
    // do not free hostp

    return 1;
}

static
statsd_server_state_t* statsd_server_state_new(zsock_t *pipe, size_t id, zconfig_t *config)
{
    statsd_server_state_t* state = zmalloc(sizeof(*state));
    state->pipe = pipe;

    state->updates = zsock_new(ZMQ_PULL);
    assert(state->updates);

    int rc = zsock_bind(state->updates, "inproc://statsd-updates");
    assert(rc == 0);

    rc = setup_statsd_udp_socket_and_sever_address(state, config);
    assert(rc > 0);

    return state;
}

static
void statsd_server_state_destroy(statsd_server_state_t **state_p)
{
    statsd_server_state_t *state = *state_p;
    zsock_destroy(&state->updates);
    if (state->statsd_socket > 0) {
        close(state->statsd_socket);
    }
    *state_p = NULL;
}


static
void server_print_buffer(statsd_server_state_t *state)
{
    char log_buffer[2*BUFFER_SIZE+1];
    memset(log_buffer, 0, sizeof(log_buffer));
    char *p = &log_buffer[0];
    for(int i = 0; i < BUFFER_SIZE; i++) {
        char c = state->buffer[i];
        if (c == '\n') {
            *p++ = '\\';
            *p++ = 'n';
        } else {
            *p++ = c;
            if (!c)
                break;
        }
    }
    printf("[D] statsd[0]: buffer(%d)[ %s]\n", state->buffer_used, log_buffer);
}

static
void server_flush_buffer(statsd_server_state_t *state)
{
    if (0) server_print_buffer(state);
    int rc = sendto(state->statsd_socket, state->buffer, state->buffer_used, 0,
                    (struct sockaddr *)&state->servaddr, sizeof(state->servaddr));
    if (rc < 0) {
        fprintf(stderr, "[E] statsd[0]: cannot send updates: %s\n", strerror(errno));
    }
    state->buffer_used = 0;
    memset(state->buffer, 0, sizeof(state->buffer));
}

static
int server_append_to_buffer(statsd_server_state_t *state, const char* data, size_t len)
{
    if (len + 1 > BUFFER_SIZE) {
        fprintf(stderr, "[E] statsd[0]: dropped data packet larger than bufffer: %zu\n", len);
        return 0;
    }
    if (state->buffer_used + len + 1 > BUFFER_SIZE) {
        server_flush_buffer(state);
    }
    memcpy(&state->buffer[state->buffer_used], data, len);
    state->buffer_used += len;
    state->buffer[state->buffer_used] = '\n';
    state->buffer_used++;
    // server_print_buffer(state);
    return 1;
}

static
int server_add_update(zloop_t *loop, zsock_t *socket, void *args)
{
    statsd_server_state_t *state = args;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *data = zmsg_popstr(msg);
        assert(data);
        size_t n = strlen(data);
        zmsg_destroy(&msg);
        // printf("[D] statsd[%zu]: received update: %s\n", state->id, data);
        server_append_to_buffer(state, data, n);
        state->update_count++;
        state->update_bytes += n + 1;
        free(data);
    }
    return 0;
}


static
void statsd_server_tick(statsd_server_state_t *state)
{
    printf("[D] statsd[%zu]: %zu updates, %zu bytes\n", state->id, state->update_count, state->update_bytes);
    server_flush_buffer(state);
    state->update_count = 0;
    state->update_bytes = 0;
}

// perform actor command: "$TERM" or "tick". othwerwise log error.
static
int actor_command(zloop_t *loop, zsock_t *socket, void *args)
{
    int rc = 0;
    statsd_server_state_t *state = args;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        assert(cmd);
        zmsg_destroy(&msg);
        if (streq(cmd, "$TERM")) {
            // printf("[D] statsd[%d]: received $TERM command\n", state->id);
            rc = -1;
        } else if (streq(cmd, "tick")) {
            statsd_server_tick(state);
        } else {
            fprintf(stderr, "[E] statsd[%zu]: received unknown actor command: %s\n", state->id, cmd);
        }
        free(cmd);
    }
    return rc;
}

// zactor loop
void statsd_actor_fn(zsock_t *pipe, void *args)
{
    set_thread_name("statsd[0]");

    int rc;
    size_t id = 0;
    zconfig_t *config = args;
    statsd_server_state_t* state = statsd_server_state_new(pipe, id, config);
    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup handler for actor messages
    rc = zloop_reader(loop, state->pipe, actor_command, state);
    assert(rc == 0);

    // setup handler for the updates socket
    rc = zloop_reader(loop, state->updates, server_add_update, state);
    assert(rc == 0);

    // run the loop
    fprintf(stdout, "[I] statsd[%zu]: listening\n", id);
    rc = zloop_start(loop);
    log_zmq_error(rc);

    // shutdown
    fprintf(stdout, "[I] statsd[%zu]: shutting down\n", id);
    zloop_destroy(&loop);
    assert(loop == NULL);
    statsd_server_state_destroy(&state);
    fprintf(stdout, "[I] statsd[%zu]: terminated\n", id);
}
