#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <json-c/json.h>

void assert_x(int rc, const char* error_text) {
  if (rc != 0) {
      printf("Failed assertion: %s\n", error_text);
      exit(1);
  }
}

static inline
void log_zmq_error(int rc)
{
  if (rc != 0) {
      printf("rc: %d, errno: %d (%s)\n", rc, errno, zmq_strerror(errno));
  }
}

/* global config */
static zconfig_t* config = NULL;

/* msg stats */
typedef struct {
    size_t transmitted;
    size_t dropped;
} msg_stats_t;

/* controller state */
typedef struct {
    void* subscriber_pipe;
    void* parser_pipe;
    void* request_writer_pipe;
    void* stats_updater_pipe;
    msg_stats_t msg_stats;
} controller_state_t;

/* subscriber state */
typedef struct {
    void *controller_socket;
    void *sub_socket;
    void *push_socket;
    msg_stats_t msg_stats;
} subscriber_state_t;

/* parser state */
typedef struct {
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    msg_stats_t msg_stats;
    json_tokener* tokener;
} parser_state_t;

/* stats updater state */
typedef struct {
    void *controller_socket;
    void *push_socket;
    msg_stats_t msg_stats;
} stats_updater_state_t;

/* request writer state */
typedef struct {
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    msg_stats_t msg_stats;
} request_writer_state_t;


void* subscriber_sub_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_SUB);
    assert(socket);
    zsocket_set_rcvhwm(socket, 1000);
    zsocket_set_linger(socket, 0);
    zsocket_set_reconnect_ivl(socket, 100); // 100 ms
    zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

    // connect socket to endpoints
    zconfig_t *endpoints = zconfig_locate(config, "frontend/endpoints");
    assert(endpoints);
    zconfig_t *endpoint = zconfig_child(endpoints);
    assert(endpoint);
    do {
        zconfig_t *binding = zconfig_child(endpoint);
        assert(binding);
        do {
            char *spec = zconfig_value(binding);
            int rc = zsocket_connect(socket, spec);
            assert(rc == 0);
            binding = zconfig_next(binding);
        } while (binding);
        endpoint = zconfig_next(endpoint);
    } while (endpoint);

    return socket;
}

void* subscriber_push_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_PUSH);
    assert(socket);
    int rc = zsocket_bind(socket, "inproc://subscriber");
    assert(rc == 0);
    return socket;
}

void subscriber(void *args, zctx_t *ctx, void *pipe)
{
    subscriber_state_t state;
    state.controller_socket = pipe;
    state.sub_socket = subscriber_sub_socket_new(ctx);
    zsocket_set_subscribe(state.sub_socket, "");
    state.push_socket = subscriber_push_socket_new(ctx);
    while (!zctx_interrupted) {
        zmsg_t *msg = zmsg_recv(state.sub_socket);
        if (msg != NULL) {
            zmsg_send(&msg, state.push_socket);
        }
    }
}

void* parser_pull_socket_new(zctx_t *context)
{
    int rc;
    void *socket = zsocket_new(context, ZMQ_PULL);
    assert(socket);
    // connect socket, taking thread startup time of subscriber into account
    // TODO: this is a hack. better let controller coordinate this
    for (int i=0; i<10; i++) {
        rc = zsocket_connect(socket, "inproc://subscriber");
        if (rc == 0) break;
        zclock_sleep(100);
    }
    log_zmq_error(rc);
    assert(rc == 0);
    return socket;
}

json_object* parse_json_body(zframe_t *body, json_tokener* tokener)
{
    char* json_data = (char*)zframe_data(body);
    int json_data_len = (int)zframe_size(body);
    json_object *jobj = json_tokener_parse_ex(tokener, json_data, json_data_len);
    enum json_tokener_error jerr = json_tokener_get_error(tokener);
    if (jerr != json_tokener_success) {
        fprintf(stderr, "Error: %s\n", json_tokener_error_desc(jerr));
        // Handle errors, as appropriate for your application.
    } else {
        // const char *json_str_orig = zframe_strdup(body);
        // printf("%s\n", json_str_orig);
        // free(json_str_orig);
        // const char *json_str = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
        // printf("%s\n", json_str);
        // don't try to free the json string. it will crash.
    }
    if (tokener->char_offset < json_data_len) // XXX shouldn't access internal fields
    {
        fprintf(stderr, "Warning: %s\n", "extranoeus data in message payload");
        // Handle extra characters after parsed object as desired.
        // e.g. issue an error, parse another object from that point, etc...
    }
    return jobj;
}

void parse_msg_and_forward_interesting_requests(zmsg_t *msg, parser_state_t *state)
{
    zmsg_dump(msg);
    zframe_t *stream  = zmsg_first(msg);
    zframe_t *topic   = zmsg_next(msg);
    zframe_t *body    = zmsg_next(msg);
    json_object *jobj = parse_json_body(body, state->tokener);
    if (jobj != NULL) {
        double total_time = 0.0;
        json_object *total_time_obj = NULL;
        if (json_object_object_get_ex(jobj, "total_time", &total_time_obj)) {
            total_time = json_object_get_double(total_time_obj);
        }
        printf("total_time: %f\n", total_time);
        json_object_put(jobj);
    }
}

void parser(void *args, zctx_t *ctx, void *pipe)
{
    parser_state_t state;
    state.controller_socket = pipe;
    state.pull_socket = parser_pull_socket_new(ctx);
    assert( state.tokener = json_tokener_new() );
    while (!zctx_interrupted) {
        zmsg_t *msg = zmsg_recv(state.pull_socket);
        if (msg != NULL) {
            parse_msg_and_forward_interesting_requests(msg, &state);
            zmsg_destroy(&msg);
        }
    }
}

void stats_updater(void *args, zctx_t *ctx, void *pipe)
{
    stats_updater_state_t state;
    state.controller_socket = pipe;
    while (!zctx_interrupted) {
        sleep(1);
        printf("stats updater not yet implementd\n");
    }
}

void request_writer(void *args, zctx_t *ctx, void *pipe)
{
    request_writer_state_t state;
    state.controller_socket = pipe;
    while (!zctx_interrupted) {
        sleep(1);
        printf("request writer not yet implementd\n");
    }
}

int collect_stats_and_forward(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  printf("forwarding not yet implementd\n");
  return 0;
}

int main(int argc, char const * const *argv)
{
    int rc;
    const char *config_file = "logjam.conf";

    if (argc > 2) {
        fprintf(stderr, "usage: %s [config-file]\n", argv[0]);
        exit(0);
    }
    if (argc == 2) {
        config_file = argv[1];
    }
    if (!zsys_file_exists(config_file)) {
        fprintf(stderr, "missing config file: %s\n", config_file);
        exit(0);
    }

    // load config
    config = zconfig_load((char*)config_file);

    setvbuf(stdout,NULL,_IOLBF,0);
    setvbuf(stderr,NULL,_IOLBF,0);

    // establish global zeromq context
    zctx_t *context = zctx_new();
    assert(context);
    zctx_set_rcvhwm(context, 1000);
    zctx_set_sndhwm(context, 1000);
    zctx_set_linger(context, 100);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    controller_state_t state;
    // start all worker threads
    state.subscriber_pipe     = zthread_fork(context, subscriber, &config);
    state.parser_pipe         = zthread_fork(context, parser, &config);
    state.stats_updater_pipe  = zthread_fork(context, stats_updater, &config);
    state.request_writer_pipe = zthread_fork(context, request_writer, &config);

    // flush increments to database every 1000 ms
    rc = zloop_timer(loop, 1000, 0, collect_stats_and_forward, &state);
    assert(rc == 0);

    // run the loop
    rc = zloop_start(loop);
    printf("shutting down: %d\n", rc);

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);

    zctx_destroy(&context);

    return 0;
}
