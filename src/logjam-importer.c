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

/* resource maps */
#define MAX_RESOURCE_COUNT 100
static zhash_t* resource_to_int = NULL;
static char *int_to_resource[MAX_RESOURCE_COUNT];
static size_t last_resource_index = 0;

static char *time_resources[MAX_RESOURCE_COUNT];
static size_t last_time_resource_index = 0;

static char *other_time_resources[MAX_RESOURCE_COUNT];
static size_t last_other_time_resource_index = 0;

static char *call_resources[MAX_RESOURCE_COUNT];
static size_t last_call_resource_index = 0;

static char *memory_resources[MAX_RESOURCE_COUNT];
static size_t last_memory_resource_index = 0;

static char *heap_resources[MAX_RESOURCE_COUNT];
static size_t last_heap_resource_index = 0;

static inline size_t r2i(const char* resource)
{
    return (size_t)zhash_lookup(resource_to_int, resource);
}

static inline const char* i2r(size_t i)
{
    assert(i <= last_resource_index);
    return (const char*)(int_to_resource[i]);
}

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
    void *processors;
} parser_state_t;

/* processor state */
typedef struct {
    char *stream;
    char *date;
    int request_count;
    void *modules;
    void *totals;
    void *minutes;
    void *quants;
} processor_t;

/* request info */
typedef struct {
    const char* page;
    const char* module;
    double total_time;
    int response_code;
    int severity;
    int minute;
} request_data_t;

/* increments */
typedef struct {
    size_t request_count;
    double *metrics;
    double *metrics_sq;
    json_object *others;
} increments_t;

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

processor_t* processor_new(char *stream, char *date)
{
    processor_t *p = malloc(sizeof(processor_t));
    p->stream = stream;
    p->date = date;
    p->request_count = 0;
    p->modules = zhash_new();
    p->totals = zhash_new();
    p->minutes = zhash_new();
    p->quants = zhash_new();
    return p;
}

processor_t* processor_create(zframe_t* stream_frame, parser_state_t* parser_state, json_object *request)
{
    size_t n = zframe_size(stream_frame);
    char stream[n+1];
    memcpy(stream, zframe_data(stream_frame), n);
    stream[n] = '\0';
    processor_t *p = zhash_lookup(parser_state->processors, stream);
    if (p == NULL) {
        p = processor_new(zframe_strdup(stream_frame), NULL/*date*/);
        zhash_insert(parser_state->processors, p->stream, p);
    }
    return p;
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

void dump_json_object(json_object *jobj) {
    const char *json_str = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
    printf("%s\n", json_str);
    // don't try to free the json string. it will crash.
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
        // dump_json_object(jobj);
    }
    if (tokener->char_offset < json_data_len) // XXX shouldn't access internal fields
    {
        fprintf(stderr, "Warning: %s\n", "extranoeus data in message payload");
        // Handle extra characters after parsed object as desired.
        // e.g. issue an error, parse another object from that point, etc...
    }
    return jobj;
}

int ignore_request(json_object *request)
{
    //TODO: how to implement this generically
    return 0;
}

const char* append_to_json_string(json_object **jobj, const char* old_str, const char* add_str)
{
    int old_len = strlen(old_str);
    int add_len = strlen(add_str);
    int new_len = old_len + add_len;
    char new_str_value[new_len+1];
    memcpy(new_str_value, old_str, old_len);
    memcpy(new_str_value + old_len, add_str, add_len);
    new_str_value[new_len] = '\0';
    json_object_put(*jobj);
    *jobj = json_object_new_string(new_str_value);
    return json_object_get_string(*jobj);
}

const char* processor_setup_page(processor_t *self, json_object *request)
{
    json_object *page_obj = NULL;
    if (json_object_object_get_ex(request, "action", &page_obj)) {
        json_object_get(page_obj);
        json_object_object_del(request, "action");
    } else {
        page_obj = json_object_new_string("Unknown#unknown_method");
    }

    const char *page_str = json_object_get_string(page_obj);

    if (!strchr(page_str, '#'))
        page_str = append_to_json_string(&page_obj, page_str, "#unknown_method");
    else if (page_str[strlen(page_str)-1] == '#')
        page_str = append_to_json_string(&page_obj, page_str, "unknown_method");

    json_object_object_add(request, "page", page_obj);

    return page_str;
}

const char* processor_setup_module(processor_t *self, const char *page)
{
    int max_mod_len = strlen(page);
    char module_str[max_mod_len+1];
    char *mod_ptr = strchr(page, ':');
    strcpy(module_str, "::");
    if (mod_ptr != NULL){
        if (mod_ptr != page) {
            int mod_len = mod_ptr - page;
            memcpy(module_str, page, mod_len);
            module_str[mod_len] = '\0';
        }
    } else {
        char *action_ptr = strchr(page, '#');
        if (action_ptr != NULL) {
            int mod_len = action_ptr - page;
            memcpy(module_str, page, mod_len);
            module_str[mod_len] = '\0';
        }
    }
    char *module = zhash_lookup(self->modules, module_str);
    if (module == NULL) {
        module = malloc(strlen(module_str)+1);
        assert(module);
        strcpy(module, module_str);
        int rc = zhash_insert(self->modules, module, module);
        assert(rc == 0);
    }
    // printf("page: %s\n", page);
    // printf("module: %s\n", module);
    return module;
}

int processor_setup_response_code(processor_t *self, json_object *request)
{
    json_object *code_obj = NULL;
    int response_code = 500;
    if (json_object_object_get_ex(request, "code", &code_obj)) {
        json_object_get(code_obj);
        json_object_object_del(request, "code");
        response_code = json_object_get_int(code_obj);
    } else {
        code_obj = json_object_new_int(response_code);
    }
    json_object_object_add(request, "response_code", code_obj);
    // printf("response_code: %d\n", response_code);
    return response_code;
}

double processor_setup_total_time(processor_t *self, json_object *request)
{
    // TODO: might be better to drop requests without total_time
    double total_time;
    json_object *total_time_obj = NULL;
    if (json_object_object_get_ex(request, "total_time", &total_time_obj)) {
        total_time = json_object_get_double(total_time_obj);
        if (total_time == 0.0) {
            total_time = 1.0;
            total_time_obj = json_object_new_double(total_time);
            json_object_object_add(request, "total_time", total_time_obj);
        }
    } else {
        total_time = 1.0;
        total_time_obj = json_object_new_double(total_time);
        json_object_object_add(request, "total_time", total_time_obj);
    }
    // printf("total_time: %f\n", total_time);
    return total_time;
}

int processor_setup_severity(processor_t *self, json_object *request)
{
    // TODO: autodetect severity form log lines if present
    int severity = 5;
    json_object *severity_obj = NULL;
    if (json_object_object_get_ex(request, "severity", &severity_obj)) {
        severity = json_object_get_int(severity_obj);
    } else {
        severity_obj = json_object_new_int(severity);
        json_object_object_add(request, "severity", severity_obj);
    }
    // printf("severity: %d\n", severity);
    return severity;
}

int processor_setup_minute(processor_t *self, json_object *request)
{
    // TODO: protect against bad started_at data
    int minute = 0;
    json_object *started_at_obj = NULL;
    if (json_object_object_get_ex(request, "started_at", &started_at_obj)) {
        const char *started_at = json_object_get_string(started_at_obj);
        char hours[3] = {started_at[11], started_at[12], '\0'};
        char minutes[3] = {started_at[14], started_at[15], '\0'};
        minute = 60 * atoi(hours) + atoi(minutes);
    }
    json_object *minute_obj = json_object_new_int(minute);
    json_object_object_add(request, "minute", minute_obj);
    // printf("minute: %d\n", minute);
    return minute;
}

void processor_setup_other_time(processor_t *self, json_object *request, double total_time)
{
    double other_time = total_time;
    for (size_t i = 0; i <= last_other_time_resource_index; i++) {
        json_object *time_val;
        if (json_object_object_get_ex(request, other_time_resources[i], &time_val)) {
            double v = json_object_get_double(time_val);
            other_time -= v;
        }
    }
    json_object_object_add(request, "other_time", json_object_new_double(other_time));
    // printf("other_time: %f\n", other_time);
}

void processor_setup_allocated_memory(processor_t *self, json_object *request)
{
    json_object *allocated_memory_obj;
    if (json_object_object_get_ex(request, "allocated_memory", &allocated_memory_obj))
        return;
    json_object *allocated_objects_obj;
    if (!json_object_object_get_ex(request, "allocated_objects", &allocated_objects_obj))
        return;
    json_object *allocated_bytes_obj;
    if (json_object_object_get_ex(request, "allocated_bytes", &allocated_bytes_obj)) {
        long allocated_objects = json_object_get_int64(allocated_objects_obj);
        long allocated_bytes = json_object_get_int64(allocated_bytes_obj);
        // assume 64bit ruby
        long allocated_memory = allocated_bytes + allocated_objects * 40;
        json_object_object_add(request, "allocated_memory", json_object_new_int64(allocated_memory));
        // printf("allocated memory: %lu\n", allocated_memory);
    }
}

increments_t* increments_new(request_data_t* request_data, json_object *request)
{
    const size_t inc_size = sizeof(increments_t);
    increments_t* increments = malloc(inc_size);
    memset(increments, 0, inc_size);

    const size_t metrics_size = sizeof(double) * (last_resource_index + 1);
    increments->metrics = malloc(metrics_size);
    memset(increments->metrics, 0, metrics_size);

    increments->metrics_sq = malloc(metrics_size);
    memset(increments->metrics_sq, 0, metrics_size);

    increments->request_count = 1;
    increments->others = json_object_new_object();
    return increments;
}

void increments_destroy(increments_t **increments)
{
    json_object_put((*increments)->others);
    free((*increments)->metrics);
    free((*increments)->metrics_sq);
    free(*increments);
    *increments = NULL;
}

void increments_fill_metrics(increments_t *increments, json_object *request)
{
    const int n = last_resource_index;
    for (size_t i=0; i <= n; i++) {
        json_object* metrics_value;
        if (json_object_object_get_ex(request, int_to_resource[i], &metrics_value)) {
            double v = json_object_get_double(metrics_value);
            increments->metrics[i] = v;
            increments->metrics_sq[i] = v*v;
        }
    }
}

void increments_fill_apdex(increments_t *increments, request_data_t *request_data)
{
    double total_time = request_data->total_time;
    long response_code = request_data->response_code;
    json_object *one = json_object_new_double(1.0);
    json_object *others = increments->others;

    if (total_time >= 2000 || response_code >= 500) {
        json_object_object_add(others, "apdex.frustrated", one);
    } else if (total_time < 100) {
        json_object_object_add(others, "apdex.happy", one);
        json_object_object_add(others, "apdex.satisfied", one);
    } else if (total_time < 500) {
        json_object_object_add(others, "apdex.satisfied", one);
    } else if (total_time < 2000) {
        json_object_object_add(others, "apdex.tolerating", one);
    }
}

void increments_fill_response_code(increments_t *increments, request_data_t *request_data)
{
    json_object *one = json_object_new_double(1.0);
    char rsp[256];
    snprintf(rsp, 256, "response.%d", request_data->response_code);
    json_object_object_add(increments->others, rsp, one);
}

void increments_fill_severity(increments_t *increments, request_data_t *request_data)
{
    json_object *one = json_object_new_double(1.0);
    char rsp[256];
    snprintf(rsp, 256, "severity.%d", request_data->severity);
    json_object_object_add(increments->others, rsp, one);
}

void increments_fill_exceptions(increments_t *increments, request_data_t *request_data)
{
    // TODO: implement
}

void increments_fill_caller_info(increments_t *increments, request_data_t *request_data)
{
    // TODO: implement
}

void processor_add_request(processor_t *self, parser_state_t *pstate, json_object *request)
{
    if (ignore_request(request)) return;
    self->request_count++;

    // dump_json_object(request);
    request_data_t request_data;
    request_data.page = processor_setup_page(self, request);
    request_data.module = processor_setup_module(self, request_data.page);
    request_data.response_code = processor_setup_response_code(self, request);
    request_data.severity = processor_setup_severity(self, request);
    request_data.minute = processor_setup_minute(self, request);
    request_data.total_time = processor_setup_total_time(self, request);
    processor_setup_other_time(self, request, request_data.total_time);
    processor_setup_allocated_memory(self, request);

    increments_t* increments = increments_new(&request_data, request);
    increments_fill_metrics(increments, request);
    increments_fill_apdex(increments, &request_data);
    increments_fill_response_code(increments, &request_data);
    increments_fill_severity(increments, &request_data);
    increments_fill_exceptions(increments, &request_data);
    increments_fill_caller_info(increments, &request_data);

    increments_destroy(&increments);
    // dump_json_object(request);
}

void processor_add_event(processor_t *self, parser_state_t *pstate, json_object *request)
{
    // TODO: not yet implemented
}

void processor_add_js_exception(processor_t *self, parser_state_t *pstate, json_object *request)
{
    // TODO: not yet implemented
}

void parse_msg_and_forward_interesting_requests(zmsg_t *msg, parser_state_t *parser_state)
{
    // zmsg_dump(msg);
    zframe_t *stream  = zmsg_first(msg);
    zframe_t *topic   = zmsg_next(msg);
    zframe_t *body    = zmsg_last(msg);
    json_object *request = parse_json_body(body, parser_state->tokener);
    if (request != NULL) {
        char *topic_str = (char*) zframe_data(topic);
        processor_t *processor = processor_create(stream, parser_state, request);
        if (!strncmp("logs", topic_str, 4))
            processor_add_request(processor, parser_state, request);
        else if (!strncmp("javascript", topic_str, 10))
            processor_add_js_exception(processor, parser_state, request);
        else if (!strncmp("events", topic_str, 6))
            processor_add_event(processor, parser_state, request);
        else {
            // silently ignore unknown request data
        }
        json_object_put(request);
    }
}

void parser(void *args, zctx_t *ctx, void *pipe)
{
    parser_state_t state;
    state.controller_socket = pipe;
    state.pull_socket = parser_pull_socket_new(ctx);
    assert( state.tokener = json_tokener_new() );
    assert( state.processors = zhash_new() );
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
    }
}

void request_writer(void *args, zctx_t *ctx, void *pipe)
{
    request_writer_state_t state;
    state.controller_socket = pipe;
    while (!zctx_interrupted) {
        sleep(1);
    }
}

int collect_stats_and_forward(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  return 0;
}

void add_resources_of_type(const char *type, char **type_map, size_t *type_idx)
{
    char path[256] = {'\0'};
    strcpy(path, "metrics/");
    strcpy(path+strlen("metrics/"), type);
    zconfig_t *metrics = zconfig_locate(config, path);
    assert(metrics);
    zconfig_t *metric = zconfig_child(metrics);
    assert(metric);
    do {
        char *resource = zconfig_name(metric);
        zhash_insert(resource_to_int, resource, (void*)last_resource_index);
        int_to_resource[last_resource_index++] = resource;
        type_map[(*type_idx)++] = resource;
        metric = zconfig_next(metric);
        assert(last_resource_index < MAX_RESOURCE_COUNT);
    } while (metric);
    (*type_idx) -= 1;

    // set up other_time_resources
    if (!strcmp(type, "time")) {
        for (size_t k = 0; k <= *type_idx; k++) {
            char *r = type_map[k];
            if (strcmp(r, "total_time") && strcmp(r, "gc_time") && strcmp(r, "other_time")) {
                other_time_resources[last_other_time_resource_index++] = r;
            }
        }
        last_other_time_resource_index--;

        // printf("other time resources:\n");
        // for (size_t j=0; j<=last_other_time_resource_index; j++) {
        //      puts(other_time_resources[j]);
        // }
    }

    // printf("%s resources:\n", type);
    // for (size_t j=0; j<=*type_idx; j++) {
    //     puts(type_map[j]);
    // }
}

// setup bidirectional mapping between resource names and small integers
void setup_resource_maps()
{
    //TODO: move this to autoconf
    assert(sizeof(size_t) == sizeof(void*));

    resource_to_int = zhash_new();
    add_resources_of_type("time", time_resources, &last_time_resource_index);
    add_resources_of_type("calls", call_resources, &last_call_resource_index);
    add_resources_of_type("memory", memory_resources, &last_memory_resource_index);
    add_resources_of_type("heap", heap_resources, &last_heap_resource_index);
    last_resource_index--;

    for (size_t j=0; j<=last_resource_index; j++) {
        const char *r = i2r(j);
        printf("%s = %zu\n", r, r2i(r));
    }
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
    setup_resource_maps();

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
