#define _GNU_SOURCE
#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <json-c/json.h>
#include <bson.h>
#include <mongoc.h>

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
char *config_file = "logjam.conf";
static bool dryrun = false;
//TODO: get from config
const char *mongo_uri = "mongodb://127.0.0.1:27017/";

/* resource maps */
#define MAX_RESOURCE_COUNT 100
static zhash_t* resource_to_int = NULL;
static char *int_to_resource[MAX_RESOURCE_COUNT];
static char *int_to_resource_sq[MAX_RESOURCE_COUNT];
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

static size_t allocated_objects_index, allocated_bytes_index;

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

#define NUM_PARSERS 2

/* controller state */
typedef struct {
    void* subscriber_pipe;
    void* parser_pipes[NUM_PARSERS];
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
    size_t request_count;
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    msg_stats_t msg_stats;
    json_tokener* tokener;
    zhash_t *processors;
} parser_state_t;

/* processor state */
typedef struct {
    char *stream;
    size_t request_count;
    zhash_t *modules;
    zhash_t *totals;
    zhash_t *minutes;
    zhash_t *quants;
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
// TODO: support integer vlaues (for call metrics)
typedef struct {
    double val;
    double val_squared;
} metric_pair_t;

typedef struct {
    size_t request_count;
    metric_pair_t *metrics;
    json_object *others;
} increments_t;

#define METRICS_ARRAY_SIZE (sizeof(metric_pair_t) * (last_resource_index + 1))

typedef struct {
    mongoc_collection_t *totals;
    mongoc_collection_t *minutes;
    mongoc_collection_t *quants;
} stream_collections_t;

/* stats updater state */
typedef struct {
    mongoc_client_t *mongo_client;
    mongoc_collection_t *global_collection;
    zhash_t *stream_collections;
    void *controller_socket;
    void *push_socket;
    msg_stats_t msg_stats;
} stats_updater_state_t;

/* request writer state */
typedef struct {
    mongoc_client_t* mongo_client;
    zhash_t *request_collections;
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    size_t request_count;
    msg_stats_t msg_stats;
} request_writer_state_t;


static mongoc_write_concern_t *wc_no_wait = NULL;
static mongoc_write_concern_t *wc_wait = NULL;
static mongoc_index_opt_t index_opt_background;

void initialize_mongo_db_globals()
{
    mongoc_init();

    wc_wait = mongoc_write_concern_new();
    mongoc_write_concern_set_w(wc_wait, MONGOC_WRITE_CONCERN_W_DEFAULT);

    wc_no_wait = mongoc_write_concern_new();
    mongoc_write_concern_set_w(wc_no_wait, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);

    mongoc_index_opt_init(&index_opt_background);
    index_opt_background.background = true;
}

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

processor_t* processor_new(char *stream)
{
    processor_t *p = malloc(sizeof(processor_t));
    p->stream = strdup(stream);
    p->request_count = 0;
    p->modules = zhash_new();
    p->totals = zhash_new();
    p->minutes = zhash_new();
    p->quants = zhash_new();
    return p;
}

void processor_destroy(void* processor)
{
    //void* because we want to use it as a zhash_free_fn
    processor_t* p = processor;
    // printf("destroying processor: %s. requests: %zu\n", p->stream, p->request_count);
    free(p->stream);
    zhash_destroy(&p->modules);
    zhash_destroy(&p->totals);
    zhash_destroy(&p->minutes);
    zhash_destroy(&p->quants);
    free(p);
}

#define STREAM_PREFIX "request-stream-"
// strlen(STREAM_PREFIX)
#define STREAM_PREFIX_LEN 15
// ISO date: 2014-11-11

processor_t* processor_create(zframe_t* stream_frame, parser_state_t* parser_state, json_object *request)
{
    size_t n = zframe_size(stream_frame);
    char stream[n+100];
    strcpy(stream, "logjam-");
    memcpy(stream+7, zframe_data(stream_frame)+15, n-15);
    stream[n+7-15] = '-';
    stream[n+7-14] = '\0';

    json_object* started_at_value;
    if (json_object_object_get_ex(request, "started_at", &started_at_value)) {
        const char *date_str = json_object_get_string(started_at_value);
        strncpy(&stream[n+7-14], date_str, 10);
        stream[n+7-14+10] = '\0';
    } else {
        fprintf(stderr, "dropped request without started_at date");
        return NULL;
    }

    // printf("stream: %s\n", stream);

    processor_t *p = zhash_lookup(parser_state->processors, stream);
    if (p == NULL) {
        p = processor_new(stream);
        int rc = zhash_insert(parser_state->processors, stream, p);
        assert(rc ==0);
        zhash_freefn(parser_state->processors, stream, processor_destroy);
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

void* parser_push_socket_new(zctx_t *context)
{
    int rc;
    void *socket = zsocket_new(context, ZMQ_PUSH);
    assert(socket);
    // connect socket, taking thread startup time of request_writer into account
    // TODO: this is a hack. better let controller coordinate this
    for (int i=0; i<10; i++) {
        rc = zsocket_connect(socket, "inproc://request_writer");
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

increments_t* increments_new()
{
    const size_t inc_size = sizeof(increments_t);
    increments_t* increments = malloc(inc_size);
    memset(increments, 0, inc_size);

    const size_t metrics_size = METRICS_ARRAY_SIZE;
    increments->metrics = malloc(metrics_size);
    memset(increments->metrics, 0, metrics_size);

    increments->request_count = 1;
    increments->others = json_object_new_object();
    return increments;
}

void increments_destroy(void *increments)
{
    // void* because of zhash_destroy
    increments_t *incs = increments;
    json_object_put(incs->others);
    free(incs->metrics);
    free(incs);
}

increments_t* increments_clone(increments_t* increments)
{
    increments_t* new_increments = increments_new();
    new_increments->request_count = increments->request_count;
    memcpy(new_increments->metrics, increments->metrics, METRICS_ARRAY_SIZE);
    json_object_object_foreach(increments->others, key, value) {
        json_object_get(value);
        json_object_object_add(new_increments->others, key, value);
    }
    return new_increments;
}

void increments_fill_metrics(increments_t *increments, json_object *request)
{
    const int n = last_resource_index;
    for (size_t i=0; i <= n; i++) {
        json_object* metrics_value;
        if (json_object_object_get_ex(request, int_to_resource[i], &metrics_value)) {
            double v = json_object_get_double(metrics_value);
            metric_pair_t *p = &increments->metrics[i];
            p->val = v;
            p->val_squared = v*v;
        }
    }
}

#define NEW_INT1 (json_object_new_int(1))

void increments_fill_apdex(increments_t *increments, request_data_t *request_data)
{
    double total_time = request_data->total_time;
    long response_code = request_data->response_code;
    json_object *others = increments->others;

    if (total_time >= 2000 || response_code >= 500) {
        json_object_object_add(others, "apdex.frustrated", NEW_INT1);
    } else if (total_time < 100) {
        json_object_object_add(others, "apdex.happy", NEW_INT1);
        json_object_object_add(others, "apdex.satisfied", NEW_INT1);
    } else if (total_time < 500) {
        json_object_object_add(others, "apdex.satisfied", NEW_INT1);
    } else if (total_time < 2000) {
        json_object_object_add(others, "apdex.tolerating", NEW_INT1);
    }
}

void increments_fill_response_code(increments_t *increments, request_data_t *request_data)
{
    char rsp[256];
    snprintf(rsp, 256, "response.%d", request_data->response_code);
    json_object_object_add(increments->others, rsp, NEW_INT1);
}

void increments_fill_severity(increments_t *increments, request_data_t *request_data)
{
    char sev[256];
    snprintf(sev, 256, "severity.%d", request_data->severity);
    json_object_object_add(increments->others, sev, NEW_INT1);
}

int replace_dots(char *s)
{
    if (s == NULL) return 0;
    int count = 0;
    char c;
    while ((c = *s) != '\0') {
        if (c == '.') {
            *s = '_';
            count++;
        }
        s++;
    }
    return count;
}

void increments_fill_exceptions(increments_t *increments, json_object *request)
{
    json_object* exceptions_obj;
    if (json_object_object_get_ex(request, "exceptions", &exceptions_obj)) {
        int num_ex = json_object_array_length(exceptions_obj);
        if (num_ex == 0) {
            json_object_object_del(request, "exceptions");
            return;
        }
        for (int i=0; i<num_ex; i++) {
            json_object* ex_obj = json_object_array_get_idx(exceptions_obj, i);
            const char *ex_str = json_object_get_string(ex_obj);
            size_t n = strlen(ex_str);
            char ex_str_dup[n+12];
            strcpy(ex_str_dup, "exceptions.");
            strcpy(ex_str_dup+11, ex_str);
            replace_dots(ex_str_dup+11);
            // printf("EXCEPTION: %s\n", ex_str_dup);
            json_object_object_add(increments->others, ex_str_dup, NEW_INT1);
        }
    }
}

void increments_fill_caller_info(increments_t *increments, json_object *request)
{
    json_object *caller_action_obj;
    if (json_object_object_get_ex(request, "caller_action", &caller_action_obj)) {
        const char *caller_action = json_object_get_string(caller_action_obj);
        if (caller_action == NULL || *caller_action == '\0') return;
        json_object *caller_id_obj;
        if (json_object_object_get_ex(request, "caller_id", &caller_id_obj)) {
            const char *caller_id = json_object_get_string(caller_id_obj);
            if (caller_id == NULL || *caller_id == '\0') return;
            size_t n = strlen(caller_id);
            char app[n], env[n], rid[n];
            if (3 == sscanf(caller_id, "%[^-]-%[^-]-%[^-]", app, env, rid)) {
                size_t app_len = strlen(app);
                size_t action_len = strlen(caller_action);
                char caller_name[app_len + action_len + 2 + 8];
                strcpy(caller_name, "callers.");
                strcpy(caller_name + 8, app);
                caller_name[app_len + 8] = '-';
                strcpy(caller_name + app_len + 1 + 8, caller_action);
                replace_dots(caller_name+9);
                // printf("CALLER: %s\n", caller_name);
                json_object_object_add(increments->others, caller_name, NEW_INT1);
            }
        }
    }
}

void increments_add(increments_t *stored_increments, increments_t* increments)
{
    stored_increments->request_count += increments->request_count;
    for (size_t i=0; i<=last_resource_index; i++) {
        metric_pair_t *stored = &(stored_increments->metrics[i]);
        metric_pair_t *addend = &(increments->metrics[i]);
        stored->val += addend->val;
        stored->val_squared += addend->val_squared;
    }
    json_object_object_foreach(increments->others, key, value) {
        json_object *stored_obj, *new_obj = NULL;
        bool perform_addition = json_object_object_get_ex(stored_increments->others, key, &stored_obj);
        switch (json_object_get_type(value)) {
        case json_type_double: {
            double addend = json_object_get_double(value);
            if (perform_addition) {
                double stored = json_object_get_double(stored_obj);
                new_obj = json_object_new_double(stored + addend);
            } else {
                new_obj = json_object_new_double(addend);
            }
            break;
        }
        case json_type_int: {
            int addend = json_object_get_int(value);
            if (perform_addition) {
                int stored = json_object_get_int(stored_obj);
                new_obj = json_object_new_int(stored + addend);
            } else {
                new_obj = json_object_new_int(addend);
            }
            break;
        }
        default:
            fprintf(stderr, "unknown increment type\n");
        }
        json_object_object_add(stored_increments->others, key, new_obj);
    }
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

int dump_module_name(const char* key, void *module, void *arg)
{
    printf("module: %s\n", (char*)module);
    return 0;
}

void dump_metrics(metric_pair_t *metrics)
{
    for (size_t i=0; i<=last_resource_index; i++) {
        if (metrics[i].val > 0) {
            printf("%s:%f:%f\n", int_to_resource[i], metrics[i].val, metrics[i].val_squared);
        }
    }
}

int dump_increments(const char *key, void *total, void *arg)
{
    puts("------------------------------------------------");
    printf("action: %s\n", key);
    increments_t* increments = total;
    printf("requests: %zu\n", increments->request_count);
    dump_metrics(increments->metrics);
    dump_json_object(increments->others);
    return 0;
}

void processor_dump_state(processor_t *self)
{
    puts("================================================");
    printf("stream: %s\n", self->stream);
    printf("processed requests: %zu\n", self->request_count);
    zhash_foreach(self->modules, dump_module_name, NULL);
    zhash_foreach(self->totals, dump_increments, NULL);
    zhash_foreach(self->minutes, dump_increments, NULL);
}

int processor_dump_state_from_zhash(const char* stream, void* processor, void* arg)
{
    assert(!strcmp(((processor_t*)processor)->stream,stream));
    processor_dump_state(processor);
    return 0;
}

bson_t* increments_to_bson(const char* namespace, increments_t* increments)
{
    // dump_increments(namespace, increments, NULL);

    bson_t *incs = bson_new();
    bson_append_int32(incs, "count", 5, increments->request_count);

    for (size_t i=0; i<last_resource_index; i++) {
        double val = increments->metrics[i].val;
        if (val > 0) {
            const char *name = int_to_resource[i];
            bson_append_double(incs, name, strlen(name), val);
            const char *name_sq = int_to_resource_sq[i];
            bson_append_double(incs, name_sq, strlen(name_sq), increments->metrics[i].val_squared);
        }
    }

    json_object_object_foreach(increments->others, key, value_obj) {
        size_t n = strlen(key);
        switch (json_object_get_type(value_obj)) {
        case json_type_int:
            bson_append_int32(incs, key, n, json_object_get_int(value_obj));
            break;
        case json_type_double:
            bson_append_double(incs, key, n, json_object_get_double(value_obj));
            break;
        default:
            fprintf(stderr, "unsupported json type in json to bson conversion\n");
        }
    }

    bson_t *document = bson_new();
    bson_append_document(document, "$inc", 4, incs);

    // size_t n;
    // char* bs = bson_as_json(document, &n);
    // printf("document. size: %zu; value:%s\n", n, bs);
    // bson_destroy(bs);

    bson_destroy(incs);

    return document;
}

int minutes_add_increments(const char* namespace, void* data, void* arg)
{
    mongoc_collection_t *collection = arg;
    increments_t* increments = data;

    int minute = 0;
    char* p = (char*) namespace;
    while (isdigit(*p)) {
        minute *= 10;
        minute += *(p++) - '0';
    }
    p++;

    bson_t *selector = bson_new();
    assert( bson_append_utf8(selector, "page", 4, p, strlen(p)) );
    assert( bson_append_int32(selector, "minute", 6, minute ) );

    // size_t n;
    // char* bs = bson_as_json(selector, &n);
    // printf("selector. size: %zu; value:%s\n", n, bs);
    // bson_destroy(bs);

    bson_t *document = increments_to_bson(namespace, increments);
    bson_error_t *error = NULL;
    if (!mongoc_collection_update(collection, MONGOC_UPDATE_UPSERT, selector, document, wc_no_wait, error)) {
        fprintf(stderr, "update failed on totals\n");
    }

    bson_destroy(selector);
    bson_destroy(document);
    return 0;
}

int totals_add_increments(const char* namespace, void* data, void* arg)
{
    mongoc_collection_t *collection = arg;
    increments_t* increments = data;

    bson_t *selector = bson_new();
    assert( bson_append_utf8(selector, "page", 4, namespace, strlen(namespace)) );

    // size_t n;
    // char* bs = bson_as_json(selector, &n);
    // printf("selector. size: %zu; value:%s\n", n, bs);
    // bson_destroy(bs);

    bson_t *document = increments_to_bson(namespace, increments);
    bson_error_t *error = NULL;
    if (!mongoc_collection_update(collection, MONGOC_UPDATE_UPSERT, selector, document, wc_no_wait, error)) {
        fprintf(stderr, "update failed on totals\n");
    }

    bson_destroy(selector);
    bson_destroy(document);
    return 0;
}

int quants_add_quants(const char* namespace, void* data, void* arg)
{
    mongoc_collection_t *collection = arg;

    // extract keys from namespace
    char* p = (char*) namespace;
    char kind[2];
    kind[0] = *(p++);
    kind[1] = '\0';
    // skip '-''
    p++;
    size_t quant = 0;
    while (isdigit(*p)) {
        quant *= 10;
        quant += *(p++) - '0';
    }
    // skip -
    p++;

    size_t resource_index = 0;
    while (isdigit(*p)) {
        resource_index *= 10;
        resource_index += *(p++) - '0';
    }
    // skip -
    p++;
    const char *resource = i2r(resource_index);

    bson_t *selector = bson_new();
    bson_append_utf8(selector, "page", 4, p, strlen(p));
    bson_append_utf8(selector, "kind", 4, kind, 1);
    bson_append_int32(selector, "quant", 5, quant);

    // size_t n;
    // char* bs = bson_as_json(selector, &n);
    // printf("selector. size: %zu; value:%s\n", n, bs);
    // bson_destroy(bs);

    bson_t *incs = bson_new();
    bson_append_int32(incs, resource, strlen(resource), (size_t)data);

    bson_t *document = bson_new();
    bson_append_document(document, "$inc", 4, incs);

    // bs = bson_as_json(document, &n);
    // printf("document. size: %zu; value:%s\n", n, bs);
    // bson_destroy(bs);

    bson_error_t *error = NULL;
    if (!mongoc_collection_update(collection, MONGOC_UPDATE_UPSERT, selector, document, wc_no_wait, error)) {
        fprintf(stderr, "update failed on totals\n");
    }

    bson_destroy(selector);
    bson_destroy(incs);
    bson_destroy(document);
    return 0;
}

void ensure_known_database(mongoc_client_t *client, const char* db_name)
{
    mongoc_collection_t *meta_collection = mongoc_client_get_collection(client, "logjam-global", "metadata");
    bson_t *selector = bson_new();
    assert(bson_append_utf8(selector, "name", 4, "databases", 9));

    bson_t *document = bson_new();
    bson_t *sub_doc = bson_new();
    bson_append_utf8(sub_doc, "value", 5, db_name, -1);
    bson_append_document(document, "$addToSet", 9, sub_doc);

    bson_error_t error;
    if (!mongoc_collection_update(meta_collection, MONGOC_UPDATE_UPSERT, selector, document, wc_no_wait, &error)) {
        fprintf(stderr, "update failed on totals\n");
    }

    bson_destroy(selector);
    bson_destroy(document);
    bson_destroy(sub_doc);

    mongoc_collection_destroy(meta_collection);
}

stream_collections_t *stream_collections_new(mongoc_client_t* client, const char* stream)
{
    stream_collections_t *collections = malloc(sizeof(stream_collections_t));
    assert(collections);
    bson_error_t error;
    bson_t *keys;

    collections->totals = mongoc_client_get_collection(client, stream, "totals");
    keys = bson_new();
    assert(bson_append_int32(keys, "page", 4, 1));
    mongoc_collection_ensure_index(collections->totals, keys, &index_opt_background, &error);
    bson_destroy(keys);

    collections->minutes = mongoc_client_get_collection(client, stream, "minutes");
    keys = bson_new();
    assert(bson_append_int32(keys, "page", 4, 1));
    assert(bson_append_int32(keys, "minutes", 6, 1));
    mongoc_collection_ensure_index(collections->minutes, keys, &index_opt_background, &error);
    bson_destroy(keys);

    collections->quants = mongoc_client_get_collection(client, stream, "quants");
    keys = bson_new();
    assert(bson_append_int32(keys, "page", 4, 1));
    assert(bson_append_int32(keys, "kind", 4, 1));
    assert(bson_append_int32(keys, "quant", 5, 1));
    mongoc_collection_ensure_index(collections->quants, keys, &index_opt_background, &error);
    bson_destroy(keys);

    return collections;
}

void destroy_stream_collections(stream_collections_t* collections)
{
    mongoc_collection_destroy(collections->totals);
    mongoc_collection_destroy(collections->minutes);
    mongoc_collection_destroy(collections->quants);
    free(collections);
}

stream_collections_t *stats_updater_get_collections(stats_updater_state_t *self, const char* stream)
{
    stream_collections_t *collections = zhash_lookup(self->stream_collections, stream);
    if (collections == NULL) {
        ensure_known_database(self->mongo_client, stream);
        collections = stream_collections_new(self->mongo_client, stream);
        assert(collections);
        zhash_insert(self->stream_collections, stream, collections);
        zhash_freefn(self->stream_collections, stream, (zhash_free_fn*)destroy_stream_collections);
    }
    return collections;
}

int processor_update_mongo_db(const char* stream, void* data, void* arg)
{
    stats_updater_state_t *state = arg;
    processor_t *processor = data;
    stream_collections_t *collections = stats_updater_get_collections(state, processor->stream);

    zhash_foreach(processor->totals, totals_add_increments, collections->totals);
    zhash_foreach(processor->minutes, minutes_add_increments, collections->minutes);
    zhash_foreach(processor->quants, quants_add_quants, collections->quants);

    return 0;
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
            memcpy(module_str+2, page, mod_len);
            module_str[mod_len+2] = '\0';
        }
    } else {
        char *action_ptr = strchr(page, '#');
        if (action_ptr != NULL) {
            int mod_len = action_ptr - page;
            memcpy(module_str+2, page, mod_len);
            module_str[mod_len+2] = '\0';
        }
    }
    char *module = zhash_lookup(self->modules, module_str);
    if (module == NULL) {
        module = strdup(module_str);
        int rc = zhash_insert(self->modules, module, module);
        assert(rc == 0);
        zhash_freefn(self->modules, module, free);
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
    // TODO: autodetect severity from log lines if present (seems missing often in production)
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

void processor_add_totals(processor_t *self, const char* namespace, increments_t *increments)
{
    increments_t *stored_increments = zhash_lookup(self->totals, namespace);
    if (stored_increments) {
        increments_add(stored_increments, increments);
    } else {
        increments_t *duped_increments = increments_clone(increments);
        int rc = zhash_insert(self->totals, namespace, duped_increments);
        assert(rc == 0);
        assert(zhash_freefn(self->totals, namespace, increments_destroy));
    }
}

void processor_add_minutes(processor_t *self, const char* namespace, size_t minute, increments_t *increments)
{
    char key[2000];
    snprintf(key, 2000, "%lu-%s", minute, namespace);
    increments_t *stored_increments = zhash_lookup(self->minutes, key);
    if (stored_increments) {
        increments_add(stored_increments, increments);
    } else {
        increments_t *duped_increments = increments_clone(increments);
        int rc = zhash_insert(self->minutes, key, duped_increments);
        assert(rc == 0);
        assert(zhash_freefn(self->minutes, key, increments_destroy));
    }
}

int add_quant_to_quants_hash(const char* key, void* data, void *arg)
{
    zhash_t* target = arg;
    void *stored = zhash_lookup(target, key);
    if (stored) {
        size_t new_val = ((size_t)stored) + ((size_t)data);
        zhash_update(target, key, (void*)new_val);
    } else {
        zhash_insert(target, key, stored);
    }
    return 0;
}

void quants_combine(zhash_t *target, zhash_t *source)
{
    zhash_foreach(source, add_quant_to_quants_hash, target);
}

void add_quant(const char* namespace, size_t resource_idx, char kind, size_t quant, zhash_t* quants)
{
    char key[2000];
    sprintf(key, "%c-%zu-%zu-%s", kind, quant, resource_idx, namespace);
    // printf("QUANT-KEY: %s\n", key);
    void *stored = zhash_lookup(quants, key);
    if (stored) {
        size_t new_val = ((size_t)stored) + 1;
        zhash_update(quants, key, (void*)new_val);
    } else {
        zhash_insert(quants, key, (void*)1);
    }
}

void processor_add_quants(processor_t *self, const char* namespace, increments_t *increments)
{
    for (int i=0; i<last_resource_index; i++){
        double val = increments->metrics[i].val;
        if (val > 0) {
            char kind;
            double d;
            if (i <= last_time_resource_index) {
                kind = 't';
                d = 100.0;
            } else if (i == allocated_objects_index) {
                kind = 'm';
                d = 10000.0;
            } else if (i == allocated_bytes_index) {
                kind = 'm';
                d = 100000.0;
            } else {
                continue;
            }
            size_t x = (ceil(floor(val/d))+1) * d;
            add_quant(namespace, i, kind, x, self->quants);
            add_quant("all_pages", i, kind, x, self->quants);
        }
    }
}

//TODO: generalize this
bool interesting_request(request_data_t *request_data, json_object *request)
{
    return
        request_data->total_time > 100 ||
        request_data->severity > 1 ||
        request_data->response_code >= 400;
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

    increments_t* increments = increments_new();
    increments_fill_metrics(increments, request);
    increments_fill_apdex(increments, &request_data);
    increments_fill_response_code(increments, &request_data);
    increments_fill_severity(increments, &request_data);
    increments_fill_exceptions(increments, request);
    increments_fill_caller_info(increments, request);

    processor_add_totals(self, request_data.page, increments);
    processor_add_totals(self, request_data.module, increments);
    processor_add_totals(self, "all_pages", increments);

    processor_add_minutes(self, request_data.page, request_data.minute, increments);
    processor_add_minutes(self, request_data.module, request_data.minute, increments);
    processor_add_minutes(self, "all_pages", request_data.minute, increments);

    processor_add_quants(self, request_data.page, increments);

    increments_destroy(increments);
    // dump_json_object(request);
    // if (self->request_count % 100 == 0) {
    //     processor_dump_state(self);
    // }
    if (interesting_request(&request_data, request)) {
        json_object_get(request);
        zmsg_t *msg = zmsg_new();
        zmsg_addstr(msg, self->stream);
        zmsg_addmem(msg, &request, sizeof(json_object*));
        zmsg_send(&msg, pstate->push_socket);
    }
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

        if (processor == NULL) return;

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

zhash_t* processor_hash_new()
{
    zhash_t *hash = zhash_new();
    assert(hash);
    return hash;
}

void parser(void *args, zctx_t *ctx, void *pipe)
{
    parser_state_t state;
    state.request_count = 0;
    state.controller_socket = pipe;
    state.pull_socket = parser_pull_socket_new(ctx);
    state.push_socket = parser_push_socket_new(ctx);
    assert( state.tokener = json_tokener_new() );
    state.processors = processor_hash_new();

    zpoller_t *poller = zpoller_new(state.controller_socket, state.pull_socket, NULL);
    assert(poller);

    while (!zctx_interrupted) {
        // -1 == block until something is readable
        void *socket = zpoller_wait(poller, -1);
        zmsg_t *msg = NULL;
        if (socket == state.controller_socket) {
            // tick
            printf("parser: tick (%zu messages)\n", state.request_count);
            msg = zmsg_recv(state.controller_socket);
            zmsg_t *answer = zmsg_new();
            zmsg_addmem(answer, &state.processors, sizeof(zhash_t*));
            zmsg_addmem(answer, &state.request_count, sizeof(size_t));
            zmsg_send(&answer, state.controller_socket);
            state.request_count = 0;
            state.processors = processor_hash_new();
        } else if (socket == state.pull_socket) {
            msg = zmsg_recv(state.pull_socket);
            if (msg != NULL) {
                state.request_count++;
                parse_msg_and_forward_interesting_requests(msg, &state);
            }
        } else {
            // interrupted
            break;
        }
        zmsg_destroy(&msg);
    }
}

void extract_parser_state(zmsg_t* msg, zhash_t **processors, size_t *request_count)
{
    zframe_t *first = zmsg_first(msg);
    zframe_t *second = zmsg_next(msg);
    assert(zframe_size(first) == sizeof(zhash_t*));
    memcpy(&*processors, zframe_data(first), sizeof(zhash_t*));
    assert(zframe_size(second) == sizeof(size_t));
    memcpy(request_count, zframe_data(second), sizeof(size_t));
}

void stats_updater(void *args, zctx_t *ctx, void *pipe)
{
    stats_updater_state_t state;
    state.controller_socket = pipe;
    state.mongo_client = mongoc_client_new(mongo_uri);
    assert(state.mongo_client);
    state.stream_collections = zhash_new();
    size_t ticks = 0;

    while (!zctx_interrupted) {
        zmsg_t *msg = zmsg_recv(pipe);
        if (msg != NULL) {
            int64_t start_time_ms = zclock_time();
            zhash_t *processors;
            size_t request_count;
            extract_parser_state(msg, &processors, &request_count);
            size_t num_procs = zhash_size(processors);
            if (!dryrun) {
                zhash_foreach(processors, processor_update_mongo_db, &state);
            }
            // refresh database information every minute
            if (ticks++ % 60 == 0) {
                zhash_destroy(&state.stream_collections);
                state.stream_collections = zhash_new();
            }
            zhash_destroy(&processors);
            zmsg_destroy(&msg);
            int64_t end_time_ms = zclock_time();
            printf("stats updater: %zu updates (%d ms)\n", num_procs, (int)(end_time_ms - start_time_ms));
        }
    }

    zhash_destroy(&state.stream_collections);
    mongoc_client_destroy(state.mongo_client);
}

void* request_writer_pull_socket_new(zctx_t *context)
{
    void *socket = zsocket_new(context, ZMQ_PULL);
    assert(socket);
    int rc = zsocket_bind(socket, "inproc://request_writer");
    assert(rc == 0);
    return socket;
}

void add_request_field_index(const char* field, mongoc_collection_t *requests_collection)
{
    bson_error_t error;
    bson_t *index_keys;

    // collection.create_index([ [f, 1] ], :background => true, :sparse => true)
    index_keys = bson_new();
    bson_append_int32(index_keys, field, strlen(field), 1);
    mongoc_collection_ensure_index(requests_collection, index_keys, &index_opt_background, &error);
    bson_destroy(index_keys);

    // collection.create_index([ ["page", 1], [f, 1] ], :background => true)
    index_keys = bson_new();
    bson_append_int32(index_keys, "page", 4, 1);
    bson_append_int32(index_keys, field, strlen(field), 1);
    mongoc_collection_ensure_index(requests_collection, index_keys, &index_opt_background, &error);
    bson_destroy(index_keys);
}

void add_request_collection_indexes(const char* stream, mongoc_collection_t *requests_collection, request_writer_state_t* state)
{
    bson_error_t error;
    bson_t *index_keys;

    // collection.create_index([ ["metrics.n", 1], ["metrics.v", -1] ], :background => true)
    index_keys = bson_new();
    bson_append_int32(index_keys, "metrics.n", 9, 1);
    bson_append_int32(index_keys, "metrics.v", 9, -1);
    mongoc_collection_ensure_index(requests_collection, index_keys, &index_opt_background, &error);
    bson_destroy(index_keys);

    // collection.create_index([ ["page", 1], ["metrics.n", 1], ["metrics.v", -1] ], :background => true
    index_keys = bson_new();
    bson_append_int32(index_keys, "page", 4, 1);
    bson_append_int32(index_keys, "metrics.n", 9, 1);
    bson_append_int32(index_keys, "metrics.v", 9, -1);
    mongoc_collection_ensure_index(requests_collection, index_keys, &index_opt_background, &error);
    bson_destroy(index_keys);

    add_request_field_index("response_code", requests_collection);
    add_request_field_index("severity",      requests_collection);
    add_request_field_index("minute",        requests_collection);
    add_request_field_index("exceptions",    requests_collection);
}

mongoc_collection_t* request_writer_get_request_collection(request_writer_state_t* self, const char* stream)
{
    mongoc_collection_t *collection = zhash_lookup(self->request_collections, stream);
    if (collection == NULL) {
        // printf("creating requests collection: %s\n", stream);
        collection = mongoc_client_get_collection(self->mongo_client, stream, "requests");
        add_request_collection_indexes(stream, collection, self);
        zhash_insert(self->request_collections, stream, collection);
        zhash_freefn(self->request_collections, stream, (zhash_free_fn*)mongoc_collection_destroy);
    }
    return collection;
}

static void json_object_to_bson(json_object *j, bson_t *b);

//TODO: optimize this!
//TODO: validate utf8!
//TODO: replace dots in keys!
static void json_key_to_bson_key(bson_t *b, json_object *val, const char *key)
{
    enum json_type type = json_object_get_type(val);
    switch (type) {
    case json_type_boolean:
        bson_append_bool(b, key, -1, json_object_get_boolean(val));
        break;
    case json_type_double:
        bson_append_double(b, key, -1, json_object_get_double(val));
        break;
    case json_type_int:
        bson_append_int32(b, key, -1, json_object_get_int(val));
        break;
    case json_type_object: {
        bson_t *sub = bson_new();
        json_object_to_bson(val, sub);
        bson_append_document(b, key, -1, sub);
        bson_destroy(sub);
        break;
    }
    case json_type_array: {
        bson_t *sub = bson_new();
        int array_len = json_object_array_length(val);
        for (int pos = 0; pos < array_len; pos++) {
            char nk[100];
            sprintf(nk, "%d", pos);
            json_key_to_bson_key(sub, json_object_array_get_idx(val, pos), nk);
        }
        bson_append_array(b, key, -1, sub);
        bson_destroy(sub);
        break;
    }
    case json_type_string:
        bson_append_utf8(b, key, -1, json_object_get_string(val), -1);
        break;
    case json_type_null:
        bson_append_null(b, key, -1);
        break;
    default:
        fprintf(stderr, "unexpected json type: %s\n", json_type_to_name(type));
        break;
    }
}

static void json_object_to_bson(json_object *j, bson_t* b)
{
  json_object_object_foreach(j, key, val) {
      json_key_to_bson_key(b, val, key);
  }
}

bool json_object_is_zero(json_object* jobj)
{
    enum json_type type = json_object_get_type(jobj);
    if (type == json_type_double) {
        return 0.0 == json_object_get_double(jobj);
    }
    else if (type == json_type_int) {
        return 0 == json_object_get_int(jobj);
    }
    return false;
}

void convert_metrics_for_indexing(json_object *request)
{
    json_object *metrics = json_object_new_array();
    for (int i=0; i<last_resource_index; i++) {
        const char* resource = int_to_resource[i];
        json_object *resource_val;
        if (json_object_object_get_ex(request, resource, &resource_val)) {
            json_object_get(resource_val);
            json_object_object_del(request, resource);
            if (json_object_is_zero(resource_val)) {
                json_object_put(resource_val);
            } else {
                json_object *metric_pair = json_object_new_object();
                json_object_object_add(metric_pair, "n", json_object_new_string(resource));
                json_object_object_add(metric_pair, "v", resource_val);
                json_object_array_add(metrics, metric_pair);
            }
        }
    }
    json_object_object_add(request, "metrics", metrics);
}

void store_request(const char* stream, json_object* request, request_writer_state_t* state)
{
    // dump_json_object(request);

    mongoc_collection_t *requests_collection = request_writer_get_request_collection(state, stream);
    json_object *request_id_obj;

    if (json_object_object_get_ex(request, "request_id", &request_id_obj)) {
        const char *request_id = json_object_get_string(request_id_obj);
        json_object_get(request_id_obj);
        json_object_object_del(request, "request_id");

        convert_metrics_for_indexing(request);

        bson_t *document = bson_sized_new(2048);
        json_object_to_bson(request, document);
        // TODO: protect against non uuids (l != 32)
        bson_append_binary(document, "_id", 3, BSON_SUBTYPE_UUID_DEPRECATED, (uint8_t*)request_id, strlen(request_id));

        // size_t n;
        // char* bs = bson_as_json(document, &n);
        // printf("doument. size: %zu; value:%s\n", n, bs);
        // bson_destroy(bs);

        bson_error_t error;
        if (!mongoc_collection_insert(requests_collection, MONGOC_INSERT_NONE, document, wc_no_wait, &error)) {
            printf("insert failed for request document: %s\n", error.message);
        }

        json_object_put(request_id_obj);
        bson_destroy(document);
    } else {
        fprintf(stderr, "dropped request without request_id\n");
    }
}

void handle_request_msg(zmsg_t* msg, request_writer_state_t* state)
{
    zframe_t *first = zmsg_first(msg);
    zframe_t *second = zmsg_next(msg);
    size_t stream_len = zframe_size(first);
    char stream[stream_len+1];
    memcpy(stream, zframe_data(first), stream_len);
    stream[stream_len] = '\0';
    // printf("request_writer: stream: %s\n", stream);
    json_object *request;
    memcpy(&request, zframe_data(second), sizeof(json_object*));
    // dump_json_object(request);
    store_request(stream, request, state);
    json_object_put(request);
}

void request_writer(void *args, zctx_t *ctx, void *pipe)
{
    request_writer_state_t state;
    state.request_count = 0;
    state.controller_socket = pipe;
    state.pull_socket = request_writer_pull_socket_new(ctx);
    state.mongo_client = mongoc_client_new(mongo_uri);
    assert(state.mongo_client);
    state.request_collections = zhash_new();
    size_t ticks = 0;

    zpoller_t *poller = zpoller_new(state.controller_socket, state.pull_socket, NULL);
    assert(poller);

    while (!zctx_interrupted) {
        // -1 == block until something is readable
        void *socket = zpoller_wait(poller, -1);
        zmsg_t *msg = NULL;
        if (socket == state.controller_socket) {
            // tick
            printf("request_writer: tick (%zu messages)\n", state.request_count);
            // free collection pointers every minute
            msg = zmsg_recv(state.controller_socket);
            if (ticks++ % 60 == 0) {
                printf("request_writer: freeing request collections\n");
                zhash_destroy(&state.request_collections);
                state.request_collections = zhash_new();
            }
            state.request_count = 0;
        } else if (socket == state.pull_socket) {
            msg = zmsg_recv(state.pull_socket);
            if (msg != NULL) {
                state.request_count++;
                if (!dryrun) {
                    handle_request_msg(msg, &state);
                }
            }
        } else {
            // interrupted
            break;
        }
        zmsg_destroy(&msg);
    }

    zhash_destroy(&state.request_collections);
    mongoc_client_destroy(state.mongo_client);
}

typedef struct {
    zhash_t *source;
    zhash_t *target;
} hash_pair_t;


int add_modules(const char* module, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    char *dest = zhash_lookup(pair->target, module);
    if (dest == NULL) {
        zhash_insert(pair->target, module, data);
        zhash_freefn(pair->target, module, free);
        zhash_freefn(pair->source, module, NULL);
    }
    return 0;
}

int add_increments(const char* namespace, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    increments_t *dest_increments = zhash_lookup(pair->target, namespace);
    if (dest_increments == NULL) {
        zhash_insert(pair->target, namespace, data);
        zhash_freefn(pair->target, namespace, increments_destroy);
        zhash_freefn(pair->source, namespace, NULL);
    } else {
        increments_add(dest_increments, (increments_t*)data);
    }
    return 0;
}

void modules_combine(zhash_t* target, zhash_t *source)
{
    hash_pair_t hash_pair;
    hash_pair.source = source;
    hash_pair.target = target;
    zhash_foreach(source, add_modules, &hash_pair);
}

void increments_combine(zhash_t* target, zhash_t *source)
{
    hash_pair_t hash_pair;
    hash_pair.source = source;
    hash_pair.target = target;
    zhash_foreach(source, add_increments, &hash_pair);
}

void processor_combine(processor_t* target, processor_t* source)
{
    // printf("combining %s\n", target->stream);
    assert(!strcmp(target->stream, source->stream));
    target->request_count += source->request_count;
    modules_combine(target->modules, source->modules);
    increments_combine(target->totals, source->totals);
    increments_combine(target->minutes, source->minutes);
    quants_combine(target->quants, source->quants);
}

int add_streams(const char* stream, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    // printf("checking %s\n", stream);
    processor_t *dest = zhash_lookup(pair->target, stream);
    if (dest == NULL) {
        zhash_insert(pair->target, stream, data);
        zhash_freefn(pair->target, stream, processor_destroy);
        zhash_freefn(pair->source, stream, NULL);
    } else {
        processor_combine(dest, (processor_t*)data);
    }
    return 0;
}

int collect_stats_and_forward(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    controller_state_t *state = arg;
    zhash_t *processors[NUM_PARSERS];
    size_t request_counts[NUM_PARSERS];
    int64_t start_time_ms = zclock_time();

    zmsg_t *tick = zmsg_new();
    zmsg_addstr(tick, "tick");
    zmsg_send(&tick, state->request_writer_pipe);

    for (size_t i=0; i<NUM_PARSERS; i++) {
        void* parser_pipe = state->parser_pipes[i];
        zmsg_t *tick = zmsg_new();
        zmsg_addstr(tick, "tick");
        zmsg_send(&tick, parser_pipe);
        zmsg_t *response = zmsg_recv(parser_pipe);
        extract_parser_state(response, &processors[i], &request_counts[i]);
        zmsg_destroy(&response);
    }

    size_t request_count = request_counts[0];
    for (size_t i=1; i<NUM_PARSERS; i++) {
        request_count += request_counts[i];
    }

    for (size_t i=1; i<NUM_PARSERS; i++) {
        hash_pair_t pair;
        pair.source = processors[i];
        pair.target = processors[0];
        zhash_foreach(pair.source, add_streams, &pair);
        zhash_destroy(&processors[i]);
    }

    zmsg_t *stats_msg = zmsg_new();
    zmsg_addmem(stats_msg, &processors[0], sizeof(zhash_t*));
    zmsg_addmem(stats_msg, &request_count, sizeof(size_t));
    zmsg_send(&stats_msg, state->stats_updater_pipe);

    int64_t end_time_ms = zclock_time();
    printf("stats collector: %zu messages (%zu ms)\n", request_count, (size_t)(end_time_ms - start_time_ms));

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
        int_to_resource[last_resource_index] = resource;
        char resource_sq[256] = {'\0'};
        strcpy(resource_sq, resource);
        strcpy(resource_sq+strlen(resource), "_sq");
        int_to_resource_sq[last_resource_index++] = strdup(resource_sq);
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

    allocated_objects_index = r2i("allocated_memory");
    allocated_bytes_index = r2i("allocated_bytes");

    for (size_t j=0; j<=last_resource_index; j++) {
        const char *r = i2r(j);
        printf("%s = %zu\n", r, r2i(r));
    }
}

void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-n] [-c config-file]\n", argv[0]);
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "nc:")) != -1) {
        switch (c) {
        case 'n':
            dryrun = true;;
            break;
        case 'c':
            config_file = optarg;
            break;
        case '?':
            if (optopt == 'c')
                fprintf (stderr, "option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf (stderr, "unknown option `-%c'.\n", optopt);
            else
                fprintf (stderr, "unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            exit(1);
        }
    }
}

int main(int argc, char * const *argv)
{
    int rc;
    process_arguments(argc, argv);

    if (!zsys_file_exists(config_file)) {
        fprintf(stderr, "missing config file: %s\n", config_file);
        exit(1);
    }

    // load config
    config = zconfig_load((char*)config_file);
    setup_resource_maps();

    setvbuf(stdout,NULL,_IOLBF,0);
    setvbuf(stderr,NULL,_IOLBF,0);

    // initialize mongodb client
    initialize_mongo_db_globals();

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
    state.subscriber_pipe = zthread_fork(context, subscriber, &config);
    state.stats_updater_pipe = zthread_fork(context, stats_updater, &config);
    state.request_writer_pipe = zthread_fork(context, request_writer, &config);
    for (size_t i=0; i<NUM_PARSERS; i++) {
        state.parser_pipes[i] = zthread_fork(context, parser, &config);
    }

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
