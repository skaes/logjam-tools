#include "importer-requestwriter.h"
#include "importer-livestream.h"
#include "importer-indexer.h"
#include "importer-resources.h"
#include "importer-mongoutils.h"
#include "statsd-client.h"


/*
 * connections: n_w = num_writers, n_p = num_parsers, "o" = bind, "[<>v^]" = connect
 *
 *                              controller
 *                                  |
 *                                 PIPE
 *               PUSH    PULL       |       PUSH    PULL
 *  parser(n_p)  >----------o  writer(n_w)  >----------o  live stream publisher
 *
 */

// It might be better to insert a load balancer device between parsers and requests writers.

typedef struct {
    zconfig_t* config;
    char me[16];
    size_t id;
    mongoc_client_t* mongo_clients[MAX_DATABASES];
    zhash_t *request_collections;
    zhash_t *metrics_collections;
    zhash_t *jse_collections;
    zhash_t *events_collections;
    zsock_t *pipe;         // actor command pipe
    zsock_t *pull_socket;
    zsock_t *live_stream_socket;
    int updates_count;     // updates performend since last tick
    int update_time;       // processing time since last tick (micro seconds)
    statsd_client_t *statsd_client;
} request_writer_state_t;


static
zsock_t* request_writer_pull_socket_new(int i)
{
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    int rc = zsock_bind(socket, "inproc://request-writer-%d", i);
    assert(rc == 0);
    return socket;
}

static
mongoc_collection_t* request_writer_get_request_collection(request_writer_state_t* self, const char* db_name, stream_info_t *stream_info)
{
    if (dryrun) return NULL;
    mongoc_collection_t *collection = zhash_lookup(self->request_collections, db_name);
    if (collection == NULL) {
        // printf("[D] creating requests collection: %s\n", db_name);
        mongoc_client_t *mongo_client = self->mongo_clients[stream_info->db];
        collection = mongoc_client_get_collection(mongo_client, db_name, "requests");
        // add_request_collection_indexes(db_name, collection);
        zhash_insert(self->request_collections, db_name, collection);
        zhash_freefn(self->request_collections, db_name, (zhash_free_fn*)mongoc_collection_destroy);
    }
    return collection;
}

static
mongoc_collection_t* request_writer_get_metrics_collection(request_writer_state_t* self, const char* db_name, stream_info_t *stream_info)
{
    if (dryrun) return NULL;
    mongoc_collection_t *collection = zhash_lookup(self->metrics_collections, db_name);
    if (collection == NULL) {
        // printf("[D] creating metrics collection: %s\n", db_name);
        mongoc_client_t *mongo_client = self->mongo_clients[stream_info->db];
        collection = mongoc_client_get_collection(mongo_client, db_name, "metrics");
        zhash_insert(self->metrics_collections, db_name, collection);
        zhash_freefn(self->metrics_collections, db_name, (zhash_free_fn*)mongoc_collection_destroy);
    }
    return collection;
}

static
mongoc_collection_t* request_writer_get_jse_collection(request_writer_state_t* self, const char* db_name, stream_info_t *stream_info)
{
    if (dryrun) return NULL;
    mongoc_collection_t *collection = zhash_lookup(self->jse_collections, db_name);
    if (collection == NULL) {
        // printf("[D] creating jse collection: %s\n", db_name);
        mongoc_client_t *mongo_client = self->mongo_clients[stream_info->db];
        collection = mongoc_client_get_collection(mongo_client, db_name, "js_exceptions");
        // add_jse_collection_indexes(db_name, collection);
        zhash_insert(self->jse_collections, db_name, collection);
        zhash_freefn(self->jse_collections, db_name, (zhash_free_fn*)mongoc_collection_destroy);
    }
    return collection;
}

static
mongoc_collection_t* request_writer_get_events_collection(request_writer_state_t* self, const char* db_name, stream_info_t *stream_info)
{
    if (dryrun) return NULL;
    mongoc_collection_t *collection = zhash_lookup(self->events_collections, db_name);
    if (collection == NULL) {
        // printf("[D] creating events collection: %s\n", db_name);
        mongoc_client_t *mongo_client = self->mongo_clients[stream_info->db];
        collection = mongoc_client_get_collection(mongo_client, db_name, "events");
        zhash_insert(self->events_collections, db_name, collection);
        zhash_freefn(self->events_collections, db_name, (zhash_free_fn*)mongoc_collection_destroy);
    }
    return collection;
}

static
int bson_append_win1252(bson_t *b, const char *key, size_t key_len, const char* val, size_t val_len)
{
    char utf8[6*val_len+1];
    int new_len = convert_to_win1252(val, val_len, utf8);
    return bson_append_utf8(b, key, key_len, utf8, new_len);
}


static
void json_object_to_bson(const char* context, json_object *j, bson_t *b);

//TODO: optimize this!
static
void json_key_to_bson_key(const char* context, bson_t *b, json_object *val, const char *key)
{
    size_t n = strlen(key);
    char safe_key[4*n+1];
    int len = copy_replace_dots_and_dollars(safe_key, key);

    if (!bson_utf8_validate(safe_key, len, false)) {
        char tmp[6*len+1];
        len = convert_to_win1252(safe_key, len, tmp);
        strcpy(safe_key, tmp);
    }
    // printf("[D] safe_key: %s\n", safe_key);

    enum json_type type = json_object_get_type(val);
    switch (type) {
    case json_type_boolean:
        bson_append_bool(b, safe_key, len, json_object_get_boolean(val));
        break;
    case json_type_double:
        bson_append_double(b, safe_key, len, json_object_get_double(val));
        break;
    case json_type_int:
        bson_append_int64(b, safe_key, len, json_object_get_int64(val));
        break;
    case json_type_object: {
        bson_t *sub = bson_new();
        json_object_to_bson(context, val, sub);
        bson_append_document(b, safe_key, len, sub);
        bson_destroy(sub);
        break;
    }
    case json_type_array: {
        bson_t *sub = bson_new();
        int array_len = json_object_array_length(val);
        for (int pos = 0; pos < array_len; pos++) {
            char nk[100];
            sprintf(nk, "%d", pos);
            json_key_to_bson_key(context, sub, json_object_array_get_idx(val, pos), nk);
        }
        bson_append_array(b, safe_key, len, sub);
        bson_destroy(sub);
        break;
    }
    case json_type_string: {
        const char *str = json_object_get_string(val);
        size_t n = json_object_get_string_len(val);
        if (bson_utf8_validate(str, n, false /* disallow embedded null characters */)) {
            bson_append_utf8(b, safe_key, len, str, n);
        } else {
            fprintf(stderr,
                    "[W] invalid utf8. context: %s,  key: %s, value[len=%d]: %*s\n",
                    context, safe_key, (int)n, (int)n, str);
            // bson_append_binary(b, safe_key, len, BSON_SUBTYPE_BINARY, (uint8_t*)str, n);
            bson_append_win1252(b, safe_key, len, str, n);
        }
        break;
    }
    case json_type_null:
        bson_append_null(b, safe_key, len);
        break;
    default:
        fprintf(stderr, "[E] unexpected json type: %s\n", json_type_to_name(type));
        break;
    }
}

static
void json_object_to_bson(const char *context, json_object *j, bson_t* b)
{
  json_object_object_foreach(j, key, val) {
      json_key_to_bson_key(context, b, val, key);
  }
}

static
bool json_object_is_zero(json_object* jobj)
{
    enum json_type type = json_object_get_type(jobj);
    if (type == json_type_double) {
        return 0.0 == json_object_get_double(jobj);
    }
    else if (type == json_type_int) {
        return 0 == json_object_get_int64(jobj);
    }
    return false;
}

static
bson_t* convert_metrics_for_indexing(json_object *request)
{
    bson_t* metrics_doc = bson_new();
    json_object *metrics = json_object_new_array();
    for (int i=0; i<=last_resource_offset; i++) {
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

                enum json_type type = json_object_get_type(resource_val);
                if (type == json_type_double) {
                    double val = json_object_get_double(resource_val);
                    bson_append_double(metrics_doc, resource, strlen(resource), val);
                }
                else if (type == json_type_int) {
                    int64_t val = json_object_get_int64(resource_val);
                    bson_append_int64(metrics_doc, resource, strlen(resource), val);
                }
            }
        }
    }
    json_object_object_add(request, "metrics", metrics);
    return metrics_doc;
}

static
void add_metrics_to_metrics_collection(const char* db_name, stream_info_t* stream_info, bson_t* metrics, const char* page, const char* module, int minute, const char* rid, bson_oid_t* oid, request_writer_state_t* state)
{
    mongoc_collection_t *metrics_collection = request_writer_get_metrics_collection(state, db_name, stream_info);
    size_t n = bson_count_keys(metrics);
    bson_t *docs[n];
    bson_iter_t iter;
    bson_iter_init(&iter, metrics);
    bson_t** p = docs;
    while (*module == ':') module++;
    while (bson_iter_next(&iter)) {
        *p = bson_new();
        bson_append_utf8(*p, "page", 4, page, strlen(page));
        bson_append_utf8(*p, "module", 6, module, strlen(module));
        bson_append_int32(*p, "minute", 6, minute);
        const char *metric = bson_iter_key(&iter);
        bson_append_utf8(*p, "metric", 6,  metric, strlen(metric));
        bson_append_iter(*p, "value", 5, &iter);
        if (rid)
            bson_append_utf8(*p, "rid", 3, rid, strlen(rid));
        else
            bson_append_oid(*p, "rid", 3, oid);
        if (0) {
            size_t m;
            char* bjs = bson_as_json(*p, &m);
            // printf("[D] METRIC %s\n", bjs);
            bson_free(bjs);
        }
        p++;
    }
    if (!dryrun) {
        bson_t *opts = NULL;
        bson_t reply;
        bson_error_t error;
        bool ok = mongoc_collection_insert_many(metrics_collection, (const bson_t**)docs, n, opts, &reply, &error);
        if (!ok) {
            size_t m;
            char* bjs = bson_as_json(metrics, &m);
            char *reply_str = bson_as_json(&reply, NULL);
            fprintf(stderr,
                    "[E] could not insert metrics on %s: (%d) %s\n"
                    "[E] metrics document size: %zu; value: %s\n"
                    "[E] reply: %s\n",
                    db_name, error.code, error.message, n, bjs, reply_str);
            bson_free(bjs);
            bson_free(reply_str);
        }
    }
    for (size_t i=0; i<n; i++) {
        bson_destroy(docs[i]);
    }
}

static
json_object* store_request(const char* db_name, stream_info_t* stream_info, json_object* request, const char* module, request_writer_state_t* state)
{
    // dump_json_object(stdout, "[D]", request);
    bson_t *metrics = convert_metrics_for_indexing(request);
    if (0) {
        size_t n;
        char* bs = bson_as_json(metrics, &n);
        printf("[D] metrics document. size: %zu; value: %s\n", n, bs);
        bson_free(bs);
    }
    mongoc_collection_t *requests_collection = request_writer_get_request_collection(state, db_name, stream_info);
    bson_t *document = bson_sized_new(2048);

    json_object *request_id_obj;
    const char *request_id = NULL;
    if (json_object_object_get_ex(request, "request_id", &request_id_obj)) {
        request_id = json_object_get_string(request_id_obj);
        if (request_id==NULL || strlen(request_id) != 32) {
            // this can't be a UUID
            fprintf(stderr, "[E] invalid request_id: %s (stream: %s)\n", request_id, stream_info->key);
            dump_json_object(stderr, "[E]", request);
            request_id = NULL;
            request_id_obj = NULL;
        } else {
            json_object_get(request_id_obj);
            bson_append_binary(document, "_id", 3, BSON_SUBTYPE_UUID_DEPRECATED, (uint8_t*)request_id, 32);
        }
        json_object_object_del(request, "request_id");
    }
    bson_oid_t *oid = NULL;
    if (request_id == NULL) {
        // generate an oid
        oid = zmalloc(sizeof(*oid));
        bson_oid_init(oid, NULL);
        bson_append_oid(document, "_id", 3, oid);
        // printf("[D] generated oid for document:\n");
    }
    {
        size_t n = 1024;
        char context[n];
        snprintf(context, n, "%s:%s", db_name, request_id);
        json_object_to_bson(context, request, document);
    }

    // size_t n;
    // char* bs = bson_as_json(document, &n);
    // printf("[D] doument. size: %zu; value:%s\n", n, bs);
    // bson_free(bs);

    if (!dryrun) {
        bson_error_t error;
        int tries = TOKU_TX_RETRIES;
    retry:
        if (!mongoc_collection_insert(requests_collection, MONGOC_INSERT_NONE, document, wc_no_wait, &error)) {
            if ((error.code == TOKU_TX_LOCK_FAILED) && (--tries > 0)) {
                fprintf(stderr, "[W] retrying request insert operation on %s\n", db_name);
                goto retry;
            } else {
                size_t n;
                char* bjs = bson_as_json(document, &n);
                fprintf(stderr,
                        "[E] insert failed for request document with rid '%s' on %s: (%d) %s\n"
                        "[E] document size: %zu; value: %s\n",
                        request_id, db_name, error.code, error.message, n, bjs);
                bson_free(bjs);
            }
        }
    }
    bson_destroy(document);

    json_object *page_obj;
    if (json_object_object_get_ex(request, "page", &page_obj)) {
        const char* page = json_object_get_string(page_obj);
        json_object *minute_obj;
        if (json_object_object_get_ex(request, "minute", &minute_obj)) {
            int minute = json_object_get_int(minute_obj);
            add_metrics_to_metrics_collection(db_name, stream_info, metrics, page, module, minute, request_id, oid, state);
        }
    }

    if (oid)
        free(oid);
    bson_destroy(metrics);

    return request_id_obj;
}

static
void store_js_exception(const char* db_name, stream_info_t *stream_info, json_object* request, request_writer_state_t* state)
{
    mongoc_collection_t *jse_collection = request_writer_get_jse_collection(state, db_name, stream_info);
    bson_t *document = bson_sized_new(1024);
    json_object_to_bson("js_exception", request, document);

    if (!dryrun) {
        bson_error_t error;
        int tries = TOKU_TX_RETRIES;
    retry:
        if (!mongoc_collection_insert(jse_collection, MONGOC_INSERT_NONE, document, wc_no_wait, &error)) {
            if ((error.code == TOKU_TX_LOCK_FAILED) && (--tries > 0)) {
                fprintf(stderr, "[W] retrying exception insert operation on %s\n", db_name);
                goto retry;
            } else {
                size_t n;
                char* bjs = bson_as_json(document, &n);
                fprintf(stderr,
                        "[E] insert failed for exception document on %s: (%d) %s\n"
                        "[E] document size: %zu; value: %s\n",
                        db_name, error.code, error.message, n, bjs);
                bson_free(bjs);
            }
        }
    }
    bson_destroy(document);
}

static
void store_event(const char* db_name, stream_info_t *stream_info, json_object* request, request_writer_state_t* state)
{
    mongoc_collection_t *events_collection = request_writer_get_events_collection(state, db_name, stream_info);
    bson_t *document = bson_sized_new(1024);
    json_object_to_bson("event", request, document);

    json_object *uuid_obj;
    const char *uuid = NULL;
    if (json_object_object_get_ex(request, "uuid", &uuid_obj)) {
        uuid = json_object_get_string(uuid_obj);
        int len = strlen(uuid);
        if (len != 32) {
            // this can't be a UUID
            fprintf(stderr, "[W] invalid uuid: %s (stream: %s)\n", uuid, stream_info->key);
            dump_json_object(stderr, "[W]", request);
            uuid = NULL;
            uuid_obj = NULL;
        } else {
            json_object_get(uuid_obj);
            bson_append_binary(document, "_id", 3, BSON_SUBTYPE_UUID_DEPRECATED, (uint8_t*)uuid, 32);
        }
        json_object_object_del(request, "uuid");
    }
    if (uuid == NULL) {
        // generate an oid
        bson_oid_t oid;
        bson_oid_init(&oid, NULL);
        bson_append_oid(document, "_id", 3, &oid);
        // printf("[D] generated oid for document:\n");
    }
    {
        size_t n = 1024;
        char context[n];
        snprintf(context, n, "%s:%s", db_name, uuid);
        json_object_to_bson(context, request, document);
    }

    if (!dryrun) {
        bson_error_t error;
        int tries = TOKU_TX_RETRIES;
    retry:
        if (!mongoc_collection_insert(events_collection, MONGOC_INSERT_NONE, document, wc_no_wait, &error)) {
            if ((error.code == TOKU_TX_LOCK_FAILED) && (--tries > 0)) {
                fprintf(stderr, "[W] retrying event insert operation on %s\n", db_name);
                goto retry;
            } else {
                size_t n;
                char* bjs = bson_as_json(document, &n);
                fprintf(stderr,
                        "[E] insert failed for event document on %s: (%d) %s\n"
                        "[E] document size: %zu; value: %s\n",
                        db_name, error.code, error.message, n, bjs);
                bson_free(bjs);
            }
        }
    }
    bson_destroy(document);
}

static
json_object* extract_error_description(json_object* request, int severity)
{
    json_object *error_line = NULL;
    json_object *lines;
    if (json_object_object_get_ex(request, "lines", &lines)) {
        int len = json_object_array_length(lines);
        for (int i=0; i<len; i++) {
            json_object* line = json_object_array_get_idx(lines, i);
            if (line) {
                json_object* sev_obj = json_object_array_get_idx(line, 0);
                if (sev_obj != NULL && json_object_get_int(sev_obj) >= severity) {
                    error_line = json_object_array_get_idx(line, 2);
                    break;
                }
            }
        }
    }
    const char *description;
    if (error_line) {
        description = json_object_get_string(error_line);
    } else {
        description = "------ unknown ------";
    }
    return json_object_new_string(description);
}

static
void request_writer_publish_error(stream_info_t* stream_info, const char* module, json_object* request,
                                  request_writer_state_t* state, json_object* request_id)
{
    if (request_id == NULL) return;

    json_object *severity_obj;
    if (json_object_object_get_ex(request, "severity", &severity_obj)) {
        int severity = json_object_get_int(severity_obj);
        if (severity > 1) {
            json_object *error_info = json_object_new_object();
            json_object_get(request_id);
            json_object_object_add(error_info, "request_id", request_id);

            json_object_get(severity_obj);
            json_object_object_add(error_info, "severity", severity_obj);

            json_object *action;
            assert( json_object_object_get_ex(request, "page", &action) );
            json_object_get(action);
            json_object_object_add(error_info, "action", action);

            json_object *rsp;
            assert( json_object_object_get_ex(request, "response_code", &rsp) );
            json_object_get(rsp);
            json_object_object_add(error_info, "response_code", rsp);

            json_object *started_at;
            assert( json_object_object_get_ex(request, "started_at", &started_at) );
            json_object_get(started_at);
            json_object_object_add(error_info, "time", started_at);

            json_object *description = extract_error_description(request, severity);
            json_object_object_add(error_info, "description", description);

            json_object *arror = json_object_new_array();
            json_object_array_add(arror, error_info);

            const char *json_str = json_object_to_json_string_ext(arror, JSON_C_TO_STRING_PLAIN);

            publish_error_for_module(stream_info, "all_pages", json_str, state->live_stream_socket);
            publish_error_for_module(stream_info, module, json_str, state->live_stream_socket);

            json_object_put(arror);
        }
    }

    json_object_put(request_id);
}

static
void handle_request_msg(zmsg_t* msg, request_writer_state_t* state)
{
    zframe_t *db_frame = zmsg_first(msg);
    zframe_t *type_frame = zmsg_next(msg);
    zframe_t *mod_frame = zmsg_next(msg);
    zframe_t *body_frame = zmsg_next(msg);
    zframe_t *stream_frame = zmsg_next(msg);

    size_t db_name_len = zframe_size(db_frame);
    char db_name[db_name_len+1];
    memcpy(db_name, zframe_data(db_frame), db_name_len);
    db_name[db_name_len] = '\0';
    // printf("[D] request_writer: db name: %s\n", db_name);

    stream_info_t *stream_info;
    assert(zframe_size(stream_frame) == sizeof(stream_info_t*));
    memcpy(&stream_info, zframe_data(stream_frame), sizeof(stream_info_t*));
    // printf("[D] request_writer: stream name: %s\n", stream_info->key);

    size_t mod_len = zframe_size(mod_frame);
    char module[mod_len+1];
    memcpy(module, zframe_data(mod_frame), mod_len);
    module[mod_len] = '\0';

    json_object *request, *request_id;
    assert(zframe_size(body_frame) == sizeof(json_object*));
    memcpy(&request, zframe_data(body_frame), sizeof(json_object*));
    // dump_json_object(stdout, "[D]", request);

    assert(zframe_size(type_frame) == 1);
    char task_type = *((char*)zframe_data(type_frame));

    if (!dryrun) {
        switch (task_type) {
        case 'r':
            request_id = store_request(db_name, stream_info, request, module, state);
            request_writer_publish_error(stream_info, module, request, state, request_id);
            break;
        case 'j':
            store_js_exception(db_name, stream_info, request, state);
            break;
        case 'e':
            store_event(db_name, stream_info, request, state);
            break;
        default:
            fprintf(stderr, "[E] unknown task type for request_writer: %c\n", task_type);
        }
    }
    json_object_put(request);
}

static
request_writer_state_t* request_writer_state_new(zconfig_t *config, size_t id)
{
    request_writer_state_t *state = zmalloc(sizeof(request_writer_state_t));
    state->config = config;
    state->id = id;
    snprintf(state->me, 16, "writer[%zu]", id);
    state->pull_socket = request_writer_pull_socket_new(id);
    state->live_stream_socket = live_stream_client_socket_new(config);
    for (int i=0; i<num_databases; i++) {
        state->mongo_clients[i] = mongoc_client_new(databases[i]);
        assert(state->mongo_clients[i]);
    }
    state->request_collections = zhash_new();
    state->metrics_collections = zhash_new();
    state->jse_collections = zhash_new();
    state->events_collections = zhash_new();
    state->statsd_client = statsd_client_new(config, state->me);
    return state;
}

static
void request_writer_state_destroy(request_writer_state_t **state_p)
{
    request_writer_state_t *state = *state_p;
    // must not destroy the pipe, as it's owned by the actor
    zsock_destroy(&state->pull_socket);
    zsock_destroy(&state->live_stream_socket);
    zhash_destroy(&state->request_collections);
    zhash_destroy(&state->metrics_collections);
    zhash_destroy(&state->jse_collections);
    zhash_destroy(&state->events_collections);
    for (int i=0; i<num_databases; i++) {
        mongoc_client_destroy(state->mongo_clients[i]);
    }
    statsd_client_destroy(&state->statsd_client);
    free(state);
    *state_p = NULL;
}

static void request_writer(zsock_t *pipe, void *args)
{
    request_writer_state_t *state = (request_writer_state_t*)args;
    state->pipe = pipe;
    set_thread_name(state->me);
    size_t id = state->id;

    if (!quiet)
        printf("[I] writer [%zu]: starting\n", id);

    size_t ticks = 0;
    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    zpoller_t *poller = zpoller_new(state->pipe, state->pull_socket, NULL);
    assert(poller);

    while (!zsys_interrupted) {
        // printf("[D] writer [%zu]: polling\n", id);
        // we wait for at most one second
        void *socket = zpoller_wait(poller, 1000);
        zmsg_t *msg = NULL;
        if (socket == state->pipe) {
            msg = zmsg_recv(state->pipe);
            char *cmd = zmsg_popstr(msg);
            zmsg_destroy(&msg);
            if (streq(cmd, "tick")) {
                if (verbose && (state->updates_count || state->update_time))
                    printf("[I] writer [%zu]: tick (%d requests, %d ms)\n", id, state->updates_count, state->update_time/1000);
                statsd_client_count(state->statsd_client, "importer.inserts.count", state->updates_count);
                statsd_client_timing(state->statsd_client, "importer.inserts.time", state->update_time/1000);
                if (ticks++ % PING_INTERVAL == 0) {
                    // ping mongodb to reestablish connection if it got lost
                    for (int i=0; i<num_databases; i++) {
                        mongo_client_ping(state->mongo_clients[i]);
                    }
                }
                // free collection pointers every hour
                if (ticks % COLLECTION_REFRESH_INTERVAL == COLLECTION_REFRESH_INTERVAL - id - 1) {
                    printf("[I] writer [%zu]: freeing request collections\n", id);
                    zhash_destroy(&state->request_collections);
                    zhash_destroy(&state->jse_collections);
                    zhash_destroy(&state->events_collections);
                    state->request_collections = zhash_new();
                    state->jse_collections = zhash_new();
                    state->events_collections = zhash_new();
                }
                state->updates_count = 0;
                state->update_time = 0;
                free(cmd);
            } else if (streq(cmd, "$TERM")) {
                // printf("[D] writer [%zu]: received $TERM command\n", id);
                free(cmd);
                break;
            } else {
                printf("[E] writer [%zu]: received unknown command: %s\n", id, cmd);
                assert(false);
            }
        } else if (socket == state->pull_socket) {
            msg = zmsg_recv(state->pull_socket);
            if (msg != NULL) {
                int64_t start_time_us = zclock_usecs();
                handle_request_msg(msg, state);
                zmsg_destroy(&msg);
                __sync_sub_and_fetch(&queued_inserts, 1);
                int64_t end_time_us = zclock_usecs();
                state->updates_count++;
                state->update_time += end_time_us - start_time_us;
            }
        } else if (socket) {
            // if socket is not null, something is horribly broken
            printf("[E] writer [%zu]: broken poller. committing suicide.\n", id);
            assert(false);
        }
        else {
            // probably interrupted by signal handler
            // if so, loop will terminate on condition !zsys_interrupted
        }
    }

    if (!quiet)
        printf("[I] writer [%zu]: shutting down\n", id);

    request_writer_state_destroy(&state);

    if (!quiet)
        printf("[I] writer [%zu]: terminated\n", id);
}

zactor_t* request_writer_new(zconfig_t *config, size_t id)
{
    request_writer_state_t *state = request_writer_state_new(config, id);
    return zactor_new(request_writer, state);
}
