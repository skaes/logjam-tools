#include "logjam-streaminfo.h"
#include "device-tracker.h"
#include <pthread.h>

typedef struct {
    bool received_term_cmd;         // whether we have received a TERM command
} stream_updater_state_t;

// all configured streams
static zhash_t *configured_streams = NULL;
// all active stream names
static zlist_t *active_stream_names = NULL;
// all streams we want to subscribe to
static zlist_t *stream_subscriptions = NULL;
// lock around all stream access operations
static pthread_mutex_t lock;
// logjam url, to be used for retrieving stream information
static const char* streams_url = NULL;
// whether we subscribe to a subset of streams
static bool have_subscription_pattern;
// sbuscription pattern
static const char *subscription_pattern = NULL;
// httpp client
static zhttp_client_t *client = NULL;

// callback for creating streams
static stream_fn *create_stream_callback = NULL;
// callback for freeing streams
static stream_fn *free_stream_callback = NULL;

// one tick per second
static uint64_t ticks = 0;

void set_stream_create_fn(stream_fn *f)
{
    create_stream_callback = f;
}

void set_stream_free_fn(stream_fn *f)
{
    free_stream_callback = f;
}

stream_info_t* get_stream_info(const char* stream_name, zhash_t *thread_local_cache)
{
    stream_info_t *stream_info = NULL;
    if (thread_local_cache) {
        stream_info = zhash_lookup(thread_local_cache, stream_name);
        if (stream_info) {
            __atomic_fetch_add(&stream_info->ref_count, 1, __ATOMIC_SEQ_CST);
            return stream_info;
        }
    }
    pthread_mutex_lock(&lock);
    stream_info = zhash_lookup(configured_streams, stream_name);
    if (stream_info)
        __atomic_fetch_add(&stream_info->ref_count, 1, __ATOMIC_SEQ_CST);
    pthread_mutex_unlock(&lock);
    if (stream_info && thread_local_cache) {
        zhash_insert(thread_local_cache, stream_name, stream_info);
        zhash_freefn(thread_local_cache, stream_name, (zhash_free_fn*)release_stream_info);
        __atomic_fetch_add(&stream_info->ref_count, 1, __ATOMIC_SEQ_CST);
    }
    return stream_info;
}

const char* get_subscription_pattern()
{
    return subscription_pattern;
}

zlist_t* get_stream_subscriptions()
{
    pthread_mutex_lock(&lock);
    zlist_t *names = zlist_dup(stream_subscriptions);
    pthread_mutex_unlock(&lock);
    return names;
}

zlist_t* get_active_stream_names()
{
    pthread_mutex_lock(&lock);
    zlist_t *names = zlist_dup(active_stream_names);
    pthread_mutex_unlock(&lock);
    return names;
}

static
void add_module_import_threshold_settings(stream_info_t* info, json_object *stream_obj)
{
    json_object *import_thresholds;
    if (!json_object_object_get_ex(stream_obj, "import_thresholds", &import_thresholds)) {
        return;
    }
    if (json_object_get_type(import_thresholds) != json_type_array) {
        return;
    }
    int n = json_object_array_length(import_thresholds);
    if (n==0) {
        return;
    }
    info->module_thresholds = zmalloc(n * sizeof(module_threshold_t));
    int m = 0;
    for (int i = 0; i<n; i++) {
        json_object *threshold_pair = json_object_array_get_idx(import_thresholds, i);
        if (json_object_get_type(threshold_pair) != json_type_array) {
            continue;
        }
        if (json_object_array_length(threshold_pair) < 2) {
            continue;
        }
        json_object *name_obj = json_object_array_get_idx(threshold_pair, 0);
        json_object *value_obj = json_object_array_get_idx(threshold_pair, 1);
        info->module_thresholds[m].name = strdup(json_object_get_string(name_obj));
        info->module_thresholds[m].value = json_object_get_int(value_obj);
        m++;
    }
    info->module_threshold_count = m;
}

static inline size_t str_count(const char* str, char c)
{
    size_t count = 0;
    while (*str) if (*str++ == c) ++count;
    return count;
}

static
void add_backend_only_requests_settings(stream_info_t* info, const char* value)
{
    size_t len = strlen(value);
    if (streq(value, "*")) {
        info->all_requests_are_backend_only_requests = 1;
    } else if (len>0) {
        int n = str_count(value, ',') + 1;
        info->backend_only_requests_size = n;
        info->backend_only_requests = zmalloc(sizeof(char*)*n);
        char *valdup = strdup(value);
        char *prefix = strtok(valdup, ",");
        int i = 0;
        while (prefix) {
            info->backend_only_requests[i++] = strdup(prefix);
            prefix = strtok(NULL, ",");
        }
        free(valdup);
    }
}

static
void add_api_requests_settings(stream_info_t* info, json_object* obj)
{
    if (json_object_get_type(obj) != json_type_array)
        return;
    int len = json_object_array_length(obj);
    if (len == 0) {
        info->all_requests_are_api_requests = 0;
        return;
    }
    info->api_requests_size = len;
    info->api_requests = zmalloc(len * sizeof(char*));
    for (int i=0; i<len; i++) {
        json_object *prefix_obj = json_object_array_get_idx(obj, i);
        const char *prefix = json_object_get_string(prefix_obj);
        if (i==0 && streq(prefix, ""))
            info->all_requests_are_api_requests = 1;
        info->api_requests[i] = strdup(prefix);
    }
}

static
void add_stream_settings(stream_info_t *info, json_object *stream_obj)
{
    json_object *obj;
    if (json_object_object_get_ex(stream_obj, "import_threshold", &obj)) {
        info->import_threshold = json_object_get_int(obj);
    }
    if (json_object_object_get_ex(stream_obj, "import_thresholds", &obj)) {
        add_module_import_threshold_settings(info, stream_obj);
    }
    if (json_object_object_get_ex(stream_obj, "database_cleaning_threshold", &obj)) {
        info->database_cleaning_threshold = json_object_get_int(obj);
    }
    if (json_object_object_get_ex(stream_obj, "request_cleaning_threshold", &obj)) {
        info->request_cleaning_threshold = json_object_get_int(obj);
    }
    if (json_object_object_get_ex(stream_obj, "ignored_request_uri", &obj)) {
        info->ignored_request_prefix = strdup(json_object_get_string(obj));
    }
    if (json_object_object_get_ex(stream_obj, "backend_only_requests", &obj)) {
        add_backend_only_requests_settings(info, json_object_get_string(obj));
    }
    if (json_object_object_get_ex(stream_obj, "api_requests", &obj)) {
        add_api_requests_settings(info, obj);
    }
    if (json_object_object_get_ex(stream_obj, "sampling_rate_400s", &obj)) {
        info->sampling_rate_400s = json_object_get_double(obj);
        info->sampling_rate_400s_threshold = MAX_RANDOM_VALUE * info->sampling_rate_400s;
    }
    if (json_object_object_get_ex(stream_obj, "database_number", &obj)) {
        info->db = json_object_get_int(obj);
    }
    if (json_object_object_get_ex(stream_obj, "max_inserts_per_second", &obj)) {
        info->requests_inserted->cap = json_object_get_int(obj);
    } else {
        info->requests_inserted->cap = DEFAULT_MAX_INSERTS_PER_SECOND;
    }
}

static
stream_info_t* stream_info_new(const char* key, json_object *stream_obj)
{

    // printf("[D] stream-op: allocating stream %s\n", key);

    stream_info_t *info = zmalloc(sizeof(stream_info_t));
    assert(info);

    info->ref_count = 1;
    info->key = strdup(key);
    info->key_len = strlen(info->key);

    char app[256];
    char env[256];
    bool ok = extract_app_env(info->key, 256, app, env);
    if (!ok) {
        fprintf(stderr, "[E] ignored invalid stream: %s\n", info->key);
        free(info->key);
        free(info);
        return NULL;
    }
    info->app = strdup(app);
    info->app_len = strlen(app);
    assert(info->app_len > 0);

    info->env = strdup(env);
    info->env_len = strlen(env);
    assert(info->env_len > 0);

    char yek[info->key_len+1];
    snprintf(yek, info->key_len+1, "%s.%s", env, app);
    info->yek = strdup(yek);

    info->requests_inserted = zmalloc(sizeof(requests_inserted_t));
    info->free_requests_inserted = true;
    add_stream_settings(info, stream_obj);

    info->known_modules = zhash_new();
    assert(info->known_modules);

    return info;
}

void release_stream_info(stream_info_t *info)
{
    int32_t ref_count = __atomic_fetch_add(&info->ref_count, -1, __ATOMIC_SEQ_CST);
    // printf("[D] stream-op: decreased ref count for stream %s: %d\n", info->key, ref_count-1);
    if (ref_count > 1)
        return;

    // printf("[D] stream-op: freeing stream %s\n", info->key);
    free(info->key);
    free(info->yek);
    free(info->app);
    free(info->env);
    if (info->module_thresholds) {
        int n = info->module_threshold_count;
        for (int i=0; i<n; i++)
            free(info->module_thresholds[i].name);
        free(info->module_thresholds);
    }
    free(info->ignored_request_prefix);
    if (info->backend_only_requests) {
        int n = info->backend_only_requests_size;
        for (int i=0; i<n; i++)
            free(info->backend_only_requests[i]);
        free(info->backend_only_requests);
    }
    if (info->api_requests) {
        int n = info->api_requests_size;
        for (int i=0; i<n; i++)
            free(info->api_requests[i]);
        free(info->api_requests);
    }
    zhash_destroy(&info->known_modules);

    if (info->free_requests_inserted)
        free(info->requests_inserted);

    if (info->free_callback)
        info->free_callback(info);

    free(info);
}

static
void dump_stream_info(stream_info_t *stream)
{
    printf("[D] ====================\n");
    printf("[D] key: %s\n", stream->key);
    printf("[D] yek: %s\n", stream->yek);
    printf("[D] app: %s\n", stream->app);
    printf("[D] env: %s\n", stream->env);
    printf("[D] ignored_request_uri: %s\n", stream->ignored_request_prefix);
    printf("[D] database_cleaning_threshold: %d\n", stream->database_cleaning_threshold);
    printf("[D] request_cleaning_threshold: %d\n", stream->request_cleaning_threshold);
    printf("[D] import_threshold: %d\n", stream->import_threshold);
    for (int i = 0; i<stream->module_threshold_count; i++) {
        printf("[D] module_import_threshold: %s = %zu\n", stream->module_thresholds[i].name, stream->module_thresholds[i].value);
    }
    printf("[D] all requests are backend only requests: %d\n", stream->all_requests_are_backend_only_requests);
    int n = stream->backend_only_requests_size;
    printf("[D] backend only requests size: %d\n", n);
    if (n > 0) {
        printf("[D] backend only requests: ");
        printf("%s", stream->backend_only_requests[0]);
        for (int i=1; i<n; i++)
            printf(",%s", stream->backend_only_requests[i]);
        printf("\n");
    }
    n = stream->api_requests_size;
    printf("[D] api requests size: %d\n", n);
    if (n > 0) {
        printf("[D] api requests: ");
        printf("%s", stream->api_requests[0]);
        for (int i=1; i<n; i++)
            printf(",%s", stream->api_requests[i]);
        printf("\n");
    }
}

static
zhash_t* get_streams()
{
    zhash_t *streams = NULL;

    zhttp_request_t *request = zhttp_request_new();
    zhttp_response_t *response = NULL;
    zhttp_request_set_url(request, streams_url);
    zhttp_request_set_method(request, "GET");
    zhash_t *headers = zhttp_request_headers(request);
    zhash_insert (headers, "Accept", "application/json");

    int rc = zhttp_request_send(request, client, 10000, NULL, NULL);
    if (rc) goto cleanup;

    void *user_arg1, *user_arg2;
    response = zhttp_response_new ();
    rc = zhttp_response_recv(response, client, &user_arg1, &user_arg2);
    if (rc) goto cleanup;

    const char* body = zhttp_response_content(response);
    const int body_len = zhttp_response_content_length(response);

    json_tokener* tokener = json_tokener_new();
    json_object *streams_obj = parse_json_data(body, body_len, tokener);
    json_tokener_free(tokener);
    if (streams_obj == NULL) goto cleanup;

    streams = zhash_new();
    json_object_object_foreach(streams_obj, key, val) {
        stream_info_t *stream = stream_info_new(key, val);
        if (stream) {
            if (0) dump_stream_info(stream);
            zhash_insert(streams, key, stream);
            zhash_freefn(streams, key, (zhash_free_fn*)release_stream_info);
        }
    }
    json_object_put(streams_obj);

 cleanup:
    zhttp_request_destroy(&request);
    zhttp_response_destroy(&response);
    return streams;
}

static
bool update_stream_config()
{
    printf("[I] stream-updater: updating stream config\n");

    zhash_t *new_streams = get_streams();
    if (new_streams == NULL) {
        fprintf(stderr, "[E] stream-updater: could not retrieve streams from logjam instance\n");
        return false;
    }

    zlist_t *new_subscriptions = zlist_new();
    zlist_autofree(new_subscriptions);
    zlist_comparefn(new_subscriptions, (zlist_compare_fn *) strcmp);
    zlist_t *new_active_streams = zlist_new();
    zlist_autofree(new_active_streams);
    zlist_comparefn(new_active_streams, (zlist_compare_fn *) strcmp);

    stream_info_t *info = zhash_first(new_streams);
    while (info) {
        const char *key = zhash_cursor(new_streams);
        if (!have_subscription_pattern) {
            zlist_append(new_active_streams, (void*)key);
        } else
            if (strstr(key, subscription_pattern) != NULL) {
                zlist_append(new_subscriptions, (void*)key);
                zlist_append(new_active_streams, (void*)key);
            }
        info = zhash_next(new_streams);
    }

    pthread_mutex_lock(&lock);
    info = zhash_first(new_streams);
    while (info) {
        info->free_callback = free_stream_callback;
        stream_info_t *old_info = configured_streams ? zhash_lookup(configured_streams, info->key) : NULL;
        if (old_info) {
            // stream already existed
            info->inserts_total = old_info->inserts_total;
            int64_t new_cap = info->requests_inserted->cap;
            __atomic_store_n(&old_info->requests_inserted->cap, new_cap, __ATOMIC_SEQ_CST);
            free(info->requests_inserted);
            old_info->free_requests_inserted = false;
            info->requests_inserted = old_info->requests_inserted;
            old_info->free_callback = NULL;
        } else if (create_stream_callback) {
            // create inserts_total counter for new stream
            create_stream_callback(info);
        }
        info = zhash_next(new_streams);
    }
    zhash_destroy(&configured_streams);
    zlist_destroy(&stream_subscriptions);
    zlist_destroy(&active_stream_names);
    configured_streams = new_streams;
    stream_subscriptions = new_subscriptions;
    active_stream_names = new_active_streams;
    pthread_mutex_unlock(&lock);

    printf("[I] stream-updater: updated stream config\n");

    return true;
}

bool setup_stream_config(const char* logjam_url, const char *pattern)
{
    streams_url = logjam_url;
    subscription_pattern = pattern;
    have_subscription_pattern = strcmp("", pattern);
    if (have_subscription_pattern)
        log_gaps = false;

    int rc = pthread_mutex_init(&lock, NULL);
    assert(rc == 0);

    client = zhttp_client_new(debug);
    assert(client);
    if (update_stream_config()) {
        return true;
    }
    zhttp_client_destroy(&client);
    return false;
}

#define ONE_DAY_MS (1000 * 60 * 60 * 24)

void update_known_modules(stream_info_t *stream_info, zhash_t* module_hash)
{
    uint64_t now = zclock_time();
    uint64_t age_threshold = now - ONE_DAY_MS;
    zhash_t *known_modules = stream_info->known_modules;

    // update timestamps for modules just seen
    void *elem = zhash_first(module_hash);
    while (elem) {
        const char *module = zhash_cursor(module_hash);
        zhash_update(known_modules, module, (void*)now);
        elem = zhash_next(module_hash);
    }

    // delete modules we haven't heard from for over a day
    zlist_t* modules = zhash_keys(known_modules);
    const char* module = zlist_first(modules);
    while (module) {
        uint64_t last_seen = (uint64_t)zhash_lookup(known_modules, module);
        if (last_seen < age_threshold) {
            zhash_delete(known_modules, module);
        }
        module = zlist_next(modules);
    }

    // update all_pages, unless no module is left
    if (zhash_size(known_modules) > 0)
        zhash_update(known_modules, "all_pages", (void*)now);

    zlist_destroy(&modules);
}

const char* throttling_reason_str(throttling_reason_t reason)
{
    switch (reason) {
    case NOT_THROTTLED:
        return "not throttled";
    case THROTTLE_MAX_INSERTS_PER_SECOND:
        return "max inserts reached";
    case THROTTLE_SOFT_LIMIT_STORAGE_SIZE:
        return "disk soft limit reached";
    case THROTTLE_HARD_LIMIT_STORAGE_SIZE:
        return "disk hard limit reached";
    default:
        return "unknown throttling reason";
    }
}

bool throttle_request_for_stream(stream_info_t *stream_info)
{
    int64_t current = __atomic_add_fetch(&stream_info->requests_inserted->current, 1, __ATOMIC_SEQ_CST);
    int64_t cap = __atomic_load_n(&stream_info->requests_inserted->cap, __ATOMIC_SEQ_CST);
    // printf("[D] throttle_request_for_stream %s(%p): cap: %" PRIi64 ", inserted: %" PRIi64 "\n", stream_info->key, stream_info, cap, current);
    return current > cap;
}

static void reset_request_counters()
{
    zlist_t* stream_names = get_active_stream_names();
    const char* stream_name = zlist_first(stream_names);
    while (stream_name) {
        stream_info_t* stream_info = get_stream_info(stream_name, NULL);
        int64_t current = __atomic_exchange_n(&stream_info->requests_inserted->current, 0, __ATOMIC_SEQ_CST);
        if (verbose) {
            int64_t cap = __atomic_load_n(&stream_info->requests_inserted->cap, __ATOMIC_SEQ_CST);
            if (current > cap)
                printf("[I] stream-updater: %s(%p): inserted %" PRIi64 ", throttled: %" PRIi64 "\n", stream_name, stream_info, cap, current - cap);
            else
                printf("[D] stream-updater: %s(%p): inserted %" PRIi64 ", capacity: %" PRIi64 "\n", stream_name, stream_info, current, cap);
        }
        release_stream_info(stream_info);
        stream_name = zlist_next(stream_names);
    }
    zlist_destroy(&stream_names);
}

static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    update_stream_config();
    return 0;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *arg)
{
    int rc = 0;
    stream_updater_state_t *state = arg;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            state->received_term_cmd = true;
            // fprintf(stderr, "[D] stream-updater: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            if (verbose)
                printf("[I] stream-updater: tick\n");
            ticks++;
            // printf("[D] stream-updater: resetting request counters\n");
            reset_request_counters();
        } else {
            fprintf(stderr, "[E] stream-updater: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}


void stream_config_updater(zsock_t *pipe, void *args)
{
    set_thread_name("stream-updater");

    int rc;
    stream_updater_state_t state = { .received_term_cmd = false };

    // signal readyiness
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);
    // we rely on the controller shutting us down
    zloop_ignore_interrupts(loop);

    // setup handler for actor messages
    rc = zloop_reader(loop, pipe, actor_command, &state);
    assert(rc == 0);

    // rfresh stream config every 60 seconds
    rc = zloop_timer(loop, 60000, 0, timer_event, &state);
    assert(rc != -1);

    // run the loop
    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        if (!state.received_term_cmd)
            log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        printf("[I] stream-updater: shutting down\n");

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);
    zhttp_client_destroy(&client);

    if (!quiet)
        printf("[I] stream-updater: terminated\n");
}

void adjust_caller_info(const char* path, const char* module, json_object *request, stream_info_t *stream_info)
{
    // check whether we have a HTTP request
    if (path == NULL)
        return;
    // check whether app has no api requests at all
    if (!stream_info->all_requests_are_api_requests && stream_info->api_requests_size == 0)
        return;
    // check whether we have an api request
    while (*module == ':') module++;
    bool api_request = stream_info->all_requests_are_api_requests;
    if (!api_request) {
        for (int i = 0; i < stream_info->api_requests_size; i++) {
            if (streq(module, stream_info->api_requests[i])) {
                api_request = true;
                break;
            }
        }
        if (!api_request)
            return;
    }
    // set caller_id if not present
    bool dump = false;
    json_object *caller_id_obj;
    if (!json_object_object_get_ex(request, "caller_id", &caller_id_obj)) {
        char rid[256] = {'\0'};
        sprintf(rid, "unknown-%s-unknown", stream_info->env);
        json_object_object_add(request, "caller_id", json_object_new_string(rid));
        if (debug) {
            printf("[D] added missing caller id for %s\n", stream_info->key);
            dump = true;
        }
    } else {
        const char *caller_id = json_object_get_string(caller_id_obj);
        if (caller_id == NULL || *caller_id == '\0') {
            char rid[256] = {'\0'};
            sprintf(rid, "unknown-%s-unknown", stream_info->env);
            json_object_object_add(request, "caller_id", json_object_new_string(rid));
            if (debug) {
                printf("[D] added missing caller id for %s\n", stream_info->key);
                dump = true;
            }
        }
    }
    // set caller_action if not present
    json_object *caller_action_obj;
    if (!json_object_object_get_ex(request, "caller_action", &caller_action_obj)) {
        json_object_object_add(request, "caller_action", json_object_new_string("Unknown#unknown"));
        if (debug) {
            printf("[D] added missing caller action for %s\n", stream_info->key);
            dump = true;
        }
    } else {
        const char *caller_action = json_object_get_string(caller_action_obj);
        if (caller_action == NULL || *caller_action == '\0') {
            json_object_object_add(request, "caller_action", json_object_new_string("Unknown#unknown"));
            if (debug) {
                printf("[D] added missing caller action for %s\n", stream_info->key);
                dump = true;
            }
        }
    }
    if (dump)
        dump_json_object(stderr, "[D] modified caller info", request);
}
