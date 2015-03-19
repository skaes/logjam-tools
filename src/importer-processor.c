#include "importer-common.h"
#include "importer-increments.h"
#include "importer-parser.h"
#include "importer-processor.h"
#include "importer-livestream.h"
#include "importer-streaminfo.h"
#include "importer-resources.h"

#define DB_PREFIX "logjam-"
#define DB_PREFIX_LEN 7

processor_state_t* processor_new(char *db_name)
{
    // printf("[D] creating processor for db: %s\n", db_name);
    // check whether it's a known stream and return NULL if not
    size_t n = strlen(db_name) - DB_PREFIX_LEN;
    char stream_name[n+1];
    strcpy(stream_name, db_name + DB_PREFIX_LEN);
    stream_name[n-11] = '\0';

    stream_info_t *stream_info = zhash_lookup(configured_streams, stream_name);
    if (stream_info == NULL) {
        fprintf(stderr, "[E] did not find stream info: %s\n", stream_name);
        return NULL;
    }
    // printf("[D] found stream info for stream %s: %s\n", stream_name, stream_info->key);

    processor_state_t *p = zmalloc(sizeof(processor_state_t));
    p->db_name = strdup(db_name);
    p->stream_info = stream_info;
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
    processor_state_t* p = processor;
    // printf("[D] destroying processor: %s. requests: %zu\n", p->db_name, p->request_count);
    free(p->db_name);
    zhash_destroy(&p->modules);
    zhash_destroy(&p->totals);
    zhash_destroy(&p->minutes);
    zhash_destroy(&p->quants);
    free(p);
}


static
void dump_modules_hash(zhash_t *modules)
{
    const char* module = zhash_first(modules);
    while (module) {
        printf("[D] module: %s\n", module);
        module = zhash_next(modules);
    }
}

static
void dump_increments_hash(zhash_t *increments_hash)
{
    increments_t *increments = zhash_first(increments_hash);
    while (increments) {
        const char *action = zhash_cursor(increments_hash);
        dump_increments(action, increments);
        increments = zhash_next(increments_hash);
    }
}

static
void processor_dump_state(processor_state_t *self)
{
    puts("[D] ================================================");
    printf("[D] db_name: %s\n", self->db_name);
    printf("[D] processed requests: %zu\n", self->request_count);
    dump_modules_hash(self->modules);
    dump_increments_hash(self->totals);
    dump_increments_hash(self->minutes);
}


static
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

static
const char* processor_setup_page(processor_state_t *self, json_object *request)
{
    json_object *page_obj = NULL;
    if (json_object_object_get_ex(request, "action", &page_obj)) {
        json_object_get(page_obj);
        json_object_object_del(request, "action");
    } else if (json_object_object_get_ex(request, "logjam_action", &page_obj)) {
        json_object_get(page_obj);
        json_object_object_del(request, "logjam_action");
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

static
const char* processor_setup_module(processor_state_t *self, const char *page)
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
    // printf("[D] page: %s\n", page);
    // printf("[D] module: %s\n", module);
    return module;
}

static
int processor_setup_response_code(processor_state_t *self, json_object *request)
{
    json_object *code_obj = NULL;
    int response_code = 500;
    if (json_object_object_get_ex(request, "code", &code_obj)) {
        response_code = json_object_get_int(code_obj);
        json_object_object_del(request, "code");
    }
    json_object_object_add(request, "response_code", json_object_new_int(response_code));
    // printf("[D] response_code: %d\n", response_code);
    return response_code;
}

static
double processor_setup_time(processor_state_t *self, json_object *request, const char *time_name, const char *duplicate)
{
    // TODO: might be better to drop requests without total_time
    double total_time;
    json_object *total_time_obj = NULL;
    if (json_object_object_get_ex(request, time_name, &total_time_obj)) {
        total_time = json_object_get_double(total_time_obj);
        if (total_time == 0.0) {
            total_time = 1.0;
            total_time_obj = json_object_new_double(total_time);
            json_object_object_add(request, time_name, total_time_obj);
        }
    } else {
        total_time = 1.0;
        total_time_obj = json_object_new_double(total_time);
        json_object_object_add(request, time_name, total_time_obj);
    }
    // printf("[D] %s: %f\n", time_name, total_time);
    if (duplicate) {
        // TODO: check whether we could simply share the object
        json_object_object_add(request, duplicate, json_object_new_double(total_time));
    }
    return total_time;
}

static
int extract_severity_from_lines_object(json_object* lines)
{
    int log_level = -1;
    if (lines != NULL && json_object_get_type(lines) == json_type_array) {
        int array_len = json_object_array_length(lines);
        for (int i=0; i<array_len; i++) {
            json_object *line = json_object_array_get_idx(lines, i);
            if (line && json_object_get_type(line) == json_type_array) {
                json_object *level = json_object_array_get_idx(line, 0);
                if (level) {
                    int new_level = json_object_get_int(level);
                    if (new_level > log_level) {
                        log_level = new_level;
                    }
                }
            }
        }
    }
    // protect against unknown log levels
    return (log_level > 5) ? -1 : log_level;
}

static
int processor_setup_severity(processor_state_t *self, json_object *request)
{
    int severity = 1;
    json_object *severity_obj;
    if (json_object_object_get_ex(request, "severity", &severity_obj)) {
        severity = json_object_get_int(severity_obj);
    } else {
        json_object *lines_obj;
        if (json_object_object_get_ex(request, "lines", &lines_obj)) {
            int extracted_severity = extract_severity_from_lines_object(lines_obj);
            if (extracted_severity != -1) {
                severity = extracted_severity;
            }
        }
        severity_obj = json_object_new_int(severity);
        json_object_object_add(request, "severity", severity_obj);
    }
    return severity;
    // printf("[D] severity: %d\n\n", severity);
}

static
int processor_setup_minute(processor_state_t *self, json_object *request)
{
    // we know that started_at data is valid since we already checked that
    // when determining which processor to call
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
    // printf("[D] minute: %d\n", minute);
    return minute;
}

static
void processor_setup_other_time(processor_state_t *self, json_object *request, double total_time)
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
    // printf("[D] other_time: %f\n", other_time);
}

static
void processor_setup_allocated_memory(processor_state_t *self, json_object *request)
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
        // printf("[D] allocated memory: %lu\n", allocated_memory);
    }
}

static
int processor_setup_heap_growth(processor_state_t *self, json_object *request)
{
    json_object *heap_growth_obj = NULL;
    int heap_growth = 0;
    if (json_object_object_get_ex(request, "heap_growth", &heap_growth_obj)) {
        heap_growth = json_object_get_int(heap_growth_obj);
    }
    // printf("[D] heap_growth: %d\n", heap_growth);
    return heap_growth;
}

static
json_object* processor_setup_exceptions(processor_state_t *self, json_object *request)
{
    json_object* exceptions;
    if (json_object_object_get_ex(request, "exceptions", &exceptions)) {
        int num_ex = json_object_array_length(exceptions);
        if (num_ex == 0) {
            json_object_object_del(request, "exceptions");
            return NULL;
        }
    }
    return exceptions;
}

static
void processor_add_totals(processor_state_t *self, const char* namespace, increments_t *increments)
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

static
void processor_add_minutes(processor_state_t *self, const char* namespace, size_t minute, increments_t *increments)
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

#define QUANTS_ARRAY_SIZE (sizeof(size_t) * (last_resource_offset + 1))

static
void add_quant(const char* namespace, size_t resource_idx, char kind, size_t quant, zhash_t* quants)
{
    char key[2000];
    sprintf(key, "%c-%zu-%s", kind, quant, namespace);
    // printf("[D] QUANT-KEY: %s\n", key);
    size_t *stored = zhash_lookup(quants, key);
    if (stored == NULL) {
        stored = zmalloc(QUANTS_ARRAY_SIZE);
        zhash_insert(quants, key, stored);
        zhash_freefn(quants, key, free);
    }
    stored[resource_idx]++;
}

static
void processor_add_quants(processor_state_t *self, const char* namespace, increments_t *increments)
{
    for (int i=0; i<=last_resource_offset; i++){
        double val = increments->metrics[i].val;
        if (val > 0) {
            char kind;
            double d;
            // printf("[D] trying to add quant: %d=%s\n", i, i2r(i));
            if (i <= last_time_resource_offset) {
                kind = 't';
                d = 100.0;
            } else if (i == allocated_objects_index) {
                kind = 'm';
                d = 10000.0;
            } else if (i == allocated_bytes_index) {
                kind = 'm';
                d = 100000.0;
            } else if ((i > last_heap_resource_offset) && (i <= last_frontend_resource_offset)) {
                kind = 'f';
                d = 100.0;
            } else {
                // printf("[D] skipping quant: %s\n", i2r(i));
                continue;
            }
            size_t x = (ceil(floor(val/d))+1) * d;
            add_quant(namespace, i, kind, x, self->quants);
            add_quant("all_pages", i, kind, x, self->quants);
        }
    }
}

static
bool interesting_request(request_data_t *request_data, json_object *request, stream_info_t* info)
{
    int time_threshold = info ? info->import_threshold : global_total_time_import_threshold;
    if (request_data->total_time > time_threshold)
        return true;
    if (request_data->severity > 1)
        return true;
    if (request_data->response_code >= 400)
        return true;
    if (request_data->exceptions != NULL)
        return true;
    if (request_data->heap_growth > 0)
        return true;
    if (info == NULL)
        return false;
    for (int i=0; i<info->module_threshold_count; i++) {
        if (!strcmp(request_data->module+2, info->module_thresholds[i].name)) {
            if (request_data->total_time > info->module_thresholds[i].value) {
                // printf("[D] INTERESTING: %s: %f\n", request_data->module+2, request_data->total_time);
                return true;
            } else
                return false;
        }
    }
    return false;
}

static
int ignore_request(json_object *request, stream_info_t* info)
{
    int rc = 0;
    json_object *req_info;
    if (json_object_object_get_ex(request, "request_info", &req_info)) {
        json_object *url_obj;
        if (json_object_object_get_ex(req_info, "url", &url_obj)) {
            const char *url = json_object_get_string(url_obj);
            const char *prefix = info ? info->ignored_request_prefix : global_ignored_request_prefix;
            if (prefix != NULL && strstr(url, prefix) == url) {
                rc = 1;
            }
        }
    }
    return rc;
}

static int
backend_only_request(const char *action, stream_info_t *stream)
{
    assert(action);

    if (stream->all_requests_are_backend_only_requests)
        return 1;

    if (stream->backend_only_requests_size == 0)
        return 0;

    int n = stream->backend_only_requests_size;
    for (int i = 0; i < n; i++)
        if (strstr(action, stream->backend_only_requests[i]) == action)
            return 1;

    return 0;
}

void processor_add_request(processor_state_t *self, parser_state_t *pstate, json_object *request)
{
    if (ignore_request(request, self->stream_info)) return;

    // dump_json_object(stdout, request);
    request_data_t request_data;
    request_data.page = processor_setup_page(self, request);
    request_data.module = processor_setup_module(self, request_data.page);
    request_data.response_code = processor_setup_response_code(self, request);
    request_data.severity = processor_setup_severity(self, request);
    request_data.minute = processor_setup_minute(self, request);
    request_data.total_time = processor_setup_time(self, request, "total_time", NULL);

    request_data.exceptions = processor_setup_exceptions(self, request);
    processor_setup_other_time(self, request, request_data.total_time);
    processor_setup_allocated_memory(self, request);
    request_data.heap_growth = processor_setup_heap_growth(self, request);

    increments_t* increments = increments_new();
    increments->backend_request_count = 1;
    increments_fill_metrics(increments, request);
    increments_fill_apdex(increments, request_data.total_time);
    increments_fill_response_code(increments, &request_data);
    increments_fill_severity(increments, &request_data);
    increments_fill_caller_info(increments, request);
    increments_fill_exceptions(increments, request_data.exceptions);

    processor_add_totals(self, request_data.page, increments);
    processor_add_totals(self, request_data.module, increments);
    processor_add_totals(self, "all_pages", increments);

    processor_add_minutes(self, request_data.page, request_data.minute, increments);
    processor_add_minutes(self, request_data.module, request_data.minute, increments);
    processor_add_minutes(self, "all_pages", request_data.minute, increments);

    processor_add_quants(self, request_data.page, increments);

    increments_destroy(increments);

    if (!backend_only_request(request_data.page, self->stream_info)) {
        json_object *request_id_obj;
        if (json_object_object_get_ex(request, "request_id", &request_id_obj)) {
            const char *uuid = json_object_get_string(request_id_obj);
            if (uuid) {
                char app_env_uuid[1024] = {0};
                snprintf(app_env_uuid, 1024, "%s-%s", self->stream_info->key, uuid);
                tracker_add_uuid(pstate->tracker, app_env_uuid);
            }
        }
    } else {
        // printf("[D] ignored tracking for backend only request: %s\n", request_data.page);
    }

    if (0) {
        dump_json_object(stdout, request);
        if (self->request_count % 100 == 0) {
            processor_dump_state(self);
        }
    }

    if (interesting_request(&request_data, request, self->stream_info)) {
        json_object_get(request);
        zmsg_t *msg = zmsg_new();
        zmsg_addstr(msg, self->db_name);
        zmsg_addstr(msg, "r");
        zmsg_addstr(msg, request_data.module);
        zmsg_addptr(msg, request);
        zmsg_addptr(msg, self->stream_info);
        if (!output_socket_ready(pstate->push_socket, 0)) {
            fprintf(stderr, "[W] parser [%zu]: push socket not ready\n", pstate->id);
        }
        zmsg_send(&msg, pstate->push_socket);
    }
}

static
char* extract_page_for_jse(json_object *request)
{
    json_object *page_obj = NULL;
    if (json_object_object_get_ex(request, "logjam_action", &page_obj)) {
        page_obj = json_object_new_string(json_object_get_string(page_obj));
    } else {
        page_obj = json_object_new_string("Unknown#unknown_method");
    }

    const char *page_str = json_object_get_string(page_obj);

    if (!strchr(page_str, '#'))
        page_str = append_to_json_string(&page_obj, page_str, "#unknown_method");
    else if (page_str[strlen(page_str)-1] == '#')
        page_str = append_to_json_string(&page_obj, page_str, "unknown_method");

    char *page = strdup(page_str);
    json_object_put(page_obj);
    return page;
}

static
char* exctract_key_from_jse_description(json_object *request)
{
    json_object *description_obj = NULL;
    const char *description;
    if (json_object_object_get_ex(request, "description", &description_obj)) {
        description = json_object_get_string(description_obj);
    } else {
        description = "unknown_exception";
    }
    char *result = strdup(description);
    return result;
}

void processor_add_js_exception(processor_state_t *self, parser_state_t *pstate, json_object *request)
{
    char *page = extract_page_for_jse(request);
    char *js_exception = exctract_key_from_jse_description(request);

    if (strlen(js_exception) == 0) {
        fprintf(stderr, "[E] could not extract js_exception from request. ignoring.\n");
        dump_json_object(stderr, request);
        free(page);
        free(js_exception);
        return;
    }

    int minute = processor_setup_minute(self, request);
    const char *module = processor_setup_module(self, page);

    increments_t* increments = increments_new();
    increments_fill_js_exception(increments, js_exception);

    processor_add_totals(self, "all_pages", increments);
    processor_add_minutes(self, "all_pages", minute, increments);

    if (strstr(page, "#unknown_method") == NULL) {
        processor_add_totals(self, page, increments);
        processor_add_minutes(self, page, minute, increments);
    }

    if (strcmp(module, "Unknown") != 0) {
        processor_add_totals(self, module, increments);
        processor_add_minutes(self, module, minute, increments);
    }

    increments_destroy(increments);
    free(page);
    free(js_exception);

    json_object_get(request);
    zmsg_t *msg = zmsg_new();
    zmsg_addstr(msg, self->db_name);
    zmsg_addstr(msg, "j");
    zmsg_addstr(msg, module);
    zmsg_addptr(msg, request);
    zmsg_addptr(msg, self->stream_info);
    zmsg_send(&msg, pstate->push_socket);
}

void processor_add_event(processor_state_t *self, parser_state_t *pstate, json_object *request)
{
    processor_setup_minute(self, request);
    json_object_get(request);
    zmsg_t *msg = zmsg_new();
    zmsg_addstr(msg, self->db_name);
    zmsg_addstr(msg, "e");
    zmsg_addstr(msg, "");
    zmsg_addptr(msg, request);
    zmsg_addptr(msg, self->stream_info);
    zmsg_send(&msg, pstate->push_socket);
}

static
bool sorted_ascending(int64_t *a, int n)
{
    for (int i=1; i<n; i++) {
        if (a[i] < a[i-1])
            return false;
    }
    return true;
}

// correct negative values at the beginning of timestamps series
// often, request_start and response_start are equal and negative
// and everything before it is zero

static
void auto_correct_prefix(int64_t *a, int n)
{
    for (int i=0; i<n; i++) {
        if (a[i]<0)
            a[i] = 0;
        else if (a[i]>0)
            break;
    }
}

static
void make_relative(int64_t *a, int n, int j)
{
    int64_t start = a[j];
    for (int i=0; i<n; i++) {
        if (a[i] > 0)
            a[i] -= start;
    }
}

static
int extract_frontend_timings(json_object *request, int64_t *timings, int num_timings, const char *type)
{
    json_object *rts_object = NULL;
    const char *rts = NULL;
    if (json_object_object_get_ex(request, "rts", &rts_object)) {
        rts = json_object_get_string(rts_object);
        // fprintf(stdout, "[D] RTS: %s\n", rts);
    }
    if (!rts) {
        fprintf(stderr, "[E] processor: dropped %s request without timing information\n", type);
        return 0;
    }
    int n = 0;
    const char *p = rts;
    char c;
    int64_t *times = timings;
    int64_t value = 0;
    while (1) {
        c = *p++;
        if (c==',' || c==0) {
            times[n] = value;
            value = 0;
            n++;
            if (n == num_timings && c!=0) {
                fprintf(stderr, "[E] processor: too many frontend timing values: %s\n", rts);
                return 0;
            } else if (!c)
                break;
        } else {
            int x = c - '0';
            if (x < 0 || x > 9) {
                fprintf(stderr, "[E] processor: invalid character in frontend timing information: %s\n", p-1);
                return 0;
            } else {
                value = value * 10 + x;
            }
        }
    }
    if (n < num_timings) {
        fprintf(stderr, "[E] processor: not enough timings: expected %d, got %d\n", num_timings, n);
        return 0;
    } else {
        // for (int i=0; i<num_timings; i++) {
        //     printf("[D] processor: time[%d]=%" PRIi64 "\n", i, timings[i]);
        // }
        return 1;
    }
}

#define NUM_TIMINGS 16
#define navigationStart 0
// #define unloadEventStart -1
// #define unloadEventEnd -1
// #define redirectStart -1
// #define redirectEnd -1
#define fetchStart 1
#define domainLookupStart 2
#define domainLookupEnd 3
#define connectStart 4
#define connectEnd 5
// #define secureConnectionStart -1
#define requestStart 6
#define responseStart 7
#define responseEnd 8
#define domLoading 9
#define domInteractive 10
#define domContentLoadedEventStart 11
#define domContentLoadedEventEnd 12
#define domComplete 13
#define loadEventStart 14
#define loadEventEnd 15


static
int convert_frontend_timings_to_json(json_object *request, int64_t *timings, int64_t  *mtimes, int64_t* dom_interactive)
{
    make_relative(timings, NUM_TIMINGS, fetchStart);
    auto_correct_prefix(timings, NUM_TIMINGS);

    int64_t utimes[] = {
        timings[requestStart],
        timings[responseStart],
        timings[responseEnd],
        timings[domComplete],
        timings[loadEventEnd]
    };

    if (utimes[0] < 0 || !sorted_ascending(utimes, 5) ) {
        fprintf(stderr,
                "[W] processor: dropped frontend request due to invalid timings: "
                "%" PRIi64 ", %" PRIi64 ", %" PRIi64 ", %" PRIi64 ", %" PRIi64 "\n",
                utimes[0], utimes[1], utimes[2], utimes[3], utimes[4]);
        return 0;
    }

    int64_t connect_time    = mtimes[0] = timings[requestStart];
    int64_t request_time    = mtimes[1] = timings[responseStart] - timings[requestStart];
    int64_t response_time   = mtimes[2] = timings[responseEnd] - timings[responseStart];
    int64_t processing_time = mtimes[3] = timings[domComplete] - timings[responseEnd];
    int64_t load_time       = mtimes[4] = timings[loadEventEnd] - timings[domComplete];
    int64_t page_time       = mtimes[5] = timings[loadEventEnd];
    *dom_interactive        = timings[domInteractive] - connect_time;
    if (*dom_interactive < 0)
        *dom_interactive = page_time;

    json_object_object_add(request, "connect_time", json_object_new_int64(connect_time));
    json_object_object_add(request, "request_time", json_object_new_int64(request_time));
    json_object_object_add(request, "response_time", json_object_new_int64(response_time));
    json_object_object_add(request, "processing_time", json_object_new_int64(processing_time));
    json_object_object_add(request, "load_time", json_object_new_int64(load_time));
    json_object_object_add(request, "page_time", json_object_new_int64(page_time));

    // fprintf(stderr, "[D] SPEEDUP %.2lld \n", timings[domComplete] - timings[domInteractive]);
    // dump_json_object(stdout, request);

    return 1;
}

static
int check_frontend_request_validity(parser_state_t *pstate, json_object *request, const char* type, zmsg_t* msg)
{
    json_object *request_id_obj;
    const char *uuid = NULL;
    if (json_object_object_get_ex(request, "logjam_request_id", &request_id_obj)
        || json_object_object_get_ex(request, "request_id", &request_id_obj)) {
        uuid = json_object_get_string(request_id_obj);
    }
    if (!uuid) {
        fprintf(stderr, "[E] processor: dropped %s request without request_id\n", type);
        dump_json_object(stderr, request);
        return 0;
    }
    if (tracker_delete_uuid(pstate->tracker, uuid, msg, type)) {
        // fprintf(stderr, "[D] processor: tracker found %s request with request_id: %s\n", uuid, type);
        // dump_json_object(stdout, request);
        return 1;
    } else {
        // fprintf(stderr, "[D] processor: tracker could process %s request: request_id %s\n", type, uuid);
        // dump_json_object(stderr, request);
        return 0;
    }
}


void processor_add_frontend_data(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t* msg)
{
    // dump_json_object(stderr, request);
    // if (self->request_count % 100 == 0) {
    //      processor_dump_state(self);
    // }

    int64_t timings[16];
    if (!extract_frontend_timings(request, timings, 16, "frontend")) {
        fprintf(stderr, "[E] processor: dropped request with invalid timing information\n");
        return;
    }
    if (!check_frontend_request_validity(pstate, request, "frontend", msg))
        return;

    int64_t mtimes[6];
    int64_t dom_interactive;
    if (!convert_frontend_timings_to_json(request, timings, mtimes, &dom_interactive))
        return;

    request_data_t request_data;
    request_data.page = processor_setup_page(self, request);
    request_data.module = processor_setup_module(self, request_data.page);
    request_data.minute = processor_setup_minute(self, request);
    request_data.total_time = processor_setup_time(self, request, "page_time", "frontend_time");

    // TODO: revisit when switching to percentiles
    if (request_data.total_time > 300000) {
        fprintf(stderr, "[W] dropped request data with nonsensical page_time\n");
        dump_json_object(stderr, request);
        return;
    }

    increments_t* increments = increments_new();
    increments->page_request_count = 1;
    increments_fill_metrics(increments, request);
    increments_fill_frontend_apdex(increments, request_data.total_time);
    const char* satisfaction = increments_fill_page_apdex(increments, dom_interactive);

    processor_add_totals(self, request_data.page, increments);
    processor_add_totals(self, request_data.module, increments);
    processor_add_totals(self, "all_pages", increments);

    processor_add_minutes(self, request_data.page, request_data.minute, increments);
    processor_add_minutes(self, request_data.module, request_data.minute, increments);
    processor_add_minutes(self, "all_pages", request_data.minute, increments);

    processor_add_quants(self, request_data.page, increments);

    // send statsd updates
    const char* envapp = self->stream_info->yek;
    char buffer[1024];
    size_t n = sizeof(buffer);
    snprintf(buffer, n, "%s.page.%s", envapp, satisfaction);
    statsd_client_increment(pstate->statsd_client, buffer);

    snprintf(buffer, n, "%s.page.sum", envapp);
    statsd_client_increment(pstate->statsd_client, buffer);

    snprintf(buffer, n, "%s.page.connect_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[0]);

    snprintf(buffer, n, "%s.page.request_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[1]);

    snprintf(buffer, n, "%s.page.response_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[2]);

    snprintf(buffer, n, "%s.page.processing_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[3]);

    snprintf(buffer, n, "%s.page.load_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[4]);

    snprintf(buffer, n, "%s.page.page_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, mtimes[5]);

    // dump_increments("add_frontend_data", increments, NULL);

    increments_destroy(increments);

    // TODO: store interesting requests
}

void processor_add_ajax_data(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t *msg)
{
    // dump_json_object(stdout, request);
    // if (self->request_count % 100 == 0) {
    //     processor_dump_state(self);
    // }

    int64_t timings[2];
    if (!extract_frontend_timings(request, timings, 2, "ajax")) {
        fprintf(stderr, "[E] processor: dropped request with invalid timing information\n");
        return;
    }
    if (!check_frontend_request_validity(pstate, request, "ajax", msg))
        return;

    json_object_object_add(request, "ajax_time", json_object_new_int64(timings[1] - timings[0]));

    request_data_t request_data;
    request_data.page = processor_setup_page(self, request);
    request_data.module = processor_setup_module(self, request_data.page);
    request_data.minute = processor_setup_minute(self, request);
    request_data.total_time = processor_setup_time(self, request, "ajax_time", "frontend_time");

    // TODO: revisit when switching to percentiles
    if (request_data.total_time > 300000 || request_data.total_time < 0) {
        fprintf(stderr, "[W] dropped request data with nonsensical ajax_time\n");
        dump_json_object(stderr, request);
        return;
    }

    increments_t* increments = increments_new();
    increments->ajax_request_count = 1;
    increments_fill_metrics(increments, request);
    increments_fill_frontend_apdex(increments, request_data.total_time);
    const char* satisfaction = increments_fill_ajax_apdex(increments, request_data.total_time);

    processor_add_totals(self, request_data.page, increments);
    processor_add_totals(self, request_data.module, increments);
    processor_add_totals(self, "all_pages", increments);

    processor_add_minutes(self, request_data.page, request_data.minute, increments);
    processor_add_minutes(self, request_data.module, request_data.minute, increments);
    processor_add_minutes(self, "all_pages", request_data.minute, increments);

    processor_add_quants(self, request_data.page, increments);

    // send statsd updates
    const char* envapp = self->stream_info->yek;
    char buffer[1024];
    size_t n = sizeof(buffer);
    snprintf(buffer, n, "%s.ajax.%s", envapp, satisfaction);
    statsd_client_increment(pstate->statsd_client, buffer);

    snprintf(buffer, n, "%s.ajax.sum", envapp);
    statsd_client_increment(pstate->statsd_client, buffer);

    snprintf(buffer, n, "%s.ajax.ajax_time", envapp);
    statsd_client_timing(pstate->statsd_client, buffer, request_data.total_time);

    // dump_increments("add_ajax_data", increments, NULL);

    increments_destroy(increments);

    // TODO: store interesting requests
}

