#include "importer-common.h"
#include "importer-resources.h"
#include "importer-increments.h"


void dump_metrics(metric_pair_t *metrics)
{
    for (size_t i=0; i<=last_resource_offset; i++) {
        if (metrics[i].val > 0) {
            printf("[D] %s:%f:sq(%f):max(%f)\n", int_to_resource[i], metrics[i].val, metrics[i].val_squared, metrics[i].val_max);
        }
    }
}

void dump_increments(const char *action, increments_t *increments)
{
    puts("[D] ------------------------------------------------");
    printf("[D] action: %s\n", action);
    printf("[D] backend requests: %zu\n", increments->backend_request_count);
    printf("[D] page requests: %zu\n", increments->page_request_count);
    printf("[D] ajax requests: %zu\n", increments->ajax_request_count);
    dump_metrics(increments->metrics);
    dump_json_object(stdout, "[D]", increments->others);
}

#define METRICS_ARRAY_SIZE (sizeof(metric_pair_t) * (last_resource_offset + 1))

increments_t* increments_new()
{
    const size_t inc_size = sizeof(increments_t);
    increments_t* increments = zmalloc(inc_size);

    const size_t metrics_size = METRICS_ARRAY_SIZE;
    increments->metrics = zmalloc(metrics_size);

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
    new_increments->backend_request_count = increments->backend_request_count;
    new_increments->page_request_count = increments->page_request_count;
    new_increments->ajax_request_count = increments->ajax_request_count;
    memcpy(new_increments->metrics, increments->metrics, METRICS_ARRAY_SIZE);
    json_object_object_foreach(increments->others, key, value) {
        json_object_get(value);
        json_object_object_add(new_increments->others, key, value);
    }
    return new_increments;
}

// TODO: this is horribly inefficient. redesign logjam protocol
// so that metrics come in a sub hash (or several)
void increments_fill_metrics(increments_t *increments, json_object *request)
{
    const int n = last_resource_offset;
    for (size_t i=0; i <= n; i++) {
        json_object* metrics_value;
        if (json_object_object_get_ex(request, int_to_resource[i], &metrics_value)) {
            double v = json_object_get_double(metrics_value);
            metric_pair_t *p = &increments->metrics[i];
            p->val = v;
            p->val_squared = v*v;
            p->val_max = v;
        }
    }
}

void increments_add_metrics_to_json(increments_t *increments, json_object *jobj)
{
    const int n = last_resource_offset;
    for (size_t i=0; i <= n; i++) {
        metric_pair_t *p = &increments->metrics[i];
        double v = p->val;
        if (v > 0) {
            json_object_object_add(jobj, int_to_resource[i], json_object_new_double(v));
        }
    }
}

#define NEW_INT1 (json_object_new_int(1))


const char* increments_fill_apdex(increments_t *increments, double total_time)
{
    json_object *others = increments->others;

    if (total_time < 100) {
        json_object_object_add(others, "apdex.happy", NEW_INT1);
        json_object_object_add(others, "apdex.satisfied", NEW_INT1);
        return "satisfied";
    } else if (total_time < 500) {
        json_object_object_add(others, "apdex.satisfied", NEW_INT1);
        return "satisfied";
    } else if (total_time < 2000) {
        json_object_object_add(others, "apdex.tolerating", NEW_INT1);
        return "tolerating";
    } else {
        json_object_object_add(others, "apdex.frustrated", NEW_INT1);
        return "frustrated";
    }
}

const char* increments_fill_frontend_apdex(increments_t *increments, double total_time)
{
    json_object *others = increments->others;

    if (total_time < 500) {
        json_object_object_add(others, "fapdex.happy", NEW_INT1);
        json_object_object_add(others, "fapdex.satisfied", NEW_INT1);
        return "satisfied";
    }
    else if (total_time < 2000) {
        json_object_object_add(others, "fapdex.satisfied", NEW_INT1);
        return "satisfied";
    } else if (total_time < 8000) {
        json_object_object_add(others, "fapdex.tolerating", NEW_INT1);
        return "tolerating";
    } else {
        json_object_object_add(others, "fapdex.frustrated", NEW_INT1);
        return "frustrated";
    }
}

const char* increments_fill_page_apdex(increments_t *increments, double total_time)
{
    json_object *others = increments->others;

    if (total_time < 500) {
        json_object_object_add(others, "papdex.happy", NEW_INT1);
        json_object_object_add(others, "papdex.satisfied", NEW_INT1);
        return "satisfied";
    }
    else if (total_time < 2000) {
        json_object_object_add(others, "papdex.satisfied", NEW_INT1);
        return "satisfied";
    } else if (total_time < 8000) {
        json_object_object_add(others, "papdex.tolerating", NEW_INT1);
        return "tolerating";
    } else {
        json_object_object_add(others, "papdex.frustrated", NEW_INT1);
        return "frustrated";
    }
}

const char* increments_fill_ajax_apdex(increments_t *increments, double total_time)
{
    json_object *others = increments->others;

    if (total_time < 500) {
        json_object_object_add(others, "xapdex.happy", NEW_INT1);
        json_object_object_add(others, "xapdex.satisfied", NEW_INT1);
        return "satisfied";
    }
    else if (total_time < 2000) {
        json_object_object_add(others, "xapdex.satisfied", NEW_INT1);
        return "satisfied";
    } else if (total_time < 8000) {
        json_object_object_add(others, "xapdex.tolerating", NEW_INT1);
        return "tolerating";
    } else {
        json_object_object_add(others, "xapdex.frustrated", NEW_INT1);
        return "frustrated";
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

void increments_fill_exceptions(increments_t *increments, json_object *exceptions)
{
    if (exceptions == NULL)
        return;
    int n = json_object_array_length(exceptions);
    if (n == 0)
        return;

    for (int i=0; i<n; i++) {
        json_object* ex_obj = json_object_array_get_idx(exceptions, i);
        const char *ex_str = json_object_get_string(ex_obj);
        size_t n = strlen(ex_str);
        char ex_str_dup[n+12];
        strcpy(ex_str_dup, "exceptions.");
        strcpy(ex_str_dup+11, ex_str);
        int replaced_count = replace_dots_and_dollars(ex_str_dup+11);
        // printf("[D] EXCEPTION: %s\n", ex_str_dup);
        if (replaced_count > 0) {
            json_object* new_ex = json_object_new_string(ex_str_dup+11);
            json_object_array_put_idx(exceptions, i, new_ex);
        }
        json_object_object_add(increments->others, ex_str_dup, NEW_INT1);
    }
}

void increments_fill_soft_exceptions(increments_t *increments, json_object *soft_exceptions)
{
  if (soft_exceptions == NULL)
    return;
  int n = json_object_array_length(soft_exceptions);
  if (n == 0)
    return;

  for (int i=0; i<n; i++) {
    json_object* ex_obj = json_object_array_get_idx(soft_exceptions, i);
    const char *ex_str = json_object_get_string(ex_obj);
    size_t n = strlen(ex_str);
    char ex_str_dup[n+17];
    strcpy(ex_str_dup, "soft_exceptions.");
    strcpy(ex_str_dup+16, ex_str);
    int replaced_count = replace_dots_and_dollars(ex_str_dup+16);
    // printf("[D] EXCEPTION: %s\n", ex_str_dup);
    if (replaced_count > 0) {
      json_object* new_ex = json_object_new_string(ex_str_dup+16);
      json_object_array_put_idx(soft_exceptions, i, new_ex);
    }
    json_object_object_add(increments->others, ex_str_dup, NEW_INT1);
  }
}

void increments_fill_js_exception(increments_t *increments, const char *js_exception)
{
    size_t n = strlen(js_exception);
    int l = 14;
    char xbuffer[l+3*n+1];
    strcpy(xbuffer, "js_exceptions.");
    uri_replace_dots_and_dollars(xbuffer+l, js_exception);
    // printf("[D] JS EXCEPTION: %s\n", xbuffer);
    json_object_object_add(increments->others, xbuffer, NEW_INT1);
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
            size_t n = strlen(caller_id) + 1;
            char app[n], env[n], rid[n];
            if (3 == sscanf(caller_id, "%[^-]-%[^-]-%[^-]", app, env, rid)) {
                size_t app_len = strlen(app) + 1;
                size_t action_len = strlen(caller_action) + 1;
                char caller_name[4*(app_len + action_len) + 2 + 8];
                strcpy(caller_name, "callers.");
                int real_app_len = copy_replace_dots_and_dollars(caller_name + 8, app);
                caller_name[real_app_len + 8] = '-';
                copy_replace_dots_and_dollars(caller_name + 8 + real_app_len + 1, caller_action);
                // printf("[D] CALLER: %s\n", caller_name);
                json_object_object_add(increments->others, caller_name, NEW_INT1);
            }
        }
    }
}

void increments_fill_sender_info(increments_t *increments, json_object *request)
{
    json_object *sender_action_obj;
    if (json_object_object_get_ex(request, "sender_action", &sender_action_obj)) {
        const char *sender_action = json_object_get_string(sender_action_obj);
        if (sender_action == NULL || *sender_action == '\0') return;
        json_object *sender_id_obj;
        if (json_object_object_get_ex(request, "sender_id", &sender_id_obj)) {
            const char *sender_id = json_object_get_string(sender_id_obj);
            if (sender_id == NULL || *sender_id == '\0') return;
            size_t n = strlen(sender_id) + 1;
            char app[n], env[n], rid[n];
            if (3 == sscanf(sender_id, "%[^-]-%[^-]-%[^-]", app, env, rid)) {
                size_t app_len = strlen(app) + 1;
                size_t action_len = strlen(sender_action) + 1;
                char sender_name[4*(app_len + action_len) + 2 + 8];
                strcpy(sender_name, "senders.");
                int real_app_len = copy_replace_dots_and_dollars(sender_name + 8, app);
                sender_name[real_app_len + 8] = '-';
                copy_replace_dots_and_dollars(sender_name + 8 + real_app_len + 1, sender_action);
                // printf("[D] SENDER: %s\n", sender_name);
                json_object_object_add(increments->others, sender_name, NEW_INT1);
            }
        }
    }
}

void increments_add(increments_t *stored_increments, increments_t* increments)
{
    stored_increments->backend_request_count += increments->backend_request_count;
    stored_increments->page_request_count += increments->page_request_count;
    stored_increments->ajax_request_count += increments->ajax_request_count;
    for (size_t i=0; i<=last_resource_offset; i++) {
        metric_pair_t *stored = &(stored_increments->metrics[i]);
        metric_pair_t *addend = &(increments->metrics[i]);
        stored->val += addend->val;
        stored->val_squared += addend->val_squared;
        if (stored->val_max < addend->val_max)
            stored->val_max = addend->val_max;
    }
    json_object_object_foreach(increments->others, key, value) {
        json_object *stored_obj = NULL;
        json_object *new_obj = NULL;
        bool perform_addition = json_object_object_get_ex(stored_increments->others, key, &stored_obj);
        enum json_type type = json_object_get_type(value);
        switch (type) {
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
            fprintf(stderr, "[E] unknown increment type: %s, for key: %s\n", json_type_to_name(type), key);
            dump_json_object(stderr, "[E]", increments->others);
        }
        if (new_obj) {
            json_object_object_add(stored_increments->others, key, new_obj);
        }
    }
}
