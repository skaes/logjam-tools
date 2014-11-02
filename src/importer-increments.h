#ifndef __LOGJAM_IMPORTER_INCREMENTS_H_INCLUDED__
#define __LOGJAM_IMPORTER_INCREMENTS_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

// TODO: support integer values (for call metrics)

typedef struct {
    double val;
    double val_squared;
} metric_pair_t;

typedef struct {
    size_t backend_request_count;
    size_t page_request_count;
    size_t ajax_request_count;
    metric_pair_t *metrics;
    json_object *others;
} increments_t;

typedef struct {
    const char* page;
    const char* module;
    double total_time;
    int response_code;
    int severity;
    int minute;
    int heap_growth;
    json_object* exceptions;
} request_data_t;

extern increments_t* increments_new();
extern void increments_destroy(void *increments);
extern increments_t* increments_clone(increments_t* increments);
extern void increments_add(increments_t *stored_increments, increments_t* increments);
extern void increments_fill_metrics(increments_t *increments, json_object *request);
extern void increments_add_metrics_to_json(increments_t *increments, json_object *jobj);
extern void increments_fill_apdex(increments_t *increments, double total_time);
extern void increments_fill_frontend_apdex(increments_t *increments, double total_time);
extern void increments_fill_page_apdex(increments_t *increments, double total_time);
extern void increments_fill_ajax_apdex(increments_t *increments, double total_time);
extern void increments_fill_response_code(increments_t *increments, request_data_t *request_data);
extern void increments_fill_severity(increments_t *increments, request_data_t *request_data);
extern void increments_fill_exceptions(increments_t *increments, json_object *exceptions);
extern void increments_fill_js_exception(increments_t *increments, const char *js_exception);
extern void increments_fill_caller_info(increments_t *increments, json_object *request);

extern void dump_metrics(metric_pair_t *metrics);
extern int dump_increments(const char *key, void *total, void *arg);

#ifdef __cplusplus
}
#endif

#endif
