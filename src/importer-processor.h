#ifndef __LOGJAM_IMPORTER_PROCESSOR_H_INCLUDED__
#define __LOGJAM_IMPORTER_PROCESSOR_H_INCLUDED__

#include "importer-parser.h"
#include "logjam-streaminfo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    stream_info_t *stream_info;
    char *db_name;
    size_t request_count;
    zhash_t *modules;
    zhash_t *totals;
    zhash_t *minutes;
    zhash_t *quants;
    zhash_t *histograms;
    zhash_t *agents;
} processor_state_t;

extern processor_state_t* processor_new(stream_info_t *stream_info, char *db_name);
extern void processor_destroy(void* processor);
extern void processor_add_request(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t *msg);
extern void processor_add_js_exception(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t *msg);
extern void processor_add_event(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern enum fe_msg_drop_reason processor_add_frontend_data(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t *msg);
extern enum fe_msg_drop_reason processor_add_ajax_data(processor_state_t *self, parser_state_t *pstate, json_object *request, zmsg_t *msg);
extern int processor_set_frontend_apdex_attribute(const char *attr);
extern void dump_histogram(const char* key, size_t *h);
extern void dump_histograms(zhash_t* histograms);

#ifdef __cplusplus
}
#endif

#endif
