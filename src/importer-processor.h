#ifndef __LOGJAM_IMPORTER_PROCESSOR_H_INCLUDED__
#define __LOGJAM_IMPORTER_PROCESSOR_H_INCLUDED__

#include "importer-parser.h"
#include "importer-streaminfo.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char *db_name;
    stream_info_t* stream_info;
    size_t request_count;
    zhash_t *modules;
    zhash_t *totals;
    zhash_t *minutes;
    zhash_t *quants;
} processor_state_t;

extern processor_state_t* processor_new(char *db_name);
extern void processor_destroy(void* processor);
extern void processor_add_request(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern void processor_add_js_exception(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern void processor_add_event(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern void processor_add_frontend_data(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern void processor_add_ajax_data(processor_state_t *self, parser_state_t *pstate, json_object *request);
extern int processor_publish_totals(const char* db_name, void *processor, void *live_stream_socket);

#ifdef __cplusplus
}
#endif

#endif
