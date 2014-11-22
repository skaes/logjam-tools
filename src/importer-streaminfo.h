#ifndef __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__
#define __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char* name;
    size_t value;
} module_threshold_t;


typedef struct {
    const char *key;      // [app,env].join('-')
    const char *yek;      // [env,app].join('.')
    const char *app;
    const char *env;
    size_t key_len;
    size_t app_len;
    size_t env_len;
    int db;
    int import_threshold;
    int module_threshold_count;
    module_threshold_t *module_thresholds;
    const char *ignored_request_prefix;
    const char **backend_only_requests;
    int backend_only_requests_size;
    int all_requests_are_backend_only_requests;
    zhash_t *known_modules;
} stream_info_t;

extern int global_total_time_import_threshold;
extern const char* global_ignored_request_prefix;

// all configured streams
extern zhash_t *configured_streams;
// all streams we want to subscribe to
extern zhash_t *stream_subscriptions;

extern void setup_stream_config(zconfig_t* config, const char* pattern);
extern void update_known_modules(stream_info_t *stream_info, zhash_t* module_hash);

#ifdef __cplusplus
}
#endif

#endif
