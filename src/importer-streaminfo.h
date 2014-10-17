#ifndef LOGJAM_IMPORTER_STREAM_INFO_H
#define LOGJAM_IMPORTER_STREAM_INFO_H

#include "importer-common.h"

typedef struct {
    const char* name;
    size_t value;
} module_threshold_t;

typedef struct {
    const char *key;
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
} stream_info_t;

extern int global_total_time_import_threshold;
extern const char* global_ignored_request_prefix;

// all configured streams
extern zhash_t *configured_streams;
// all streams we want to subscribe to
extern zhash_t *stream_subscriptions;

void setup_stream_config(zconfig_t* config, const char* pattern);

#endif
