#ifndef __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__
#define __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char* name;
    size_t value;
} module_threshold_t;

typedef struct {
    int32_t ref_count;
    char *key;      // [app,env].join('-')
    char *yek;      // [env,app].join('.')
    char *app;
    char *env;
    size_t key_len;
    size_t app_len;
    size_t env_len;
    int db;
    int database_cleaning_threshold;
    int request_cleaning_threshold;
    int import_threshold;
    int module_threshold_count;
    int64_t storage_size;
    double sampling_rate_400s;
    long sampling_rate_400s_threshold;
    module_threshold_t *module_thresholds;
    char *ignored_request_prefix;
    char **backend_only_requests;
    int backend_only_requests_size;
    int all_requests_are_backend_only_requests;
    char **api_requests;
    int api_requests_size;
    int all_requests_are_api_requests;
    zhash_t *known_modules;
} stream_info_t;

extern stream_info_t* get_stream(const char* stream_name);
extern void put_stream(stream_info_t *stream);
extern const char* get_subscription_pattern();
extern zlist_t* get_stream_subscriptions();
extern zlist_t* get_active_stream_names();

extern void stream_config_updater(zsock_t *pipe, void *args);

#define MAX_RANDOM_VALUE ((1L<<31) - 1)
#define TEN_PERCENT_OF_MAX_RANDOM 214748364

// request storage size soft limit is 15 GB, hard limit 30 GB, per app
#define SOFT_LIMIT_STORAGE_SIZE 16106127360
#define HARD_LIMIT_STORAGE_SIZE 32212254720

extern bool setup_stream_config(const char* logjam_url, const char* pattern);
extern void update_known_modules(stream_info_t *stream_info, zhash_t* module_hash);

typedef int sampling_reason_t;
#define SAMPLE_SLOW_REQUEST    1
#define SAMPLE_LOG_SEVERITY 1<<1
#define SAMPLE_500          1<<2
#define SAMPLE_400          1<<3
#define SAMPLE_EXCEPTIONS   1<<4
#define SAMPLE_HEAP_GROWTH  1<<5

#define LOG_SEVERITY_DEBUG 0
#define LOG_SEVERITY_INFO  1
#define LOG_SEVERITY_WARN  2
#define LOG_SEVERITY_ERROR 3
#define LOG_SEVERITY_FATAL 4
#define LOG_SEVERITY_ANY   5

#ifdef __cplusplus
}
#endif

#endif
