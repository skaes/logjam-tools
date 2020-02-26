#ifndef __LOGJAM_IMPORTER_STREAM_INFO_TYPES_H_INCLUDED__
#define __LOGJAM_IMPORTER_STREAM_INFO_TYPES_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char* name;
    size_t value;
} module_threshold_t;

#define DEFAULT_MAX_INSERTS_PER_SECOND 100

typedef struct {
    int64_t cap;                       // number of insertions allowed during one tick
    int64_t current;                   // number of requests inserted since the last tick
} requests_inserted_t;

typedef void (stream_fn) (void *stream);

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
    void *inserts_total;
    stream_fn *free_callback;
    requests_inserted_t *requests_inserted;
    bool free_requests_inserted;
} stream_info_t;


#ifdef __cplusplus
}
#endif

#endif
