#ifndef __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__
#define __LOGJAM_IMPORTER_STREAM_INFO_H_INCLUDED__

#include "logjam-streaminfo-types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void set_stream_create_fn(stream_fn *f);
extern void set_stream_free_fn(stream_fn *f);

extern stream_info_t* get_stream_info(const char* stream_name, zhash_t* thread_local_cache);
static inline void reference_stream_info(stream_info_t *stream_info) {
    __atomic_fetch_add(&stream_info->ref_count, 1, __ATOMIC_SEQ_CST);
}
extern void release_stream_info(stream_info_t *stream_info);
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
extern void adjust_caller_info(const char* path, const char* module, json_object *request, stream_info_t *stream_info);
extern bool throttle_request_for_stream(stream_info_t *stream_info);

typedef int sampling_reason_t;
#define SAMPLE_SLOW_REQUEST    1
#define SAMPLE_LOG_SEVERITY 1<<1
#define SAMPLE_500          1<<2
#define SAMPLE_400          1<<3
#define SAMPLE_EXCEPTIONS   1<<4
#define SAMPLE_HEAP_GROWTH  1<<5
#define SAMPLE_000          1<<6

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
