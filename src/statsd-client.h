#ifndef __LOGJAM_STATSD_CLIENT_H_INCLUDED__
#define __LOGJAM_STATSD_CLIENT_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _statsd_client_t statsd_client_t;

extern statsd_client_t* statsd_client_new(zconfig_t *config, const char* owner);
extern void statsd_client_destroy(statsd_client_t **self_p);
extern int statsd_client_increment(statsd_client_t *self, char *name);
extern int statsd_client_decrement(statsd_client_t *self, char *name);
extern int statsd_client_count(statsd_client_t *self, char *name, size_t count);
extern int statsd_client_gauge(statsd_client_t *self, char *name, size_t value);
extern int statsd_client_timing(statsd_client_t *self, char *name, size_t ms);

extern void statsd_actor_fn(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
