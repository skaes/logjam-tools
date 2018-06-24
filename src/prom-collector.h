#ifndef __LOGJAM_PROM_COLLECTOR_H_INCLUDED__
#define __LOGJAM_PROM_COLLECTOR_H_INCLUDED__

#include "importer-common.h"
#include "importer-streaminfo.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void prom_collector_actor_fn(zsock_t *pipe, void *args);
void prom_collector_publish(zsock_t *prom_collector_socket, const char* app_env, const char* body);

#ifdef __cplusplus
}
#endif

#endif
