#ifndef __LOGJAM_UNKNOWN_STREAMS_COLLECTOR_H_INCLUDED__
#define __LOGJAM_UNKNOWN_STREAMS_COLLECTOR_H_INCLUDED__

#include "importer-common.h"
#include "logjam-streaminfo.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void unknown_streams_collector_actor_fn(zsock_t *pipe, void *args);
extern zsock_t* unknown_streams_collector_client_socket_new(zconfig_t* config);

#ifdef __cplusplus
}
#endif

#endif
