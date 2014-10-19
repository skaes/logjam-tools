#ifndef __LOGJAM_IMPORTER_SUBSCRIBER_H_INCLUDED__
#define __LOGJAM_IMPORTER_SUBSCRIBER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void subscriber(void *args, zctx_t *ctx, void *pipe);

#ifdef __cplusplus
}
#endif

#endif
