#ifndef __LOGJAM_IMPORTER_REQUEST_WRITER_H_INCLUDED__
#define __LOGJAM_IMPORTER_REQUEST_WRITER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void request_writer(void *args, zctx_t *ctx, void *pipe);

#ifdef __cplusplus
}
#endif

#endif
