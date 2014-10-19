#ifndef __LOGJAM_IMPORTER_REQUEST_WRITER_H_INCLUDED__
#define __LOGJAM_IMPORTER_REQUEST_WRITER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

    extern void request_writer(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
