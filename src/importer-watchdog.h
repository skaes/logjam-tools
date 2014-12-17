#ifndef __LOGJAM_IMPORTER_WATCHDOG_H_INCLUDED__
#define __LOGJAM_IMPORTER_WATCHDOG_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void watchdog(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
