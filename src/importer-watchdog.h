#ifndef __LOGJAM_IMPORTER_WATCHDOG_H_INCLUDED__
#define __LOGJAM_IMPORTER_WATCHDOG_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* watchdog_new(uint32_t credit, size_t id);
extern void watchdog_destroy(zactor_t **watchdog_p);

#ifdef __cplusplus
}
#endif

#endif
