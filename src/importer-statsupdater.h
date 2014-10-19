#ifndef __LOGJAM_IMPORTER_STATS_UPDATER_H_INCLUDED__
#define __LOGJAM_IMPORTER_STATS_UPDATER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void stats_updater(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
