#ifndef __LOGJAM_IMPORTER_STATS_UPDATER_H_INCLUDED__
#define __LOGJAM_IMPORTER_STATS_UPDATER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* stats_updater_new(zconfig_t *config, size_t id);

#ifdef __cplusplus
}
#endif

#endif
