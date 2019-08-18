#ifndef __LOGJAM_IMPORTER_CONTROLLER_H__
#define __LOGJAM_IMPORTER_CONTROLLER_H__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int run_controller_loop(zconfig_t* config, size_t io_threads, const char *logjam_url, const char* subscription_pattern);

#ifdef __cplusplus
}
#endif

#endif
