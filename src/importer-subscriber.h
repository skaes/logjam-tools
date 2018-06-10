#ifndef __LOGJAM_IMPORTER_SUBSCRIBER_H_INCLUDED__
#define __LOGJAM_IMPORTER_SUBSCRIBER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* subscriber_new(zconfig_t *config, size_t id);
extern void subscriber_destroy(zactor_t **subscriber_p);

#ifdef __cplusplus
}
#endif

#endif
