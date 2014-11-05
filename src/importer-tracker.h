#ifndef __LOGJAM_IMPORTER_TRACKER_H_INCLUDED__
#define __LOGJAM_IMPORTER_TRACKER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _uuid_tracker_t uuid_tracker_t;

extern uuid_tracker_t* tracker_new();
extern void tracker_destroy(uuid_tracker_t **tracker);
extern int tracker_add_uuid(uuid_tracker_t *tracker, const char* uuid);
extern int tracker_delete_uuid(uuid_tracker_t *tracker, const char* uuid, zmsg_t** data);

extern void tracker(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
