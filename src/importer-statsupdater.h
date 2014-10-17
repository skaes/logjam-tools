#ifndef LOGJAM_IMPORTER_STATS_UPDATER_H
#define LOGJAM_IMPORTER_STATS_UPDATER_H

#include "importer-common.h"

/* stats updater state */
typedef struct {
    size_t id;
    mongoc_client_t *mongo_clients[MAX_DATABASES];
    mongoc_collection_t *global_collection;
    zhash_t *stats_collections;
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    size_t updates_count;
} stats_updater_state_t;

extern void stats_updater(void *args, zctx_t *ctx, void *pipe);

#endif
