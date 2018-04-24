#ifndef __LOGJAM_IMPORTER_MONGO_UTILS_H_INCLUDED__
#define __LOGJAM_IMPORTER_MONGO_UTILS_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern size_t num_databases;
extern const char *databases[MAX_DATABASES];

extern mongoc_write_concern_t *wc_no_wait;
extern mongoc_write_concern_t *wc_wait;
extern mongoc_index_opt_t index_opt_default;
extern mongoc_index_opt_t index_opt_sparse;
extern mongoc_index_opt_t index_opt_unique;

extern void initialize_mongo_db_globals(zconfig_t* config);
extern void ensure_known_database(mongoc_client_t *client, const char* db_name);
extern int mongo_client_ping(mongoc_client_t *client);

#ifdef __cplusplus
}
#endif

#endif
