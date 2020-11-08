#ifndef __LOGJAM_IMPORTER_MONGO_UTILS_H_INCLUDED__
#define __LOGJAM_IMPORTER_MONGO_UTILS_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int num_databases;
extern const char *databases[MAX_DATABASES];

extern mongoc_write_concern_t *wc_no_wait;
extern mongoc_write_concern_t *wc_wait;

extern void initialize_mongo_db_globals(zconfig_t* config);
extern bool ensure_known_database(mongoc_client_t *client, const char* db_name);
extern bool ensure_known_databases(mongoc_client_t *client, zlist_t *db_names);
extern int mongo_client_ping(mongoc_client_t *client);

#ifdef __cplusplus
}
#endif

#endif
