#ifndef LOGJAM_IMPORTER_MONGO_UTILS_H
#define LOGJAM_IMPORTER_MONGO_UTILS_H

#include "importer-common.h"

#define USE_UNACKNOWLEDGED_WRITES 0
#define USE_BACKGROUND_INDEX_BUILDS 1
#define TOKU_TX_LOCK_FAILED 16759
#define TOKU_TX_RETRIES 2

#if USE_UNACKNOWLEDGED_WRITES == 1
#define USE_PINGS true
#else
#define USE_PINGS false
#endif

extern bool dryrun;

extern size_t num_databases;
extern const char *databases[MAX_DATABASES];

extern mongoc_write_concern_t *wc_no_wait;
extern mongoc_write_concern_t *wc_wait;
extern mongoc_index_opt_t index_opt_default;
extern mongoc_index_opt_t index_opt_sparse;

extern void my_mongo_log_handler(mongoc_log_level_t log_level, const char *log_domain,  const char *message, void *user_data);
extern void initialize_mongo_db_globals(zconfig_t* config);
extern void ensure_known_database(mongoc_client_t *client, const char* db_name);
extern int mongo_client_ping(mongoc_client_t *client);

#endif
