#include "importer-mongoutils.h"

size_t num_databases = 0;
const char *databases[MAX_DATABASES];

mongoc_write_concern_t *wc_no_wait = NULL;
mongoc_write_concern_t *wc_wait = NULL;
mongoc_index_opt_t index_opt_default;
mongoc_index_opt_t index_opt_sparse;

static
void my_mongo_log_handler(mongoc_log_level_t log_level, const char *log_domain, const char *message, void *user_data)
{
   FILE *stream;
   char level = 'I';

   switch (log_level) {
   case MONGOC_LOG_LEVEL_ERROR:
   case MONGOC_LOG_LEVEL_CRITICAL:
       level = 'E';
       stream = stderr;
       break;
   case MONGOC_LOG_LEVEL_WARNING:
       level = 'W';
       stream = stderr;
       break;
   case MONGOC_LOG_LEVEL_MESSAGE:
   case MONGOC_LOG_LEVEL_INFO:
       level = 'I';
       stream = stdout;
       break;
   case MONGOC_LOG_LEVEL_DEBUG:
   case MONGOC_LOG_LEVEL_TRACE:
       level = 'D';
       stream = stdout;
       break;
   default:
       stream = stdout;
   }

   fprintf(stream, "[%c] monogc[%s]: %s\n", level, log_domain, message);
}

void initialize_mongo_db_globals(zconfig_t* config)
{
    mongoc_init();
    mongoc_log_set_handler(my_mongo_log_handler, NULL);

    wc_wait = mongoc_write_concern_new();
    mongoc_write_concern_set_w(wc_wait, MONGOC_WRITE_CONCERN_W_DEFAULT);

    wc_no_wait = mongoc_write_concern_new();
    if (USE_UNACKNOWLEDGED_WRITES)
        // TODO: this leads to illegal opcodes on the server
       mongoc_write_concern_set_w(wc_no_wait, MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);
    else
        mongoc_write_concern_set_w(wc_no_wait, MONGOC_WRITE_CONCERN_W_DEFAULT);

    mongoc_index_opt_init(&index_opt_default);
    if (USE_BACKGROUND_INDEX_BUILDS)
        index_opt_default.background = true;
    else
        index_opt_default.background = false;

    mongoc_index_opt_init(&index_opt_sparse);
    index_opt_sparse.sparse = true;
    if (USE_BACKGROUND_INDEX_BUILDS)
        index_opt_sparse.background = true;
    else
        index_opt_sparse.background = false;

    zconfig_t* dbs = zconfig_locate(config, "backend/databases");
    if (dbs) {
        zconfig_t *db = zconfig_child(dbs);
        while (db) {
            assert(num_databases < MAX_DATABASES);
            char *uri = zconfig_value(db);
            if (uri != NULL) {
                databases[num_databases] = strdup(uri);
                printf("[I] database[%zu]: %s\n", num_databases, uri);
                num_databases++;
            }
            db = zconfig_next(db);
        }
    }
    if (num_databases == 0) {
        databases[num_databases] = DEFAULT_MONGO_URI;
        printf("[I] database[%zu]: %s\n", num_databases, DEFAULT_MONGO_URI);
        num_databases++;
    }
}


void ensure_known_database(mongoc_client_t *client, const char* db_name)
{
    mongoc_collection_t *meta_collection = mongoc_client_get_collection(client, "logjam-global", "metadata");
    bson_t *selector = bson_new();
    assert(bson_append_utf8(selector, "name", 4, "databases", 9));

    bson_t *document = bson_new();
    bson_t *sub_doc = bson_new();
    bson_append_utf8(sub_doc, "value", 5, db_name, -1);
    bson_append_document(document, "$addToSet", 9, sub_doc);

    if (!dryrun) {
        bson_error_t error;
        int tries = TOKU_TX_RETRIES+3; // try harder than for normal updates
    retry:
        if (!mongoc_collection_update(meta_collection, MONGOC_UPDATE_UPSERT, selector, document, wc_no_wait, &error)) {
            if ((error.code == TOKU_TX_LOCK_FAILED) && (--tries > 0)) {
                fprintf(stderr, "[W] retrying update on logjam-global: %s\n", db_name);
                goto retry;
            } else {
                fprintf(stderr, "[E] update failed on logjam-global: (%d) %s\n", error.code, error.message);
            }
        }
    }

    bson_destroy(selector);
    bson_destroy(document);
    bson_destroy(sub_doc);

    mongoc_collection_destroy(meta_collection);
}

int mongo_client_ping(mongoc_client_t *client)
{
    int available = 1;
#if USE_PINGS == 1
    bson_t ping;
    bson_init(&ping);
    bson_append_int32(&ping, "ping", 4, 1);

    mongoc_database_t *database = mongoc_client_get_database(client, "logjam-global");
    mongoc_cursor_t *cursor = mongoc_database_command(database, 0, 0, 1, 0, &ping, NULL, NULL);

    const bson_t *reply;
    bson_error_t error;
    if (mongoc_cursor_next(cursor, &reply)) {
        available = 0;
        // char *str = bson_as_json(reply, NULL);
        // fprintf(stdout, "D %s\n", str);
        // bson_free(str);
    } else if (mongoc_cursor_error(cursor, &error)) {
        fprintf(stderr, "[E] ping failure: (%d) %s\n", error.code, error.message);
    }
    bson_destroy(&ping);
    mongoc_cursor_destroy(cursor);
    mongoc_database_destroy(database);
#endif
    return available;
}
