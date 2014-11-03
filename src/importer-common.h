#ifndef __LOGJAM_IMPORTER_COMMON_H_INCLUDED__
#define __LOGJAM_IMPORTER_COMMON_H_INCLUDED__

#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <json-c/json.h>
#include <bson.h>
#include <mongoc.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef DEBUG
#undef DEBUG
#define DEBUG 1
#else
#define DEBUG 0
#endif

#define INVALID_DATE -1

#if DEBUG == 1
#define ONLY_ONE_THREAD_EACH
#endif

#ifdef ONLY_ONE_THREAD_EACH
#define NUM_PARSERS 1
#define NUM_UPDATERS 1
#define NUM_WRITERS 1
#else
#define NUM_PARSERS 4
#define NUM_UPDATERS 10
#define NUM_WRITERS 10
#endif

// discard all messages which differ by more than 1 hour from the current time
// if we have larger clockdrift: tough luck
#define INVALID_MSG_AGE_THRESHOLD 3600

#define MAX_DATABASES 100
#define DEFAULT_MONGO_URI "mongodb://127.0.0.1:27017/"

#define ISO_DATE_STR_LEN 11
extern char iso_date_today[ISO_DATE_STR_LEN];
extern char iso_date_tomorrow[ISO_DATE_STR_LEN];
extern time_t time_last_tick;

extern void dump_json_object(FILE *f, json_object *jobj);
extern void my_zframe_fprint(zframe_t *self, const char *prefix, FILE *file);
extern void my_zmsg_fprint(zmsg_t* self, const char* prefix, FILE* file);
extern bool output_socket_ready(zsock_t* socket, int msecs);

extern int replace_dots_and_dollars(char *s);
extern int copy_replace_dots_and_dollars(char* buffer, const char *s);
extern int uri_replace_dots_and_dollars(char* buffer, const char *s);
extern int convert_to_win1252(const char *str, size_t n, char *utf8);

extern void config_file_init(const char* file_name);
extern bool config_file_has_changed();
extern bool config_update_date_info();
extern int set_thread_name(const char* name);


#define USE_UNACKNOWLEDGED_WRITES 0
#define USE_BACKGROUND_INDEX_BUILDS 1
#define TOKU_TX_LOCK_FAILED 16759
#define TOKU_TX_RETRIES 2

#if USE_UNACKNOWLEDGED_WRITES == 1
#define USE_PINGS true
#else
#define USE_PINGS false
#endif

// these are all tick counts
#define PING_INTERVAL 5
#define COLLECTION_REFRESH_INTERVAL 3600
#define CONFIG_FILE_CHECK_INTERVAL 10


static inline
void log_zmq_error(int rc)
{
  if (rc != 0) {
      fprintf(stderr, "[E] rc: %d, errno: %d (%s)\n", rc, errno, zmq_strerror(errno));
  }
}

#ifdef __cplusplus
}
#endif

#endif
