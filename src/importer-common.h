#ifndef __LOGJAM_IMPORTER_COMMON_H_INCLUDED__
#define __LOGJAM_IMPORTER_COMMON_H_INCLUDED__

#include <bson.h>
#include <mongoc.h>
#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_SUBSCRIBERS 10
#define MAX_PARSERS 32
#define MAX_ADDERS 16
#define MAX_WRITERS 20
#define MAX_UPDATERS 20

extern unsigned long num_subscribers;
extern unsigned long num_parsers;
extern unsigned long num_writers;
extern unsigned long num_updaters;

extern int queued_updates;
extern int queued_inserts;

#define DEFAULT_RCV_HWM      100000
#define DEFAULT_RCV_HWM_STR "100000"
#define DEFAULT_SND_HWM      100000
#define DEFAULT_SND_HWM_STR "100000"
#define HWM_UNLIMITED 0

#define DEFAULT_ROUTER_PORT 9604
#define DEFAULT_PULL_PORT 9605
#define DEFAULT_SUB_PORT 9606
#define DEFAULT_LIVE_STREAM_PORT 9607
#define DEFAULT_UNKNOWN_STREAMS_COLLECTOR_PORT 9612
#define DEFAULT_METRICS_PORT 9610

#define DEFAULT_REP_CONNECTION_SPEC "tcp://localhost:9604"
#define DEFAULT_PULL_CONNECTION_SPEC "tcp://localhost:9605"
#define DEFAULT_SUB_CONNECTION_SPEC "tcp://localhost:9606"
#define DEFAULT_LIVE_STREAM_CONNECTION "tcp://*:9607"
#define DEFAULT_UNKNOWN_STREAMS_COLLECTOR_CONNECTION "tcp://*:9612"

extern char* live_stream_connection_spec;
extern char* unknown_streams_collector_connection_spec;
extern int router_port;
extern int pull_port;
extern int sub_port;
extern int rcv_hwm;
extern int snd_hwm;
extern zlist_t* hosts;
extern FILE* frontend_timings;

#define INVALID_DATE -1

// discard all messages which differ by more than 2 days from the current time
// if we have longer running jobs : tough luck
#define INVALID_MSG_AGE_THRESHOLD 172800

#define MAX_DATABASES 100
#define DEFAULT_MONGO_URI "mongodb://127.0.0.1:27017/"

extern bool dryrun;
extern bool verbose;
extern bool debug;
extern bool quiet;
extern bool send_statsd_msgs;
extern bool initialize_dbs;

extern char iso_date_today[ISO_DATE_STR_LEN];
extern char iso_date_tomorrow[ISO_DATE_STR_LEN];
extern time_t time_last_tick;

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

#if USE_UNACKNOWLEDGED_WRITES == 1
#define USE_PINGS true
#else
#define USE_PINGS false
#endif

// these are all tick counts
#define PING_INTERVAL 5
#define COLLECTION_REFRESH_INTERVAL 3600
#define CONFIG_FILE_CHECK_INTERVAL 10
#define DATABASE_INFO_REFRESH_INTERVAL 60
#define DATABASE_UPDATE_INTERVAL 5

// maximum size of histograms stored in mongo
#define HISTOGRAM_SIZE 22

#ifdef __cplusplus
}
#endif

#endif
