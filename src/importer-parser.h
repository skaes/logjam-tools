#ifndef __LOGJAM_IMPORTER_PARSER_H_INCLUDED__
#define __LOGJAM_IMPORTER_PARSER_H_INCLUDED__

#include "importer-common.h"
#include "importer-tracker.h"
#include "statsd-client.h"

#ifdef __cplusplus
extern "C" {
#endif

// frontend messages with total time larger than 60000 are dropped
// this needs to be revisited if we move to percentiles
#define FE_MSG_OUTLIER_THRESHOLD_MS 60000

enum fe_msg_drop_reason {
    FE_MSG_ACCEPTED   = 0, // not dropped at all. must be zero.
    FE_MSG_OUTLIER    = 1, // page_time larger than FE_MSG_OUTLIER_THRESHOLD_MS
    FE_MSG_NAV_TIMING = 2, // browser doesn't support navigation timing API
    FE_MSG_ILLEGAL    = 3, // no corresponding backend request
    FE_MSG_CORRUPTED  = 4, // missing or excess data
    FE_MSG_INVALID    = 5  // data couldn't be aranged in order
};
#define FE_MSG_NUM_REASONS 6

typedef struct {
    size_t received;                          // how many messages we got
    size_t dropped;                           // how many we dropped
    size_t drop_reasons[FE_MSG_NUM_REASONS];  // how many we dropped for a specific reason
} frontend_stats_t;

typedef struct {
    size_t received_backend;                     // how many messages we got
    size_t received_frontend;                    // how many messages we got
    size_t fe_dropped;                           // how many we dropped
    size_t fe_drop_reasons[FE_MSG_NUM_REASONS];  // how many we dropped for a specific reason
} user_agent_stats_t;

typedef struct {
    size_t id;
    char me[16];
    zconfig_t *config;
    size_t parsed_msgs_count;
    frontend_stats_t fe_stats;
    zsock_t *pipe;
    zsock_t *pull_socket;
    zsock_t *push_socket;
    zsock_t *indexer_socket;
    json_tokener* tokener;
    zhash_t *processors;
    uuid_tracker_t *tracker;
    statsd_client_t *statsd_client;
} parser_state_t;

extern zactor_t* parser_new(zconfig_t *config, size_t id);
extern void parser_destroy(zactor_t **parser_p);

#ifdef __cplusplus
}
#endif

#endif
