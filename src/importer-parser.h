#ifndef __LOGJAM_IMPORTER_PARSER_H_INCLUDED__
#define __LOGJAM_IMPORTER_PARSER_H_INCLUDED__

#include "importer-common.h"
#include "importer-tracker.h"
#include "statsd-client.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    size_t id;
    char me[16];
    zconfig_t *config;
    size_t parsed_msgs_count;
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
