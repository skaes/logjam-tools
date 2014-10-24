#ifndef __LOGJAM_IMPORTER_PARSER_H_INCLUDED__
#define __LOGJAM_IMPORTER_PARSER_H_INCLUDED__

#include "importer-common.h"
#include "importer-tracker.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    size_t id;
    size_t parsed_msgs_count;
    zsock_t *controller_socket;
    zsock_t *pull_socket;
    zsock_t *push_socket;
    zsock_t *indexer_socket;
    json_tokener* tokener;
    zhash_t *processors;
    uuid_tracker_t *tracker;
} parser_state_t;

extern void parser(zsock_t *pipe, void *args);

#ifdef __cplusplus
}
#endif

#endif
