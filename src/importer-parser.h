#ifndef LOGJAM_IMPORTER_PARSER_H
#define LOGJAM_IMPORTER_PARSER_H

#include "importer-common.h"

typedef struct {
    size_t id;
    size_t parsed_msgs_count;
    void *controller_socket;
    void *pull_socket;
    void *push_socket;
    void *indexer_socket;
    json_tokener* tokener;
    zhash_t *processors;
} parser_state_t;

extern void parser(void *args, zctx_t *ctx, void *pipe);

#endif
