#ifndef __LOGJAM_IMPORTER_PARSER_H_INCLUDED__
#define __LOGJAM_IMPORTER_PARSER_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef __cplusplus
}
#endif

#endif
