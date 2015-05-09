#ifndef __GRAYLOG_FORWARDER_COMMON_H_INCLUDED__
#define __GRAYLOG_FORWARDER_COMMON_H_INCLUDED__

#include <zlib.h>

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool dryrun;
extern bool compress_gelf;

#define MAX_PARSERS 20
extern unsigned int num_parsers;

typedef struct {
    Bytef *data;
    uLongf len;
} compressed_gelf_t;

extern compressed_gelf_t* compressed_gelf_new(Bytef *data, uLongf len);
extern void compressed_gelf_destroy(compressed_gelf_t **self_p);

#endif
