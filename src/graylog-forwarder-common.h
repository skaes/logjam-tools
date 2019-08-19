#ifndef __GRAYLOG_FORWARDER_COMMON_H_INCLUDED__
#define __GRAYLOG_FORWARDER_COMMON_H_INCLUDED__

#include <zlib.h>

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern bool compress_gelf;

#define DEFAULT_RCV_HWM       10000
#define DEFAULT_RCV_HWM_STR  "10000"
#define DEFAULT_SND_HWM      100000
#define DEFAULT_SND_HWM_STR "100000"

#define DEFAULT_INTERFACE_PORT 9609
#define DEFAULT_INTERFACE "tcp://0.0.0.0:9609"

extern zlist_t *hosts;
extern char *interface;
extern zlist_t *subscriptions;

extern int rcv_hwm;
extern int snd_hwm;

#define MAX_PARSERS 20
extern unsigned int num_parsers;

typedef struct {
    Bytef *data;
    uLongf len;
} compressed_gelf_t;

extern compressed_gelf_t* compressed_gelf_new(Bytef *data, uLongf len);
extern void compressed_gelf_destroy(compressed_gelf_t **self_p);

#endif
