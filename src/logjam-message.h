#ifndef __LOGJAM_MESSAGE_H_INCLUDED__
#define __LOGJAM_MESSAGE_H_INCLUDED__

#include <czmq.h>
#include <json_tokener.h>
#include "gelf-message.h"

typedef struct {
    zframe_t *frames[4];
    size_t size;
    char* stream;
} logjam_message;

logjam_message* logjam_message_read(zsock_t *receiver);

gelf_message* logjam_message_to_gelf(logjam_message *logjam_msg, json_tokener *tokener, zhash_t* stream_info_cache, zchunk_t *decompression_buffer, zchunk_t *scratch_buffer, zhash_t *header_fields);

void logjam_message_destroy(logjam_message **msg);

#endif
