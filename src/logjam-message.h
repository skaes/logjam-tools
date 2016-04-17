#ifndef __LOGJAM_MESSAGE_H_INCLUDED__
#define __LOGJAM_MESSAGE_H_INCLUDED__

#include <czmq.h>
#include "gelf-message.h"

typedef struct _logjam_message logjam_message;

logjam_message* logjam_message_read(zsock_t *receiver);

gelf_message* logjam_message_to_gelf(logjam_message *logjam_msg, zchunk_t *decompression_buffer);

size_t logjam_message_size(logjam_message *msg);

void logjam_message_destroy(logjam_message **msg);

#endif
