#ifndef LOGJAM_IMPORTER_LIVESTREAM_H
#define LOGJAM_IMPORTER_LIVESTREAM_H

#include "importer-common.h"
#include "importer-streaminfo.h"

extern void* live_stream_socket_new(zctx_t *context);
extern void live_stream_publish(void *live_stream_socket, const char* key, const char* json_str);
extern void publish_error_for_module(stream_info_t *stream_info, const char* module, const char* json_str, void* live_stream_socket);

#endif
