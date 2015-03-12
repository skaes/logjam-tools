#ifndef __LOGJAM_GRAYLOG_FORWARDER_PARSER_H_INCLUDED__
#define __LOGJAM_GRAYLOG_FORWARDER_PARSER_H_INCLUDED__

#include "graylog-forwarder-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* graylog_forwarder_parser_new(zconfig_t *config, size_t id);
extern void graylog_forwarder_parser_destroy(zactor_t **parser_p);

#ifdef __cplusplus
}
#endif

#endif
