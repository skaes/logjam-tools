#ifndef __LOGJAM_GRAYLOG_FORWARDER_SUBSCRIBER_H_INCLUDED__
#define __LOGJAM_GRAYLOG_FORWARDER_SUBSCRIBER_H_INCLUDED__

#include "graylog-forwarder-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* graylog_forwarder_subscriber_new(zconfig_t *config, zlist_t *devices, int rcv_hwm, int send_hwm);

#ifdef __cplusplus
}
#endif

#endif
