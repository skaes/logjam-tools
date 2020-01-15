#ifndef __LOGJAM_GRAYLOG_FORWARDER_CONTROLLER_H__
#define __LOGJAM_GRAYLOG_FORWARDER_CONTROLLER_H__

#include "graylog-forwarder-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int graylog_forwarder_run_controller_loop(zconfig_t* config, zlist_t* devices, const char *subscription_pattern, const char *logjam_url, int rcv_hwm, int send_hwm, int heartbeat_abort_ticks);

#ifdef __cplusplus
}
#endif

#endif
