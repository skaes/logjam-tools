#ifndef __LOGJAM_GRAYLOG_FORWARDER_CONTROLLER_H__
#define __LOGJAM_GRAYLOG_FORWARDER_CONTROLLER_H__

#include "graylog-forwarder-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int graylog_forwarder_run_controller_loop(zconfig_t* config);

#ifdef __cplusplus
}
#endif

#endif
