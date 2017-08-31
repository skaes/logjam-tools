#ifndef __LOGJAM_DEVICE_TRACKER_H_INCLUDED__
#define __LOGJAM_DEVICE_TRACKER_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

#include "../config.h"
#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include "logjam-util.h"

typedef struct _device_tracker_t device_tracker_t;

extern device_tracker_t* device_tracker_new(zlist_t* known_devices, zsock_t* sub_socket);
extern void device_tracker_destroy(device_tracker_t** tracker);
extern int64_t device_tracker_calculate_gap(device_tracker_t* tracker, msg_meta_t* meta, const char* pub_spec);
extern void device_tracker_reconnect_stale_devices(device_tracker_t* tracker);

extern bool log_gaps;

#ifdef __cplusplus
}
#endif

#endif
