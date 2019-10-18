#ifndef __LOGJAM_DEVICE_PROMETHEUS_CLIENT_H_INCLUDED__
#define __LOGJAM_DEVICE_PROMETHEUS_CLIENT_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void device_prometheus_client_init(const char* address, const char* device, int num_compressors);
extern void device_prometheus_client_shutdown();

extern void device_prometheus_client_count_msgs_received(double value);
extern void device_prometheus_client_count_bytes_received(double value);
extern void device_prometheus_client_count_msgs_compressed(double value);
extern void device_prometheus_client_count_bytes_compressed(double value);
extern void device_prometheus_client_count_pings(double value);
extern void device_prometheus_client_count_ping(const char* app_env);
extern void device_prometheus_client_record_rusage();
extern void device_prometheus_client_record_rusage_compressor(int i);

#ifdef __cplusplus
}
#endif

#endif
