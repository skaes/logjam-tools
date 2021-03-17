#ifndef __LOGJAM_GRAYLOG_FORWARDER_PROMETHEUS_CLIENT_H_INCLUDED__
#define __LOGJAM_GRAYLOG_FORWARDER_PROMETHEUS_CLIENT_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void graylog_forwarder_prometheus_client_init(const char* address, int num_parsers);
extern void graylog_forwarder_prometheus_client_shutdown();

extern void graylog_forwarder_prometheus_client_count_msgs_received(double value);
extern void graylog_forwarder_prometheus_client_count_bytes_received(double value);
extern void graylog_forwarder_prometheus_client_count_msgs_forwarded(double value);
extern void graylog_forwarder_prometheus_client_count_bytes_forwarded(double value);
extern void graylog_forwarder_prometheus_client_count_gelf_bytes(double value);
extern void graylog_forwarder_prometheus_client_record_rusage_subscriber();
extern void graylog_forwarder_prometheus_client_record_rusage_parser(int i);
extern void graylog_forwarder_prometheus_client_record_rusage_writer();

extern void graylog_forwarder_prometheus_client_count_msg_for_stream(const char* app_env);
extern void graylog_forwarder_prometheus_client_count_forwarded_bytes_for_stream(const char* app_env, double value);
extern void graylog_forwarder_prometheus_client_count_gelf_source_bytes_for_stream(const char* app_env, double value);
extern void graylog_forwarder_prometheus_client_delete_old_stream_counters(int64_t max_age);
extern void graylog_forwarder_prometheus_client_record_device_sequence_number(uint32_t, const char* device, uint64_t n);

#ifdef __cplusplus
}
#endif

#endif
