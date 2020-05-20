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

#ifdef __cplusplus
}
#endif

#endif
