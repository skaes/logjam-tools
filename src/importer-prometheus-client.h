#ifndef __LOGJAM_IMPORTER_PROMETHEUS_CLIENT_H_INCLUDED__
#define __LOGJAM_IMPORTER_PROMETHEUS_CLIENT_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void importer_prometheus_client_init(const char* address);
extern void importer_prometheus_client_shutdown();

extern void importer_prometheus_client_count_updates(double value);
extern void importer_prometheus_client_count_inserts(double value);
extern void importer_prometheus_client_count_msgs_missed(double value);
extern void importer_prometheus_client_count_msgs_received(double value);
extern void importer_prometheus_client_count_bytes_received(double value);
extern void importer_prometheus_client_count_msgs_dropped(double value);
extern void importer_prometheus_client_count_msgs_blocked(double value);
extern void importer_prometheus_client_count_msgs_parsed(double value);
extern void importer_prometheus_client_count_updates_blocked(double value);
extern void importer_prometheus_client_count_inserts_failed(double value);
extern void importer_prometheus_client_gauge_queued_inserts(double value);
extern void importer_prometheus_client_gauge_queued_updates(double value);
extern void importer_prometheus_client_time_inserts(double value);
extern void importer_prometheus_client_time_updates(double value);

#ifdef __cplusplus
}
#endif

#endif
