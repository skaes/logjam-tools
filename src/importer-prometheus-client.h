#ifndef __LOGJAM_IMPORTER_PROMETHEUS_CLIENT_H_INCLUDED__
#define __LOGJAM_IMPORTER_PROMETHEUS_CLIENT_H_INCLUDED__

#include "importer-common.h"
#include "logjam-streaminfo-types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint num_subscribers;
    uint num_parsers;
    uint num_writers;
    uint num_updaters;
} importer_prometheus_client_params_t;

extern void importer_prometheus_client_init(const char* address, importer_prometheus_client_params_t params);
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
extern void importer_prometheus_client_record_rusage_subscriber(uint i);
extern void importer_prometheus_client_record_rusage_parser(uint i);
extern void importer_prometheus_client_record_rusage_writer(uint i);
extern void importer_prometheus_client_record_rusage_updater(uint i);

extern void importer_prometheus_client_create_stream_counters(stream_info_t *stream);
extern void importer_prometheus_client_destroy_stream_counters(stream_info_t *stream);
extern void importer_prometheus_client_count_inserts_for_stream(stream_info_t *stream, double value);

#ifdef __cplusplus
}
#endif

#endif
