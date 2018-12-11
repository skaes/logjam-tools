#ifndef __LOGJAM_PROMETHEUS_CLIENT_H_INCLUDED__
#define __LOGJAM_PROMETHEUS_CLIENT_H_INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

#define IMPORTER_UPDATE_COUNT 1
#define IMPORTER_UPDATE_TIME 2
#define IMPORTER_INSERT_COUNT 3
#define IMPORTER_INSERT_TIME 4
#define IMPORTER_MSGS_RECEIVED_COUNT 5
#define IMPORTER_MSGS_MISSED_COUNT 6
#define IMPORTER_MSGS_PARSED_COUNT 7
#define IMPORTER_QUEUED_UPDATES_COUNT 8
#define IMPORTER_QUEUED_INSERTS_COUNT 9

extern void prometheus_client_init(const char* address);
extern void prometheus_client_shutdown();

extern void prometheus_client_count(int metric, double value);
extern void prometheus_client_gauge(int metric, double value);
extern void prometheus_client_timing(int metric, double value);

#ifdef __cplusplus
}
#endif

#endif
