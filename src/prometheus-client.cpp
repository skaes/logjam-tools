#include <prometheus/exposer.h>
#include "prometheus-client.h"
#include <prometheus/registry.h>

static struct prometheus_client_t {
    prometheus::Exposer *exposer;
    std::shared_ptr<prometheus::Registry> registry;
    prometheus::Family<prometheus::Counter> *updates_total_family;
    prometheus::Counter *updates_total;
    prometheus::Family<prometheus::Counter> *updates_seconds_family;
    prometheus::Counter *updates_seconds;
    prometheus::Family<prometheus::Counter> *inserts_total_family;
    prometheus::Counter *inserts_total;
    prometheus::Family<prometheus::Counter> *inserts_seconds_family;
    prometheus::Counter *inserts_seconds;
    prometheus::Family<prometheus::Counter> *received_msgs_total_family;
    prometheus::Counter *received_msgs_total;
    prometheus::Family<prometheus::Counter> *missed_msgs_total_family;
    prometheus::Counter *missed_msgs_total;
    prometheus::Family<prometheus::Counter> *parsed_msgs_total_family;
    prometheus::Counter *parsed_msgs_total;
    prometheus::Family<prometheus::Gauge> *queued_updates_family;
    prometheus::Gauge *queued_updates;
    prometheus::Family<prometheus::Gauge> *queued_inserts_family;
    prometheus::Gauge *queued_inserts;
} client;

void prometheus_client_init(const char* address)
{
    // create a http server running on the given address
    client.exposer = new prometheus::Exposer{address};
    // create a metrics registry
    client.registry = std::make_shared<prometheus::Registry>();

    // declare metrics families, counters and gauges
    client.updates_total_family = &prometheus::BuildCounter()
        .Name("importer_updates_total")
        .Help("How many updates has this importer performed")
        .Register(*client.registry);

    client.updates_total = &client.updates_total_family->Add({});

    client.updates_seconds_family = &prometheus::BuildCounter()
        .Name("importer_updates_seconds")
        .Help("How many seconds has this importer spent updating")
        .Register(*client.registry);

    client.updates_seconds = &client.updates_seconds_family->Add({});

    client.inserts_total_family = &prometheus::BuildCounter()
        .Name("importer_inserts_total")
        .Help("How many inserts has this importer performed")
        .Register(*client.registry);

    client.inserts_total = &client.inserts_total_family->Add({});

    client.inserts_seconds_family = &prometheus::BuildCounter()
        .Name("importer_inserts_seconds")
        .Help("How many seconds has this imorter spent inserting")
        .Register(*client.registry);

    client.inserts_seconds = &client.inserts_seconds_family->Add({});

    client.received_msgs_total_family = &prometheus::BuildCounter()
        .Name("importer_msgs_received_total")
        .Help("How many logjam messages has this importer received")
        .Register(*client.registry);

    client.received_msgs_total = &client.received_msgs_total_family->Add({});

    client.missed_msgs_total_family = &prometheus::BuildCounter()
        .Name("importer_msgs_missed_total")
        .Help("How many logjam messages where missed by this importer")
        .Register(*client.registry);

    client.missed_msgs_total = &client.missed_msgs_total_family->Add({});

    client.parsed_msgs_total_family = &prometheus::BuildCounter()
        .Name("importer_msgs_parsed_total")
        .Help("How many logjam messages where parsed by this importer")
        .Register(*client.registry);

    client.parsed_msgs_total = &client.parsed_msgs_total_family->Add({});

    client.queued_updates_family = &prometheus::BuildGauge()
        .Name("importer_updates_queued")
        .Help("How many database updates are currently waiting to be processed by the importer")
        .Register(*client.registry);

    client.queued_updates = &client.queued_updates_family->Add({});

    client.queued_inserts_family = &prometheus::BuildGauge()
        .Name("importer_inserts_queued")
        .Help("How many database inserts are currently waiting to be processed by the importer")
        .Register(*client.registry);

    client.queued_inserts = &client.queued_inserts_family->Add({});

    // ask the exposer to scrape the registry on incoming scrapes
    client.exposer->RegisterCollectable(client.registry);
}

void prometheus_client_shutdown()
{
    delete client.exposer;
    client.exposer = NULL;
}

void prometheus_client_count(int metric, double value)
{
    switch (metric) {
    case IMPORTER_UPDATE_COUNT:
        client.updates_total->Increment(value);
        break;
    case IMPORTER_INSERT_COUNT:
        client.inserts_total->Increment(value);
        break;
    case IMPORTER_MSGS_RECEIVED_COUNT:
        client.received_msgs_total->Increment(value);
        break;
    case IMPORTER_MSGS_MISSED_COUNT:
        client.missed_msgs_total->Increment(value);
        break;
    case IMPORTER_MSGS_PARSED_COUNT:
        client.parsed_msgs_total->Increment(value);
        break;
    }
}

void prometheus_client_gauge(int metric, double value)
{
    switch (metric) {
    case IMPORTER_QUEUED_UPDATES_COUNT:
        client.queued_updates->Set(value);
        break;
    case IMPORTER_QUEUED_INSERTS_COUNT:
        client.queued_inserts->Set(value);
        break;
    }
}

void prometheus_client_timing(int metric, double value)
{
    switch (metric) {
    case IMPORTER_UPDATE_TIME:
        client.updates_seconds->Increment(value);
        break;
    case IMPORTER_INSERT_TIME:
        client.inserts_seconds->Increment(value);
        break;
    }
}
