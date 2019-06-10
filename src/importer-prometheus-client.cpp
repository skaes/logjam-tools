#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "importer-prometheus-client.h"

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
    prometheus::Family<prometheus::Counter> *received_bytes_total_family;
    prometheus::Counter *received_bytes_total;
    prometheus::Family<prometheus::Counter> *missed_msgs_total_family;
    prometheus::Counter *missed_msgs_total;
    prometheus::Family<prometheus::Counter> *dropped_msgs_total_family;
    prometheus::Counter *dropped_msgs_total;
    prometheus::Family<prometheus::Counter> *blocked_msgs_total_family;
    prometheus::Counter *blocked_msgs_total;
    prometheus::Family<prometheus::Counter> *parsed_msgs_total_family;
    prometheus::Counter *parsed_msgs_total;
    prometheus::Family<prometheus::Gauge> *queued_updates_family;
    prometheus::Gauge *queued_updates;
    prometheus::Family<prometheus::Gauge> *queued_inserts_family;
    prometheus::Gauge *queued_inserts;
    prometheus::Counter *blocked_updates_total;
    prometheus::Family<prometheus::Counter> *blocked_updates_total_family;
    prometheus::Counter *failed_inserts_total;
    prometheus::Family<prometheus::Counter> *failed_inserts_total_family;
} client;

void prometheus_client_init(const char* address)
{
    // create a http server running on the given address
    client.exposer = new prometheus::Exposer{address};
    // create a metrics registry
    client.registry = std::make_shared<prometheus::Registry>();

    // declare metrics families, counters and gauges
    client.updates_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:updates_total")
        .Help("How many updates has this importer performed")
        .Register(*client.registry);

    client.updates_total = &client.updates_total_family->Add({});

    client.updates_seconds_family = &prometheus::BuildCounter()
        .Name("logjam:importer:updates_seconds")
        .Help("How many seconds has this importer spent updating")
        .Register(*client.registry);

    client.updates_seconds = &client.updates_seconds_family->Add({});

    client.inserts_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_total")
        .Help("How many inserts has this importer performed")
        .Register(*client.registry);

    client.inserts_total = &client.inserts_total_family->Add({});

    client.inserts_seconds_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_seconds")
        .Help("How many seconds has this imorter spent inserting")
        .Register(*client.registry);

    client.inserts_seconds = &client.inserts_seconds_family->Add({});

    client.received_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_received_total")
        .Help("How many logjam messages has this importer received")
        .Register(*client.registry);

    client.received_msgs_total = &client.received_msgs_total_family->Add({});

    client.received_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_received_bytes_total")
        .Help("How many bytes of logjam messages has this importer received")
        .Register(*client.registry);

    client.received_bytes_total = &client.received_bytes_total_family->Add({});

    client.missed_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_missed_total")
        .Help("How many logjam messages were missed by this importer")
        .Register(*client.registry);

    client.missed_msgs_total = &client.missed_msgs_total_family->Add({});

    client.dropped_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_dropped_total")
        .Help("How many logjam messages were dropped by this importer")
        .Register(*client.registry);

    client.dropped_msgs_total = &client.dropped_msgs_total_family->Add({});

    client.blocked_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_blocked_total")
        .Help("How many logjam messages caused the importer to block")
        .Register(*client.registry);

    client.blocked_msgs_total = &client.blocked_msgs_total_family->Add({});

    client.parsed_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:msgs_parsed_total")
        .Help("How many logjam messages were parsed by this importer")
        .Register(*client.registry);

    client.parsed_msgs_total = &client.parsed_msgs_total_family->Add({});

    client.queued_updates_family = &prometheus::BuildGauge()
        .Name("logjam:importer:updates_queued")
        .Help("How many database updates are currently waiting to be processed by the importer")
        .Register(*client.registry);

    client.queued_updates = &client.queued_updates_family->Add({});

    client.queued_inserts_family = &prometheus::BuildGauge()
        .Name("logjam:importer:inserts_queued")
        .Help("How many database inserts are currently waiting to be processed by the importer")
        .Register(*client.registry);

    client.queued_inserts = &client.queued_inserts_family->Add({});

    client.blocked_updates_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:updates_blocked_total")
        .Help("How many update msgs caused the importer controller to block")
        .Register(*client.registry);

    client.blocked_updates_total = &client.blocked_updates_total_family->Add({});

    client.failed_inserts_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_failed_total")
        .Help("How many update database inserts failed")
        .Register(*client.registry);

    client.failed_inserts_total = &client.failed_inserts_total_family->Add({});


    // ask the exposer to scrape the registry on incoming scrapes
    client.exposer->RegisterCollectable(client.registry);
}

void prometheus_client_shutdown()
{
    delete client.exposer;
    client.exposer = NULL;
}

void prometheus_client_count_updates(double value)
{
    client.updates_total->Increment(value);
}

void prometheus_client_count_inserts(double value)
{
    client.inserts_total->Increment(value);
}

void prometheus_client_count_msgs_received(double value)
{
    client.received_msgs_total->Increment(value);
}

void prometheus_client_count_bytes_received(double value)
{
    client.received_bytes_total->Increment(value);
}

void prometheus_client_count_msgs_missed(double value)
{
    client.missed_msgs_total->Increment(value);
}

void prometheus_client_count_msgs_blocked(double value)
{
    client.blocked_msgs_total->Increment(value);
}

void prometheus_client_count_msgs_dropped(double value)
{
    client.dropped_msgs_total->Increment(value);
}

void prometheus_client_count_updates_blocked(double value)
{
    client.blocked_updates_total->Increment(value);
}

void prometheus_client_count_msgs_parsed(double value)
{
    client.parsed_msgs_total->Increment(value);
}

void prometheus_client_gauge_queued_updates(double value)
{
    client.queued_updates->Set(value);
}

void prometheus_client_gauge_queued_inserts(double value)
{
    client.queued_inserts->Set(value);
}

void prometheus_client_time_updates(double value)
{
    client.updates_seconds->Increment(value);
}

void prometheus_client_time_inserts(double value)
{
    client.inserts_seconds->Increment(value);
}

void prometheus_client_count_inserts_failed(double value)
{
    client.failed_inserts_total->Increment(value);
}
