#include <prometheus-client.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>

static struct prometheus_client_t {
    prometheus::Exposer *exposer;
    std::shared_ptr<prometheus::Registry> registry;
    prometheus::Family<prometheus::Counter> *update_count_family;
    prometheus::Family<prometheus::Counter> *update_time_family;
    prometheus::Counter *update_count;
    prometheus::Counter *update_time;
    prometheus::Family<prometheus::Counter> *insert_count_family;
    prometheus::Family<prometheus::Counter> *insert_time_family;
    prometheus::Counter *insert_count;
    prometheus::Counter *insert_time;
    prometheus::Family<prometheus::Counter> *received_count_family;
    prometheus::Counter *received_count;
    prometheus::Family<prometheus::Counter> *missed_count_family;
    prometheus::Counter *missed_count;
    prometheus::Family<prometheus::Counter> *parsed_count_family;
    prometheus::Counter *parsed_count;
    prometheus::Family<prometheus::Gauge> *queued_updates_count_family;
    prometheus::Gauge *queued_updates_count;
    prometheus::Family<prometheus::Gauge> *queued_inserts_count_family;
    prometheus::Gauge *queued_inserts_count;
} client;

void prometheus_client_init(const char* address)
{
    // create a http server running on port 8080
    client.exposer = new prometheus::Exposer{address};
    // create a metrics registry with component=main labels applied to
    // all its metrics
    client.registry = std::make_shared<prometheus::Registry>();

    client.update_count_family = &prometheus::BuildCounter()
        .Name("importer_updates_count")
        .Help("How many updates is this server processing")
        .Register(*client.registry);

    client.update_count = &client.update_count_family->Add({});

    client.update_time_family = &prometheus::BuildCounter()
        .Name("importer_updates_time")
        .Help("How many seconds is this server running?")
        .Register(*client.registry);

    client.update_time = &client.update_time_family->Add({});

    client.insert_count_family = &prometheus::BuildCounter()
        .Name("importer_inserts_count")
        .Help("How many inserts is this server processing")
        .Register(*client.registry);

    client.insert_count = &client.insert_count_family->Add({});

    client.insert_time_family = &prometheus::BuildCounter()
        .Name("importer_inserts_time")
        .Help("How many seconds is this server running?")
        .Register(*client.registry);

    client.insert_time = &client.insert_time_family->Add({});

    client.received_count_family = &prometheus::BuildCounter()
        .Name("importer_msgs_received_count")
        .Help("How many receiveds is this server processing")
        .Register(*client.registry);

    client.received_count = &client.received_count_family->Add({});

    client.missed_count_family = &prometheus::BuildCounter()
        .Name("importer_msgs_missed_count")
        .Help("How many log messages where missed on the bus")
        .Register(*client.registry);

    client.missed_count = &client.missed_count_family->Add({});

    client.parsed_count_family = &prometheus::BuildCounter()
        .Name("importer_msgs_parsed_count")
        .Help("How many messages where parsed by the importer")
        .Register(*client.registry);

    client.parsed_count = &client.parsed_count_family->Add({});

    client.queued_updates_count_family = &prometheus::BuildGauge()
        .Name("importer_updates_queued_count")
        .Help("How many database updates are currently queued")
        .Register(*client.registry);

    client.queued_updates_count = &client.queued_updates_count_family->Add({});

    client.queued_inserts_count_family = &prometheus::BuildGauge()
        .Name("importer_inserts_queued_count")
        .Help("How many database inserts are currently queued")
        .Register(*client.registry);

    client.queued_inserts_count = &client.queued_inserts_count_family->Add({});

    // ask the exposer to scrape the registry on incoming scrapes
    // client.exposer->RegisterCollectable(client.registry);
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
        client.update_count->Increment(value);
    case IMPORTER_INSERT_COUNT:
        client.insert_count->Increment(value);
    case IMPORTER_MSGS_RECEIVED_COUNT:
        client.received_count->Increment(value);
    case IMPORTER_MSGS_MISSED_COUNT:
        client.missed_count->Increment(value);
    case IMPORTER_MSGS_PARSED_COUNT:
        client.parsed_count->Increment(value);
    }
}

void prometheus_client_gauge(int metric, double value)
{
    switch (metric) {
    case IMPORTER_QUEUED_UPDATES_COUNT:
        client.queued_updates_count->Increment(value);
    case IMPORTER_QUEUED_INSERTS_COUNT:
        client.queued_inserts_count->Increment(value);
    }
}

void prometheus_client_timing(int metric, double value)
{
    switch (metric) {
    case IMPORTER_UPDATE_TIME:
        client.update_time->Increment(value);
    case IMPORTER_INSERT_TIME:
        client.insert_time->Increment(value);
    }
}
