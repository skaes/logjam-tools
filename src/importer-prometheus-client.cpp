#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "importer-prometheus-client.h"
#include <sys/resource.h>

static struct prometheus_client_t {
    prometheus::Exposer *exposer;
    std::shared_ptr<prometheus::Registry> registry;
    prometheus::Family<prometheus::Counter> *updates_total_family;
    prometheus::Counter *updates_total;
    prometheus::Family<prometheus::Counter> *updates_seconds_family;
    prometheus::Counter *updates_seconds;
    prometheus::Family<prometheus::Counter> *inserts_total_family;
    prometheus::Family<prometheus::Counter> *inserts_throttled_total_family;
    prometheus::Counter *inserts_total;
    prometheus::Counter *inserts_throttled_total;
    prometheus::Family<prometheus::Counter> *inserts_by_stream_total_family;
    prometheus::Family<prometheus::Counter> *inserts_throttled_by_stream_total_family;
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
    std::vector<prometheus::Counter*> cpu_seconds_total_subscribers;
    std::vector<prometheus::Counter*> cpu_seconds_total_parsers;
    std::vector<prometheus::Counter*> cpu_seconds_total_writers;
    std::vector<prometheus::Counter*> cpu_seconds_total_updaters;
    prometheus::Family<prometheus::Counter> *cpu_seconds_total_family;
} client;

void importer_prometheus_client_init(const char* address, importer_prometheus_client_params_t params)
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

    client.inserts_throttled_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_throttled_total")
        .Help("How many inserts has this importer rejected")
        .Register(*client.registry);

    client.inserts_throttled_total = &client.inserts_throttled_total_family->Add({});

    client.inserts_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_by_stream_total")
        .Help("How many inserts has this importer performed for the given stream")
        .Register(*client.registry);

    client.inserts_throttled_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:inserts_throttled_by_stream_total")
        .Help("How many inserts has this importer rejected for the given stream")
        .Register(*client.registry);

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

    client.cpu_seconds_total_family = &prometheus::BuildCounter()
        .Name("logjam:importer:cpu_seconds_total")
        .Help("How many CPU seconds importer threads have used")
        .Register(*client.registry);

    for (uint i=0; i<params.num_subscribers; i++) {
        char name[256];
        sprintf(name, "subscriber%d", i);
        client.cpu_seconds_total_subscribers.push_back(&client.cpu_seconds_total_family->Add({{"thread", name}}));
    }
    for (uint i=0; i<params.num_parsers; i++) {
        char name[256];
        sprintf(name, "parser%d", i);
        client.cpu_seconds_total_parsers.push_back(&client.cpu_seconds_total_family->Add({{"thread", name}}));
    }
    for (uint i=0; i<params.num_writers; i++) {
        char name[256];
        sprintf(name, "writer%d", i);
        client.cpu_seconds_total_writers.push_back(&client.cpu_seconds_total_family->Add({{"thread", name}}));
    }
    for (uint i=0; i<params.num_updaters; i++) {
        char name[256];
        sprintf(name, "updater%d", i);
        client.cpu_seconds_total_updaters.push_back(&client.cpu_seconds_total_family->Add({{"thread", name}}));
    }

    // ask the exposer to scrape the registry on incoming scrapes
    client.exposer->RegisterCollectable(client.registry);
}

void importer_prometheus_client_shutdown()
{
    delete client.exposer;
    client.exposer = NULL;
}

void importer_prometheus_client_count_updates(double value)
{
    client.updates_total->Increment(value);
}

void importer_prometheus_client_count_inserts(double value)
{
    client.inserts_total->Increment(value);
}

void importer_prometheus_client_count_msgs_received(double value)
{
    client.received_msgs_total->Increment(value);
}

void importer_prometheus_client_count_bytes_received(double value)
{
    client.received_bytes_total->Increment(value);
}

void importer_prometheus_client_count_msgs_missed(double value)
{
    client.missed_msgs_total->Increment(value);
}

void importer_prometheus_client_count_msgs_blocked(double value)
{
    client.blocked_msgs_total->Increment(value);
}

void importer_prometheus_client_count_msgs_dropped(double value)
{
    client.dropped_msgs_total->Increment(value);
}

void importer_prometheus_client_count_updates_blocked(double value)
{
    client.blocked_updates_total->Increment(value);
}

void importer_prometheus_client_count_msgs_parsed(double value)
{
    client.parsed_msgs_total->Increment(value);
}

void importer_prometheus_client_gauge_queued_updates(double value)
{
    client.queued_updates->Set(value);
}

void importer_prometheus_client_gauge_queued_inserts(double value)
{
    client.queued_inserts->Set(value);
}

void importer_prometheus_client_time_updates(double value)
{
    client.updates_seconds->Increment(value);
}

void importer_prometheus_client_time_inserts(double value)
{
    client.inserts_seconds->Increment(value);
}

void importer_prometheus_client_count_inserts_failed(double value)
{
    client.failed_inserts_total->Increment(value);
}

static
double get_combined_cpu_usage()
{
    struct rusage usage;
    int rc;
#ifdef RUSAGE_THREAD
    rc = getrusage(RUSAGE_THREAD, &usage);
#else
    rc = getrusage(RUSAGE_SELF, &usage);
#endif
    if (rc) return 0;

    struct timeval total;
    timeradd(&usage.ru_utime, &usage.ru_stime, &total);
    return total.tv_sec + (double)total.tv_usec/1000000;
}

void importer_prometheus_client_record_rusage_subscriber(uint i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_seconds_total_subscribers[i]->Value();
    client.cpu_seconds_total_subscribers[i]->Increment(value - oldvalue);
}

void importer_prometheus_client_record_rusage_parser(uint i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_seconds_total_parsers[i]->Value();
    client.cpu_seconds_total_parsers[i]->Increment(value - oldvalue);
}

void importer_prometheus_client_record_rusage_writer(uint i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_seconds_total_writers[i]->Value();
    client.cpu_seconds_total_writers[i]->Increment(value - oldvalue);
}

void importer_prometheus_client_record_rusage_updater(uint i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_seconds_total_updaters[i]->Value();
    client.cpu_seconds_total_updaters[i]->Increment(value - oldvalue);
}

// caller must hold lock on stream
void importer_prometheus_client_create_stream_counters(stream_info_t *stream)
{
    stream->inserts_total = &client.inserts_by_stream_total_family->Add({{"stream", stream->key}});
    stream->inserts_throttled_total = &client.inserts_throttled_by_stream_total_family->Add({{"stream", stream->key}});
}

// caller must hold lock on stream
void importer_prometheus_client_destroy_stream_counters(stream_info_t *stream)
{
    prometheus::Counter* counter = (prometheus::Counter*)stream->inserts_total;
    client.inserts_by_stream_total_family->Remove(counter);
    delete counter;
    counter = (prometheus::Counter*)stream->inserts_throttled_total;
    client.inserts_throttled_by_stream_total_family->Remove(counter);
    delete counter;
}

// caller must hold lock on stream
void importer_prometheus_client_count_inserts_for_stream(stream_info_t *stream, double value)
{
    ((prometheus::Counter*)stream->inserts_total)->Increment(value);
}

// caller must hold lock on stream
void importer_prometheus_client_count_throttled_inserts_for_stream(stream_info_t *stream, double value)
{
    client.inserts_throttled_total->Increment(value);
    ((prometheus::Counter*)stream->inserts_throttled_total)->Increment(value);
}
