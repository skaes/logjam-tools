#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "graylog-forwarder-prometheus-client.h"
#include <sys/resource.h>

typedef struct {
    prometheus::Counter *forwarded_msgs_total;
    prometheus::Counter *forwarded_bytes_total;
    prometheus::Counter *gelf_source_bytes_total;
    int64_t last_seen;
} stream_counters_t;

static std::mutex mutex;

static struct prometheus_client_t {
    prometheus::Exposer *exposer;
    std::shared_ptr<prometheus::Registry> registry;
    prometheus::Family<prometheus::Counter> *received_msgs_total_family;
    prometheus::Counter *received_msgs_total;
    prometheus::Family<prometheus::Counter> *received_bytes_total_family;
    prometheus::Counter *received_bytes_total;
    prometheus::Family<prometheus::Counter> *forwarded_msgs_total_family;
    prometheus::Family<prometheus::Counter> *forwarded_msgs_by_stream_total_family;
    prometheus::Counter *forwarded_msgs_total;
    prometheus::Family<prometheus::Counter> *forwarded_bytes_total_family;
    prometheus::Family<prometheus::Counter> *forwarded_bytes_by_stream_total_family;
    prometheus::Counter *forwarded_bytes_total;
    prometheus::Family<prometheus::Counter> *gelf_source_bytes_total_family;
    prometheus::Family<prometheus::Counter> *gelf_source_bytes_by_stream_total_family;
    prometheus::Counter *gelf_source_bytes_total;
    prometheus::Family<prometheus::Counter> *cpu_usage_total_family;
    prometheus::Counter *cpu_usage_total_subscriber;
    prometheus::Counter *cpu_usage_total_writer;
    std::vector<prometheus::Counter*> cpu_usage_total_parsers;
    std::unordered_map<std::string, stream_counters_t*> counters_by_stream_total_map;
    prometheus::Family<prometheus::Gauge> *sequence_number_family;
    std::unordered_map<uint32_t, prometheus::Gauge*> sequence_numbers;
} client;

void graylog_forwarder_prometheus_client_init(const char* address, int num_parsers)
{
    // create a http server running on the given address
    client.exposer = new prometheus::Exposer{address};
    // create a metrics registry
    client.registry = std::make_shared<prometheus::Registry>();

    client.received_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_received_total")
        .Help("How many logjam messages has this graylog_forwarder received")
        .Register(*client.registry);

    client.received_msgs_total = &client.received_msgs_total_family->Add({});

    client.received_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_received_bytes_total")
        .Help("How many bytes of logjam messages has this graylog_forwarder received")
        .Register(*client.registry);

    client.received_bytes_total = &client.received_bytes_total_family->Add({});

    client.forwarded_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_forwarded_total")
        .Help("How many graylog messages has this graylog_forwarder forwarded")
        .Register(*client.registry);

    client.forwarded_msgs_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_forwarded_by_stream_total")
        .Help("How many graylog messages has this graylog_forwarder forwarded for a specific stream")
        .Register(*client.registry);

    client.forwarded_msgs_total = &client.forwarded_msgs_total_family->Add({});

    client.forwarded_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_forwarded_bytes_total")
        .Help("How many bytes of graylog messages has this graylog_forwarder forwarded")
        .Register(*client.registry);

    client.forwarded_bytes_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:msgs_forwarded_bytes_by_stream_total")
        .Help("How many bytes of graylog messages has this graylog_forwarder forwarded for a specific stream")
        .Register(*client.registry);

    client.forwarded_bytes_total = &client.forwarded_bytes_total_family->Add({});

    client.gelf_source_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:gelf_source_bytes_total")
        .Help("How many bytes of gelf source has this graylog_forwarder produced")
        .Register(*client.registry);

    client.gelf_source_bytes_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:gelf_source_bytes_by_stream_total")
        .Help("How many bytes of gelf source has this graylog_forwarder produced for a specific stream")
        .Register(*client.registry);

    client.gelf_source_bytes_total = &client.gelf_source_bytes_total_family->Add({});

    client.cpu_usage_total_family = &prometheus::BuildCounter()
        .Name("logjam:graylog_forwarder:cpu_seconds_total")
        .Help("Sum of user and system CPU usage per thread")
        .Register(*client.registry);

    client.cpu_usage_total_subscriber = &client.cpu_usage_total_family->Add({{"thread", "subscriber"}});
    client.cpu_usage_total_writer = &client.cpu_usage_total_family->Add({{"thread", "writer"}});
    client.cpu_usage_total_parsers = {};
    for (int i=0; i<num_parsers; i++) {
        char name[256];
        snprintf(name, sizeof(name), "parser%d", i);
        client.cpu_usage_total_parsers.push_back(&client.cpu_usage_total_family->Add({{"thread", name}}));
    }

    client.sequence_number_family = &prometheus::BuildGauge()
        .Name("logjam:msgbus:sequence")
        .Help("Current sequence number for the given logjam device")
        .Register(*client.registry);

    // ask the exposer to scrape the registry on incoming scrapes
    client.exposer->RegisterCollectable(client.registry);
}

void graylog_forwarder_prometheus_client_shutdown()
{
    delete client.exposer;
    client.exposer = NULL;
}

void graylog_forwarder_prometheus_client_count_msgs_received(double value)
{
    client.received_msgs_total->Increment(value);
}

void graylog_forwarder_prometheus_client_count_bytes_received(double value)
{
    client.received_bytes_total->Increment(value);
}

void graylog_forwarder_prometheus_client_count_msgs_forwarded(double value)
{
    client.forwarded_msgs_total->Increment(value);
}

void graylog_forwarder_prometheus_client_count_bytes_forwarded(double value)
{
    client.forwarded_bytes_total->Increment(value);
}

void graylog_forwarder_prometheus_client_count_gelf_bytes(double value)
{
    client.gelf_source_bytes_total->Increment(value);
}

stream_counters_t * get_counter(const char* app_env)
{
    std::lock_guard<std::mutex> lock(mutex);
    std::string stream(app_env);
    std::unordered_map<std::string,stream_counters_t*>::const_iterator got = client.counters_by_stream_total_map.find(stream);
    stream_counters_t *counter;
    if (got == client.counters_by_stream_total_map.end()) {
        counter = new(stream_counters_t);
        counter->forwarded_msgs_total = &client.forwarded_msgs_by_stream_total_family->Add({{"stream", app_env}});
        counter->forwarded_bytes_total = &client.forwarded_bytes_by_stream_total_family->Add({{"stream", app_env}});
        counter->gelf_source_bytes_total = &client.gelf_source_bytes_by_stream_total_family->Add({{"stream", app_env}});
        client.counters_by_stream_total_map[stream] = counter;
    } else
        counter = got->second;
    return counter;
}

void graylog_forwarder_prometheus_client_count_msg_for_stream(const char* app_env)
{
    get_counter(app_env)->forwarded_msgs_total->Increment(1);
}

void graylog_forwarder_prometheus_client_count_forwarded_bytes_for_stream(const char* app_env, double value)
{
    get_counter(app_env)->forwarded_bytes_total->Increment(value);
}

void graylog_forwarder_prometheus_client_count_gelf_source_bytes_for_stream(const char* app_env, double value)
{
    get_counter(app_env)->gelf_source_bytes_total->Increment(value);
}

void graylog_forwarder_prometheus_client_delete_old_stream_counters(int64_t max_age)
{
    std::lock_guard<std::mutex> lock(mutex);
    int64_t threshold = zclock_time() - max_age;
    std::unordered_map<std::string,stream_counters_t*>::iterator it = client.counters_by_stream_total_map.begin();
    while (it != client.counters_by_stream_total_map.end()) {
        if (it->second->last_seen < threshold) {
            stream_counters_t *counter = it->second;
            client.forwarded_msgs_total_family->Remove(counter->forwarded_msgs_total);
            client.forwarded_bytes_total_family->Remove(counter->forwarded_bytes_total);
            client.gelf_source_bytes_total_family->Remove(counter->gelf_source_bytes_total);
            delete counter;
            it = client.counters_by_stream_total_map.erase(it);
        } else {
            it++;
        }
    }
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

void graylog_forwarder_prometheus_client_record_rusage_subscriber()
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_usage_total_subscriber->Value();
    client.cpu_usage_total_subscriber->Increment(value - oldvalue);
}

void graylog_forwarder_prometheus_client_record_rusage_parser(int i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_usage_total_parsers[i]->Value();
    client.cpu_usage_total_parsers[i]->Increment(value - oldvalue);
}

void graylog_forwarder_prometheus_client_record_rusage_writer()
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_usage_total_writer->Value();
    client.cpu_usage_total_writer->Increment(value - oldvalue);
}

void graylog_forwarder_prometheus_client_record_device_sequence_number(uint32_t id, const char* device, uint64_t n)
{
    if (id) {
        std::lock_guard<std::mutex> lock(mutex);
        std::unordered_map<uint32_t,prometheus::Gauge*>::const_iterator got = client.sequence_numbers.find(id);
        prometheus::Gauge* sequence_number;
        if (got == client.sequence_numbers.end()) {
            sequence_number = &client.sequence_number_family->Add({{"app", "logjam-graylog-forwarder"}, {"device", device}});
            client.sequence_numbers[id] = sequence_number;
        } else {
            sequence_number = got->second;
        }
        sequence_number->Set(n);
    }

}
