#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "prometheus/gauge.h"
#include "device-prometheus-client.h"
#include <sys/resource.h>

typedef struct {
    prometheus::Counter *counter;
    int64_t last_seen;
} stream_counter_t;

static struct prometheus_client_t {
    prometheus::Exposer *exposer;
    std::shared_ptr<prometheus::Registry> registry;
    prometheus::Family<prometheus::Counter> *received_msgs_total_family;
    prometheus::Counter *received_msgs_total;
    prometheus::Family<prometheus::Counter> *received_bytes_total_family;
    prometheus::Counter *received_bytes_total;
    prometheus::Family<prometheus::Counter> *compressed_msgs_total_family;
    prometheus::Counter *compressed_msgs_total;
    prometheus::Family<prometheus::Counter> *compressed_bytes_total_family;
    prometheus::Counter *compressed_bytes_total;
    prometheus::Family<prometheus::Counter> *invalid_msgs_total_family;
    prometheus::Counter *invalid_msgs_total;
    prometheus::Family<prometheus::Counter> *cpu_usage_total_family;
    prometheus::Counter *cpu_usage_total;
    std::vector<prometheus::Counter*> cpu_usage_total_compressors;
    prometheus::Family<prometheus::Counter> *ping_count_total_family;
    prometheus::Counter *ping_count_total;
    prometheus::Family<prometheus::Counter> *ping_count_by_stream_total_family;
    std::unordered_map<std::string, stream_counter_t*> ping_count_by_stream_total_map;
    prometheus::Family<prometheus::Counter> *broken_meta_count_total_family;
    prometheus::Counter *broken_meta_count_total;
    prometheus::Family<prometheus::Counter> *broken_meta_count_by_stream_total_family;
    std::unordered_map<std::string, stream_counter_t*> broken_meta_count_by_stream_total_map;
    prometheus::Family<prometheus::Gauge> *app_start_time_family;
    prometheus::Gauge *app_start_time;
} client;

void device_prometheus_client_init(const char* address, const char* device, int num_compressors)
{
    // create a http server running on the given address
    client.exposer = new prometheus::Exposer{address};
    // create a metrics registry
    client.registry = std::make_shared<prometheus::Registry>();

    client.received_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:msgs_received_total")
        .Help("How many logjam messages has this device received")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.received_msgs_total = &client.received_msgs_total_family->Add({});

    client.invalid_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:msgs_invalid_total")
        .Help("How many broken logjam messages have reached this device")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.invalid_msgs_total = &client.invalid_msgs_total_family->Add({});

    client.received_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:msgs_received_bytes_total")
        .Help("How many bytes of logjam messages has this device received")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.received_bytes_total = &client.received_bytes_total_family->Add({});

    client.compressed_msgs_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:msgs_compressed_total")
        .Help("How many logjam messages has this device compressed")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.compressed_msgs_total = &client.compressed_msgs_total_family->Add({});

    client.compressed_bytes_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:msgs_compressed_bytes_total")
        .Help("How many bytes of logjam messages has this device compressed")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.compressed_bytes_total = &client.compressed_bytes_total_family->Add({});

    client.cpu_usage_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:cpu_seconds_total")
        .Help("Sum of user and system CPU usage per thread")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.cpu_usage_total = &client.cpu_usage_total_family->Add({{"thread", "main"}});
    client.cpu_usage_total_compressors = {};
    for (int i=0; i<num_compressors; i++) {
        char name[256];
        sprintf(name, "compressor%d", i);
        client.cpu_usage_total_compressors.push_back(&client.cpu_usage_total_family->Add({{"thread", name}}));
    }

    client.ping_count_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:ping_count_total")
        .Help("How many ping requests this device has processed")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.ping_count_total = &client.ping_count_total_family->Add({});

    client.ping_count_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:ping_count_by_stream_total")
        .Help("How many ping requests this device has processed for a given stream")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.broken_meta_count_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:broken_meta_count_total")
        .Help("How many requests with broken meta information this device has processed")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.broken_meta_count_total = &client.broken_meta_count_total_family->Add({});

    client.broken_meta_count_by_stream_total_family = &prometheus::BuildCounter()
        .Name("logjam:device:broken_meta_count_by_stream_total")
        .Help("How many requests with broken meta information this device has processed for a given stream")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.app_start_time_family = &prometheus::BuildGauge()
        .Name("logjam:device:app_start_time")
        .Help("Timestamp when the device started")
        .Labels({{"device", device}})
        .Register(*client.registry);

    client.app_start_time = &client.app_start_time_family->Add({});

    // ask the exposer to scrape the registry on incoming scrapes
    client.exposer->RegisterCollectable(client.registry);
}

void device_prometheus_client_shutdown()
{
    delete client.exposer;
    client.exposer = NULL;
}

void device_prometheus_client_count_msgs_received(double value)
{
    client.received_msgs_total->Increment(value);
}

void device_prometheus_client_count_bytes_received(double value)
{
    client.received_bytes_total->Increment(value);
}

void device_prometheus_client_count_msgs_compressed(double value)
{
    client.compressed_msgs_total->Increment(value);
}

void device_prometheus_client_count_bytes_compressed(double value)
{
    client.compressed_bytes_total->Increment(value);
}

void device_prometheus_client_count_invalid_messages(double value)
{
    client.invalid_msgs_total->Increment(value);
}

void device_prometheus_client_count_pings(double value)
{
    client.ping_count_total->Increment(value);
}

void device_prometheus_client_count_ping(const char* app_env)
{
    std::string stream(app_env);
    std::unordered_map<std::string,stream_counter_t*>::const_iterator got = client.ping_count_by_stream_total_map.find(stream);
    stream_counter_t *counter;
    if (got == client.ping_count_by_stream_total_map.end()) {
        counter = new(stream_counter_t);
        counter->counter = &client.ping_count_by_stream_total_family->Add({{"stream", app_env}});
        client.ping_count_by_stream_total_map[stream] = counter;
    } else
        counter = got->second;
    counter->counter->Increment(1);
    counter->last_seen = zclock_time();
}

void device_prometheus_client_delete_old_ping_counters(int64_t max_age)
{
    int64_t threshold = zclock_time() - max_age;
    std::unordered_map<std::string,stream_counter_t*>::iterator it = client.ping_count_by_stream_total_map.begin();
    while (it != client.ping_count_by_stream_total_map.end()) {
        if (it->second->last_seen < threshold) {
            stream_counter_t *counter = it->second;
            client.ping_count_by_stream_total_family->Remove(counter->counter);
            delete counter;
            it = client.ping_count_by_stream_total_map.erase(it);
        } else {
            it++;
        }
    }
}

void device_prometheus_client_count_broken_metas(double value)
{
    client.broken_meta_count_total->Increment(value);
}

void device_prometheus_client_count_broken_meta(const char* app_env)
{
    std::string stream(app_env);
    std::unordered_map<std::string,stream_counter_t*>::const_iterator got = client.broken_meta_count_by_stream_total_map.find(stream);
    stream_counter_t *counter;
    if (got == client.broken_meta_count_by_stream_total_map.end()) {
        counter = new(stream_counter_t);
        counter->counter = &client.broken_meta_count_by_stream_total_family->Add({{"stream", app_env}});
        client.broken_meta_count_by_stream_total_map[stream] = counter;
    } else
        counter = got->second;
    counter->counter->Increment(1);
    counter->last_seen = zclock_time();
}

void device_prometheus_client_delete_old_broken_meta_counters(int64_t max_age)
{
    int64_t threshold = zclock_time() - max_age;
    std::unordered_map<std::string,stream_counter_t*>::iterator it = client.broken_meta_count_by_stream_total_map.begin();
    while (it != client.broken_meta_count_by_stream_total_map.end()) {
        if (it->second->last_seen < threshold) {
            stream_counter_t *counter = it->second;
            client.broken_meta_count_by_stream_total_family->Remove(counter->counter);
            delete counter;
            it = client.broken_meta_count_by_stream_total_map.erase(it);
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

void device_prometheus_client_record_rusage()
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_usage_total->Value();
    client.cpu_usage_total->Increment(value - oldvalue);
}

void device_prometheus_client_record_rusage_compressor(int i)
{
    double value = get_combined_cpu_usage();
    double oldvalue = client.cpu_usage_total_compressors[i]->Value();
    client.cpu_usage_total_compressors[i]->Increment(value - oldvalue);
}

void device_prometheus_set_start_time() 
{
    client.app_start_time->SetToCurrentTime();
}
