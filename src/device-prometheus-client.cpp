#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "device-prometheus-client.h"
#include <sys/resource.h>

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
    prometheus::Family<prometheus::Counter> *cpu_usage_total_family;
    prometheus::Counter *cpu_usage_total;
    std::vector<prometheus::Counter*> cpu_usage_total_compressors;
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
