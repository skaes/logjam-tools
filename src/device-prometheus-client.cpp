#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include "device-prometheus-client.h"

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
} client;

void device_prometheus_client_init(const char* address, const char* device)
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
