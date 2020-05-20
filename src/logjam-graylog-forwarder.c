#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <getopt.h>
#include "graylog-forwarder-common.h"
#include "graylog-forwarder-controller.h"
#include "graylog-forwarder-prometheus-client.h"

// flags
bool dryrun = false;
bool verbose = false;
bool debug = false;
bool quiet = false;

#define DEFAULT_ABORT_AFTER 60
int heartbeat_abort_after = -1;

// global config
static zconfig_t* config = NULL;
static zfile_t *config_file = NULL;
static char *config_file_name = "logjam.conf";
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";
static const char *logjam_url = "http://localhost:3000/";
static char *logjam_stream_url = "http://localhost:3000/admin/streams";
static const char* subscription_pattern = NULL;

const char* default_datacenter = "unknown";

int metrics_port = 8083;
char metrics_address[256] = {0};
const char *metrics_ip = "0.0.0.0";

static void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [options]\n"
            "\nOptions:\n"
            "  -c, --config F             read config from file\n"
            "  -e, --subscribe S,T        subscription patterns\n"
            "  -h, --hosts H,I            specs of devices to connect to\n"
            "  -i, --interface I          zmq spec of interface on which to listen\n"
            "  -n, --dryrun               don't send data to graylog\n"
            "  -p, --parsers N            use N threads for parsing log messages\n"
            "  -z, --compress             compress data sent to graylog\n"
            "  -R, --rcv-hwm N            high watermark for input socket\n"
            "  -S, --snd-hwm N            high watermark for output socket\n"
            "  -L, --logjam-url U         url from where to retrieve stream config\n"
            "  -A, --abort                abort after missing heartbeats for this many seconds\n"
            "  -d, --datacenter           assume this datacenter for messages with a datacenter field\n"
            "  -m, --metrics-port N       port to use for prometheus path /metrics\n"
            "  -M, --metrics-ip N         ip for binding metrics endpoint\n"
            "  -T, --trim-frequency N     malloc trim freqency in seconds, 0 means no trimming\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_DEVICES             specs of devices to connect to\n"
            "  LOGJAM_SUBSCRIPTIONS       subscription patterns\n"
            "  LOGJAM_RCV_HWM             high watermark for input socket\n"
            "  LOGJAM_SND_HWM             high watermark for output socket\n"
            "  LOGJAM_INTERFACE           zmq spec of interface on which to liste\n"
            "  LOGJAM_ABORT_AFTER         abort after missing heartbeats for this many seconds\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    char *v;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "compress",       no_argument,       0, 'z' },
        { "config",         required_argument, 0, 'c' },
        { "dryrun",         no_argument,       0, 'n' },
        { "help",           no_argument,       0,  0  },
        { "hosts",          required_argument, 0, 'h' },
        { "interface",      required_argument, 0, 'i' },
        { "parsers",        required_argument, 0, 'p' },
        { "quiet",          no_argument,       0, 'q' },
        { "rcv-hwm",        required_argument, 0, 'R' },
        { "snd-hwm",        required_argument, 0, 'S' },
        { "subscribe",      required_argument, 0, 'e' },
        { "verbose",        no_argument,       0, 'v' },
        { "logjam-url",     required_argument, 0, 'L' },
        { "abort",          required_argument, 0, 'A' },
        { "datacenter",     required_argument, 0, 'd' },
        { "metrics-port",   required_argument, 0, 'm' },
        { "metrics-ip",     required_argument, 0, 'M' },
        { "trim-frequency", required_argument, 0, 'T' },
        { 0,                0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqc:np:zh:S:R:e:L:A:d:m:M:T:", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug= true;
            else
                verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'd':
            default_datacenter = optarg;
            break;
        case 'n':
            dryrun = true;
            break;
        case 'm':
            metrics_port = atoi(optarg);
            break;
        case 'M': {
            metrics_ip = optarg;
            break;
        }
        case 'z':
            compress_gelf = true;
            break;
        case 'p': {
            unsigned int n = strtoul(optarg, NULL, 0);
            if (n <= MAX_PARSERS)
                num_parsers = n;
            else {
                fprintf(stderr, "parameter value 'p' can not be greater than %d\n", MAX_PARSERS);
                exit(1);
            }
            break;
        }
        case 'h':
            hosts = split_delimited_string(optarg);
            if (hosts == NULL || zlist_size(hosts) == 0) {
                printf("[E] must specify at least one device to connect to\n");
                exit(1);
            }
            break;
        case 'i':
            interface = optarg;
            break;
        case 'e':
            subscription_pattern = optarg;
            break;
        case 'R':
            rcv_hwm = atoi(optarg);
            break;
        case 'S':
            snd_hwm = atoi(optarg);
            break;
        case 'T':
            malloc_trim_frequency = atoi(optarg);
            break;
        case 'L':
            logjam_url = optarg;
            break;
        case 'A':
            heartbeat_abort_after = atoi(optarg);
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (optopt == 'c' || optopt == 'p')
                fprintf(stderr, "option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            fprintf(stderr, "BUG: can't process option -%c\n", optopt);
            exit(1);
        }
    }

    if (hosts == NULL)
        hosts = split_delimited_string(getenv("LOGJAM_DEVICES"));

    if (hosts)
        augment_zmq_connection_specs(&hosts, 9606);

    if (interface == NULL)
        interface = getenv("LOGJAM_INTERACE");
    if (interface)
        interface = augment_zmq_connection_spec(interface, 9610);

    if (subscription_pattern == NULL)
        subscription_pattern = getenv("LOGJAM_SUBSCRIPTIONS");
    if (subscription_pattern == NULL)
        subscription_pattern = "";

    if (rcv_hwm == -1 && (v = getenv("LOGJAM_RCV_HWM")))
        rcv_hwm = atoi(v);

    if (snd_hwm == -1 && (v = getenv("LOGJAM_SND_HWM")))
        snd_hwm = atoi(v);

    int l = strlen(logjam_url);
    int n = asprintf(&logjam_stream_url, "%s%s", logjam_url, (logjam_url[l-1] == '/') ? "admin/streams" : "/admin/streams");
    assert(n>0);

    if (heartbeat_abort_after == -1) {
        if (( v = getenv("LOGJAM_ABORT_AFTER") ))
            heartbeat_abort_after = atoi(v);
        else
            heartbeat_abort_after = DEFAULT_ABORT_AFTER;
    }
}

static void config_file_init()
{
    config_file = zfile_new(NULL, config_file_name);
    config_file_last_modified = zfile_modified(config_file);
    config_file_digest = strdup(zfile_digest(config_file));
}

int main(int argc, char * const *argv)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    // load config
    if (!zsys_file_exists(config_file_name)) {
        fprintf(stderr, "[W] missing config file: %s\n", config_file_name);
        config = zconfig_new("", NULL);
    } else if (zsys_file_exists(config_file_name)) {
        // load config
        config_file_init();
        config = zconfig_load((char*)config_file_name);
    }

    // initalize prometheus client
    snprintf(metrics_address, sizeof(metrics_address), "%s:%d", metrics_ip, metrics_port);
    graylog_forwarder_prometheus_client_init(metrics_address, num_parsers);

    // convert config file to list of devices if none where specified as a parameter or env variable
    if (hosts == NULL || zlist_size(hosts) == 0) {
        if (hosts == NULL)
            hosts = zlist_new();
        zconfig_t *endpoints = zconfig_locate(config, "/logjam/endpoints");
        if (!endpoints) {
            zlist_append(hosts, "tcp://localhost:9606");
        } else {
            zconfig_t *endpoint = zconfig_child(endpoints);
            while (endpoint) {
                char *spec = zconfig_value(endpoint);
                char *new_spec = augment_zmq_connection_spec(spec, 9606);
                zlist_append(hosts, new_spec);
                endpoint = zconfig_next(endpoint);
            }
        }
    }

    // configure graylog endpoint
    if (interface == NULL)
        interface = zconfig_resolve(config, "/graylog/endpoint", DEFAULT_INTERFACE);

    // set inbound high-water-mark
    if (rcv_hwm == -1)
        rcv_hwm = atoi(zconfig_resolve(config, "/logjam/high_water_mark", DEFAULT_RCV_HWM_STR));

    // set outbound high-water-mark
    if (snd_hwm == -1)
        snd_hwm = atoi(zconfig_resolve(config, "/graylog/high_water_mark", DEFAULT_SND_HWM_STR));

    if (!quiet)
        printf("[I] started %s\n"
               "[I] interface %s\n"
               "[I] rcv-hwm:  %d\n"
               "[I] snd-hwm:  %d\n"
               "[I] abort:  %d\n"
               , argv[0], interface, rcv_hwm, snd_hwm, heartbeat_abort_after);

    int rc = graylog_forwarder_run_controller_loop(config, hosts, subscription_pattern, logjam_stream_url, rcv_hwm, snd_hwm, heartbeat_abort_after);

    graylog_forwarder_prometheus_client_shutdown();

    return rc;
}
