#include "importer-controller.h"
#include "importer-streaminfo.h"
#include "importer-resources.h"
#include "importer-mongoutils.h"
#include "importer-processor.h"
#include "prometheus-client.h"
#include <getopt.h>

int snd_hwm = -1;
int rcv_hwm = -1;
int pull_port = -1;
int router_port = -1;
int sub_port = -1;
int metrics_port = -1;
char metrics_address[256] = {0};
char* live_stream_connection_spec = NULL;
char* prom_collector_connection_spec = NULL;
const char *metrics_ip = "127.0.0.1";
zlist_t *hosts = NULL;

static const char *subscription_pattern = NULL;
static const char *config_file_name = "logjam.conf";

FILE* frontend_timings = NULL;
static char *frontend_timings_file_name = NULL;
static char *frontend_timings_apdex_attr = NULL;

static char* num_subscribers_arg_value = NULL;
static char* num_parsers_arg_value = NULL;
static char* num_updaters_arg_value = NULL;
static char* num_writers_arg_value = NULL;
static size_t io_threads = 1;

static void setup_thread_counts(zconfig_t* config)
{
    if (!num_subscribers_arg_value)
        num_subscribers_arg_value = zconfig_resolve(config, "frontend/threads/subscribers", NULL);
    if (num_subscribers_arg_value)
        num_subscribers = strtoul(num_subscribers_arg_value, NULL, 0);

    if (!num_parsers_arg_value)
        num_parsers_arg_value = zconfig_resolve(config, "frontend/threads/parsers", NULL);
    if (num_parsers_arg_value)
        num_parsers = strtoul(num_parsers_arg_value, NULL, 0);

    if (!num_updaters_arg_value)
        num_updaters_arg_value = zconfig_resolve(config, "frontend/threads/updaters", NULL);
    if (num_updaters_arg_value)
        num_updaters = strtoul(num_updaters_arg_value, NULL, 0);

    if (!num_writers_arg_value)
        num_writers_arg_value = zconfig_resolve(config, "frontend/threads/writers", NULL);
    if (num_writers_arg_value)
        num_writers = strtoul(num_writers_arg_value, NULL, 0);
}

void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -a, --apdex-attribute A    frontend apdex attribute\n"
            "  -c, --config C             zeromq config file\n"
            "  -f, --frontend-log F       frontend timings log file\n"
            "  -h, --hosts H,I            specs of devices to connect to\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -l, --live-stream S        zmq bind spec for publishing live stream data\n"
            "  -p, --parsers N            number of parser threads\n"
            "  -b, --subscribers N        number of subscriber threads\n"
            "  -u, --updaters N           number of db stats updater threads\n"
            "  -q, --quiet                supress most output\n"
            "  -s, --subscribe S          only process streams with S as substring\n"
            "  -t, --router-port N        port number of zeromq router socket\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "  -w, --writers N            number of db request writer threads\n"
            "  -D, --device-port N        port for connecting to logjam devices\n"
            "  -N, --no-statsd            don't send statsd updates\n"
            "  -P, --input-port N         pull port for receiving logjam messages\n"
            "  -R, --rcv-hwm N            high watermark for input socket\n"
            "  -S, --snd-hwm N            high watermark for output socket\n"
            "  -m, --metrics-port N       port to use for prometheus path /metrics\n"
            "  -M, --metrics-ip N         ip for binding metrucs endpoint\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_DEVICES             specs of devices to connect to\n"
            "  LOGJAM_STREAM_FILTER       only process streams with given substring\n"
            "  LOGJAM_RCV_HWM             high watermark for input socket\n"
            "  LOGJAM_SND_HWM             high watermark for output socket\n"
            , argv[0]);
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    char *v;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "config",           required_argument, 0, 'c' },
        { "device-id",        required_argument, 0, 'd' },
        { "device-port",      required_argument, 0, 'D' },
        { "dryrun",           no_argument,       0, 'n' },
        { "help",             no_argument,       0,  0  },
        { "hosts",            required_argument, 0, 'h' },
        { "input-port",       required_argument, 0, 'P' },
        { "io-threads",       required_argument, 0, 'i' },
        { "live-stream",      required_argument, 0, 'l' },
        { "prom-export",      required_argument, 0, 'x' },
        { "no-statsd",        no_argument,       0, 'N' },
        { "output-port",      required_argument, 0, 'P' },
        { "quiet",            no_argument,       0, 'q' },
        { "rcv-hwm",          required_argument, 0, 'R' },
        { "router-port",      required_argument, 0, 't' },
        { "snd-hwm",          required_argument, 0, 'S' },
        { "subscribe",        required_argument, 0, 's' },
        { "subscribers",      required_argument, 0, 'b' },
        { "metrics-port",     required_argument, 0, 'm' },
        { "metrics-ip",       required_argument, 0, 'M' },
        { "verbose",          no_argument,       0, 'v' },
        { 0,                  0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "a:b:c:f:nm:p:qs:u:vw:x:i:P:R:S:l:h:D:t:NM:", long_options, &longindex)) != -1) {
        switch (c) {
        case 'n':
            dryrun = true;
            break;
        case 'v':
            if (verbose)
                debug = true;
            else
                verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'f':
            frontend_timings_file_name = optarg;
            break;
        case 's':
            subscription_pattern = optarg;
            break;
        case 'a':
            frontend_timings_apdex_attr = optarg;
            break;
        case 'p': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_PARSERS)
                num_parsers_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'p' cannot be larger than %d\n", MAX_PARSERS);
                exit(1);
            }
            break;
        }
        case 'u': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_UPDATERS)
                num_updaters_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'u' cannot be larger than %d\n", MAX_UPDATERS);
                exit(1);
            }
            break;
        }
        case 'w': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_UPDATERS)
                num_writers_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'w' cannot be larger than %d\n", MAX_UPDATERS);
                exit(1);
            }
            break;
        }
        case 'b': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_SUBSCRIBERS)
                num_subscribers_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'b' cannot be larger than %d\n", MAX_SUBSCRIBERS);
                exit(1);
            }
            break;
        }
        case 'M': {
            metrics_ip = optarg;
            break;
        }
        case 'h':
            hosts = split_delimited_string(optarg);
            if (zlist_size(hosts) == 0) {
                printf("[E] must specifiy at least one device to connect to\n");
                exit(1);
            }
            break;
        case 'i':
            io_threads = atoi(optarg);
            break;
        case 'R':
            rcv_hwm = atoi(optarg);
            break;
        case 'S':
            snd_hwm = atoi(optarg);
            break;
        case 'P':
            pull_port = atoi(optarg);
            break;
        case 't':
            router_port = atoi(optarg);
            break;
        case 'D':
            sub_port = atoi(optarg);
            break;
        case 'm':
            metrics_port = atoi(optarg);
            break;
        case 'N':
            send_statsd_msgs = false;
            break;
        case 'l':
            live_stream_connection_spec = augment_zmq_connection_spec(optarg, DEFAULT_LIVE_STREAM_PORT);
            break;
        case 'x':
            prom_collector_connection_spec = augment_zmq_connection_spec(optarg, DEFAULT_PROM_COLLECTOR_PORT);
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("acfpsuwiPRSlhD", optopt))
                fprintf(stderr, "[E] option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "[E] unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "[E] unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            fprintf(stderr, "BUG: can't process option -%c\n", optopt);
            exit(1);
        }
    }

    if (pull_port == -1)
        pull_port = DEFAULT_PULL_PORT;
    if (sub_port == -1)
        sub_port = DEFAULT_SUB_PORT;
    if (router_port == -1)
        router_port = DEFAULT_ROUTER_PORT;
    if (metrics_port == -1)
        metrics_port = DEFAULT_METRICS_PORT;

    if (hosts == NULL && (v = getenv("LOGJAM_DEVICES")))
        hosts = split_delimited_string(v);
    if (hosts != NULL)
        augment_zmq_connection_specs(&hosts, sub_port);

    if (subscription_pattern == NULL)
        subscription_pattern = getenv("LOGJAM_STREAM_FILTER");
    if (subscription_pattern == NULL)
        subscription_pattern = "";

    if (rcv_hwm == -1) {
        if (( v = getenv("LOGJAM_RCV_HWM") ))
            rcv_hwm = atoi(v);
        else
            rcv_hwm = DEFAULT_RCV_HWM;
    }

    if (snd_hwm == -1) {
        if (( v = getenv("LOGJAM_SND_HWM") ))
            snd_hwm = atoi(v);
        else
            snd_hwm = DEFAULT_SND_HWM;
    }
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    // setup frontend debug logging if requested
    if (frontend_timings_file_name) {
        frontend_timings = fopen(frontend_timings_file_name, "a");
        if (!frontend_timings) {
            fprintf(stderr, "[E] could not open frontend timings logfile: %s\n", strerror(errno));
            exit(1);
        }
    }
    if (frontend_timings_apdex_attr) {
        if (!processor_set_frontend_apdex_attribute(frontend_timings_apdex_attr)) {
            fprintf(stderr, "[E] invalid frontend apdex attribute name: %s\n", frontend_timings_apdex_attr);
            exit(1);
        }
    }

    // verify config file exists
    if (!zsys_file_exists(config_file_name)) {
        fprintf(stderr, "[E] missing config file: %s\n", config_file_name);
        exit(1);
    }
    config_file_init(config_file_name);
    config_update_date_info();

    // load config
    zconfig_t* config = zconfig_load((char*)config_file_name);
    // zconfig_print(config);

    if (live_stream_connection_spec == NULL)
        live_stream_connection_spec = zconfig_resolve(config, "frontend/endpoints/livestream/pub", DEFAULT_LIVE_STREAM_CONNECTION);

    if (prom_collector_connection_spec == NULL)
        prom_collector_connection_spec = zconfig_resolve(config, "frontend/endpoints/promcollector/pub", DEFAULT_PROM_COLLECTOR_CONNECTION);

    setup_thread_counts(config);

    if (!quiet)
        printf("[I] started %s\n"
               "[I] pull-port:     %d\n"
               "[I] sub-port:      %d\n"
               "[I] live-stream:   %s\n"
               "[I] io-threads:    %zu\n"
               "[I] rcv-hwm:       %d\n"
               "[I] snd-hwm:       %d\n"
               "[I] parsers:       %zu\n"
               "[I] writers:       %zu\n"
               "[I] updaters:      %zu\n"
               "[I] subscription:  %s\n"
               , argv[0], pull_port, sub_port, live_stream_connection_spec, io_threads, rcv_hwm, snd_hwm,
               num_parsers, num_writers, num_updaters, subscription_pattern);

    initialize_mongo_db_globals(config);
    snprintf(metrics_address, sizeof(metrics_address), "%s:%d", metrics_ip, metrics_port);
    prometheus_client_init(metrics_address);

    setup_resource_maps(config);
    setup_stream_config(config, subscription_pattern);

    return run_controller_loop(config, io_threads);
}
