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

// global config
static zconfig_t* config = NULL;
static zfile_t *config_file = NULL;
static char *config_file_name = "logjam.conf";
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";

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
            "  -z, --compress M           compress data sent to graylog\n"
            "  -R, --rcv-hwm N            high watermark for input socket\n"
            "  -S, --snd-hwm N            high watermark for output socket\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_DEVICES             specs of devices to connect to\n"
            "  LOGJAM_SUBSCRIPTIONS       subscription patterns\n"
            "  LOGJAM_RCV_HWM             high watermark for input socket\n"
            "  LOGJAM_SND_HWM             high watermark for output socket\n"
            "  LOGJAM_INTERFACE           zmq spec of interface on which to liste\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    char *v;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "compress",      no_argument,       0, 'z' },
        { "config",        required_argument, 0, 'c' },
        { "dryrun",        no_argument,       0, 'n' },
        { "help",          no_argument,       0,  0  },
        { "hosts",         required_argument, 0, 'h' },
        { "interface",     required_argument, 0, 'i' },
        { "parsers",       required_argument, 0, 'p' },
        { "quiet",         no_argument,       0, 'q' },
        { "rcv-hwm",       required_argument, 0, 'R' },
        { "snd-hwm",       required_argument, 0, 'S' },
        { "subscribe",     required_argument, 0, 'e' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqc:np:zh:S:R:e", long_options, &longindex)) != -1) {
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
        case 'n':
            dryrun = true;
            break;
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
                printf("[E] must specifiy at least one device to connect to\n");
                exit(1);
            }
            break;
        case 'i':
            interface = optarg;
            break;
        case 'e':
            subscriptions = split_delimited_string(optarg);
            break;
        case 'R':
            rcv_hwm = atoi(optarg);
            break;
        case 'S':
            snd_hwm = atoi(optarg);
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

    if (subscriptions == NULL)
        subscriptions = split_delimited_string(getenv("LOGJAM_SUBSCRIPTIONS"));

    if (rcv_hwm == -1 && (v = getenv("LOGJAM_RCV_HWM")))
        rcv_hwm = atoi(v);

    if (snd_hwm == -1 && (v = getenv("LOGJAM_SND_HWM")))
        snd_hwm = atoi(v);
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
               , argv[0], interface, rcv_hwm, snd_hwm);

    return graylog_forwarder_run_controller_loop(config, hosts, subscriptions, rcv_hwm, snd_hwm);
}
