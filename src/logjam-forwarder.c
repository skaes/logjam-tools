#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <getopt.h>
#include "logjam-util.h"
#include "device-tracker.h"

// shared globals
bool verbose = false;
bool quiet = false;
bool debug = false;

/* global config */
static zconfig_t* config = NULL;
static zfile_t *config_file = NULL;
static char *config_file_name = "logjam.conf";
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";
static bool config_file_exists = false;
// check every 10 ticks whether config file has changed
#define CONFIG_FILE_CHECK_INTERVAL 10

static int pull_port = 9606;
static int downstream_port  = 9604;

#define DEFAULT_RCV_HWM  10000
#define DEFAULT_SND_HWM 100000

static int rcv_hwm = -1;
static int snd_hwm = -1;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;

static size_t dropped_messages_count = 0;

static device_tracker_t *tracker = NULL;
static zlist_t *hosts = NULL;
static zlist_t *downstream_devices = NULL;
static zlist_t *subscriptions = NULL;

static size_t io_threads = 1;

static msg_meta_t msg_meta = META_INFO_EMPTY;

static uint64_t global_time = 0;

typedef struct {
    // raw zmq sockets, to avoid zsock_resolve
    void *receiver;
    void *publisher;
} publisher_state_t;


static void config_file_init()
{
    config_file = zfile_new(NULL, config_file_name);
    config_file_last_modified = zfile_modified(config_file);
    config_file_digest = strdup(zfile_digest(config_file));
}

static bool config_file_has_changed()
{
    bool changed = false;
    if (config_file_exists) {
        zfile_restat(config_file);
        if (config_file_last_modified != zfile_modified(config_file)) {
            const char *new_digest = zfile_digest(config_file);
            // printf("[D] old digest: %s\n[D] new digest: %s\n", config_file_digest, new_digest);
            changed = strcmp(config_file_digest, new_digest) != 0;
        }
    }
    return changed;
}

static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    static size_t last_received_count   = 0;
    static size_t last_received_bytes   = 0;
    static size_t last_dropped_count   = 0;

    size_t message_count    = received_messages_count - last_received_count;
    size_t message_bytes    = received_messages_bytes - last_received_bytes;
    size_t dropped_messages = dropped_messages_count - last_dropped_count;

    double avg_msg_size        = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size        = received_messages_max_bytes / 1024.0;

    if (!quiet) {
        printf("[I] processed %5zu messages[dropped: %zu] (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
               message_count, dropped_messages, message_bytes/1024.0, avg_msg_size, max_msg_size);
    }

    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    last_dropped_count = dropped_messages_count;
    received_messages_max_bytes = 0;

    global_time = zclock_time();

    static size_t ticks = 0;
    bool terminate = (++ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    if (terminate) {
        printf("[I] detected config change. terminating.\n");
        zsys_interrupted = 1;
    }
    if (ticks % HEART_BEAT_INTERVAL == 0)
        device_tracker_reconnect_stale_devices(tracker);

    return 0;
}

static int read_zmq_message_and_forward(zloop_t *loop, zsock_t *sock, void *callback_data)
{
    int i = 0;
    zmq_msg_t message_parts[4];
    publisher_state_t *state = (publisher_state_t*)callback_data;
    void *socket = zsock_resolve(sock);

    // read the message parts, possibly including the message meta info
    while (!zsys_interrupted) {
        // printf("[D] receiving part %d\n", i+1);
        if (i>3) {
            zmq_msg_t dummy_msg;
            zmq_msg_init(&dummy_msg);
            zmq_recvmsg(socket, &dummy_msg, 0);
            zmq_msg_close(&dummy_msg);
        } else {
            zmq_msg_init(&message_parts[i]);
            zmq_recvmsg(socket, &message_parts[i], 0);
        }
        if (!zsocket_rcvmore(socket))
            break;
        i++;
    }
    if (i<2) {
        if (!zsys_interrupted) {
            fprintf(stderr, "[E] received only %d message parts\n", i);
        }
        goto cleanup;
    } else if (i>3) {
        fprintf(stderr, "[E] received more than 4 message parts\n");
        goto cleanup;
    }

    zmq_msg_t *body = &message_parts[2];
    msg_meta_t meta = META_INFO_EMPTY;
    if (i==3)
        zmq_msg_extract_meta_info(&message_parts[3], &meta);

    // my_zmq_msg_fprint(&message_parts[0], 3, "MESSAGE", stdout);
    // dump_meta_info(prefix, &meta);

    if (meta.created_ms)
        msg_meta.created_ms = meta.created_ms;
    else
        msg_meta.created_ms = global_time;

    size_t msg_bytes = zmq_msg_size(body);
    received_messages_count++;
    received_messages_bytes += msg_bytes;
    if (msg_bytes > received_messages_max_bytes)
        received_messages_max_bytes = msg_bytes;

    msg_meta.compression_method = meta.compression_method;
    // forward to comsumer
    msg_meta.sequence_number++;
    if (debug) {
        my_zmq_msg_fprint(&message_parts[0], 3, "[D]", stdout);
        dump_meta_info("[D]", &msg_meta);
    }
    char *pub_spec = NULL;
    bool is_heartbeat = zmq_msg_size(&message_parts[0]) == 9 && strncmp("heartbeat", zmq_msg_data(&message_parts[0]), 9) == 0;
    if (is_heartbeat) {
        if (debug)
            printf("[D] received heartbeat message from device %u\n", msg_meta.device_number);
        pub_spec = strndup(zmq_msg_data(&message_parts[1]), zmq_msg_size(&message_parts[1]));
    }
    device_tracker_calculate_gap(tracker, &msg_meta, pub_spec);
    if (!is_heartbeat) {
        int rc = publish_on_zmq_transport(&message_parts[0], state->publisher, &msg_meta, ZMQ_DONTWAIT);
        if (rc == -1) {
            dropped_messages_count++;
        }
    }

 cleanup:
    for (;i>=0;i--) {
        zmq_msg_close(&message_parts[i]);
    }

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -c, --config C              zeromq config file\n"
            "  -d, --device-id N           device id (integer)\n"
            "  -e, --subscribe A,B         subscription patterns\n"
            "  -f, --forward A,B           specs of devices to forward messages to\n"
            "  -h, --hosts H,I             specs of devices to retrieve messages from\n"
            "  -i, --io-threads N          zeromq io threads\n"
            "  -p, --input-port N          port number of zeromq input socket\n"
            "  -q, --quiet                 supress most output\n"
            "  -v, --verbose               log more (use -vv for debug output)\n"
            "  -P, --output-port N         port number of downstream broker sockets\n"
            "  -R, --rcv-hwm N             high watermark for input socket\n"
            "  -S, --snd-hwm N             high watermark for output socket\n"
            "      --help                  display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_DEVICES              specs of devices to retrieve messages from\n"
            "  LOGJAM_SUBSCRIPTIONS        subscription patterns\n"
            "  LOGJAM_DOWNSTREAM_DEVICES   specs of devices to forward messages to\n"
            "  LOGJAM_RCV_HWM              high watermark for input socket\n"
            "  LOGJAM_SND_HWM              high watermark for output socket\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    char *v;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "config",        required_argument, 0, 'c' },
        { "device-id",     required_argument, 0, 'd' },
        { "forward",       required_argument, 0, 'f' },
        { "help",          no_argument,       0,  0  },
        { "hosts",         required_argument, 0, 'h' },
        { "input-port",    required_argument, 0, 'p' },
        { "io-threads",    required_argument, 0, 'i' },
        { "output-port",   required_argument, 0, 'P' },
        { "quiet",         no_argument,       0, 'q' },
        { "rcv-hwm",       required_argument, 0, 'R' },
        { "snd-hwm",       required_argument, 0, 'S' },
        { "subscribe",     required_argument, 0, 'e' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqd:p:P:R:S:c:e:i:s:h:f:", long_options, &longindex)) != -1) {
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
        case 'e':
            subscriptions = split_delimited_string(optarg);
            break;
        case 'f':
            downstream_devices = split_delimited_string(optarg);
            if (downstream_devices == NULL || zlist_size(downstream_devices) == 0) {
                printf("[E] must specifiy at least one downstream device to connect to\n");
                exit(1);
            }
            break;
        case 'd':
            msg_meta.device_number = atoi(optarg);
            break;
        case 'p':
            pull_port = atoi(optarg);
            break;
        case 'P':
            downstream_port = atoi(optarg);
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'i':
            io_threads = atoi(optarg);
            if (io_threads == 0) {
                printf("[E] invalid io-threads value: must be greater than 0\n");
                exit(1);
            }
            break;
        case 'h':
            hosts = split_delimited_string(optarg);
            if (hosts == NULL || zlist_size(hosts) == 0) {
                printf("[E] must specifiy at least one device to connect to\n");
                exit(1);
            }
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
            if (strchr("defpcish", optopt))
                fprintf(stderr, "option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            fprintf(stderr, "BUG: can't process option -%c\n", c);
            exit(1);
        }
    }

    if (hosts == NULL) {
        hosts = split_delimited_string(getenv("LOGJAM_DEVICES"));
        if (hosts == NULL)
            hosts = zlist_new();
        if (zlist_size(hosts) == 0)
            zlist_push(hosts, strdup("localhost"));
    }
    augment_zmq_connection_specs(&hosts, pull_port);

    if (downstream_devices == NULL) {
        downstream_devices = split_delimited_string(getenv("LOGJAM_DOWNSTREAM_DEVICES"));
        if (downstream_devices == NULL)
            downstream_devices = zlist_new();
        if (zlist_size(downstream_devices) == 0)
            zlist_push(downstream_devices, strdup("localhost"));
    }
    augment_zmq_connection_specs(&downstream_devices, downstream_port);

    if (subscriptions == NULL)
        subscriptions = split_delimited_string(getenv("LOGJAM_SUBSCRIPTIONS"));

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
    int rc = 0;
    process_arguments(argc, argv);

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    if (!quiet)
        printf("[I] started %s\n"
               "[I] sub-port:    %d\n"
               "[I] downstream-port:   %d\n"
               "[I] io-threads:  %lu\n"
               "[I] rcv-hwm:  %d\n"
               "[I] snd-hwm:  %d\n"
               , argv[0], pull_port, downstream_port, io_threads, rcv_hwm, snd_hwm);

    // load config
    config_file_exists = zsys_file_exists(config_file_name);
    if (config_file_exists) {
        config_file_init();
        config = zconfig_load((char*)config_file_name);
    }

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    // create socket to receive messages on
    zsock_t *receiver = zsock_new(ZMQ_SUB);
    assert_x(receiver != NULL, "sub socket creation failed", __FILE__, __LINE__);
    zsock_set_rcvhwm(receiver, rcv_hwm);

    // bind externally
    char* host = zlist_first(hosts);
    while (host) {
        if (!quiet)
            printf("[I] connecting to: %s\n", host);
        rc = zsock_connect(receiver, "%s", host);
        assert_x(rc == 0, "sub socket connect failed", __FILE__, __LINE__);
        host = zlist_next(hosts);
    }
    tracker = device_tracker_new(hosts, receiver);

    // create socket for publishing
    zsock_t *publisher = zsock_new(ZMQ_DEALER);
    assert_x(publisher != NULL, "dealer socket creation failed", __FILE__, __LINE__);
    zsock_set_sndhwm(publisher, snd_hwm);

    // connect to all downstream devices
    char* spec = zlist_first(downstream_devices);
    while (spec) {
        if (!quiet)
            printf("[I] connecting to: %s\n", spec);
        rc = zsock_connect(publisher, "%s", spec);
        assert_x(rc == 0, "dealer socket connect failed", __FILE__, __LINE__);
        spec = zlist_next(downstream_devices);
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // calculate statistics every 1000 ms
    int timer_id = zloop_timer(loop, 1000, 0, timer_event, NULL);
    assert(timer_id != -1);

    // setup handler for the receiver socket
    publisher_state_t publisher_state = {
        .receiver = zsock_resolve(receiver),
        .publisher = zsock_resolve(publisher),
    };

    // setup handdler for messages incoming from the outside or rabbit_listener
    rc = zloop_reader(loop, receiver, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // initialize clock
    global_time = zclock_time();

    // setup subscriptions
    if (subscriptions == NULL || zlist_size(subscriptions) == 0) {
        if (!quiet)
            printf("[I] subscribing to all log messages\n");
        zsock_set_subscribe(receiver, "");
    } else {
        char *subscription = zlist_first(subscriptions);
        while (subscription) {
            if (!quiet)
                printf("[I] subscribing to %s\n", subscription);
            zsock_set_subscribe(receiver, subscription);
            subscription = zlist_next(subscriptions);
        }
        zsock_set_subscribe(receiver, "heartbeat");
    }

    // run the loop
    if (!zsys_interrupted) {
        if (verbose)
            printf("[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        if (verbose)
            printf("[I] main event zloop terminated with return code %d\n", rc);
    }

    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet) {
        printf("[I] received %zu messages\n", received_messages_count);
        printf("[I] dropped %zu messages\n", dropped_messages_count);
        printf("[I] shutting down\n");
    }

    zlist_destroy(&hosts);
    zlist_destroy(&subscriptions);
    zsock_destroy(&receiver);
    zsock_destroy(&publisher);
    device_tracker_destroy(&tracker);
    zsys_shutdown();

    if (!quiet)
        printf("[I] terminated\n");

    return rc;
}
