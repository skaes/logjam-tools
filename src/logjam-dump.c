#include "logjam-util.h"
#include "device-tracker.h"
#include "importer-watchdog.h"
#include <getopt.h>

FILE* dump_file = NULL;
zchunk_t *dump_decompress_buffer;
static char *dump_file_name = "logjam-stream.dump";

#define DEFAULT_ABORT_AFTER 60
int heartbeat_abort_after = -1;

static size_t io_threads = 1;
bool verbose = false;
bool debug = false;
bool quiet = false;
bool append_to_dump_file = false;
bool payload_only = false;
bool stream_only = false;
bool filter_on_topic = false;
bool use_text_output = false;
static char *filter_topic = NULL;

static int sub_port = -1;
static zlist_t *connection_specs = NULL;
static zlist_t *subscriptions = NULL;

#define DEFAULT_SUB_PORT 9606

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;
static size_t message_gaps = 0;

static device_tracker_t *tracker = NULL;

static int timer_event( zloop_t *loop, int timer_id, void *arg)
{
    static size_t ticks = 0;
    static size_t last_received_count = 0;
    static size_t last_received_bytes = 0;
    size_t message_count = received_messages_count - last_received_count;
    size_t message_bytes = received_messages_bytes - last_received_bytes;
    double avg_msg_size = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size = received_messages_max_bytes / 1024.0;
    if (!quiet) {
        printf("[I] processed %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB (gaps: %zu)\n",
               message_count, message_bytes/1024.0, avg_msg_size, max_msg_size, message_gaps);
    }
    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    message_gaps = 0;
    if (++ticks % HEART_BEAT_INTERVAL == 0)
        device_tracker_reconnect_stale_devices(tracker);
    fflush(dump_file);
    return 0;
}

static int read_zmq_message_and_dump(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    zmsg_t *msg = zmsg_recv(socket);
    if (!msg) return 1;

    msg_meta_t meta;
    msg_extract_meta_info(msg, &meta);

    if (debug) {
        my_zmsg_fprint(msg, "[D]", stdout);
        dump_meta_info("[D]", &meta);
    }

    // check for gaps and process heart beats
    zframe_t *first = zmsg_first(msg);
    char *pub_spec = NULL;
    bool is_heartbeat = zframe_streq(first, "heartbeat");
    if (is_heartbeat) {
        if (verbose)
            printf("[I] received heartbeat from device %d\n", meta.device_number);
        zframe_t *spec_frame = zmsg_next(msg);
        pub_spec = zframe_strdup(spec_frame);
    }
    message_gaps += device_tracker_calculate_gap(tracker, &meta, pub_spec);

    // calculate stats
    if (!is_heartbeat) {
        size_t msg_bytes = zmsg_content_size(msg);
        received_messages_count++;
        received_messages_bytes += msg_bytes;
        if (msg_bytes > received_messages_max_bytes)
            received_messages_max_bytes = msg_bytes;
    }

    zframe_t *stream_frame = zmsg_first(msg);  // iterate to topic frame
    zframe_t *topic_frame = zmsg_next(msg);
    char *topic_str = (char*) zframe_data(topic_frame);
    // dump message to file annd free memory
    if (!is_heartbeat) {
        if (filter_on_topic && strncmp(filter_topic, topic_str, strlen(filter_topic))) {
            // do nothing, frame topic does not match filter topic
        }
        else if (stream_only) {
            printf("%.*s\n", (int)zframe_size(stream_frame), zframe_data(stream_frame));
        } else if (payload_only) {
            dump_message_payload(msg, dump_file, dump_decompress_buffer);
        } else if (use_text_output) {
            dump_message_as_json(msg, stdout, dump_decompress_buffer);
        } else {
            zmsg_savex(msg, dump_file);
        }
    }
    zmsg_destroy(&msg);

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options] [dump-file-name]\n"
            "\nOptions:\n"
            "  -a, --append               append dump file instead of overwriting it\n"
            "  -s, --subscribe A,B        subscription patterns\n"
            "  -h, --hosts H,I            specs of devices to connect to\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -p, --input-port N         port number of zeromq input socket\n"
            "  -l, --payload-only         only write the message payload\n"
            "  -S, --stream-only          write the stream name to stdout (ignores dump-file)\n"
            "  -T, --text                 write messages in text format (JSON) to stdout (ignores dump-file)\n"
            "  -t, --topic                only write the messages from given app-env\n"
            "  -A, --abort                abort after missing heartbeats for this many seconds\n"
            "  -q, --quiet                don't log anything\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_ABORT_AFTER         abort after missing heartbeats for this many seconds\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    int c;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "help",          no_argument,       0,  0  },
        { "append",        no_argument,       0, 'a' },
        { "hosts",         required_argument, 0, 'h' },
        { "subscribe",     required_argument, 0, 's' },
        { "input-port",    required_argument, 0, 'p' },
        { "io-threads",    required_argument, 0, 'i' },
        { "quiet",         no_argument,       0, 'q' },
        { "verbose",       no_argument,       0, 'v' },
        { "payload-only",  no_argument,       0, 'l' },
        { "stream-only",   no_argument,       0, 'S' },
        { "topic",         required_argument, 0, 't' },
        { "text",          no_argument,       0, 'T' },
        { "abort",         required_argument, 0, 'A' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqi:h:p:s:lt:aA:TS", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug = true;
            else
                verbose = true;
            break;
        case 'q':
            quiet = true;
            verbose = false;
            debug= false;
            break;
        case 'a':
            append_to_dump_file = true;
            break;
        case 'i':
            io_threads = atoi(optarg);
            break;
        case 'p':
            sub_port = atoi(optarg);
            break;
        case 'h':
            connection_specs = split_delimited_string(optarg);
            break;
        case 's':
            subscriptions = split_delimited_string(optarg);
            break;
        case 'l':
            payload_only = true;
            break;
        case 't':
            filter_on_topic = true;
            filter_topic = optarg;
            break;
        case 'A':
            heartbeat_abort_after = atoi(optarg);
            break;
        case 'T':
            use_text_output = true;
            break;
        case 'S':
            stream_only = true;
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("hip", optopt))
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

    if (optind + 1 < argc) {
        fprintf(stderr, "[E] too many arguments\n");
        print_usage(argv);
        exit(1);
    } else if (optind + 1 == argc) {
        dump_file_name = argv[argc-1];
    }

    if (sub_port == -1)
        sub_port = DEFAULT_SUB_PORT;

    if (connection_specs == NULL) {
        connection_specs = zlist_new();
        zlist_append(connection_specs, strdup("localhost"));
    }
    augment_zmq_connection_specs(&connection_specs, sub_port);

    const char *v;
    if (heartbeat_abort_after == -1) {
        if (( v = getenv("LOGJAM_ABORT_AFTER") ))
            heartbeat_abort_after = atoi(v);
        else
            heartbeat_abort_after = DEFAULT_ABORT_AFTER;
    }
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    dump_decompress_buffer = zchunk_new(NULL, INITIAL_DECOMPRESSION_BUFFER_SIZE);
    // open dump file
    dump_file = fopen(dump_file_name, append_to_dump_file ? "a" : "w");
    if (!dump_file) {
        fprintf(stderr, "[E] could not open dump file: %s\n", strerror(errno));
        exit(1);
    }
    if (verbose) printf("[I] dumping stream to %s\n", dump_file_name);


    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    // create socket to receive messages on
    zsock_t *receiver = zsock_new(ZMQ_SUB);
    assert_x(receiver != NULL, "zmq socket creation failed", __FILE__, __LINE__);

    // connect socket
    char *spec = zlist_first(connection_specs);
    while (spec) {
        if (!quiet)
            printf("[I] connecting SUB socket to %s\n", spec);
        int rc = zsock_connect(receiver, "%s", spec);
        log_zmq_error(rc, __FILE__, __LINE__);
        assert(rc == 0);
        spec = zlist_next(connection_specs);
    }

    // receive everything
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

    // configure the socket
    zsock_set_rcvhwm(receiver, 100000);

    // create device tracker
    tracker = device_tracker_new(connection_specs, receiver);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    int rc = zloop_reader(loop, receiver, read_zmq_message_and_dump, NULL);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // calculate statistics every 1000 ms
    int timer_id = zloop_timer(loop, 1000, 0, timer_event, receiver);
    assert(timer_id != -1);

    // run the loop
    if (!zsys_interrupted) {
        if (verbose) printf("[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        if (verbose) printf("[I] main event loop terminated with return code %d\n", rc);
    }

    if (!quiet)
        printf("[I] received %zu messages\n", received_messages_count);

    // clean up
    if (verbose) printf("[I] shutting down\n");

    device_tracker_destroy(&tracker);
    fclose(dump_file);
    zchunk_destroy(&dump_decompress_buffer);
    zloop_destroy(&loop);
    assert(loop == NULL);
    zsock_destroy(&receiver);
    zsys_shutdown();

    if (verbose) printf("[I] terminated\n");

    return 0;
}
