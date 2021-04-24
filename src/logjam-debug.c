#include "logjam-util.h"
#include "device-tracker.h"
#include "importer-watchdog.h"
#include <getopt.h>

bool verbose = false;
bool debug = false;
bool quiet = false;

static int sub_port = -1;
static zlist_t *connection_specs = NULL;
static zlist_t *subscriptions = NULL;

zchunk_t *dump_decompress_buffer;
json_tokener *tokener;

#define DEFAULT_SUB_PORT 9651


json_object* message_to_json(zmsg_t *self, zchunk_t *buffer)
{
    assert (self);
    assert (zmsg_is (self));
    assert (zmsg_size(self) == 4);

    json_object *payload = json_object_new_object();

    zframe_t *stream_frame = zmsg_first(self);
    json_object *stream = json_object_new_string_len((const char*)zframe_data(stream_frame), zframe_size(stream_frame));
    json_object_object_add(payload, "stream", stream);

    zframe_t *topic_frame = zmsg_next(self);
    json_object *topic = json_object_new_string_len((const char*)zframe_data(topic_frame), zframe_size(topic_frame));
    json_object_object_add(payload, "topic", topic);

    zframe_t *body_frame = zmsg_next (self);

    msg_meta_t meta = META_INFO_EMPTY;
    msg_extract_meta_info(self, &meta);
    int compression_method = meta.compression_method;
    char *body;
    size_t body_len;
    if (compression_method) {
        int rc = decompress_frame(body_frame, compression_method, buffer, &body, &body_len);
        if (rc == 0) {
            fprintf(stderr, "[E] decompressor: could not decompress payload from\n");
            body = "... payload coud not be decompressed ...";
            body_len = 3;
        }
    } else {
        body = (char*)zframe_data(body_frame);
        body_len = zframe_size(body_frame);
    }

    json_object* parse_json_data(const char *json_data, size_t json_data_len, json_tokener* tokener);

    json_object *body_obj = parse_json_data((const char*)body, body_len, tokener);
    json_object_object_add(payload, "payload", body_obj);

    json_object_object_add(payload, "meta", meta_info_to_json(&meta));

    return payload;
}


static int read_zmq_message_and_print(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    zmsg_t *msg = zmsg_recv(socket);
    if (!msg) return 1;

    msg_meta_t meta;
    msg_extract_meta_info(msg, &meta);

    if (debug) {
        my_zmsg_fprint(msg, "[D]", stderr);
        dump_meta_info("[D]", &meta);
    }

    zmsg_first(msg);
    json_object *json = message_to_json(msg, dump_decompress_buffer);
    const char* line = json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
    fwrite(line, strlen(line), 1, stdout);
    fwrite("\n", 1, 1, stdout);
    fflush(stdout);

    json_object_put(json);
    zmsg_destroy(&msg);

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options] [dump-file-name]\n"
            "\nOptions:\n"
            "  -s, --subscribe A,B        subscription patterns\n"
            "  -c, --connect S            debug endpoint to connect to\n"
            "  -q, --quiet                don't log anything\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_ABORT_AFTER         abort after missing heartbeats for this many seconds\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "help",          no_argument,       0,  0  },
        { "connect",       required_argument, 0, 'c' },
        { "subscribe",     required_argument, 0, 's' },
        { "quiet",         no_argument,       0, 'q' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqs:r:c:", long_options, &longindex)) != -1) {
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
        case 'c':
            connection_specs = split_delimited_string(optarg);
            break;
        case 's':
            subscriptions = split_delimited_string(optarg);
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
    }

    if (sub_port == -1)
        sub_port = DEFAULT_SUB_PORT;

    if (connection_specs == NULL) {
        connection_specs = zlist_new();
        zlist_append(connection_specs, strdup("localhost"));
    }
    augment_zmq_connection_specs(&connection_specs, sub_port);
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    dump_decompress_buffer = zchunk_new(NULL, INITIAL_DECOMPRESSION_BUFFER_SIZE);
    tokener = json_tokener_new();

    // set global config
    zsys_init();
    zsys_set_rcvhwm(1000);
    zsys_set_sndhwm(1000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);

    // create socket to receive messages on
    zsock_t *receiver = zsock_new(ZMQ_SUB);
    assert_x(receiver != NULL, "zmq socket creation failed", __FILE__, __LINE__);

    // connect socket
    char *spec = zlist_first(connection_specs);
    while (spec) {
        if (verbose)
            fprintf(stderr, "[I] connecting SUB socket to %s\n", spec);
        int rc = zsock_connect(receiver, "%s", spec);
        log_zmq_error(rc, __FILE__, __LINE__);
        assert(rc == 0);
        spec = zlist_next(connection_specs);
    }

    // setup subscriptions
    if (subscriptions == NULL || zlist_size(subscriptions) == 0) {
        if (verbose)
            fprintf(stderr, "[I] subscribing to all log messages\n");
        zsock_set_subscribe(receiver, "");
    } else {
        char *subscription = zlist_first(subscriptions);
        while (subscription) {
            if (verbose)
                fprintf(stderr, "[I] subscribing to %s\n", subscription);
            zsock_set_subscribe(receiver, subscription);
            subscription = zlist_next(subscriptions);
        }
        zsock_set_subscribe(receiver, "heartbeat");
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    int rc = zloop_reader(loop, receiver, read_zmq_message_and_print, NULL);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // run the loop
    if (!zsys_interrupted) {
        if (verbose) fprintf(stderr, "[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        if (verbose) fprintf(stderr, "[I] main event loop terminated with return code %d\n", rc);
    }

    // clean up
    if (verbose) fprintf(stderr, "[I] shutting down\n");

    zchunk_destroy(&dump_decompress_buffer);
    zloop_destroy(&loop);
    assert(loop == NULL);
    zsock_destroy(&receiver);
    zsys_shutdown();

    if (verbose) fprintf(stderr, "[I] terminated\n");

    return 0;
}
