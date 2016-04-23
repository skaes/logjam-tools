#include "logjam-util.h"
#include <getopt.h>

FILE* dump_file = NULL;
static char *dump_file_name = "logjam-stream.dump";

static size_t io_threads = 1;
static bool verbose = false;
static bool debug = false;

static int sub_port = -1;
static zlist_t *connection_specs = NULL;

#define DEFAULT_SUB_PORT 9606
#define DEFAULT_CONNECTION_SPEC "tcp://localhost:9606"

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;

static int timer_event( zloop_t *loop, int timer_id, void *arg)
{
    static size_t last_received_count = 0;
    static size_t last_received_bytes = 0;
    size_t message_count = received_messages_count - last_received_count;
    size_t message_bytes = received_messages_bytes - last_received_bytes;
    double avg_msg_size = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size = received_messages_max_bytes / 1024.0;
    printf("[I] processed %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           message_count, message_bytes/1024.0, avg_msg_size, max_msg_size);
    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;

    return 0;
}

static int read_zmq_message_and_dump(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    zmsg_t *msg = zmsg_recv(socket);
    if (!msg) return 1;

    if (debug) {
        my_zmsg_fprint(msg, "[D]", stdout);
        msg_meta_t meta;
        msg_extract_meta_info(msg, &meta);
        dump_meta_info(&meta);
    }

    // calculate stats
    size_t msg_bytes = zmsg_content_size(msg);
    received_messages_count++;
    received_messages_bytes += msg_bytes;
    if (msg_bytes > received_messages_max_bytes)
        received_messages_max_bytes = msg_bytes;

    // dump message to file annd free memory
    zmsg_savex(msg, dump_file);
    zmsg_destroy(&msg);

    return 0;
}

void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options] [dump-file-name]\n"
            "\nOptions:\n"
            "  -e, --subscribe A,B        subscription patterns\n"
            "  -h, --hosts H,I            specs of devices to connect to\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -p, --input-port N         port number of zeromq input socket\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "      --help                 display this message\n"
            , argv[0]);
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "help",          no_argument,       0,  0  },
        { "hosts",         required_argument, 0, 'h' },
        { "input-port",    required_argument, 0, 'p' },
        { "io-threads",    required_argument, 0, 'i' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vc:i:h:p:", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug= true;
            else
                verbose = true;
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
    } else if (optind +1 == argc) {
        dump_file_name = argv[argc-1];
    }

    if (sub_port == -1)
        sub_port = DEFAULT_SUB_PORT;

    if (connection_specs == NULL) {
        connection_specs = zlist_new();
        zlist_append(connection_specs, DEFAULT_CONNECTION_SPEC);
    }
    augment_zmq_connection_specs(&connection_specs, sub_port);
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    // open dump file
    dump_file = fopen(dump_file_name, "w");
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
        printf("[I] connecting SUB socket to %s\n", spec);
        int rc = zsock_connect(receiver, "%s", spec);
        log_zmq_error(rc, __FILE__, __LINE__);
        assert(rc == 0);
        spec = zlist_next(connection_specs);
    }

    // receive everything
    zsock_set_subscribe(receiver, "");

    //  configure the socket
    zsock_set_rcvhwm(receiver, 100000);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    int rc = zloop_reader(loop, receiver, read_zmq_message_and_dump, NULL);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // calculate statistics every 1000 ms
    int timer_id = 1;
    rc = zloop_timer(loop, 1000, 0, timer_event, &timer_id);
    assert(rc != -1);

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

    printf("[I] received %zu messages\n", received_messages_count);

    // clean up
    if (verbose) printf("[I] shutting down\n");

    fclose(dump_file);
    zloop_destroy(&loop);
    assert(loop == NULL);
    zsock_destroy(&receiver);
    zsys_shutdown();

    if (verbose) printf("[I] terminated\n");

    return 0;
}
