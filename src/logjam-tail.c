#include "logjam-util.h"
#include <getopt.h>

static bool verbose = false;
static bool debug = false;

static size_t io_threads = 1;
#define DEFAULT_SUB_PORT 9601
#define DEFAULT_CONNECTION_SPEC "tcp://localhost:9601"
static char* connection_spec = NULL;

static size_t processed_lines_count = 0;
static size_t processed_lines_bytes = 0;
static size_t processed_lines_max_bytes = 0;

static int timer_event( zloop_t *loop, int timer_id, void *arg)
{
    static size_t last_processed_count = 0;
    static size_t last_processed_bytes = 0;
    size_t message_count = processed_lines_count - last_processed_count;
    size_t message_bytes = processed_lines_bytes - last_processed_bytes;
    double avg_line_size = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_line_size = processed_lines_max_bytes / 1024.0;
    if (verbose)
        printf("[I] processed %zu lines (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
               message_count, message_bytes/1024.0, avg_line_size, max_line_size);
    last_processed_count = processed_lines_count;
    last_processed_bytes = processed_lines_bytes;
    processed_lines_max_bytes = 0;

    return 0;
}

static int read_msg_and_print(zloop_t *loop, zsock_t *socket, void* arg)
{
    zmsg_t *msg = zmsg_recv(socket);
    zframe_t *content = zmsg_first(msg);
    int line_length = zframe_size(content);
    const char* line = (const char*)zframe_data(content);

    // print line
    printf("%.*s\n", line_length, line);

    // calculate stats
    processed_lines_count++;
    processed_lines_bytes += line_length;
    if (line_length > processed_lines_max_bytes)
        processed_lines_max_bytes = line_length;

    zmsg_destroy(&msg);

    return 0;
}

void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "  -c, --connect S            zmq specification for connecting SUB socket\n"
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
        { "io-threads",    required_argument, 0, 'i' },
        { "connect",       required_argument, 0, 'c' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vi:p:", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug = true;
            else
                verbose = true;
            break;
        case 'i':
            io_threads = atoi(optarg);
            break;
        case 'c':
            connection_spec = augment_zmq_connection_spec(optarg, DEFAULT_SUB_PORT);
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("riz", optopt))
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

    if (connection_spec == NULL)
        connection_spec = DEFAULT_CONNECTION_SPEC;
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    // create socket to publish messages on
    zsock_t *receiver = zsock_new(ZMQ_SUB);
    assert_x(receiver != NULL, "[E] zmq socket creation failed", __FILE__, __LINE__);

    // configure the socket
    zsock_set_subscribe(receiver, "");
    zsock_set_sndhwm(receiver, 100000);

    // connect socket
    if (verbose) printf("[I] connecting SUB socket to %s\n", connection_spec);
    int rc = zsock_connect(receiver, "%s", connection_spec);
    assert_x(rc==0, "[E] sub socket connct failed", __FILE__, __LINE__);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // calculate statistics every 1000 ms
    int timer_id = 1;
    rc = zloop_timer(loop, 1000, 0, timer_event, &timer_id);
    assert(rc != -1);

    // register a reader for the SUB socket
    rc = zloop_reader(loop, receiver, read_msg_and_print, NULL);
    assert(rc==0);

    if (!zsys_interrupted) {
        if (verbose) printf("[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            if (!zsys_interrupted)
                log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        if (verbose) printf("[I] main event loop terminated with return code %d\n", rc);
    }

    // clean up
    if (verbose) printf("[I] shutting down\n");

    zloop_destroy(&loop);
    assert(loop == NULL);
    zsock_destroy(&receiver);
    zsys_shutdown();

    if (verbose) printf("[I] terminated\n");

    return 0;
}
