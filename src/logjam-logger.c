#include "logjam-util.h"
#include <getopt.h>

static bool verbose = false;
static bool debug = false;

static size_t io_threads = 1;
#define DEFAULT_PUB_PORT 9601
#define DEFAULT_BIND_SPEC "tcp://*:9601"
static char* bind_spec = NULL;

static size_t processed_lines_count = 0;
static size_t processed_lines_bytes = 0;
static size_t processed_lines_max_bytes = 0;

static char* buffer = NULL;
static size_t buffer_size;
#define INITIAL_BUFFER_SIZE 8*1024
static bool terminating = false;

static char *topic = "";
static char *app_name = NULL;

static char* log_id = "logjam-logger";
static int log_level = LOG_INFO;
static int log_facility = LOG_USER;
static bool log_to_syslog = false;

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

static int read_line_and_forward(zloop_t *loop, zmq_pollitem_t *item, void* arg)
{
    zsock_t *socket = arg;
    zmsg_t *msg = zmsg_new();
    assert(msg);
    zmsg_addstr(msg, topic);

    // read input and append it ot message
    ssize_t line_length = getline(&buffer, &buffer_size, stdin);
    if (line_length == -1) {
        if (verbose) printf("[I] end of input. aborting.\n");
        terminating = true;
        return -1;
    }
    zmsg_addmem(msg, buffer, line_length - 1);

    if (log_to_syslog) {
        if (topic)
            syslog(log_level, "%s", buffer);
        else
            syslog(log_level, "%s:%s", topic, buffer);
    }

    // calculate stats
    processed_lines_count++;
    processed_lines_bytes += line_length;
    if (line_length > processed_lines_max_bytes)
        processed_lines_max_bytes = line_length;

    if (debug)
        my_zmsg_fprint(msg, "[D]", stdout);

    // send message and destroy it
    zmsg_send(&msg, socket);

    return 0;
}

void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -a, --app                  app name for syslog (default: %s)\n"
            "  -b, --bind I               zmq specification for binding pub socket\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -s, --syslog [F.L]         send data to syslog with optional facility.level\n"
            "                             facility can be one of (user, local0 ... local7), default: user\n"
            "                             level can be one of (error, warn, notice, info),  default: info\n"
            "  -t, --topic                data for topic frame\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "      --help                 display this message\n"
            , argv[0], log_id);
}

bool scan_syslog_param(char *arg, int* level, int* facility)
{
    char facility_buffer[256];
    char level_buffer[256];

    if (strlen(arg) > 256) {
        fprintf(stderr, "[E] syslog priority param too large\n");
        return false;
    }

    if (2 != sscanf(arg, "%[^.].%s", facility_buffer, level_buffer)) {
        fprintf(stderr, "[E] syslog priority param should be facility.level\n");
        return false;
    }

    if (!strncasecmp(level_buffer, "error", 3))
        *level = LOG_ERR;
    else if (!strncasecmp(level_buffer, "warn", 4))
        *level = LOG_WARNING;
    else if (!strncasecmp(level_buffer, "notice", 4))
        *level = LOG_NOTICE;
    else if (streq(level_buffer, "info"))
        *level = LOG_INFO;
    else {
        printf("[E] unknown log level %s\n", level_buffer);
        return false;
    }

    if (!strncasecmp(facility_buffer, "local0", 6))
        *facility = LOG_LOCAL0;
    else if (!strncasecmp(facility_buffer, "local1", 6))
        *facility = LOG_LOCAL1;
    else if (!strncasecmp(facility_buffer, "local2", 6))
        *facility = LOG_LOCAL2;
    else if (!strncasecmp(facility_buffer, "local3", 6))
        *facility = LOG_LOCAL3;
    else if (!strncasecmp(facility_buffer, "local4", 6))
        *facility = LOG_LOCAL4;
    else if (!strncasecmp(facility_buffer, "local5", 6))
        *facility = LOG_LOCAL5;
    else if (!strncasecmp(facility_buffer, "local6", 6))
        *facility = LOG_LOCAL6;
    else if (!strncasecmp(facility_buffer, "local7", 6))
        *facility = LOG_LOCAL7;
    else if (!strncasecmp(facility_buffer, "user", 5))
        *facility = LOG_USER;
    else {
        printf("[E] unknown log facility %s\n", level_buffer);
        return false;
    }

    return true;
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "help",          no_argument,       0,  0  },
        { "app",           required_argument, 0, 'a' },
        { "bind",          required_argument, 0, 'b' },
        { "io-threads",    required_argument, 0, 'i' },
        { "syslog",        optional_argument, 0, 's' },
        { "topic",         required_argument, 0, 't' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, ":vi:b:t:a:s:", long_options, &longindex)) != -1) {
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
        case 'a':
            app_name = optarg;
            break;
        case 'b':
            bind_spec = augment_zmq_connection_spec(optarg, DEFAULT_PUB_PORT);
            break;
        case 't':
            topic = optarg;
            break;
        case 's':
            log_to_syslog = true;
            if (!scan_syslog_param(optarg, &log_facility, &log_level)) {
                print_usage(argv);
                exit(1);
            }
            break;
        case ':':
            if (optopt == 's') {
                log_to_syslog = true;
            } else {
                print_usage(argv);
                exit(1);
            }
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("abits", optopt))
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

    if (bind_spec == NULL)
        bind_spec = DEFAULT_BIND_SPEC;
}

int main(int argc, char * const *argv)
{
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    openlog(log_id, LOG_PID, log_facility);

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    // create socket to publish messages on
    zsock_t *sender = zsock_new(ZMQ_PUB);
    assert_x(sender != NULL, "[E] zmq socket creation failed", __FILE__, __LINE__);

    // configure the socket
    zsock_set_sndhwm(sender, 100000);

    // connect socket
    if (verbose) printf("[I] binding PUB socket to %s\n", bind_spec);
    int rc = zsock_bind(sender, "%s", bind_spec);
    assert_x(rc>0, "[E] pub socket bind failed", __FILE__, __LINE__);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // calculate statistics every 1000 ms
    int timer_id = 1;
    rc = zloop_timer(loop, 1000, 0, timer_event, &timer_id);
    assert(rc != -1);

    // register FILE descriptor for pollin events
    zmq_pollitem_t stdio_item = {
        .fd = fileno(stdin),
        .events = ZMQ_POLLIN
    };
    rc = zloop_poller(loop, &stdio_item, read_line_and_forward, sender);
    assert(rc==0);

    // allocate the input buffer
    buffer = zmalloc(INITIAL_BUFFER_SIZE);

    if (!zsys_interrupted) {
        if (verbose) printf("[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            if (!terminating)
                log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        if (verbose) printf("[I] main event loop terminated with return code %d\n", rc);
    }

    // clean up
    if (verbose) printf("[I] shutting down\n");

    zloop_destroy(&loop);
    assert(loop == NULL);
    zsock_destroy(&sender);
    zsys_shutdown();

    closelog();

    if (verbose) printf("[I] terminated\n");

    return 0;
}
