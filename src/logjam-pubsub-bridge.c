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
#include "message-compressor.h"

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
static int pub_port = 9608;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;

static size_t decompressed_messages_count = 0;
static size_t decompressed_messages_bytes = 0;
static size_t decompressed_messages_max_bytes = 0;

static zlist_t *hosts = NULL;
static zlist_t *subscriptions = NULL;

static size_t io_threads = 1;
static size_t num_compressors = 4;

static msg_meta_t msg_meta = META_INFO_EMPTY;

#define MAX_COMPRESSORS 64
static uint64_t global_time = 0;

typedef struct {
    // raw zmq sockets, to avoid zsock_resolve
    void *receiver;
    void *publisher;
    void *compressor_input;
    void *compressor_output;
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
    static size_t last_decompressed_count = 0;
    static size_t last_decompressed_bytes = 0;

    size_t message_count    = received_messages_count - last_received_count;
    size_t message_bytes    = received_messages_bytes - last_received_bytes;
    size_t decompressed_count = decompressed_messages_count - last_decompressed_count;
    size_t decompressed_bytes = decompressed_messages_bytes - last_decompressed_bytes;

    double avg_msg_size        = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size        = received_messages_max_bytes / 1024.0;
    double avg_decompressed_size = decompressed_count ? (decompressed_bytes / 1024.0) / decompressed_count : 0;
    double max_decompressed_size = decompressed_messages_max_bytes / 1024.0;

    printf("[I] processed %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           message_count, message_bytes/1024.0, avg_msg_size, max_msg_size);

    printf("[I] compressd %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           decompressed_count, decompressed_bytes/1024.0, avg_decompressed_size, max_decompressed_size);

    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    last_decompressed_count = decompressed_messages_count;
    last_decompressed_bytes = decompressed_messages_bytes;
    decompressed_messages_max_bytes = 0;

    global_time = zclock_time();

    static size_t ticks = 0;
    bool terminate = (++ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    if (terminate) {
        printf("[I] detected config change. terminating.\n");
        zsys_interrupted = 1;
    }

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

    // const char *prefix = socket == state->compressor_output ? "EXTERNAL MESSAGE" : "INTERNAL MESSAGE";
    // my_zmq_msg_fprint(&message_parts[0], 3, prefix, stdout);
    // dump_meta_info(&meta);

    if (meta.created_ms)
        msg_meta.created_ms = meta.created_ms;
    else
        msg_meta.created_ms = global_time;

    size_t msg_bytes = zmq_msg_size(body);
    if (socket == state->compressor_output) {
        decompressed_messages_count++;
        decompressed_messages_bytes += msg_bytes;
        if (msg_bytes > decompressed_messages_max_bytes)
            decompressed_messages_max_bytes = msg_bytes;
    } else {
        received_messages_count++;
        received_messages_bytes += msg_bytes;
        if (msg_bytes > received_messages_max_bytes)
            received_messages_max_bytes = msg_bytes;
    }

    msg_meta.compression_method = meta.compression_method;
    if (meta.compression_method) {
        // decompress
        publish_on_zmq_transport(&message_parts[0], state->compressor_input, &msg_meta, 0);
    } else {
        // forward to comsumer
        msg_meta.sequence_number++;
        if (debug) {
            my_zmq_msg_fprint(&message_parts[0], 3, "[D]", stdout);
            dump_meta_info(&msg_meta);
        }
        publish_on_zmq_transport(&message_parts[0], state->publisher, &msg_meta, ZMQ_DONTWAIT);
    }

 cleanup:
    for (;i>=0;i--) {
        zmq_msg_close(&message_parts[i]);
    }

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-v] [-q] [-d device number] [-p sub-port] [-c config-file] [-e subscription] [-i io-threads] [-s num-decompressors] [-h hosts]\n", argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "vqd:p:c:e:i:s:h:")) != -1) {
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
        case 'd':
            msg_meta.device_number = atoi(optarg);
            break;
        case 'p':
            pull_port = atoi(optarg);
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'i':
            io_threads = atoi(optarg);
            break;
        case 's':
            num_compressors = atoi(optarg);
            if (num_compressors > MAX_COMPRESSORS) {
                num_compressors = MAX_COMPRESSORS;
                printf("[I] number of compressors reduced to %d\n", MAX_COMPRESSORS);
            }
            break;
        case 'h':
            hosts = split_delimited_string(optarg);
            break;
        case '?':
            if (strchr("depcish", optopt))
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

    if (subscriptions == NULL)
        subscriptions = split_delimited_string(getenv("LOGJAM_PUBSUB_BRIDGE_SUBSCRIPTIONS"));

    if (hosts == NULL) {
        hosts = split_delimited_string(getenv("LOGJAM_PUBSUB_BRIDGE_DEVICES"));
        if (hosts == NULL) {
            hosts = zlist_new();
            zlist_push(hosts, "localhost");
        }
    }
}

int main(int argc, char * const *argv)
{
    int rc = 0;
    process_arguments(argc, argv);

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    // TODO: figure out sensible port numbers
    pub_port = pull_port + 1;

    printf("[I] started %s\n"
           "[I] sub-port:    %d\n"
           "[I] push-port:   %d\n"
           "[I] io-threads:  %lu\n",
           argv[0], pull_port, pub_port, io_threads);

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
    assert_x(receiver != NULL, "zmq socket creation failed");
    zsock_set_rcvhwm(receiver, 100000);

    // bind externally
    char* host = zlist_first(hosts);
    while (host) {
        rc = zsock_connect(receiver, "tcp://%s:%d", host, pull_port);
        assert_x(rc == 0, "receiver socket: external connect failed");
        host = zlist_next(hosts);
    }

    // create socket for publishing
    zsock_t *publisher = zsock_new(ZMQ_PUSH);
    assert_x(publisher != NULL, "publisher socket creation failed");
    zsock_set_sndhwm(publisher, 1000000);

    rc = zsock_bind(publisher, "tcp://%s:%d", "*", pub_port);
    assert_x(rc == pub_port, "publisher socket bind failed");

    // create compressor sockets
    zsock_t *compressor_input = zsock_new(ZMQ_PUSH);
    assert_x(compressor_input != NULL, "compressor input socket creation failed");
    rc = zsock_bind(compressor_input, "inproc://compressor-input");
    assert_x(rc==0, "compressor input socket bind failed");

    zsock_t *compressor_output = zsock_new(ZMQ_PULL);
    assert_x(compressor_output != NULL, "compressor output socket creation failed");
    rc = zsock_bind(compressor_output, "inproc://compressor-output");
    assert_x(rc==0, "compressor output socket bind failed");

    // create compressor agents
    zactor_t *compressors[MAX_COMPRESSORS];
    for (size_t i = 0; i < num_compressors; i++)
        compressors[i] = message_decompressor_new(i);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // calculate statistics every 1000 ms
    int timer_id = 1;
    rc = zloop_timer(loop, 1000, 0, timer_event, &timer_id);
    assert(rc != -1);

    // setup handler for the receiver socket
    publisher_state_t publisher_state = {
        .receiver = zsock_resolve(receiver),
        .publisher = zsock_resolve(publisher),
        .compressor_input = zsock_resolve(compressor_input),
        .compressor_output = zsock_resolve(compressor_output),
    };

    // setup handler for compression results
    rc = zloop_reader(loop, compressor_output, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, compressor_output);

    // setup handdler for messages incoming from the outside or rabbit_listener
    rc = zloop_reader(loop, receiver, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // initialize clock
    global_time = zclock_time();

    // setup subscriptions
    if (subscriptions == NULL || zlist_size(subscriptions) == 0)
        zsock_set_subscribe(receiver, "");
    else {
        char *subscription = zlist_first(subscriptions);
        while (subscription) {
            printf("[I] subscribing to %s\n", subscription);
            zsock_set_subscribe(receiver, subscription);
            subscription = zlist_next(subscriptions);
        }
    }

    // run the loop
    if (!zsys_interrupted) {
        printf("[I] starting main event loop\n");
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
        printf("[I] main event zloop terminated with return code %d\n", rc);
    }

    zloop_destroy(&loop);
    assert(loop == NULL);

    printf("[I] received %zu messages\n", received_messages_count);
    printf("[I] shutting down\n");

    zlist_destroy(&hosts);
    zlist_destroy(&subscriptions);
    zsock_destroy(&receiver);
    zsock_destroy(&publisher);
    zsock_destroy(&compressor_input);
    zsock_destroy(&compressor_output);
    for (size_t i = 0; i < num_compressors; i++)
        zactor_destroy(&compressors[i]);
    zsys_shutdown();

    printf("[I] terminated\n");

    return rc;
}
