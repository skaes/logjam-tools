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
bool debug = false;
bool quiet = false;

#define DEFAULT_RCV_HWM  100000
#define DEFAULT_SND_HWM 1000000

int rcv_hwm = -1;
int snd_hwm = -1;

/* global config */
static int router_port = 9604;
static int pull_port = 9605;
static int pub_port = 9606;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;

static size_t compressed_messages_count = 0;
static size_t compressed_messages_bytes = 0;
static size_t compressed_messages_max_bytes = 0;

static size_t io_threads = 1;
static size_t num_compressors = 4;

static msg_meta_t msg_meta = META_INFO_EMPTY;
static char device_number_s[11] = {'0', 0};

#define MAX_COMPRESSORS 64
static int compression_method = NO_COMPRESSION;
static zchunk_t *compression_buffer;
static uint64_t global_time = 0;

typedef struct {
    // raw zmq sockets, to avoid zsock_resolve
    void *receiver;
    void *router_receiver;
    void *router_output;
    void *publisher;
    void *compressor_input;
    void *compressor_output;
} publisher_state_t;


static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    zsock_t* pub_socket = arg;

    static size_t last_received_count   = 0;
    static size_t last_received_bytes   = 0;
    static size_t last_compressed_count = 0;
    static size_t last_compressed_bytes = 0;

    size_t message_count    = received_messages_count - last_received_count;
    size_t message_bytes    = received_messages_bytes - last_received_bytes;
    size_t compressed_count = compressed_messages_count - last_compressed_count;
    size_t compressed_bytes = compressed_messages_bytes - last_compressed_bytes;

    double avg_msg_size        = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size        = received_messages_max_bytes / 1024.0;
    double avg_compressed_size = compressed_count ? (compressed_bytes / 1024.0) / compressed_count : 0;
    double max_compressed_size = compressed_messages_max_bytes / 1024.0;

    printf("[I] processed %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           message_count, message_bytes/1024.0, avg_msg_size, max_msg_size);

    printf("[I] compressd %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           compressed_count, compressed_bytes/1024.0, avg_compressed_size, max_compressed_size);

    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    last_compressed_count = compressed_messages_count;
    last_compressed_bytes = compressed_messages_bytes;
    compressed_messages_max_bytes = 0;

    // update timestamp
    global_time = zclock_time();

    static size_t ticks = 0;

    // publish heartbeat
    if (++ticks % HEART_BEAT_INTERVAL == 0) {
        msg_meta.compression_method = NO_COMPRESSION;
        msg_meta.sequence_number++;
        msg_meta.created_ms = global_time;
        if (verbose)
            printf("[I] sending heartbeat\n");
        send_heartbeat(pub_socket, &msg_meta, pub_port);
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
        if (!zsock_rcvmore(socket))
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
        compressed_messages_count++;
        compressed_messages_bytes += msg_bytes;
        if (msg_bytes > compressed_messages_max_bytes)
            compressed_messages_max_bytes = msg_bytes;
    } else {
        received_messages_count++;
        received_messages_bytes += msg_bytes;
        if (msg_bytes > received_messages_max_bytes)
            received_messages_max_bytes = msg_bytes;
    }

    if (compression_method && !meta.compression_method) {
        publish_on_zmq_transport(&message_parts[0], state->compressor_input, &msg_meta, 0);
    } else {
        msg_meta.compression_method = meta.compression_method;
        msg_meta.sequence_number++;
        // my_zmq_msg_fprint(&message_parts[0], 3, "OUT", stdout);
        // dump_meta_info(&msg_meta);
        publish_on_zmq_transport(&message_parts[0], state->publisher, &msg_meta, ZMQ_DONTWAIT);
    }

 cleanup:
    for (;i>=0;i--) {
        zmq_msg_close(&message_parts[i]);
    }

    return 0;
}

static int read_router_message_and_forward(zloop_t *loop, zsock_t *socket, void *callback_data)
{
    publisher_state_t *state = (publisher_state_t*)callback_data;
    zmsg_t* msg = zmsg_recv(socket);
    assert(msg);

    zframe_t *sender_id = zmsg_pop(msg);
    zframe_t *empty = zmsg_first(msg);

    // if the second frame is empty, we need to send a reply
    if (zframe_size(empty) > 0)
        zframe_destroy(&sender_id);
    else {
        // pop the empty frame
        empty = zmsg_pop(msg);
        zmsg_t *reply = zmsg_new();
        zmsg_append(reply, &sender_id);
        zmsg_append(reply, &empty);

        // return bad request if we don't receive 4 frames and meta frame can't be decoded
        size_t n = zmsg_size(msg);
        msg_meta_t meta;
        bool decodable = n==4 && msg_extract_meta_info(msg, &meta);

        zframe_t *app_env = zmsg_first(msg);
        bool is_ping = zframe_streq(app_env, "ping");
        if (is_ping) {
            if (decodable) {
                zmsg_addstr(reply, "200 Pong");
                zmsg_addstr(reply, my_fqdn());
            } else
                zmsg_addstr(reply, "400 Bad Request");
        } else
            zmsg_addstr(reply, decodable ? "202 Accepted" : "400 Bad Request");

        int rc = zmsg_send_and_destroy(&reply, socket);
        if (rc)
            fprintf(stderr, "[E] could not send response (%d: %s)\n", errno, zmq_strerror(errno));

        // don't forward pings
        if (is_ping) return 0;
    }

    // put message back on to the event loop
    // TODO: this is slow. refactor to forward directly.
    int rc = zmsg_send_and_destroy(&msg, state->router_output);
    if (rc)
        fprintf(stderr, "[E] could not forward router message (%d: %s)\n", errno, zmq_strerror(errno));

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -d, --device-id N          device id (integer)\n"
            "  -i, --io-threads N         zeromq io threads\n"
            "  -p, --input-port N         port number of zeromq input socket\n"
            "  -q, --quiet                supress most output\n"
            "  -s, --compressors N        number of compressor threads\n"
            "  -t, --router-port N        port number of zeromq router socket\n"
            "  -v, --verbose              log more (use -vv for debug output)\n"
            "  -x, --compress M           compress logjam traffic using (snappy|zlib)\n"
            "  -P, --output-port N        port number of zeromq ouput socket\n"
            "  -R, --rcv-hwm N            high watermark for input socket\n"
            "  -S, --snd-hwm N            high watermark for output socket\n"
            "      --help                 display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
            "  LOGJAM_RCV_HWM             high watermark for input socket\n"
            "  LOGJAM_SND_HWM             high watermark for output socket\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    char *v;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "compress",      required_argument, 0, 'x' },
        { "device-id",     required_argument, 0, 'd' },
        { "router-port",   required_argument, 0, 't' },
        { "help",          no_argument,       0,  0  },
        { "input-port",    required_argument, 0, 'p' },
        { "io-threads",    required_argument, 0, 'i' },
        { "output-port",   required_argument, 0, 'P' },
        { "quiet",         no_argument,       0, 'q' },
        { "rcv-hwm",       required_argument, 0, 'R' },
        { "snd-hwm",       required_argument, 0, 'S' },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqd:p:c:i:x:s:P:S:R:t:", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug = true;
            else
                verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case 'd':
            msg_meta.device_number = atoi(optarg);
            snprintf(device_number_s, sizeof(device_number_s), "%d", msg_meta.device_number);
            break;
        case 'p':
            pull_port = atoi(optarg);
            break;
        case 'P':
            pub_port = atoi(optarg);
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
        case 't':
            router_port = atoi(optarg);
            break;
        case 'x':
            compression_method = string_to_compression_method(optarg);
            if (compression_method)
                printf("[I] compressing streams with: %s\n", compression_method_to_string(compression_method));
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
            if (strchr("drpceixsPSRE", optopt))
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

    printf("[I] started %s\n"
           "[I] pull-port:   %d\n"
           "[I] pub-port:    %d\n"
           "[I] router-port: %d\n"
           "[I] io-threads:  %lu\n"
           "[I] rcv-hwm:     %d\n"
           "[I] snd-hwm:     %d\n"
           , argv[0], pull_port, pub_port, router_port, io_threads, rcv_hwm, snd_hwm);

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    compression_buffer = zchunk_new(NULL, INITIAL_COMPRESSION_BUFFER_SIZE);

    // create socket to receive messages on
    zsock_t *receiver = zsock_new(ZMQ_PULL);
    assert_x(receiver != NULL, "zmq socket creation failed", __FILE__, __LINE__);

    //  configure the socket
    zsock_set_rcvhwm(receiver, rcv_hwm);

    // bind externally
    rc = zsock_bind(receiver, "tcp://%s:%d", "*", pull_port);
    assert_x(rc == pull_port, "receiver socket: external bind failed", __FILE__, __LINE__);

    // bind internally
    rc = zsock_bind(receiver, "inproc://receiver");
    assert_x(rc != -1, "receiver socket: internal bind failed", __FILE__, __LINE__);

    // create and bind socket for receiving logjam messages
    zsock_t *router_receiver = zsock_new(ZMQ_ROUTER);
    assert_x(router_receiver != NULL, "zmq socket creation failed", __FILE__, __LINE__);
    rc = zsock_bind(router_receiver, "tcp://%s:%d", "*", router_port);
    assert_x(rc == router_port, "receiver socket: external bind failed", __FILE__, __LINE__);

    // create router output socket and connect to the inproc receiver
    zsock_t *router_output = zsock_new(ZMQ_PUSH);
    assert_x(router_output != NULL, "zmq socket creation failed", __FILE__, __LINE__);
    rc = zsock_connect(router_output, "inproc://receiver");
    assert(rc == 0);

    // create socket for publishing
    zsock_t *publisher = zsock_new(ZMQ_PUB);
    assert_x(publisher != NULL, "publisher socket creation failed", __FILE__, __LINE__);
    zsock_set_sndhwm(publisher, snd_hwm);

    rc = zsock_bind(publisher, "tcp://%s:%d", "*", pub_port);
    assert_x(rc == pub_port, "publisher socket bind failed", __FILE__, __LINE__);

    // create compressor sockets
    zsock_t *compressor_input = zsock_new(ZMQ_PUSH);
    assert_x(compressor_input != NULL, "compressor input socket creation failed", __FILE__, __LINE__);
    rc = zsock_bind(compressor_input, "inproc://compressor-input");
    assert_x(rc==0, "compressor input socket bind failed", __FILE__, __LINE__);

    zsock_t *compressor_output = zsock_new(ZMQ_PULL);
    assert_x(compressor_output != NULL, "compressor output socket creation failed", __FILE__, __LINE__);
    rc = zsock_bind(compressor_output, "inproc://compressor-output");
    assert_x(rc==0, "compressor output socket bind failed", __FILE__, __LINE__);

    // create compressor agents
    zactor_t *compressors[MAX_COMPRESSORS];
    for (size_t i = 0; i < num_compressors; i++)
        compressors[i] = message_compressor_new(i, compression_method);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // setup publisher state
    publisher_state_t publisher_state = {
        .receiver = zsock_resolve(receiver),
        .router_receiver = zsock_resolve(router_receiver),
        .router_output = zsock_resolve(router_output),
        .publisher = zsock_resolve(publisher),
        .compressor_input = zsock_resolve(compressor_input),
        .compressor_output = zsock_resolve(compressor_output),
    };

    // calculate statistics every 1000 ms
    int timer_id = zloop_timer(loop, 1000, 0, timer_event, publisher);
    assert(timer_id != -1);

    // setup handler for compression results
    rc = zloop_reader(loop, compressor_output, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, compressor_output);

    // setup handler for incoming messages (all from the outside)
    rc = zloop_reader(loop, receiver, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // setup handler for event messages (all from the outside)
    rc = zloop_reader(loop, router_receiver, read_router_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, router_receiver);

    // initialize clock
    global_time = zclock_time();

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

    printf("[I] received %zu messages\n", received_messages_count);

    printf("[I] shutting down\n");

    zsock_destroy(&receiver);
    zsock_destroy(&router_receiver);
    zsock_destroy(&router_output);
    zsock_destroy(&publisher);
    zsock_destroy(&compressor_input);
    zsock_destroy(&compressor_output);
    for (size_t i = 0; i < num_compressors; i++)
        zactor_destroy(&compressors[i]);
    zsys_shutdown();

    printf("[I] %s terminated\n", argv[0]);

    return rc;
}
