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
#include "rabbitmq-listener.h"


/* global config */
static zconfig_t* config = NULL;
static zfile_t *config_file = NULL;
static char *config_file_name = "logjam.conf";
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";
// check every 10 ticks whether config file has changed
#define CONFIG_FILE_CHECK_INTERVAL 10

static int pull_port = 9605;
static int pub_port = 9606;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;

static msg_meta_t msg_meta = {0,0,0};

static void meta_dump_network(msg_meta_t *meta)
{
    // copy meta
    msg_meta_t m = *meta;
    meta_network_2_native(&m);
    printf("[D] device: %hu, sequence: %llu, created: %llu\n", m.device_number, m.sequence_number, m.created_ms);
}

typedef struct {
    // raw zmq sockets, to avoid zsock_resolve
    void *receiver;
    void *publisher;
} publisher_state_t;


inline void log_zmq_error(int rc)
{
    if (rc != 0) {
        fprintf(stderr, "[E] errno: %d: %s\n", errno, zmq_strerror(errno));
    }
}

static void config_file_init()
{
    config_file = zfile_new(NULL, config_file_name);
    config_file_last_modified = zfile_modified(config_file);
    config_file_digest = strdup(zfile_digest(config_file));
}

static bool config_file_has_changed()
{
    bool changed = false;
    zfile_restat(config_file);
    if (config_file_last_modified != zfile_modified(config_file)) {
        // bug in czmq: does not reset digest on restat
        zfile_t *tmp = zfile_new(NULL, config_file_name);
        char *new_digest = zfile_digest(tmp);
        // printf("[D] old digest: %s\n[D] new digest: %s\n", config_file_digest, new_digest);
        changed = strcmp(config_file_digest, new_digest) != 0;
        zfile_destroy(&tmp);
    }
    return changed;
}

static int publish_on_zmq_transport(zmq_msg_t *message_parts, void *publisher)
{
    int rc=0;
    zmq_msg_t *app_env = &message_parts[0];
    zmq_msg_t *key     = &message_parts[1];
    zmq_msg_t *body    = &message_parts[2];

    rc = zmq_msg_send(app_env, publisher, ZMQ_SNDMORE|ZMQ_DONTWAIT);
    if (rc == -1) {
        log_zmq_error(rc);
        return rc;
    }
    rc = zmq_msg_send(key, publisher, ZMQ_SNDMORE|ZMQ_DONTWAIT);
    if (rc == -1) {
        log_zmq_error(rc);
        return rc;
    }
    rc = zmq_msg_send(body, publisher, ZMQ_SNDMORE|ZMQ_DONTWAIT);
    if (rc == -1) {
        log_zmq_error(rc);
        return rc;
    }

    msg_meta.sequence_number++;
    zmq_msg_t meta;
    zmq_msg_init_size(&meta, sizeof(msg_meta));
    memcpy(zmq_msg_data(&meta), &msg_meta, sizeof(msg_meta));
    meta_native_2_network(zmq_msg_data(&meta));

    if (0) meta_dump_network(zmq_msg_data(&meta));

    rc = zmq_msg_send(&meta, publisher, ZMQ_DONTWAIT);
    if (rc == -1) {
        log_zmq_error(rc);
    }
    zmq_msg_close(&meta);
    return rc;
}


static int timer_event(zloop_t *loop, int timer_id, void *arg)
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

    msg_meta.created_ms = zclock_time();

    static size_t ticks = 0;
    bool terminate = (++ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    if (terminate) {
        printf("[I] detected config change. terminating.\n");
        zctx_interrupted = 1;
    }

    return 0;
}

static int read_zmq_message_and_forward(zloop_t *loop, zsock_t *_receiver, void *callback_data)
{
    int i = 0;
    zmq_msg_t message_parts[4];
    publisher_state_t *state = (publisher_state_t*)callback_data;
    void *receiver = state->receiver;
    void *publisher = state->publisher;

    // read the message parts
    while (!zctx_interrupted) {
        // printf("[D] receiving part %d\n", i+1);
        if (i>3) {
            zmq_msg_t dummy_msg;
            zmq_msg_init(&dummy_msg);
            zmq_recvmsg(receiver, &dummy_msg, 0);
            zmq_msg_close(&dummy_msg);
        } else {
            zmq_msg_init(&message_parts[i]);
            zmq_recvmsg(receiver, &message_parts[i], 0);
        }
        if (!zsocket_rcvmore(receiver))
            break;
        i++;
    }
    if (i<2) {
        if (!zctx_interrupted) {
            fprintf(stderr, "[E] received only %d message parts\n", i);
        }
        goto cleanup;
    } else if (i>3) {
        fprintf(stderr, "[E] received more than 4 message parts\n");
        goto cleanup;
    }

    size_t msg_bytes = zmq_msg_size(&message_parts[2]);
    received_messages_count++;
    received_messages_bytes += msg_bytes;
    if (msg_bytes > received_messages_max_bytes)
        received_messages_max_bytes = msg_bytes;

    publish_on_zmq_transport(&message_parts[0], publisher);

 cleanup:
    for (;i>=0;i--) {
        zmq_msg_close(&message_parts[i]);
    }

    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-d device number] [-r rabbit-host] [-p pull-port] [-c config-file] [-e environment]\n", argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "d:r:p:c:e:")) != -1) {
        switch (c) {
        case 'd':
            msg_meta.device_number = atoi(optarg);
            break;
        case 'r':
            rabbit_host = optarg;
            break;
        case 'e':
            rabbit_env = optarg;
            break;
        case 'p':
            pull_port = atoi(optarg);
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case '?':
            if (optopt == 'c' || optopt == 'p' || optopt == 'r' || optopt == 'e')
                fprintf(stderr, "option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            exit(1);
        }
    }
}

int main(int argc, char * const *argv)
{
    int rc=0;
    process_arguments(argc, argv);

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    // TODO: figure out sensible port numbers
    pub_port = pull_port + 1;

    printf("[I] started logjam-device\n"
           "[I] pull-port:   %d\n"
           "[I] pub-port:    %d\n"
           "[I] rabbit-host: %s\n",
           pull_port, pub_port, rabbit_host);

    // load config
    if (zsys_file_exists(config_file_name)) {
        config_file_init();
        config = zconfig_load((char*)config_file_name);
    }

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    // zsys_set_io_threads(2);

    // create socket to receive messages on
    zsock_t *receiver = zsock_new(ZMQ_PULL);
    assert_x(receiver != NULL, "zmq socket creation failed");

    //  configure the socket
    zsock_set_rcvhwm(receiver, 100000);

    // bind externally
    rc = zsock_bind(receiver, "tcp://%s:%d", "*", pull_port);
    assert_x(rc == pull_port, "receiver socket: external bind failed");

    // bind internally
    rc = zsock_bind(receiver, "inproc://receiver");
    assert_x(rc != -1, "receiver socket: internal bind failed");

    // create socket for publishing
    zsock_t *publisher = zsock_new(ZMQ_PUB);
    assert_x(publisher != NULL, "socket creation failed");
    zsock_set_sndhwm(publisher, 100000);

    rc = zsock_bind(publisher, "tcp://%s:%d", "*", pub_port);
    assert_x(rc == pub_port, "socket bind failed");

    // setup the rabbitmq listener thread
    zactor_t *rabbit_listener = NULL;

    if (rabbit_host != NULL) {
        if (config == NULL) {
            fprintf(stderr, "[E] cannot start rabbitmq listener thread because no config file given\n");
            goto cleanup;
        } else {
            rabbit_listener = zactor_new(rabbitmq_listener, config);
            assert_x(rabbit_listener != NULL, "could not fork rabbitmq listener thread");
            printf("[I] created rabbitmq listener thread\n");
        }
    }

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
    };
    rc = zloop_reader(loop, receiver, read_zmq_message_and_forward, &publisher_state);
    assert(rc == 0);
    zloop_reader_set_tolerant(loop, receiver);

    // initialize clock
    msg_meta.created_ms = zclock_time();

    printf("starting main event loop\n");
    rc = zloop_start(loop);
    printf("main event zloop terminated with return code %d\n", rc);
    assert(rc == 0);

    zloop_destroy(&loop);
    assert(loop == NULL);

    printf("[I] received %zu messages\n", received_messages_count);

 cleanup:
    printf("[I] shutting down\n");

    zactor_destroy(&rabbit_listener);
    zsock_destroy(&receiver);
    zsock_destroy(&publisher);
    zsys_shutdown();

    printf("[I] terminated\n");

    return rc;
}
