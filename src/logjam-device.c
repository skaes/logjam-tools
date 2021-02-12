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
#include "importer-watchdog.h"
#include "device-prometheus-client.h"
#ifdef HAVE_MALLOC_TRIM
#include <malloc.h>
#endif

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
static size_t ping_count_total = 0;
static size_t invalid_messages_count_total = 0;
static size_t broken_meta_count_total = 0;

static size_t compressed_messages_count = 0;
static size_t compressed_messages_bytes = 0;
static size_t compressed_messages_max_bytes = 0;

static size_t io_threads = 1;
static size_t num_compressors = 4;

static bool allow_invalid_meta = false;

static msg_meta_t msg_meta = META_INFO_EMPTY;
static char device_number_s[11] = {'0', 0};

#define MAX_COMPRESSORS 64
static zactor_t *compressors[MAX_COMPRESSORS];
static int compression_method = NO_COMPRESSION;
static zchunk_t *compression_buffer;
static uint64_t global_time = 0;

static zactor_t *device_watchdog = NULL;

int metrics_port = 8082;
char metrics_address[256] = {0};
const char *metrics_ip = "0.0.0.0";

typedef struct {
    char *app_env;
    int64_t last_seen;
} app_env_record_t;

static zhashx_t *routing_id_to_app_env= NULL;

static void free_app_env_record(void *self)
{
    app_env_record_t *r = self;
    free(r->app_env);
    free(r);
}

static void clean_old_routing_id_entries(int64_t max_age)
{
    zlist_t *deletions = zlist_new();
    int64_t threshold = zclock_time() - max_age;
    app_env_record_t* r = zhashx_first(routing_id_to_app_env);
    while (r) {
        if (r->last_seen < threshold) {
            const char *key = zhashx_cursor(routing_id_to_app_env);
            zlist_append(deletions, (void*)key);
        }
        r = zhashx_next(routing_id_to_app_env);
    }
    r = zlist_first(deletions);
    while (r) {
        zhashx_delete(routing_id_to_app_env, r);
        r = zlist_next(deletions);
    }
    zlist_destroy(&deletions);
}

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
    static size_t last_ping_count = 0;
    static size_t last_invalid_count = 0;
    static size_t last_broken_meta_count = 0;

    size_t message_count     = received_messages_count - last_received_count;
    size_t message_bytes     = received_messages_bytes - last_received_bytes;
    size_t compressed_count  = compressed_messages_count - last_compressed_count;
    size_t compressed_bytes  = compressed_messages_bytes - last_compressed_bytes;
    size_t ping_count        = ping_count_total - last_ping_count;
    size_t invalid_count     = invalid_messages_count_total - last_invalid_count;
    size_t broken_meta_count = broken_meta_count_total - last_broken_meta_count;

    device_prometheus_client_count_msgs_received(message_count);
    device_prometheus_client_count_bytes_received(message_bytes);
    device_prometheus_client_count_msgs_compressed(compressed_count);
    device_prometheus_client_count_bytes_compressed(compressed_bytes);
    device_prometheus_client_count_pings(ping_count);
    device_prometheus_client_count_invalid_messages(invalid_count);
    device_prometheus_client_count_broken_metas(broken_meta_count);
    device_prometheus_client_record_rusage();

    double avg_msg_size        = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size        = received_messages_max_bytes / 1024.0;
    double avg_compressed_size = compressed_count ? (compressed_bytes / 1024.0) / compressed_count : 0;
    double max_compressed_size = compressed_messages_max_bytes / 1024.0;

    printf("[I] processed %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           message_count, message_bytes/1024.0, avg_msg_size, max_msg_size);

    printf("[I] compressd %zu messages (%.2f KB), avg: %.2f KB, max: %.2f KB\n",
           compressed_count, compressed_bytes/1024.0, avg_compressed_size, max_compressed_size);

    printf("[I] pings: %zu, invalid msgs: %zu, broken metas: %zu\n",
           ping_count, invalid_count, broken_meta_count);

    last_received_count = received_messages_count;
    last_ping_count = ping_count_total;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    last_compressed_count = compressed_messages_count;
    last_compressed_bytes = compressed_messages_bytes;
    compressed_messages_max_bytes = 0;
    last_invalid_count = invalid_messages_count_total;
    last_broken_meta_count = broken_meta_count_total;

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

    // tick compressors
    for (size_t i = 0; i < num_compressors; i++)
        zstr_send(compressors[i], "tick");

    // tick watchdog
    zstr_send(device_watchdog, "tick");

    // delete old ping counters and broken meta counters, once per minute.
    if (ticks % 60 == 0) {
        int64_t max_age = 1000 * (debug ? 60 : 60 * 60);
        // max age is given in milliseconds.
        clean_old_routing_id_entries(max_age);
        device_prometheus_client_delete_old_ping_counters(max_age);
        device_prometheus_client_delete_old_broken_meta_counters(max_age);
    }

#ifdef HAVE_MALLOC_TRIM
    // try to reduce memory usage. unclear whether this helps at all.
    if (malloc_trim_frequency > 0 && ticks % malloc_trim_frequency == 0 && !zsys_interrupted)
         malloc_trim(0);
#endif

    return 0;
}

static int read_multipart_msg(void* socket, zmq_msg_t *parts, int n, int* read)
{
    int i = 0;
    int rc = 0;
    // read the message parts
    while (1) {
        // printf("[D] receiving part %d\n", i+1);
        if (i<n) {
            zmq_msg_init(&parts[i]);
            rc = zmq_recvmsg(socket, &parts[i], 0);
        } else {
            zmq_msg_t dummy_msg;
            zmq_msg_init(&dummy_msg);
            rc = zmq_recvmsg(socket, &dummy_msg, 0);
            zmq_msg_close(&dummy_msg);
        }
        if (rc == -1) {
            if (i<n)
                zmq_msg_close(&parts[i]);
            if (errno == EINTR) {
                if (i == 0) {
                    *read = 0;
                    return -1;
                } else {
                    continue;
                }
            } else {
                *read = i+1;
                return -1;
            }
        }
        if (!zsock_rcvmore(socket))
            break;
        i++;
    }
    *read = i+1;
    return 0;
}

static bool warn_msg_size(zmq_msg_t *parts, int n, int min, int max)
{
    if (n<min) {
        if (!zsys_interrupted) {
            fprintf(stderr, "[E] received less than the expected %d message parts: got %d\n", min, n);
            my_zmq_msg_fprint(parts, n, "[E] MSG", stderr);
        }
        return true;
    }
    if (n>max) {
        fprintf(stderr, "[E] received more than the expectd %d message parts: got %d\n", max, n);
        my_zmq_msg_fprint(parts, n, "[E] MSG", stderr);
        return true;
    }
    return false;
}

static void update_message_stats(void* socket, publisher_state_t *state, zmq_msg_t* body)
{
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
}

static void compress_or_forward(zmq_msg_t* parts,  msg_meta_t *meta, publisher_state_t *state)
{
    if (meta->created_ms)
        msg_meta.created_ms = meta->created_ms;
    else
        msg_meta.created_ms = global_time;

    if (compression_method && !meta->compression_method) {
        publish_on_zmq_transport(&parts[0], state->compressor_input, &msg_meta, 0);
    } else {
        msg_meta.compression_method = meta->compression_method;
        msg_meta.sequence_number++;
        // my_zmq_msg_fprint(&parts[0], 3, "OUT", stdout);
        // dump_meta_info("META", &msg_meta);
        publish_on_zmq_transport(&parts[0], state->publisher, &msg_meta, ZMQ_DONTWAIT);
    }
}

static void record_broken_meta(zmq_msg_t *stream_part)
{
    int n = zmq_msg_size(stream_part);
    unsigned char* data = zmq_msg_data(stream_part);
    char app_env[n+1];
    memcpy(app_env, data, n);
    app_env[n] = '\0';
    device_prometheus_client_count_broken_meta(app_env);
}

static int read_zmq_message_and_forward(zloop_t *loop, zsock_t *sock, void *callback_data)
{
    zmq_msg_t message_parts[10];
    publisher_state_t *state = (publisher_state_t*)callback_data;
    void *socket = zsock_resolve(sock);

    int n;
    int rc = read_multipart_msg(socket, message_parts, 10, &n);
    if (rc) {
        fprintf(stderr, "[E] unexpected error on recv: %d (%s)\n", errno, zmq_strerror(errno));
        goto cleanup;
    }

    // The old pull socket interface did not require meta information to be sent and there
    // might be some old clients left. Otherwise we'd demand 4 parts here.
    if (warn_msg_size(message_parts, n, 3, 4)) {
        invalid_messages_count_total++;
        goto cleanup;
    }

    msg_meta_t meta = META_INFO_EMPTY;
    if (n==4) {
        if (!zmq_msg_extract_meta_info(&message_parts[3], &meta)) {
            invalid_messages_count_total++;
            broken_meta_count_total++;
            record_broken_meta(&message_parts[0]);
            if (verbose) {
                fprintf(stderr, "[W] meta info could not be decoded: %d\n", n);
                my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
            }
            if (!allow_invalid_meta)
                goto cleanup;
        }
    }

    zmq_msg_t *app_env = &message_parts[0];
    if (!well_formed_stream_name(zmq_msg_data(app_env), zmq_msg_size(app_env))) {
        invalid_messages_count_total++;
        fprintf(stderr, "[E] malformed stream name\n");
        my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
        goto cleanup;
    }

    zmq_msg_t *topic = &message_parts[1];
    if (!well_formed_topic(zmq_msg_data(topic), zmq_msg_size(topic))) {
        invalid_messages_count_total++;
        fprintf(stderr, "[E] malformed routing key\n");
        my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
        goto cleanup;
    }

    update_message_stats(socket, state, &message_parts[2]);
    compress_or_forward(message_parts, &meta, state);

 cleanup:
    for (int i=n-1; i>=0; i--)
        zmq_msg_close(&message_parts[i]);

    return 0;
}

static void record_routing_id_and_app_env(zmq_msg_t *sender_id, const char *stream, int m)
{
    int n = zmq_msg_size(sender_id);
    char routing_id[2*n+1];
    unsigned char* data = zmq_msg_data(sender_id);
    for (int i=0; i<n; i++)
        sprintf(&routing_id[2*i], "%02X", data[i]);
    if (debug) {
        char stream_str[m+1];
        memcpy(stream_str, stream, m);
        stream_str[m] = '\0';
        printf("[D] routingid[%d], stream[%d]: %s,%s\n", 2*n, m, routing_id, stream_str);
    }
    if (!zhashx_lookup(routing_id_to_app_env, routing_id)) {
        char stream_str[m+1];
        memcpy(stream_str, stream, m);
        stream_str[m] = '\0';
        app_env_record_t *r = zmalloc(sizeof(*r));
        r->app_env = strdup(stream_str);
        zhashx_update(routing_id_to_app_env, routing_id, r);
        zhashx_freefn(routing_id_to_app_env, routing_id, free_app_env_record);
    }
}

static void record_ping(zmq_msg_t *sender_id, const char *routing_key, int routing_key_len)
{
    if (routing_key_len > 0) {
        // application sent app-env as the routing key
        char app_env[routing_key_len+1];
        memcpy(app_env, routing_key, routing_key_len);
        app_env[routing_key_len] = '\0';
        device_prometheus_client_count_ping(app_env);
    } else {
        int sender_id_len = zmq_msg_size(sender_id);
        char routing_id[2*sender_id_len+1];
        unsigned char* data = zmq_msg_data(sender_id);
        for (int i=0; i<sender_id_len; i++)
            sprintf(&routing_id[2*i], "%02X", data[i]);
        const char* app_env = "unknown-unknown";
        app_env_record_t *r = zhashx_lookup(routing_id_to_app_env, routing_id);
        if (r) {
            r->last_seen = zclock_time();
            app_env = r->app_env;
        }
        device_prometheus_client_count_ping(app_env);
    }
}

static int read_router_message_and_forward(zloop_t *loop, zsock_t *sock, void *callback_data)
{
    zmq_msg_t message_parts[12];
    publisher_state_t *state = (publisher_state_t*)callback_data;
    void *socket = zsock_resolve(sock);

    int n;
    int rc = read_multipart_msg(socket, message_parts, 12, &n);
    if (rc) {
        fprintf(stderr, "[E] unexpected error on recv: %d (%s)\n", errno, zmq_strerror(errno));
        goto cleanup;
    }

    // my_zmq_msg_fprint(message_parts, n, "[D] MSG", stderr);

    zmq_msg_t *routing_id = &message_parts[0];

    // if the second frame is present and empty, we need to send a reply, no matter how
    // broken the whole message is
    bool send_reply = n > 1 && zmq_msg_size(&message_parts[1]) == 0;
    int app_env_index = send_reply ? 2 : 1;
    int expected_parts = 4 + app_env_index;

    // try to decode meta information if we have enough frames
    msg_meta_t meta = META_INFO_EMPTY;
    bool decoded = false;
    if (n == expected_parts) {
        decoded = zmq_msg_extract_meta_info(&message_parts[app_env_index+3], &meta);
        if (!decoded) {
            broken_meta_count_total++;
            record_broken_meta(&message_parts[app_env_index]);
        }
    }

    // determine stream, if possible
    char *app_env;
    int app_env_len;
    if (app_env_index < n) {
        app_env = zmq_msg_data(&message_parts[app_env_index]);
        app_env_len = zmq_msg_size(&message_parts[app_env_index]);
    } else {
        app_env = "unknown-unknown";
        app_env_len = 15;
    }
    bool is_ping = app_env_len == 4 && !strncmp(app_env, "ping", 4);
    bool valid_stream = is_ping || (app_env_index < n && well_formed_stream_name(app_env, app_env_len));
    zmq_msg_t *topic = &message_parts[app_env_index+1];
    bool valid_topic = is_ping || (app_env_index+1 < n && well_formed_topic(zmq_msg_data(topic), zmq_msg_size(topic)));

    if (!send_reply) {
        record_routing_id_and_app_env(routing_id, app_env, app_env_len);
    } else {
        zmsg_t *reply = zmsg_new();
        zmsg_addmem(reply, zmq_msg_data(routing_id), zmq_msg_size(routing_id));
        zmsg_addmem(reply, app_env, app_env_len);

        // return bad request if we didn't get enough frames or the meta frame can't be
        // decoded or stream name is not well formed
        if (is_ping) {
            valid_stream = true;
            ping_count_total++;
            if (decoded) {
                zmsg_addstr(reply, "200 OK");
                zmsg_addstr(reply, my_fqdn());
            } else {
                zmsg_addstr(reply, "400 Bad Request");
            }
            if (valid_stream && (app_env_index+1 < n)) {
                record_ping(routing_id, zmq_msg_data(topic), zmq_msg_size(topic));
            }
        } else {
            // a normal message, but asking for a reply
            if (valid_stream) {
                zmsg_addstr(reply, decoded ? "202 Accepted" : "400 Bad Request");
                record_routing_id_and_app_env(routing_id, app_env, app_env_len);
            } else
                zmsg_addstr(reply, "400 Bad Request");
        }

        int rc = zmsg_send_and_destroy(&reply, sock);
        if (rc)
            fprintf(stderr, "[E] could not send response (%d: %s)\n", errno, zmq_strerror(errno));
    }

    if (warn_msg_size(message_parts, n, expected_parts, expected_parts)) {
        invalid_messages_count_total++;
        goto cleanup;
    }

    if (!decoded) {
        invalid_messages_count_total++;
        if (verbose) {
            fprintf(stderr, "[E] meta info could not be decoded: %d\n", n);
            my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
        }
        if (!allow_invalid_meta)
            goto cleanup;
    }

    if (!valid_stream) {
        invalid_messages_count_total++;
        fprintf(stderr, "[E] malformed stream name\n");
        my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
        goto cleanup;
    }

    if (!is_ping) {
        if (!valid_topic) {
            invalid_messages_count_total++;
            fprintf(stderr, "[E] malformed routing key\n");
            my_zmq_msg_fprint(message_parts, n, "[E] MSG", stderr);
            goto cleanup;
        }

        zmq_msg_t *body = &message_parts[app_env_index+2];
        update_message_stats(socket, state, body);
        compress_or_forward(message_parts+app_env_index, &meta, state);
    }

 cleanup:
    for (int i=n-1; i>=0; i--)
        zmq_msg_close(&message_parts[i]);

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
            "  -m, --metrics-port N       port to use for prometheus path /metrics\n"
            "  -M, --metrics-ip N         ip for binding metrics endpoint\n"
            "  -T, --trim-frequency N     malloc trim freqency in seconds, 0 means no trimming\n"
            "  -A, --allow-invalid-meta   allow invalid meta data\n"
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
        { "compress",           required_argument, 0, 'x' },
        { "device-id",          required_argument, 0, 'd' },
        { "router-port",        required_argument, 0, 't' },
        { "help",               no_argument,       0,  0  },
        { "input-port",         required_argument, 0, 'p' },
        { "io-threads",         required_argument, 0, 'i' },
        { "output-port",        required_argument, 0, 'P' },
        { "quiet",              no_argument,       0, 'q' },
        { "rcv-hwm",            required_argument, 0, 'R' },
        { "snd-hwm",            required_argument, 0, 'S' },
        { "verbose",            no_argument,       0, 'v' },
        { "metrics-port",       required_argument, 0, 'm' },
        { "metrics-ip",         required_argument, 0, 'M' },
        { "trim-frequency",     required_argument, 0, 'T' },
        { "allow-invalid-meta", no_argument,       0, 'A' },
        { 0,                    0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqd:p:c:i:x:s:P:S:R:t:m:M:T:A", long_options, &longindex)) != -1) {
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
        case 'm':
            metrics_port = atoi(optarg);
            break;
        case 'M': {
            metrics_ip = optarg;
            break;
        }
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
        case 'T':
            malloc_trim_frequency = atoi(optarg);
            break;
        case 'A':
            allow_invalid_meta = true;
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("dpcixsPSRt", optopt))
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
           "[I] pull-port:    %d\n"
           "[I] pub-port:     %d\n"
           "[I] router-port:  %d\n"
           "[I] metrics-port: %d\n"
           "[I] io-threads:   %lu\n"
           "[I] rcv-hwm:      %d\n"
           "[I] snd-hwm:      %d\n"
           , argv[0], pull_port, pub_port, router_port, metrics_port, io_threads, rcv_hwm, snd_hwm);

    // set global config
    zsys_init();
    zsys_set_rcvhwm(10000);
    zsys_set_sndhwm(10000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(100);
    zsys_set_io_threads(io_threads);

    compression_buffer = zchunk_new(NULL, INITIAL_COMPRESSION_BUFFER_SIZE);
    routing_id_to_app_env = zhashx_new();

    // initalize prometheus client
    snprintf(metrics_address, sizeof(metrics_address), "%s:%d", metrics_ip, metrics_port);
    device_prometheus_client_init(metrics_address, device_number_s, num_compressors);

    device_prometheus_set_start_time();

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
    for (size_t i = 0; i < num_compressors; i++)
        compressors[i] = message_compressor_new(i, compression_method, device_prometheus_client_record_rusage_compressor);

    // create watchdog
    device_watchdog = watchdog_new(10, 1, 0);

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

    watchdog_destroy(&device_watchdog);
    zsock_destroy(&receiver);
    zsock_destroy(&router_receiver);
    zsock_destroy(&router_output);
    zsock_destroy(&publisher);
    zsock_destroy(&compressor_input);
    zsock_destroy(&compressor_output);
    for (size_t i = 0; i < num_compressors; i++)
        zactor_destroy(&compressors[i]);
    zsys_shutdown();

    device_prometheus_client_shutdown();

    printf("[I] %s terminated\n", argv[0]);

    return rc;
}
