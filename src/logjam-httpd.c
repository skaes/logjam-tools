#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <getopt.h>
#include <json-c/json.h>
#include <zlib.h>
#include "logjam-util.h"

static bool verbose = false;
static bool debug = false;
static bool quiet = false;
static bool debug_compress = false;

#define DEFAULT_RCV_HWM 100000
#define DEFAULT_SND_HWM 100000

static int rcv_hwm = -1;
static int snd_hwm = -1;

static int io_threads = 1;

static char http_response_ok [] =
    "HTTP/1.1 200 OK\r\n"
    "Cache-Control: private\r\n"
    "Content-Disposition: inline\r\n"
    "Content-Transfer-Encoding: base64\r\n"
    "Content-Type: image/gif\r\n"
    "Content-Length: 60\r\n"
    "Connection: close\r\n"
    "\r\n"
    "R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";

static char http_response_fail [] =
    "HTTP/1.1 400 RTFM\r\n"
    "Cache-Control: private\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 0\r\n"
    "Connection: close\r\n"
    "\r\n";

static char http_response_alive [] =
    "HTTP/1.1 200 OK\r\n"
    "Cache-Control: private\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 6\r\n"
    "Connection: close\r\n"
    "\r\n"
    "ALIVE\n";

static size_t ok_length, fail_length, alive_length;

#define MAX_ID_SIZE 256
#define MAX_REQUEST_SIZE 8192

static int http_port = 9705;
static int pub_port = 9706;
static char* capture_file_name = NULL;
static FILE* capture_file = NULL;

static void *http_socket = NULL;
static void *pub_socket = NULL;
static zsock_t *http_socket_wrapper = NULL;
static zsock_t *pub_socket_wrapper = NULL;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;
static size_t http_failures = 0;
static size_t dropped_messages_count = 0;

static char path_prefix_ajax[]  = "GET /logjam/ajax?";
static char path_prefix_page[]  = "GET /logjam/page?";
static char path_prefix_alive[] = "GET /alive.txt ";
static int path_prefix_length;
static int path_prefix_alive_length;

static msg_meta_t msg_meta = META_INFO_EMPTY;
static int compression = NO_COMPRESSION;
static zchunk_t* compression_buffer = NULL;
static zchunk_t* decompression_buffer = NULL;

typedef struct {
    char app[256];
    char env[256];
    const char *msg_type;
    char routing_key[256+17];
    int routing_key_len;
    char *json_str;
    int json_len;
} msg_data_t;

static char current_time_as_string[26];  // updated once per second

static void set_started_at()
{
    // update current time
    time_t now = time(NULL);
    struct tm *tm_now = localtime(&now);
    strftime(current_time_as_string, sizeof(current_time_as_string), "%Y-%m-%dT%H:%M:%S%z", tm_now);
}

static zhash_t *integer_conversions = NULL;

static void setup_integer_conversions()
{
    integer_conversions = zhash_new();
    zhash_insert(integer_conversions, "viewport_height", (void*)1);
    zhash_insert(integer_conversions, "viewport_width", (void*)1);
    zhash_insert(integer_conversions, "html_nodes", (void*)1);
    zhash_insert(integer_conversions, "script_nodes", (void*)1);
    zhash_insert(integer_conversions, "style_nodes", (void*)1);
    zhash_insert(integer_conversions, "v", (void*)1);
}

static inline
bool convert_to_integer(const char* key)
{
    return zhash_lookup(integer_conversions, key);
}

static inline
const char *json_get_value(json_object *json, const char* key)
{
    json_object *json_value_object = NULL;
    json_object_object_get_ex(json, key, &json_value_object);
    return json_object_get_string(json_value_object);
}

static
void parse_query(char *s, json_object *json)
{
    char *key;
    char *val;
    int c;
    char buf[3];
    // we know the value can't be longer than the whole request
    char value[MAX_REQUEST_SIZE];
    int offset = 0;

    key = s;
    while (*s && (*s != '=')) s++;
    if (!*s) {
        if (debug)
            printf("[E] no parameters\n");
        return;
    }
    *(s++) = '\0';

    for (val=s; *val; val++) {
        switch (*val) {
        case '%':
            buf[0] = *(++val);
            buf[1] = *(++val);
            buf[2] = '\0';
            sscanf(buf, "%2x", &c);
            break;
        case '+':
            c = ' ';
            break;
        default:
            c = *val;
        }
        value[offset++] = c;
    }
    value[offset] = '\0';
    // printf("[D] %s=%s\n", key, value);

    if (convert_to_integer(key)) {
        int64_t val = atol(value);
        json_object_object_add(json, key, json_object_new_int64(val));
    } else {
        json_object_object_add(json, key, json_object_new_string_len(value, offset));
    }
}

static
void parse_header_line(char *s, json_object *json)
{
    // printf("[D] HEADERLINE: %s\n", s);
    if (!strncasecmp(s, "User-Agent:", 11)) {
        char *agent = s+11;
        while (*agent == ' ') agent++;
        // json_object_new_string makes a copy of its parameter
        json_object_object_add(json, "user_agent", json_object_new_string(agent));
    }
}

static
void init_globals()
{
    int rc;
    set_started_at();
    setup_integer_conversions();

    ok_length = strlen (http_response_ok);
    fail_length = strlen (http_response_fail);
    alive_length = strlen (http_response_alive);
    path_prefix_length = strlen (path_prefix_ajax);
    path_prefix_alive_length = strlen (path_prefix_alive);

    // create ZMQ_STREAM socket
    http_socket_wrapper = zsock_new (ZMQ_STREAM);
    assert (http_socket_wrapper);
    http_socket = zsock_resolve (http_socket_wrapper);
    assert (http_socket);
    // make sure the http_socket blocks for at most 10ms when sending answers
    zsock_set_sndtimeo(http_socket_wrapper, 10);
    // limit size of incoming messages to protect against malicious users
    zsock_set_maxmsgsize(http_socket_wrapper, MAX_REQUEST_SIZE);
    // make max number of outstanding connections larger than the default 100
    zsock_set_backlog(http_socket_wrapper, 4096);

    // bind http socket
    rc = zsock_bind (http_socket_wrapper, "tcp://*:%d", http_port);
    assert (rc == http_port);

    // create ZMQ_PUB socket
    pub_socket_wrapper = zsock_new (ZMQ_PUB);
    assert (pub_socket_wrapper);
    pub_socket = zsock_resolve (pub_socket_wrapper);
    assert (pub_socket);

    // bind for downstream devices / logjam importer
    rc = zsock_bind(pub_socket_wrapper, "tcp://*:%d", pub_port);
    assert (rc == pub_port);

    // setup zchunks for compressing/decompressing json data
    compression_buffer = zchunk_new(NULL, INITIAL_COMPRESSION_BUFFER_SIZE);
    decompression_buffer = zchunk_new(NULL, INITIAL_COMPRESSION_BUFFER_SIZE);
}

static inline
bool extract_msg_data(char *query_string, char* headers, msg_data_t *msg_data)
{
    bool valid = false;
    json_object *json = json_object_new_object();

    // parse query string and extract parameters
    char *phrase = strtok(query_string, "&");
    while (phrase) {
        parse_query(phrase, json);
        phrase = strtok(NULL, "&");
    }

    // parse headers and extract user agent
    if (headers) {
        phrase = strtok(headers, "\r\n");
        while (phrase) {
            parse_header_line(phrase, json);
            phrase = strtok(NULL, "\r\n");
        }
    }

    // add time info
    msg_meta.created_ms = zclock_time();
    json_object_object_add(json, "started_ms", json_object_new_int64(msg_meta.created_ms));
    json_object_object_add(json, "started_at", json_object_new_string(current_time_as_string));

    const char *json_string = json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
    assert(json_string);
    msg_data->json_str = strdup(json_string);
    msg_data->json_len = strlen(json_string);
    // printf("[D] json: %s\n", msg_data->json_str);

    // check version
    const char *version = json_get_value(json, "v");
    if (!version) {
        // printf("[D] missing version\n");
        goto cleanup;
    } else if (strcmp(version, "1")) {
        // printf("[D] wrong version: %s\n", version);
        goto cleanup;
    }

    // get request id
    const char *request_id = json_get_value(json, "logjam_request_id");
    if (!request_id) {
        // printf("[D] missing request id\n");
        goto cleanup;
    }

    // get action
    const char *action = json_get_value(json, "logjam_action");
    if (!action) {
        // printf("[D] missing action\n");
        goto cleanup;
    }

    // extract app and env
    if (strlen(request_id) > 255
        || sscanf(request_id, "%[^-]-%[^-]", msg_data->app, msg_data->env) != 2) {
        goto cleanup;
    };
    // if we get here, we have a valid json object
    valid = true;

 cleanup:
    // free json object
    json_object_put(json);

    return valid;
}


static
void send_logjam_message(msg_data_t *data, msg_meta_t *meta)
{
    char app_env[256];
    int app_env_len = sprintf(app_env, "%s-%s", data->app, data->env);

    data->routing_key_len = sprintf(data->routing_key, "frontend.%s.%s.%s", data->msg_type, data->app, data->env);

    zmq_msg_t message_parts[4];
    zmq_msg_init_size(&message_parts[0], app_env_len);
    memcpy(zmq_msg_data(&message_parts[0]), app_env, app_env_len);

    zmq_msg_init_size(&message_parts[1], data->routing_key_len);
    memcpy(zmq_msg_data(&message_parts[1]), data->routing_key, data->routing_key_len);

    if (compression) {
        zmq_msg_init(&message_parts[2]);
        compress_message_data(compression, compression_buffer, &message_parts[2], data->json_str, data->json_len);
    } else {
        zmq_msg_init_size(&message_parts[2], data->json_len);
        memcpy(zmq_msg_data(&message_parts[2]), data->json_str, data->json_len);
    }

    msg_meta.compression_method = compression;
    msg_meta.sequence_number++;

    if (debug || (compression && debug_compress)) {
        printf("SENDING ====================================\n");
        my_zmq_msg_fprint(&message_parts[0], 3, "[D]", stdout);
        dump_meta_info("[D]", &msg_meta);
        if (compression) {
            printf("[D] ORIGINAL: %.*s\n", data->json_len, data->json_str);
            if (debug_compress) {
                zframe_t *body_frame = zframe_new(zmq_msg_data(&message_parts[2]), zmq_msg_size(&message_parts[2]));
                char *body = NULL;
                size_t body_len = 0;
                int rc = decompress_frame(body_frame, compression, decompression_buffer, &body, &body_len);
                printf("[D] UNCOMPRESSED: %.*s\n", (int)body_len, body);
                assert(rc==1);
                assert(body_len == (size_t)data->json_len);
                assert(strncmp(data->json_str, body, body_len) == 0);
                zframe_destroy(&body_frame);
            }
        }
    }

    int rc = publish_on_zmq_transport(message_parts, pub_socket, meta, 0);
    if (rc == -1) {
        dropped_messages_count++;
        if (verbose) {
            fprintf(stderr, "[E] could not publish metrics message\n");
            log_zmq_error(rc, __FILE__, __LINE__);
        }
    }

    zmq_msg_close(&message_parts[0]);
    zmq_msg_close(&message_parts[1]);
    zmq_msg_close(&message_parts[2]);
    zmq_msg_close(&message_parts[3]);
    free(data->json_str);
    data->json_str = NULL;
}

int process_http_request(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    int rc;

    // asume request is invalid
    bool valid = false;
    int http_return_code = 400;
    size_t message_size = 0;

    // data structure to hold the ZMQ_STREAM ID
    uint8_t id [MAX_ID_SIZE];
    size_t id_size = 0;

    // data structure to hold the ZMQ_STREAM received data
    uint8_t raw [MAX_REQUEST_SIZE+1];  // +1 for terminating null character
    int raw_size = 0;
    uint8_t first_line [sizeof(raw)+4];
    int first_line_length = 0;

    // get HTTP request; ID frame and then request
    id_size = zmq_recv (item->socket, id, MAX_ID_SIZE, 0);
    assert (id_size > 0);
    assert (id_size <= MAX_ID_SIZE);
    message_size += id_size;

    int msg_size = zmq_recv (item->socket, raw, MAX_REQUEST_SIZE, 0);
    assert (msg_size >= 0);

    if (msg_size == 0) {
        if (debug) printf("[D] received empty frame, probably connect/disconnect notification\n");
        return 0;
    }
    if (msg_size > MAX_REQUEST_SIZE)
        raw_size = MAX_REQUEST_SIZE;
    else
        raw_size = msg_size;

    msg_data_t msg_data = {};
    received_messages_count++;

    // terminate buffer with 0 character, just in case
    // sizeof(raw) = MAX_REQUEST_SIZE + 1, so this is safe:
    raw[raw_size] = 0;

    if (capture_file) {
        // dump message in binary format, compatible with czmq library zmsg_save()
        if (fwrite (&raw_size, sizeof (raw_size), 1, capture_file) != 1)
            if (verbose)
                fprintf(stderr, "[E] could not write message size to capture file\n");
        if (fwrite (raw, raw_size, 1, capture_file) != 1)
            if (verbose)
                fprintf(stderr, "[E] could not write message body to capture file\n");
    }

    if (debug)
        printf("[D] msg_size: %d, raw size: %d\n", msg_size, raw_size);

    message_size += raw_size;

    // update message stats
    received_messages_bytes += message_size;
    if (message_size > received_messages_max_bytes)
        received_messages_max_bytes = message_size;

    if (debug)
        printf("[D] raw_size=%d:\n>>>\n%.*s<<<\n", raw_size, raw_size, raw);

    // copy first line for logging purposes
    uint8_t* end_of_first_line = (uint8_t*) strstr((char*)raw, "\r\n");
    first_line_length = end_of_first_line ? end_of_first_line - raw : 0;
    if (first_line_length) {
        memcpy(first_line, raw, first_line_length);
    } else {
        first_line_length = raw_size < 80 ? raw_size : 80;
        memcpy(first_line, raw, first_line_length);
        first_line[first_line_length++] = ' ';
        first_line[first_line_length++] = '.';
        first_line[first_line_length++] = '.';
        first_line[first_line_length++] = '.';
    }
    first_line[first_line_length] = 0;

    // if the data obtained with a single read does not include the
    // end of the first line, then we consider the request invalid
    if (!end_of_first_line) {
        if (verbose)
            fprintf(stderr, "[E] %s:%d first %d bytes of request did not include CR/LF pair\n", __FILE__, __LINE__, raw_size);
        goto send_answer;
    }

    // analyze request
    bool valid_size = raw_size > path_prefix_length;
    // printf("[D] path_prefix_len: %d, raw_size: %d, size ok: %d\n", path_prefix_length, raw_size, valid_size);
    if (!valid_size) {
        if (verbose)
            fprintf(stderr, "[E] %s:%d invalid path (too short).\n", __FILE__, __LINE__);
        goto send_answer;
    }

    if (memcmp(raw, path_prefix_alive, path_prefix_alive_length) == 0) {
        // confirm liveness
        rc = zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
        if (rc == -1) {
            if (verbose)
                fprintf(stderr, "[E] %s:%d: %s. failed to send identity frame. aborting request: %s\n",
                        __FILE__, __LINE__, zmq_strerror (errno), first_line);
            return 0;
        }
        rc = zmq_send (http_socket, http_response_alive, alive_length, ZMQ_SNDMORE);
        if (rc == -1) {
            if (verbose)
                fprintf(stderr, "[E] %s:%d: %s. failed to send answer frame. aborting request: %s\n",
                        __FILE__, __LINE__, zmq_strerror (errno), first_line);
            return 0;
        }
        goto close_connection;
    } else if (memcmp(raw, path_prefix_ajax, path_prefix_length) == 0) {
        msg_data.msg_type = "ajax";
    } else if (memcmp(raw, path_prefix_page, path_prefix_length) == 0) {
        msg_data.msg_type = "page";
    } else {
        if (verbose)
            fprintf(stderr, "[E] %s:%d: invalid request prefix.\n", __FILE__, __LINE__);
        goto send_answer;
    }

    // search for first non blank character
    int i = path_prefix_length;
    while (i < raw_size && raw[i] != ' ') i++;
    int query_length = i - path_prefix_length;
    // printf("[D] >>> query_length: %d, query_string:'%.*s' <<<\n", query_length, query_length, &raw[path_prefix_length]);

    // check protocol spec
    if (memcmp(raw+i, " HTTP/1.1\r\n", 11) != 0 && memcmp(raw+i, " HTTP/1.0\r\n", 11) != 0 ) {
        if (verbose)
            fprintf(stderr, "[D] %s:%d: invalid http protocol spec %.9s\n", __FILE__, __LINE__, raw+i);
        goto send_answer;
    }

    char *query_string = (char*) &raw[path_prefix_length];
    query_string[query_length] = 0;

    char *headers = strchr(&query_string[query_length+1], '\n');
    if (headers) headers++;

    if (extract_msg_data(query_string, headers, &msg_data)) {
        send_logjam_message(&msg_data, &msg_meta);
    } else if (verbose)
        fprintf(stderr, "[E] %s:%d: invalid query string\n", __FILE__, __LINE__);

    valid = true;
    http_return_code = 200;

 send_answer:
    if (!valid) {
        if (verbose)
            fprintf(stderr, "[E] %03d %s\n", http_return_code, first_line);
    } else if (debug) {
        printf("[D] %03d %s\n", http_return_code, first_line);
    }

    // send the ID frame followed by the response
    rc = zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
    if (rc == -1) {
        if (verbose)
            fprintf(stderr, "[E] %s:%d: %s. failed to send identity frame. aborting request: %s\n",
                    __FILE__, __LINE__, zmq_strerror (errno), first_line);
        return 0;
    }
    if (valid) {
        zmq_send (http_socket, http_response_ok, ok_length, ZMQ_SNDMORE);
        if (rc == -1) {
            if (verbose)
                fprintf(stderr, "[E] %s:%d: %s. failed to send answer frame. aborting request: %s\n",
                        __FILE__, __LINE__, zmq_strerror (errno), first_line);
        }
    } else {
        http_failures++;
        zmq_send (http_socket, http_response_fail, fail_length, ZMQ_SNDMORE);
        if (rc == -1) {
            if (verbose)
                fprintf(stderr, "[E] %s:%d: %s. failed to send answer frame. aborting request: %s\n",
                        __FILE__, __LINE__, zmq_strerror (errno), first_line);
        }
    }

 close_connection:
    // close the connection by sending the ID frame followed by a zero response
    // if anything goes wrong here, die!
    rc = zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
    if (rc != (int)id_size) {
        if (verbose)
            fprintf(stderr, "[E] %s:%d: %s. failed to send identity frame. aborting request: %s\n",
                    __FILE__, __LINE__, zmq_strerror (errno), first_line);
        return 0;
    }
    rc = zmq_send (http_socket, 0, 0, ZMQ_SNDMORE);
    if (rc == -1) {
        if (verbose)
            fprintf(stderr, "[E] %s:%d: %s. failed to send delimiter frame. aborting request: %s\n",
                    __FILE__, __LINE__, zmq_strerror (errno), first_line);
    }

    return 0;
}

static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    static size_t last_received_count = 0;
    static size_t last_received_bytes = 0;
    size_t message_count = received_messages_count - last_received_count;
    size_t message_bytes = received_messages_bytes - last_received_bytes;
    double avg_msg_size = message_count ? (message_bytes / 1024.0) / message_count : 0;
    double max_msg_size = received_messages_max_bytes / 1024.0;

    if (!quiet)
        printf("[I] processed %zu messages (invalid: %zu, dropped: %zu), size: %.2f KB, avg: %.2f KB, max: %.2f KB\n",
               message_count, http_failures, dropped_messages_count, message_bytes/1024.0, avg_msg_size, max_msg_size);

    http_failures = 0;
    dropped_messages_count = 0;
    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    set_started_at();
    return 0;
}

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "\nOptions:\n"
            "  -c, --capture-file F        capture incoming traffic\n"
            "  -d, --device-id N           device id (integer)\n"
            "  -i, --io-threads N          zeromq io threads\n"
            "  -p, --input-port N          port number of zeromq input socket\n"
            "  -q, --quiet                 supress most output\n"
            "  -v, --verbose               log more (use -vv for debug output)\n"
            "  -x, --compress M            compress logjam traffic using (snappy|zlib)\n"
            "  -D, --debug-compress        check decompressability\n"
            "  -P, --output-port N         port number of zeromq ouput socket\n"
            "  -R, --rcv-hwm N             high watermark for input socket\n"
            "  -S, --snd-hwm N             high watermark for output socket\n"
            "      --help                  display this message\n"
            "\nEnvironment: (parameters take precedence)\n"
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
        { "capture-file",   required_argument, 0, 'c' },
        { "compress",       required_argument, 0, 'x' },
        { "debug-compress", no_argument,       0, 'D' },
        { "device-id",      required_argument, 0, 'd' },
        { "help",           no_argument,       0,  0  },
        { "input-port",     required_argument, 0, 'p' },
        { "io-threads",     required_argument, 0, 'i' },
        { "output-port",    required_argument, 0, 'P' },
        { "quiet",          no_argument,       0, 'q' },
        { "rcv-hwm",        required_argument, 0, 'R' },
        { "snd-hwm",        required_argument, 0, 'S' },
        { "verbose",        no_argument,       0, 'v' },
        { 0,                0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "vqd:p:P:c:x:R:S:i:D", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            if (verbose)
                debug= true;
            else
                verbose = true;
            break;
        case 'D':
            debug_compress = true;
            break;
        case 'q':
            quiet = true;
            break;
        case 'd':
            msg_meta.device_number = atoi(optarg);
            break;
        case 'i':
            io_threads = atoi(optarg);
            break;
        case 'p':
            http_port = atoi(optarg);
            break;
        case 'P':
            pub_port = atoi(optarg);
            break;
        case 'c':
            capture_file_name = strdup(optarg);
            break;
        case 'x':
            compression = string_to_compression_method(optarg);
            if (compression)
                printf("[I] compressing streams with: %s\n", compression_method_to_string(compression));
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
            if (strchr("dpPcxRSi", optopt))
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

    // set global config
    zsys_init();
    zsys_set_rcvhwm(rcv_hwm );
    zsys_set_sndhwm(snd_hwm);
    zsys_set_pipehwm(1000);
    zsys_set_linger(0);
    zsys_set_io_threads(io_threads);

    init_globals();

    // open capture file, if requested
    if (capture_file_name) {
        capture_file = fopen(capture_file_name, "a+");
        if (capture_file)
            printf("[I] saving request stream to capture file: %s\n", capture_file_name);
        else
            printf("[W] could not open cpature file for writing: %s\n", capture_file_name);
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // calculate statistics every 1000 ms
    int timer_id = 1;
    rc = zloop_timer(loop, 1000, 0, timer_event, &timer_id);
    assert(rc != -1);

    zmq_pollitem_t http_poll_item = { http_socket, 0, ZMQ_POLLIN, 0 };
    rc = zloop_poller(loop, &http_poll_item, process_http_request, NULL);
    assert(rc == 0);
    zloop_set_tolerant(loop, &http_poll_item);

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

    if (!quiet)
        printf("[I] received %zu messages\n", received_messages_count);

    zsock_destroy(&pub_socket_wrapper);
    zsock_destroy(&http_socket_wrapper);
    zhash_destroy(&integer_conversions);

    if (capture_file)
        fclose(capture_file);

    if (!quiet)
        printf("[I] shutting down\n");

    zsys_shutdown();

    if (!quiet)
        printf("[I] terminated\n");

    return 0;
}
