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
#include "logjam-util.h"

static char http_response_ok [] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Disposition: inline\r\n"
    "Content-Transfer-Encoding: binary\r\n"
    "Content-Type: image/png\r\n"
    "Cache-Control: private\r\n"
    "Content-Length: 0\r\n"
    "Connection: close\r\n"
    "\r\n";

static char http_response_fail [] =
    "HTTP/1.1 400 Bad Request\r\n"
    "Content-Disposition: inline\r\n"
    "Content-Transfer-Encoding: binary\r\n"
    "Content-Type: image/png\r\n"
    "Cache-Control: private"
    "Content-Length: 0\r\n"
    "Connection: close\r\n"
    "\r\n";

static size_t ok_length, fail_length;

static zsock_t *http_socket_wrapper = NULL;
static zsock_t *push_socket_wrapper = NULL;
static void *http_socket = NULL;
static void *push_socket = NULL;

static size_t received_messages_count = 0;
static size_t received_messages_bytes = 0;
static size_t received_messages_max_bytes = 0;
static size_t http_failures = 0;

static char path_prefix_ajax[] = "GET /tools/monitoring/ajax?";
static char path_prefix_page[] = "GET /tools/monitoring/page?";
static int path_prefix_length;

static msg_meta_t msg_meta = META_INFO_EMPTY;

typedef struct {
    char app[256];
    char env[256];
    const char *msg_type;
    char routing_key[256+17];
    int routing_key_len;
    const char *json_str;
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
void parse(char *s, json_object *json)
{
    char *key;
    char *val;
    int c;
    char buf[3];
    // we know the value can't be longer than the whole request
    char value[4096];
    int offset = 0;

    key = s;
    while (*s && (*s != '=')) s++;
    if (!*s) {
        printf("no parameters\n");
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
void init_globals()
{
    int rc;
    set_started_at();
    setup_integer_conversions();

    ok_length = strlen (http_response_ok);
    fail_length = strlen (http_response_fail);
    path_prefix_length = strlen (path_prefix_ajax);

    // create ZMQ_STREAM socket
    http_socket_wrapper = zsock_new (ZMQ_STREAM);
    assert (http_socket_wrapper);
    http_socket = zsock_resolve (http_socket_wrapper);
    assert (http_socket);

    // bind
    rc = zsock_bind (http_socket_wrapper, "tcp://*:9000");
    assert (rc == 9000);

    // create ZMQ_PUSH socket
    push_socket_wrapper = zsock_new (ZMQ_PUSH);
    assert (push_socket_wrapper);
    push_socket = zsock_resolve (push_socket_wrapper);
    assert (push_socket);

    // connect to logjam device or logjam importer
    rc = zsock_connect(push_socket_wrapper, "tcp://127.0.0.1:9605");
    assert (rc == 0);
}

static inline
bool extract_msg_data_from_query_string(char *query_string, msg_data_t *msg_data)
{
    bool valid = false;
    json_object *json = json_object_new_object();

    char *phrase = strtok(query_string, "&");
    while (phrase) {
        parse(phrase, json);
        phrase = strtok(NULL, "&");
    }

    // add time info
    msg_meta.created_ms = zclock_time();
    json_object_object_add(json, "started_ms", json_object_new_int64(msg_meta.created_ms));
    json_object_object_add(json, "started_at", json_object_new_string(current_time_as_string));

    msg_data->json_str = json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
    msg_data->json_len = strlen(msg_data->json_str);
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

static inline
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

    zmq_msg_init_size(&message_parts[2], data->json_len);
    memcpy(zmq_msg_data(&message_parts[2]), data->json_str, data->json_len);

    publish_on_zmq_transport(message_parts, push_socket, meta);

    zmq_msg_close(&message_parts[0]);
    zmq_msg_close(&message_parts[1]);
    zmq_msg_close(&message_parts[2]);
    zmq_msg_close(&message_parts[3]);
}

#define MAX_ID_SIZE 256
#define HTTP_BUFFER_SIZE 4096

int process_http_request(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    // asume request is invalid
    bool valid = false;
    size_t message_size = 0;

    // data structure to hold the ZMQ_STREAM ID
    uint8_t id [256];
    size_t id_size = 256;

    // data structure to hold the ZMQ_STREAM received data
    uint8_t raw [HTTP_BUFFER_SIZE];
    int raw_size = HTTP_BUFFER_SIZE;

    msg_data_t msg_data = {};
    msg_meta.sequence_number++;
    received_messages_count++;

    // get HTTP request; ID frame and then request
    id_size = zmq_recv (item->socket, id, MAX_ID_SIZE, 0);
    assert (id_size > 0);
    message_size += id_size;

    raw_size = zmq_recv (item->socket, raw, HTTP_BUFFER_SIZE, 0);
    assert (raw_size >= 0);
    message_size += raw_size;
    // printf("[D] >>> raw_size=%d, buffer '%.*s' <<<\n", raw_size, raw_size, raw);
    // if we get more than BUFFER_SIZE data, the request is invalid
    if (raw_size == HTTP_BUFFER_SIZE) {
        // read remaining bytes
        do {
            raw_size = zmq_recv (item->socket, raw, HTTP_BUFFER_SIZE, 0);
            message_size += raw_size;
        } while (raw_size == HTTP_BUFFER_SIZE);
        goto answer;
    }

    // analyze request
    bool valid_size = raw_size < HTTP_BUFFER_SIZE && raw_size > path_prefix_length;
    // printf("[D] path_prefix_len: %d, raw_size: %d, size ok: %d\n", path_prefix_length, raw_size, valid);
    if (!valid_size) goto answer;

    if (memcmp(raw, path_prefix_ajax, path_prefix_length) == 0) {
        msg_data.msg_type = "ajax";
    } else if (memcmp(raw, path_prefix_page, path_prefix_length) == 0) {
        msg_data.msg_type = "page";
    } else {
        // printf("[D] invalid prefix\n");
        goto answer;
    }

    // search for first non blank character
    int i = path_prefix_length;
    while (i < raw_size && raw[i] != ' ') i++;
    int query_length = i - path_prefix_length;
    // printf("[D] >>> query_length: %d, query_string:'%.*s' <<<\n", query_length, query_length, &raw[path_prefix_length]);

    // check protocol spec
    if (memcmp(raw+i, " HTTP/1.1\r\n", 11) != 0 && memcmp(raw+i, " HTTP/1.0\r\n", 11) != 0 ) {
        // printf("[D] invalid protocol spec\n");
        goto answer;
    }

    char *query_string = (char*) &raw[path_prefix_length];
    query_string[query_length] = 0;
    if (extract_msg_data_from_query_string(query_string, &msg_data))
        send_logjam_message(&msg_data, &msg_meta);

    valid = true;

 answer:
    // send the ID frame followed by the response
    zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
    if (valid) {
        zmq_send (http_socket, http_response_ok, ok_length, ZMQ_SNDMORE);
    } else {
        http_failures++;
        zmq_send (http_socket, http_response_fail, fail_length, ZMQ_SNDMORE);
    }

    // close the connection by sending the ID frame followed by a zero response
    zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
    zmq_send (http_socket, 0, 0, ZMQ_SNDMORE);

    received_messages_bytes += message_size;
    if (message_size > received_messages_max_bytes)
        received_messages_max_bytes = message_size;

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

    printf("[I] processed %zu messages (failed: %zu), size: %.2f KB, avg: %.2f KB, max: %.2f KB\n",
           message_count, http_failures, message_bytes/1024.0, avg_msg_size, max_msg_size);

    http_failures = 0;
    last_received_count = received_messages_count;
    last_received_bytes = received_messages_bytes;
    received_messages_max_bytes = 0;
    set_started_at();
    return 0;
}


int main(int argc, char * const *argv)
{
    int rc = 0;

    // set global config
    zsys_init();
    zsys_set_rcvhwm(100000);
    zsys_set_sndhwm(100000);
    zsys_set_pipehwm(1000);
    zsys_set_linger(0);
    // zsys_set_io_threads(2);

    init_globals();

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

    printf("[I] starting main event loop\n");
    rc = zloop_start(loop);
    printf("[I] main event zloop terminated with return code %d\n", rc);

    zloop_destroy(&loop);
    assert(loop == NULL);

    printf("[I] received %zu messages\n", received_messages_count);

    zsock_destroy(&push_socket_wrapper);
    zsock_destroy(&http_socket_wrapper);
    zhash_destroy(&integer_conversions);

    printf("[I] shutting down\n");
    zsys_shutdown();
    printf("[I] terminated\n");

    return 0;
}
