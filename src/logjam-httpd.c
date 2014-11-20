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

static void* ctx = NULL;
static void* http_socket = NULL;
static void* push_socket = NULL;

static char path_prefix_ajax[] = "GET /tools/monitoring/ajax?";
static char path_prefix_page[] = "GET /tools/monitoring/page?";
static int path_prefix_length;

static msg_meta_t msg_meta = META_INFO_EMPTY;

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

    json_object_object_add(json, key, json_object_new_string_len(value, offset));
}

void init_globals()
{
    int rc;

    ok_length = strlen (http_response_ok);
    fail_length = strlen (http_response_fail);
    path_prefix_length = strlen (path_prefix_ajax);

    void *ctx = zmq_ctx_new ();
    assert (ctx);
    zmq_ctx_set (ctx, ZMQ_IO_THREADS, 1);

    // create ZMQ_STREAM socket
    http_socket = zmq_socket (ctx, ZMQ_STREAM);
    assert (http_socket);

    // set some options
    int hwm = 100000;
    rc = zmq_setsockopt (http_socket, ZMQ_SNDHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    rc = zmq_setsockopt (http_socket, ZMQ_RCVHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    // bind
    rc = zmq_bind (http_socket, "tcp://*:9000");
    assert (rc == 0);

    // create ZMQ_PUSH socket
    push_socket = zmq_socket (ctx, ZMQ_PUSH);
    assert (push_socket);

    // set some options
    rc = zmq_setsockopt (push_socket, ZMQ_SNDHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    rc = zmq_setsockopt (push_socket, ZMQ_RCVHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    // connect to logjam device or logjam importer
    rc = zmq_connect (push_socket, "tcp://127.0.0.1:9605");
    assert (rc == 0);
}

#define MAX_ID_SIZE 256
#define HTTP_BUFFER_SIZE 4096

int main() {
    init_globals();

    // data structure to hold the ZMQ_STREAM ID
    uint8_t id [256];
    size_t id_size = 256;

    // data structure to hold the ZMQ_STREAM received data
    uint8_t raw [HTTP_BUFFER_SIZE];
    int raw_size = HTTP_BUFFER_SIZE;
    while (1) {
        msg_meta.sequence_number++;
        json_object *json = NULL;

        // get HTTP request; ID frame and then request
        id_size = zmq_recv (http_socket, id, MAX_ID_SIZE, 0);
        assert (id_size > 0);

        raw_size = zmq_recv (http_socket, raw, HTTP_BUFFER_SIZE, 0);
        assert (raw_size >= 0);
        // printf("[D] >>> raw_size=%d, buffer '%.*s' <<<\n", raw_size, raw_size, raw);

        // analyze request
        bool valid = raw_size < HTTP_BUFFER_SIZE && raw_size > path_prefix_length;
        // printf("[D] path_prefix_len: %d, raw_size: %d, size ok: %d\n", path_prefix_length, raw_size, valid);
        if (!valid) goto answer;

        const char *msg_type;
        if (memcmp(raw, path_prefix_ajax, path_prefix_length) == 0) {
            msg_type = "ajax";
        } else if (memcmp(raw, path_prefix_page, path_prefix_length) == 0) {
            msg_type = "page";
        } else {
            valid = false;
        }
        // printf("[D] valid prefix: %d\n", valid);
        if (!valid) goto answer;

        // search for first non blank character
        int i = path_prefix_length;
        while (i < raw_size && raw[i] != ' ') i++;
        int query_length = i - path_prefix_length;
        // printf("[D] >>> query_length: %d, query_string:'%.*s' <<<\n", query_length, query_length, &raw[path_prefix_length]);

        // check protocol spec
        if (memcmp(raw+i, " HTTP/1.1\r\n", 11) != 0 && memcmp(raw+i, " HTTP/1.0\r\n", 11) != 0 ) {
            // printf("[D] invalid protocol spec\n");
            valid = false;
            goto answer;
        }

        // parse query string
        json = json_object_new_object();
        char *query_string = (char*) &raw[path_prefix_length];
        query_string[query_length] = 0;
        char *phrase = strtok(query_string, "&");
        while (phrase) {
            parse(phrase, json);
            phrase = strtok(NULL, "&");
        }

        // add time info
        msg_meta.created_ms = zclock_time();
        json_object_object_add(json, "started_ms", json_object_new_int64(msg_meta.created_ms));
        time_t now = time(NULL);
        struct tm *tm_now = localtime(&now);
        char buffer[26];
        strftime(buffer, 26, "%Y-%m-%dT%H:%M:%S%z", tm_now);
        json_object_object_add(json, "started_at", json_object_new_string(buffer));

        const char *json_str = json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
        int json_len = strlen(json_str);
        // printf("[D] json: %s\n", json_str);

        // check version
        const char *version = json_get_value(json, "v");
        if (!version) {
            // printf("[D] missing version\n");
            valid = false;
            goto answer;
        } else if (strcmp(version, "1")) {
            // printf("[D] wrong version: %s\n", version);
            valid = false;
            goto answer;
        }

        // get request id
        const char *request_id = json_get_value(json, "logjam_request_id");
        if (!request_id) {
            // printf("[D] missing request id\n");
            valid = false;
            goto answer;
        }

        // get action
        const char *action = json_get_value(json, "logjam_action");
        if (!action) {
            // printf("[D] missing action\n");
            valid = false;
            goto answer;
        }

        // extract app and env
        char app[256], env[256];
        if (strlen(request_id) > 255 || sscanf(request_id, "%[^-]-%[^-]", app, env) != 2) {
            valid = false;
            goto answer;
        };

        char app_env[256];
        int app_env_len = sprintf(app_env, "%s-%s", app, env);

        // create routing key
        char routing_key[256 + 17];
        int routing_key_len = sprintf(routing_key, "frontend.%s.%s.%s", msg_type, app, env);

        zmq_msg_t message_parts[4];
        zmq_msg_init_size(&message_parts[0], app_env_len);
        memcpy(zmq_msg_data(&message_parts[0]), app_env, app_env_len);

        zmq_msg_init_size(&message_parts[1], routing_key_len);
        memcpy(zmq_msg_data(&message_parts[1]), routing_key, routing_key_len);

        zmq_msg_init_size(&message_parts[2], json_len);
        memcpy(zmq_msg_data(&message_parts[2]), json_str, json_len);

        publish_on_zmq_transport(message_parts, push_socket, &msg_meta);

        zmq_msg_close(&message_parts[0]);
        zmq_msg_close(&message_parts[1]);
        zmq_msg_close(&message_parts[2]);
        zmq_msg_close(&message_parts[3]);

    answer:
        // free json object
        if (json)
            json_object_put(json);

        // sends the ID frame followed by the response
        zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
        if (valid)
            zmq_send (http_socket, http_response_ok, ok_length, ZMQ_SNDMORE);
        else
            zmq_send (http_socket, http_response_fail, fail_length, ZMQ_SNDMORE);

        /* Closes the connection by sending the ID frame followed by a zero response */
        zmq_send (http_socket, id, id_size, ZMQ_SNDMORE);
        zmq_send (http_socket, 0, 0, ZMQ_SNDMORE);

        /* NOTE: If we don't use ZMQ_SNDMORE, then we won't be able to send more */
        /* message to any client */
        printf("processed %" PRIu64 " requests\n", msg_meta.sequence_number);
    }
    zmq_close (push_socket);
    zmq_close (http_socket);
    zmq_ctx_destroy (ctx);
}
