#include <zmq.h>
#include <czmq.h>
#include <limits.h>
#include "logjam-util.h"

bool output_socket_ready(zsock_t *socket, int msecs)
{
    zmq_pollitem_t items[] = { { zsock_resolve(socket), 0, ZMQ_POLLOUT, 0 } };
    int rc = zmq_poll(items, 1, msecs);
    return rc != -1 && (items[0].revents & ZMQ_POLLOUT) != 0;
}

#if !HAVE_DECL_HTONLL
uint64_t htonll(uint64_t net_number)
{
  uint64_t result = 0;
  for (int i = 0; i < (int)sizeof(result); i++) {
    result <<= CHAR_BIT;
    result += (((unsigned char *)&net_number)[i] & UCHAR_MAX);
  }
  return result;
}
#endif

#if !HAVE_DECL_NTOHLL
uint64_t ntohll(uint64_t native_number)
{
  uint64_t result = 0;
  for (int i = (int)sizeof(result) - 1; i >= 0; i--) {
    ((unsigned char *)&result)[i] = native_number & UCHAR_MAX;
    native_number >>= CHAR_BIT;
  }
  return result;
}
#endif

int set_thread_name(const char* name)
{
#if defined(HAVE_PTHREAD_SETNAME_NP) && defined(__linux__)
    pthread_t self = pthread_self();
    return pthread_setname_np(self, name);
#elif defined(HAVE_PTHREAD_SETNAME_NP) && defined(__APPLE__)
    return pthread_setname_np(name);
#else
    return 0;
#endif
}

void dump_meta_info(msg_meta_t *meta)
{
    printf("[D] meta(tag%hx version%hu device %u sequence: %" PRIu64 " created: %" PRIu64 ")\n",
           meta->tag, meta->version, meta->device_number, meta->sequence_number, meta->created_ms);
}

void dump_meta_info_network_format(msg_meta_t *meta)
{
    // copy meta
    msg_meta_t m = *meta;
    meta_info_decode(&m);
    dump_meta_info(&m);
}

int msg_extract_meta_info(zmsg_t *msg, msg_meta_t *meta)
{
    // make sure the caller is clear in his head
    assert(zmsg_size(msg) == 4);

    // pop all frames
    zframe_t *app_env_frame = zmsg_pop(msg);
    zframe_t *routing_key_frame = zmsg_pop(msg);
    zframe_t *body_frame = zmsg_pop(msg);
    zframe_t *meta_frame = zmsg_pop(msg);

    // push back everything except the meta information
    zmsg_append(msg, &app_env_frame);
    zmsg_append(msg, &routing_key_frame);
    zmsg_append(msg, &body_frame);

    // check frame size, tag and protocol version
    int rc = zframe_size(meta_frame) == sizeof(msg_meta_t);
    if (rc) {
        memcpy(meta, zframe_data(meta_frame), sizeof(msg_meta_t));
        meta_info_decode(meta);
        if (meta->tag != META_INFO_TAG || meta->version != META_INFO_VERSION)
            rc = 0;
    }
    zframe_destroy(&meta_frame);
    return rc;
}

int publish_on_zmq_transport(zmq_msg_t *message_parts, void *publisher, msg_meta_t *msg_meta)
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

    zmq_msg_t meta;
    msg_add_meta_info(&meta, msg_meta);
    if (0) dump_meta_info_network_format(zmq_msg_data(&meta));

    rc = zmq_msg_send(&meta, publisher, ZMQ_DONTWAIT);
    if (rc == -1) {
        log_zmq_error(rc);
    }
    zmq_msg_close(&meta);
    return rc;
}


json_object* parse_json_body(zframe_t *body, json_tokener* tokener)
{
    char* json_data = (char*)zframe_data(body);
    int json_data_len = (int)zframe_size(body);
    json_tokener_reset(tokener);
    json_object *jobj = json_tokener_parse_ex(tokener, json_data, json_data_len);
    enum json_tokener_error jerr = json_tokener_get_error(tokener);
    if (jerr != json_tokener_success) {
        fprintf(stderr, "[E] parse_json_body: %s\n", json_tokener_error_desc(jerr));
    } else {
        // const char *json_str_orig = zframe_strdup(body);
        // printf("[D] %s\n", json_str_orig);
        // free(json_str_orig);
        // dump_json_object(stdout, jobj);
    }
    if (tokener->char_offset < json_data_len) // XXX shouldn't access internal fields
    {
        // Handle extra characters after parsed object as desired.
        fprintf(stderr, "[W] parse_json_body: %s\n", "extranoeus data in message payload");
        my_zframe_fprint(body, "[W] MSGBODY=", stderr);
    }
    // if (strnlen(json_data, json_data_len) < json_data_len) {
    //     fprintf(stderr, "[W] parse_json_body: json payload has null bytes\ndata: %*s\n", json_data_len, json_data);
    //     dump_json_object(stdout, jobj);
    //     return NULL;
    // }
    return jobj;
}

void dump_json_object(FILE *f, json_object *jobj)
{
    const char *json_str = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
    if (f == stderr)
        fprintf(f, "[E] %s\n", json_str);
    else
        fprintf(f, "[I] %s\n", json_str);
    // don't try to free the json string. it will crash.
}

void my_zframe_fprint(zframe_t *self, const char *prefix, FILE *file)
{
    assert (self);
    if (prefix)
        fprintf (file, "%s", prefix);
    byte *data = zframe_data (self);
    size_t size = zframe_size (self);

    int is_bin = 0;
    uint char_nbr;
    for (char_nbr = 0; char_nbr < size; char_nbr++)
        if (data [char_nbr] < 9 || data [char_nbr] > 127)
            is_bin = 1;

    fprintf (file, "[%03d] ", (int) size);
    size_t max_size = is_bin? 2048: 4096;
    const char *ellipsis = "";
    if (size > max_size) {
        size = max_size;
        ellipsis = "...";
    }
    for (char_nbr = 0; char_nbr < size; char_nbr++) {
        if (is_bin)
            fprintf (file, "%02X", (unsigned char) data [char_nbr]);
        else
            fprintf (file, "%c", data [char_nbr]);
    }
    fprintf (file, "%s\n", ellipsis);
}

void my_zmsg_fprint(zmsg_t* self, const char* prefix, FILE* file)
{
    zframe_t *frame = zmsg_first(self);
    int frame_nbr = 0;
    while (frame && frame_nbr++ < 10) {
        my_zframe_fprint(frame, prefix, file);
        frame = zmsg_next(self);
    }
}
