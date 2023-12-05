#ifndef __LOGJAM_UTIL_H_INCLUDED__
#define __LOGJAM_UTIL_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

#include "../config.h"
#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <json-c/json.h>

#ifdef HAVE_MALLOC_TRIM
#include <malloc.h>
#endif
extern int malloc_trim_frequency;

extern bool dryrun;
extern bool verbose;
extern bool debug;
extern bool quiet;

#if CZMQ_VERSION >= 30003
#define zloop_ignore_interrupts(l) zloop_set_nonstop(l, true)
#endif

extern zlist_t *split_delimited_string(const char* s);
extern char* augment_zmq_connection_spec(char* spec, int default_port);
extern void augment_zmq_connection_specs(zlist_t** specs, int default_port);

// length of arrays used to store character representation of iso dates
#define ISO_DATE_STR_LEN 11
extern time_t get_iso_date_info(char today[ISO_DATE_STR_LEN], char tomorrow[ISO_DATE_STR_LEN]);

// check every 10 ticks whether config file has changed
#define CONFIG_FILE_CHECK_INTERVAL 10
// send heart beats every 5 ticks
#define HEART_BEAT_INTERVAL 5
// initial credit leads to 30 seconds
#define INITIAL_HEARTBEAT_CREDIT 6

#define NO_COMPRESSION     0
#define ZLIB_COMPRESSION   1
#define SNAPPY_COMPRESSION 2
#define LZ4_COMPRESSION 3

#define INITIAL_COMPRESSION_BUFFER_SIZE (16 * 1024)
#define INITIAL_DECOMPRESSION_BUFFER_SIZE (32 * 1024)

#define META_INFO_VERSION 1
#define META_INFO_TAG 0xcabd
#define META_INFO_TAG_LE 0xbdca
#define META_INFO_EMPTY {META_INFO_TAG, NO_COMPRESSION, META_INFO_VERSION, 0U, 0ULL, 0ULL}

// encoding of the 4th frame added by logjam device
typedef struct {
    uint16_t tag;
    uint8_t  compression_method;
    uint8_t  version;
    uint32_t device_number;
    uint64_t created_ms;
    uint64_t sequence_number;
} msg_meta_t;


#if !HAVE_DECL_HTONLL
extern uint64_t htonll(uint64_t net_number);
#endif

#if !HAVE_DECL_NTOHLL
extern uint64_t ntohll(uint64_t native_number);
#endif

extern int set_thread_name(const char* name);

extern void dump_meta_info(const char* prefix, msg_meta_t *meta);
extern void dump_meta_info_network_format(const char* prefix, msg_meta_t *meta);

static inline void meta_info_encode(msg_meta_t *meta)
{
    meta->tag = htons(META_INFO_TAG);
    meta->device_number = htonl(meta->device_number);
    meta->created_ms = htonll(meta->created_ms);
    meta->sequence_number = htonll(meta->sequence_number);
}

static inline void meta_info_decode(msg_meta_t *meta)
{
    meta->tag = ntohs(meta->tag);
    meta->device_number = ntohl(meta->device_number);
    meta->created_ms = ntohll(meta->created_ms);
    meta->sequence_number = ntohll(meta->sequence_number);
}

static inline void msg_add_meta_info(zmq_msg_t *msg, msg_meta_t *meta)
{
    zmq_msg_init_size(msg, sizeof(msg_meta_t));
    msg_meta_t *data = (msg_meta_t *) zmq_msg_data(msg);
    memcpy(data, meta,  sizeof(msg_meta_t));
    meta_info_encode(data);
}

static inline void zmsg_add_meta_info(zmsg_t *msg, msg_meta_t *meta)
{
    msg_meta_t m = *meta;
    meta_info_encode(&m);
    zmsg_addmem(msg, &m, sizeof(m));
}

extern int zmq_msg_extract_meta_info(zmq_msg_t *meta_msg, msg_meta_t *meta);
extern int msg_extract_meta_info(zmsg_t *msg, msg_meta_t *meta);
extern int frame_extract_meta_info(zframe_t *frame, msg_meta_t *meta);
extern json_object* meta_info_to_json(msg_meta_t *meta);

extern int zmsg_clear_device_and_sequence_number(zmsg_t* msg);
extern int zmsg_set_device_and_sequence_number(zmsg_t* msg, uint32_t device_number, uint64_t sequence_number);
extern json_object* meta_info_to_json(msg_meta_t *meta);

extern int string_to_compression_method(const char *s);
extern const char* compression_method_to_string(int compression_method);

extern bool output_socket_ready(zsock_t *socket, int msecs);

extern int publish_on_zmq_transport(zmq_msg_t *message_parts, void *socket, msg_meta_t *msg_meta, int flags);

extern void compress_message_data(int compression_method, zchunk_t* buffer, zmq_msg_t *body, const char *data, size_t data_len);

extern int decompress_frame(zframe_t *body_frame, int compression_method, zchunk_t *buffer, char **body, size_t* body_len);

extern int decompress_message_data(zmq_msg_t *msg, int compression_method, zchunk_t *buffer, char **body, size_t* body_len);

extern json_object* parse_json_data(const char *json_data, size_t json_data_len, json_tokener* tokener);

extern void dump_json_object(FILE *f, const char* prefix, json_object *jobj);
extern void dump_json_object_limiting_log_lines(FILE *f, const char* prefix, json_object *jobj, int max_lines);
extern json_object* limit_log_lines(json_object *jobj, int max_lines);

extern void my_zframe_fprint(zframe_t *self, const char *prefix, FILE *file);

extern void my_zmsg_fprint(zmsg_t* self, const char* prefix, FILE* file);

extern void my_zmq_msg_fprint(zmq_msg_t* msg, size_t n, const char* prefix, FILE* file);

static inline void log_zmq_error(int rc, const char* file, const int line)
{
    if (rc != 0) {
        fprintf(stderr, "[E] %s:%d: errno(%d): %s, interrupted=%d, rc=%d\n",
                file, line, errno, zmq_strerror(errno), zsys_interrupted, rc);
    }
}

static inline
void assert_x(int rc, const char* error_text, const char* file, const int line)
{
    if (!rc) {
        fprintf(stderr, "[E] %s:%d: failed assertion: %s\n", file, line, error_text);
        assert(0);
    }
}

static inline int zmsg_addptr(zmsg_t* msg, void* ptr)
{
    return zmsg_addmem(msg, &ptr, sizeof(void*));
}

static inline int zmsg_addsize(zmsg_t* msg, size_t size)
{
    return zmsg_addmem(msg, &size, sizeof(size));
}

static inline void* zframe_getptr(zframe_t* frame)
{
    assert(zframe_size(frame) == sizeof(void*));
    return *((void **) zframe_data(frame));
}

static inline void* zmsg_popptr(zmsg_t* msg)
{
    zframe_t *frame = zmsg_pop(msg);
    assert(frame);
    assert(zframe_size(frame) == sizeof(void*));
    void *ptr = *((void **) zframe_data(frame));
    zframe_destroy(&frame);
    return ptr;
}

static inline size_t zframe_getsize(zframe_t* frame)
{
    assert(zframe_size(frame) == sizeof(size_t));
    return *((size_t *) zframe_data(frame));
}

static inline int zmsg_send_and_destroy(zmsg_t** msg, zsock_t *socket)
{
    int rc = zmsg_send(msg, socket);
    if (rc) zmsg_destroy(msg);
    return rc;
}

static inline int zmsg_send_with_retry(zmsg_t** msg, zsock_t *socket)
{
    int rc;
    do {
        assert(msg);
        errno = 0;
        rc = zmsg_send(msg, socket);
    } while (rc && (errno == EINTR || errno == EAGAIN) && !zsys_interrupted);
    // czmq does not destroy the message in case of error (so a send can be retried)
    // but it also means we have to destroy it if the send fails
    if (rc)
        zmsg_destroy(msg);
    return rc;
}

static inline zmsg_t* zmsg_recv_with_retry(zsock_t *socket)
{
    zmsg_t *msg;
    do {
        msg = zmsg_recv(socket);
    } while (msg == NULL && (errno == EINTR || errno == EAGAIN) && !zsys_interrupted);
    return msg;
}

extern int zmsg_savex (zmsg_t *self, FILE *file);
extern int dump_message_payload(zmsg_t *self, FILE *file, zchunk_t *buffer);
extern int dump_message_as_json(zmsg_t *self, FILE *file, zchunk_t *buffer);
extern zmsg_t* zmsg_loadx (zmsg_t *self, FILE *file);

extern zhash_t* zlist_to_hash(zlist_t *list);
extern zlist_t* zlist_added(zlist_t *older, zlist_t *newer);
extern zlist_t* zlist_deleted(zlist_t *older, zlist_t *newer);
extern size_t zchunk_ensure_size(zchunk_t *buffer, size_t desired_size);

extern void logjam_util_test (int verbose);
extern const char* my_fqdn();
extern void send_heartbeat(zsock_t *socket, msg_meta_t* meta, int pub_port);
extern bool well_formed_stream_name(const char* s, int n);
extern bool well_formed_topic(const char* s, int n);
extern bool extract_app_env(const char* app_env, int n, char* app, char* env);
extern bool extract_app_env_rid(const char* s, int n, char* app, char* env, char* rid);
extern bool is_mobile_app(const char* stream_name);

extern void ensure_chunk_can_take(zchunk_t* buffer, size_t data_size);
extern void append_line(zchunk_t* buffer, const char* format, ...);
extern void append_null_byte(zchunk_t* buffer);

extern void filter_sensitive_cookies(json_object *request, zlist_t *keywords, zchunk_t *buffer);
extern char* replace_keywords(const char *str, zlist_t *keywords, zchunk_t *buffer);
extern size_t find_utf8_offset(const char *buf, size_t n);

#ifdef __cplusplus
}
#endif

#endif
