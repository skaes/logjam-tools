#ifndef __LOGJAM_UTIL_H_INCLUDED__
#define __LOGJAM_UTIL_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <json-c/json.h>

#define META_INFO_VERSION 1
#define META_INFO_TAG 0xcabd
#define META_INFO_EMPTY {META_INFO_TAG, META_INFO_VERSION, 0U, 0ULL, 0ULL}

// encoding of the 4th frame added by logjam device
typedef struct {
    uint16_t tag;
    uint16_t version;
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

extern void dump_meta_info(msg_meta_t *meta);
extern void dump_meta_info_network_format(msg_meta_t *meta);

static inline void meta_info_encode(msg_meta_t *meta)
{
    meta->tag = htons(META_INFO_TAG);
    meta->version = htons(META_INFO_VERSION);
    meta->device_number = htonl(meta->device_number);
    meta->created_ms = htonll(meta->created_ms);
    meta->sequence_number = htonll(meta->sequence_number);
}

static inline void meta_info_decode(msg_meta_t *meta)
{
    meta->tag = ntohs(meta->tag);
    meta->version = ntohs(meta->version);
    meta->device_number = ntohl(meta->device_number);
    meta->created_ms = ntohll(meta->created_ms);
    meta->sequence_number = ntohll(meta->sequence_number);
}

static inline void msg_add_meta_info(zmq_msg_t *msg, msg_meta_t *meta)
{
    zmq_msg_init_size(msg, sizeof(*meta));
    void *data = zmq_msg_data(msg);
    memcpy(data, meta, sizeof(*meta));
    meta_info_encode(data);
}

extern int msg_extract_meta_info(zmsg_t *msg, msg_meta_t *meta);

extern bool output_socket_ready(zsock_t *socket, int msecs);

extern int publish_on_zmq_transport(zmq_msg_t *message_parts, void *socket, msg_meta_t *msg_meta);

extern json_object* parse_json_body(zframe_t *body, json_tokener* tokener);

extern void dump_json_object(FILE *f, const char* prefix, json_object *jobj);

extern void my_zframe_fprint(zframe_t *self, const char *prefix, FILE *file);

extern void my_zmsg_fprint(zmsg_t* self, const char* prefix, FILE* file);

static inline void log_zmq_error(int rc, const char* file, const int line)
{
    if (rc != 0) {
        fprintf(stderr, "[E] %s:%d: errno(%d): %s, interrupted=%d, rc=%d\n",
                file, line, errno, zmq_strerror(errno), zsys_interrupted, rc);
    }
}

static inline int zmsg_addptr(zmsg_t* msg, void* ptr)
{
    return zmsg_addmem(msg, &ptr, sizeof(void*));
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

#ifdef __cplusplus
}
#endif

#endif
