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

#ifndef HAVE_HTONLL
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

#ifndef HAVE_NTOHLL
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
