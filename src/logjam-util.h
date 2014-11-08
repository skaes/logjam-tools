#ifndef __LOGJAM_UTIL_H_INCLUDED__
#define __LOGJAM_UTIL_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

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

extern uint64_t htonll(uint64_t net_number);
extern uint64_t ntohll(uint64_t native_number);

extern void dump_meta_info(msg_meta_t *meta);
extern void dump_meta_info_network_format(msg_meta_t *meta);

inline void meta_info_encode(msg_meta_t *meta)
{
    meta->tag = htons(META_INFO_TAG);
    meta->version = htons(META_INFO_VERSION);
    meta->device_number = htonl(meta->device_number);
    meta->created_ms = htonll(meta->created_ms);
    meta->sequence_number = htonll(meta->sequence_number);
}

inline void meta_info_decode(msg_meta_t *meta)
{
    meta->tag = ntohs(meta->tag);
    meta->version = ntohs(meta->version);
    meta->device_number = ntohl(meta->device_number);
    meta->created_ms = ntohll(meta->created_ms);
    meta->sequence_number = ntohll(meta->sequence_number);
}

inline void msg_add_meta_info(zmq_msg_t *msg, msg_meta_t *meta)
{
    zmq_msg_init_size(msg, sizeof(*meta));
    void *data = zmq_msg_data(msg);
    memcpy(data, &meta, sizeof(*meta));
    meta_info_encode(data);
}

extern int msg_extract_meta_info(zmsg_t *msg, msg_meta_t *meta);

#ifdef __cplusplus
}
#endif

#endif
