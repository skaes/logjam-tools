#ifndef __LOGJAM_UTIL_H_INCLUDED__
#define __LOGJAM_UTIL_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif


typedef struct {
    uint64_t created_ms;
    uint64_t sequence_number;
    uint16_t device_number;
} msg_meta_t;

extern uint64_t htonll(uint64_t net_number);
extern uint64_t ntohll(uint64_t native_number);


inline void meta_network_2_native(msg_meta_t *meta)
{
    meta->created_ms = htonll(meta->created_ms);
    meta->sequence_number = htonll(meta->sequence_number);
    meta->device_number = htons(meta->device_number);
}

inline void meta_native_2_network(msg_meta_t *meta)
{
    meta->created_ms = ntohll(meta->created_ms);
    meta->sequence_number = ntohll(meta->sequence_number);
    meta->device_number = ntohs(meta->device_number);
}

#ifdef __cplusplus
}
#endif

#endif
