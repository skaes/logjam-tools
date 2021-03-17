#include "device-tracker.h"

extern bool quiet;
extern bool verbose;

bool log_gaps = true;

typedef struct {
    uint32_t device_number;
    uint64_t sequence_number;
    uint64_t lost;
    int credit;
    const char* pub_spec;
    char device_num_str[11]; // at most 4294967295
} device_info_t;

struct _device_tracker_t {
    zhashx_t *seen_devices;
    zhash_t *known_devices;
    char *localhost_spec;
    zsock_t *sub_socket;
};

static size_t uint64_hash(const void *key)
{
    return (size_t) key;
}

static int uint64_comparator(const void *a, const void *b)
{
    return a < b ? -1 : (a > b ? 1 : 0);
}

static void device_info_destroy(void **item)
{
    device_info_t *info = *item;
    free((void*)info->pub_spec);
    free(info);
    *item = NULL;
}

device_tracker_t* device_tracker_new(zlist_t* known_devices, zsock_t* sub_socket)
{
    device_tracker_t *tracker = zmalloc(sizeof(*tracker));
    tracker->sub_socket = sub_socket;

    tracker->known_devices = zhash_new();
    char *spec = zlist_first(known_devices);
    while (spec) {
        zhash_insert(tracker->known_devices, spec, (void*)1);
        if (strstr(spec, "localhost") || strstr(spec, "127.0.0.1") || strstr(spec, "::1"))
            tracker->localhost_spec = spec;
        spec = zlist_next(known_devices);
    }

    tracker->seen_devices = zhashx_new();
    zhashx_set_key_hasher(tracker->seen_devices, uint64_hash);
    zhashx_set_key_destructor(tracker->seen_devices, NULL);
    zhashx_set_key_duplicator(tracker->seen_devices, NULL);
    zhashx_set_key_comparator(tracker->seen_devices, uint64_comparator);
    zhashx_set_destructor(tracker->seen_devices, device_info_destroy);

    return tracker;
}

void device_tracker_destroy(device_tracker_t** tracker)
{
    zhashx_destroy(&(*tracker)->seen_devices);
    zhash_destroy(&(*tracker)->known_devices);
    *tracker = NULL;
}

size_t device_tracker_calculate_gap(device_tracker_t* tracker, msg_meta_t* meta, const char* pub_spec)
{
    uint64_t device_number = meta->device_number;
    if (device_number == 0)
        return 0;

    uint64_t sequence_number = meta->sequence_number;
    device_info_t *info = zhashx_lookup(tracker->seen_devices, (const void*) device_number);

    if (info == NULL) {
        if (!quiet)
            printf("[I] counting gaps for device %" PRIu64 "\n", device_number);
        info = zmalloc(sizeof(*info));
        assert(info);
        info->device_number = device_number;
        info->sequence_number = sequence_number;
        info->credit = INITIAL_HEARTBEAT_CREDIT;
        info->lost = 0;
        info->pub_spec = pub_spec;
        sprintf(info->device_num_str, "%" PRIu32, (uint32_t)device_number);
        int rc = zhashx_insert(tracker->seen_devices, (const void*) device_number, info);
        assert(rc == 0);
        return 0;
    } else {
        int64_t gap = sequence_number - info->sequence_number - 1;
        if (gap) {
            if (gap < 0) {
                fprintf(stderr, "[W] sequence number for device %" PRIu64 " wrapped to %" PRIu64 " from %" PRIu64 ")\n",
                        device_number, sequence_number, info->sequence_number);
                gap = 0;
            } else if (gap > 0) {
                info->lost += gap;
                if (!quiet && log_gaps)
                    fprintf(stderr, "[W] lost %" PRIu64 " messages from device %" PRIu64 " (%" PRIu64 "-%" PRIu64 "-1)\n",
                            gap, device_number, sequence_number, info->sequence_number);
            }
        }
        info->sequence_number = sequence_number;
        info->credit = INITIAL_HEARTBEAT_CREDIT;
        if (pub_spec != NULL) {
            if (verbose)
                printf("[I] received heartbeat for connection: %s\n", pub_spec);
            if (info->pub_spec)
                free((void*)info->pub_spec);
            info->pub_spec = pub_spec;
        }
        return gap;
    }
}

void device_tracker_reconnect_stale_devices(device_tracker_t* tracker)
{
    const char *my_hostname = my_fqdn();

    // determine publishers we should reconnect to
    zlist_t *stale_devices = zlist_new();
    device_info_t *info = zhashx_first(tracker->seen_devices);
    while (info) {
        if (info->credit-- <= 0) {
            printf("[I] no credit left for device %d. spec: %s\n", info->device_number, info->pub_spec);
            if (info->pub_spec) {
                zlist_append(stale_devices, info);
            }
        } else if (info->credit < INITIAL_HEARTBEAT_CREDIT - 2) {
            printf("[I] credit left for device %d: %d\n", info->device_number, info->credit);
        }
        info = zhashx_next(tracker->seen_devices);
    }
    // handle localhost
    info = zlist_first(stale_devices);
    while (info) {
        const char *pub_spec;
        if (strstr(info->pub_spec, my_hostname))
            pub_spec = tracker->localhost_spec;
        else
            pub_spec = info->pub_spec;

        if (zhash_lookup(tracker->known_devices, pub_spec)) {
            printf("[I] disconnecting stale device %d: %s\n", info->device_number, pub_spec);
            zsock_disconnect(tracker->sub_socket, "%s", pub_spec);

            printf("[I] reconnecting device %d: %s\n", info->device_number, pub_spec);
            zsock_connect(tracker->sub_socket, "%s", pub_spec);
        } else {
            fprintf(stderr, "[E] unkown device %d sends us messages\n", info->device_number);
        }

        zhashx_delete(tracker->seen_devices, (const void*)(uint64_t)info->device_number);
        info = zlist_next(stale_devices);
    }
    zlist_destroy(&stale_devices);
}

void device_tracker_record_sequence_numbers(device_tracker_t* tracker, device_number_recorder_fn f)
{
    device_info_t *info = zhashx_first(tracker->seen_devices);
    while (info) {
        f(info->device_number, info->device_num_str, info->sequence_number);
        info = zhashx_next(tracker->seen_devices);
    }
}
