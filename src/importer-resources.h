#ifndef __LOGJAM_IMPORTER_RESOURCES_H__INCLUDED__
#define __LOGJAM_IMPORTER_RESOURCES_H__INCLUDED__

#include "importer-common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* resource maps */
#define MAX_RESOURCE_COUNT 100
extern zhash_t* resource_to_int;
extern char *int_to_resource[MAX_RESOURCE_COUNT];
extern char *int_to_resource_sq[MAX_RESOURCE_COUNT];
extern char *int_to_resource_max[MAX_RESOURCE_COUNT];
extern size_t last_resource_offset;

extern char *time_resources[MAX_RESOURCE_COUNT];
extern size_t last_time_resource_index;
extern size_t last_time_resource_offset;

extern char *other_time_resources[MAX_RESOURCE_COUNT];
extern size_t last_other_time_resource_index;

extern char *call_resources[MAX_RESOURCE_COUNT];
extern size_t last_call_resource_index;
extern size_t last_call_resource_offset;

extern char *memory_resources[MAX_RESOURCE_COUNT];
extern size_t last_memory_resource_index;
extern size_t last_memory_resource_offset;

extern char *heap_resources[MAX_RESOURCE_COUNT];
extern size_t last_heap_resource_index;
extern size_t last_heap_resource_offset;

extern char *frontend_resources[MAX_RESOURCE_COUNT];
extern size_t last_frontend_resource_index;
extern size_t last_frontend_resource_offset;

extern char *dom_resources[MAX_RESOURCE_COUNT];
extern size_t last_dom_resource_index;
extern size_t last_dom_resource_offset;

extern size_t allocated_objects_index, allocated_bytes_index, total_time_index;

// setup bidirectional mapping between resource names and small integers
extern void setup_resource_maps(zconfig_t* config);

static inline size_t r2i(const char* resource)
{
    return (size_t)zhash_lookup(resource_to_int, resource);
}

static inline const char* i2r(size_t i)
{
    assert(i <= last_resource_offset);
    return (const char*)(int_to_resource[i]);
}

#ifdef __cplusplus
}
#endif

#endif
