#include "importer-resources.h"

/* resource maps */
zhash_t* resource_to_int = NULL;
char *int_to_resource[MAX_RESOURCE_COUNT];
char *int_to_resource_sq[MAX_RESOURCE_COUNT];
size_t last_resource_offset = 0;

char *time_resources[MAX_RESOURCE_COUNT];
size_t last_time_resource_index = 0;
size_t last_time_resource_offset = 0;

char *other_time_resources[MAX_RESOURCE_COUNT];
size_t last_other_time_resource_index = 0;

char *call_resources[MAX_RESOURCE_COUNT];
size_t last_call_resource_index = 0;
size_t last_call_resource_offset = 0;

char *memory_resources[MAX_RESOURCE_COUNT];
size_t last_memory_resource_index = 0;
size_t last_memory_resource_offset = 0;

char *heap_resources[MAX_RESOURCE_COUNT];
size_t last_heap_resource_index = 0;
size_t last_heap_resource_offset = 0;

char *frontend_resources[MAX_RESOURCE_COUNT];
size_t last_frontend_resource_index = 0;
size_t last_frontend_resource_offset = 0;

char *dom_resources[MAX_RESOURCE_COUNT];
size_t last_dom_resource_index = 0;
size_t last_dom_resource_offset = 0;

size_t allocated_objects_index, allocated_bytes_index;


static
void add_resources_of_type(zconfig_t* config, const char *type, char **type_map, size_t *type_idx, size_t *type_offset)
{
    char path[256] = {'\0'};
    strcpy(path, "metrics/");
    strcpy(path+strlen("metrics/"), type);
    zconfig_t *metrics = zconfig_locate(config, path);
    assert(metrics);
    zconfig_t *metric = zconfig_child(metrics);
    assert(metric);
    do {
        char *resource = zconfig_name(metric);
        zhash_insert(resource_to_int, resource, (void*)last_resource_offset);
        int_to_resource[last_resource_offset] = resource;
        char resource_sq[256] = {'\0'};
        strcpy(resource_sq, resource);
        strcpy(resource_sq+strlen(resource), "_sq");
        int_to_resource_sq[last_resource_offset++] = strdup(resource_sq);
        type_map[(*type_idx)++] = resource;
        metric = zconfig_next(metric);
        assert(last_resource_offset < MAX_RESOURCE_COUNT);
    } while (metric);
    (*type_idx) -= 1;
    *type_offset = last_resource_offset-1;

    // set up other_time_resources
    if (!strcmp(type, "time")) {
        for (size_t k = 0; k <= *type_idx; k++) {
            char *r = type_map[k];
            if (strcmp(r, "total_time") && strcmp(r, "gc_time") && strcmp(r, "other_time")) {
                other_time_resources[last_other_time_resource_index++] = r;
            }
        }
        last_other_time_resource_index--;

        // printf("[D] other time resources:\n");
        // for (size_t j=0; j<=last_other_time_resource_index; j++) {
        //      printf("[D] %s\n", other_time_resources[j]);
        // }
    }

    // printf("[D] %s resources:\n", type);
    // for (size_t j=0; j<=*type_idx; j++) {
    //     printf("[D] %s\n", type_map[j]);
    // }
}

static
void dump_resource_maps()
{
    for (size_t j=0; j<=last_resource_offset; j++) {
        const char *r = i2r(j);
        printf("[D] %s = %zu\n", r, r2i(r));
    }
    printf("[D] %s = %zu\n", "last_time_resource_index", last_time_resource_index);
    printf("[D] %s = %zu\n", "last_call_resource_index", last_call_resource_index);
    printf("[D] %s = %zu\n", "last_memory_resource_index", last_memory_resource_index);
    printf("[D] %s = %zu\n", "last_heap_resource_index", last_heap_resource_index);
    printf("[D] %s = %zu\n", "last_frontend_resource_index", last_frontend_resource_index);
    printf("[D] %s = %zu\n", "last_dom_resource_index", last_dom_resource_index);

    printf("[D] %s = %zu\n", "last_time_resource_offset", last_time_resource_offset);
    printf("[D] %s = %zu\n", "last_call_resource_offset", last_call_resource_offset);
    printf("[D] %s = %zu\n", "last_memory_resource_offset", last_memory_resource_offset);
    printf("[D] %s = %zu\n", "last_heap_resource_offset", last_heap_resource_offset);
    printf("[D] %s = %zu\n", "last_frontend_resource_offset", last_frontend_resource_offset);
    printf("[D] %s = %zu\n", "last_dom_resource_offset", last_dom_resource_offset);
}

void setup_resource_maps(zconfig_t* config)
{
    //TODO: move this to autoconf
    assert(sizeof(size_t) == sizeof(void*));

    resource_to_int = zhash_new();
    add_resources_of_type(config, "time", time_resources, &last_time_resource_index, &last_time_resource_offset);
    add_resources_of_type(config, "call", call_resources, &last_call_resource_index, &last_call_resource_offset);
    add_resources_of_type(config, "memory", memory_resources, &last_memory_resource_index, &last_memory_resource_offset);
    add_resources_of_type(config, "heap", heap_resources, &last_heap_resource_index, &last_heap_resource_offset);
    add_resources_of_type(config, "frontend", frontend_resources, &last_frontend_resource_index, &last_frontend_resource_offset);
    add_resources_of_type(config, "dom", dom_resources, &last_dom_resource_index, &last_dom_resource_offset);
    last_resource_offset--;

    allocated_objects_index = r2i("allocated_objects");
    allocated_bytes_index = r2i("allocated_bytes");

    if (debug) dump_resource_maps();
}
