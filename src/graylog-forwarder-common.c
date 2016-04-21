#include "graylog-forwarder-common.h"

bool dryrun = false;
bool verbose = false;
bool debug = false;
bool quiet = false;
bool compress_gelf = false;

zlist_t *hosts = NULL;
char *interface = NULL;
zlist_t *subscriptions = NULL;

int rcv_hwm = -1;
int snd_hwm = -1;


compressed_gelf_t *
compressed_gelf_new(Bytef *data, uLongf len)
{
    compressed_gelf_t *self = zmalloc(sizeof(*self));
    if (self) {
        self->data = data;
        self->len = len;
    }
    return self;
}

void compressed_gelf_destroy(compressed_gelf_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        compressed_gelf_t *self = *self_p;
        free (self->data);
        free (self);
        *self_p = NULL;
    }
}
