#include <czmq.h>
#include "zring.h"

typedef struct _node_t {
    char *key;
    void *item;
} node_t;

struct _zring_t {
    zhashx_t *hash;
    zlistx_t *list;
};

zring_t *
zring_new (void)
{
    zring_t *self = (zring_t *) zmalloc (sizeof (zring_t));
    assert (self);

    self->hash = zhashx_new ();
    assert (self->hash);
    zhashx_set_key_duplicator (self->hash, NULL);
    zhashx_set_key_destructor (self->hash, NULL);

    self->list = zlistx_new ();
    assert (self->list);

    return self;
}

void
zring_destroy (zring_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        zring_t *self = *self_p;
        zhashx_destroy (&self->hash);
        zlistx_destroy (&self->list);
        free (self);
        *self_p = NULL;
    }
}

int
zring_insert (zring_t *self, const char *key, void *item)
{
    assert (self);
    assert (key);
    assert (item);

    if ( zhashx_lookup (self->hash, key) )
        return 0;

    node_t *node = zmalloc (sizeof (node_t));
    assert (node);
    node->key = strdup (key);
    node->item = item;

    void *handle = zlistx_add_end (self->list, node);
    assert (handle);

    int rc = zhashx_insert (self->hash, node->key, handle);
    assert (rc==0);

    return 1;
}


void *
zring_lookup (zring_t *self, const char *key)
{
    assert (self);
    assert (key);
    assert (self->hash);

    void *handle = zhashx_lookup (self->hash, key);
    if (handle) {
        node_t *node = zlistx_handle_item (handle);
        assert (node);
        return node->item;
    }
    return NULL;
}

void *
zring_shift (zring_t *self)
{
    assert (self);

    node_t *node = zlistx_first (self->list);
    if (node) {
        return zring_delete (self, node->key);
    }
    return NULL;
}


void *
zring_delete (zring_t *self, const char *key)
{
    assert (self);
    assert (key);

    void *handle = zhashx_lookup (self->hash, key);
    if (handle) {
        node_t *node = zlistx_handle_item (handle);
        assert (node);
        void *item = node->item;
        assert (item);

        zhashx_delete (self->hash, key);
        zlistx_delete (self->list, handle);

        free (node->key);
        free (node);

        return item;
    }
    return NULL;
}

size_t
zring_size (zring_t *self)
{
    assert (self);
    return zhashx_size (self->hash);
}

void *
zring_first (zring_t *self)
{
    assert (self);
    zlistx_first (self->list);
    node_t *node = zlistx_item (self->list);
    return node ? node->item : NULL;
}

void
zring_test (int verbose)
{
    printf (" * zring: ");

    zring_t *ring = zring_new ();
    assert (ring);
    assert (zring_size (ring) == 0);

    char *cheese = "boursin";
    char *bread = "baguette";
    char *wine = "bordeaux";

    char *cheese_key = "cheese_key";
    char *bread_key = "bread_key";
    char *wine_key = "wine_key";

    int rc = zring_insert (ring, cheese_key, cheese);
    assert (rc);
    assert (zring_size (ring) == 1);
    assert (zring_lookup (ring, cheese_key) == cheese);
    assert (zring_first (ring) == cheese);

    rc = zring_insert (ring, bread_key, bread);
    assert (rc);
    assert (zring_size (ring) == 2);
    assert (zring_lookup (ring, bread_key) == bread);
    assert (zring_first (ring) == cheese);

    rc = zring_insert (ring, wine_key, wine);
    assert (rc);
    assert (zring_size (ring) == 3);
    assert (zring_lookup (ring, wine_key) == wine);
    assert (zring_first (ring) == cheese);

    char *item = zring_delete (ring, cheese_key);
    assert (item == cheese);
    assert (zring_size (ring) == 2);
    assert (zring_lookup (ring, wine_key) == wine);
    assert (zring_lookup (ring, bread_key) == bread);
    assert (zring_first (ring) == bread);

    item = zring_shift (ring);
    assert ( item == bread );
    assert (zring_size (ring) == 1);
    assert (zring_lookup (ring, wine_key) == wine);
    assert (zring_first (ring) == wine);

    zring_destroy (&ring);
    assert (ring == NULL);
    zring_destroy (&ring);

    printf ("OK\n");

}
