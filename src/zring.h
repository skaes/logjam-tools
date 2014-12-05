#ifndef __ZRING_H_INCLUDED__
#define __ZRING_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _zring_t zring_t;

extern zring_t* zring_new (void);

extern void zring_destroy (zring_t **self_p);

extern int zring_insert (zring_t *self, const char *key, void *item);

extern void* zring_lookup (zring_t *self, const char *key);

extern void* zring_shift (zring_t *self);

extern void* zring_delete (zring_t *self, const char *key);

extern size_t zring_size (zring_t *self);

extern void* zring_first (zring_t *self);

extern void zring_test (int verbose);

#ifdef __cplusplus
}
#endif

#endif
