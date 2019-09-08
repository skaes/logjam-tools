#ifndef __LOGJAM_IMPORTER_INDEXER_H_INCLUDED__
#define __LOGJAM_IMPORTER_INDEXER_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (compressor_callback_fn) (int i);

extern zactor_t* message_compressor_new(size_t id, int compression_method, compressor_callback_fn cb);
extern zactor_t* message_decompressor_new(size_t id, compressor_callback_fn cb);

#ifdef __cplusplus
}
#endif

#endif
