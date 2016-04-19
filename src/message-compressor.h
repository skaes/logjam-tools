#ifndef __LOGJAM_IMPORTER_INDEXER_H_INCLUDED__
#define __LOGJAM_IMPORTER_INDEXER_H_INCLUDED__

#include "logjam-util.h"

#ifdef __cplusplus
extern "C" {
#endif

extern zactor_t* message_compressor_new(size_t id, int compression_method);
extern zactor_t* message_decompressor_new(size_t id);

#ifdef __cplusplus
}
#endif

#endif
