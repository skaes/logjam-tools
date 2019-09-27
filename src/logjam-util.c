#include <zmq.h>
#include <czmq.h>
#include <limits.h>
#include <zlib.h>
#include <snappy-c.h>
#include <lz4.h>
#include "logjam-util.h"

zlist_t *split_delimited_string(const char* s)
{
    zlist_t *strings = zlist_new();
    if (!s) return strings;

    char delim[] = ", ";
    char* token;
    char* state = NULL;
    char *buffer = strdup(s);

    token = strtok_r(buffer, delim, &state);
    while (token != NULL) {
        zlist_append(strings, strdup(token));
        token = strtok_r (NULL, delim, &state);
    }
    free(buffer);

    return strings;
}

char* augment_zmq_connection_spec(char* spec, int default_port)
{
    size_t n = strlen(spec);
    assert(n < 900);
    char buffer[1024] = {0};
    if (strstr(spec, "://")) {
        if (strncmp(spec, "tcp://", 6) && strncmp(spec, "ipc://", 6)) {
            fprintf(stderr, "[E] protocol not supported: %s\n", spec);
            assert(false);
        }
        strcat(buffer, spec);
    } else {
        strcat(buffer, "tcp://");
        strcat(buffer+6, spec);
        n += 6;
    }
    if (strrchr(buffer, ':') == buffer+3)
        sprintf(buffer+n, ":%d", default_port);

    if (strcmp(buffer, spec))
        return strdup(buffer);
    else
        return strdup(spec);
}

void augment_zmq_connection_specs(zlist_t** specs, int default_port)
{
    char *spec;
    zlist_t *old_list = *specs;
    zlist_t *new_list = zlist_new();
    while ( (spec = zlist_pop(old_list)) ) {
        char *new_spec = augment_zmq_connection_spec(spec, default_port);
        free(spec);
        zlist_append(new_list, new_spec);
        spec = zlist_next(old_list);
    }
    zlist_destroy(&old_list);
    *specs = new_list;
}

bool output_socket_ready(zsock_t *socket, int msecs)
{
    zmq_pollitem_t items[] = { { zsock_resolve(socket), 0, ZMQ_POLLOUT, 0 } };
    int rc = zmq_poll(items, 1, msecs);
    return rc != -1 && (items[0].revents & ZMQ_POLLOUT) != 0;
}

#if !HAVE_DECL_HTONLL
uint64_t htonll(uint64_t net_number)
{
  uint64_t result = 0;
  for (int i = 0; i < (int)sizeof(result); i++) {
    result <<= CHAR_BIT;
    result += (((unsigned char *)&net_number)[i] & UCHAR_MAX);
  }
  return result;
}
#endif

#if !HAVE_DECL_NTOHLL
uint64_t ntohll(uint64_t native_number)
{
  uint64_t result = 0;
  for (int i = (int)sizeof(result) - 1; i >= 0; i--) {
    ((unsigned char *)&result)[i] = native_number & UCHAR_MAX;
    native_number >>= CHAR_BIT;
  }
  return result;
}
#endif

int set_thread_name(const char* name)
{
#if defined(HAVE_PTHREAD_SETNAME_NP) && defined(__linux__)
    pthread_t self = pthread_self();
    return pthread_setname_np(self, name);
#elif defined(HAVE_PTHREAD_SETNAME_NP) && defined(__APPLE__)
    return pthread_setname_np(name);
#else
    return 0;
#endif
}

void dump_meta_info(const char* prefix, msg_meta_t *meta)
{
    printf("%s meta(tag:%hx version:%u compression:%u device:%u sequence:%" PRIu64 " created:%" PRIu64 ")\n",
           prefix, meta->tag, meta->version, meta->compression_method, meta->device_number, meta->sequence_number, meta->created_ms);
}

void dump_meta_info_network_format(const char* prefix, msg_meta_t *meta)
{
    // copy meta
    msg_meta_t m = *meta;
    meta_info_decode(&m);
    dump_meta_info(prefix, &m);
}

int zmq_msg_extract_meta_info(zmq_msg_t *meta_msg, msg_meta_t *meta)
{
    int rc = zmq_msg_size(meta_msg) == sizeof(msg_meta_t);
    if (rc) {
        memcpy(meta, zmq_msg_data(meta_msg), sizeof(msg_meta_t));
        meta_info_decode(meta);
        if (meta->tag != META_INFO_TAG || meta->version != META_INFO_VERSION)
            rc = 0;
    }
    return rc;
}

int frame_extract_meta_info(zframe_t *meta_frame, msg_meta_t *meta)
{
    int rc = zframe_size(meta_frame) == sizeof(msg_meta_t);
    if (rc) {
        memcpy(meta, zframe_data(meta_frame), sizeof(msg_meta_t));
        meta_info_decode(meta);
        if (meta->tag != META_INFO_TAG || meta->version != META_INFO_VERSION)
            rc = 0;
    }
    return rc;
}

int msg_extract_meta_info(zmsg_t *msg, msg_meta_t *meta)
{
    // make sure the caller is clear in his head
    assert(zmsg_size(msg) == 4);
    zframe_t *meta_frame = zmsg_last(msg);

    // check frame size, tag and protocol version
    int rc = frame_extract_meta_info(meta_frame, meta);
    return rc;
}

int zmsg_clear_device_and_sequence_number(zmsg_t* msg)
{
    if (zmsg_size(msg)!=4)
        return 0;

    zframe_t *meta_frame = zmsg_last(msg);
    assert(zframe_size(meta_frame) == sizeof(msg_meta_t));

    msg_meta_t *meta = (msg_meta_t *)zframe_data(meta_frame);
    meta->device_number = 0;
    meta->sequence_number = 0;

    return 1;
}

int zmsg_set_device_and_sequence_number(zmsg_t* msg, uint32_t device_number, uint64_t sequence_number)
{
    if (zmsg_size(msg)!=4)
        return 0;

    zframe_t *meta_frame = zmsg_last(msg);
    assert(zframe_size(meta_frame) == sizeof(msg_meta_t));

    msg_meta_t *meta = (msg_meta_t *)zframe_data(meta_frame);
    meta->device_number = htonl(device_number);
    meta->sequence_number = htonll(sequence_number);

    return 1;
}

int string_to_compression_method(const char *s)
{
    if (!strcmp("zlib", s))
        return ZLIB_COMPRESSION;
    else if (!strcmp("snappy", s))
        return SNAPPY_COMPRESSION;
    else if (!strcmp("lz4", s))
        return LZ4_COMPRESSION;
    else {
        fprintf(stderr, "unsupported compression method: '%s'\n", s);
        return NO_COMPRESSION;
    }
}

const char* compression_method_to_string(int compression_method)
{
    switch (compression_method) {
    case NO_COMPRESSION:     return "no compression";
    case ZLIB_COMPRESSION:   return "zlib";
    case SNAPPY_COMPRESSION: return "snappy";
    case LZ4_COMPRESSION:    return "lz4";
    default:                 return "unknown compression method";
    }
}

int publish_on_zmq_transport(zmq_msg_t *message_parts, void *publisher, msg_meta_t *msg_meta, int flags)
{
    int rc=0;
    zmq_msg_t *app_env = &message_parts[0];
    zmq_msg_t *key     = &message_parts[1];
    zmq_msg_t *body    = &message_parts[2];

    rc = zmq_msg_send(app_env, publisher, flags|ZMQ_SNDMORE);
    if (rc == -1) {
        if (errno != EAGAIN)
            log_zmq_error(rc, __FILE__, __LINE__);
        return rc;
    }
    rc = zmq_msg_send(key, publisher, flags|ZMQ_SNDMORE);
    if (rc == -1) {
        if (errno != EAGAIN)
            log_zmq_error(rc, __FILE__, __LINE__);
        return rc;
    }
    rc = zmq_msg_send(body, publisher, flags|ZMQ_SNDMORE);
    if (rc == -1) {
        if (errno != EAGAIN)
            log_zmq_error(rc, __FILE__, __LINE__);
        return rc;
    }

    zmq_msg_t meta;
    // dump_meta_info(msg_meta);
    msg_add_meta_info(&meta, msg_meta);
    // dump_meta_info_network_format(zmq_msg_data(&meta));

    rc = zmq_msg_send(&meta, publisher, flags);
    if (rc == -1) {
        if (errno != EAGAIN)
            log_zmq_error(rc, __FILE__, __LINE__);
    }
    zmq_msg_close(&meta);
    return rc;
}

void compress_message_data_gzip(zchunk_t* buffer, zmq_msg_t *body, const char *data, size_t data_len)
{
    const Bytef *raw_data = (Bytef *)data;
    uLong raw_len = data_len;
    uLongf compressed_len = compressBound(raw_len);
    // printf("[D] util: compression bound %zu\n", compressed_len);
    size_t buffer_size = zchunk_max_size(buffer);
    if (buffer_size < compressed_len) {
        size_t next_size = 2 * buffer_size;
        while (next_size < compressed_len)
            next_size *= 2;
        // printf("[D] util: resizing compression buffer to %zu\n", next_size);
        zchunk_resize(buffer, next_size);
    }
    Bytef *compressed_data = zchunk_data(buffer);

    // compress will update compressed_len to the actual size of the compressed data
    int rc = compress(compressed_data, &compressed_len, raw_data, raw_len);
    assert(rc == Z_OK);
    assert(compressed_len <= zchunk_max_size(buffer));

    zmq_msg_t compressed_msg;
    zmq_msg_init_size(&compressed_msg, compressed_len);
    memcpy(zmq_msg_data(&compressed_msg), compressed_data, compressed_len);
    zmq_msg_move(body, &compressed_msg);

    // printf("[D] zlib uncompressed/compressed: %ld/%ld\n", raw_len, compressed_len);
}

void compress_message_data_snappy(zchunk_t* buffer, zmq_msg_t *body, const char *data, size_t data_len)
{
    size_t max_compressed_len = snappy_max_compressed_length(data_len);
    // printf("[D] util: compression bound %zu\n", max_compressed_len);
    size_t buffer_size = zchunk_max_size(buffer);
    if (buffer_size < max_compressed_len) {
        size_t next_size = 2 * buffer_size;
        while (next_size < max_compressed_len)
            next_size *= 2;
        // printf("[D] util: resizing compression buffer to %zu\n", next_size);
        zchunk_resize(buffer, next_size);
    }
    char *compressed_data = (char*) zchunk_data(buffer);

    // compress will update compressed_len to the actual size of the compressed data
    size_t compressed_len = max_compressed_len;
    int rc = snappy_compress(data, data_len, compressed_data, &compressed_len);
    assert(rc == SNAPPY_OK);
    assert(compressed_len <= zchunk_max_size(buffer));

    zmq_msg_t compressed_msg;
    zmq_msg_init_size(&compressed_msg, compressed_len);
    memcpy(zmq_msg_data(&compressed_msg), compressed_data, compressed_len);
    rc = zmq_msg_move(body, &compressed_msg);
    assert(rc != -1);

    // printf("[D] snappy uncompressed/compressed: %ld/%ld\n", data_len, compressed_len);
}

void compress_message_data_lz4(zchunk_t* buffer, zmq_msg_t *body, const char *data, size_t data_len)
{
    int max_compressed_len = LZ4_compressBound(data_len) + 4;
    assert(max_compressed_len > 0);
    // printf("[D] util: compression bound %zu\n", max_compressed_len);
    size_t buffer_size = zchunk_max_size(buffer);
    if (buffer_size < (size_t)max_compressed_len) {
        size_t next_size = 2 * buffer_size;
        while (next_size < (size_t)max_compressed_len)
            next_size *= 2;
        // printf("[D] util: resizing compression buffer to %zu\n", next_size);
        zchunk_resize(buffer, next_size);
    }
    char *compressed_data = (char*) zchunk_data(buffer);

    // compress will set compressed_len to the size of the compressed data
    int compressed_len = LZ4_compress_default(data, compressed_data+4, data_len, max_compressed_len);
    assert(compressed_len > 0);
    assert(compressed_len <= max_compressed_len);

    int32_t encoded_len = htonl(data_len);
    memcpy(compressed_data, &encoded_len, 4);

    zmq_msg_t compressed_msg;
    zmq_msg_init_size(&compressed_msg, compressed_len+4);
    memcpy(zmq_msg_data(&compressed_msg), compressed_data, compressed_len+4);
    int rc = zmq_msg_move(body, &compressed_msg);
    assert(rc != -1);

    // printf("[D] lz4 uncompressed/compressed: %ld/%d\n", data_len, compressed_len);
}

void compress_message_data(int compression_method, zchunk_t* buffer, zmq_msg_t *body, const char *data, size_t data_len)
{
    switch (compression_method) {
    case ZLIB_COMPRESSION:
        compress_message_data_gzip(buffer, body, data, data_len);
        break;
    case SNAPPY_COMPRESSION:
        compress_message_data_snappy(buffer, body, data, data_len);
        break;
    case LZ4_COMPRESSION:
        compress_message_data_lz4(buffer, body, data, data_len);
        break;
    default:
        fprintf(stderr, "[D] unknown compression method\n");
    }
}

// we give up if the buffer needs to be larger than 10MB
const size_t max_buffer_size = 32 * 1024 * 1024;

int decompress_frame_gzip(zframe_t *body_frame, zchunk_t *buffer, char **body, size_t* body_len)
{
    uLongf dest_size = zchunk_max_size(buffer);
    Bytef *dest = zchunk_data(buffer);
    const Bytef *source = zframe_data(body_frame);
    uLong source_len = zframe_size(body_frame);

    while ( zchunk_max_size(buffer) <= max_buffer_size ) {
        if ( Z_OK == uncompress(dest, &dest_size, source, source_len) ) {
            *body = (char*) zchunk_data(buffer);
            *body_len = dest_size;
            return 1;
        } else {
            size_t next_size = 2 * zchunk_max_size(buffer);
            if (next_size > max_buffer_size)
                next_size = max_buffer_size;
            zchunk_resize(buffer, next_size);
            dest_size = next_size;
            dest = zchunk_data(buffer);
        }
    }
    return 0;
}

int decompress_frame_snappy(zframe_t *body_frame, zchunk_t *buffer, char **body, size_t* body_len)
{
    const char *source = (char*) zframe_data(body_frame);
    size_t source_len = zframe_size(body_frame);

    size_t dest_size = zchunk_max_size(buffer);
    char *dest = (char*) zchunk_data(buffer);

    *body = "";
    *body_len = 0;

    size_t uncompressed_length;
    if (SNAPPY_OK != snappy_uncompressed_length(source, source_len, &uncompressed_length)) {
        fprintf(stderr, "[E] snappy_uncompressed_length failed\n");
        return 0;
    }

    if (uncompressed_length > dest_size) {
        size_t next_size = 2 * dest_size;
        while (next_size < max_buffer_size)
            next_size *= 2;
        if (next_size > max_buffer_size)
            next_size = max_buffer_size;
        zchunk_resize(buffer, next_size);
        dest_size = next_size;
        dest = (char*) zchunk_data(buffer);
    }
    assert(dest_size >= uncompressed_length);

    if (SNAPPY_OK != snappy_uncompress(source, source_len, dest, &dest_size)) {
        fprintf(stderr, "[E] snappy_uncompress failed\n");
        return 0;
    }

    *body = dest;
    *body_len = dest_size;

    return 1;
}

size_t zchunk_ensure_size(zchunk_t *buffer, size_t desired_size)
{
    size_t current_size = zchunk_max_size(buffer);

    if (desired_size <= current_size)
        return current_size;

    size_t next_size = 2 * current_size;

    while (next_size < max_buffer_size)
        next_size *= 2;

    if (next_size > max_buffer_size)
        next_size = max_buffer_size;

    zchunk_resize(buffer, next_size);

    return next_size;
}

int decompress_frame_lz4(zframe_t *body_frame, zchunk_t *buffer, char **body, size_t* body_len)
{
    const char *source = (char*) zframe_data(body_frame);
    size_t source_len = zframe_size(body_frame);

    size_t dest_size = zchunk_max_size(buffer);
    char *dest = (char*) zchunk_data(buffer);

    *body = "";
    *body_len = 0;

    int32_t encoded_length;
    memcpy(&encoded_length, source, 4);
    size_t uncompressed_length = ntohl(encoded_length);

    if (uncompressed_length > dest_size) {
        size_t next_size = 2 * dest_size;
        while (next_size < max_buffer_size)
            next_size *= 2;
        if (next_size > max_buffer_size)
            next_size = max_buffer_size;
        zchunk_resize(buffer, next_size);
        dest_size = next_size;
        dest = (char*) zchunk_data(buffer);
    }
    assert(dest_size >= uncompressed_length);

    int decompressed_bytes = LZ4_decompress_safe(source+4, dest, source_len-4, dest_size);
    if (decompressed_bytes < 0) {
        fprintf(stderr, "[E] lz4_decompress failed\n");
        return 0;
    }

    *body = dest;
    *body_len = decompressed_bytes;

    return 1;
}

int decompress_frame(zframe_t *body_frame, int compression_method, zchunk_t *buffer, char **body, size_t* body_len)
{
    switch (compression_method) {
    case ZLIB_COMPRESSION:
        return decompress_frame_gzip(body_frame, buffer, body, body_len);
    case SNAPPY_COMPRESSION:
        return decompress_frame_snappy(body_frame, buffer, body, body_len);
    case LZ4_COMPRESSION:
        return decompress_frame_lz4(body_frame, buffer, body, body_len);
    default:
        fprintf(stderr, "[D] unknown compression method: %d\n", compression_method);
        return 0;
    }
}

json_object* parse_json_data(const char *json_data, size_t json_data_len, json_tokener* tokener)
{
    json_tokener_reset(tokener);
    json_object *jobj = json_tokener_parse_ex(tokener, json_data, json_data_len);
    enum json_tokener_error jerr = json_tokener_get_error(tokener);
    if (jerr != json_tokener_success) {
        fprintf(stderr, "[E] parse_json_body: %s\n", json_tokener_error_desc(jerr));
    } else {
        // const char *json_str_orig = zframe_strdup(body);
        // printf("[D] %s\n", json_str_orig);
        // free(json_str_orig);
        // dump_json_object(stdout, "[D]", jobj);
    }
    if (tokener->char_offset < json_data_len) // XXX shouldn't access internal fields
    {
        // Handle extra characters after parsed object as desired.
        fprintf(stderr, "[W] parse_json_body: %s\n", "extranoeus data in message payload");
        fprintf(stderr, "[W] MSGBODY=%.*s\n", (int)json_data_len, json_data);
    }
    // if (strnlen(json_data, json_data_len) < json_data_len) {
    //     fprintf(stderr, "[W] parse_json_body: json payload has null bytes\ndata: %*s\n", json_data_len, json_data);
    //     dump_json_object(stderr, "[W]", jobj);
    //     return NULL;
    // }
    return jobj;
}

void dump_json_object(FILE *f, const char* prefix, json_object *jobj)
{
    const char *json_str = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
    fprintf(f, "%s %s\n", prefix, json_str);
    // don't try to free the json string. it will crash.
}

static void print_msg(byte* data, size_t size, const char *prefix, FILE *file)
{
    if (prefix)
        fprintf (file, "%s", prefix);

    int is_bin = 0;
    uint char_nbr;
    for (char_nbr = 0; char_nbr < size; char_nbr++)
        if (data [char_nbr] < 9 || data [char_nbr] > 127)
            is_bin = 1;

    fprintf (file, "[%03d] ", (int) size);
    size_t max_size = is_bin? 2048: 4096;
    const char *ellipsis = "";
    if (size > max_size) {
        size = max_size;
        ellipsis = "...";
    }
    for (char_nbr = 0; char_nbr < size; char_nbr++) {
        if (is_bin)
            fprintf (file, "%02X", (unsigned char) data [char_nbr]);
        else
            fprintf (file, "%c", data [char_nbr]);
    }
    fprintf (file, "%s\n", ellipsis);
}

void my_zframe_fprint(zframe_t *self, const char *prefix, FILE *file)
{
    assert (self);
    byte *data = zframe_data (self);
    size_t size = zframe_size (self);
    print_msg(data, size, prefix, file);
}

void my_zmsg_fprint(zmsg_t* self, const char* prefix, FILE* file)
{
    zframe_t *frame = zmsg_first(self);
    int frame_nbr = 0;
    while (frame && frame_nbr++ < 10) {
        my_zframe_fprint(frame, prefix, file);
        frame = zmsg_next(self);
    }
}

void my_zmq_msg_fprint(zmq_msg_t* msg, size_t n, const char* prefix, FILE* file )
{
    for (size_t i = 0; i < n; i++) {
        byte* data = zmq_msg_data(&msg[i]);
        size_t size = zmq_msg_size(&msg[i]);
        print_msg(data, size, prefix, file);
    }
}

//  --------------------------------------------------------------------------
//  Save message to an open file, return 0 if OK, else -1. The message is
//  saved as a series of frames, each with length and data. Note that the
//  file is NOT guaranteed to be portable between operating systems, not
//  versions of CZMQ. The file format is at present undocumented and liable
//  to arbitrary change.

int
zmsg_savex (zmsg_t *self, FILE *file)
{
    assert (self);
    assert (zmsg_is (self));
    assert (file);

    size_t frame_count = zmsg_size (self);
    if (fwrite (&frame_count, sizeof (frame_count), 1, file) != 1)
        return -1;

    zframe_t *frame = zmsg_first (self);
    while (frame) {
        if (zmsg_savex_frame(frame, file) != 0) {
            return -1;
        } 
        frame = zmsg_next (self);
    }
    return 0;
}

// Save the payload frame of the message only
int zmsg_savex_payload(zmsg_t *self, FILE *file) {
    assert (self);
    assert (zmsg_is (self));
    assert (file);

    zframe_t *frame = zmsg_first (self); // topic
    frame = zmsg_next (self); // routing key
    frame = zmsg_next (self); //payload

    return zmsg_savex_frame(frame, file);
}

int zmsg_savex_frame(zframe_t *frame, FILE *file) {
    size_t frame_size = zframe_size (frame);
    if (fwrite (&frame_size, sizeof (frame_size), 1, file) != 1)
        return -1;
    if (fwrite (zframe_data (frame), frame_size, 1, file) != 1)
        return -1;
    return 0;
}

//  --------------------------------------------------------------------------
//  Load/append an open file into message, create new message if
//  null message provided. Returns NULL if the message could not be
//  loaded.

zmsg_t *
zmsg_loadx (zmsg_t *self, FILE *file)
{
    assert (file);
    if (!self)
        self = zmsg_new ();
    if (!self)
        return NULL;

    size_t frame_count;
    size_t rc = fread (&frame_count, sizeof (frame_count), 1, file);

    if (rc == 1) {
        for (size_t i = 0; i < frame_count; i++) {
            size_t frame_size;
            rc = fread (&frame_size, sizeof (frame_size), 1, file);
            if (rc == 1) {
                zframe_t *frame = zframe_new (NULL, frame_size);
                if (!frame) {
                    zmsg_destroy (&self);
                    return NULL;    //  Unable to allocate frame, fail
                }
                rc = fread (zframe_data (frame), frame_size, 1, file);
                if (frame_size > 0 && rc != 1) {
                    zframe_destroy (&frame);
                    zmsg_destroy (&self);
                    return NULL;    //  Corrupt file, fail
                }
                if (zmsg_append (self, &frame) == -1) {
                    zmsg_destroy (&self);
                    return NULL;    //  Unable to add frame, fail
                }
            }
            else
                break;              //  Unable to read properly, quit
        }
    }
    if (!zmsg_size (self)) {
        zmsg_destroy (&self);
        self = NULL;
    }
    return self;
}

zhash_t* zlist_to_hash(zlist_t *list)
{
    zhash_t *hash = zhash_new();
    const char* elem = zlist_first(list);
    while (elem) {
        zhash_insert(hash, elem, (void*)1);
        elem = zlist_next(list);
    }
    return hash;
}

zlist_t* zlist_added(zlist_t *old, zlist_t *new)
{
    zlist_t *added = zlist_new();
    zlist_autofree(added);
    zhash_t *old_set = zlist_to_hash(old);
    char* new_elem = zlist_first(new);
    while (new_elem) {
        if (!zhash_lookup(old_set, new_elem))
            zlist_append(added, new_elem);
        new_elem = zlist_next(new);
    }
    zhash_destroy(&old_set);
    return added;
}

zlist_t* zlist_deleted(zlist_t *old, zlist_t *new)
{
    zlist_t *deleted = zlist_new();
    zlist_autofree(deleted);
    zhash_t *new_set = zlist_to_hash(new);
    char* old_elem = zlist_first(old);
    while (old_elem) {
        if (!zhash_lookup(new_set, old_elem))
            zlist_append(deleted, old_elem);
        old_elem = zlist_next(old);
    }
    zhash_destroy(&new_set);
    return deleted;
}

// unlike zsys_hostname() this supports IPV6
const char* my_fqdn()
{
    static char* fqdn = NULL;
    static char* hostname = NULL;
    int rc;

    if (fqdn) return fqdn;

    if (!hostname) {
        char buffer[1024];
        buffer[1023] = '\0';
        rc = gethostname(buffer, 1023);
        assert(rc==0);
        hostname = strdup(buffer);
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;

    struct addrinfo *info;
    rc = getaddrinfo(hostname, NULL, &hints, &info);
    if (rc != 0) {
        fprintf(stderr, "[E] could not determine FQDN: %s\n", gai_strerror(rc));
        fprintf(stderr, "[W] using hostname: %s\n", hostname);
        return hostname;
    }

    struct addrinfo *p;
    for (p = info; p != NULL; p = p->ai_next) {
        if (fqdn) free(fqdn);
        fqdn = strdup(p->ai_canonname);
        // printf("hostname: %s\n", p->ai_canonname);
    }
    freeaddrinfo(info);

    return fqdn;
}

void send_heartbeat(zsock_t *socket, msg_meta_t *meta, int pub_port)
{
    zmsg_t *msg = zmsg_new();
    zmsg_addstr(msg, "heartbeat");
    zmsg_addstrf(msg, "tcp://%s:%d", my_fqdn(), pub_port);
    zmsg_addstr(msg, "{}"); // empty JSON body
    zmsg_add_meta_info(msg, meta);
    zmsg_send_and_destroy(&msg, socket);
}

bool extract_app_env(const char* app_env, int n, char* app, char* env)
{
    char* p = rindex(app_env, '-');
    if (!p) return false;
    int i = 0;
    while (app_env != p) {
        if (i++ >= n) return false;
        *app++ = *app_env++;
    }
    *app = 0;
    app_env++;
    i = 0;
    while (*app_env) {
        if (i++ >= n) return false;
        *env++ = *app_env++;
    }
    *env = 0;
    return true;
}

bool extract_app_env_rid(const char* s, int n, char* app, char* env, char* rid)
{
    int l = strlen(s) + 1;
    char buf[l];
    memcpy(buf, s, l);
    char* p = rindex(buf, '-');
    if (!p) return false;
    *p++ = 0;
    int i = 0;
    while (*p) {
        if (i++ >= n) return false;
        *rid++ = *p++;
    }
    *rid = 0;
    return extract_app_env(buf, n, app, env);
}

void ensure_chunk_can_take(zchunk_t* buffer, size_t data_size)
{
    size_t buffer_size = zchunk_max_size(buffer);
    size_t target_size = zchunk_size(buffer) + data_size;
    if (buffer_size < target_size) {
        size_t next_size = 2 * buffer_size;
        while (next_size < target_size)
            next_size *= 2;
        zchunk_resize(buffer, next_size);
    }
}

void append_line(zchunk_t* buffer, const char* format, ...)
{
    char line[4096];
    va_list argptr;
    va_start(argptr, format);
    int n = vsnprintf(line, 4096, format, argptr);
    va_end(argptr);
    ensure_chunk_can_take(buffer, (size_t)n);
    // append without the null byte
    zchunk_append(buffer, line, n);
}

void append_null_byte(zchunk_t* buffer)
{
    ensure_chunk_can_take(buffer, 1);
    // append without the null byte
    zchunk_append(buffer, "", 1);
}

static void test_uint64wrap (int verbose)
{
    uint64_t i = 0xffffffffffffffff;
    assert(i+1 == 0);
}

static void test_negative_numbers_conversion_to_sizet (int verbose)
{
    size_t i = (int)-1;
    // printf("%zu\n", i);
    assert(i > 0);
}


static void test_gap_calc (int verbose)
{
    uint64_t i = 0xffffffffffffffff;
    uint64_t j = 0xfffffffffffffffe;
    int64_t k = i - j -1 ;
    // printf ("%" PRId64 "\n", k);
    assert (k == 0);
    k = i - i - 1;
    assert (k == -1);
}

static void test_htonll (int verbose)
{
    assert(ntohll(htonll(0))                    == 0);
    assert(ntohll(htonll(0x1))                  == 0x1);
    assert(ntohll(htonll(0x1ff))                == 0x1ff);
    assert(ntohll(htonll(0x1ffff))              == 0x1ffff);
    assert(ntohll(htonll(0x1ffffff))            == 0x1ffffff);
    assert(ntohll(htonll(0x1fffffff))           == 0x1fffffff);
    assert(ntohll(htonll(0x1fffffffff))         == 0x1fffffffff);
    assert(ntohll(htonll(0x1fffffffffff))       == 0x1fffffffffff);
    assert(ntohll(htonll(0x1fffffffffffff))     == 0x1fffffffffffff);
    assert(ntohll(htonll(0x1fffffffffffffff))   == 0x1fffffffffffffff);
    assert(ntohll(htonll(0xffffffffffffffff))   == 0xffffffffffffffff);
}

static void test_my_fqdn (int verbose)
{
    for (int i=0; i++ < 5;) {
        const char* hostname = my_fqdn();
        if (hostname && strchr(hostname, '.')) {
            if (verbose)
                printf("\tmy_fqdn: found hostname = %s\n", hostname);
            break;
        }
        sleep(1);
    }
}

static void test_extract_app_env (int verbose)
{
    bool ok;
    char app[100];
    char env[100];
    ok = extract_app_env("", 100, app, env);
    assert(!ok);
    ok = extract_app_env("a", 100, app, env);
    assert(!ok);
    ok = extract_app_env("a-e", 100, app, env);
    assert(ok);
    assert(streq(app, "a"));
    assert(streq(env, "e"));
    ok = extract_app_env("a-b-e", 100, app, env);
    assert(ok);
    assert(streq(app, "a-b"));
    assert(streq(env, "e"));
}

static void test_extract_app_env_rid (int verbose)
{
    bool ok;
    char app[100];
    char env[100];
    char rid[100];
    ok = extract_app_env_rid("", 100, app, env, rid);
    assert(!ok);
    ok = extract_app_env_rid("a", 100, app, env, rid);
    assert(!ok);
    ok = extract_app_env_rid("a-e", 100, app, env, rid);
    assert(!ok);
    ok = extract_app_env_rid("a-e-r", 100, app, env, rid);
    assert(ok);
    assert(streq(app, "a"));
    assert(streq(env, "e"));
    assert(streq(rid, "r"));
    ok = extract_app_env_rid("a-b-e-r", 100, app, env, rid);
    assert(ok);
    assert(streq(app, "a-b"));
    assert(streq(env, "e"));
    assert(streq(rid, "r"));
}

static void test_compression_decompression (int verbose)
{
    assert(sizeof(int32_t) == 4);
    const char* test_data[5] =
        {
         "{}",
         "111111111111111111111111111111111111111111111",
         "jjskdhfuds hf e iuweu fbiuwe eubcbwebcdbcdsub sghufhuhfushd fuhsfhshfuhsfh",
         "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
         "{\"id\":\"0001\",\"type\":\"donut\",\"name\":\"Cake\",\"ppu\":0.55,\"batters\":{\"batter\":[{\"id\":\"1001\",\"type\":\"Regular\"},{\"id\":\"1002\",\"type\":\"Chocolate\"},{\"id\":\"1003\",\"type\":\"Blueberry\"},{\"id\":\"1004\",\"type\":\"Devil's Food\"}]},\"topping\":[{\"id\":\"5001\",\"type\":\"None\"},{\"id\":\"5002\",\"type\":\"Glazed\"},{\"id\":\"5005\",\"type\":\"Sugar\"},{\"id\":\"5007\",\"type\":\"Powdered Sugar\"},{\"id\":\"5006\",\"type\":\"Chocolate with Sprinkles\"},{\"id\":\"5003\",\"type\":\"Chocolate\"},{\"id\":\"5004\",\"type\":\"Maple\"}]}"
        };
    const char* method_names[4] = {"lz4", "snappy", "zlib"};
    for (int k = 0; k < 5; k++) {
        const char* data = test_data[k];
        const size_t data_len = strlen(data);
        for (int i= 0; i < 3; i++) {
            zchunk_t *buffer = zchunk_new(NULL, 10);
            const char* method_name = method_names[i];
            int method = string_to_compression_method(method_name);
            zmq_msg_t body;
            zmq_msg_init(&body);
            compress_message_data(method, buffer, &body, data, data_len);
            size_t compressed_len = zmq_msg_size(&body);
            char* decompressed;
            size_t decompressed_len;
            zframe_t *frame = zframe_new(NULL, compressed_len);
            memcpy(zframe_data(frame), zmq_msg_data(&body), compressed_len);
            zmq_msg_close(&body);
            int rc = decompress_frame(frame, method, buffer, &decompressed, &decompressed_len);
            assert(rc);
            if (decompressed_len != data_len) {
                fprintf(stderr, "method %s: decompressed length (%zu) != original data length (%zu)\n",
                        method_name, decompressed_len, data_len);
                assert(0);
            }
            assert(0 == strncmp(data, decompressed, data_len));
            zframe_destroy(&frame);
            zchunk_destroy(&buffer);
        }
    }
}

void logjam_util_test (int verbose)
{
    printf (" * logjam-utils: ");
    if (verbose)
        printf("\n");

    test_htonll (verbose);
    test_uint64wrap (verbose);
    test_gap_calc (verbose);
    test_negative_numbers_conversion_to_sizet (verbose);
    test_my_fqdn (verbose);
    test_extract_app_env (verbose);
    test_extract_app_env_rid (verbose);
    test_compression_decompression (verbose);

    printf ("OK\n");
}
