#ifndef __GELF_MESSAGE_H_INCLUDED__
#define __GELF_MESSAGE_H_INCLUDED__

#include <json-c/json.h>

#define gelf_message_add_full_message(m,v) gelf_message_add_string(m, "full_message", v)
#define gelf_message_add_timestamp(m,v) gelf_message_add_double(m, "timestamp", v)
#define gelf_message_add_level(m,v) gelf_message_add_int(m, "level", v)

typedef json_object gelf_message;

gelf_message* gelf_message_new(const char *host, const char *short_message);

void gelf_message_add_string(gelf_message *msg, const char *key, const char *value);

void gelf_message_add_double(gelf_message *msg, const char *key, double value);

void gelf_message_add_int(gelf_message *msg, const char *key, int value);

void gelf_message_add_json_object(gelf_message *msg, const char *key, json_object *obj);

const char* gelf_message_to_string(const gelf_message *msg);

void gelf_message_destroy(gelf_message **msg);

#endif
