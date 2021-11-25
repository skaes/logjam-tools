#include "gelf-message.h"

gelf_message* gelf_message_new(const char *host, const char *short_message)
{
    json_object *json = json_object_new_object();
    json_object_object_add(json, "version", json_object_new_string("1.1"));
    json_object_object_add(json, "host", json_object_new_string(host));
    json_object_object_add(json, "short_message", json_object_new_string(short_message));

    return (gelf_message *) json;
}

void gelf_message_add_string(gelf_message *msg, const char *key, const char *value)
{
    json_object *json = (json_object *) msg;
    json_object_object_add(json, key, json_object_new_string(value));
}

void gelf_message_add_double(gelf_message *msg, const char *key, double value)
{
    json_object *json = (json_object *) msg;
    json_object_object_add(json, key, json_object_new_double(value));
}

void gelf_message_add_int(gelf_message *msg, const char *key, int value)
{
    json_object *json = (json_object *) msg;
    json_object_object_add(json, key, json_object_new_int(value));
}

void gelf_message_add_json_object(gelf_message *msg, const char *key, json_object *obj)
{
    json_object *json = (json_object *) msg;
    // obj is now part of two container objects, so: increment reference count
    json_object_get(obj);
    json_object_object_add(json, key, obj);
}

const char* gelf_message_to_string(const gelf_message *msg)
{
    json_object *json = (json_object *) msg;
    return json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
}

void gelf_message_destroy(gelf_message **msg)
{
    if (*msg == NULL) return;
    json_object *json = (json_object *) *msg;
    json_object_put(json);
    *msg = NULL;
}
