#include <zmq.h>
#include <czmq.h>
#include "logjam-util.h"
#include "gelf-message.h"
#include "str-builder.h"
#include "logjam-message.h"
#include "logjam-streaminfo.h"
#include "graylog-forwarder-common.h"

const char *LOG_LEVELS_NAMES[6] = {
    "Debug",
    "Info",
    "Warn",
    "Error",
    "Fatal",
    "Unknown"
};

const int SYSLOG_MAPPING[6] = {
    7 /* Debug */,
    6 /* Info */,
    5 /* Notice */,
    4 /* Warning */,
    3 /* Error */,
    1 /* Alert */
};

static inline void str_underscore(char *str)
{
    for (char *p = str; *p; ++p) {
        if (*p == '-')
            *p = '_';
    }
}

static inline void str_lower(char *str)
{
    for (char *p = str; *p; ++p) {
        *p = tolower(*p);
    }
}

logjam_message* logjam_message_read(zsock_t *receiver)
{
    int i = 0, end_of_message = 0;
    zframe_t *frame = NULL;
    logjam_message *msg = (logjam_message *) zmalloc (sizeof (logjam_message));
    msg->size = 0;

    // read the message parts
    while (!zsys_interrupted && !end_of_message) {
        frame = zframe_recv (receiver);
        // zframe_print(frame, "FRAME");

        if (!zframe_more(frame)) {
            end_of_message = 1;
        }

        if (i>3) {
            zframe_destroy (&frame);
        } else {
            msg->frames[i] = frame;
            msg->size += zframe_size(frame);
        }
        i++;
    }

    int error = 0;
    if (i < 4) {
        if (!zsys_interrupted) {
            fprintf(stderr, "[E] received only %d message parts\n", i);
        }
        error = 1;
    } else if (i > 4) {
        fprintf(stderr, "[E] received more than 4 message parts\n");
        error = 1;
    }

    if (error) {
        for (int j = 0; j < i && j < 4; j++) {
            zframe_destroy (&msg->frames[j]);
        }
        free (msg);
        msg = NULL;
    } else {
        msg->stream = zframe_strdup(msg->frames[0]);
    }

    return msg;
}

char* extract_module(const char *action)
{
    int max_mod_len = strlen(action);
    char module_str[max_mod_len+1];
    char *mod_ptr = strchr(action, ':');
    strcpy(module_str, "::");
    if (mod_ptr != NULL){
        if (mod_ptr != action) {
            int mod_len = mod_ptr - action;
            memcpy(module_str+2, action, mod_len);
            module_str[mod_len+2] = '\0';
        }
    } else {
        char *action_ptr = strchr(action, '#');
        if (action_ptr != NULL) {
            int mod_len = action_ptr - action;
            memcpy(module_str+2, action, mod_len);
            module_str[mod_len+2] = '\0';
        }
    }
   return strdup(module_str);
}

gelf_message* logjam_message_to_gelf(logjam_message *logjam_msg, json_tokener *tokener, zhash_t *stream_info_cache, zchunk_t *decompression_buffer, zchunk_t *buffer, zhash_t *header_fields)
{
    json_object *obj = NULL, *http_request = NULL, *lines = NULL;
    const char *host = "Not found", *action = "";
    char *str = NULL;

    // extract meta information
    msg_meta_t meta;
    frame_extract_meta_info(logjam_msg->frames[3], &meta);

    char *app_env = zframe_strdup (logjam_msg->frames[0]);
    stream_info_t *stream_info = get_stream_info(app_env, stream_info_cache);
    if (stream_info == NULL) {
        if (verbose)
            fprintf(stderr, "[W] dropped request from unknown stream: %s\n", app_env);
        free(app_env);
        return NULL;
    }

    // decompress if necessary
    char *json_data;
    size_t json_data_len;
    if (meta.compression_method) {
        decompress_frame(logjam_msg->frames[2], meta.compression_method, decompression_buffer, &json_data, &json_data_len);
    } else {
        json_data = (char*)zframe_data(logjam_msg->frames[2]);
        json_data_len = zframe_size(logjam_msg->frames[2]);
    }

    // now see whether we can parse it
    json_object *request = parse_json_data(json_data, json_data_len, tokener);

    if (!request) {
        free(app_env);
        return NULL;
    }

    // dump_json_object(stdout, "[D]", request);

    if (json_object_object_get_ex (request, "host", &obj)) {
        host = json_object_get_string (obj);
    }
    if (json_object_object_get_ex (request, "action", &obj)) {
        action = json_object_get_string (obj);
        if (action == NULL)
            action = "";
    }

    int action_len = strlen (action);
    zchunk_ensure_size (buffer, action_len + 100);
    char *buf = (char*) zchunk_data (buffer);
    *buf = '\0';
    strcat(buf, action);
    char *pos = buf + action_len;

    if (action_len == 0)
        strcat (pos, "Unknown#unknown_method");
    else if (!strchr(action, '#'))
        strcat (pos, "#unknown_method");
    else if (action[action_len-1] == '#')
        strcat (pos, "unknown_method");
    action = buf;

    gelf_message *gelf_msg = gelf_message_new (host, action);

    gelf_message_add_string (gelf_msg, "_app", app_env);

    double timestamp;

    // use logjam_agent's started_ms if available, current time as fallback
    if (json_object_object_get_ex (request, "started_ms", &obj)) {
        int64_t started_ms = json_object_get_int64(obj);
        timestamp = started_ms / 1000.0;
    } else {
        timestamp = zclock_time() / 1000.0;
    }

    gelf_message_add_double(gelf_msg, "timestamp", timestamp);

    if (json_object_object_get_ex (request, "code", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_code", obj);
    }

    if (json_object_object_get_ex (request, "request_id", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_request_id", obj);
    }

    if (json_object_object_get_ex (request, "ip", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_ip", obj);
    }

    if (json_object_object_get_ex (request, "process_id", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_process_id", obj);
    }

    if (json_object_object_get_ex (request, "datacenter", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_datacenter", obj);
    } else {
        gelf_message_add_string (gelf_msg, "_datacenter", default_datacenter);
    }

    if (json_object_object_get_ex (request, "user_id", &obj)
            && json_object_get_type (obj) != json_type_null) {
        gelf_message_add_json_object (gelf_msg, "_user_id", obj);
    }

    if (json_object_object_get_ex (request, "total_time", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_total_time", obj);
    }

    if (json_object_object_get_ex (request, "request_info", &http_request)) {
        if (json_object_object_get_ex (http_request, "method", &obj)) {
            gelf_message_add_json_object (gelf_msg, "_http_method", obj);
        }

        if (json_object_object_get_ex (http_request, "url", &obj)) {
            gelf_message_add_json_object (gelf_msg, "_http_url", obj);
            const char *path = json_object_get_string(obj);
            char* module = extract_module(action);
            adjust_caller_info(path, module, request, stream_info);
            free(module);
        }

        if (json_object_object_get_ex (http_request, "headers", &obj)) {
            json_type jtype = json_object_get_type (obj);
            if (jtype != json_type_object) {
                fprintf(stderr, "[W] unexpected json data type for headers: %s; app: %s, action: %s\n",
                        json_type_to_name(jtype),
                        app_env,
                        action
                        );
                // dump_json_object(stderr, "[W]", request);
            } else {
                char header[1024] = "_http_header_";
                // clear buffer data
                zchunk_t *extra_headers = zchunk_new(0, 0);
                json_object_object_foreach (obj, key, value) {
                    char *lowkey = strdup(key);
                    str_lower(lowkey);
                    if (zhash_lookup(header_fields, lowkey)) {
                        snprintf (header, 1024, "_http_header_%s", lowkey);
                        str_underscore(header + 13);
                        gelf_message_add_json_object (gelf_msg, header, value);
                    } else {
                        if (zchunk_size(extra_headers) > 0)
                            zchunk_extend(extra_headers, "\n", 1);
                        zchunk_extend(extra_headers, lowkey, strlen(lowkey));
                        zchunk_extend(extra_headers, ": ", 2);
                        const char *val = json_object_get_string(value);
                        zchunk_extend(extra_headers, val, strlen(val));
                    }
                    free(lowkey);
                }
                if (zchunk_size(extra_headers) > 0) {
                    zchunk_extend(extra_headers, "", 1);
                    const char *data = (const char*) zchunk_data(extra_headers);
                    gelf_message_add_string(gelf_msg, "_http_headers_not_extracted", data);
                }
                zchunk_destroy(&extra_headers);
            }
        }
    }

    // needs to happen after the call to adjust_caller_info
    if (json_object_object_get_ex (request, "caller_id", &obj) && json_object_get_type(obj) == json_type_string) {
        const char *caller_id = json_object_get_string(obj);
        if (caller_id && *caller_id) {
            gelf_message_add_json_object (gelf_msg, "_caller_id", obj);
            char app[256], env[256], rid[256];
            if (extract_app_env_rid (caller_id, 256, app, env, rid)) {
                json_object *app_obj = json_object_new_string(app);
                gelf_message_add_json_object (gelf_msg, "_caller_app", app_obj);
                json_object_put(app_obj);
            }
        }
    }

    // needs to happen after the call to adjust_caller_info
    if (json_object_object_get_ex (request, "caller_action", &obj) && json_object_get_type(obj) == json_type_string) {
        const char *caller_action = json_object_get_string(obj);
        if (caller_action && *caller_action)
            gelf_message_add_json_object (gelf_msg, "_caller_action", obj);
    }

    // forward trace_id field
    if (json_object_object_get_ex (request, "trace_id", &obj) && json_object_get_type(obj) == json_type_string) {
        const char *trace_id = json_object_get_string(obj);
        if (trace_id && *trace_id) {
            gelf_message_add_json_object (gelf_msg, "_trace_id", obj);
        }
    }

    // forward sender_id field
    if (json_object_object_get_ex (request, "sender_id", &obj) && json_object_get_type(obj) == json_type_string) {
        const char *sender_id = json_object_get_string(obj);
        if (sender_id && *sender_id) {
            gelf_message_add_json_object (gelf_msg, "_sender_id", obj);
        }
    }

    // forward sender_action field
    if (json_object_object_get_ex (request, "sender_action", &obj) && json_object_get_type(obj) == json_type_string) {
        const char *sender_action = json_object_get_string(obj);
        if (sender_action && *sender_action)
            gelf_message_add_json_object (gelf_msg, "_sender_action", obj);
    }

    int level = 0; // Debug
    if (json_object_object_get_ex (request, "severity", &obj)) {
        level = json_object_get_int (obj);
    }

    if (json_object_object_get_ex (request, "lines", &lines) && json_object_get_type(lines) == json_type_array) {
        int n_lines = json_object_array_length (lines);

        str_builder *sb = sb_new (1024*10);
        for (int i = 0; i < n_lines; i++) {
            json_object *line = json_object_array_get_idx (lines, i);
            if (line && json_object_get_type (line) == json_type_array) {
                obj = json_object_array_get_idx (line, 0);
                int l = json_object_get_int (obj);
                if (l > level)
                    level = l;
                sb_append(sb, LOG_LEVELS_NAMES[l], strlen (LOG_LEVELS_NAMES[l]));
                sb_append(sb, " ", 1);

                obj = json_object_array_get_idx (line, 1);
                str = (char *) json_object_get_string(obj);
                sb_append (sb, str, strlen (str));
                sb_append (sb, " ", 1);

                obj = json_object_array_get_idx (line, 2);
                str = (char *) json_object_get_string (obj);
                sb_append (sb, str, strlen (str));
                sb_append (sb, "\n", 1);
            }
        }
        gelf_message_add_full_message (gelf_msg, sb_string(sb));
        sb_destroy (&sb);
    }

    gelf_message_add_int (gelf_msg, "level", SYSLOG_MAPPING[level]);

    gelf_message_add_int (gelf_msg, "_logjam_message_size", json_data_len);

    free (app_env);
    release_stream_info(stream_info);
    json_object_put (request);

    return gelf_msg;
}

void logjam_message_destroy(logjam_message **msg)
{
    if (*msg == NULL)
        return;

    for (int i = 0; i < 4; i++) {
        zframe_destroy (&(*msg)->frames[i]);
    }
    free ((*msg)->stream);
    free (*msg);
    *msg = NULL;
}
