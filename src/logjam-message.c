#include <zmq.h>
#include <czmq.h>
#include "logjam-util.h"
#include "gelf-message.h"
#include "str-builder.h"
#include "logjam-message.h"

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

struct _logjam_message {
    zframe_t *frames[4];
    size_t size;
};

static inline void str_normalize(char *str)
{
    for (char *p = str; *p; ++p) {
        *p = tolower(*p);
        if (*p == '-')
            *p = '_';
    }
}

size_t logjam_message_size(logjam_message *msg)
{
    return msg->size;
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
    }

    return msg;
}

gelf_message* logjam_message_to_gelf(logjam_message *logjam_msg, zchunk_t *decompression_buffer)
{
    json_object *obj = NULL, *http_request = NULL, *lines = NULL;
    const char *host = "Not found", *action = "Not found";
    char *str = NULL;

    // TODO: no need to allocate a new tokener for each message
    json_tokener* tokener = json_tokener_new();

    // extract meta information
    msg_meta_t meta;
    frame_extract_meta_info(logjam_msg->frames[3], &meta);

    // decompress if necessary
    char *json_data;
    size_t json_data_len;
    if (meta.compression_method) {
        uncompress_frame(logjam_msg->frames[2], meta.compression_method, decompression_buffer, &json_data, &json_data_len);
    } else {
        json_data = (char*)zframe_data(logjam_msg->frames[2]);
        json_data_len = zframe_size(logjam_msg->frames[2]);
    }

    // now see whether we can parse it
    json_object *request = parse_json_data(json_data, json_data_len, tokener);

    if (!request) {
        json_tokener_free(tokener);
        return NULL;
    }

    // dump_json_object(stdout, "[D]", request);

    if (json_object_object_get_ex (request, "host", &obj)) {
        host = json_object_get_string (obj);
    }

    if (json_object_object_get_ex (request, "action", &obj)) {
        action = json_object_get_string (obj);
    }

    gelf_message *gelf_msg = gelf_message_new (host, action);

    char *app_env = zframe_strdup (logjam_msg->frames[0]);
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

    if (json_object_object_get_ex (request, "caller_id", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_caller_id", obj);
    }

    if (json_object_object_get_ex (request, "caller_action", &obj)) {
        gelf_message_add_json_object (gelf_msg, "_caller_action", obj);
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
                json_object_object_foreach (obj, key, value) {
                    snprintf (header, 1024, "_http_header_%s", key);
                    str_normalize (header + 13);
                    gelf_message_add_json_object (gelf_msg, header, value);
                }
            }
        }
    }

    int level = 0; // Debug
    if (json_object_object_get_ex (request, "severity", &obj)) {
        level = json_object_get_int (obj);
    }

    if (json_object_object_get_ex (request, "lines", &lines)
            && json_object_get_type(lines) == json_type_array) {
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

    free (app_env);
    json_object_put (request);
    json_tokener_free (tokener);

    return gelf_msg;
}

void logjam_message_destroy(logjam_message **msg)
{
    for (int i = 0; i < 4; i++) {
        zframe_destroy (&(*msg)->frames[i]);
    }
    free (*msg);
    *msg = NULL;
}
