#include "importer-livestream.h"

zsock_t* live_stream_socket_new()
{
    zsock_t *live_stream_socket = zsock_new(ZMQ_PUSH);
    assert(live_stream_socket);
    int rc = zsock_connect(live_stream_socket, "tcp://localhost:9607");
    assert(rc == 0);
    return live_stream_socket;
}

void live_stream_publish(zsock_t *live_stream_socket, const char* key, const char* json_str)
{
    int rc = 0;
    zframe_t *msg_key = zframe_new(key, strlen(key));
    zframe_t *msg_body = zframe_new(json_str, strlen(json_str));
    rc = zframe_send(&msg_key, live_stream_socket, ZFRAME_MORE|ZFRAME_DONTWAIT);
    // printf("[D] MSG frame 1 to live stream: rc=%d\n", rc);
    if (rc == 0) {
        rc = zframe_send(&msg_body, live_stream_socket, ZFRAME_DONTWAIT);
        if (rc)
            printf("[E] MSG frame 2 to live stream: rc=%d\n", rc);
    } else {
        printf("[E] MSG frame 1 to live stream: rc=%d\n", rc);
        zframe_destroy(&msg_body);
    }
}

void publish_error_for_module(stream_info_t *stream_info, const char* module, const char* json_str, zsock_t* live_stream_socket)
{
    size_t n = stream_info->app_len + 1 + stream_info->env_len + 1;
    // skip :: at the beginning of module
    while (*module == ':') module++;
    size_t m = strlen(module) + 1;
    char key[n + m + 3];
    sprintf(key, "%s-%s,%s", stream_info->app, stream_info->env, module);
    // TODO: change this crap in the live stream publisher
    // tolower is unsafe and not really necessary
    for (char *p = key; *p; ++p) *p = tolower(*p);

    live_stream_publish(live_stream_socket, key, json_str);
}
