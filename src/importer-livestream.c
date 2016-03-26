#include "importer-livestream.h"

zsock_t* live_stream_socket_new(zconfig_t* config)
{
    const char* connection_spec = zconfig_resolve(config, "livestream/endpoint", "tcp://localhost:9607");
    zsock_t *live_stream_socket = zsock_new(ZMQ_PUSH);
    assert(live_stream_socket);
    zsock_set_sndtimeo(live_stream_socket, 10);
    int rc = zsock_connect(live_stream_socket, "%s", connection_spec);
    assert(rc == 0);
    return live_stream_socket;
}

void live_stream_publish(zsock_t *live_stream_socket, const char* key, const char* json_str)
{
    if (dryrun) return;

    if (output_socket_ready(live_stream_socket, 0))
        zstr_sendx(live_stream_socket, key, json_str, NULL);
    else if (!zsys_interrupted)
        fprintf(stderr, "[E] live stream socket buffer full\n");
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
