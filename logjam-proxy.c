#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

void assert_x(int rc, const char* error_text) {
  if (rc != 0) {
      printf("Failed assertion: %s\n", error_text);
      exit(1);
  }
}

void log_zmq_error(int rc)
{
  if (rc != 0) {
      printf("rc: %d, errno: %d (%s)\n", rc, errno, zmq_strerror(errno));
  }
}

static zhash_t *subscriptions = NULL;

void* sub_socket_new(zctx_t *context)
{
    int rc;
    void *socket = zsocket_new(context, ZMQ_SUB);
    assert(socket);
    zsocket_set_rcvhwm(socket, 1000);
    zsocket_set_linger(socket, 500);
    zsocket_set_reconnect_ivl(socket, 100); // 100 ms
    zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s
    return socket;
}

typedef struct {
    char *stream;
    void *socket;
    size_t messages_transmitted;
    size_t messages_dropped;
} subscription_t;

void* worker_socket_new(zctx_t *context, char* file_name, char* ipcdir)
{
    int rc;
    void *socket = zsocket_new(context, ZMQ_PUSH);
    assert(socket);
    zsocket_set_sndhwm(socket, 5000);
    zsocket_set_linger(socket, 100);
    assert(ipcdir);
    assert(*ipcdir == '/');
    rc = zsocket_connect(socket, "ipc:/%s/%s", ipcdir, file_name);
    assert(rc == 0);
    return socket;
}

void add_subscription(zctx_t *context, void* sub_socket, char* stream, char* ipcdir)
{
    subscription_t *subscription = malloc(sizeof(subscription_t));
    assert(subscription);
    subscription->stream = stream;
    subscription->messages_dropped = 0;
    subscription->messages_transmitted = 0;
    subscription->socket = worker_socket_new(context, stream, ipcdir);
    assert(subscription->socket);
    zsocket_set_subscribe(sub_socket, stream);
    zhash_update(subscriptions, stream, subscription);
}

int dump_subscription(const char *key, void *item, void *argument)
{
    subscription_t *sub = item;
    printf("----------------------\n");
    printf("stream:  %s\n", sub->stream);
    printf("queued:  %lu\n", sub->messages_transmitted);
    printf("dropped: %lu\n", sub->messages_dropped);
    return 0;
}

int forward_message_for_subscription(zmsg_t *msg, subscription_t *subscription)
{
    int rc;
    zframe_t *stream = zmsg_pop(msg);
    rc = zframe_send(&stream, subscription->socket, ZFRAME_MORE|ZFRAME_DONTWAIT);
    if (rc!=0) {
        subscription->messages_dropped++;
        // log_zmq_error(rc);
        return rc;
    }
    zframe_t *routing_key = zmsg_pop(msg);
    rc = zframe_send(&routing_key, subscription->socket, ZFRAME_MORE|ZFRAME_DONTWAIT);
    if (rc!=0) {
        subscription->messages_dropped++;
        // log_zmq_error(rc);
        return rc;
    }
    zframe_t *message_body = zmsg_pop(msg);
    rc = zframe_send(&message_body, subscription->socket, ZFRAME_DONTWAIT);
    if (rc!=0) {
        subscription->messages_dropped++;
        // log_zmq_error(rc);
        return rc;
    }
    subscription->messages_transmitted++;
    return 0;
}

void get_subscription_and_forward_message(zmsg_t* msg)
{
    zframe_t* topic_frame = zmsg_first(msg);
    size_t frame_length = zframe_size(topic_frame);
    char key[frame_length+1];
    memcpy(key, zframe_data(topic_frame), frame_length);
    key[frame_length] = '\0';

    subscription_t *subscription = zhash_lookup(subscriptions, key);

    if (subscription) {
        forward_message_for_subscription(msg, subscription);
    } else {
        fprintf(stderr, "no subscription for: %s\n", key);
    }
}

void* configure(zconfig_t *config, zctx_t *context)
{
    int rc;
    zconfig_t *current;

    void* sub_socket = sub_socket_new(context);

    zconfig_t *endpoints = zconfig_locate(config, "frontend/endpoints");
    current = zconfig_child(endpoints);
    assert(current);
    do {
        char *binding = zconfig_value(current);
        rc = zsocket_connect(sub_socket, binding);
        assert(rc == 0);
        current = zconfig_next(current);
    } while (current);

    char *ipcdir = zconfig_resolve(config, "ipcdir", "/tmp");
    zconfig_t *streams = zconfig_locate(config, "backend/streams");
    current = zconfig_child(streams);
    assert(current);
    do {
        char *stream = zconfig_name(current);
        add_subscription(context, sub_socket, stream, ipcdir);
        current = zconfig_next(current);
    } while (current);

    return sub_socket;
}

int main(int argc, char const * const *argv)
{
    const char *config_file = "logjam.conf";
    if (argc > 2) {
        fprintf(stderr, "usage: %s [config-file]\n", argv[0]);
        exit(0);
    }
    if (argc == 2) {
        config_file = argv[1];
    }
    if (!zsys_file_exists(config_file)) {
        fprintf(stderr, "missing config file: %s\n", config_file);
        exit(0);
    }
    zconfig_t* config = zconfig_load((char*)config_file);

    setvbuf(stdout,NULL,_IOLBF,0);
    setvbuf(stderr,NULL,_IOLBF,0);

    zctx_t *context = zctx_new();
    assert(context);
    zctx_set_rcvhwm(context, 1000);
    zctx_set_linger(context, 100);

    subscriptions = zhash_new();
    void *sub_socket = configure(config, context);

    while (1) {
        zmsg_t *msg = zmsg_recv(sub_socket);
        if (zctx_interrupted)
            break;

        assert(msg);
        assert(zmsg_size(msg) == 3);
        zmsg_dump(msg);

        get_subscription_and_forward_message(msg);

        zmsg_destroy(&msg);
    }

    zhash_foreach(subscriptions, dump_subscription, NULL);

    zsocket_destroy(context, sub_socket);
    zctx_destroy(&context);

    return 0;
}
