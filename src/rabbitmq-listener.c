#include <zmq.h>
#include <czmq.h>
#include <amqp_tcp_socket.h>
#include <amqp.h>
#include <amqp_framing.h>
#include "rabbitmq-listener.h"

#if defined(__APPLE__) && defined(SO_NOSIGPIPE)
#define FIX_SIG_PIPE
#endif

#define FRAME_MAX_DEFAULT 131072
#define OUR_FRAME_MAX (8 * FRAME_MAX_DEFAULT)

char* rabbit_host = NULL;
char* rabbit_env = "development";
int   rabbit_port = 5672;

typedef struct {
    amqp_connection_state_t conn;
    void *receiver;
} rabbit_listener_state_t;

static
void log_error(int x, char const *context) {
    if (x < 0) {
        char const *errstr = amqp_error_string2(-x);
        fprintf(stderr, "[E] %s: %s\n", context, errstr);
    }
}

static
int log_amqp_error(amqp_rpc_reply_t x, char const *context) {
    switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
        return 0;

    case AMQP_RESPONSE_NONE:
        fprintf(stderr, "[E] %s: missing RPC reply type!\n", context);
        break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
        fprintf(stderr, "[E] %s: %s\n", context, amqp_error_string2(x.library_error));
        break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
        switch (x.reply.id) {
	case AMQP_CONNECTION_CLOSE_METHOD: {
            amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
            fprintf(stderr, "[E] %s: server connection error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
            break;
	}
	case AMQP_CHANNEL_CLOSE_METHOD: {
            amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
            fprintf(stderr, "[E] %s: server channel error %d, message: %.*s\n",
                    context,
                    m->reply_code,
                    (int) m->reply_text.len, (char *) m->reply_text.bytes);
            break;
	}
	default:
            fprintf(stderr, "[E] %s: unknown server error, method id 0x%08X\n", context, x.reply.id);
            break;
        }
        break;
    }

    return 1;
}

static
void die_on_amqp_error(amqp_rpc_reply_t x, char const *context)
{
    if (0 != log_amqp_error(x, context)) {
        assert(false);
    }
}

static
bool output_socket_ready(void *socket)
{
    int msecs = 0;
    zmq_pollitem_t items[] = { { socket, 0, ZMQ_POLLOUT, 0 } };
    int rc = zmq_poll(items, 1, msecs);
    return rc != -1 && (items[0].revents & ZMQ_POLLOUT) != 0;
}

static
amqp_connection_state_t setup_amqp_connection()
{
    amqp_connection_state_t connection = amqp_new_connection();
    amqp_socket_t *socket = amqp_tcp_socket_new(connection);

#ifdef FIX_SIG_PIPE
    // why doesn't librabbitmq do this, eh?
    int sockfd = amqp_get_sockfd(connection);
    int one = 1; //
    setsockopt(sockfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    int status = amqp_socket_open(socket, rabbit_host, rabbit_port);
    assert_x(!status, "Opening RabbitMQ socket");

    die_on_amqp_error(amqp_login(connection, "/", 0, OUR_FRAME_MAX, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                      "Logging in to RabbitMQ");
    amqp_channel_open(connection, 1);
    die_on_amqp_error(amqp_get_rpc_reply(connection), "Opening AMQP channel");

    return connection;
}

static
void shutdown_amqp_connection(amqp_connection_state_t connection, int amqp_rc, uint num_channels)
{
    // close amqp connection, including all open channels
    if (amqp_rc >= 0) {
        for (int i=1; i <= num_channels; i++)
            log_amqp_error(amqp_channel_close(connection, i, AMQP_REPLY_SUCCESS), "closing AMQP channel");
        log_amqp_error(amqp_connection_close(connection, AMQP_REPLY_SUCCESS), "closing AMQP connection");
    }
    log_error(amqp_destroy_connection(connection), "closing AMQP connection");
}

static
void rabbitmq_add_queue(amqp_connection_state_t conn, amqp_channel_t* channel_ref, const char *stream)
{
    size_t n = strlen(stream);
    char app[n], env[n];
    memset(app, 0, n);
    memset(env, 0, n);
    sscanf(stream, "%[^-]-%[^-]", app, env);
    if (strcmp(env, rabbit_env)) {
        printf("[I] skipping: %s-%s\n", app, env);
        return;
    }
    // printf("[D] processing stream %s-%s\n", app, env);

    char exchange[n+16];
    memset(exchange, 0, n+16);
    sprintf(exchange, "request-stream-%s-%s", app, env);
    // printf("[D] exchange: %s\n", exchange);

    char queue[n+15];
    memset(queue, 0, n+15);
    sprintf(queue, "logjam-device-%s-%s", app, env);
    // printf("[D] queue: %s\n", queue);

    printf("[I] binding: %s ==> %s\n", exchange, queue);

    amqp_channel_t channel = ++(*channel_ref);
    if (channel > 1) {
        // printf("[D] opening channel %d\n", channel);
        amqp_channel_open(conn, channel);
        die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening AMQP channel");
    }

    // amqp_exchange_declare(amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t exchange,
    //                       amqp_bytes_t type, amqp_boolean_t passive, amqp_boolean_t durable, amqp_table_t arguments);
    amqp_exchange_declare(conn, channel, amqp_cstring_bytes(exchange), amqp_cstring_bytes("topic"),
                          0, 1, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "declaring exchange");

    amqp_bytes_t queuename = amqp_cstring_bytes(queue);

    struct amqp_table_entry_t_ queue_arg_table_entries[1] = {
        { amqp_cstring_bytes("x-message-ttl"), { AMQP_FIELD_KIND_I32, { 60 * 1000 } } }
    };
    amqp_table_t queue_argument_table = {1, queue_arg_table_entries};

    // amqp_queue_declare(amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue,
    //                    amqp_boolean_t passive, amqp_boolean_t durable, amqp_boolean_t exclusive, amqp_boolean_t auto_delete,
    //                    amqp_table_t arguments);
    amqp_queue_declare(conn, channel, queuename, 0, 0, 0, 1, queue_argument_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "declaring queue");

    // amqp_queue_bind(amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue,
    //                 amqp_bytes_t exchange, amqp_bytes_t routing_key, amqp_table_t arguments)
    const char *routing_key = "#";
    amqp_queue_bind(conn, channel, queuename, amqp_cstring_bytes(exchange), amqp_cstring_bytes(routing_key), amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "binding queue");

    // amqp_basic_consume(amqp_connection_state_t state, amqp_channel_t channel, amqp_bytes_t queue, amqp_bytes_t consumer_tag,
    // amqp_boolean_t no_local, amqp_boolean_t no_ack, amqp_boolean_t exclusive, amqp_table_t arguments)
    amqp_basic_consume(conn, channel, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(conn), "consuming");
}

static
int rabbitmq_consume_message_and_forward(zloop_t *loop, zmq_pollitem_t *item, void* arg)
{
    rabbit_listener_state_t *state = (rabbit_listener_state_t*)arg;
    void *receiver = state->receiver;
    amqp_connection_state_t conn = state->conn;
    amqp_rpc_reply_t res;
    amqp_envelope_t envelope;

    amqp_maybe_release_buffers(conn);

    // printf("[D] consuming\n");
    res = amqp_consume_message(conn, &envelope, NULL, 0);

    if (AMQP_RESPONSE_NORMAL != res.reply_type) {
        zctx_interrupted = 1;
        log_amqp_error(res, "consuming from RabbitMQ");
        return -1;
    }

    // printf("[D] delivery %u, exchange %.*s routingkey %.*s\n",
    //        (unsigned) envelope.delivery_tag,
    //        (int) envelope.exchange.len, (char *) envelope.exchange.bytes,
    //        (int) envelope.routing_key.len, (char *) envelope.routing_key.bytes);
    //
    // if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
    //     printf("[D] content-type: %.*s\n",
    //            (int) envelope.message.properties.content_type.len,
    //            (char *) envelope.message.properties.content_type.bytes);
    // }

    // send messages to main thread
    zmsg_t *msg = zmsg_new();
    // skip request-stream prefix, leaving only app-env
    if (envelope.exchange.len > 15 && !strncmp(envelope.exchange.bytes, "request-stream-", 15))
        zmsg_addmem(msg, envelope.exchange.bytes+15, envelope.exchange.len-15);
    else
        zmsg_addmem(msg, envelope.exchange.bytes, envelope.exchange.len);
    zmsg_addmem(msg, envelope.routing_key.bytes, envelope.routing_key.len);
    zmsg_addmem(msg, envelope.message.body.bytes, envelope.message.body.len);
    // zmsg_dump(msg);

    if (output_socket_ready(receiver)) {
        zmsg_send(&msg, receiver);
    } else {
        if (!zctx_interrupted)
            fprintf(stderr, "[E] dropped message because receiver socket wasn't ready\n");
        zmsg_destroy(&msg);
    }

    amqp_destroy_envelope(&envelope);

    return 0;
}

static
int rabbitmq_setup_queues(amqp_connection_state_t conn, zconfig_t *config)
{
    amqp_channel_t channel = 1;

    zconfig_t *streams = zconfig_locate(config, "backend/streams");
    assert(streams);

    zconfig_t *stream_config = zconfig_child(streams);
    assert(stream_config);

    do {
        char *stream = zconfig_name(stream_config);
        rabbitmq_add_queue(conn, &channel, stream);
        stream_config = zconfig_next(stream_config);
    } while (stream_config && !zctx_interrupted);

    return channel;
}

static
int pipe_command(zloop_t *loop, zsock_t *pipe, void *args)
{
    zmsg_t *msg = zmsg_recv(pipe);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        zmsg_destroy(&msg);
        if (streq(cmd, "$TERM")) {
            free(cmd);
            return -1;
        } else {
            printf("[E] rabbitmq_listener received unknown actor command: %s\n", cmd);
            free(cmd);
            assert(false);
        }
    }
    return 0;
}

void rabbitmq_listener(zsock_t *pipe, void* args)
{
    // signal readyiness immediately so that zmq publishers are already processed
    // while the rabbitmq exchanges/queues/bindings are created
    zsock_signal(pipe, 0);

    amqp_connection_state_t conn = setup_amqp_connection();
    int last_channel = rabbitmq_setup_queues(conn, (zconfig_t*)args);

    // connect to the receiver socket
    zsock_t *receiver = zsock_new(ZMQ_PUSH);
    zsock_set_sndhwm(receiver, 10000);
    zsock_connect(receiver, "inproc://receiver");

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // register actor command handler
    int rc = zloop_reader(loop, pipe, pipe_command, NULL);
    assert(rc==0);

    // register rabbitmq socket for pollin events
    zmq_pollitem_t rabbit_item = {
        .fd = amqp_get_sockfd(conn),
        .events = ZMQ_POLLIN
    };
    rabbit_listener_state_t listener_state = {
        .conn = conn,
        .receiver = zsock_resolve(receiver)
    };
    rc = zloop_poller(loop, &rabbit_item, rabbitmq_consume_message_and_forward, &listener_state);
    assert(rc==0);

    // start event loop
    zloop_start(loop);

    // shutdown
    zloop_destroy(&loop);
    zsock_destroy(&receiver);
    shutdown_amqp_connection(conn, 0, last_channel);
}
