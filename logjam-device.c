#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <amqp.h>
#include <amqp_framing.h>


void log_error(int x, char const *context) {
  if (x < 0) {
    char *errstr = amqp_error_string(-x);
    fprintf(stderr, "%s: %s\n", context, errstr);
    free(errstr);
  }
}

void die_on_error(int x, char const *context) {
  if (x < 0) {
    log_error(x, context);
    exit(1);
  }
}

int log_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return 0;

    case AMQP_RESPONSE_NONE:
      fprintf(stderr, "%s: missing RPC reply type!\n", context);
      break;

    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      fprintf(stderr, "%s: %s\n", context, amqp_error_string(x.library_error));
      break;

    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id) {
	case AMQP_CONNECTION_CLOSE_METHOD: {
	  amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
	  fprintf(stderr, "%s: server connection error %d, message: %.*s\n",
		  context,
		  m->reply_code,
		  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  break;
	}
	case AMQP_CHANNEL_CLOSE_METHOD: {
	  amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
	  fprintf(stderr, "%s: server channel error %d, message: %.*s\n",
		  context,
		  m->reply_code,
		  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  break;
	}
	default:
	  fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
	  break;
      }
      break;
  }

  return 1;
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
  if (0 != log_amqp_error(x, context)) {
    exit(1);
  }
}

void assert_x(int rc, const char* error_text) {
  if (rc != 0) {
    printf("Failed assertion: %s\n", error_text);
    exit(1);
  }
}

static char* rabbit_host = "localhost";
static int   rabbit_port = 5672;

int main(int argc, char const * const *argv)
{
  int rc=0, amqp_rc=0;
  int my_port;
  unsigned long received_count = 0;

  my_port = atoi(argv[1]);
  rabbit_port = atoi(argv[2]);

  // create context
  zctx_t *context = zctx_new();
  assert_x(context==NULL, "zmq_init failed");

  // set default socket options
  zctx_set_linger(context, 100);
  zctx_set_hwm(context, 1000);

  // create socket to receive messages on
  void *receiver = zsocket_new(context, ZMQ_PULL);
  assert_x(receiver==NULL, "zmq socket creation failed");

  //  configure the socket
  zsocket_set_hwm(receiver, 100);
  zsocket_set_linger(receiver, 1000);

  rc = zsocket_bind(receiver, "tcp://%s:%d", "*", my_port);
  assert_x(rc!=my_port, "zmq socket bind failed");

  // setup amqp connection
  int sockfd;
  amqp_connection_state_t conn;
  conn = amqp_new_connection();

  die_on_error(sockfd = amqp_open_socket(rabbit_host, rabbit_port), "Opening RabbitMQ socket");

#if defined(__APPLE__) && defined(SO_NOSIGPIPE)
  // why doesn't librabbitmq do this, eh?
  int one = 1; /* for setsockopt */
  setsockopt(sockfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  amqp_set_sockfd(conn, sockfd);

  die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                    "Logging in to RabbitMQ");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening AMQP channel");

  // hash of already declared exchanges
  zhash_t *exchanges = zhash_new();

  // process tasks forever
  while (!zctx_interrupted) {
    // read the message parts
    int i = 0;
    zmq_msg_t message_parts[3];
    int64_t more;
    size_t more_size = sizeof(more);
    while (!zctx_interrupted) {
      // printf("receiving part %d\n", i+1);
      if (i>2) {
        printf("Received more than 3 message parts\n");
        exit(1);
      }
      zmq_msg_init(&message_parts[i]);
      zmq_recv(receiver, &message_parts[i], 0);
      zmq_getsockopt(receiver, ZMQ_RCVMORE, &more, &more_size);
      if (!more)
        break;
      i++;
    }
    if (i<2) {
      if (!zctx_interrupted) {
        printf("Received only %d message parts\n", i);
      }
      goto cleanup;
    }
    received_count++;

    // extract data
    amqp_bytes_t exchange_name;
    exchange_name.len   = zmq_msg_size(&message_parts[0]);
    exchange_name.bytes = zmq_msg_data(&message_parts[0]);

    char *exchange_string = malloc(exchange_name.len + 1);
    memcpy(exchange_string, exchange_name.bytes, exchange_name.len);
    exchange_string[exchange_name.len] = 0;

    int found = zhash_insert(exchanges, exchange_string, NULL);
    if (-1 == found) {
      free(exchange_string);
    } else {
      // declare exchange, as we haven't seen it before
      amqp_bytes_t exchange_type;
      exchange_type = amqp_cstring_bytes("topic");
      amqp_exchange_declare(conn, 1, exchange_name, exchange_type, 0, 0, amqp_empty_table);
      die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring AMQP exchange");
    }

    amqp_bytes_t routing_key;
    routing_key.len   = zmq_msg_size(&message_parts[1]);
    routing_key.bytes = zmq_msg_data(&message_parts[1]);

    amqp_bytes_t message_body;
    message_body.len   = zmq_msg_size(&message_parts[2]);
    message_body.bytes = zmq_msg_data(&message_parts[2]);

    //printf("publishing\n");

    // send message to rabbit
    amqp_rc = amqp_basic_publish(conn,
                                 1,
                                 exchange_name,
                                 routing_key,
                                 0,
                                 0,
                                 NULL,
                                 message_body);

    if (amqp_rc < 0) {
      log_error(amqp_rc, "Publishing via AMQP");
      zctx_interrupted = 1;
    }

  cleanup:
    for(;i>=0;i--) {
      zmq_msg_close(&message_parts[i]);
    }

  }

  printf("received %lu messages", received_count);

  // close zmq socket and context
  zsocket_destroy(context, receiver);
  zctx_destroy(&context);

  // close amqp connection
  if (amqp_rc >= 0) {
    log_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing AMQP channel");
    log_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing AMQP connection");
  }
  log_error(amqp_destroy_connection(conn), "Ending AMQP connection");

  return (amqp_rc < 0);
}
