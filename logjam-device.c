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

void die_on_error(int x, char const *context) {
  if (x < 0) {
    char *errstr = amqp_error_string(-x);
    fprintf(stderr, "%s: %s\n", context, errstr);
    free(errstr);
    exit(1);
  }
}

void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return;

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

  exit(1);
}

void assert_x(int rc, const char* error_text) {
  if (rc != 0) {
    printf("Failed assertion: %s\n", error_text);
    exit(1);
  }
}

int main(int argc, char const * const *argv)
{
  int rc;
  int my_port;
  int rabbit_port;

  my_port = atoi(argv[1]);
  rabbit_port = atoi(argv[2]);

  void *context = zmq_init(1);
  assert_x(context==NULL, "zmq_init failed");

  //  Socket to receive messages on
  void *receiver = zmq_socket(context, ZMQ_PULL);
  assert_x(receiver==NULL, "zmq socket creation failed");

  //  Configure the socket
  int64_t hwm = 100;
  rc = zmq_setsockopt(receiver, ZMQ_HWM, &hwm, sizeof hwm);
  assert_x(rc, "could not set zmq HWM");

  char connection_spec[256];
  int last = snprintf(connection_spec, sizeof(connection_spec), "tcp://*:%d", my_port);
  connection_spec[last] = 0;

  rc = zmq_bind(receiver, connection_spec);
  assert_x(rc, "zmq socket creation failed");

  // setup amqp connection
  int sockfd;
  amqp_connection_state_t conn;
  conn = amqp_new_connection();

  die_on_error(sockfd = amqp_open_socket("localhost", rabbit_port), "Opening socket");
  amqp_set_sockfd(conn, sockfd);

  die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"), "Logging in");
  amqp_channel_open(conn, 1);
  die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");

  // hash of already declared exchanges
  zhash_t * exchanges = zhash_new();

  // process tasks forever
  while (1) {
    // read the message parts
    int i = 0;
    zmq_msg_t message_parts[3];
    int64_t more;
    size_t more_size = sizeof(more);
    while (1) {
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
      printf("Received only %d message parts\n", i);
      goto cleanup;
    }

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
      die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring exchange");
    }

    amqp_bytes_t routing_key;
    routing_key.len   = zmq_msg_size(&message_parts[1]);
    routing_key.bytes = zmq_msg_data(&message_parts[1]);

    amqp_bytes_t message_body;
    message_body.len   = zmq_msg_size(&message_parts[2]);
    message_body.bytes = zmq_msg_data(&message_parts[2]);

    // send message to rabbit
    die_on_error(amqp_basic_publish(conn,
				    1,
				    exchange_name,
				    routing_key,
				    0,
				    0,
				    NULL,
				    message_body),
		 "Publishing");

  cleanup:
    for(;i>=0;i--) {
      zmq_msg_close(&message_parts[i]);
    }
  }

  // close zmq socket and context
  zmq_close(receiver);
  zmq_term(context);

  // close amqp connection
  die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
  die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS), "Closing connection");
  die_on_error(amqp_destroy_connection(conn), "Ending connection");

  return 0;
}
