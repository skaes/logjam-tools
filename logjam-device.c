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
static unsigned long received_messages_count = 0;
static int TIMER_ID = 1;
static amqp_connection_state_t connection;
static int amqp_rc = 0;
// hash of already declared exchanges
static zhash_t *exchanges = NULL;

void setup_amqp_connection()
{
  int sockfd;
  connection = amqp_new_connection();

  die_on_error(sockfd = amqp_open_socket(rabbit_host, rabbit_port), "Opening RabbitMQ socket");

#if defined(__APPLE__) && defined(SO_NOSIGPIPE)
  // why doesn't librabbitmq do this, eh?
  int one = 1; /* for setsockopt */
  setsockopt(sockfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

  amqp_set_sockfd(connection, sockfd);

  die_on_amqp_error(amqp_login(connection, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
                    "Logging in to RabbitMQ");
  amqp_channel_open(connection, 1);
  die_on_amqp_error(amqp_get_rpc_reply(connection), "Opening AMQP channel");
}


void shutdown_amqp_connection()
{
  // close amqp connection
  if (amqp_rc >= 0) {
    log_amqp_error(amqp_channel_close(connection, 1, AMQP_REPLY_SUCCESS), "Closing AMQP channel");
    log_amqp_error(amqp_connection_close(connection, AMQP_REPLY_SUCCESS), "Closing AMQP connection");
  }
  log_error(amqp_destroy_connection(connection), "Ending AMQP connection");
}


int timer_event(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  static unsigned long last_received_count = 0;
  printf("processed %lu messages.\n", received_messages_count-last_received_count);
  last_received_count = received_messages_count;
  return 0;
}


int read_message_and_forward_to_amqp(zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
  int i = 0;
  zmq_msg_t message_parts[3];
  void *receiver = item->socket;

  // read the message parts
  while (!zctx_interrupted) {
    // printf("receiving part %d\n", i+1);
    if (i>2) {
      printf("Received more than 3 message parts\n");
      exit(1);
    }
    zmq_msg_init(&message_parts[i]);
#if ZMQ_VERSION >= 30200
    zmq_recvmsg(receiver, &message_parts[i], 0);
#else
    zmq_recv(receiver, &message_parts[i], 0);
#endif
    if (!zsocket_rcvmore(receiver))
      break;
    i++;
  }
  if (i<2) {
    if (!zctx_interrupted) {
      printf("Received only %d message parts\n", i);
    }
    goto cleanup;
  }
  received_messages_count++;

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
    amqp_exchange_declare(connection, 1, exchange_name, exchange_type, 0, 0, amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(connection), "Declaring AMQP exchange");
  }

  amqp_bytes_t routing_key;
  routing_key.len   = zmq_msg_size(&message_parts[1]);
  routing_key.bytes = zmq_msg_data(&message_parts[1]);

  amqp_bytes_t message_body;
  message_body.len   = zmq_msg_size(&message_parts[2]);
  message_body.bytes = zmq_msg_data(&message_parts[2]);

  //printf("publishing\n");

  // send message to rabbit
  amqp_rc = amqp_basic_publish(connection,
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


int main(int argc, char const * const *argv)
{
  int rc=0, amqp_rc=0;
  int my_port;

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

  setup_amqp_connection();

  // so far, no exchanges are known
  exchanges = zhash_new();

  // set up event loop
  zloop_t *loop = zloop_new();
  assert(loop);
  zloop_set_verbose(loop, 0);

  // calculate statistics every 1000 ms
  rc = zloop_timer(loop, 1000, 0, timer_event, &TIMER_ID);
  assert(rc == 0);

  // setup handler for the receiver socket
  zmq_pollitem_t item;
  item.socket = receiver;
  item.events =  ZMQ_POLLIN;
  rc = zloop_poller(loop, &item, read_message_and_forward_to_amqp, NULL);
  assert(rc == 0);

  rc = zloop_start(loop);
  // printf("zloop return: %d", rc);

  zloop_destroy (&loop);
  assert(loop == NULL);

  printf("received %lu messages", received_messages_count);

  // close zmq socket and context
  zsocket_destroy(context, receiver);
  zctx_destroy(&context);

  shutdown_amqp_connection();

  return (amqp_rc < 0);
}
