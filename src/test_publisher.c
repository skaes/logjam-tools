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
    printf("rc: %d, errno: %d(%s)\n", rc, errno, zmq_strerror(errno));
  }
}

int timer_event(zloop_t *loop, int timer_id, void *pusher)
{
  int rc;
  static int message_count = 0;
  zmsg_t *message = zmsg_new();
  zmsg_addstr(message, "request-stream-test-development");
  zmsg_addstr(message, "routing-key");
  zmsg_addstrf(message, "message-body %d", message_count++);
  rc = zmsg_send(&message, pusher);
  if (!zctx_interrupted) {
    assert(rc==0);
  }
  return 0;
}

int forward(zloop_t *loop, zmq_pollitem_t *item, void *publisher)
{
  int rc;
  zmsg_t *message = zmsg_recv(item->socket);

  rc = zmsg_send(&message, publisher);
  assert(rc == 0);
  return 0;
}


int main(int argc, char const * const *argv)
{
  int rc;

  zctx_t *context = zctx_new();
  assert(context);
  zctx_set_sndhwm(context, 1);
  zctx_set_linger(context, 100);

  void *pusher = zsocket_new(context, ZMQ_PUSH);
  assert(pusher);
  zsocket_set_sndhwm(pusher, 1000);
  zsocket_set_linger(pusher, 500);
  rc = zsocket_connect(pusher, "tcp://localhost:12345");
  assert(rc==0);

  void *puller = zsocket_new(context, ZMQ_PULL);
  assert(puller);
  zsocket_set_rcvhwm(puller, 1000);
  zsocket_set_linger(puller, 500);
  rc = zsocket_bind(puller, "tcp://*:12345");
  if (rc != 12345){
    printf("bind failed: %s\n", zmq_strerror(errno));
  }
  assert(rc == 12345);

  void *publisher = zsocket_new(context, ZMQ_PUB);
  assert(publisher);
  zsocket_set_sndhwm(publisher, 1000);
  zsocket_set_linger(publisher, 500);
  rc = zsocket_bind(publisher, "tcp://*:12346");
  assert(rc==12346);

  // set up event loop
  zloop_t *loop = zloop_new();
  assert(loop);
  zloop_set_verbose(loop, 0);

  // push data every 10 ms
  rc = zloop_timer(loop, 1, 0, timer_event, pusher);
  assert(rc != -1);

  zmq_pollitem_t item;
  item.socket = puller;
  item.events = ZMQ_POLLIN;
  rc = zloop_poller(loop, &item, forward, publisher);
  assert(rc == 0);

  rc = zloop_start(loop);
  // printf("zloop return: %d", rc);

  zloop_destroy(&loop);
  assert(loop == NULL);

  // cleanup
  zctx_destroy(&context);

  return 0;
}
