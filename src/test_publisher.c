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

int timer_event(zloop_t *loop, int timer_id, void *pusher)
{
  int rc;
  static int message_count = 0;
  zmsg_t *message = zmsg_new();
  zmsg_addstr(message, "request-stream-test-development");
  zmsg_addstr(message, "routing-key");
  zmsg_addstrf(message, "message-body %d", message_count++);
  rc = zmsg_send(&message, pusher);
  if (!zsys_interrupted) {
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

  zsys_set_sndhwm(1);
  zsys_set_linger(100);

  void *pusher = zsock_new(ZMQ_PUSH);
  assert(pusher);
  zsock_set_sndhwm(pusher, 1000);
  zsock_set_linger(pusher, 500);
  rc = zsock_connect(pusher, "tcp://localhost:12345");
  assert(rc==0);

  void *puller = zsock_new(ZMQ_PULL);
  assert(puller);
  zsock_set_rcvhwm(puller, 1000);
  zsock_set_linger(puller, 500);
  rc = zsock_bind(puller, "tcp://*:12345");
  if (rc != 12345){
    printf("bind failed: %s\n", zmq_strerror(errno));
  }
  assert(rc == 12345);

  void *publisher = zsock_new(ZMQ_PUB);
  assert(publisher);
  zsock_set_sndhwm(publisher, 1000);
  zsock_set_linger(publisher, 500);
  rc = zsock_bind(publisher, "tcp://*:12346");
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
  printf("zloop return: %d", rc);

  zloop_destroy(&loop);
  assert(loop == NULL);

  return 0;
}
