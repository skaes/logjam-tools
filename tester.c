#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>

#define MESSAGE_BODY_SIZE 4096

void assert_x(int rc, const char* error_text) {
  if (rc != 0) {
    printf("Failed assertion: %s\n", error_text);
    exit(1);
  }
}

struct _zmsg_t {
    zlist_t *frames;            //  List of frames
    size_t content_size;        //  Total content size
};

int
zmsg_send_dont_wait(zmsg_t **self_p, void *socket)
{
  assert (self_p);
  assert (socket);
  zmsg_t *self = *self_p;

  int rc = 0;
  if (self) {
    zframe_t *frame = (zframe_t *) zlist_pop(self->frames);
    while (frame) {
      rc = zframe_send(&frame, socket,
                       zlist_size(self->frames) ? ZFRAME_MORE|ZFRAME_DONTWAIT : ZFRAME_DONTWAIT);
      if (rc != 0)
        break;
      frame = (zframe_t *) zlist_pop(self->frames);
    }
    zmsg_destroy(self_p);
  }
  return rc;
}

int main(int argc, char const * const *argv)
{
  int message_count = argc>1 ? atoi(argv[1]) : 100000;
  int rc;

  zctx_t *context = zctx_new();
  assert(context);
  zctx_set_hwm(context, 1);
  zctx_set_linger(context, 100);

  void *socket = zsocket_new(context, ZMQ_PUSH);
  assert(socket);
  // zsocket_set_sndtimeo(socket, 10);
  zsocket_set_hwm(socket, 100);
  zsocket_set_linger(socket, 1000);
  zsocket_set_reconnect_ivl(socket, 100); // 100 ms
  zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s
  // printf("hwm %d\n", zsocket_hwm(socket));
  // printf("sndbuf %d\n", zsocket_sndbuf(socket));
  // printf("swap %d\n", zsocket_swap(socket));
  // printf("linger %d\n", zsocket_linger(socket));
  // printf("sndtimeo %d\n", zsocket_sndtimeo(socket));
  // printf("reconnect_ivl %d\n", zsocket_reconnect_ivl(socket));
  // printf("reconnect_ivl_max %d\n", zsocket_reconnect_ivl_max(socket));

  rc = zsocket_connect(socket, "tcp://localhost:12345");
  assert(rc==0);

  char data[MESSAGE_BODY_SIZE];
  memset(data, 'a', MESSAGE_BODY_SIZE);

  char *exchanges[2] = {"zmq-device-1", "zmq-device-2"};

  int i = 0, sent = 0, lost = 0;
  for (i=0; i<message_count; i++) {
    if (zctx_interrupted)
      break;
    zmsg_t *message = zmsg_new();
    zmsg_addstr(message, exchanges[i%2]);
    zmsg_addstr(message, "logjam.zmq.test.%d", i);
    zmsg_addmem(message, data, MESSAGE_BODY_SIZE);
    // zmsg_dump(message);
    rc = zmsg_send_dont_wait(&message, socket);
    assert(message == NULL);
    if (rc == 0) {
      sent++;
    } else {
      lost++;
    }
    usleep(20);
  }

  printf("sent: %d\n", sent);
  printf("lost: %d\n", lost);

  zsocket_destroy(context, socket);
  zctx_destroy(&context);

  return 0;
}
