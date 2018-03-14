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
  int socket_count  = argc>1 ? atoi(argv[1]) : 1;
  int message_count = argc>2 ? atoi(argv[2]) : 100000;
  int rc;

  zsys_set_sndhwm(1);
  zsys_set_linger(100);

  // zmq 2.2 only supports up to 512 sockets
  assert_x(ZMQ_VERSION <= 20200 && socket_count > 512,
           "zeromq < 3.2 only supports up to 512 sockets");

  void **sockets = zmalloc(socket_count * sizeof(void*));

  int j;
  for (j=0; j<socket_count; j++) {
    void *socket = zsock_new(ZMQ_PUSH);
    if (NULL==socket) {
      printf("Error occurred during %dth call to zsock_new: %s\n", j, zmq_strerror (errno));
      exit(1);
    }
    // zsock_set_sndtimeo(socket, 10);
    zsock_set_sndhwm(socket, 10);
    zsock_set_linger(socket, 50);
    zsock_set_reconnect_ivl(socket, 100); // 100 ms
    zsock_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s
    // printf("hwm %d\n", zsock_hwm(socket));
    // printf("sndbuf %d\n", zsock_sndbuf(socket));
    // printf("swap %d\n", zsock_swap(socket));
    // printf("linger %d\n", zsock_linger(socket));
    // printf("sndtimeo %d\n", zsock_sndtimeo(socket));
    // printf("reconnect_ivl %d\n", zsock_reconnect_ivl(socket));
    // printf("reconnect_ivl_max %d\n", zsock_reconnect_ivl_max(socket));

    rc = zsock_connect(socket, "tcp://localhost:12345");
    assert(rc==0);
    sockets[j] = socket;
    zclock_sleep(10); // sleep 10 ms
  }

  char data[MESSAGE_BODY_SIZE];
  memset(data, 'a', MESSAGE_BODY_SIZE);

  char *exchanges[2] = {"zmq-device-1", "zmq-device-2"};
  // char *keys[2] = {"1","2"};

  int i = 0, queued = 0, rejected = 0;
  for (i=0; i<message_count; i++) {
    if (zsys_interrupted)
      break;
    zmsg_t *message = zmsg_new();
    zmsg_addstr(message, exchanges[i%2]);
    zmsg_addstrf(message, "logjam.zmq.test.%d.%d", i%2, i);
    zmsg_addmem(message, data, MESSAGE_BODY_SIZE);
    // zmsg_dump(message);
    rc = zmsg_send_dont_wait(&message, sockets[i%socket_count]);
    assert(message == NULL);
    if (rc == 0) {
      queued++;
    } else {
      rejected++;
    }
    // usleep(20);
    usleep(1000);
  }

  printf("queued:   %8d\n", queued);
  printf("rejected: %8d\n", rejected);

  for (i=0;i<socket_count;i++) {
    zsock_destroy(sockets[i]);
  }
  free(sockets);

  return 0;
}
