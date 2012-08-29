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

int main(int argc, char const * const *argv)
{
  int message_count = 100000;
  int rc;

  zctx_t *context = zctx_new();
  assert(context);
  zctx_set_hwm(context, 1);
  zctx_set_linger(context, 100);

  void *socket = zsocket_new(context, ZMQ_PUSH);
  assert(socket);
  // zsocket_set_sndtimeo(socket, 5);
  // zsocket_set_hwm(socket, 1);

  rc = zsocket_connect(socket, "tcp://localhost:12345");
  assert(rc==0);

  char data[MESSAGE_BODY_SIZE];
  memset(data, 'a', MESSAGE_BODY_SIZE);

  char *exchanges[2] = {"zmq-bridge-tester-1", "zmq-bridge-tester-2"};

  int i = 0;
  for (i=0; i<message_count; i++) {
    if (zctx_interrupted)
      break;
    zmsg_t *message = zmsg_new();
    zmsg_addstr(message, exchanges[i%2]);
    zmsg_addstr(message, "logjam.zmq.test.%d", i);
    zmsg_addmem(message, data, MESSAGE_BODY_SIZE);
    zmsg_dump(message);
    rc = zmsg_send(&message, socket);
    assert(message == NULL);
    assert(rc == 0);
    zclock_sleep(100);
  }

  printf("published: %d\n", i);

  zsocket_destroy(context, socket);
  zctx_destroy(&context);

  return 0;
}
