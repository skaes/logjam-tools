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

int main(int argc, char const * const *argv)
{
  int rc;

  zctx_t *context = zctx_new();
  assert(context);
  zctx_set_hwm(context, 1);
  zctx_set_linger(context, 100);

  void *socket = zsocket_new(context, ZMQ_PUB);
  assert(socket);

  zsocket_set_hwm(socket, 1000);
  zsocket_set_linger(socket, 500);
  zsocket_set_reconnect_ivl(socket, 100); // 100 ms
  zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

  rc = zsocket_bind(socket, "tcp://*:12346");
  assert(rc==12346);

  zmsg_t *msg = NULL;

  while (!zctx_interrupted) {
    zmsg_t *message = zmsg_new();
    zmsg_addstr(message, "testtesttest");
    zmsg_addstr(message, "data");
    rc = zmsg_send(&message, socket);
    if (!zctx_interrupted) {
      assert(rc==0);
    }
  }

  zsocket_destroy(context, socket);
  zctx_destroy(&context);

  return 0;
}
