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

int main(int argc, char const * const *argv)
{
  int rc;

  zctx_t *context = zctx_new();
  assert(context);
  zctx_set_rcvhwm(context, 1000);
  zctx_set_linger(context, 100);

  void *socket = zsocket_new(context, ZMQ_PULL);
  assert(socket);

  zsocket_set_rcvhwm(socket, 1000);
  zsocket_set_linger(socket, 500);
  zsocket_set_reconnect_ivl(socket, 100); // 100 ms
  zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

  rc = zsocket_connect(socket, "tcp://localhost:9651");
  assert(rc==0);

  zmsg_t *msg = NULL;

  size_t lost = 0;
  size_t last_num = 0;
  while (1) {
    msg = zmsg_recv(socket);
    if (zctx_interrupted)
      break;
    assert(msg);
    // assert(zmsg_size(msg) == 3);
    zmsg_dump(msg);
    zframe_t *last_frame = zmsg_last(msg);
    char *str = zframe_strdup(last_frame);
    size_t n = atol(str);
    free(str);
    if (n != last_num + 1 && last_num != 0) {
        lost += n - (last_num + 1);
    }
    last_num = n;
    zmsg_destroy(&msg);
  }

  zsocket_destroy(context, socket);
  zctx_destroy(&context);

  printf("lost messages: %zu\n", lost);
  return 0;
}
