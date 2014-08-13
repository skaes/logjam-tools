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
  zctx_set_rcvhwm(context, 1);
  zctx_set_linger(context, 100);

  void *socket = zsocket_new(context, ZMQ_SUB);
  assert(socket);

  zsocket_set_rcvhwm(socket, 100000);
  zsocket_set_linger(socket, 500);
  zsocket_set_reconnect_ivl(socket, 100); // 100 ms
  zsocket_set_reconnect_ivl_max(socket, 10 * 1000); // 10 s

  zsocket_set_subscribe(socket, "");

  const char* host = "localhost";
  if (argc>1) host = argv[1];

  rc = zsocket_connect(socket, "tcp://%s:9651", host);
  assert(rc==0);

  zmsg_t *msg = NULL;

  size_t received = 0;
  size_t lost = 0;
  size_t last_num = 0;

  while (1) {
    msg = zmsg_recv(socket);
    if (zctx_interrupted)
      break;
    assert(msg);
    received++;
    // assert(zmsg_size(msg) == 3);
    // zmsg_dump(msg);
    zframe_t *last_frame = zmsg_last(msg);
    char *str = zframe_strdup(last_frame);
    size_t n = atol(str);
    free(str);
    if (n > last_num + 1 && last_num != 0) {
        lost += n - (last_num + 1);
    }
    last_num = n;
    zmsg_destroy(&msg);
  }

  zsocket_destroy(context, socket);
  zctx_destroy(&context);

  printf("\nlost:     %7zu\nreceived: %7zu\n", lost, received);

  return 0;
}
