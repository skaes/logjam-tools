#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <getopt.h>
#include "logjam-util.h"

int main() {
    int rc;

    void *ctx = zmq_ctx_new ();
    assert (ctx);
    zmq_ctx_set (ctx, ZMQ_IO_THREADS, 1);

    // create ZMQ_STREAM socket
    void *socket = zmq_socket (ctx, ZMQ_STREAM);
    assert (socket);

    // set some options
    int hwm = 100000;
    rc = zmq_setsockopt (socket, ZMQ_SNDHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    rc = zmq_setsockopt (socket, ZMQ_RCVHWM, &hwm, sizeof (hwm));
    assert (rc == 0);
    // bind
    rc = zmq_bind (socket, "tcp://*:9000");
    assert (rc == 0);

    /* Data structure to hold the ZMQ_STREAM ID */
    uint8_t id [256];
    size_t id_size = 256;

    /* Data structure to hold the ZMQ_STREAM received data */
    uint8_t raw [256];
    size_t raw_size = 256;
    while (1) {
        /* Get HTTP request; ID frame and then request */
        id_size = zmq_recv (socket, id, 256, 0);
        assert (id_size > 0);
        do {
            raw_size = zmq_recv (socket, raw, 256, 0);
            assert (raw_size >= 0);
        } while (raw_size == 256);

        /* Prepares the response */
        char http_response [] =
            "HTTP/1.0 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 0\r\n"
            "\r\n"
            "Hello, World!";

        /* Sends the ID frame followed by the response */
        zmq_send (socket, id, id_size, ZMQ_SNDMORE);
        zmq_send (socket, http_response, strlen (http_response), ZMQ_SNDMORE);

        /* Closes the connection by sending the ID frame followed by a zero response */
        zmq_send (socket, id, id_size, ZMQ_SNDMORE);
        zmq_send (socket, 0, 0, ZMQ_SNDMORE);

        /* NOTE: If we don't use ZMQ_SNDMORE, then we won't be able to send more */
        /* message to any client */
    }
    zmq_close (socket); zmq_ctx_destroy (ctx);
}
