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
#include "zring.h"

int main()
{
    zring_test(1);

    return 0;
}
