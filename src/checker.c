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

bool verbose = false;

static void print_usage(char * const *argv)
{
    fprintf(stderr,
            "usage: %s [options]\n"
            "Options:\n"
            "  -v, --verbose              log more\n"
            "      --help                 display this message\n"
            , argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    int longindex = 0;
    opterr = 0;

    static struct option long_options[] = {
        { "help",          no_argument,       0,  0  },
        { "verbose",       no_argument,       0, 'v' },
        { 0,               0,                 0,  0  }
    };

    while ((c = getopt_long(argc, argv, "v", long_options, &longindex)) != -1) {
        switch (c) {
        case 'v':
            verbose = 1;
            break;
        case 0:
            print_usage(argv);
            exit(0);
            break;
        case '?':
            if (strchr("", optopt))
                fprintf(stderr, "[E] option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "[E] unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "[E] unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            fprintf(stderr, "BUG: can't process option -%c\n", optopt);
            exit(1);
        }
    }
}

int main(int argc, char * const *argv)
{
    process_arguments(argc, argv);
    zring_test(verbose);
    logjam_util_test(verbose);
    return 0;
}
