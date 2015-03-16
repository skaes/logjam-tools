#include <zmq.h>
#include <czmq.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdint.h>
#include <getopt.h>
#include "graylog-forwarder-common.h"
#include "graylog-forwarder-controller.h"

// global config
static zconfig_t* config = NULL;
static zfile_t *config_file = NULL;
static char *config_file_name = "logjam.conf";
static time_t config_file_last_modified = 0;
static char *config_file_digest = "";

static void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-n] [-z] [-c config-file]\n", argv[0]);
}

static void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "c:nz")) != -1) {
        switch (c) {
        case 'c':
            config_file_name = optarg;
            break;
        case 'n':
            dryrun = true;
            break;
        case 'z':
            compress_gelf = true;
            break;
        case '?':
            if (optopt == 'c' )
                fprintf(stderr, "option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            exit(1);
        }
    }
}

static void config_file_init()
{
    config_file = zfile_new(NULL, config_file_name);
    config_file_last_modified = zfile_modified(config_file);
    config_file_digest = strdup(zfile_digest(config_file));
}

int main(int argc, char * const *argv)
{
    process_arguments(argc, argv);

    if (!zsys_file_exists(config_file_name)) {
        fprintf(stderr, "[E] missing config file: %s\n", config_file_name);
        exit(1);
    }

    // load config
    if (zsys_file_exists(config_file_name)) {
        config_file_init();
        config = zconfig_load((char*)config_file_name);
    }

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    return graylog_forwarder_run_controller_loop(config);
}
