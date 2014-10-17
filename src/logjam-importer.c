#include "importer-controller.h"
#include "importer-streaminfo.h"
#include "importer-resources.h"
#include "importer-mongoutils.h"

static char *subscription_pattern = "";
static const char *config_file_name = "logjam.conf";

void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-n] [-p stream-pattern] [-c config-file]\n", argv[0]);
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "nc:p:")) != -1) {
        switch (c) {
        case 'n':
            dryrun = true;;
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'p':
            subscription_pattern = optarg;
            break;
        case '?':
            if (optopt == 'c')
                fprintf(stderr, "[E] option -%c requires an argument.\n", optopt);
            else if (isprint (optopt))
                fprintf(stderr, "[E] unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "[E] unknown option character `\\x%x'.\n", optopt);
            print_usage(argv);
            exit(1);
        default:
            exit(1);
        }
    }
}

int main(int argc, char * const *argv)
{
    process_arguments(argc, argv);

    if (!zsys_file_exists(config_file_name)) {
        fprintf(stderr, "[E] missing config file: %s\n", config_file_name);
        exit(1);
    } else {
        config_file_init(config_file_name);
    }

    config_update_date_info();

    // load config
    zconfig_t* config = zconfig_load((char*)config_file_name);
    // zconfig_print(config);

    initialize_mongo_db_globals(config);

    setup_resource_maps(config);
    setup_stream_config(config, subscription_pattern);

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    return run_controller_loop(config);
}
