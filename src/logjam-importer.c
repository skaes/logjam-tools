#include "importer-controller.h"
#include "importer-streaminfo.h"
#include "importer-resources.h"
#include "importer-mongoutils.h"
#include "importer-processor.h"

static char *subscription_pattern = "";
static const char *config_file_name = "logjam.conf";

FILE* frontend_timings = NULL;
static char *frontend_timings_file_name = NULL;
static char *frontend_timings_apdex_attr = NULL;

void print_usage(char * const *argv)
{
    fprintf(stderr, "usage: %s [-n] [-v] [-q] [-i io-threads] [-p num-parsers] [-u num-updaters] [-w num_writers] [-s stream-pattern] [-c config-file] [-f frontend-timings-log-file] [-a frontend-apdex-attribute]\n", argv[0]);
}

static char* num_parsers_arg_value = NULL;
static char* num_updaters_arg_value = NULL;
static char* num_writers_arg_value = NULL;
static size_t io_threads = 0;

static void setup_thread_counts(zconfig_t* config)
{
    if (!num_parsers_arg_value)
        num_parsers_arg_value = zconfig_resolve(config, "frontend/threads/parsers", NULL);
    if (num_parsers_arg_value)
        num_parsers = strtoul(num_parsers_arg_value, NULL, 0);

    if (!num_updaters_arg_value)
        num_updaters_arg_value = zconfig_resolve(config, "frontend/threads/updaters", NULL);
    if (num_updaters_arg_value)
        num_updaters = strtoul(num_updaters_arg_value, NULL, 0);

    if (!num_writers_arg_value)
        num_writers_arg_value = zconfig_resolve(config, "frontend/threads/writers", NULL);
    if (num_writers_arg_value)
        num_writers = strtoul(num_writers_arg_value, NULL, 0);
}

void process_arguments(int argc, char * const *argv)
{
    char c;
    opterr = 0;
    while ((c = getopt(argc, argv, "a:c:f:np:qs:u:vw:i:")) != -1) {
        switch (c) {
        case 'n':
            dryrun = true;
            break;
        case 'v':
            if (verbose)
                debug = true;
            else
                verbose = true;
            break;
        case 'q':
            quiet = true;
            break;
        case 'c':
            config_file_name = optarg;
            break;
        case 'f':
            frontend_timings_file_name = optarg;
            break;
        case 's':
            subscription_pattern = optarg;
            break;
        case 'a':
            frontend_timings_apdex_attr = optarg;
            break;
        case 'p': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_PARSERS)
                num_parsers_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'p' cannot be larger than %d\n", MAX_PARSERS);
                exit(1);
            }
            break;
        }
        case 'u': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_UPDATERS)
                num_updaters_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'u' cannot be larger than %d\n", MAX_UPDATERS);
                exit(1);
            }
            break;
        }
        case 'w': {
            unsigned long n = strtoul(optarg, NULL, 0);
            if (n <= MAX_UPDATERS)
                num_writers_arg_value = strdup(optarg);
            else {
                fprintf(stderr, "[E] parameter value 'w' cannot be larger than %d\n", MAX_UPDATERS);
                exit(1);
            }
            break;
        }
        case 'i':
            io_threads = atoi(optarg);
            break;
        case '?':
            if (optopt == 'a' || optopt == 'c' || optopt == 'f' || optopt == 'p' || optopt == 's' || optopt == 'u' || optopt == 'w' || optopt == 'i')
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
    // don't buffer stdout and stderr
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    process_arguments(argc, argv);

    // setup frontend debug logging if requested
    if (frontend_timings_file_name) {
        frontend_timings = fopen(frontend_timings_file_name, "a");
        if (!frontend_timings) {
            fprintf(stderr, "[E] could not open frontend timings logfile: %s\n", strerror(errno));
            exit(1);
        }
    }
    if (frontend_timings_apdex_attr) {
        if (!processor_set_frontend_apdex_attribute(frontend_timings_apdex_attr)) {
            fprintf(stderr, "[E] invalid frontend apdex attribite name: %s\n", frontend_timings_apdex_attr);
            exit(1);
        }
    }

    // verify config file exists
    if (!zsys_file_exists(config_file_name)) {
        fprintf(stderr, "[E] missing config file: %s\n", config_file_name);
        exit(1);
    }
    config_file_init(config_file_name);

    config_update_date_info();

    // load config
    zconfig_t* config = zconfig_load((char*)config_file_name);
    // zconfig_print(config);

    initialize_mongo_db_globals(config);

    setup_resource_maps(config);
    setup_stream_config(config, subscription_pattern);
    setup_thread_counts(config);

    return run_controller_loop(config, io_threads);
}
