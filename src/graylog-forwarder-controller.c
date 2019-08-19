#include "graylog-forwarder-controller.h"
#include "graylog-forwarder-subscriber.h"
#include "graylog-forwarder-parser.h"
#include "graylog-forwarder-writer.h"
#include "logjam-streaminfo.h"

/*
 *                 --- PIPE ---  subscriber
 *  controller:    --- PIPE ---  parsers(NUM_PARSERS)
 *                 --- PIPE ---  writer
*/

// The controller creates all other threads/actors.

unsigned int num_parsers = 8;

typedef struct {
    zconfig_t *config;
    zactor_t *subscriber;
    zactor_t *parsers[MAX_PARSERS];
    zactor_t *writer;
    zactor_t *stream_config_updater;
} controller_state_t;


static
bool controller_create_actors(controller_state_t *state, zlist_t* devices, int rcv_hwm, int send_hwm)
{

    // start the stream config updater
    state->stream_config_updater = zactor_new(stream_config_updater, NULL);

    // create subscriber
    state->subscriber = graylog_forwarder_subscriber_new(state->config, devices, rcv_hwm, send_hwm);

    // create the parsers
    for (size_t i=0; i<num_parsers; i++) {
        state->parsers[i] = graylog_forwarder_parser_new(state->config, i);
    }

    // create writer
    state->writer = zactor_new(graylog_forwarder_writer, state->config);

    return !zsys_interrupted;
}

static
void controller_destroy_actors(controller_state_t *state)
{
    zactor_destroy(&state->stream_config_updater);
    zactor_destroy(&state->subscriber);
    zactor_destroy(&state->writer);
    for (size_t i=0; i<num_parsers; i++) {
        graylog_forwarder_parser_destroy(&state->parsers[i]);
    }
}

static
int send_tick_commands(zloop_t *loop, int timer_id, void *arg)
{
    controller_state_t *state = arg;

    // send tick commands to actors to let them print out their stats
    zstr_send(state->subscriber, "tick");
    zstr_send(state->writer, "tick");

    int rc = zloop_timer(loop, 1000, 1, send_tick_commands, state);
    assert(rc != -1);

    return 0;
}

int graylog_forwarder_run_controller_loop(zconfig_t* config, zlist_t* devices, const char *subscription_pattern, const char* logjam_url, int rcv_hwm, int send_hwm)
{
    set_thread_name("controller");

    zsys_init();

    int rc = 0;
    if (!setup_stream_config(logjam_url, subscription_pattern)) {
        rc = 1;
        goto shutdown;
    }

    controller_state_t state = {.config = config};
    bool start_up_complete = controller_create_actors(&state, devices, rcv_hwm, send_hwm);

    if (!start_up_complete) {
        rc = 1;
        goto exit;
    }

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // send tick commands every second
    rc = zloop_timer(loop, 1000, 1, send_tick_commands, &state);
    assert(rc != -1);
    rc = 0;

    // run the loop
    // when running under the google profiler, zmq_poll terminates with EINTR
    // so we keep the loop running in this case
    if (!zsys_interrupted) {
        bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
        do {
            rc = zloop_start(loop);
            should_continue_to_run &= errno == EINTR && !zsys_interrupted;
            log_zmq_error(rc, __FILE__, __LINE__);
        } while (should_continue_to_run);
    }
    printf("[I] controller: shutting down\n");

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);

 exit:
    printf("[I] controller: destroying actor threads\n");
    controller_destroy_actors(&state);
 shutdown:
    printf("[I] controller: calling zsys_shutdown\n");
    zsys_shutdown();

    printf("[I] controller: terminated\n");
    return rc;
}
