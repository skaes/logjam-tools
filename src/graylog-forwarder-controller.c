#include "graylog-forwarder-controller.h"
#include "graylog-forwarder-subscriber.h"
#include "graylog-forwarder-parser.h"
#include "graylog-forwarder-writer.h"
#include "graylog-forwarder-prometheus-client.h"
#include "logjam-streaminfo.h"
#include "importer-watchdog.h"

/*
 *                 --- PIPE ---  subscriber
 *  controller:    --- PIPE ---  parsers(NUM_PARSERS)
 *                 --- PIPE ---  writer
 *                 --- PIPE ---  watchdog
 */

// The controller creates all other threads/actors.

unsigned int num_parsers = 8;

typedef struct {
    zconfig_t *config;
    zactor_t *subscriber;
    zactor_t *parsers[MAX_PARSERS];
    zactor_t *writer;
    zactor_t *stream_config_updater;
    zactor_t *watchdog;
} controller_state_t;


static
bool controller_create_actors(controller_state_t *state, zlist_t* devices, int rcv_hwm, int send_hwm, int heartbeat_abort_after)
{

    // start the stream config updater
    state->stream_config_updater = stream_config_updater_new(NULL);

    // create subscriber
    state->subscriber = graylog_forwarder_subscriber_new(state->config, devices, rcv_hwm, send_hwm);

    // create the parsers
    for (size_t i=0; i<num_parsers; i++) {
        state->parsers[i] = graylog_forwarder_parser_new(state->config, i);
    }

    // create writer
    state->writer = zactor_new(graylog_forwarder_writer, state->config);

    // create watchdog
    state->watchdog = watchdog_new(heartbeat_abort_after, HEART_BEAT_INTERVAL, 0);

    return !zsys_interrupted;
}

static
void controller_destroy_actors(controller_state_t *state)
{
    zactor_destroy(&state->subscriber);
    zactor_destroy(&state->writer);
    for (size_t i=0; i<num_parsers; i++) {
        graylog_forwarder_parser_destroy(&state->parsers[i]);
    }
    stream_config_updater_destroy(&state->stream_config_updater);
    watchdog_destroy(&state->watchdog);
}

static
int send_tick_commands(zloop_t *loop, int timer_id, void *arg)
{
    controller_state_t *state = arg;

    // clean up old prometheus counters every 7 days
    graylog_forwarder_prometheus_client_delete_old_stream_counters(1000/*ms*/ * 3600/*seconds*/ * 24/*hours*/ * 7/*days*/);

    // send tick commands to actors to let them print out their stats
    zstr_send(state->writer, "tick");
    zstr_send(state->subscriber, "tick");
    for (size_t i=0; i<num_parsers; i++) {
        zstr_send(state->parsers[i], "tick");
    }

    // get number of messages received by subscriber
    size_t messages_received = 0;
    zmsg_t *response = zmsg_recv(state->subscriber);
    if (response) {
        zframe_t *frame = zmsg_first(response);
        messages_received = zframe_getsize(frame);
        zmsg_destroy(&response);
    }
    if (messages_received > 0)
        zstr_send(state->watchdog, "tick");

    int rc = zloop_timer(loop, 1000, 1, send_tick_commands, state);
    assert(rc != -1);

#ifdef HAVE_MALLOC_TRIM
    static size_t ticks = 0;
    // try to reduce memory usage. unclear whether this helps at all.
    if (malloc_trim_frequency > 0 && ++ticks % malloc_trim_frequency == 0 && !zsys_interrupted)
         malloc_trim(0);
#endif

    return 0;
}

int graylog_forwarder_run_controller_loop(zconfig_t* config, zlist_t* devices, const char *subscription_pattern, const char* logjam_url, int rcv_hwm, int send_hwm, int heartbeart_abort_ticks)
{
    set_thread_name("controller");

    zsys_init();

    int rc = 0;
    if (!setup_stream_config(logjam_url, subscription_pattern)) {
        rc = 1;
        goto shutdown;
    }

    controller_state_t state = {.config = config};
    bool start_up_complete = controller_create_actors(&state, devices, rcv_hwm, send_hwm, heartbeart_abort_ticks);

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
