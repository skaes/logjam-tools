#include "importer-controller.h"
#include "importer-increments.h"
#include "importer-parser.h"
#include "importer-adder.h"
#include "importer-processor.h"
#include "importer-livestream.h"
#include "importer-statsupdater.h"
#include "importer-requestwriter.h"
#include "importer-indexer.h"
#include "importer-subscriber.h"
#include "importer-watchdog.h"
#include "statsd-client.h"

/*
 * connections: n_w = num_writers, n_p = num_parsers, n_u= num_updaters, n_a = num_adders "[<>^v]" = connect, "o" = bind
 *
 *                 --- PIPE ---  indexer
 *                 --- PIPE ---  subscriber
 *                 --- PIPE ---  parsers(n_p)
 *                 --- PIPE ---  adders(n_s)
 *  controller:    --- PIPE ---  writers(n_w)
 *                 --- PIPE ---  updaters(n_u)
 *                 --- PIPE ---  tracker
 *                 --- PIPE ---  watchdog
 *                 --- PIPE ---  live stream publisher
 *
 *                 PUSH    PULL
 *                 o----------<  updaters(n_u)
 *
 *                 DEALER   REP
 *                 o----------<  adders(n_a)
 *
 *                 PUSH    PULL
 *                 >----------o  live stream publisher
*/

// The controller creates all other threads, collects data from the parsers every second,
// combines the data, sends db update requests to the updaters and also feeds the live stream.
// The data from the parsers is collected using the pipes, but maybe we should have an
// independent socket for this. The controller send ticks to the watchdog, which aborts
// the whole process if it doesn't receive ticks for ten consecutive seconds.

unsigned long num_parsers = 8;
unsigned long num_writers = 10;
unsigned long num_updaters = 10;
unsigned long num_adders = 4;

typedef struct {
    zconfig_t *config;
    zactor_t *statsd_server;
    zactor_t *subscriber;
    zactor_t *indexer;
    zactor_t *tracker;
    zactor_t *watchdog;
    zactor_t *parsers[MAX_PARSERS];
    zactor_t *adders[MAX_ADDERS];
    zactor_t *writers[MAX_WRITERS];
    zactor_t *updaters[MAX_UPDATERS];
    zactor_t *live_stream_publisher;
    zsock_t *updates_socket;
    size_t updates_blocked;
    zsock_t *adder_socket;
    zsock_t *live_stream_socket;
    size_t ticks;
} controller_state_t;


static
void extract_parser_state(zmsg_t* msg, zhash_t **processors, size_t *parsed_msgs_count, frontend_stats_t *fe_stats)
{
    zframe_t *first = zmsg_first(msg);
    zframe_t *second = zmsg_next(msg);
    zframe_t *third = zmsg_next(msg);
    assert(zframe_size(first) == sizeof(zhash_t*));
    memcpy(&*processors, zframe_data(first), sizeof(zhash_t*));
    assert(zframe_size(second) == sizeof(size_t));
    memcpy(parsed_msgs_count, zframe_data(second), sizeof(size_t));
    assert(zframe_size(third) == sizeof(frontend_stats_t));
    memcpy(fe_stats, zframe_data(third), sizeof(frontend_stats_t));
}


static
void publish_totals(stream_info_t *stream_info, zhash_t *totals, zsock_t *live_stream_socket)
{
    size_t n = stream_info->app_len + 1 + stream_info->env_len;
    zhash_t *known_modules = stream_info->known_modules;
    void *value = zhash_first(known_modules);
    while (value) {
        const char *module = zhash_cursor(known_modules);
        const char *namespace = module;
        // skip :: at the beginning of module
        while (*module == ':') module++;
        size_t m = strlen(module);
        char key[n + m + 3];
        sprintf(key, "%s-%s,%s", stream_info->app, stream_info->env, module);
        // TODO: change this crap in the live stream publisher
        // tolower is unsafe and not really necessary
        for (char *p = key; *p; ++p) *p = tolower(*p);

        // printf("[D] publishing totals for module: %s, key: %s\n", module, key);
        json_object *json = json_object_new_object();
        increments_t *incs = totals ? zhash_lookup(totals, namespace) : NULL;
        if (incs) {
            json_object_object_add(json, "count", json_object_new_int(incs->backend_request_count));
            json_object_object_add(json, "page_count", json_object_new_int(incs->page_request_count));
            json_object_object_add(json, "ajax_count", json_object_new_int(incs->ajax_request_count));
            increments_add_metrics_to_json(incs, json);

        } else {
            json_object_object_add(json, "count", json_object_new_int(0));
            json_object_object_add(json, "page_count", json_object_new_int(0));
            json_object_object_add(json, "ajax_count", json_object_new_int(0));
        }
        const char* json_str = json_object_to_json_string_ext(json, JSON_C_TO_STRING_PLAIN);
        live_stream_publish(live_stream_socket, key, json_str);
        json_object_put(json);

        value = zhash_next(known_modules);
    }
}

static
void publish_totals_for_every_known_stream(controller_state_t *state, zhash_t *processors)
{
    zhash_t *published_streams= zhash_new();

    // publish updates for all streams where we received some data
    processor_state_t* processor = zhash_first(processors);
    while (processor) {
        stream_info_t *stream_info = processor->stream_info;
        update_known_modules(stream_info, processor->modules);
        zhash_insert(published_streams, stream_info->key, (void*)1);
        publish_totals(stream_info, processor->totals, state->live_stream_socket);
        processor = zhash_next(processors);
    }

    // publish updates for all streams where we didn't receive anything
    stream_info_t *stream_info = zhash_first(configured_streams);
    while (stream_info) {
        if (!zhash_lookup(published_streams, stream_info->key)) {
            publish_totals(stream_info, NULL, state->live_stream_socket);
        }
        stream_info = zhash_next(configured_streams);
    }

    zhash_destroy(&published_streams);
}

static
int collect_stats_and_forward(zloop_t *loop, int timer_id, void *arg)
{
    int64_t start_time_ms = zclock_mono();
    controller_state_t *state = arg;
    zhash_t *processors[num_parsers];
    size_t parsed_msgs_counts[num_parsers];
    frontend_stats_t fe_stats[num_parsers];

    state->ticks++;

    // tell tracker, subscriber, live stream publisher and  stats server to tick
    zstr_send(state->statsd_server, "tick");
    zstr_send(state->subscriber, "tick");
    zstr_send(state->tracker, "tick");
    zstr_send(state->live_stream_publisher, "tick");

    // printf("[D] controller: collecting data from parsers: tick[%zu]\n", state->ticks);
    for (size_t i=0; i<num_parsers; i++) {
        zactor_t* parser = state->parsers[i];
        zstr_send(parser, "tick");
        zmsg_t *response = zmsg_recv(parser);
        if (response) {
            extract_parser_state(response, &processors[i], &parsed_msgs_counts[i], &fe_stats[i]);
            zmsg_destroy(&response);
        }
    }

    zlist_t *additions = zlist_new();
    zlist_append(additions, processors[0]);

    // printf("[D] controller: combining processors states\n");
    size_t parsed_msgs_count = parsed_msgs_counts[0];
    frontend_stats_t front_stats = fe_stats[0];
    for (int i=1; i<num_parsers; i++) {
        parsed_msgs_count += parsed_msgs_counts[i];
        front_stats.received += fe_stats[i].received;
        front_stats.dropped += fe_stats[i].dropped;
        for (int j=0; j<FE_MSG_NUM_REASONS; j++)
            front_stats.drop_reasons[j] += fe_stats[i].drop_reasons[j];
        zlist_append(additions, processors[i]);
    }

    // merge processors
    int l = zlist_size(additions);
    while (l>1) {
        int n = l / 2;
        for (int i = 0; i < n; i++ ) {
            zhash_t *p1 = zlist_pop(additions);
            zhash_t *p2 = zlist_pop(additions);
            zmsg_t *request = zmsg_new();
            // empty envelope REP socket
            zmsg_addstr(request, "");
            zmsg_addptr(request, p1);
            zmsg_addptr(request, p2);
            int rc = zmsg_send_with_retry(&request, state->adder_socket);
            assert(rc==0);
        }
        for (int i = 0; i < n; i++ ) {
            zmsg_t *reply = zmsg_recv_with_retry(state->adder_socket);
            assert(reply);
            // discard empty reply envelope
            char *empty = zmsg_popstr(reply);
            if (empty) {
                assert( streq(empty, "") );
                free(empty);
            }
            zhash_t *p = zmsg_popptr(reply);
            zlist_append(additions, p);
            zmsg_destroy(&reply);
        }
        l = zlist_size(additions);
    }

    processors[0] = zlist_pop(additions);
    zlist_destroy(&additions);

    // publish on live stream (need to do this while we still own the processors)
    // printf("[D] controller: publishing live streams\n");
    publish_totals_for_every_known_stream(state, processors[0]);

    // tell indexer to tick
    zstr_send(state->indexer, "tick");

    // tell stats updaters to tick
    for (int i=0; i<num_updaters; i++) {
        zstr_send(state->updaters[i], "tick");
    }

    // forward to stats_updaters
    // printf("[D] controller: forwarding updates\n");
    zlist_t *db_names = zhash_keys(processors[0]);
    const char* db_name = zlist_first(db_names);
    while (db_name != NULL) {
        processor_state_t *proc = zhash_lookup(processors[0], db_name);
        // printf("[D] forwarding %s\n", db_name);
        zmsg_t *stats_msg;

        // send totals updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "t");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->totals);
        proc->totals = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send_and_destroy(&stats_msg, state->updates_socket);

        // send minutes updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "m");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->minutes);
        proc->minutes = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send_and_destroy(&stats_msg, state->updates_socket);

        // send quants updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "q");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->quants);
        proc->quants = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send_and_destroy(&stats_msg, state->updates_socket);

        // send agents updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "a");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->agents);
        proc->agents = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send_and_destroy(&stats_msg, state->updates_socket);

        db_name = zlist_next(db_names);
    }
    zlist_destroy(&db_names);
    zhash_destroy(&processors[0]);

    // tell request writers to tick
    for (int i=0; i<num_writers; i++) {
        zstr_send(state->writers[i], "tick");
    }

    bool terminate = (state->ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    int64_t end_time_ms = zclock_mono();
    int runtime = end_time_ms - start_time_ms;
    int next_tick = runtime > 999 ? 1 : 1000 - runtime;
    double received_percent = parsed_msgs_count == 0 ? 0 : ((double) front_stats.received / parsed_msgs_count) * 100;
    double dropped_percent  = front_stats.received == 0 ? 0 : ((double) front_stats.dropped / front_stats.received) * 100;
    printf("[I] controller: %5zu messages (%3d ms); frontend: %3zu [%4.1f%%] (dropped: %2zu [%4.1f%%])\n",
           parsed_msgs_count, runtime,
           front_stats.received, received_percent,
           front_stats.dropped, dropped_percent) ;

    // log a warning about the number of blocked updates
    if (state->updates_blocked) {
        fprintf(stderr, "[W] controller: updates blocked: %zu\n", state->updates_blocked);
        state->updates_blocked = 0;
    }

    // signal liveness to watchdog, unless we're dropping all frontend requests
    if (front_stats.received == 0 || front_stats.dropped < front_stats.received) {
        zstr_send(state->watchdog, "tick");
    }

    if (terminate) {
        printf("[I] controller: detected config change. terminating.\n");
        zsys_interrupted = 1;
    } else {
        int rc = zloop_timer(loop, next_tick, 1, collect_stats_and_forward, state);
        assert(rc != -1);
    }

    return 0;
}

static
bool controller_create_actors(controller_state_t *state)
{
    // start the statsd updater
    state->statsd_server = zactor_new(statsd_actor_fn, state->config);
    // start the live stream publisher
    state->live_stream_publisher = zactor_new(live_stream_actor_fn, state->config);
    // start the indexer
    state->indexer = zactor_new(indexer, NULL);
    // create subscriber
    state->subscriber = zactor_new(subscriber, state->config);
    //start the tracker
    state->tracker = zactor_new(tracker, NULL);

    // create socket for stats updates
    state->updates_socket = zsock_new(ZMQ_PUSH);
    assert(state->updates_socket);
    zsock_set_sndtimeo(state->updates_socket, 10);
    zsock_set_sndhwm(state->updates_socket, 5000);
    int rc = zsock_bind(state->updates_socket, "inproc://stats-updates");
    assert(rc == 0);

    // create socket for adders
    state->adder_socket = zsock_new(ZMQ_DEALER);
    assert(state->adder_socket);
    zsock_set_sndtimeo(state->adder_socket, 10);
    rc = zsock_bind(state->adder_socket, "inproc://adders");
    assert(rc == 0);

    // connect to live stream
    state->live_stream_socket = live_stream_client_socket_new(state->config);

    for (size_t i=0; i<num_writers; i++) {
        state->writers[i] = request_writer_new(state->config, i);
    }
    for (size_t i=0; i<num_updaters; i++) {
        state->updaters[i] = zactor_new(stats_updater, (void*)i);
    }
    for (size_t i=0; i<num_parsers; i++) {
        state->parsers[i] = parser_new(state->config, i);
    }
    num_adders = num_parsers / 2;
    for (size_t i=0; i<num_adders; i++) {
        state->adders[i] = zactor_new(adder, (void*)i);
    }

    // create watchdog
    state->watchdog = zactor_new(watchdog, state->config);

    return !zsys_interrupted;
}

static
void controller_destroy_actors(controller_state_t *state)
{
    if (verbose) printf("[D] controller: destroying watchdog\n");
    zactor_destroy(&state->watchdog);

    if (verbose) printf("[D] controller: destroying subscriber\n");
    zactor_destroy(&state->subscriber);

    for (size_t i=0; i<num_parsers; i++) {
        if (verbose) printf("[D] controller: destroying parser[%zu]\n", i);
        parser_destroy(&state->parsers[i]);
    }

    for (size_t i=0; i<num_writers; i++) {
        if (verbose) printf("[D] controller: destroying writer[%zu]\n", i);
        zactor_destroy(&state->writers[i]);
    }

    for (size_t i=0; i<num_updaters; i++) {
        if (verbose) printf("[D] controller: destroying updater[%zu]\n", i);
        zactor_destroy(&state->updaters[i]);
    }

    for (size_t i=0; i<num_adders; i++) {
        if (verbose) printf("[D] controller: destroying adder[%zu]\n", i);
        zactor_destroy(&state->adders[i]);
    }

    if (verbose) printf("[D] controller: destroying tracker\n");
    zactor_destroy(&state->tracker);

    if (verbose) printf("[D] controller: destroying statsd\n");
    zactor_destroy(&state->statsd_server);

    if (verbose) printf("[D] controller: destroying indexer\n");
    zactor_destroy(&state->indexer);

    if (verbose) printf("[D] controller: destroying live stream publisher\n");
    zactor_destroy(&state->live_stream_publisher);

    if (verbose) printf("[D] controller: destroying live stream socket\n");
    zsock_destroy(&state->live_stream_socket);

    if (verbose) printf("[D] controller: destroying updates socket\n");
    zsock_destroy(&state->updates_socket);

    if (verbose) printf("[D] controller: destroying adder socket\n");
    zsock_destroy(&state->adder_socket);
}

int run_controller_loop(zconfig_t* config, size_t io_threads)
{
    set_thread_name("controller[0]");
    printf("[I] controller: starting\n");

    int rc;
    // set global config
    zsys_init();

    // argument takes precedence over config file
    if (io_threads == 0)
        io_threads = atoi(zconfig_resolve(config, "frontend/threads/zmq_io", "1"));
    if (verbose)
        printf("[D] io-threads: %lu\n", io_threads);
    zsys_set_io_threads(io_threads);

    zsys_set_rcvhwm(1000);
    zsys_set_sndhwm(1000);
    zsys_set_linger(0);

    controller_state_t state = {.ticks = 0, .config = config, .updates_blocked = 0};
    bool start_up_complete = controller_create_actors(&state);

    if (!start_up_complete)
        goto exit;

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // flush increments to database every 1000 ms
    rc = zloop_timer(loop, 1000, 1, collect_stats_and_forward, &state);
    assert(rc != -1);

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

    // shutdown
    printf("[I] controller: shutting down\n");
    zloop_destroy(&loop);
    assert(loop == NULL);

 exit:
    controller_destroy_actors(&state);
    zsys_shutdown();

    printf("[I] controller: terminated\n");
    return 0;
}
