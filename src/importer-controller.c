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
#include "unknown-streams-collector.h"
#include "importer-prometheus-client.h"

/*
 * connections: n_s = num_subscribers, n_w = num_writers, n_p = num_parsers, n_u= num_updaters, n_a = num_adders "[<>^v]" = connect, "o" = bind
 *
 *                 --- PIPE ---  indexer
 *                 --- PIPE ---  subscribers(n_s)
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

unsigned long num_subscribers = 1;
unsigned long num_parsers = 8;
unsigned long num_writers = 10;
unsigned long num_updaters = 10;
unsigned long num_adders = 4;

typedef struct {
    zconfig_t *config;
    zactor_t *stream_config_updater;
    zactor_t *indexer;
    zactor_t *tracker;
    zactor_t *controller_watchdog;
    zactor_t *subscriber_watchdog;
    zactor_t *subscribers[MAX_SUBSCRIBERS];
    zactor_t *parsers[MAX_PARSERS];
    zactor_t *adders[MAX_ADDERS];
    zactor_t *writers[MAX_WRITERS];
    zactor_t *updaters[MAX_UPDATERS];
    zactor_t *live_stream_publisher;
    zactor_t *unknown_streams_collector;
    zsock_t *updates_socket;
    size_t updates_blocked;
    zsock_t *adder_socket;
    zsock_t *live_stream_socket;
    size_t ticks;
    zlist_t *collected_processors;
} controller_state_t;


static
void extract_parser_state(controller_state_t *state, zmsg_t* msg, zhash_t **processors, size_t *parsed_msgs_count, frontend_stats_t *fe_stats)
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
    zlist_t *streams = get_active_stream_names();
    const char *stream = zlist_first(streams);
    while (stream) {
        stream_info_t *stream_info = get_stream_info(stream, NULL);
        if (stream_info) {
            if (!zhash_lookup(published_streams, stream)) {
                publish_totals(stream_info, NULL, state->live_stream_socket);
            }
            release_stream_info(stream_info);
        }
        stream = zlist_next(streams);
    }
    zlist_destroy(&streams);
    zhash_destroy(&published_streams);
}

static
void merge_processors(controller_state_t *state, zlist_t *additions)
{
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
            if (zsys_interrupted)
                return;
            assert(rc==0);
        }
        for (int i = 0; i < n; i++ ) {
            zmsg_t *reply = zmsg_recv_with_retry(state->adder_socket);
            if (zsys_interrupted)
                return;
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

}

static
void forward_updates(controller_state_t *state, zhash_t *processor)
{
    zlist_t *db_names = zhash_keys(processor);
    const char* db_name = zlist_first(db_names);
    while (db_name != NULL) {
        processor_state_t *proc = zhash_lookup(processor, db_name);
        // printf("[D] forwarding %s\n", db_name);
        zmsg_t *stats_msg;

        // send totals updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "t");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        reference_stream_info(proc->stream_info);
        zmsg_addptr(stats_msg, proc->totals);
        proc->totals = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        if (zmsg_send_and_destroy(&stats_msg, state->updates_socket))
            release_stream_info(proc->stream_info);
        else
            __atomic_add_fetch(&queued_updates, 1, __ATOMIC_SEQ_CST);

        // send minutes updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "m");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        reference_stream_info(proc->stream_info);
        zmsg_addptr(stats_msg, proc->minutes);
        proc->minutes = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        if (zmsg_send_and_destroy(&stats_msg, state->updates_socket))
            release_stream_info(proc->stream_info);
        else
            __atomic_add_fetch(&queued_updates, 1, __ATOMIC_SEQ_CST);

        // send quants updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "q");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        reference_stream_info(proc->stream_info);
        zmsg_addptr(stats_msg, proc->quants);
        proc->quants = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        if (zmsg_send_and_destroy(&stats_msg, state->updates_socket))
            release_stream_info(proc->stream_info);
        else
            __atomic_add_fetch(&queued_updates, 1, __ATOMIC_SEQ_CST);

        // send histogram updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "h");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        reference_stream_info(proc->stream_info);
        zmsg_addptr(stats_msg, proc->histograms);
        proc->histograms = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        if (zmsg_send_and_destroy(&stats_msg, state->updates_socket))
            release_stream_info(proc->stream_info);
        else
            __atomic_add_fetch(&queued_updates, 1, __ATOMIC_SEQ_CST);

        // send agents updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "a");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        reference_stream_info(proc->stream_info);
        zmsg_addptr(stats_msg, proc->agents);
        proc->agents = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            if (!state->updates_blocked++)
                fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        if (zmsg_send_and_destroy(&stats_msg, state->updates_socket))
            release_stream_info(proc->stream_info);
        else
            __atomic_add_fetch(&queued_updates, 1, __ATOMIC_SEQ_CST);

        db_name = zlist_next(db_names);
    }
    zlist_destroy(&db_names);
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

    // tell tracker, subscribers, live stream publisher and stream updater to tick
    zstr_send(state->stream_config_updater, "tick");

    size_t messages_received = 0;
    for (size_t i=0; i<num_subscribers; i++) {
        zstr_send(state->subscribers[i], "tick");
        zmsg_t *response = zmsg_recv(state->subscribers[i]);
        if (response) {
            zframe_t *frame = zmsg_first(response);
            messages_received += zframe_getsize(frame);
            zmsg_destroy(&response);
        }
    }
    if (messages_received > 0 && state->subscriber_watchdog)
        zstr_send(state->subscriber_watchdog, "tick");

    zstr_send(state->tracker, "tick");
    zstr_send(state->live_stream_publisher, "tick");

    // printf("[D] controller: collecting data from parsers: tick[%zu]\n", state->ticks);
    for (size_t i=0; i<num_parsers; i++) {
        zactor_t* parser = state->parsers[i];
        zstr_send(parser, "tick");
        zmsg_t *response = zmsg_recv(parser);
        if (response) {
            extract_parser_state(state, response, &processors[i], &parsed_msgs_counts[i], &fe_stats[i]);
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

    merge_processors(state, additions);
    zhash_t *merged_processors = zlist_pop(additions);
    zlist_destroy(&additions);

    // publish on live stream (need to do this while we still own the processor)
    // printf("[D] controller: publishing live streams\n");
    publish_totals_for_every_known_stream(state, merged_processors);

    // tell indexer to tick
    // printf("[D] controller: ticking indexer\n");
    zstr_send(state->indexer, "tick");

    // tell stats updaters to tick
    // printf("[D] controller: ticking updaters\n");
    for (int i=0; i<num_updaters; i++) {
        zstr_send(state->updaters[i], "tick");
    }

    zlist_append(state->collected_processors, merged_processors);
    // combine stats of collected processor from last tick with current one
    if (zlist_size(state->collected_processors) > 1) {
        // printf("[D] controller: merging processors\n");
        merge_processors(state, state->collected_processors);
    }

    // forward to stats_updaters
    if (state->ticks % DATABASE_UPDATE_INTERVAL == 0) {
        // printf("[D] controller: forwarding updates\n");
        zhash_t *processors = zlist_pop(state->collected_processors);
        forward_updates(state, processors);
        zhash_destroy(&processors);
    }

    // tell request writers to tick
    // printf("[D] controller: ticking writers\n");
    for (int i=0; i<num_writers; i++) {
        zstr_send(state->writers[i], "tick");
    }

    bool terminate = (state->ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    int64_t end_time_ms = zclock_mono();
    int runtime = end_time_ms - start_time_ms;
    int next_tick = runtime > 999 ? 1 : 1000 - runtime;
    double received_percent = parsed_msgs_count == 0 ? 0 : ((double) front_stats.received / parsed_msgs_count) * 100;
    double dropped_percent  = front_stats.received == 0 ? 0 : ((double) front_stats.dropped / front_stats.received) * 100;
    int updates, inserts;
    __atomic_load(&queued_updates, &updates, __ATOMIC_SEQ_CST);
    __atomic_load(&queued_inserts, &inserts, __ATOMIC_SEQ_CST);
    printf("[I] controller: %5zu messages (%3d ms); frontend: %3zu [%4.1f%%] (dropped: %2zu [%4.1f%%]); queued updates/inserts: %4d/%4d\n",
           parsed_msgs_count, runtime,
           front_stats.received, received_percent,
           front_stats.dropped, dropped_percent,
           updates, inserts) ;

    if (updates < 0) {
        printf("[E] controller: queued updates are negative: %d\n", updates);
        updates = 0;
    }
    if (inserts < 0) {
        printf("[E] controller: queued inserts are negative: %d\n", inserts);
        inserts = 0;
    }
    importer_prometheus_client_gauge_queued_updates(updates);
    importer_prometheus_client_gauge_queued_inserts(inserts);

    importer_prometheus_client_count_updates_blocked(state->updates_blocked);

    // log a warning about the number of blocked updates
    if (state->updates_blocked) {
        fprintf(stderr, "[W] controller: updates blocked: %zu\n", state->updates_blocked);
        state->updates_blocked = 0;
    }

    // signal liveness to watchdog, unless we're dropping all frontend requests
    if (front_stats.received == 0 || front_stats.dropped < front_stats.received) {
        zstr_send(state->controller_watchdog, "tick");
    } else {
        fprintf(stderr, "[W] controller: dropped all frontend stats: %zu\n", front_stats.dropped);
    }

    if (terminate) {
        printf("[I] controller: detected config change. terminating.\n");
        zsys_interrupted = 1;
    } else {
        int rc = zloop_timer(loop, next_tick, 1, collect_stats_and_forward, state);
        assert(rc != -1);
    }

#ifdef HAVE_MALLOC_TRIM
    // try to reduce memory usage. unclear whether this helps at all.
    if (malloc_trim_frequency > 0 && state->ticks % malloc_trim_frequency == 0 && !zsys_interrupted)
         malloc_trim(0);
#endif

    // printf("[D] controller: finished tick[%zu]\n", state->ticks);

    return 0;
}

static
bool controller_create_actors(controller_state_t *state, uint64_t indexer_opts)
{
    // initialize mongo client
    if (!dryrun)
        mongoc_init();

    // start the stream config updater (pass something not null to make it send indexer messages)
    state->stream_config_updater = stream_config_updater_new((void*)1);
    // start the indexer
    state->indexer = zactor_new(indexer, (void*)indexer_opts);
    if (initialize_dbs) return !zsys_interrupted;

    // start the live stream publisher
    state->live_stream_publisher = zactor_new(live_stream_actor_fn, state->config);
    // start the unknown streams collector
    state->unknown_streams_collector = zactor_new(unknown_streams_collector_actor_fn, NULL);

    // create subscribers
    for (size_t i=0; i<num_subscribers; i++) {
        state->subscribers[i] = subscriber_new(state->config, i);
    }
    //start the tracker
    state->tracker = zactor_new(tracker, NULL);

    // create socket for stats updates
    state->updates_socket = zsock_new(ZMQ_PUSH);
    assert(state->updates_socket);
    zsock_set_sndtimeo(state->updates_socket, 10);
    zsock_set_sndhwm(state->updates_socket, HWM_UNLIMITED);
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
        state->updaters[i] = stats_updater_new(state->config, i);
    }
    for (size_t i=0; i<num_parsers; i++) {
        state->parsers[i] = parser_new(state->config, i);
    }
    num_adders = (num_parsers + 1) / 2;
    for (size_t i=0; i<num_adders; i++) {
        state->adders[i] = zactor_new(adder, (void*)i);
    }

    // create watchdogs
    state->controller_watchdog = watchdog_new(10, 1, 0);
    if (zlist_size(hosts) > 0)
        state->subscriber_watchdog = watchdog_new(60, HEART_BEAT_INTERVAL, 1);

    return !zsys_interrupted;
}

static
void controller_destroy_actors(controller_state_t *state)
{
    if (state->controller_watchdog) {
        if (verbose) printf("[D] controller: destroying controller watchdog\n");
        watchdog_destroy(&state->controller_watchdog);
    }

    if (state->subscriber_watchdog) {
        if (verbose) printf("[D] controller: destroying subscriber watchdog\n");
        watchdog_destroy(&state->subscriber_watchdog);
    }

    if (state->stream_config_updater) {
        if (verbose) printf("[D] controller: destroying stream config updater\n");
        stream_config_updater_destroy(&state->stream_config_updater);
    }

    for (size_t i=0; i<num_subscribers; i++) {
        if (state->subscribers[i]) {
            if (verbose) printf("[D] controller: destroying subscriber[%zu]\n", i);
            subscriber_destroy(&state->subscribers[i]);
        }
    }

    for (size_t i=0; i<num_parsers; i++) {
        if (state->parsers[i]) {
            if (verbose) printf("[D] controller: destroying parser[%zu]\n", i);
            parser_destroy(&state->parsers[i]);
        }
    }

    for (size_t i=0; i<num_writers; i++) {
        if (state->writers[i]) {
            if (verbose) printf("[D] controller: destroying writer[%zu]\n", i);
            zactor_destroy(&state->writers[i]);
        }
    }

    for (size_t i=0; i<num_updaters; i++) {
        if (state->updaters[i]) {
            if (verbose) printf("[D] controller: destroying updater[%zu]\n", i);
            zactor_destroy(&state->updaters[i]);
        }
    }

    for (size_t i=0; i<num_adders; i++) {
        if (state->adders[i]) {
            if (verbose) printf("[D] controller: destroying adder[%zu]\n", i);
            zactor_destroy(&state->adders[i]);
        }
    }

    if (state->tracker) {
        if (verbose) printf("[D] controller: destroying tracker\n");
        zactor_destroy(&state->tracker);
    }

    if (state->indexer) {
        if (verbose) printf("[D] controller: destroying indexer\n");
        zactor_destroy(&state->indexer);
    }

    if (state->live_stream_publisher) {
        if (verbose) printf("[D] controller: destroying live stream publisher\n");
        zactor_destroy(&state->live_stream_publisher);
    }

    if (state->unknown_streams_collector) {
        if (verbose) printf("[D] controller: destroying unknown streams collector\n");
        zactor_destroy(&state->unknown_streams_collector);
    }

    if (state->live_stream_socket) {
        if (verbose) printf("[D] controller: destroying live stream socket\n");
        zsock_destroy(&state->live_stream_socket);
    }

    if (state->updates_socket) {
        if (verbose) printf("[D] controller: destroying updates socket\n");
        zsock_destroy(&state->updates_socket);
    }

    if (state->adder_socket) {
        if (verbose) printf("[D] controller: destroying adder socket\n");
        zsock_destroy(&state->adder_socket);
    }

    // shut down mongo client
    if (!dryrun)
        mongoc_cleanup();
}


#define MAX_ALLOWED_SHUTDOWN_TIME_SECONDS 10

static void alarm_handler(int sig, siginfo_t *si, void *uc) {
    printf("[W] controller: aborting shutdown sequence\n");
    signal(sig, SIG_IGN);
    abort();
}

static int start_shutdown_timer() {
    struct itimerval its;
    struct sigaction sa;

    // Establish handler for timer signal
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = alarm_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGALRM, &sa, NULL) == -1)
        return -1;

    // Start the timer
    its.it_value.tv_sec = MAX_ALLOWED_SHUTDOWN_TIME_SECONDS;
    its.it_value.tv_usec = 0;
    its.it_interval.tv_sec = 0;
    its.it_interval.tv_usec = 0;

    return setitimer(ITIMER_REAL, &its, NULL);
}

static int stop_shutdown_timer() {
    struct itimerval its;

    its.it_value.tv_sec = 0;
    its.it_value.tv_usec = 0;
    its.it_interval.tv_sec = 0;
    its.it_interval.tv_usec = 0;

    return setitimer(ITIMER_REAL, &its, NULL);
}

int run_controller_loop(zconfig_t* config, size_t io_threads, const char *logjam_url, const char* subscription_pattern, uint64_t indexer_opts)
{
    set_thread_name("controller[0]");
    printf("[I] controller: starting\n");

    int rc = 0;
    // set global config
    zsys_init();

    // argument takes precedence over config file
    if (io_threads == 0)
        io_threads = atoi(zconfig_resolve(config, "frontend/threads/zmq_io", "1"));
    if (verbose)
        printf("[D] io-threads: %lu\n", io_threads);
    zsys_set_io_threads(io_threads);

    zsys_set_rcvhwm(DEFAULT_RCV_HWM);
    zsys_set_sndhwm(DEFAULT_SND_HWM);
    zsys_set_linger(0);

    set_stream_create_fn((stream_fn*)importer_prometheus_client_create_stream_counters);
    set_stream_free_fn((stream_fn*)importer_prometheus_client_destroy_stream_counters);

    if (!setup_stream_config(logjam_url, subscription_pattern)) {
        rc = 1;
        goto shutdown;
    }
    controller_state_t state;
    memset(&state, 0, sizeof(state));
    state.config = config,
    state.collected_processors = zlist_new();
    assert(state.collected_processors);
    bool start_up_complete = controller_create_actors(&state, indexer_opts);

    if (!start_up_complete) {
        rc = 1;
        goto cleanup;
    }
    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // combine increments every 1000 ms
    rc = zloop_timer(loop, 1000, 1, collect_stats_and_forward, &state);
    assert(rc != -1);
    rc = 0;

    // run the loop
    // when running under the google profiler, zmq_poll terminates with EINTR
    // so we keep the loop running in this case
    if (!zsys_interrupted && !initialize_dbs) {
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

    zhash_t *p = NULL;
 cleanup:
    // free collected processors
    while ( (p = zlist_pop(state.collected_processors) )) {
        zhash_destroy(&p);
    }
    zlist_destroy(&state.collected_processors);
    // create apocalypse timer
    if (start_shutdown_timer() == -1)
        printf("[W] controller: could not start shutdown timer\n");
    else
        printf("[I] controller: started shutdown timer\n");

    // destroy actors
    controller_destroy_actors(&state);

 shutdown:
    // wait for actors to finish
    zsys_shutdown();

    // uncomment this to debug shutdown timer
    // make sure timer fires
    // sleep(MAX_ALLOWED_SHUTDOWN_TIME_SECONDS + 2);

    // delete the timer
    if (stop_shutdown_timer() == -1)
        printf("[W] controller: could not clear shutdown timer\n");
    else
        printf("[I] controller: cleared shutdown timer\n");

    printf("[I] controller: terminated\n");
    return rc;
}
