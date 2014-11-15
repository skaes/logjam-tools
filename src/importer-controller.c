#include "importer-controller.h"
#include "importer-increments.h"
#include "importer-parser.h"
#include "importer-processor.h"
#include "importer-livestream.h"
#include "importer-statsupdater.h"
#include "importer-requestwriter.h"
#include "importer-indexer.h"
#include "importer-subscriber.h"
#include "importer-resources.h"


/*
 * connections: n_w = NUM_WRITERS, n_p = NUM_PARSERS, nu_= NUM_UPDATERS, "[<>^v]" = connect, "o" = bind
 *
 *                 --- PIPE ---  indexer
 *                 --- PIPE ---  subscriber
 *                 --- PIPE ---  parsers(n_p)
 *  controller:    --- PIPE ---  writers(n_w)
 *                 --- PIPE ---  updaters(n_u)
 *                 --- PIPE ---  tracker
 *
 *                 PUSH    PULL
 *                 o----------<  updaters(n_u)
 *
 *                 PUSH    PULL
 *                 >----------o  live stream server
*/

// The controller creates all other threads, collects data from the parsers every second,
// combines the data, sends db update requests to the updaters and also feeds the live stream.
// The data from the pasrers is collected using the pipes, but maybe we should have an
// independent socket for this.

typedef struct {
    zconfig_t *config;
    zactor_t *subscriber;
    zactor_t *indexer;
    zactor_t *tracker;
    zactor_t *parsers[NUM_PARSERS];
    zactor_t *writers[NUM_WRITERS];
    zactor_t *updaters[NUM_UPDATERS];
    zsock_t *updates_socket;
    zsock_t *live_stream_socket;
    size_t ticks;
} controller_state_t;


static
void extract_parser_state(zmsg_t* msg, zhash_t **processors, size_t *parsed_msgs_count)
{
    zframe_t *first = zmsg_first(msg);
    zframe_t *second = zmsg_next(msg);
    assert(zframe_size(first) == sizeof(zhash_t*));
    memcpy(&*processors, zframe_data(first), sizeof(zhash_t*));
    assert(zframe_size(second) == sizeof(size_t));
    memcpy(parsed_msgs_count, zframe_data(second), sizeof(size_t));
}

static
void merge_quants(zhash_t *target, zhash_t *source)
{
    size_t *source_quants = NULL;

    while ( (source_quants = zhash_first(source)) ) {
        const char* key = zhash_cursor(source);
        assert(key);
        size_t *dest = zhash_lookup(target, key);
        if (dest) {
            for (int i=0; i <= last_resource_offset; i++) {
                dest[i] += source_quants[i];
            }
        } else {
            zhash_insert(target, key, source_quants);
            zhash_freefn(target, key, free);
            zhash_freefn(source, key, NULL);
        }
        zhash_delete(source, key);
    }
}

static
void merge_modules(zhash_t* target, zhash_t *source)
{
    char *source_module = NULL;

    while ( (source_module = zhash_first(source)) ) {
        const char* module = zhash_cursor(source);
        assert(module);
        char *dest_module = zhash_lookup(target, module);
        if (!dest_module) {
            zhash_insert(target, module, source_module);
            zhash_freefn(target, module, free);
            zhash_freefn(source, module, NULL);
        }
        zhash_delete(source, module);
    }
}

static
void merge_increments(zhash_t* target, zhash_t *source)
{
    increments_t *source_increments = NULL;

    while ( (source_increments = zhash_first(source)) ) {
        const char* namespace = zhash_cursor(source);
        assert(namespace);
        increments_t *dest_increments = zhash_lookup(target, namespace);
        if (dest_increments) {
            increments_add(dest_increments, source_increments);
        } else {
            zhash_insert(target, namespace, source_increments);
            zhash_freefn(target, namespace, increments_destroy);
            zhash_freefn(source, namespace, NULL);
        }
        zhash_delete(source, namespace);
    }
}


static
void merge_processors(zhash_t *target, zhash_t *source)
{
    processor_state_t* source_processor = NULL;

    while ( (source_processor = zhash_first(source)) ) {
        const char *db_name = zhash_cursor(source);
        assert(db_name);
        processor_state_t *dest_processor = zhash_lookup(target, db_name);

        if (dest_processor) {
            // printf("[D] combining %s\n", dest_processor->db_name);
            assert( streq(dest_processor->db_name, source_processor->db_name) );
            dest_processor->request_count += source_processor->request_count;
            merge_modules(dest_processor->modules, source_processor->modules);
            merge_increments(dest_processor->totals, source_processor->totals);
            merge_increments(dest_processor->minutes, source_processor->minutes);
            merge_quants(dest_processor->quants, source_processor->quants);
        } else {
            zhash_insert(target, db_name, source_processor);
            zhash_freefn(target, db_name, processor_destroy);
            zhash_freefn(source, db_name, NULL);
        }
        zhash_delete(source, db_name);
    }
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
    processor_state_t* processor = zhash_first(processors);
    zhash_t *published_streams= zhash_new();
    while (processor) {
        stream_info_t *stream_info = processor->stream_info;
        update_known_modules(stream_info, processor->modules);
        zhash_insert(published_streams, stream_info->key, (void*)1);
        publish_totals(stream_info, processor->totals, state->live_stream_socket);
        processor = zhash_next(processors);
    }

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
    zhash_t *processors[NUM_PARSERS];
    size_t parsed_msgs_counts[NUM_PARSERS];

    state->ticks++;

    // tell tracker and subscriber to tick
    zstr_send(state->subscriber, "tick");
    zstr_send(state->tracker, "tick");

    // printf("[D] controller: collecting data from parsers: tick[%zu]\n", state->ticks);
    for (size_t i=0; i<NUM_PARSERS; i++) {
        zactor_t* parser = state->parsers[i];
        zstr_send(parser, "tick");
        zmsg_t *response = zmsg_recv(parser);
        extract_parser_state(response, &processors[i], &parsed_msgs_counts[i]);
        zmsg_destroy(&response);
    }

    // printf("[D] controller: combining processors states\n");
    size_t parsed_msgs_count = parsed_msgs_counts[0];
    for (size_t i=1; i<NUM_PARSERS; i++) {
        parsed_msgs_count += parsed_msgs_counts[i];
        merge_processors(processors[0], processors[i]);
        zhash_destroy(&processors[i]);
    }

    // publish on live stream (need to do this while we still own the processors)
    // printf("[D] controller: publishing live streams\n");
    publish_totals_for_every_known_stream(state, processors[0]);

    // tell indexer to tick
    zstr_send(state->indexer, "tick");

    // tell stats updaters to tick
    for (int i=0; i<NUM_UPDATERS; i++) {
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
            fprintf(stderr, "[W] controller: updates push socket not ready\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        // send minutes updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "m");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->minutes);
        proc->minutes = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        // send quants updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "q");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addptr(stats_msg, proc->stream_info);
        zmsg_addptr(stats_msg, proc->quants);
        proc->quants = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            fprintf(stderr, "[W] controller: updates push socket not ready. blocking!\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        db_name = zlist_next(db_names);
    }
    zlist_destroy(&db_names);
    zhash_destroy(&processors[0]);

    // tell request writers to tick
    for (int i=0; i<NUM_WRITERS; i++) {
        zstr_send(state->writers[i], "tick");
    }

    bool terminate = (state->ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    int64_t end_time_ms = zclock_mono();
    int runtime = end_time_ms - start_time_ms;
    int next_tick = runtime > 999 ? 1 : 1000 - runtime;
    printf("[I] controller: %5zu messages (%d ms)\n", parsed_msgs_count, runtime);

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
    // start the indexer
    state->indexer = zactor_new(indexer, NULL);

    // create subscriber
    state->subscriber = zactor_new(subscriber, state->config);

    //start the tracker
    state->tracker = zactor_new(tracker, NULL);

    // create socket for stats updates
    state->updates_socket = zsock_new(ZMQ_PUSH);
    int rc = zsock_bind(state->updates_socket, "inproc://stats-updates");
    assert(rc == 0);

    // connect to live stream
    state->live_stream_socket = live_stream_socket_new();

    for (size_t i=0; i<NUM_WRITERS; i++) {
        state->writers[i] = zactor_new(request_writer, (void*)i);
    }
    for (size_t i=0; i<NUM_UPDATERS; i++) {
        state->updaters[i] = zactor_new(stats_updater, (void*)i);
    }
    for (size_t i=0; i<NUM_PARSERS; i++) {
        state->parsers[i] = zactor_new(parser, (void*)i);
    }

    return !zsys_interrupted;
}

static
void controller_destroy_actors(controller_state_t *state)
{
    zactor_destroy(&state->subscriber);
    zactor_destroy(&state->indexer);
    zactor_destroy(&state->tracker);
    for (size_t i=0; i<NUM_PARSERS; i++) {
        zactor_destroy(&state->parsers[i]);
    }
    for (size_t i=0; i<NUM_WRITERS; i++) {
        zactor_destroy(&state->writers[i]);
    }
    for (size_t i=0; i<NUM_UPDATERS; i++) {
        zactor_destroy(&state->updaters[i]);
    }
    zsock_destroy(&state->live_stream_socket);
    zsock_destroy(&state->updates_socket);
}

int run_controller_loop(zconfig_t* config)
{
    set_thread_name("controller[0]");

    int rc;
    // set global config
    zsys_init();
    zsys_set_io_threads(1);
    zsys_set_rcvhwm(1000);
    zsys_set_sndhwm(1000);
    zsys_set_linger(100);

    controller_state_t state = {.ticks = 0, .config = config};
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

    if (!zsys_interrupted) {
        // run the loop
        zloop_start(loop);
        printf("[I] controller: shutting down\n");
    }

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);

 exit:
    controller_destroy_actors(&state);
    zsys_shutdown();

    printf("[I] controller: terminated\n");
    return 0;
}
