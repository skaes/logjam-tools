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

typedef struct {
    void *subscriber_pipe;
    void *indexer_pipe;
    void *parser_pipes[NUM_PARSERS];
    void *writer_pipes[NUM_WRITERS];
    void *updater_pipes[NUM_UPDATERS];
    void *updates_socket;
    void *live_stream_socket;
    size_t ticks;
} controller_state_t;


typedef struct {
    zhash_t *source;
    zhash_t *target;
} hash_pair_t;


static
int add_quant_to_quants_hash(const char* key, void* data, void *arg)
{
    hash_pair_t *p = arg;
    size_t *stored = zhash_lookup(p->target, key);
    if (stored != NULL) {
        for (int i=0; i <= last_resource_offset; i++) {
            stored[i] += ((size_t*)data)[i];
        }
    } else {
        zhash_insert(p->target, key, data);
        zhash_freefn(p->target, key, free);
        zhash_freefn(p->source, key, NULL);
    }
    return 0;
}

static
void combine_quants(zhash_t *target, zhash_t *source)
{
    hash_pair_t hash_pair;
    hash_pair.source = source;
    hash_pair.target = target;
    zhash_foreach(source, add_quant_to_quants_hash, &hash_pair);
}

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
int add_modules(const char* module, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    char *dest = zhash_lookup(pair->target, module);
    if (dest == NULL) {
        zhash_insert(pair->target, module, data);
        zhash_freefn(pair->target, module, free);
        zhash_freefn(pair->source, module, NULL);
    }
    return 0;
}

static
int add_increments(const char* namespace, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    increments_t *dest_increments = zhash_lookup(pair->target, namespace);
    if (dest_increments == NULL) {
        zhash_insert(pair->target, namespace, data);
        zhash_freefn(pair->target, namespace, increments_destroy);
        zhash_freefn(pair->source, namespace, NULL);
    } else {
        increments_add(dest_increments, (increments_t*)data);
    }
    return 0;
}

static
void combine_modules(zhash_t* target, zhash_t *source)
{
    hash_pair_t hash_pair;
    hash_pair.source = source;
    hash_pair.target = target;
    zhash_foreach(source, add_modules, &hash_pair);
}

static
void combine_increments(zhash_t* target, zhash_t *source)
{
    hash_pair_t hash_pair;
    hash_pair.source = source;
    hash_pair.target = target;
    zhash_foreach(source, add_increments, &hash_pair);
}

static
void combine_processors(processor_state_t* target, processor_state_t* source)
{
    // printf("[D] combining %s\n", target->db_name);
    assert(!strcmp(target->db_name, source->db_name));
    target->request_count += source->request_count;
    combine_modules(target->modules, source->modules);
    combine_increments(target->totals, source->totals);
    combine_increments(target->minutes, source->minutes);
    combine_quants(target->quants, source->quants);
}

static
int merge_processors(const char* db_name, void* data, void* arg)
{
    hash_pair_t *pair = arg;
    // printf("[D] checking %s\n", db_name);
    processor_state_t *dest = zhash_lookup(pair->target, db_name);
    if (dest == NULL) {
        zhash_insert(pair->target, db_name, data);
        zhash_freefn(pair->target, db_name, processor_destroy);
        zhash_freefn(pair->source, db_name, NULL);
    } else {
        combine_processors(dest, (processor_state_t*)data);
    }
    return 0;
}

static
int collect_stats_and_forward(zloop_t *loop, int timer_id, void *arg)
{
    int64_t start_time_ms = zclock_time();
    controller_state_t *state = arg;
    zhash_t *processors[NUM_PARSERS];
    size_t parsed_msgs_counts[NUM_PARSERS];

    state->ticks++;

    for (size_t i=0; i<NUM_PARSERS; i++) {
        void* parser_pipe = state->parser_pipes[i];
        zmsg_t *tick = zmsg_new();
        zmsg_addstr(tick, "tick");
        zmsg_send(&tick, parser_pipe);
        zmsg_t *response = zmsg_recv(parser_pipe);
        extract_parser_state(response, &processors[i], &parsed_msgs_counts[i]);
        zmsg_destroy(&response);
    }

    size_t parsed_msgs_count = parsed_msgs_counts[0];
    for (size_t i=1; i<NUM_PARSERS; i++) {
        parsed_msgs_count += parsed_msgs_counts[i];
    }

    for (size_t i=1; i<NUM_PARSERS; i++) {
        hash_pair_t pair;
        pair.source = processors[i];
        pair.target = processors[0];
        zhash_foreach(pair.source, merge_processors, &pair);
        zhash_destroy(&processors[i]);
    }

    // publish on live stream (need to do this while we still own the processors)
    zhash_foreach(processors[0], processor_publish_totals, state->live_stream_socket);

    // tell indexer to tick
    {
        zmsg_t *tick = zmsg_new();
        zmsg_addstr(tick, "tick");
        zmsg_send(&tick, state->indexer_pipe);
    }

    // tell stats updaters to tick
    for (int i=0; i<NUM_UPDATERS; i++) {
        zmsg_t *tick = zmsg_new();
        zmsg_addstr(tick, "tick");
        zmsg_send(&tick, state->updater_pipes[i]);
    }

    // forward to stats_updaters
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
        zmsg_addmem(stats_msg, &proc->stream_info, sizeof(proc->stream_info));
        zmsg_addmem(stats_msg, &proc->totals, sizeof(proc->totals));
        proc->totals = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            fprintf(stderr, "[W] controller: updates push socket not ready\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        // send minutes updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "m");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addmem(stats_msg, &proc->stream_info, sizeof(proc->stream_info));
        zmsg_addmem(stats_msg, &proc->minutes, sizeof(proc->minutes));
        proc->minutes = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            fprintf(stderr, "[W] controller: updates push socket not ready\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        // send quants updates
        stats_msg = zmsg_new();
        zmsg_addstr(stats_msg, "q");
        zmsg_addstr(stats_msg, proc->db_name);
        zmsg_addmem(stats_msg, &proc->stream_info, sizeof(proc->stream_info));
        zmsg_addmem(stats_msg, &proc->quants, sizeof(proc->quants));
        proc->quants = NULL;
        if (!output_socket_ready(state->updates_socket, 0)) {
            fprintf(stderr, "[W] controller: updates push socket not ready\n");
        }
        zmsg_send(&stats_msg, state->updates_socket);

        db_name = zlist_next(db_names);
    }
    zlist_destroy(&db_names);
    zhash_destroy(&processors[0]);

    // tell request writers to tick
    for (int i=0; i<NUM_WRITERS; i++) {
        zmsg_t *tick = zmsg_new();
        zmsg_addstr(tick, "tick");
        zmsg_send(&tick, state->writer_pipes[i]);
    }

    bool terminate = (state->ticks % CONFIG_FILE_CHECK_INTERVAL == 0) && config_file_has_changed();
    int64_t end_time_ms = zclock_time();
    int runtime = end_time_ms - start_time_ms;
    int next_tick = runtime > 999 ? 1 : 1000 - runtime;
    printf("[I] controller: %5zu messages (%d ms)\n", parsed_msgs_count, runtime);

    if (terminate) {
        printf("[I] controller: detected config change. terminating.\n");
        zctx_interrupted = 1;
    } else {
        int rc = zloop_timer(loop, next_tick, 1, collect_stats_and_forward, state);
        assert(rc != -1);
    }

    return 0;
}

int run_controller_loop(zconfig_t* config)
{
    int rc;

    // establish global zeromq context
    zctx_t *context = zctx_new();
    assert(context);
    zctx_set_rcvhwm(context, 1000);
    zctx_set_sndhwm(context, 1000);
    zctx_set_linger(context, 100);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    controller_state_t state = {.ticks = 0};

    // start the indexer
    state.indexer_pipe = zthread_fork(context, indexer, NULL);
    {
        // wait for initial db index creation
        zmsg_t * msg = zmsg_recv(state.indexer_pipe);
        zmsg_destroy(&msg);

        if (zctx_interrupted) goto exit;
    }

    // create socket for stats updates
    state.updates_socket = zsocket_new(context, ZMQ_PUSH);
    rc = zsocket_bind(state.updates_socket, "inproc://stats-updates");
    assert(rc == 0);

    // connect to live stream
    state.live_stream_socket = live_stream_socket_new(context);

    // start all worker threads
    state.subscriber_pipe = zthread_fork(context, subscriber, config);
    for (size_t i=0; i<NUM_WRITERS; i++) {
        state.writer_pipes[i] = zthread_fork(context, request_writer, (void*)i);
    }
    for (size_t i=0; i<NUM_UPDATERS; i++) {
        state.updater_pipes[i] = zthread_fork(context, stats_updater, (void*)i);
    }
    for (size_t i=0; i<NUM_PARSERS; i++) {
        state.parser_pipes[i] = zthread_fork(context, parser, (void*)i);
    }

    // flush increments to database every 1000 ms
    rc = zloop_timer(loop, 1000, 1, collect_stats_and_forward, &state);
    assert(rc != -1);

    if (!zctx_interrupted) {
        // run the loop
        rc = zloop_start(loop);
        printf("[I] shutting down: %d\n", rc);
    }

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);

 exit:
    zctx_destroy(&context);

    return 0;
}
