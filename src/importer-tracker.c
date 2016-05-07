#include "importer-tracker.h"
#include "zring.h"

/*
 * connections:  n_p = num_parsers, "[<>^v]" = connect, "o" = bind
 *
 *                               controller
 *                                   |
 *                                  PIPE
 *                                   |    PUSH   PULL
 *                                tracker >---------o subscriber
 *                                o     o
 *                            REP |     | PULL
 *                   deletes *    |     |     * inserts
 *                            REQ |     | PUSH
 *                                ^     ^
 *                              parser(n_p)
*/


// tracker client state
struct _uuid_tracker_t {
    zsock_t *additions;    // inserts, client socket
    zsock_t *deletions;    // deletes, client socket
};

// tracker server state
typedef struct {
    size_t id;                    // 0
    uint64_t current_time_ms;     // updated by time event to save cpu cycles
    uint64_t age_threshold_ms;    // drop entries older than this timestamp
    size_t added;                 // number of inserts since last tick
    size_t deleted;               // number of deletes since last tick
    size_t expired;               // number of expired entries since last tick
    size_t failed;                // number of failed deletions since last tick
    size_t duplicates;            // number of duplicate inserts since last tick
    zsock_t *additions;           // inserts, server socket
    zsock_t *deletions;           // deletions, server socket
    zsock_t *subscriber;          // send retriable frontend request inserts back to subscriber
    zsock_t *pipe;                // controller pipe
    zring_t *uuids;               // inserted backend request uuids    [uuid --> insertion time]
    zring_t *failures;            // failed frontend request deletions [uuid --> {insertion time, original zmq message}]
    zring_t *successes;           // successfully processed deletions  [uuid --> insertion time]
    bool received_term_cmd;       // whether we have received a TERM command
} tracker_state_t;

// see ^^^failures^^^
typedef struct {
    uint64_t created_time_ms;
    zmsg_t *msg;
} failure_t;


// construct client instance
uuid_tracker_t* tracker_new()
{
    int rc;
    uuid_tracker_t *tracker = (uuid_tracker_t *) zmalloc(sizeof(*tracker));

    tracker->additions = zsock_new(ZMQ_PUSH);
    assert(tracker->additions);
    zsock_set_sndtimeo(tracker->additions, 10);
    rc = zsock_connect(tracker->additions, "inproc://tracker-additions");
    assert(rc != -1);

    tracker->deletions = zsock_new(ZMQ_REQ);
    assert(tracker->deletions);
    zsock_set_sndtimeo(tracker->deletions, 10);
    zsock_set_rcvtimeo(tracker->deletions, 1000);
    zsock_connect(tracker->deletions, "inproc://tracker-deletions");
    assert(rc != -1);

    return tracker;
}

// destroy client instance
void tracker_destroy(uuid_tracker_t **tracker)
{
    uuid_tracker_t *t = *tracker;
    zsock_destroy(&t->additions);
    zsock_destroy(&t->deletions);
    free(t);
    *tracker = NULL;
}

// client interface to send uuid addition requests to server (asynchronously)
int tracker_add_uuid(uuid_tracker_t *tracker, const char* uuid)
{
    return zstr_send(tracker->additions, uuid);
}

// client interface to send uuid deletion requests to server (synchronously)
// returns whether request has been successfull
int tracker_delete_uuid(uuid_tracker_t *tracker, const char* uuid, zmsg_t* original_msg, const char* request_type)
{
    zmsg_t *msg = zmsg_new();
    assert(msg);
    zmsg_addstr(msg, uuid);
    zmsg_addptr(msg, original_msg);
    zmsg_addstr(msg, request_type);

    if (zmsg_send_with_retry(&msg, tracker->deletions)) {
        // we got interrupted
        return 0;
    }

    msg = zmsg_recv(tracker->deletions);
    int deleted = 0;
    if (msg) {
        zframe_t *rcf = zmsg_first(msg);
        if (rcf) {
            assert(zframe_size(rcf) == sizeof(int));
            deleted = *((int*)zframe_data(rcf));
        }
        zmsg_destroy(&msg);
    }
    return deleted;
}

#define EXPIRE_THRESHOLD_1MINUTE (1000 * 60 * 1)
#define EXPIRE_THRESHOLD_5MINUTES (1000 * 60 * 5)
#define EXPIRE_THRESHOLD_MS EXPIRE_THRESHOLD_5MINUTES

// set current server time and expiry threshold (called from timer callback function)
static
void tracker_state_set_time_params(tracker_state_t* state)
{
    state->current_time_ms = zclock_time();
    state->age_threshold_ms = state->current_time_ms - EXPIRE_THRESHOLD_MS;
}

// initialize server state
static
tracker_state_t* tracker_state_new(zsock_t *pipe, size_t id)
{
    int rc;
    tracker_state_t* ts = (tracker_state_t*) zmalloc(sizeof(*ts));
    ts->id = id;
    ts->pipe = pipe;
    ts->uuids = zring_new();
    ts->failures = zring_new();
    ts->successes = zring_new();

    tracker_state_set_time_params(ts);

    ts->additions = zsock_new(ZMQ_PULL);
    assert(ts->additions);
    rc = zsock_bind(ts->additions, "inproc://tracker-additions");
    assert(rc != -1);

    ts->deletions = zsock_new(ZMQ_REP);
    assert(ts->deletions);
    rc = zsock_bind(ts->deletions, "inproc://tracker-deletions");
    assert(rc != -1);

    ts->subscriber = zsock_new(ZMQ_PUSH);
    assert(ts->subscriber);
    rc = zsock_connect(ts->subscriber, "inproc://subscriber-pull");
    assert(rc != -1);

    return ts;
}

// destroy server state
static
void tracker_state_destroy(tracker_state_t **tracker)
{
    tracker_state_t *ts = *tracker;
    zsock_destroy(&ts->additions);
    zsock_destroy(&ts->deletions);
    zsock_destroy(&ts->subscriber);
    zring_destroy(&ts->uuids);
    zring_destroy(&ts->failures);
    zring_destroy(&ts->successes);
    *tracker = NULL;
}

// remove expired uuids from server state
static
void clean_expired_uuids(tracker_state_t *state, uint64_t age_threshold)
{
    zring_t *uuids = state->uuids;
    uint64_t item;
    while ( (item = (uint64_t)zring_first(uuids)) ) {
        if (item < age_threshold) {
            // const char *uuid = zring_key(uuids);
            // printf("[D] tracker[%zu]: expired uuid: %s\n", state->id, uuid);
            state->expired++;
            zring_shift(uuids);
        } else {
            break;
        }
    }
}

// remove expired failures from server state
static
void clean_expired_failures(tracker_state_t *state, uint64_t age_threshold)
{
    zring_t *failures = state->failures;
    failure_t *failure;
    while ( (failure = zring_first(failures)) ) {
        if (failure->created_time_ms < age_threshold) {
            // const char *uuid = zring_key(failures);
            // printf("[D] tracker[%zu]: failed uuid: %s\n", state->id, uuid);
            state->failed++;
            zring_shift(failures);
            zmsg_destroy(&failure->msg);
            free(failure);
        } else {
            break;
        }
    }
}

// remove expired successes from server state
static
void clean_expired_successes(tracker_state_t *state, uint64_t age_threshold)
{
    zring_t *successes = state->successes;
    uint64_t item;
    while ( (item = (uint64_t)zring_first(successes)) ) {
        if (item < age_threshold) {
            // const char *uuid = zring_key(successes);
            // printf("[D] tracker[%zu]: success uuid: %s\n", state->id, uuid);
            zring_shift(successes);
        } else {
            break;
        }
    }
}

// remove expired uuids, failures and successes from server state
static
void server_clean_expired_items(tracker_state_t *state)
{
    uint64_t age_threshold = state->age_threshold_ms;
    clean_expired_uuids(state, age_threshold);
    clean_expired_failures(state, age_threshold);
    clean_expired_successes(state, age_threshold);
}

// add a uuid
static
int server_add_uuid(zloop_t *loop, zsock_t *socket, void *args)
{
    tracker_state_t *state = args;
    zmsg_t *msg = zmsg_recv(socket);
    assert(msg);
    char *uuid = zmsg_popstr(msg);
    assert(uuid);
    failure_t *failure = zring_lookup(state->failures, uuid);
    if (failure) {
        // printf("[D] tracker[%zu]: forwarding late backend uuid: %s\n", state->id, uuid);
        zring_delete(state->failures, uuid);
        zring_insert(state->uuids, uuid, (void*)state->current_time_ms);
        state->added++;
        zmsg_send_with_retry(&failure->msg, state->subscriber);
        free(failure);
    } else {
        uint64_t seen = (uint64_t)zring_lookup(state->successes, uuid) || (uint64_t)zring_lookup(state->uuids, uuid);
        if (seen) {
            fprintf(stderr, "[E] tracker[%zu]: refused adding duplicate backend uuid: %s\n", state->id, uuid);
        } else {
            // printf("[D] tracker[%zu]: adding uuid: %s\n", state->id, uuid);
            zring_insert(state->uuids, uuid, (void*)state->current_time_ms);
            state->added++;
        }
    }
    free(uuid);
    zmsg_destroy(&msg);
    return 0;
}


// delete a uuid
static
int server_delete_uuid(zloop_t *loop, zsock_t *socket, void *arg)
{
    int rc = 0;
    tracker_state_t *state = arg;
    server_clean_expired_items(state);
    zmsg_t *msg = zmsg_recv(socket);
    assert(msg);
    char *uuid = zmsg_popstr(msg);
    assert(uuid);
    zmsg_t *original_msg = zmsg_popptr(msg);
    assert(original_msg);
    char *request_type = zmsg_popstr(msg);
    assert(request_type);
    uint64_t seen;
    if ( (seen = (uint64_t)zring_lookup(state->uuids, uuid)) ) {
        // printf("[D] tracker[%zu]: found uuid: %s\n", state->id, uuid);
        rc = 1;
        zring_delete(state->uuids, uuid);
        zring_insert(state->successes, uuid, (void*)seen);
        state->deleted++;
    } else if ( zring_lookup(state->successes, uuid) || zring_lookup(state->failures, uuid) ) {
        // fprintf(stderr, "[W] tracker[%zu]: duplicate %s uuid: %s\n", state->id, request_type, uuid);
        state->duplicates++;
    } else {
        // printf("[D] tracker[%zu]: missing uuid: %s\n", state->id, uuid);
        failure_t *failure = zmalloc(sizeof(*failure));
        failure->created_time_ms = state->current_time_ms;
        failure->msg = zmsg_dup(original_msg);
        zmsg_clear_device_and_sequence_number(failure->msg);
        zring_insert(state->failures, uuid, failure);
    }
    free(uuid);
    free(request_type);
    zmsg_addmem(msg, &rc, sizeof(rc));
    zmsg_send_with_retry(&msg, socket);
    return 0;
}

// reset servers state and log state
static
void tracker_tick(tracker_state_t *state)
{
    server_clean_expired_items(state);
    if (verbose) {
        printf("[I] tracker[%zu]: uuid hash size %zu"
               "(added=%zu, deleted=%zu, expired=%zu, failed=%zu, delayed=%zu, duplicates=%zu)\n",
               state->id, zring_size(state->uuids), state->added, state->deleted, state->expired,
               state->failed, zring_size(state->failures), state->duplicates);
    }
    state->added = 0;
    state->deleted = 0;
    state->expired = 0;
    state->failed = 0;
    state->duplicates = 0;
}

// perform actor command: "$TERM" or "tick". othwerwise log error.
static
int actor_command(zloop_t *loop, zsock_t *socket, void *args)
{
    int rc = 0;
    tracker_state_t *state = args;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        assert(cmd);
        zmsg_destroy(&msg);
        if (streq(cmd, "$TERM")) {
            state->received_term_cmd = true;
            // printf("[D] tracker[%d]: received $TERM command\n", state->id);
            rc = -1;
        } else if (streq(cmd, "tick")) {
            tracker_tick(state);
        } else {
            fprintf(stderr, "[E] tracker[%zu]: received unknown actor command: %s\n", state->id, cmd);
        }
        free(cmd);
    }
    return rc;
}

// update seriver time and expiry
static
int timer_event(zloop_t *loop, int timer_id, void *args)
{
    tracker_state_t* state = (tracker_state_t*)args;
    tracker_state_set_time_params(state);
    return 0;
}

// zactor loop
void tracker(zsock_t *pipe, void *args)
{
    set_thread_name("tracker[0]");

    int rc;
    size_t id = 0;
    tracker_state_t* state = (tracker_state_t*) tracker_state_new(pipe, id);
    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);
    // we rely on the controller shutting us down
    zloop_ignore_interrupts(loop);

    // setup timer to track current time using 10ms resolution
    int timer_id = zloop_timer(loop, 10, 0, timer_event, state);
    assert(timer_id != -1);

    // setup handler for actor messages
    rc = zloop_reader(loop, state->pipe, actor_command, state);
    assert(rc == 0);

    // setup handler for the additions socket
    rc = zloop_reader(loop, state->additions, server_add_uuid, state);
    assert(rc == 0);

    // setup handler for the deletions socket
    rc = zloop_reader(loop, state->deletions, server_delete_uuid, state);
    assert(rc == 0);

    // run the loop
    if (!quiet)
        printf("[I] tracker[%zu]: listening\n", id);

    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        if (!state->received_term_cmd)
            log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    // shutdown
    if (!quiet)
        printf("[I] tracker[%zu]: shutting down\n", id);

    zloop_destroy(&loop);
    assert(loop == NULL);
    tracker_state_destroy(&state);

    if (!quiet)
        printf("[I] tracker[%zu]: terminated\n", id);
}
