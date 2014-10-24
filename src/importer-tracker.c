#include "importer-tracker.h"

struct _uuid_tracker_t{
    zsock_t *additions;
    zsock_t *deletions;
};

typedef struct {
    size_t id;
    uint64_t current_time_ms;
    uint64_t age_threshold_ms;
    size_t added;
    size_t deleted;
    size_t expired;
    zsock_t *additions;
    zsock_t *deletions;
    zsock_t *pipe;
    zring_t *uuids;
} tracker_state_t;

uuid_tracker_t* tracker_new()
{
    int rc;
    uuid_tracker_t *tracker = (uuid_tracker_t *) zmalloc(sizeof(*tracker));

    tracker->additions = zsock_new(ZMQ_PUSH);
    assert(tracker->additions);
    rc = zsock_connect(tracker->additions, "inproc://tracker-additions");
    assert(rc != -1);

    tracker->deletions = zsock_new(ZMQ_REQ);
    assert(tracker->deletions);
    zsock_connect(tracker->deletions, "inproc://tracker-deletions");
    assert(rc != -1);

    return tracker;
}

void tracker_destroy(uuid_tracker_t **tracker)
{
    uuid_tracker_t *t = *tracker;
    zsock_destroy(&t->additions);
    zsock_destroy(&t->deletions);
    free(t);
    *tracker = NULL;
}

int tracker_add_uuid(uuid_tracker_t *tracker, const char* uuid)
{
    return zstr_send(tracker->additions, uuid);
}

int tracker_delete_uuid(uuid_tracker_t *tracker, const char* uuid)
{
    zstr_send(tracker->deletions, uuid);
    zmsg_t *msg = zmsg_recv(tracker->deletions);
    int deleted = 0;
    if (msg) {
        zframe_t *rcf = zmsg_first(msg);
        if (rcf) {
            assert(zframe_size(rcf) == sizeof(int));
            deleted = *((int*)zframe_data(rcf));
        }
    }
    zmsg_destroy(&msg);
    return deleted;
}

#define ONE_MINUTE_MS (1000 * 60)

static
void tracker_state_set_time_params(tracker_state_t* state)
{
    state->current_time_ms = zclock_time();
    state->age_threshold_ms = state->current_time_ms - ONE_MINUTE_MS;
}

static
tracker_state_t* tracker_state_new(zsock_t *pipe, size_t id)
{
    int rc;
    tracker_state_t* ts = (tracker_state_t*) zmalloc(sizeof(*ts));
    ts->id = id;
    ts->pipe = pipe;
    ts->uuids = zring_new();

    ts->additions = zsock_new(ZMQ_PULL);
    assert(ts->additions);
    rc = zsock_bind(ts->additions, "inproc://tracker-additions");
    assert(rc != -1);

    ts->deletions = zsock_new(ZMQ_REP);
    assert(ts->deletions);
    rc = zsock_bind(ts->deletions, "inproc://tracker-deletions");
    assert(rc != -1);

    tracker_state_set_time_params(ts);

    return ts;
}

void tracker_state_destroy(tracker_state_t **tracker)
{
    tracker_state_t *ts = *tracker;
    zsock_destroy(&ts->additions);
    zsock_destroy(&ts->deletions);
    zring_destroy(&ts->uuids);
    *tracker = NULL;
}

static
int server_clean_old_uuids(tracker_state_t *state)
{
    zring_t *uuids = state->uuids;
    uint64_t age_threshold = state->age_threshold_ms;
    int expired = 0;

    uint64_t item;
    while ( (item = (uint64_t)zring_first(uuids)) ) {
        if (item < age_threshold) {
            expired++;
            zring_remove(uuids, (void*)item);
        } else {
            break;
        }
    }

    state->expired += expired;
    return expired;
}

static
int server_add_uuid(zloop_t *loop, zsock_t *socket, void *args)
{
    tracker_state_t *state = args;
    state->added++;
    zmsg_t *msg = zmsg_recv(socket);
    assert(msg);
    char *uuid = zmsg_popstr(msg);
    assert(uuid);
    //fprintf(stdout, "[D] tracker[%zu]: adding uuid: %s\n", state->id, uuid);
    zring_insert(state->uuids, uuid, (void*)state->current_time_ms);
    free(uuid);
    zmsg_destroy(&msg);
    return 0;
}

static
int server_delete_uuid(zloop_t *loop, zsock_t *socket, void *args)
{
    int rc = 0;
    tracker_state_t *state = args;
    state->deleted++;
    server_clean_old_uuids(state);
    zmsg_t *msg = zmsg_recv(socket);
    assert(msg);
    char *uuid = zmsg_popstr(msg);
    assert(uuid);
    if (zring_lookup(state->uuids, uuid)) {
        // fprintf(stdout, "[D] tracker[%zu]: found uuid: %s\n", state->id, uuid);
        rc = 1;
        zring_delete(state->uuids, uuid);
    } else {
        // fprintf(stdout, "[D] tracker[%zu]: did not find uuid: %s\n", state->id, uuid);
    }
    free(uuid);
    zmsg_addmem(msg, &rc, sizeof(rc));
    zmsg_send(&msg, socket);
    return 0;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *args)
{
    tracker_state_t *state = args;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        assert(cmd);
        zmsg_destroy(&msg);
        if (streq(cmd, "$TERM")) {
            // fprintf(stderr, "[D] tracker[%d]: received $TERM command\n", state->id);
            free(cmd);
            return -1;
        } else if (streq(cmd, "tick")) {
            server_clean_old_uuids(state);
            fprintf(stdout, "[I] tracker[%zu]: uuid hash size %zu (added=%zu, deleted=%zu, expired=%zu)\n",
                    state->id, zring_size(state->uuids), state->added, state->deleted, state->expired);
            state->added = 0;
            state->deleted = 0;
            state->expired = 0;
        } else {
            fprintf(stderr, "[E] tracker[%zu]: received unknown actor command: %s\n", state->id, cmd);
        }
        free(cmd);
    }
    return 0;
}

int server_timer_event(zloop_t *loop, int timer_id, void *args)
{
    tracker_state_t* state = (tracker_state_t*)args;
    tracker_state_set_time_params(state);
    return 0;
}

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

    // setup timer to track current time using 10ms resolution
    int timer_id = zloop_timer(loop, 10, 0, server_timer_event, state);
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
    fprintf(stdout, "[I] tracker[%zu]: listening\n", id);
    rc = zloop_start(loop);
    log_zmq_error(rc);

    // shutdown
    fprintf(stdout, "[I] tracker[%zu]: shutting down\n", id);
    zloop_destroy(&loop);
    assert(loop == NULL);
    tracker_state_destroy(&state);
    fprintf(stdout, "[I] tracker[%zu]: terminated\n", id);
}
