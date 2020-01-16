#include "importer-watchdog.h"

// the watchdog actor aborts the process if does not receive ticks for
// a given number of seconds.

typedef struct {
    size_t id;
    char me[16];
    uint heartbeat_interval;   // heartbeat interval (seconds)
    int initial_credit;        // initial credit
    int credit;                // number of ticks left before we shut down
    bool received_term_cmd;    // whether we have received a TERM command
} watchdog_state_t;

static watchdog_state_t* watchdog_state_new(uint abort_after, uint heartbeat_interval, size_t id)
{
    watchdog_state_t *state = zmalloc(sizeof(watchdog_state_t));
    state->id = id;
    snprintf(state->me, 16, "watchdog[%zu]", id);
    state->heartbeat_interval = heartbeat_interval;
    state->initial_credit = abort_after / heartbeat_interval;
    state->credit = state->initial_credit;
    state->received_term_cmd = false;
    if (debug) {
        printf("%s: abort_after: %d, inital_credit:%d, credit: %d, heartbeat_interval: %d\n",
               state->me, abort_after, state->initial_credit, state->credit, state->heartbeat_interval);
    }
    return state;
}

static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    watchdog_state_t *state = arg;
    state->credit--;
    if (state->credit <= 0) {
        fflush(stdout);
        fprintf(stderr, "[E] watchdog[%zu]: no credit left, aborting process\n", state->id);
        abort();
    } else if (state->credit < state->initial_credit - 1) {
        printf("[I] watchdog[%zu]: credit left: %d\n", state->id, state->credit);
    }
    return 0;
}

static
int actor_command(zloop_t *loop, zsock_t *socket, void *arg)
{
    int rc = 0;
    watchdog_state_t *state = arg;
    zmsg_t *msg = zmsg_recv(socket);
    if (msg) {
        char *cmd = zmsg_popstr(msg);
        if (streq(cmd, "$TERM")) {
            state->received_term_cmd = true;
            // fprintf(stderr, "[D] watchdog[0]: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            if (debug)
                printf("[I] watchdog[%zu]: credit: %d\n", state->id, state->credit);
            state->credit = state->initial_credit;
        } else {
            fprintf(stderr, "[E] watchdog[%zu]: received unknown actor command: %s\n", state->id, cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}

static
void watchdog(zsock_t *pipe, void *args)
{
    int rc;
    watchdog_state_t *state = args;
    size_t id = state->id;
    set_thread_name(state->me);

    // signal readyiness
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);
    // we rely on the controller shutting us down
    zloop_ignore_interrupts(loop);

    // decrease credit every heartbeat interval seconds
    rc = zloop_timer(loop, 1000*state->heartbeat_interval, 0, timer_event, state);
    assert(rc != -1);

    // setup handler for actor messages
    rc = zloop_reader(loop, pipe, actor_command, state);
    assert(rc == 0);

    if (!quiet)
        printf("[I] watchdog[%zu]: starting\n", id);

    // run the loop
    bool should_continue_to_run = getenv("CPUPROFILE") != NULL;
    do {
        rc = zloop_start(loop);
        should_continue_to_run &= errno == EINTR;
        if (!state->received_term_cmd)
            log_zmq_error(rc, __FILE__, __LINE__);
    } while (should_continue_to_run);

    if (!quiet)
        printf("[I] watchdog[%zu]: shutting down\n", id);

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);
    free(state);

    if (!quiet)
        printf("[I] watchdog[%zu]: terminated\n", id);
}

zactor_t* watchdog_new(uint credit, uint heartbeat_interval, size_t id)
{
    watchdog_state_t *state = watchdog_state_new(credit, heartbeat_interval, id);
    return zactor_new(watchdog, state);
}

void watchdog_destroy(zactor_t **watchdog_p)
{
    zactor_destroy(watchdog_p);
}
