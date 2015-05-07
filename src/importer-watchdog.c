#include "importer-watchdog.h"

// the watchdog actor aborts the process if does not receive ticks for
// 10 consecutive ticks

#define CREDIT 10

typedef struct {
    int credit;
} watchdog_state_t;


static int timer_event(zloop_t *loop, int timer_id, void *arg)
{
    watchdog_state_t *state = arg;
    state->credit--;
    if (verbose) {
        printf("[I] watchdog: credit: %d\n", state->credit);
    }
    if (state->credit == 0) {
        fflush(stdout);
        fprintf(stderr, "[E] watchdog: no credit left, aborting process\n");
        abort();
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
            // fprintf(stderr, "[D] watchdog[0]: received $TERM command\n");
            rc = -1;
        }
        else if (streq(cmd, "tick")) {
            if (verbose)
                printf("[I] watchdog: credit: %d\n", state->credit);
            state->credit = CREDIT;
        } else {
            fprintf(stderr, "[E] watchdog[0]: received unknown actor command: %s\n", cmd);
        }
        free(cmd);
        zmsg_destroy(&msg);
    }
    return rc;
}


void watchdog(zsock_t *pipe, void *args)
{
    set_thread_name("watchdog[0]");

    int rc;
    watchdog_state_t state = { .credit = CREDIT };

    // signal readyiness
    zsock_signal(pipe, 0);

    // set up event loop
    zloop_t *loop = zloop_new();
    assert(loop);
    zloop_set_verbose(loop, 0);

    // decrease credit every second
    rc = zloop_timer(loop, 1000, 0, timer_event, &state);
    assert(rc != -1);

    // setup handler for actor messages
    rc = zloop_reader(loop, pipe, actor_command, &state);
    assert(rc == 0);

    // run the loop
    if (!zsys_interrupted) {
        zloop_start(loop);
        if (!quiet)
            printf("[I] watchdog[0]: shutting down\n");
    }

    // shutdown
    zloop_destroy(&loop);
    assert(loop == NULL);

    if (!quiet)
        printf("[I] watchdog[0]: terminated\n");
}
