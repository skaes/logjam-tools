#include "importer-adder.h"
#include "importer-increments.h"
#include "importer-parser.h"
#include "importer-adder.h"
#include "importer-processor.h"
#include "importer-resources.h"

/*
 * connections: n_w = num_writers, n_p = num_parsers, "o" = bind, "[<>v^]" = connect
 *
 *                            controller
 *                                |
 *                               PIPE
 *                  REQ     REP   |
 *      controller  o---------< adder
 */

// Adder is a simple agent which merges parser results. It connects to
// an inproc REP socket on which it receives requests to merge a list
// of parser results and sends the merged data back.

static
zsock_t* adder_reply_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_REP);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://adders");
    log_zmq_error(rc, __FILE__, __LINE__);
    assert(rc == 0);
    return socket;
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
void merge_agents(zhash_t* target, zhash_t *source)
{
    user_agent_stats_t *source_agent_stats;

    while ( (source_agent_stats = zhash_first(source)) ) {
        const char* agent = zhash_cursor(source);
        assert(agent);
        user_agent_stats_t *dest_agent_stats = zhash_lookup(target, agent);
        if (dest_agent_stats) {
            dest_agent_stats->received_backend += source_agent_stats->received_backend;
            dest_agent_stats->received_frontend += source_agent_stats->received_frontend;
            dest_agent_stats->fe_dropped += source_agent_stats->fe_dropped;
            for (int i=0; i<FE_MSG_NUM_REASONS; i++)
                dest_agent_stats->fe_drop_reasons[i] += source_agent_stats->fe_drop_reasons[i];
        } else {
            zhash_insert(target, agent, source_agent_stats);
            zhash_freefn(target, agent, free);
            zhash_freefn(source, agent, NULL);
        }
        zhash_delete(source, agent);
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
            merge_agents(dest_processor->agents, source_processor->agents);
        } else {
            zhash_insert(target, db_name, source_processor);
            zhash_freefn(target, db_name, processor_destroy);
            zhash_freefn(source, db_name, NULL);
        }
        zhash_delete(source, db_name);
    }
}

static
zmsg_t* handle_addition(zmsg_t* msg)
{
    zhash_t *source = zmsg_popptr(msg);
    zhash_t *target = zmsg_popptr(msg);
    merge_processors(target, source);
    zhash_destroy(&source);
    zmsg_t *result = zmsg_new();
    assert(result);
    zmsg_addptr(result, target);
    return result;
}

void adder(zsock_t *pipe, void *args)
{
    size_t id = (size_t) args;
    char thread_name[16];
    memset(thread_name, 0, 16);
    snprintf(thread_name, 16, "adder[%zu]", id);
    set_thread_name(thread_name);

    if (!quiet)
        printf("[I] adder[%zu]: starting\n", id);

    zsock_t *additions = adder_reply_socket_new();

    // signal readyiness
    zsock_signal(pipe, 0);

    zpoller_t *poller = zpoller_new(pipe, additions, NULL);
    assert(poller);

    while (!zsys_interrupted) {
        // printf("adder[%zu]: polling\n", id);
        // wait at most one second
        void *socket = zpoller_wait(poller, 1000);
        zmsg_t *msg = NULL;
        if (socket == additions) {
            msg = zmsg_recv(additions);
            zmsg_t *answer = handle_addition(msg);
            int rc = zmsg_send_and_destroy(&answer, additions);
            assert(rc == 0);
        }
        else if (socket == pipe) {
            msg = zmsg_recv(socket);
            char *cmd = zmsg_popstr(msg);
            if (streq(cmd, "$TERM")) {
                // printf("[D] adder[%zu]: received $TERM command\n", id);
                zmsg_destroy(&msg);
                free(cmd);
                break;
            } else {
                printf("[E] adder[%zu]: received unknown command: %s\n", id, cmd);
                assert(false);
            }
        }
        else if (socket) {
            // if socket is not null, something is horribly broken
            printf("[E] adder[%zu]: broken poller. committing suicide.\n", id);
            assert(false);
        }
        else {
            // probably interrupted by signal handler
            // if so, loop will terminate on condition !zsys_interrupted
        }
        zmsg_destroy(&msg);
    }

    if (!quiet)
        printf("[I] adder[%zu]: shutting down\n", id);

    zsock_destroy(&additions);

    if (!quiet)
        printf("[I] adder[%zu]: terminated\n", id);
}
