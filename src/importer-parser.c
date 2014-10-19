#include "importer-common.h"
#include "importer-parser.h"
#include "importer-processor.h"
#include "importer-parser.h"

/*
 * connections: n_w = NUM_WRITERS, n_p = NUM_PARSERS, "[<>^v]" = connect, "o" = bind
 *
 *                            controller
 *                                |
 *                               PIPE
 *              PUSH    PULL      |        PUSH       PULL
 *  subscriber  o----------<  parser(n_p)  >-------------o  request_writer(n_w)
 *                                v
 *                                |
 *                                o
 *                             indexer
*/

// Q: Why do we connect to the writers instead of connecting the writers to the parser?
// A: I think this is upside down, but was maybe caused by dropped requests.
// It might be better to insert a load balancer device between parsers and request writers

static
void connect_multiple(zsock_t* socket, const char* name, int which)
{
    for (int i=0; i<which; i++) {
        // TODO: HACK!
        int rc;
        for (int j=0; j<10; j++) {
            rc = zsock_connect(socket, "inproc://%s-%d", name, i);
            if (rc == 0) break;
            zclock_sleep(100); // ms
        }
        log_zmq_error(rc);
        assert(rc == 0);
    }
}

static
zsock_t* parser_pull_socket_new()
{
    int rc;
    zsock_t *socket = zsock_new(ZMQ_PULL);
    assert(socket);
    // connect socket, taking thread startup time into account
    // TODO: this is a hack. better let controller coordinate this
    for (int i=0; i<10; i++) {
        rc = zsock_connect(socket, "inproc://subscriber");
        if (rc == 0) break;
        zclock_sleep(100);
    }
    log_zmq_error(rc);
    assert(rc == 0);
    return socket;
}

static
zsock_t* parser_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    connect_multiple(socket, "request-writer", NUM_WRITERS);
    return socket;
}

static
zsock_t* parser_indexer_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    int rc = zsock_connect(socket, "inproc://indexer");
    assert (rc == 0);
    return socket;
}

static
time_t valid_database_date(const char *date)
{
    if (strlen(date) < 19) {
        fprintf(stderr, "[E] detected crippled date string: %s\n", date);
        return INVALID_DATE;
    }
    struct tm time;
    memset(&time, 0, sizeof(time));
    // fill in correct TZ and DST info
    localtime_r(&time_last_tick, &time);
    const char* format = date[10] == 'T' ? "%Y-%m-%dT%H:%M:%S" : "%Y-%m-%d %H:%M:%S";
    if (!strptime(date, format, &time)) {
        fprintf(stderr, "[E] could not parse date: %s\n", date);
        return INVALID_DATE;
    }
    time_t res = mktime(&time);

    // char b[100];
    // ctime_r(&time_last_tick, b);
    // puts(b);
    // ctime_r(&res, b);
    // puts(b);

    int drift = abs( difftime (res,time_last_tick) );
    if ( drift > INVALID_MSG_AGE_THRESHOLD) {
        fprintf(stderr, "[E] detected intolerable clock drift: %d seconds\n", drift);
        return INVALID_DATE;
    }
    else
        return res;
}

static
processor_state_t* processor_create(zframe_t* stream_frame, parser_state_t* parser_state, json_object *request)
{
    size_t n = zframe_size(stream_frame);
    char db_name[n+100];
    strcpy(db_name, "logjam-");
    // printf("[D] db_name: %s\n", db_name);

    const char *stream_chars = (char*)zframe_data(stream_frame);
    if (n > 15 && !strncmp("request-stream-", stream_chars, 15)) {
        memcpy(db_name+7, stream_chars+15, n-15);
        db_name[n+7-15] = '-';
        db_name[n+7-14] = '\0';
    } else {
        memcpy(db_name+7, stream_chars, n);
        db_name[n+7] = '-';
        db_name[n+7+1] = '\0';
    }
    // printf("[D] db_name: %s\n", db_name);

    json_object* started_at_value;
    if (!json_object_object_get_ex(request, "started_at", &started_at_value)) {
        fprintf(stderr, "[E] dropped request without started_at date\n");
        return NULL;
    }
    const char *date_str = json_object_get_string(started_at_value);
    if (INVALID_DATE == valid_database_date(date_str)) {
        fprintf(stderr, "[E] dropped request for %s with invalid started_at date: %s\n", db_name, date_str);
        return NULL;
    }
    strncpy(&db_name[n+7+1], date_str, 10);
    db_name[n+7+1+10] = '\0';
    // printf("[D] db_name: %s\n", db_name);

    processor_state_t *p = zhash_lookup(parser_state->processors, db_name);
    if (p == NULL) {
        p = processor_new(db_name);
        if (p) {
            int rc = zhash_insert(parser_state->processors, db_name, p);
            assert(rc ==0);
            zhash_freefn(parser_state->processors, db_name, processor_destroy);
            // send msg to indexer to create db indexes
            zmsg_t *msg = zmsg_new();
            assert(msg);
            zmsg_addstr(msg, db_name);
            zmsg_addmem(msg, &p->stream_info, sizeof(stream_info_t*));
            zmsg_send(&msg, parser_state->indexer_socket);
        }
    }
    return p;
}


static
json_object* parse_json_body(zframe_t *body, json_tokener* tokener)
{
    char* json_data = (char*)zframe_data(body);
    int json_data_len = (int)zframe_size(body);
    json_tokener_reset(tokener);
    json_object *jobj = json_tokener_parse_ex(tokener, json_data, json_data_len);
    enum json_tokener_error jerr = json_tokener_get_error(tokener);
    if (jerr != json_tokener_success) {
        fprintf(stderr, "[E] parse_json_body: %s\n", json_tokener_error_desc(jerr));
    } else {
        // const char *json_str_orig = zframe_strdup(body);
        // printf("[D] %s\n", json_str_orig);
        // free(json_str_orig);
        // dump_json_object(stdout, jobj);
    }
    if (tokener->char_offset < json_data_len) // XXX shouldn't access internal fields
    {
        // Handle extra characters after parsed object as desired.
        fprintf(stderr, "[W] parse_json_body: %s\n", "extranoeus data in message payload");
        my_zframe_fprint(body, "[W] MSGBODY=", stderr);
    }
    // if (strnlen(json_data, json_data_len) < json_data_len) {
    //     fprintf(stderr, "[W] parse_json_body: json payload has null bytes\ndata: %*s\n", json_data_len, json_data);
    //     dump_json_object(stdout, jobj);
    //     return NULL;
    // }
    return jobj;
}

static
void parse_msg_and_forward_interesting_requests(zmsg_t *msg, parser_state_t *parser_state)
{
    // zmsg_dump(msg);
    if (zmsg_size(msg) < 3) {
        fprintf(stderr, "[E] parser received incomplete message\n");
        my_zmsg_fprint(msg, "[E] FRAME=", stderr);
    }
    zframe_t *stream  = zmsg_first(msg);
    zframe_t *topic   = zmsg_next(msg);
    zframe_t *body    = zmsg_next(msg);
    json_object *request = parse_json_body(body, parser_state->tokener);
    if (request != NULL) {
        char *topic_str = (char*) zframe_data(topic);
        int n = zframe_size(topic);
        processor_state_t *processor = processor_create(stream, parser_state, request);

        if (processor == NULL) {
            fprintf(stderr, "[E] could not create processor\n");
            my_zmsg_fprint(msg, "[E] FRAME=", stderr);
            return;
        }
        processor->request_count++;

        if (n >= 4 && !strncmp("logs", topic_str, 4))
            processor_add_request(processor, parser_state, request);
        else if (n >= 10 && !strncmp("javascript", topic_str, 10))
            processor_add_js_exception(processor, parser_state, request);
        else if (n >= 6 && !strncmp("events", topic_str, 6))
            processor_add_event(processor, parser_state, request);
        else if (n >= 13 && !strncmp("frontend.page", topic_str, 13))
            processor_add_frontend_data(processor, parser_state, request);
        else if (n >= 13 && !strncmp("frontend.ajax", topic_str, 13))
            processor_add_ajax_data(processor, parser_state, request);
        else {
            fprintf(stderr, "[W] unknown topic key\n");
            my_zmsg_fprint(msg, "[E] FRAME=", stderr);
        }
        json_object_put(request);
    } else {
        fprintf(stderr, "[E] parse error\n");
        my_zmsg_fprint(msg, "[E] MSGFRAME=", stderr);
    }
}

static
zhash_t* processor_hash_new()
{
    zhash_t *hash = zhash_new();
    assert(hash);
    return hash;
}

static
parser_state_t* parser_state_new(zsock_t *pipe, size_t id)
{
    parser_state_t *state = (parser_state_t *) zmalloc(sizeof(parser_state_t));
    state->id = id;
    state->parsed_msgs_count = 0;
    state->controller_socket = pipe;
    state->pull_socket = parser_pull_socket_new();
    state->push_socket = parser_push_socket_new();
    state->indexer_socket = parser_indexer_socket_new();
    assert( state->tokener = json_tokener_new() );
    state->processors = processor_hash_new();
    return state;
}

static
void parser_state_destroy(parser_state_t **state_p)
{
    parser_state_t *state = *state_p;
    // must not destroy the pipe, as it's owned by the actor
    zsock_destroy(&state->pull_socket);
    zsock_destroy(&state->push_socket);
    zsock_destroy(&state->indexer_socket);
    zhash_destroy(&state->processors);
    *state_p = NULL;
}

void parser(zsock_t *pipe, void *args)
{
    size_t id = (size_t)args;
    parser_state_t *state = parser_state_new(pipe, id);
    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    zpoller_t *poller = zpoller_new(state->controller_socket, state->pull_socket, NULL);
    assert(poller);

    while (!zctx_interrupted) {
        // -1 == block until something is readable
        void *socket = zpoller_wait(poller, -1);
        zmsg_t *msg = NULL;
        if (socket == state->controller_socket) {
            msg = zmsg_recv(state->controller_socket);
            char *cmd = zmsg_popstr(msg);
            zmsg_destroy(&msg);
            if (streq(cmd, "tick")) {
                if (state->parsed_msgs_count)
                    printf("[I] parser [%zu]: tick (%zu messages)\n", state->id, state->parsed_msgs_count);
                zmsg_t *answer = zmsg_new();
                zmsg_addmem(answer, &state->processors, sizeof(zhash_t*));
                zmsg_addmem(answer, &state->parsed_msgs_count, sizeof(size_t));
                zmsg_send(&answer, state->controller_socket);
                state->parsed_msgs_count = 0;
                state->processors = processor_hash_new();
                free(cmd);
            } else if (streq(cmd, "$TERM")) {
                // printf("[D] parser [%zu]: received $TERM command\n", id);
                free(cmd);
                break;
            } else {
                printf("[E] parser [%zu]: received unknnown command: %s\n", id, cmd);
                free(cmd);
                assert(false);
            }
        } else if (socket == state->pull_socket) {
            msg = zmsg_recv(state->pull_socket);
            if (msg != NULL) {
                state->parsed_msgs_count++;
                parse_msg_and_forward_interesting_requests(msg, state);
                zmsg_destroy(&msg);
            }
        } else {
            // msg == NULL, probably interrupted by signal handler
            break;
        }
    }

    printf("[I] parser [%zu]: shutting down\n", id);
    parser_state_destroy(&state);
    printf("[I] parser [%zu]: terminated\n", id);
}
