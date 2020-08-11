#include "importer-common.h"
#include "importer-parser.h"
#include "importer-processor.h"
#include "importer-parser.h"
#include "importer-prometheus-client.h"

/*
 * connections: n_w = num_writers, n_p = num_parsers, "[<>^v]" = connect, "o" = bind
 *
 *                               controller
 *                                  |
 *                                 PIPE
 *              PUSH    PULL        |              PUSH       PULL
 *  subscriber  o----------<    parser(n_p)        >-------------o  request_writer(n_w)
 *                       PUSH v   REQ v v PUSH  v PUSH
 *                            |       | |       |
 *                       PULL o   REP o o PULL  o PULL
 *                      indexer      tracker    prometheus collector
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
        log_zmq_error(rc, __FILE__, __LINE__);
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
    for (int j = 0; j < num_subscribers; j++) {
        for (int i=0; i<10; i++) {
            rc = zsock_connect(socket, "inproc://subscriber-%d", j);
            if (rc == 0) break;
            zclock_sleep(100);
        }
        if (rc) break;
    }
    log_zmq_error(rc, __FILE__, __LINE__);
    assert(rc == 0);
    return socket;
}

static
zsock_t* parser_push_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    zsock_set_sndtimeo(socket, 10);
    zsock_set_sndhwm(socket, HWM_UNLIMITED);
    connect_multiple(socket, "request-writer", num_writers);
    return socket;
}

static
zsock_t* parser_indexer_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    zsock_set_sndtimeo(socket, 10);
    int rc = zsock_connect(socket, "inproc://indexer");
    assert (rc == 0);
    return socket;
}

static
zsock_t* parser_unknown_stream_collector_socket_new()
{
    zsock_t *socket = zsock_new(ZMQ_PUSH);
    assert(socket);
    zsock_set_sndtimeo(socket, 10);
    int rc = zsock_connect(socket, "inproc://unknown-streams-collector");
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

    int drift = abs((int) difftime(res, time_last_tick) );
    if (drift > INVALID_MSG_AGE_THRESHOLD) {
        fprintf(stderr, "[E] detected intolerable clock drift: %d seconds\n", drift);
        return INVALID_DATE;
    }
    else
        return res;
}

static
processor_state_t* processor_create(zmsg_t** msg, zframe_t* stream_frame, parser_state_t* parser_state, json_object *request, bool *known_stream)
{
    // extract stream name onto the stack and add null char
    const char *stream_chars = (char*)zframe_data(stream_frame);
    size_t stream_name_len = zframe_size(stream_frame);
    char stream_name[stream_name_len+1];
    memcpy(stream_name, stream_chars, stream_name_len);
    stream_name[stream_name_len] = '\0';

    // check whether it's a known stream and return NULL if not
    stream_info_t *stream_info = get_stream_info(stream_name, parser_state->stream_info_cache);
    *known_stream = stream_info != NULL;
    if (stream_info == NULL) {
        if (!is_mobile_app(stream_name)) {
            zmsg_send_and_destroy(msg, parser_state->unknown_streams_collector_socket);
        }
        return NULL;
    }
    // printf("[D] found stream info for stream %s: %s\n", stream_name, stream_info->key);

    char db_name[stream_name_len+100];
    strcpy(db_name, "logjam-");
    strcat(db_name+7, stream_name);
    db_name[stream_name_len+7] = '-';
    db_name[stream_name_len+7+1] = '\0';
    // printf("[D] db_name: %s\n", db_name);

    json_object* started_at_value;
    if (!json_object_object_get_ex(request, "started_at", &started_at_value)) {
        fprintf(stderr, "[E] dropped request without started_at date\n");
        release_stream_info(stream_info);
        return NULL;
    }
    const char *date_str = json_object_get_string(started_at_value);
    if (INVALID_DATE == valid_database_date(date_str)) {
        db_name[stream_name_len+7] = '\0';
        json_object* action_object;
        const char* action = NULL;
        if (json_object_object_get_ex(request, "action", &action_object)
            || json_object_object_get_ex(request, "logjam_action", &action_object)
            || json_object_object_get_ex(request, "page", &action_object))
            action = json_object_get_string(action_object);
        fprintf(stderr, "[E] dropped request for %*s with invalid started_at date: %s. action: %s\n", (int)stream_name_len, stream_name, date_str, action);
        release_stream_info(stream_info);
        return NULL;
    }
    strncpy(&db_name[stream_name_len+7+1], date_str, 10);
    db_name[stream_name_len+7+1+10] = '\0';
    // printf("[D] db_name: %s\n", db_name);

    processor_state_t *p = zhash_lookup(parser_state->processors, db_name);
    if (p)
        release_stream_info(stream_info);
    else {
        p = processor_new(stream_info, db_name);
        assert(p);
        int rc = zhash_insert(parser_state->processors, db_name, p);
        assert(rc ==0);
        zhash_freefn(parser_state->processors, db_name, processor_destroy);
        // send msg to indexer to create db indexes
        indexer_ensure_indexes(stream_info, db_name, parser_state->indexer_socket);
    }
    return p;
}

static
void parse_msg_and_forward_interesting_requests(zmsg_t **msgptr, parser_state_t *parser_state)
{
    zmsg_t* msg = *msgptr;
    // zmsg_dump(msg);
    // slow down parser for testing
    // zclock_sleep(100);

    if (zmsg_size(msg) != 4) {
        fprintf(stderr, "[E] parser received incomplete message\n");
        my_zmsg_fprint(msg, "[E] MSG", stderr);
    }
    zframe_t *stream_frame  = zmsg_first(msg);
    zframe_t *topic_frame   = zmsg_next(msg);
    zframe_t *body_frame    = zmsg_next(msg);
    zframe_t *meta_frame    = zmsg_next(msg);

    if (!well_formed_stream_name((const char*)zframe_data(stream_frame), zframe_size(stream_frame))) {
        fprintf(stderr, "[E] parser received malformed stream name\n");
        my_zmsg_fprint(msg, "[E] MSG", stderr);
        return;
    }

    msg_meta_t meta = META_INFO_EMPTY;
    if (!frame_extract_meta_info(meta_frame, &meta)) {
        fprintf(stderr, "[E] parser could not decode meta info\n");
        my_zmsg_fprint(msg, "[E] MSG", stderr);
        return;
    }

    char *body;
    size_t body_len;
    if (meta.compression_method) {
        int rc = decompress_frame(body_frame, meta.compression_method, parser_state->decompression_buffer, &body, &body_len);
        if (!rc) {
            char *app_env = (char*) zframe_data(stream_frame);
            int n = zframe_size(stream_frame);
            const char *method_name = compression_method_to_string(meta.compression_method);
            fprintf(stderr, "[E] parser could not decompress payload from %.*s (%s)\n", n, app_env, method_name);
            dump_meta_info("[E]", &meta);
            my_zmsg_fprint(msg, "[E] MSG", stderr);
            return;
        }
    } else {
        body = (char*) zframe_data(body_frame);
        body_len = zframe_size(body_frame);
    }

    json_object *request = parse_json_data(body, body_len, parser_state->tokener);
    if (request != NULL) {
        // dump_json_object_limiting_log_lines(stdout, "[D] REQUEST", request, 10);
        char *topic_str = (char*) zframe_data(topic_frame);
        int n = zframe_size(topic_frame);
        bool known_stream;
        processor_state_t *processor = processor_create(msgptr, stream_frame, parser_state, request, &known_stream);
        if (processor == NULL) {
            if (known_stream)
                dump_json_object_limiting_log_lines(stderr, "[E] could not create processor for request: ", request, 10);
            json_object_put(request);
            return;
        }
        processor->request_count++;

        if (n >= 4 && !strncmp("logs", topic_str, 4))
            processor_add_request(processor, parser_state, request);
        else if (n >= 10 && !strncmp("javascript", topic_str, 10))
            processor_add_js_exception(processor, parser_state, request);
        else if (n >= 6 && !strncmp("events", topic_str, 6))
            processor_add_event(processor, parser_state, request);
        else if (n >= 13 && !strncmp("frontend.page", topic_str, 13)) {
            parser_state->fe_stats.received++;
            enum fe_msg_drop_reason reason = processor_add_frontend_data(processor, parser_state, request, msg);
            if (reason)
                parser_state->fe_stats.dropped++;
            parser_state->fe_stats.drop_reasons[reason]++;
        } else if (n >= 13 && !strncmp("frontend.ajax", topic_str, 13)) {
            parser_state->fe_stats.received++;
            enum fe_msg_drop_reason reason = processor_add_ajax_data(processor, parser_state, request, msg);
            if (reason)
                parser_state->fe_stats.dropped++;
            parser_state->fe_stats.drop_reasons[reason]++;
        } else if (n >= 6 && !strncmp("mobile", topic_str, 6)) {
            // ignore message for now
        } else {
            fprintf(stderr, "[W] unknown topic key\n");
            my_zmsg_fprint(msg, "[E] MSG", stderr);
        }
        json_object_put(request);
    } else {
        fprintf(stderr, "[E] parse error\n");
        my_zmsg_fprint(msg, "[E] MSG", stderr);
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
parser_state_t* parser_state_new(zconfig_t* config, size_t id)
{
    parser_state_t *state = zmalloc(sizeof(*state));
    state->config = config;
    state->id = id;
    snprintf(state->me, 16, "parser[%zu]", id);
    state->pull_socket = parser_pull_socket_new();
    state->push_socket = parser_push_socket_new();
    state->unknown_streams_collector_socket = parser_unknown_stream_collector_socket_new();
    state->indexer_socket = parser_indexer_socket_new();
    state->tokener = json_tokener_new();
    assert(state->tokener);
    state->processors = processor_hash_new();
    state->stream_info_cache = zhash_new();
    state->tracker = tracker_new();
    state->statsd_client = statsd_client_new(config, state->me);
    state->decompression_buffer = zchunk_new(NULL, INITIAL_DECOMPRESSION_BUFFER_SIZE);
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
    zsock_destroy(&state->unknown_streams_collector_socket);
    zhash_destroy(&state->processors);
    zhash_destroy(&state->stream_info_cache);
    tracker_destroy(&state->tracker);
    statsd_client_destroy(&state->statsd_client);
    zchunk_destroy(&state->decompression_buffer);
    free(state);
    *state_p = NULL;
}

static
void parser(zsock_t *pipe, void *args)
{
    parser_state_t *state = (parser_state_t*)args;
    state->pipe = pipe;
    set_thread_name(state->me);
    size_t id = state->id;

    static uint64_t ticks = 0;

    if (!quiet)
        printf("[I] parser [%zu]: starting\n", id);

    // signal readyiness after sockets have been created
    zsock_signal(pipe, 0);

    zpoller_t *poller = zpoller_new(state->pipe, state->pull_socket, NULL);
    assert(poller);

    while (!zsys_interrupted) {
        // wait at most one second
        void *socket = zpoller_wait(poller, 1000);
        zmsg_t *msg = NULL;
        if (socket == state->pipe) {
            msg = zmsg_recv(state->pipe);
            if (!msg) continue;
            char *cmd = zmsg_popstr(msg);
            zmsg_destroy(&msg);
            if (streq(cmd, "tick")) {
                if (state->parsed_msgs_count && verbose)
                    printf("[I] parser [%zu]: tick (%zu messages, %zu frontend)\n", id, state->parsed_msgs_count, state->fe_stats.received);
                statsd_client_count(state->statsd_client, "importer.parses.count", state->parsed_msgs_count);
                importer_prometheus_client_count_msgs_parsed(state->parsed_msgs_count);
                importer_prometheus_client_record_rusage_parser(state->id);
                zmsg_t *answer = zmsg_new();
                zmsg_addptr(answer, state->processors);
                zmsg_addmem(answer, &state->parsed_msgs_count, sizeof(state->parsed_msgs_count));
                zmsg_addmem(answer, &state->fe_stats, sizeof(state->fe_stats));
                zmsg_send_with_retry(&answer, state->pipe);
                state->parsed_msgs_count = 0;
                memset(&state->fe_stats, 0, sizeof(state->fe_stats));
                state->processors = processor_hash_new();
                if (++ticks % 60 == 0) {
                    zhash_destroy(&state->stream_info_cache);
                    state->stream_info_cache = zhash_new();
                }
                free(cmd);
            } else if (streq(cmd, "$TERM")) {
                // printf("[D] parser [%zu]: received $TERM command\n", id);
                free(cmd);
                break;
            } else {
                printf("[E] parser [%zu]: received unknown command: %s\n", id, cmd);
                free(cmd);
                assert(false);
            }
        } else if (socket == state->pull_socket) {
            msg = zmsg_recv(state->pull_socket);
            if (msg != NULL) {
                state->parsed_msgs_count++;
                parse_msg_and_forward_interesting_requests(&msg, state);
                zmsg_destroy(&msg);
            } else {
                // msg == NULL, probably interrupted by signal handler
                break;
            }
        } else if (socket) {
            // if socket is not null, something is horribly broken
            printf("[E] parser [%zu]: broken poller. committing suicide.\n", id);
            assert(false);
        } else {
            // probably interrupted by signal handler
            // if so, loop will terminate on condition !zsys_interrupted
        }
    }

    if (!quiet)
        printf("[I] parser [%zu]: shutting down\n", id);

    parser_state_destroy(&state);

    if (!quiet)
        printf("[I] parser [%zu]: terminated\n", id);
}

zactor_t* parser_new(zconfig_t *config, size_t id)
{
    parser_state_t *state = parser_state_new(config, id);
    return zactor_new(parser, state);
}

void parser_destroy(zactor_t **parser_p)
{
    zactor_destroy(parser_p);
}
