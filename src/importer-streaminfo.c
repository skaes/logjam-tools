#include "importer-streaminfo.h"
#include "importer-mongoutils.h"

int global_total_time_import_threshold = 0;
const char* global_ignored_request_prefix = NULL;

// all configured streams
zhash_t *configured_streams = NULL;
// all streams we want to subscribe to
zhash_t *stream_subscriptions = NULL;


zlist_t* get_stream_settings(zconfig_t* config, stream_info_t *info, const char* name)
{
    zconfig_t *setting;
    char key[528] = {'0'};

    zlist_t *settings = zlist_new();
    sprintf(key, "backend/streams/%s/%s", info->key, name);
    setting = zconfig_locate(config, key);
    if (setting)
        zlist_push(settings, setting);

    sprintf(key, "backend/defaults/environments/%s/%s", info->env, name);
    setting = zconfig_locate(config, key);
    if (setting)
        zlist_push(settings, setting);

    sprintf(key, "backend/defaults/applications/%s/%s", info->app, name);
    setting = zconfig_locate(config, key);
    if (setting)
        zlist_push(settings, setting);

    sprintf(key, "backend/defaults/%s", name);
    setting = zconfig_locate(config, key);
    if (setting)
        zlist_push(settings, setting);

    return settings;
}

void add_threshold_settings(zconfig_t* config, stream_info_t* info)
{
    info->import_threshold = global_total_time_import_threshold;
    zlist_t *settings = get_stream_settings(config, info, "import_threshold");
    zconfig_t *setting = zlist_first(settings);
    zhash_t *module_settings = zhash_new();
    while (setting) {
        info->import_threshold = atoi(zconfig_value(setting));
        zconfig_t *module_setting = zconfig_child(setting);
        while (module_setting) {
            char *module_name = zconfig_name(module_setting);
            size_t threshold_value = atoi(zconfig_value(module_setting));
            zhash_update(module_settings, module_name, (void*)threshold_value);
            module_setting = zconfig_next(module_setting);
        }
        setting = zlist_next(settings);
    }
    zlist_destroy(&settings);
    int n = zhash_size(module_settings);
    info->module_threshold_count = n;
    info->module_thresholds = malloc(n * sizeof(module_threshold_t));
    zlist_t *modules = zhash_keys(module_settings);
    int i = 0;
    const char *module = zlist_first(modules);
    while (module) {
        info->module_thresholds[i].name = strdup(module);
        info->module_thresholds[i].value = (size_t)zhash_lookup(module_settings, module);
        i++;
        module = zlist_next(modules);
    }
    zlist_destroy(&modules);
    zhash_destroy(&module_settings);
}

void add_ignored_request_settings(zconfig_t* config, stream_info_t* info)
{
    info->ignored_request_prefix = global_ignored_request_prefix;
    zlist_t* settings = get_stream_settings(config, info, "ignored_request_uri");
    zconfig_t *setting = zlist_first(settings);
    while (setting) {
        info->ignored_request_prefix = zconfig_value(setting);
        setting = zlist_next(settings);
    }
    zlist_destroy(&settings);
}

stream_info_t* stream_info_new(zconfig_t *config, zconfig_t *stream_config)
{
    stream_info_t *info = malloc(sizeof(stream_info_t));
    info->key = zconfig_name(stream_config);
    if (!strncmp(info->key, "request-stream-", 15)) {
        info->key += 15;
    }
    info->key_len = strlen(info->key);

    char app[256] = {'\0'};
    char env[256] = {'\0'};;
    int n = sscanf(info->key, "%[^-]-%[^-]", app, env);
    assert(n == 2);

    info->app = strdup(app);
    info->app_len = strlen(app);
    assert(info->app_len > 0);

    info->env = strdup(env);
    info->env_len = strlen(env);
    assert(info->env_len > 0);

    info->db = 0;
    zconfig_t *db_setting = zconfig_locate(stream_config, "db");
    if (db_setting) {
        const char* dbval = zconfig_value(db_setting);
        int db_num = atoi(dbval);
        // printf("[D] db for %s-%s: %d (numdbs: %zu)\n", info->app, info->env, db_num, num_databases);
        assert(db_num < num_databases);
        info->db = db_num;
    }
    add_threshold_settings(config, info);
    add_ignored_request_settings(config, info);

    return info;
}

void dump_stream_info(stream_info_t *stream)
{
    printf("[D] key: %s\n", stream->key);
    printf("[D] app: %s\n", stream->app);
    printf("[D] env: %s\n", stream->env);
    printf("[D] ignored_request_uri: %s\n", stream->ignored_request_prefix);
    printf("[D] import_threshold: %d\n", stream->import_threshold);
    for (int i = 0; i<stream->module_threshold_count; i++) {
        printf("[D] module_threshold: %s = %zu\n", stream->module_thresholds[i].name, stream->module_thresholds[i].value);
    }
}

void setup_stream_config(zconfig_t *config, const char *pattern)
{
    bool have_subscription_pattern = strcmp("", pattern);

    zconfig_t *import_threshold_config = zconfig_locate(config, "backend/defaults/import_threshold");
    if (import_threshold_config) {
        int t = atoi(zconfig_value(import_threshold_config));
        // printf("[D] setting global import threshold: %d\n", t);
        global_total_time_import_threshold = t;
    }

    zconfig_t *ignored_requests_config = zconfig_locate(config, "backend/defaults/ignored_request_uri");
    if (ignored_requests_config) {
        const char *prefix = zconfig_value(ignored_requests_config);
        // printf("[D] setting global ignored_requests uri: %s\n", prefix);
        global_ignored_request_prefix = prefix;
    }

    configured_streams = zhash_new();
    stream_subscriptions = zhash_new();

    zconfig_t *all_streams = zconfig_locate(config, "backend/streams");
    assert(all_streams);
    zconfig_t *stream = zconfig_child(all_streams);
    assert(stream);

    do {
        stream_info_t *stream_info = stream_info_new(config, stream);
        const char *key = stream_info->key;
        // dump_stream_info(stream_info);
        zhash_insert(configured_streams, key, stream_info);
        if (have_subscription_pattern && strstr(key, pattern) != NULL) {
            int rc = zhash_insert(stream_subscriptions, key, stream_info);
            assert(rc == 0);
        }
        stream = zconfig_next(stream);
    } while (stream);
}
