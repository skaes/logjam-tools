#ifndef __RABBITMQ_LISTENER_H_INCLUDED__
#define __RABBITMQ_LISTENER_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

// rabbit options
extern char* rabbit_host;
extern char* rabbit_env;
extern int   rabbit_port;

// zactor
extern void rabbitmq_listener(zsock_t *pipe, void* args);

static inline
void assert_x(int rc, const char* error_text)
{
    if (!rc) {
        fprintf(stderr, "[E] Failed assertion: %s\n", error_text);
        assert(0);
    }
}

#ifdef __cplusplus
}
#endif

#endif
