#ifndef __RABBITMQ_LISTENER_H_INCLUDED__
#define __RABBITMQ_LISTENER_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

// rabbit options
extern char* rabbit_host;
extern char* rabbit_env;
extern int   rabbit_port;

#define DEFAULT_RABBIT_ENV "development"

// zactor
extern void rabbitmq_listener(zsock_t *pipe, void* args);

#ifdef __cplusplus
}
#endif

#endif
