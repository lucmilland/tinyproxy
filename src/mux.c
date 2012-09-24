#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <zmq.h>

#include "log.h"
#include "main.h"
#include "conf.h"
#include "mux.h"

struct mux_context {
  int server;        /* UDP socket to guardserv */
  void *zmq_context; /* ZMQ context for the following :*/
  void *clients;     /* ZMQ socket to tinyproxys (they are clients) */
};
typedef struct mux_context *mux_context_t;

static pthread_t listener; /* thread listening response from guardserv */
static pthread_t worker;   /* thread listening queries from tinyproxy */

/***************
 * zmq helpers
 ***************/

/*  Receive 0MQ string from socket and convert into C string
 *  Caller must free returned string. Returns NULL if the context
 *  is being terminated.
 */
static char *
s_recv (void *zsocket) {
  zmq_msg_t message;
  int size;
  char *string;

  zmq_msg_init (&message);
  if (zmq_recv (zsocket, &message, 0))
    return (NULL);
  size = zmq_msg_size (&message);
  string = (char *)malloc (size + 1);
  memcpy (string, zmq_msg_data (&message), size);
  zmq_msg_close (&message);
  string [size] = 0;
  return (string);
}

/*
 * Convert C string to 0MQ string and send to socket
 */
static int s_send (void *sock, const char *string) {
    int rc;
    zmq_msg_t message;
    zmq_msg_init_size (&message, strlen (string));
    memcpy (zmq_msg_data (&message), string, strlen (string));
    rc = zmq_send (sock, &message, 0);
    zmq_msg_close (&message);
    return (rc);
}

/*
 * Sends string as 0MQ string, as multipart non-terminal
 */
static int s_sendmore (void *sock, const char *string) {
    int rc;
    zmq_msg_t message;
    zmq_msg_init_size (&message, strlen (string));
    memcpy (zmq_msg_data (&message), string, strlen (string));
    rc = zmq_send (sock, &message, ZMQ_SNDMORE);
    zmq_msg_close (&message);
    return (rc);
}

/*
 * Resolve service hostname
 */
static struct addrinfo *
get_server_connection(const char *hostname) {
  struct addrinfo hints, *result;
  int s;
  
  memset(&hints, 0, sizeof(struct addrinfo));
  /* hints.ai_family = AF_UNSPEC;*/    /* Allow IPv4 or IPv6 */
  hints.ai_family = AF_INET;    /* Allow IPv4 or IPv6 */
  hints.ai_socktype = SOCK_DGRAM; /* Datagram socket */
  hints.ai_flags = 0;
  hints.ai_protocol = 0;          /* Any protocol */

  s = getaddrinfo(hostname, "http-filter", &hints, &result);
  if (s != 0) {
    log_message(LOG_ERR, gai_strerror(s));
    return NULL;
  }

  return result;
}

/*
 * Init a new context
 * Return NULL on error
 */
static mux_context_t
mux_init(char *hostname) {
  mux_context_t ctx;
  struct addrinfo *rp, *serv_addr;

  ctx = (mux_context_t)malloc(sizeof(struct mux_context));
  if (!ctx) {
    log_message(LOG_ERR, "Can't allocate context");
    return NULL;
  }

  /* init upstream socket */

  serv_addr = get_server_connection(hostname);
  if (!serv_addr) {
    /* error */
    return NULL;
  }

  for (rp = serv_addr; rp != NULL; rp = rp->ai_next) {

    ctx->server = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

    if (ctx->server == -1)
      continue;

    if (connect(ctx->server, rp->ai_addr, rp->ai_addrlen) != -1) 
      break;

    close(ctx->server);
  }
  if (rp == NULL) {
    log_message(LOG_ERR, "Can't create socket");
    return NULL;
  }
  freeaddrinfo(serv_addr);

  /* init zmq sockets */
  ctx->zmq_context = zmq_init(1);

  /* listen requests from tinyproxy */
  ctx->clients = zmq_socket(ctx->zmq_context, ZMQ_ROUTER);
  zmq_bind (ctx->clients, "ipc:///tmp/http-filter");

  return ctx;
}

/*
 * Destroy
 */
static void
mux_destroy(mux_context_t ctx) {
  if (ctx->clients)
    zmq_close(ctx->clients);
  if (ctx->zmq_context)
    zmq_term(ctx->zmq_context);
  free(ctx);
}

/*
 * response listener thread
 */
static void *
listener_thread(void *args) {
  int ret;
  char id[1024];
  char *buffer;
  mux_context_t mux;  
  zmq_msg_t id_message;

  mux = (mux_context_t)args;

  while (1) {
    /* wait for a response */
    ret = recv(mux->server, &id, sizeof(id), 0);
    /* fprintf(stderr, "answer : %d bytes : %s\n", ret, id); */

    if (ret < 0) {
      log_message(LOG_ERR, "error while reading response");
      break;
    } else if (ret == 0) {
      log_message(LOG_ERR, "peer has closed connection");
      break;
    } else if (ret < 3) {
      log_message(LOG_ERR, "response is too short");
      break;
    }

    buffer = (char*)&id;
    do {
      buffer++;
    } while (*buffer != ' ' && *buffer);
    if (*buffer == ' ') *buffer++ = '\0';

    /* redistribute response to owner */

    /* re-send caller's ID ... */
    zmq_msg_init_data (&id_message, (void*)&id, strlen(id), NULL, NULL);
    zmq_send(mux->clients, &id_message, ZMQ_SNDMORE);
    s_sendmore(mux->clients, "");
    /* ... then response */
    s_send(mux->clients, buffer);

  }
  return NULL;
}

/*
 * Worker thread
 */
static void *
worker_task(void *args) {
  mux_context_t mux;
  char *ip_address, *string_address, *ident, *method, *url, *empty;
  zmq_msg_t id_message;
  char *request;

  mux = (mux_context_t)args;

  while (1) {

    /* get caller's ID */
    zmq_msg_init (&id_message);
    if (zmq_recv (mux->clients, &id_message, 0)) {
      log_message(LOG_ERR, "Can't get caller's ID");
      break;
    }

    empty = s_recv(mux->clients);
    assert (*empty == '\0');

    ip_address = s_recv(mux->clients);
    string_address = s_recv(mux->clients);
    ident = s_recv(mux->clients);
    method = s_recv(mux->clients);
    url = s_recv(mux->clients);

    if (asprintf(&request, "%s %s %s/%s %s %s", (char*)zmq_msg_data(&id_message),
		 url, ip_address, string_address, ident, method) < 0) {
      log_message(LOG_ERR, "failed to allocate string");
      break;
    }
    /* fprintf(stderr, "request : %s\n", request); */
    free(empty);
    free(ip_address);
    free(string_address);
    free(ident);
    free(method);
    free(url);
    zmq_msg_close (&id_message);

    /* queue request */
    if (send(mux->server, request, strlen(request), 0) < 0) {
      log_message(LOG_ERR, "error occured sending message");
      break;
    }
    free(request);
  }

  pthread_exit(0);
}

void *
remote_filter_mux_init(char *hostname) {
  mux_context_t mux;

  mux = mux_init(hostname);
  if (!mux) {
    log_message(LOG_ERR, "Can't init");
    return NULL;
  }
  pthread_create (&listener, NULL, listener_thread, (void*)mux);
  pthread_create (&worker , NULL, worker_task, (void*)mux);
  return mux;
}

void
remote_filter_mux_kill(void *mux) {
  pthread_cancel(listener);
  pthread_cancel(worker);
  mux_destroy((mux_context_t)mux);
}

