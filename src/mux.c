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
#include <zmq.h>

#define WORKERS 1

struct mux_context {
  int server;
  void *clients;
  void *zmq_context;  
};
typedef struct mux_context *mux_context_t;

/* prototypes */
mux_context_t mux_init(void);

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
    perror( gai_strerror(s) );
    return NULL;
  }

  return result;
}

/*
 * Init a new context
 * Return NULL on error
 */
mux_context_t
mux_init() {
  mux_context_t ctx;
  struct addrinfo *rp, *serv_addr;

  ctx = (mux_context_t)malloc(sizeof(struct mux_context));
  if (!ctx) {
    perror("Can't allocate context");
    return NULL;
  }

  /* init upstream socket */

  serv_addr = get_server_connection("localhost");
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
    perror("Can't create socket");
    return NULL;
  }
  freeaddrinfo(serv_addr);

  /* init zmq */
  ctx->zmq_context = zmq_init(1);
  ctx->clients = zmq_socket(ctx->zmq_context, ZMQ_ROUTER);
  zmq_bind (ctx->clients, "ipc:///var/run/http-filter");

  return ctx;
}

/*
 * Destroy
 */
void
mux_destroy(mux_context_t ctx) {
  if (ctx->clients)
    zmq_close(ctx->clients);
  if (ctx->zmq_context)
    zmq_term(ctx->zmq_context);
  free(ctx);
}

/*
 * Reset context and connections
 */
mux_context_t
mux_reset(mux_context_t ctx) {
  mux_destroy(ctx);
  return mux_init();
}

/*
 * Queue request
 */
static char *
queue_request(mux_context_t mux, char *request) {
  int ret;
  char buffer[1024];

  if (send(mux->server, request, strlen(request), 0) < 0) {
    perror("error occured sending message");
  }
  free(request);

  /* wait for response */
  ret = recv(mux->server, buffer, sizeof(buffer), 0);
  if (ret < 0) {
    perror("error while reading response");
    return NULL;
  } else if (ret == 0) {
    perror("peer has closed connection");
    return NULL;
  }
  return strdup(buffer);
}

/*
 * Worker thread
 */
static void *
worker_task(void *args) {
  mux_context_t mux;
  char *ip_address, *string_address, *ident, *method, *url, *empty;
  zmq_msg_t id_message;
  char *request, *response;

  mux = mux_init();
  if (!mux) {
    perror("Can't init");
    return NULL;
  }

  while (1) {
    /* get caller's ID */
    zmq_msg_init (&id_message);
    if (zmq_recv (mux->clients, &id_message, 0)) {
      perror("Can't get caller's ID");
      break;
    }

    empty = s_recv(mux->clients);
    assert (*empty == '\0');

    ip_address = s_recv(mux->clients);
    string_address = s_recv(mux->clients);
    ident = s_recv(mux->clients);
    method = s_recv(mux->clients);
    url = s_recv(mux->clients);

    if (asprintf(&request, "%s %s/%s %s %s", 
		 url, ip_address, string_address, ident, method) < 0) {
      perror("failed to allocate string");
      break;
    }

    free(empty);
    free(ip_address);
    free(string_address);
    free(ident);
    free(method);
    free(url);

    /* queue request */
    response = queue_request(mux, request);

    if (response == NULL) {
      perror("Error on request");
    }
    /* re-send caller's ID */
    zmq_send(mux->clients, &id_message, ZMQ_SNDMORE);
    s_sendmore(mux->clients, "");

    s_send(mux->clients, response);

    zmq_msg_close (&id_message);
    if (response)
      free(response);
  }
}

/*
 * Main
 */
int
main() {
  pthread_t worker[WORKERS];

  /*for (i = 0; i < WORKERS; i++) {
  #  pthread_create (&(worker[i]), NULL, worker_task, NULL);
  }*/
  worker_task(NULL);
  /*getchar();*/
  return 0;
}
