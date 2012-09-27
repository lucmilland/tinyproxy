#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <poll.h>
#include <zmq.h>

#include "log.h"
#include "hashmap.h"
#include "main.h"
#include "conf.h"
#include "mux.h"

struct mux_context {
  int server;        /* UDP socket to guardserv */
  struct sockaddr_in sockaddr;
  socklen_t socklen;

  void *zmq_context; /* ZMQ context for the following */

  hashmap_t pendings;/* pending calls hashmap */
  int next_pending_id; /* next request id */
  pthread_mutex_t pendings_mutex;

  pthread_t worker_thread;  /* thread listening for queries from tinyproxy */
};
typedef struct mux_context *mux_context_t;

struct pending_req {
  int id;
  struct timeval time;
  char *request;
};

/***************
 * zmq helpers
 ***************/

/*
 * Convert C string to 0MQ string and send to socket
 */
static int
s_send (void *sock, const char *string) {
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
static int
s_sendmore (void *sock, const char *string) {
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
  hints.ai_family = AF_INET;
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

  /* initialize pendings queue */
  pthread_mutex_init(&ctx->pendings_mutex, NULL);
  ctx->pendings = hashmap_create(32);
  ctx->next_pending_id = 17;

  /* init upstream socket */
  serv_addr = get_server_connection(hostname);
  if (!serv_addr) {
    log_message(LOG_ERR, "Can't connect to %s", hostname);
    return NULL;
  }

  /* TODO: select ones of known servers  */
  for (rp = serv_addr; rp != NULL; rp = rp->ai_next) {

    ctx->server = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

    if (ctx->server == -1)
      continue;

    memcpy(&ctx->sockaddr, rp->ai_addr, rp->ai_addrlen);
    ctx->socklen = rp->ai_addrlen;
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

  return ctx;
}

/*
 * Destroy
 */
static void
mux_destroy(mux_context_t ctx) {
  if (ctx->zmq_context)
    zmq_term(ctx->zmq_context);
  free(ctx);
}

static void
dump_pendings(mux_context_t mux) {
  hashmap_iter iter;
  char *key;
  struct pending_req *data;

  for (iter = hashmap_first(mux->pendings);
       !hashmap_is_end(mux->pendings, iter); 
       iter++) {
    hashmap_return_entry(mux->pendings, iter, &key, (void**)&data);
    log_message(LOG_ERR, "pending %s : %p", key, (void*)data);
  }
}

#define ID_MESSAGE     0
#define EMPTY_MESSAGE  1
#define IP_MESSAGE     2
#define HOST_MESSAGE   3
#define IDENT_MESSAGE  4
#define METHOD_MESSAGE 5
#define URL_MESSAGE    6

/*
 * Register request and send it to remote guardserv
 */
static int
queue_request(mux_context_t mux, zmq_msg_t messages[7]) {
  struct pending_req req;
  size_t key_length;
  char key[24];
  char *p;


  pthread_mutex_lock(&mux->pendings_mutex);
  gettimeofday(&req.time, NULL);

  /* build request key as req_id@ID */
  req.id = mux->next_pending_id++;
  key_length = snprintf(key, sizeof(key), "%d@", req.id );
  if ( key_length >= sizeof(key) - zmq_msg_size(&messages[ID_MESSAGE]) ) {
    log_message(LOG_ERR, "key is too long (%s)", key);
    pthread_mutex_unlock(&mux->pendings_mutex);
    return -1;
  }
  memcpy(key + key_length, zmq_msg_data(&messages[ID_MESSAGE]), 
	 zmq_msg_size(&messages[ID_MESSAGE]));
  *(key + key_length + zmq_msg_size(&messages[ID_MESSAGE])) = '\0';
    
  /* build request string of the form : */
  /* KEY URL IP/HOST IDENT METHOD */
  req.request = (char*)malloc(strlen(key) +
			      zmq_msg_size(&messages[ID_MESSAGE]) +
			      zmq_msg_size(&messages[URL_MESSAGE]) +
			      zmq_msg_size(&messages[IP_MESSAGE]) +
			      zmq_msg_size(&messages[HOST_MESSAGE]) +
			      zmq_msg_size(&messages[IDENT_MESSAGE]) +
			      zmq_msg_size(&messages[METHOD_MESSAGE]) +
			      4 + /* 4 whitespaces */
			      1 + /* one slash */
			      1   /* final '\0' character */);
  if (req.request == NULL) {
    log_message(LOG_ERR, "failed to allocate string");
    return -1;
  }
 
  p = req.request;

  memcpy(p, key, strlen(key));
  p += strlen(key);
  *p++ = ' ';

  memcpy(p, zmq_msg_data(&messages[URL_MESSAGE]), zmq_msg_size(&messages[URL_MESSAGE]));
  p += zmq_msg_size(&messages[URL_MESSAGE]);
  *p++ = ' ';

  memcpy(p, zmq_msg_data(&messages[IP_MESSAGE]), zmq_msg_size(&messages[IP_MESSAGE]));
  p += zmq_msg_size(&messages[IP_MESSAGE]);
  *p++ = '/';

  memcpy(p, zmq_msg_data(&messages[HOST_MESSAGE]), zmq_msg_size(&messages[HOST_MESSAGE]));
  p += zmq_msg_size(&messages[HOST_MESSAGE]);
  *p++ = ' ';

  memcpy(p, zmq_msg_data(&messages[IDENT_MESSAGE]), zmq_msg_size(&messages[IDENT_MESSAGE]));
  p += zmq_msg_size(&messages[IDENT_MESSAGE]);
  *p++ = ' ';

  memcpy(p, zmq_msg_data(&messages[METHOD_MESSAGE]), zmq_msg_size(&messages[METHOD_MESSAGE]));
  p += zmq_msg_size(&messages[METHOD_MESSAGE]);
  *p = '\0';

  if ( hashmap_insert(mux->pendings, key,
		      (void*)&req, sizeof(struct pending_req)) < 0 ) {
    log_message(LOG_ERR, "can't insert key (%s)", key);
    pthread_mutex_unlock(&mux->pendings_mutex);
    return -1;
  }
  pthread_mutex_unlock(&mux->pendings_mutex);

  /* fprintf(stderr, "request : %s\n", req.request); */

  /* finaly, send request */
  if ( sendto(mux->server, req.request, strlen(req.request), 0,
	      &mux->sockaddr, mux->socklen) < 0 ) {
    log_message(LOG_ERR, "error occured sending message");
    pthread_mutex_lock(&mux->pendings_mutex);
    free(req.request);
    hashmap_remove(mux->pendings, key);
    pthread_mutex_unlock(&mux->pendings_mutex);
    return -1;
  }
  return 0;
}

/*
 * Handle client request
 */
static int
handle_client(mux_context_t mux, void *clients) {
  zmq_msg_t messages[7];
  int i;

  /* get caller messages */
  for (i=0; i < 7; i++) {
    zmq_msg_init (&messages[i]);
    if (zmq_recv (clients, &messages[i], 0)) {
      log_message(LOG_ERR, "Can't get message part");
      return -1;
    }
  }

  /* queue request */
  queue_request(mux, messages);

  /* clear messages */
  for (i=0; i < 7; i++)
    zmq_msg_close(&messages[i]);

  return 0;
}

/*
 * Handle listener response
 */
static int
handle_response(mux_context_t mux, void *clients) {
  int ret;
  int delta;
  struct pending_req *req;
  struct timeval now;
  char id[250];
  char *buffer;
  char *zmq_sender;

  /* read for a response */
  ret = recv(mux->server, &id, sizeof(id), 0);
  /* fprintf(stderr, "answer : %d bytes : %s\n", ret, id); */

  if (ret < 0) {
    log_message(LOG_ERR, "error while reading response");
    return -1;
  } else if (ret == 0) {
    log_message(LOG_ERR, "peer has closed connection");
    return -1;
  } else if (ret < 3) {
    log_message(LOG_ERR, "response is too short");
    return -1;
  }

  gettimeofday(&now, NULL);

  /* isolate query id */
  buffer = (char*)&id;
  do {
    buffer++;
  } while (*buffer != ' ' && *buffer);
  if (*buffer == ' ') *buffer++ = '\0';

  /* split query id of the form id@zmq_sender */
  zmq_sender = (char*)&id;
  do {
    zmq_sender++;
  } while (*zmq_sender != '@' && *zmq_sender);
  if (*zmq_sender == '@') 
    zmq_sender++;
  else {
    log_message(LOG_ERR, "invalid response ID");
    return -1;
  }

  /* lookup message in pendings */
  pthread_mutex_lock(&mux->pendings_mutex);

  if ( hashmap_entry_by_key(mux->pendings, id, (void **)&req) <= 0 ) {
    /* request is not known : maybe a duplicate answer */
    log_message(LOG_WARNING, "unknown ID in response : [%s]", id);
    pthread_mutex_unlock(&mux->pendings_mutex);
    return 0;
  }
  free(req->request);
  hashmap_remove(mux->pendings, id);
  pthread_mutex_unlock(&mux->pendings_mutex);

  delta = now.tv_sec * 1000000 + now.tv_usec -
    req->time.tv_sec * 1000000 - req->time.tv_usec;
      
  log_message(LOG_INFO, "Request time %f ms. id = [%s], sender = [%s]\n",
	      delta / 1000., id, zmq_sender);

  /* redistribute response to owner */
  s_sendmore(clients, zmq_sender); /* send caller's ID ... */
  s_sendmore(clients, "");
  s_send(clients, buffer);         /* ... then response */

  return 0;
}

/*
 * Worker thread
 */
static void *
worker_task(void *args) {
  mux_context_t mux;
  zmq_pollitem_t pollers[2];
  void *clients;     /* ZMQ socket to tinyproxys (they are clients) */

  mux = (mux_context_t)args;

  /* listen requests from tinyproxy */
  clients = zmq_socket(mux->zmq_context, ZMQ_ROUTER);
  zmq_bind(clients, "ipc:///tmp/http-filter");

  pollers[0].socket = clients;
  pollers[0].fd = 0;
  pollers[0].events = ZMQ_POLLIN;
  pollers[0].revents = 0;

  pollers[1].socket = NULL;
  pollers[1].fd = mux->server;
  pollers[1].events = ZMQ_POLLIN;
  pollers[1].revents = 0;

  while (1) {
    if ( zmq_poll (pollers, 2, -1) < 0 ){
      log_message(LOG_ERR, "error on poll : %s", strerror(errno));
    };
    if (pollers[0].revents & ZMQ_POLLIN) {
      /* a request from tinyproxy client */
      handle_client(mux, clients);
    }
    if ( pollers[1].revents & ZMQ_POLLIN ) {
      /* a response from guardserv */
      handle_response(mux, clients);
    }
  }

  zmq_close(clients);
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
  
  /* create worker thread */
  pthread_create (&mux->worker_thread , NULL, worker_task, (void*)mux);

  return mux;
}

void
remote_filter_mux_kill(void *args) {
  mux_context_t mux;

  mux = (mux_context_t)args;
  pthread_cancel(mux->worker_thread);
  mux_destroy(mux);
}

