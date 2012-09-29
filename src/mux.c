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

/* Max UDP message length. Low to avoid fragmentation and accomodate 
   various encapsulations */
#define MAX_MTU 1400

struct pending_req {
  uint32_t id;
  uint32_t seq;             /* times this request was emitted */
  struct timeval time;      /* request time */
  int len;                  /* current length of buffer */
  char *buffer;
  int count;                /* number of requests in buffer */
};

/* pendings hashmap is cleaned every QUEUE_CLEANUP_PERIOD milliseconds */
#define QUEUE_CLEANUP_PERIOD 100

struct mux_context {
  int server;        /* UDP socket to guardserv */
  struct sockaddr_in sockaddr;
  socklen_t socklen;
  void *zmq_context; /* ZMQ context for the following */
  hashmap_t pendings;/* pending calls hashmap */
  pthread_t worker_thread;  /* thread listening for queries from tinyproxy */
  struct pending_req request; /* current request beeing built */
};
typedef struct mux_context *mux_context_t;

/* forward declarations */
static int request_init(struct pending_req *req, int id);


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
  ctx->pendings = hashmap_create(32);

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

  /* init request queues */
  request_init(&ctx->request, 17);

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

/*
 * clean-up pending calls, re-emitting old ones, cancelling really too old ones
 */
static int
cleanup_pendings(mux_context_t mux, struct timeval *now) {
  hashmap_iter iter;
  char *key;
  struct pending_req *req;
  int timeout;

  for (iter = hashmap_first(mux->pendings);
       !hashmap_is_end(mux->pendings, iter); 
       iter++) {
    hashmap_return_entry(mux->pendings, iter, &key, (void**)&req);

    if (req->seq == 2) {
      /* this request may last long : realloc to save space */
      req->buffer = (char*)realloc((void*)req->buffer, req->len);
    }

    if (req->seq <= 6) {
      /* timeout schedule is 20ms, 40ms, 80ms, ..., 1280ms, ... */
      timeout = 10 * (2 << req->seq) * 1000;
    } else {
      /* ..., 2048ms, 4096ms, ... , 65536ms */
      timeout = (2 << req->seq) * 1000;
    }

    if (now->tv_sec * 1000000 + now->tv_usec -
	req->time.tv_sec * 1000000 - req->time.tv_usec > timeout) {
      log_message(LOG_INFO, "timeout, seq=%d", req->seq);

      req->seq++;
      if (req->seq >= 16) {
	/* enough is enough : cancel request */
	free(req->buffer);
	hashmap_remove(mux->pendings, key);
      } else {
	/* re-emit */
	sendto(mux->server, req->buffer, req->len, 0,
	       &mux->sockaddr, mux->socklen);
      }
    }
  }
  return 0;
}

/*
 * initialize a multi-part request
 */
static int
request_init(struct pending_req *req, int id) {
  req->id = id;
  req->seq = 0;  
  req->len = 0;
  req->buffer = (char*)malloc(MAX_MTU);
  *(uint32_t*)(req->buffer) = htonl(id);
  req->len += sizeof(uint32_t);
  req->count = 0;
  return 0;
}

/*
 * flush current request and increment request id
 */
static int
request_flush(mux_context_t mux) {
  char key[10];

  if (mux->request.count == 0) {
    /* nothing to flush */
    return 0;
  }

  /* insert request in pendings */
  snprintf(key, sizeof(key), "%d", mux->request.id);
  if ( hashmap_insert(mux->pendings, key,
		      (void*)&mux->request, sizeof(struct pending_req)) < 0 ) {
    log_message(LOG_ERR, "can't insert key (%s)", key);
    return -1;
  }

  /* send request to guardserv */
  sendto(mux->server, mux->request.buffer, mux->request.len, 0,
	 &mux->sockaddr, mux->socklen);

  /* init new request */
  request_init(&mux->request, mux->request.id + 1);
  return 0;
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
queue_request(mux_context_t mux, zmq_msg_t messages[7], struct timeval *now) {
  char *p;
  int len;


  /* build request string of the form : */
  /* ID URL IP/HOST IDENT METHOD */
  len = (zmq_msg_size(&messages[ID_MESSAGE]) +
	 zmq_msg_size(&messages[URL_MESSAGE]) +
	 zmq_msg_size(&messages[IP_MESSAGE]) +
	 zmq_msg_size(&messages[HOST_MESSAGE]) +
	 zmq_msg_size(&messages[IDENT_MESSAGE]) +
	 zmq_msg_size(&messages[METHOD_MESSAGE]) +
	 4 + /* 4 whitespaces */
	 1 + /* one slash */
	 1   /* final '\0' character */);

  if (len >= MAX_MTU) {
    log_message(LOG_ERR, "request too big");
    return -1;
  }

  /* flush queue if necessary */
  if (mux->request.len + len >= MAX_MTU) {
    if (request_flush(mux) < 0) {
      return -1;
    }
  }

  /* init time on first request */
  if (mux->request.count == 0)
    memcpy(&mux->request.time, now, sizeof(struct timeval));

  /* copy fields */
  p = mux->request.buffer + mux->request.len;
 
  memcpy(p, zmq_msg_data(&messages[ID_MESSAGE]), zmq_msg_size(&messages[ID_MESSAGE]));
  p += zmq_msg_size(&messages[ID_MESSAGE]);
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

  /* update new buffer length and request count */
  mux->request.len += len;
  mux->request.count ++;

  return 0;
}

/*
 * Handle client request
 */
static int
handle_client(mux_context_t mux, void *clients, struct timeval *now) {
  zmq_msg_t messages[7];
  int i;
  int ret;

  for ( ;; ) {

    /* receive message id */
    zmq_msg_init (&messages[0]);
    ret = zmq_recv (clients, &messages[0], ZMQ_NOBLOCK);
    if (ret == -1) {
      if (errno == EAGAIN) {
	/* exhausted receive queue : proceed. */
	break;
      } else {
	log_message(LOG_ERR, "Can't read from zsocket : %p", strerror(errno));
	return -1;
      }
    }

    /* get next parts of caller message */
    for (i=1; i < 7; i++) {
      zmq_msg_init (&messages[i]);
      if (zmq_recv (clients, &messages[i], 0)) {
	log_message(LOG_ERR, "Can't get message part");
	for ( ; i > 0; i--) zmq_msg_close(&messages[i]);
	return -1;
      }
    }

    /* queue request */
    ret = queue_request(mux, messages, now);

    /* clear messages */
    for (i=0; i < 7; i++)
      zmq_msg_close(&messages[i]);

    if (ret == -1) {
      /* an error occured */
      return -1;
    }
  }

  return 0;
}

/*
 * Handle listener response
 */
static int
handle_response(mux_context_t mux, void *clients, struct timeval *now) {
  int len;
  int delta;
  struct pending_req *req;
  char buffer[MAX_MTU];
  char *id;
  char *response;
  char *zmq_sender;

  /* read for a response */
  len = recv(mux->server, &buffer, sizeof(buffer), 0);
  /* fprintf(stderr, "answer : %d bytes : [%s]\n", len, buffer); */

  if (len < 0) {
    log_message(LOG_ERR, "error while reading response");
    return -1;
  } else if (len == 0) {
    log_message(LOG_ERR, "peer has closed connection");
    return -1;
  } else if (len < 3) {
    log_message(LOG_ERR, "response is too short");
    return -1;
  }

  /* isolate query id */
  id = (char*)&buffer;
  response = (char*)&buffer;
  do {
    response++; len--;
  } while (*response != ' ' && *response);
  if (*response == ' ') {
    *response++ = '\0';
    len--;
  }

  /* lookup message in pendings */
  if ( hashmap_entry_by_key(mux->pendings, id, (void **)&req) <= 0 ) {
    /* request is not known : maybe a duplicate answer */
    log_message(LOG_INFO, "unknown ID in response : [%s]", id);
    return 0;
  }
  /* remove message from pendings */
  free(req->buffer);
  hashmap_remove(mux->pendings, id);
  
  delta = now->tv_sec * 1000000 + now->tv_usec -
    req->time.tv_sec * 1000000 - req->time.tv_usec;
  log_message(LOG_INFO, "Request time %f ms. id = [%s]",
	      delta / 1000., id);

  /* split queries of the form :
     ZMQ_SENDER RESPONSE
  */
  while (len > 0) {
    zmq_sender = response;
    do {
      response++; len--;
    } while (*response != ' ' && *response);
    if (*response == ' ') {
      *response++ = '\0';
      len --;
    } else if (*response != '\0') {
      log_message(LOG_ERR, "invalid response ID in [%s], len=%d", zmq_sender, len);
      return -1;
    }

    /* redistribute response to owner */
    s_sendmore(clients, zmq_sender);   /* send caller's ID ... */
    s_sendmore(clients, "");
    s_send(clients, response);         /* ... then response */

    /* move pointer to tail */
    len -= strlen(response) + 1;
    response += strlen(response) + 1;
    if (len > 0)
      log_message(LOG_ERR, "iterating at [%s], len=%d", response, len);

  }

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
  struct timeval now;
  int ret;

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
    ret = zmq_poll (pollers, 2, QUEUE_CLEANUP_PERIOD);

    gettimeofday(&now, NULL);

    if ( ret < 0 ){
      log_message(LOG_ERR, "error on poll : %s", strerror(errno));
    };

    if (pollers[0].revents & ZMQ_POLLIN) {
      /* a request from tinyproxy client */
      handle_client(mux, clients, &now);
      request_flush(mux);
    }

    if ( pollers[1].revents & ZMQ_POLLIN ) {
      /* a response from guardserv */
      handle_response(mux, clients, &now);
    }

    if ( ret == 0 ) {
      cleanup_pendings(mux, &now);
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

