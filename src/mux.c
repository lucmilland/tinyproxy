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

#define PROTOCOL_VERSION 0x0001

struct packet_header {
  uint32_t id;
  uint16_t version;
  char seq;
} __attribute__((__packed__));

struct pending_req {
  uint32_t id;
  uint32_t seq;             /* times this request was emitted */
  struct timeval time;      /* request time */
  int len;                  /* current length of buffer */
  char *buffer;
  int count;                /* number of requests in buffer */
  int guard_index; /* index in guardservs of server used to send this */
};

/* pendings hashmap is cleaned every QUEUE_CLEANUP_PERIOD milliseconds */
#define QUEUE_CLEANUP_PERIOD 40

/*
 * A socket to a foreign guardserv
 */
struct guard_socket {
  int fd;          /* UDP socket to guardserv */
  struct sockaddr_in sockaddr;
  socklen_t socklen;
  float mean_time; /* weighted average of previous response times, in milliseconds */

  /* current health state of connection. bigger is better.
     values <= 0 means disconnected
  */
  int health;
};

/* maximum number of guardservers we can sendto */
#define MAX_GUARDSERVERS 3
struct guard_socket guardservs[MAX_GUARDSERVERS]; /* list of guardservers */

struct mux_context {
  void *zmq_context; /* ZMQ context for the following */
  hashmap_t pendings;/* pending calls hashmap */
  pthread_t worker_thread;  /* thread listening for queries from tinyproxy */
  char *hostname;
  int need_reconnect; /* some sockets need reconnect */
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
 * get a connected guardserver that is not the one at index.
 * return -1 if none can be found.
 */
static int
guardsockets_get_other(int guard) {
  int i, idx;
  int seed;

  seed = rand();

  for (i=0; i < MAX_GUARDSERVERS; i++) {
    idx = (i + seed) % MAX_GUARDSERVERS;
    if (guardservs[idx].fd != -1 && idx != guard && guardservs[i].health > 0) {
      return idx;
    }
  }
  return -1;
}

/*
 * get fastest guardserver. 
 * return -1 if none can be found.
 */
static int
guardsockets_get_fastest(void) {
  int i;
  int fastest = -1;

  /* lie, sometimes, and return some random server for testing */
  if (rand() % 100 == 0) {
    fastest = guardsockets_get_other(-1);
    if (fastest != -1) return fastest;
  }

  /* otherwise, really get fastest server */
  for (i=0; i < MAX_GUARDSERVERS; i++) {
    if (guardservs[i].fd != -1 &&
	guardservs[i].health > 0 &&
	(fastest == -1 || 
	 guardservs[i].mean_time < guardservs[fastest].mean_time)) {
      fastest = i;
    }
  }
  
  return fastest;
}

/* remove any call in pendings that refers to guard,
   moving them to another guard if available */
static void
purge_pendings(hashmap_t requests, int guard) {
  hashmap_iter iter;
  char *key;
  struct pending_req *req;
  int other;

  other = guardsockets_get_other(guard);
  
  for (iter = hashmap_first(requests);
       !hashmap_is_end(requests, iter); 
       iter++) {
    hashmap_return_entry(requests, iter, &key, (void**)&req);
    if (req->guard_index == guard) {
      if (other != -1) {
	req->guard_index = other;
	req->seq = 3; /* small shortcut to re-send request quickly */
      } else {
	/* no other guard is available : forget request */
	free(req->buffer);
	hashmap_remove(requests, key);
	iter --;
      }
    }
  }
}
/*
 * reconnect guardservers
 */
static int
guardsockets_reconnect(mux_context_t mux) {
  struct addrinfo *rp, *serv_addr;
  int count;

  /* get server addresses */
  serv_addr = get_server_connection(mux->hostname);
  if (!serv_addr) {
    log_message(LOG_ERR, "Can't resolve %s", mux->hostname);
    return -1;
  }

  for (count = 0, rp = serv_addr ; 
       count < MAX_GUARDSERVERS && rp != NULL ;
       rp = rp->ai_next) {

    if (guardservs[count].health > 0) {
      /* socket is in good shape. keep it. */
      count ++;
      continue;
    }

    log_message(LOG_INFO, "Trying to guardserv at %s", 
		inet_ntoa(((struct sockaddr_in *)rp->ai_addr)->sin_addr));

    if (guardservs[count].fd != -1) {
      /* purge pending calls before closing */
      purge_pendings(mux->pendings, count);
      close(guardservs[count].fd);
    }

    guardservs[count].fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);

    if (guardservs[count].fd == -1)
      /* can't create socket, move to next server */
      continue;

    memcpy(&guardservs[count].sockaddr, rp->ai_addr, rp->ai_addrlen);
    guardservs[count].socklen = rp->ai_addrlen;
    /* start with 40ms average response time */
    guardservs[count].mean_time = 40;
    guardservs[count].health = 20;

    count ++;
  }
  if (count == 0) {
    log_message(LOG_ERR, "Can't create socket");
    return -1;
  }

  freeaddrinfo(serv_addr);
  return 0;
}

/*
 * init guardserver connexions to hostname
 */
static int
guardsockets_init(mux_context_t mux) {
  int i;

  for (i = 0; i < MAX_GUARDSERVERS; i++) {
    guardservs[i].fd = -1;
    guardservs[i].health = -1;
  }

  guardsockets_reconnect(mux);

  return 0;
}

/*
 * Init a new context
 * Return NULL on error
 */
static mux_context_t
mux_init(char *hostname) {
  mux_context_t ctx;

  ctx = (mux_context_t)malloc(sizeof(struct mux_context));
  if (!ctx) {
    log_message(LOG_ERR, "Can't allocate context");
    return NULL;
  }

  ctx->hostname = hostname;
  ctx->need_reconnect = 0;

  /* initialize pendings queue */
  ctx->pendings = hashmap_create(32);

  /* initialize guardserv connexions */
  guardsockets_init(ctx);

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
  struct packet_header header;
  int timeout;
  int guard;

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
      timeout = (2 << (req->seq + 4)) * 1000;
    }

    if (now->tv_sec * 1000000 + now->tv_usec -
	req->time.tv_sec * 1000000 - req->time.tv_usec > timeout) {
      log_message(LOG_INFO, "timeout, req=%d, seq=%d, timeout=%fms, serv=%s",
		  req->id, req->seq, timeout / 1000., 
		  inet_ntoa(guardservs[req->guard_index].sockaddr.sin_addr));

      req->seq++;
      if (req->seq >= 12) {
	/* enough is enough : cancel request */
	free(req->buffer);
	hashmap_remove(mux->pendings, key);
	iter --;

	/* flag socket for reconnexion */
	guardservs[req->guard_index].health = -1;
	mux->need_reconnect = 1;

	continue;

      } else {

	if (timeout > 2 * 1000 * 1000) {
	  /* when timeout is above 2s, re-emit to another guardserver */
	  guard = guardsockets_get_other(req->guard_index);
	  if (guard != -1) {
	    req->guard_index = guard;
	    guardservs[req->guard_index].health -= 1;
	    if (guardservs[req->guard_index].health <= 0)
	      mux->need_reconnect = 1;
	  } else {
	    guard = req->guard_index;
	  }
	} else {
	  guard = req->guard_index;
	}

	/* re-emit when timeout is more than 140% of mean_time */
	if (timeout / 1000.  > guardservs[guard].mean_time * 1.4) {
	  /* re-emit */
	  log_message(LOG_INFO, "re-emit, req=%d, seq=%d, serv=%s (%d)",
		      req->id, req->seq,
		      inet_ntoa(guardservs[guard].sockaddr.sin_addr), guard);

	  /* init header */
	  header.version = htons(PROTOCOL_VERSION);
	  header.id = htonl(req->id);
	  header.seq = (char)req->seq;
	  memcpy(req->buffer, &header, sizeof(struct packet_header));

	  sendto(guardservs[guard].fd, req->buffer, req->len, 0,
		 &guardservs[guard].sockaddr,
		 guardservs[guard].socklen);
	}
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

  /* leave room for header */
  req->len += sizeof(struct packet_header);

  req->count = 0;
  req->guard_index = guardsockets_get_fastest();
  return 0;
}

/*
 * flush current request and increment request id
 */
static int
request_flush(mux_context_t mux) {
  char key[10];
  struct packet_header header;

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

  /* init header */
  header.version = htons(PROTOCOL_VERSION);
  header.id = htonl(mux->request.id);
  header.seq = (char)mux->request.seq;
  memcpy(mux->request.buffer, &header, sizeof(struct packet_header));

  /* send request to guardserv */
  sendto(guardservs[mux->request.guard_index].fd, 
	 mux->request.buffer, mux->request.len, 0,
	 &guardservs[mux->request.guard_index].sockaddr,
	 guardservs[mux->request.guard_index].socklen);

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
 * Add request to request queue
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
handle_response(mux_context_t mux, int guard, void *clients, struct timeval *now) {
  int len;
  int delta;
  struct pending_req *req;
  char buffer[MAX_MTU];
  char *id;
  char *response;
  char *zmq_sender;

  for ( ;; ) {

    /* read for a response */
    len = recv(guardservs[guard].fd, &buffer, sizeof(buffer), MSG_DONTWAIT);
    /* log_message(LOG_INFO, "answer : %d bytes : [%s]\n", len, buffer); */

    if (len == -1) {
      if (errno ==  EAGAIN || errno == EWOULDBLOCK) {
	/* done reading messages */
	break;
      } else {
	log_message(LOG_ERR, "error while reading response : %s", strerror(errno));
	return -1;
      }
    } else if (len == 0) {
      log_message(LOG_ERR, "peer has closed connection");
      guardservs[guard].health = -1;
      mux->need_reconnect = 1;
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
  
    /* register request duration to mean_time*/
    delta = now->tv_sec * 1000 + now->tv_usec / 1000 -
      req->time.tv_sec * 1000 - req->time.tv_usec / 1000;
    guardservs[req->guard_index].mean_time = ( guardservs[req->guard_index].mean_time + 0.2 * delta) / 1.2;

    log_message(LOG_INFO, "Request time %d ms. id = [%s]. Mean : %f ms",
		delta, id, guardservs[req->guard_index].mean_time);


    /* split responses of the form :
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
    }
  }
  return 0;
}

/*
 * Worker thread
 */
static void *
worker_task(void *args) {
  mux_context_t mux;
  zmq_pollitem_t pollers[1 + MAX_GUARDSERVERS];
  int polled_guards[1 + MAX_GUARDSERVERS];
  void *clients;     /* ZMQ socket to tinyproxys (they are clients) */
  struct timeval now;
  struct timeval lastcleanup;
  int ret, i;
  int guard, count;

  mux = (mux_context_t)args;

  /* listen requests from tinyproxy */
  clients = zmq_socket(mux->zmq_context, ZMQ_ROUTER);
  zmq_bind(clients, "ipc:///tmp/http-filter");

  pollers[0].socket = clients;
  pollers[0].fd = 0;
  pollers[0].events = ZMQ_POLLIN;
  pollers[0].revents = 0;

  count = 1;

  for (guard = 0; guard < MAX_GUARDSERVERS; guard ++) {
    if (guardservs[guard].fd != -1) {
      pollers[count].socket = NULL;
      pollers[count].fd = guardservs[guard].fd;
      pollers[count].events = ZMQ_POLLIN;
      pollers[count].revents = 0;
      polled_guards[count] = guard;
      count++;
    }
  }

  gettimeofday(&lastcleanup, NULL);

  while (1) {
    ret = zmq_poll (pollers, count, QUEUE_CLEANUP_PERIOD * 1000);

    gettimeofday(&now, NULL);

    if ( ret < 0 ){
      log_message(LOG_ERR, "error on poll : %s", strerror(errno));
    };

    if (pollers[0].revents & ZMQ_POLLIN) {
      /* a request from tinyproxy client */
      handle_client(mux, clients, &now);
      request_flush(mux);
    }

    for (i = 1; i < count; i ++) {
      if ( pollers[i].revents & ZMQ_POLLIN ) {
	/* a response from guardserv */
	handle_response(mux, polled_guards[i], clients, &now);
      }
    }

    if ( (now.tv_sec - lastcleanup.tv_sec) * 1000000 + 
	 (now.tv_usec - lastcleanup.tv_usec) > QUEUE_CLEANUP_PERIOD * 1000 ) {
      cleanup_pendings(mux, &now);
      memcpy(&lastcleanup, &now, sizeof(struct timeval));
    }

    if (mux->need_reconnect) {
      guardsockets_reconnect(mux);
      mux->need_reconnect= 0;
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

