/* tinyproxy - A fast light-weight HTTP proxy
 * Copyright (C) 1999 George Talusan <gstalusan@uwaterloo.ca>
 * Copyright (C) 2002 James E. Flemer <jflemer@acm.jhu.edu>
 * Copyright (C) 2002 Robert James Kaes <rjkaes@users.sourceforge.net>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

/* A substring of the domain to be filtered goes into the file
 * pointed at by DEFAULT_FILTER.
 */

#include <zmq.h>
#include "main.h"

#include "remotefilter.h"
#include "heap.h"
#include "log.h"
#include "reqs.h"
#include "conf.h"
#include "conns.h"

/* static int err; */

static int already_init = 0;
static void *context;
static void *requester;
static remote_filter_policy_t default_policy = REMOTE_FILTER_DEFAULT_ALLOW;


/*  Receive 0MQ string from socket and convert into C string
  Caller must free returned string. Returns NULL if the context
  is being terminated.
*/
static char *s_recv (void *sock) {
    zmq_msg_t message;
    int size;
    char *string;

    zmq_msg_init (&message);
    if (zmq_recv (sock, &message, 0))
        return (NULL);
    size = zmq_msg_size (&message);
    string = (char *)malloc (size + 1);
    memcpy (string, zmq_msg_data (&message), size);
    zmq_msg_close (&message);
    string[size] = '\0';
    return (string);
}

/*
  Convert C string to 0MQ string and send to socket
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

/*  Sends string as 0MQ string, as multipart non-terminal
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
 * Initializes connection to remote squidguard
 */
void remote_filter_init (void)
{
  if (!already_init && config.remotefilter) {
    context = zmq_init (1);
    requester = zmq_socket (context, ZMQ_REQ);
    zmq_connect (requester, config.remotefilter);
    already_init = 1;
  }
}

/* close server connection */
void remote_filter_destroy (void)
{
  if (already_init) {
    zmq_close (requester);
    zmq_term (context);
    already_init = 0;
  }
}

/* Return NULL to allow, non-NULL to redirect */
char *remote_filter (struct request_s *request, const char *url,
		     struct conn_s *connptr)
{
  char* reply;

  log_message (LOG_DEBUG, "filtering: %s %s %s %d/%s\n", request->host,
	       request->method, request->protocol, request->port,
	       request->path);

  if (!already_init)
    return (char *)NULL;

  s_sendmore(requester, connptr->client_ip_addr);
  s_sendmore(requester, connptr->client_string_addr);
  s_sendmore(requester, config.ident);
  s_sendmore(requester, request->method);
  s_send(requester, url);

  reply = s_recv(requester);

  if (reply == NULL) {
    log_message (LOG_ERR, "Error: %d\n", errno);
  } else {
    if (strlen(reply) != 0) {
      /* squidGuard said "filter this" */
      return reply;
    } else {
      free(reply);
      return (char *)NULL;
    }
  }
  return (char *)NULL;
}

/*
 * Set the default filtering policy
 */
void remote_filter_set_default_policy (remote_filter_policy_t policy)
{
        default_policy = policy;
}
