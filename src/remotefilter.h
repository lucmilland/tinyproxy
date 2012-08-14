/* tinyproxy - A fast light-weight HTTP proxy
 * Copyright (C) 1999 George Talusan <gstalusan@uwaterloo.ca>
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

/* See 'filter.c' for detailed information. */

#ifndef _TINYPROXY_REMOTEFILTER_H_
#define _TINYPROXY_REMOTEFILTER_H_

#include "reqs.h"
#include "conns.h"

typedef enum {
        REMOTE_FILTER_DEFAULT_ALLOW,
        REMOTE_FILTER_DEFAULT_DENY
} remote_filter_policy_t;

extern void remote_filter_init (void);
extern void remote_filter_destroy (void);
extern int remote_filter (struct request_s *request, const char *url, struct conn_s *connptr);

extern void remote_filter_set_default_policy (remote_filter_policy_t policy);

#endif
