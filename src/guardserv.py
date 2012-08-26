#!/usr/bin/env python

import zmq
import random
import subprocess

context = zmq.Context()
socket = context.socket(zmq.ROUTER)
socket.bind("tcp://*:5555")

guard = subprocess.Popen('squidGuard', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

while True:
    id = socket.recv()
    socket.recv() # empty message frame REQ 
    client_ip = socket.recv()
    client_fqdn = socket.recv()
    method = socket.recv()
    query = socket.recv()
    print '%s %s/%s - %s\n' % (query, client_ip, client_fqdn, method)
    guard.stdin.write('%s %s/%s - %s\n' % (query, client_ip, client_fqdn, method))
    out = guard.stdout.readline().strip()
    print "result : [%s]" % out
    
    socket.send(id, zmq.SNDMORE)
    socket.send("", zmq.SNDMORE)
    socket.send(out.split(' ')[0])
