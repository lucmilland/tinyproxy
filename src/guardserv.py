#!/usr/bin/env python

import zmq
import random
import subprocess
import threading
from time import sleep

# number of squidguard processes
WORKER_THREADS = 3

worker_addr = "inproc://squidguard"

def guard_worker(context, terminate):

    guard = subprocess.Popen('squidGuard', stdin=subprocess.PIPE, 
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # socket to talk to dispatcher
    socket = context.socket(zmq.REP)
    socket.connect(worker_addr)

    while not terminate.is_set():
        try:
            client_ip, client_fqdn, ident, \
                method, query = socket.recv_multipart()
        except zmq.ZMQError, e:
            print e
            continue
        guard.stdin.write('%s %s/%s %s %s\n' % (query, client_ip, client_fqdn,
                                                ident, method))
        out = guard.stdout.readline().strip()
        print '%s %s/%s %s %s -> [%s]' % (query, client_ip, client_fqdn, ident, method, out)
        socket.send(out.split(' ')[0])


context = zmq.Context(1)

# socket to talk to clients
clients = context.socket(zmq.ROUTER)
clients.bind("tcp://*:5555")

# socket to talk to workers
guards = context.socket(zmq.DEALER)
guards.bind(worker_addr)

terminate = threading.Event()

for i in range(WORKER_THREADS):
    thread = threading.Thread(target=guard_worker, args=(context, terminate))
    thread.start()

try:
    zmq.device(zmq.QUEUE, clients, guards)
except:
    pass

terminate.set()
sleep(1) # wait threads to terminate
clients.close()
guards.close()
context.term()
