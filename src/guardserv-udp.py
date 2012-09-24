import zmq
import socket
import subprocess
import threading
from time import sleep
import signal
import random

# number of squidguard processes                                                                                             
WORKER_THREADS = 3
MAX_DGRAM_SIZE = 16*1024
bind_addr = ('', 10100)
worker_addr = "inproc://guard-serv"

terminate = threading.Event()

def signal_handler(signum, frame):
    terminate.set()
signal.signal(signal.SIGINT, signal_handler)

def guard_worker(context, listener, terminate):

    guard = subprocess.Popen('squidGuard', stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # zmq socket to talk to dispatcher
    try:
        zsock = context.socket(zmq.REQ)
        zsock.connect(worker_addr)
    except:
        terminate.set()
        return

    while not terminate.is_set():
        # notify main thread we are ready
        zsock.send("READY")

        try:
            client = zsock.recv_pyobj()
            id, request = zsock.recv().split(' ', 1)
        except:
            terminate.set()
            break

        #print "%s doing : %s for %s" % (guard.pid, request, id)

        guard.stdin.write('%s\n' % request)

        out = guard.stdout.readline().strip()

        #sleep(random.randrange(0,3))

        # send answer to client socket
        if out:
            listener.sendto('%s %s\0' % (id, out.split(' ')[0]), client)
        else:
            listener.sendto('%s\0' % id, client)

    guard.terminate()
    zsock.close()

if __name__ == '__main__':

    # bind INET socket to listen to incoming requests
    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener.bind(bind_addr)

    # spawn zmq workers
    context = zmq.Context(1)
    for i in range(WORKER_THREADS):
        thread = threading.Thread(target=guard_worker, args=(context, listener, terminate))
        thread.start()

    # socket to talk to workers
    guards = context.socket(zmq.ROUTER)
    guards.bind(worker_addr)

    while not terminate.is_set():

        # get a worker that said "READY"
        try:
            address, empty, ready = guards.recv_multipart()
        except Exception, e:
            print e
            terminate.set()
            break

        # receive from udp socket
        try:
            request, client = listener.recvfrom(MAX_DGRAM_SIZE)
        except Exception, e:
            print e
            terminate.set()
            break

        # distribute request to worker
        try:
            guards.send(address, zmq.SNDMORE)
            guards.send("", zmq.SNDMORE)
            guards.send_pyobj(client, zmq.SNDMORE)
            guards.send(request)
        except Exception, e:
            print "can't send : %s" % e
            terminate.set()
            break

    terminate.set()
    sleep(1) # wait threads to terminate
    listener.close()
    guards.close()
    context.term()
    listener.close()
