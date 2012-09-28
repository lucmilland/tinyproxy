import zmq
import socket
import subprocess
import threading
from time import sleep
import signal
import random
import struct

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
            _in = zsock.recv()
            id = struct.unpack("!l", _in[0:4])[0]
            requests = _in[4:].split('\0')
            del requests[-1]  # delete last empty string 
        except:
            terminate.set()
            raise
            break

        response = []

        
        '''
        print "\ndoing for %s :" % id
        for request in requests:
            print "\t%s" % request
        '''
        for request in requests:

            if not request:
                continue

            caller, url = request.split(' ', 1)
            #print "%s doing : %s for %s@%d (through %s)" % (guard.pid, url, caller, id, client)

            guard.stdin.write('%s\n' % url)

            out = guard.stdout.readline().strip()

            #sleep(random.randrange(0,4))
            #print "\tsquidguard said : [%s]" % (out)

            response.append('%s %s' % (caller, out.split(' ')[0]));
        # send answer to client socket
        if response:
            listener.sendto('%d %s\0' % (id, '\0'.join(response)), client)
        else:
            listener.sendto('%d\0' % id, client)

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
