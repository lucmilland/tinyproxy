import socket
import subprocess
import threading
from time import sleep

# number of squidguard processes                                                                                             
#WORKER_THREADS = 3
MAX_DGRAM_SIZE = 1500
worker_addr = "inproc://squidguard"
bind_addr = ('', 10100)

def guard_worker(listener):

    guard = subprocess.Popen('squidGuard', stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    while True:
        request, client = listener.recvfrom(MAX_DGRAM_SIZE)
        guard.stdin.write('%s\n' % request)

        out = guard.stdout.readline().strip().split(' ')[0]
        listener.sendto('%s\0' % out, client)

if __name__ == '__main__':

    listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listener.bind(bind_addr)

    guard_worker(listener)
