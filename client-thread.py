# -*- coding=utf-8 -*-
from __future__ import print_function
from socket import *
import pickle, random, threading, time, os, sys, Queue

# -----------------------------------------------
''' classes
'''

class c_pkt(object):
  # client packet which has base(seq number) and data
  def __init__(self, base, data):
      self.base = base 
      self.data = data

class s_pkt(object):
  # server packet which has ack and rwnd
  def __init__(self, ack, rwnd):
      self.ack = ack
      self.rwnd = rwnd

class file_info(object):
  # the name and size of the file sent
  def __init__(self, name, size):
    self.name = name
    self.size = size

# -----------------------------------------------
''' globals
'''

windows = Queue.Queue()

def f():
  a = 1
seq_limit = 1000
time_limit = 1
time_count = 0
timer = threading.Timer(time_limit, f)
data_size = 10000
packet_size = 60000

ip = sys.argv[1]
fileName = sys.argv[2]
filePath = "D:\\" + fileName
file_size = os.path.getsize(filePath)
file_sent = pickle.dumps(file_info(fileName, file_size))
num_of_times = file_size / data_size

base = 0 # (LstByteAcked + 1) % seq_limit
nextseq = 0 # (LastByteSent + 1) % seq_limit
rwnd = 100
cwnd = 1
ssthresh = 0

count = 0
last_count = 0
start = 0
end = 0

lock = threading.Lock()
tcp = socket(AF_INET, SOCK_STREAM)
tcp.connect((ip, 9999))
tcp.send(file_sent)
port = tcp.recv(1024)
tcp.close()
print("port: %d" % int(port))
server_addr = (ip, int(port))
 
# -----------------------------------------------
''' functions
'''

def print_progress():
  global last_count
  while True:
    if start == 0:
      continue
    time.sleep(1)
    count_dif = count - last_count
    speed = 0
    unit = 'Kbps'
    if count_dif > 0:
      if count_dif >= 100:
        speed = count_dif * 10.0 / 1024.0
        unit = 'Mbps'
      else:
        speed = count_dif * 10.0
        unit = 'Kbps'

    percent = 100 * count / num_of_times
    progress(percent, 50, speed, unit)
    last_count = count
    if end == 1:
      break
  progress(percent, 50, -1)
  end_time = time.asctime( time.localtime(time.time()) )
  print("\nend time: ", end_time)


def progress(percent, width = 50, speed = -1, unit = 'bps'):
  if percent >= 100:
		percent = 100
  show_str = ('[%%-%ds]' % width) % (int(width * percent / 100) * "#")
  if speed == -1:
    print('\r%s %d%% done!    ' % (show_str, percent), end = '')
  else:
    print('\r%s %d%%  %.1f%s' % (show_str, percent, speed, unit), end = '')

def resend():
  '''
    send all the packet in the windows, 
    which have been sent but not acked yet
  '''
  
  global timer, lock, time_count, cwnd, ssthresh, end, rwnd
  ssthresh = cwnd / 2
  cwnd = 1
  lock.acquire()
  if end == 0:
    win_size = windows.qsize()
    if win_size == 0:
      print("no resend")
    for _ in range(0, win_size):
      packet = windows.get()
      client_socket.sendto(packet, server_addr)
      #p = pickle.loads(packet)
      #if packet[0] == "e":
        #print("resend %d" % int(p.base))
      
      windows.put(packet)
  lock.release()
  if time_count >= 5:
    print("time_count: %d, rwnd: %d, cwnd: %d" % (time_count, rwnd, cwnd))
    print(end)
  time_count = time_count + 1
  timer.cancel()
  timer = threading.Timer(time_limit + time_count, resend)
  if end == 0:
    timer.start()

def receive():
  '''
    receive from server, and if there is no response,
    resend all the packet
  '''
  global base, timer, rwnd, count, start, lock, time_count, cwnd, ssthresh, end
  while True:
    if start == 0:
      continue
    if rwnd != 0:
      timer.cancel()
      timer = threading.Timer(time_limit, resend)
      timer.start()
    # if rwnd is 0, the transmit function will send(resend) packet
    response, _ = client_socket.recvfrom(packet_size)
    if response == "exit":
      #print(" server exit") 
      end = 1
      timer.cancel()
      time.sleep(2)
      break
    else:

      server_pkt = pickle.loads(response)
      rwnd = int(server_pkt.rwnd)
      
      #print ("ack: %d, rwnd: %d, cwnd: %d" % (int(server_pkt.ack), int(server_pkt.rwnd), cwnd))
      timer.cancel()
      if (int(server_pkt.ack) >= base and int(server_pkt.ack) - base < 50) or (base - int(server_pkt.ack) > 800) :
        base = (int(server_pkt.ack) + 1) % seq_limit
        #print ("base: %d" % base)
        if cwnd >= ssthresh:
          cwnd = cwnd + 1
        else:
          cwnd = cwnd * 2

        time_count = 0
        lock.acquire()
        while not windows.empty():
          count = count + 1
      
          packet = windows.get()
          p = pickle.loads(packet)
          #print ("base: %d, pop: %d" % (base, int(p.base)))
          if ((p.base + 1) % seq_limit) >= base:
            break
        lock.release()
        # progress(100 * count / num_of_times)
      

def send_a_packet():
  global f, data_size, nextseq, server_addr
  data = f.read(data_size)
  if data:
    packet = pickle.dumps(c_pkt(nextseq, data))
    client_socket.sendto(packet, server_addr)
    windows.put(packet)
    nextseq = (nextseq + 1) % seq_limit
  else:
    client_socket.sendto("exit " + str(nextseq), server_addr)

def transmit():
  '''
    send new packet when there are still spaces,
    and send a small packet when rwnd is 0
  '''
  global start, nextseq, base, seq_limit, rwnd, f, data_size, windows, lock, cwnd
  rwnd_count = 0
  while True:
    # number of packets sent but not acked should smaller than rwnd
    if (nextseq - base + seq_limit) % seq_limit < min(rwnd, cwnd):
      data = f.read(data_size)
      if data:
        packet = pickle.dumps(c_pkt(nextseq, data))
        client_socket.sendto(packet, server_addr)
        if start == 0:
          start = 1
          start_time = time.asctime( time.localtime(time.time()) )
          print("start time: ", start_time)
        lock.acquire()
        windows.put(packet)
        lock.release()
        #print("put: %d" % nextseq)
        nextseq = (nextseq + 1) % seq_limit
      else:
        #print(" send exit", end = '') 
        client_socket.sendto("exit " + str(nextseq), server_addr)
        packet = "exit " + str(nextseq)
        lock.acquire()
        windows.put(packet) # ?
        lock.release()
        break
    if rwnd != 0:
      rwnd_count = 0
    # do sth when rwnd is 0
    elif rwnd == 0:
      #print ("rwnd == 0")
      if rwnd_count % 10000 == 0:
        #print("rwnd == 0")
        client_socket.sendto("No Buffer", server_addr)
      rwnd_count = (rwnd_count + 1) % 10000

# -----------------------------------------------

client_socket = socket(AF_INET, SOCK_DGRAM)
f = open(filePath.decode('UTF-8'), 'rb')
# global timer
timer = threading.Timer(time_limit, resend)
# receive from server
t = threading.Thread(target = receive)
t.start()
#print bar
p = threading.Thread(target = print_progress)
p.start()
# send packets
transmit()

t.join()
f.close()
client_socket.close()