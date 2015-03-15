#!/usr/bin/env python
import socket
import time
import thread
import time
from random import randint
from threading import Thread, Lock

MAX = 10
TCP_IP = '127.0.0.1'
TCP_PORT1 = 8001 #port to listen to connections to from nodeA
TCP_PORT2 = 8002 #port to listen to connections to from nodeB
TCP_PORT3 = 8003 #port to listen to connections to from nodeC
TCP_PORT4 = 8004 #port to listen to connections to from nodeD
BUFFER_SIZE = 200

counter_mutex = Lock()
sequence_mutex = Lock()
A_mutex = Lock()
B_mutex = Lock()
C_mutex = Lock()
D_mutex = Lock()

#signal handler
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

def send_delayed_messageA(data, delay, num):
	time.sleep(delay)
	A_mutex.acquire()
	message = data + " " + str(num)
	while len(message) < 99:
		message += "."
	conn1.send(message + "\n")
	A_mutex.release()
def send_delayed_messageB(data, delay, num):
	time.sleep(delay)
	B_mutex.acquire()
	message = data + " " + str(num)
	while len(message) < 99:
		message += "."
	conn2.send(message + "\n")
	B_mutex.release()
def send_delayed_messageC(data, delay, num):
	time.sleep(delay)
	C_mutex.acquire()
	message = data + " " + str(num)
	while len(message) < 99:
		message += "."
	conn3.send(message + "\n")
	C_mutex.release()
def send_delayed_messageD(data, delay, num):
	time.sleep(delay)
	D_mutex.acquire()	
	message = data + " " + str(num)
	while len(message) < 99:
		message += "."
	conn4.send(message + "\n")
	D_mutex.release()

#method to broadcast messages to all nodes
def broadcast(data, num):
	print data + " " + str(num)
	thread.start_new_thread(send_delayed_messageA, (data, randint(2, MAX), num))
	thread.start_new_thread(send_delayed_messageB, (data, randint(2, MAX), num))
	thread.start_new_thread(send_delayed_messageC, (data, randint(2, MAX), num))
	thread.start_new_thread(send_delayed_messageD, (data, randint(2, MAX), num))

#method to recieve input from nodeA
def receive_from_nodeA():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT1))
	s.listen(1)
	global conn1
	conn1, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn1.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		sequence_mutex.acquire()
		global sequence
		sequence += 1 #increment sequence number within lock
		temp_sequence = sequence #store sequence number into local variable
		sequence_mutex.release()
		if len(data) > 0:
			broadcast(data, temp_sequence)
		data = ""
		
#method to recieve input from nodeB
def receive_from_nodeB():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT2))
	s.listen(1)
	global conn2
	conn2, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn2.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		sequence_mutex.acquire()
		global sequence
		sequence += 1 #increment sequence number within lock
		temp_sequence = sequence #store sequence number into local variable
		sequence_mutex.release()
		if len(data) > 0:
			broadcast(data, temp_sequence)
		data = ""

#method to recieve input from nodeC
def receive_from_nodeC():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT3))
	s.listen(1)
	global conn3
	conn3, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn3.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		sequence_mutex.acquire()
		global sequence
		sequence += 1 #increment sequence number within lock
		temp_sequence = sequence #store sequence number into local variable
		sequence_mutex.release()
		if len(data) > 0:
			broadcast(data, temp_sequence)
		data = ""

#method to recieve input from nodeD
def receive_from_nodeD():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT4))
	s.listen(1)
	global conn4
	conn4, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn4.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		sequence_mutex.acquire()
		global sequence
		sequence += 1 #increment sequence number within lock
		temp_sequence = sequence #store sequence number into local variable
		sequence_mutex.release()
		if len(data) > 0:
			broadcast(data, temp_sequence)
		data = ""

def main():
	global counter
	global sequence
	sequence = 0
	counter = 0
	thread.start_new_thread(receive_from_nodeA, ())
	thread.start_new_thread(receive_from_nodeB, ())
	thread.start_new_thread(receive_from_nodeC, ())
	thread.start_new_thread(receive_from_nodeD, ())
	while 1:
		x = 1

main()