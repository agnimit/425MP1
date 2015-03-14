#!/usr/bin/env python
import socket
import time
import thread
import time
from random import randint
from threading import Thread, Lock

MAX = 10
TCP_IP = '127.0.0.1'
TCP_PORT1 = 4001
TCP_PORT2 = 4002
TCP_PORT3 = 4003
TCP_PORT4 = 4004
BUFFER_SIZE = 200
#dictionary of queues to handle messages between the various channels. Key "AB" refers to queue which handles messages sent from A to B
info = {"AB": [Lock(), 0, []], "BA": [Lock(), 0, []], "AC": [Lock(), 0, []], "CA": [Lock(), 0, []],  
		"AD": [Lock(), 0, []],  "DA": [Lock(), 0, []],  "BC": [Lock(), 0, []],  "CB": [Lock(), 0, []],  
		"BD": [Lock(), 0, []],  "DB": [Lock(), 0, []],  "CD": [Lock(), 0, []],  "DC": [Lock(), 0, []]}

counter_mutex = Lock()	
#signal handler
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

def send_delayed_messageA(data, delay):
	if "search" not in data:
		time.sleep(delay)
	conn1.send(data + "\n")
def send_delayed_messageB(data, delay):
	if "search" not in data:
		time.sleep(delay)
	conn2.send(data + "\n")
def send_delayed_messageC(data, delay):
	if "search" not in data:
		time.sleep(delay)
	conn3.send(data + "\n")
def send_delayed_messageD(data, delay):
	if "search" not in data:
		time.sleep(delay)	
	conn4.send(data + "\n")

#randint(0, MAX)
#method to broadcast data to all nodes
def broadcast(data):
	thread.start_new_thread(send_delayed_messageA, (data, randint(0, MAX)))
	thread.start_new_thread(send_delayed_messageB, (data, randint(0, MAX)))
	thread.start_new_thread(send_delayed_messageC, (data, randint(0, MAX)))
	thread.start_new_thread(send_delayed_messageD, (data, randint(0, MAX)))

#method to take ensure FIFO ordering of messages being sent
def delay(destination, sender):
	key = sender + destination
	info[key][0].acquire()
	max_time = 0
	info[key][1]+=1
	queue = info[key][2]
	count = info[key][1]
	#loop to calculate delay according to FIFO principles	
	for i in queue:
		if max_time < i[1]:
			max_time = i[1]
	info[key][0].release()
	max_delay = max_time - time.time()	
	if max_delay < 0:
		max_delay = 0	
	if max_delay >= MAX:
		max_delay = MAX	
	time.sleep(max_delay) 

	info[key][0].acquire()
	command = (queue.pop(0))[0]
	parse_server_message(command, MAX, sender)			
	info[key][0].release()

#parses the message and sends it to the appropriate destination
def parse_server_message(command, max_delay, sender):

	message = command[5:len(command) - 2]
	destination = command[len(command) - 1]
	if "value" in command and "time" in command: #if statement for eventual consistency commands
		temp = command.split(" ")
		value_here = temp[5]
		time_here = temp[7]
		final_message = "Received \"" + message + "\" from " + sender
	elif "has" in message or "have" in message: #if statement to handle search command
		final_message = message	
	else:
		final_message = "Received \"" + message + "\" from " + sender + ", Max delay is " + str(max_delay) + " s, system time is " + str(time.time())
	if destination == 'A':
		conn1.send(final_message + "\n")
	if destination == 'B':
		conn2.send(final_message + "\n")
	if destination == 'C':
		conn3.send(final_message + "\n")
	if destination == 'D':
		conn4.send(final_message + "\n")

#method to send acks to nodes to tell them that they are connected to the server
def send_acknowledgements():
	global counter
	while counter < 4:
		waiting = 1

	conn1.send("You are connected to the server!")
	conn2.send("You are connected to the server!")	
	conn3.send("You are connected to the server!")	
	conn4.send("You are connected to the server!")		

#method to receive messages from nodeA
def receive_from_nodeA():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT1))
	s.listen(1)
	count = 0
	global conn1
	conn1, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn1.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "sen" in data.lower() and data[0] != 'A':
			print data
			destination = data[len(data)-1]
			if destination != "A" and destination in "ABCD":
				key = "A" + destination
				queue = info[key][2] #appends message to appropriate queue to ensure FIFO
				queue.append([data, time.time() + randint(0, MAX)])
				if "has" not in data or "have" not in data:
					thread.start_new_thread(delay, (destination, "A"))
				else:
					parse_server_message(data, 0, "A")	
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1
		
#method to receive messages from nodeB
def receive_from_nodeB():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT2))
	s.listen(1)
	count = 0
	global conn2
	conn2, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn2.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "sen" in data.lower():
			print data
			destination = data[len(data)-1]
			if destination != "B" and destination in "ABCD":
				key = "B" + destination
				queue = info[key][2] #appends message to appropriate queue to ensure FIFO
				queue.append([data, time.time() + randint(0, MAX)])
				if "has" not in data or "have" not in data:
					thread.start_new_thread(delay, (destination, "B"))
				else:
					parse_server_message(data, 0, "B")
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1

#method to receive messages from nodeC
def receive_from_nodeC():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT3))
	s.listen(1)
	count = 0
	global conn3
	conn3, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn3.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "sen" in data.lower():
			print data
			destination = data[len(data)-1]
			if destination != "C" and destination in "ABCD":
				key = "C" + destination
				queue = info[key][2] #appends message to appropriate queue to ensure FIFO
				queue.append([data, time.time() + randint(0, MAX)])
				if "has" not in data or "have" not in data:
					thread.start_new_thread(delay, (destination, "C"))
				else:
					parse_server_message(data, 0, "C")
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1

#method to receive messages from nodeD
def receive_from_nodeD():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	s.bind((TCP_IP, TCP_PORT4))
	s.listen(1)
	count = 0
	global conn4
	conn4, addr = s.accept()
	counter_mutex.acquire()
	global counter
	counter+=1
	counter_mutex.release()
	while 1:
		data = conn4.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "sen" in data.lower():
			print data
			destination = data[len(data)-1]
			if destination != "D" and destination in "ABCD":
				key = "D" + destination
				queue = info[key][2] #appends message to appropriate queue to ensure FIFO
				queue.append([data, time.time() + randint(0, MAX)])
				if "has" not in data or "have" not in data:
					thread.start_new_thread(delay, (destination, "D"))
				else:
					parse_server_message(data, 0, "D")
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1

def main():
	global counter
	counter = 0
	#spawn threads to get messages from the 4 different nodes
	thread.start_new_thread(receive_from_nodeA, ())
	thread.start_new_thread(receive_from_nodeB, ())
	thread.start_new_thread(receive_from_nodeC, ())
	thread.start_new_thread(receive_from_nodeD, ())
	thread.start_new_thread(send_acknowledgements, ())
	while 1:
		x = 1

main()
