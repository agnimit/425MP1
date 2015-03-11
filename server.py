#server's port is 8000
#!/usr/bin/env python
import socket
import time
import thread
import time
from random import randint
from threading import Thread, Lock

TCP_IP = '127.0.0.1'
TCP_PORT1 = 4001
TCP_PORT2 = 4002
TCP_PORT3 = 4003
TCP_PORT4 = 4004
BUFFER_SIZE = 200
info = {"AB": [Lock(), 0, []], "BA": [Lock(), 0, []], "AC": [Lock(), 0, []], "CA": [Lock(), 0, []],  
		"AD": [Lock(), 0, []],  "DA": [Lock(), 0, []],  "BC": [Lock(), 0, []],  "CB": [Lock(), 0, []],  
		"BD": [Lock(), 0, []],  "DB": [Lock(), 0, []],  "CD": [Lock(), 0, []],  "DC": [Lock(), 0, []]}

counter_mutex = Lock()	

def send_delayed_messageA(data, delay):
	time.sleep(delay)
	conn1.send(data + "\n")
def send_delayed_messageB(data, delay):
	time.sleep(delay)
	conn2.send(data + "\n")
def send_delayed_messageC(data, delay):
	time.sleep(delay)
	conn3.send(data + "\n")
def send_delayed_messageD(data, delay):
	time.sleep(delay)	
	conn4.send(data + "\n")

def broadcast(data):
	thread.start_new_thread(send_delayed_messageA, (data, randint(0,5)))
	thread.start_new_thread(send_delayed_messageB, (data, randint(0,5)))
	thread.start_new_thread(send_delayed_messageC, (data, randint(0,5)))
	thread.start_new_thread(send_delayed_messageD, (data, randint(0,5)))

def delay(destination, sender):
	key = sender + destination
	info[key][0].acquire()
	max_time = 0
	info[key][1]+=1
	queue = info[key][2]
	count = info[key][1]	
	for i in queue:
		if max_time < i[1]:
			max_time = i[1]
	info[key][0].release()
	max_delay = max_time - time.time()	
	if max_delay < 0:
		max_delay = 0	
	time.sleep(max_delay)

	info[key][0].acquire()
	command = (queue.pop(0))[0]
	parse_server_message(command, max_delay, sender)			
	info[key][0].release()

def parse_server_message(command, max_delay, sender):

	message = command[5:len(command) - 2]
	destination = command[len(command) - 1]
	if "value,time" in command:
		message = command[5:12]
		temp = command.split(" ")
		value_here = temp[6]
		time_here = temp[7]
		final_message = "Received \"" + message + "\" from " + sender + ", value is " + str(value_here) + " Max delay is " + str(max_delay) + " s, system time is " + str(time_here)
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

def send_acknowledgements():
	global counter
	while counter < 4:
		waiting = 1

	conn1.send("You are connected to the server!")
	conn2.send("You are connected to the server!")	
	conn3.send("You are connected to the server!")	
	conn4.send("You are connected to the server!")		


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
		if "sen" in data.lower():
			print data
			destination = data[len(data)-1]
			key = "A" + destination
			queue = info[key][2]
			queue.append([data, time.time() + randint(0, 5)])
			thread.start_new_thread(delay, (destination, "A"))
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1
		

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
			key = "B" + destination
			queue = info[key][2]
			queue.append([data, time.time() + randint(0, 5)])
			thread.start_new_thread(delay, (destination, "B"))
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1


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
			key = "C" + destination
			queue = info[key][2]
			queue.append([data, time.time() + randint(0, 5)])
			thread.start_new_thread(delay, (destination, "C"))
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1


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
			key = "D" + destination
			queue = info[key][2]
			queue.append([data, time.time() + randint(0, 5)])
			thread.start_new_thread(delay, (destination, "D"))
		elif len(data) > 0:
			broadcast(data)		
		data = ""
		count+=1

def main():
	global counter
	counter = 0
	thread.start_new_thread(receive_from_nodeA, ())
	thread.start_new_thread(receive_from_nodeB, ())
	thread.start_new_thread(receive_from_nodeC, ())
	thread.start_new_thread(receive_from_nodeD, ())
	thread.start_new_thread(send_acknowledgements, ())
	while 1:
		x = 1

main()
