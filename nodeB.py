#!/usr/bin/env python

import socket
import time
import thread 
import fileinput
import sys

TCP_IP = '127.0.0.1'
TCP_PORT = 4002
TCP_PORT_SEQUENCER = 8002
BUFFER_SIZE = 1024

key_value = {}
from_server = []
from_sequencer = {}

def sleep_and_send(data, delay):
	time.sleep(float(delay))
	sequencer.send(data + "\n")
	server.send(data + "\n")

def readInputs():
	readFile = False
	f = open('nodeBCommands.txt', 'r')
	while 1:
		if readFile == False:
			data = f.readline().replace("\n", '')
			print data
			if data == '':
				f.close()
				readFile = True
		if readFile == True:
			data = raw_input("")
		if "show_all" in data:
			for a in key_value:
				sys.stdout.write(str(a) + ': ' + str(key_value[a]) + '  ')	
			sys.stdout.write("\n")	
			continue	
		elif len(data) > 0 and "send" in data.lower():
			server.send(data + "\n")
			time.sleep(0.3)
			if "send" in data.lower():
				print "Sent \"" + data[5:len(data)-2] + "\" to " + data[len(data)-1] + ", System time is: " + str(time.time())
			continue
		if readFile == True:		
			delay = raw_input("").split()
		else:
			delay = f.readline().replace("\n", '').split()
		thread.start_new_thread(sleep_and_send, (data, delay[1],))			

def total_order():
	global s
	match = 1
	while match == 1:
		if (s+1) in from_sequencer.keys():
			message = from_sequencer[s + 1]
			for string in from_server:
				if message == string:
					print message	
					if "insert" in message or "update" in message:
						parsed = message.split(' ')
						key = int(parsed[1])
						value = int(parsed[2])
						model = parsed[3]
						key_value[key] = value	
						del from_sequencer[s + 1]
						from_server.remove(message)
						s = s + 1
		else:
			match = 0		
def readData_server():
	global server
	while 1:
		data = server.recv(BUFFER_SIZE)
		if len(data) > 0 and "received" in data.lower():
			print data[0:len(data) - 1]
		else:
			from_server.append(data)	
		data = ""

def readData_sequencer():
	global sequencer
	while 1:
		data = sequencer.recv(BUFFER_SIZE)
		if len(data) > 0 and "connected" not in data:
			data = data.replace("\n", "")
			#print "Received " + data
			parsed = data.split(' ')
			sequence_num = int(parsed[4])
			message = parsed[0] + " " + parsed[1] + " " + parsed[2] + " " + parsed[3]
			from_sequencer[sequence_num] = message
			total_order()
			data = ""

def main():
	global server
	global sequencer
	global s
	s = 0
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server.connect((TCP_IP, TCP_PORT))
	sequencer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sequencer.connect((TCP_IP, TCP_PORT_SEQUENCER))
	server_connection = 0
	while server_connection == 0:
		data = server.recv(BUFFER_SIZE)
		if len(data) > 0:
			print data
			server_connection = 1

	# f = open('nodeACommands.txt', 'r')
	# for line in f:
	# 	server.send(line)
	# 	print "Sent \"" + line[5:len(line)-3] + "\" to " + line[len(line)-2] + ", System time is: " + str(time.time())
	# 	time.sleep(1)
	# f.close()

	thread.start_new_thread(readInputs, ())
	thread.start_new_thread(readData_server, ())
	thread.start_new_thread(readData_sequencer, ())
		
	while 1:
		running = 1

main()		