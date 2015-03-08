#!/usr/bin/env python

import socket
import time
import thread 
import fileinput
import sys

TCP_IP = '127.0.0.1'
TCP_PORT = 4001
TCP_PORT_SEQUENCER = 8001
BUFFER_SIZE = 1024

key_value = {}
eventual = {}
from_server = []
from_sequencer = {}

def insert_and_update(key, value):
	key_value[key] = value	

def get(key):
	if key in key_value.keys():
		print "The corresponding value for key " + str(key) + " is: " + str(key_value[key])
	else:
		print "Key does not exist" 

def delete(key):
	if key in key_value.keys():
		print "Delete of key " + str(key) + " was succesful!"
		del key_value[key]
	else:
		print "Key does not exist"	

def sent_eventual(data):
	message = data[6:len(data)]
	print data
	parsed = message.split(' ')
	model = int(parsed[len(parsed) - 2])
	key = int(parsed[1])
	eventual[message] += 1
	if eventual[message] == (model - 2):
		del eventual[message]
		if "insert" in message or "update" in message:
			value = int(parsed[2])
			insert_and_update(key, value)
		if "get" in message:
			delete(key)
		if "delete" in message:
			delete(key)
		print message	

def received_eventual(data):
	destination = data[0]
	message = data[20:len(data) - 2]
	if data[0] != "A":
		print data
	parsed = message.split(' ')
	key = int(parsed[1])
	if "insert" in message or "update" in message:
		value = int(parsed[2])
		insert_and_update(key, value)
	if "get" in message:
		delete(key)
	if "delete" in message:
		delete(key)
	server.send("SendA " + message + " " + destination)	

def sleep_and_send(data, delay):
	time.sleep(float(delay))
	parsed = data.split(' ')
	model = int(parsed[len(parsed) - 1])
	if model == 1 or model == 2: #linear or sequential, so send to sequencer
		if model == 2 and "get" in data: #sequential consistency read. do it right away
			key = int(parsed[1])
			get(key)
		else:
			sequencer.send(data + "\n")
			server.send(data + "\n")
	if model == 3 or model == 4: #eventual consistency
		eventual[data] = 0
		server.send("A eventual request: " + data +"\n")	

def readInputs():
	readFile = False
	f = open('A.txt', 'r')
	while 1:
		data = ''
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
		if "send" in data.lower():
			server.send(data + "\n")
			time.sleep(0.2)
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
					parsed = message.split(' ')
					key = int(parsed[1])
					print message[0: len(message) - 2]	
					if "insert" in message or "update" in message:
						value = int(parsed[2])
						insert_and_update(key, value)
						from_server.remove(message)
					if "get" in message:
						get(key)
					if "delete" in message:
						delete(key)		
					del from_sequencer[s + 1]				
					s = s + 1		
		else:
			match = 0		
def readData_server():
	global server
	while 1:
		data = server.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "send" in data.lower() and ("insert" in data or "get" in data or "update" in data or "delete" in data): #I sent the eventual request
			sent_eventual(data)
		elif "eventual request" in data: #I did not send, but received the eventual request
			received_eventual(data)	
		elif "send" in data.lower():	
			print data[0:len(data) - 1]
		elif len(data) > 0:
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
			if "insert" in data or "update" in data:
				sequence_num = int(parsed[4])
				message = parsed[0] + " " + parsed[1] + " " + parsed[2] + " " + parsed[3]
			elif "get" or "delete" in data:	
				sequence_num = int(parsed[3])
				message = parsed[0] + " " + parsed[1] + " " + parsed[2]
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

	thread.start_new_thread(readInputs, ())
	thread.start_new_thread(readData_server, ())
	thread.start_new_thread(readData_sequencer, ())
		
	while 1:
		running = 1

main()		