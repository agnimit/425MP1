#!/usr/bin/env python
import socket
import time
import thread 
import fileinput
import sys
import re
from threading import Thread, Lock
eventual_mutex = Lock()
send_mutex = Lock()	

MAX = 10
TCP_IP = '127.0.0.1'
TCP_PORT = 4002
TCP_PORT_SEQUENCER = 8002
BUFFER_SIZE = 1024

key_value = {} #dictionary to represent key-value storage
eventual = {} #dictionary to handle eventual inserts, updates, and deletes
eventual_read = {} #dictionary to handle eventual gets
from_server = [] #list to hold messages from server for totall ordered purposes
from_sequencer = {} #dictionary to hole messages from sequencer for totally ordered purposes

#signal handler
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

#method to handle insert and update commands
def insert_and_update(key, value):
	key_value[key] = (value, time.time())

#method to handle get command
def get(key):
	if key in key_value.keys():
		print "The corresponding value for key " + str(key) + " is: " + str(key_value[key][0])
	else:
		print "Key does not exist" 

#message to handle delete command
def delete(key):
	if key in key_value.keys():
		print "Delete of key " + str(key) + " was succesful!"
		del key_value[key]
	else:
		print "Key does not exist"	

#I called an insert, update, delete, or get method using model 3 or 4
def sent_eventual(data):
	global recieved
	eventual_mutex.acquire()
	message = data[10:data.index("from") - 2]
	parsed = message.split(' ')
	key = int(parsed[1])
	k = parsed[0] + " " + parsed[1] + " " + parsed[2]
	if "get" not in message and message in eventual.keys(): #eventual insert or update
		model = int(parsed[len(parsed) - 1])
		value = int(parsed[2])
		if "insert" in message or "update" in message:
			eventual[message] += 1
			if eventual[message] == (model - 2): #check if the number of acknowledgements is equal to the model - 2 i.e. 1 for model 3
				del eventual[message]
				print message + " completed!"
				recieved = True		
	elif k in eventual_read.keys():
		if "get" in message and key in key_value.keys(): #eventual read
			model = int(parsed[2])		
			high_message = ""
			eventual_read[k].append(message) #appends message in list. we want to check timestamps of this list later on.
			curr_val, curr_time = key_value[key]
			if len(eventual_read[k]) >= (model-2):
				if len(eventual_read[k]) == model - 2:
					eventual_read[k].append("get " + str(key) + " " + str(curr_val) + " " + str(curr_time)) #appends my own key value and timestamp
				max_time = -1
				for i in eventual_read[k]:
					parse = i.split(" ")
					curr = float(parse[len(parse)-1])
					if max_time < curr: #decides maximum timestamp
						max_time = curr
						high_message = "The read of key " + parse[1] + " resulted in value " + parse[2] + " with timestamp " + str(max_time)		
				if len(eventual_read[k]) == (model - 1): #check if the number of acknowledgements is equal to the model - 2 i.e. 1 for model 3
					print "Local key: " +  str(key) + ", value: " + str(curr_val) + ", timestamp: " + str(curr_time) 			
					print high_message
				if len(eventual_read[k]) == 4: #if statement to do the repairs after ALL acknowledgements have arrived.
					inconst_repair = high_message.split(" ")
					key_value[key] = int(inconst_repair[8]), float(inconst_repair[11]) #does the inconsistency repair on reads
					rep_val, rep_time = key_value[key]
					print "Read inconsistency repair on key: " + str(key) + ". Value: " + str(rep_val) + " Timestamp: " + str(rep_time)
					del eventual_read[k]
					recieved = True	
		elif "get" in message:
			print "Key does not exist"
			recieved = True	
	global deleted			
	if "delete" in message: #eventual delete
		if deleted == False:
			delete(key)
			deleted = True
			recieved = True	
	eventual_mutex.release()	

#I revieved an eventual consistency command from another node
def received_eventual(data):
	destination = data[0]
	message = data[20:len(data)]
	split_data = data.split(' ')
	timing = float(split_data[len(split_data)-1])
	if data[0] != "B": #because we broadcasted message, we don't want to count a message sent from myself
		parsed = message.split(' ')
		key = int(parsed[1])		
		if "insert" in message or "update" in message: #eventual insert or update request
			print data[20:len(data) - 2]		
			value = int(parsed[2])
			insert_and_update(key, value)
			server.send("SenB " + message + " " + destination + "\n")	
		if "get" in message: #eventual read request from another node
			if key in key_value:		
				(val, curr_time) = key_value[key]
				server.send("SenB " + message + " value: " + str(val) + " time: " + str(curr_time) + " " + destination + "\n") #we send timestamp back
		if "delete" in message: #eventual delete request
			print data[20:len(data) - 2]		
			delete(key)
			server.send("SenB " + message + " " + destination + "\n")		

#function to simulate the delays by sleeping and sends message to server
def sleep_and_send(data, delay):
	#time.sleep(float(delay))
	send_mutex.acquire()
	parsed = data.split(' ')
	model = int(parsed[len(parsed) - 1])
	if model == 1 or model == 2: #linear or sequential, so send to sequencer
		sequencer.send(data + "\n")
		server.send(data + "\n")
	if model == 3 or model == 4: #eventual consistency
		if "update" in data or "insert" in data:
			insert_and_update(int(parsed[1]), int(parsed[2]))
			eventual[data] = 0
		if "get" in data:
			if int(parsed[1]) not in key_value.keys():
				print "Key does not exist"
				return
			eventual_read[data] = []
		server.send("B eventual request: " + data +"\n")
	send_mutex.release()		

#function to read inputs whether they come from file or typed in by user
def readInputs():
	readFile = False
	f = open('B.txt', 'r')
	while 1:
		data = ''
		if readFile == False: #we are not done finishing reading the file
			data = '' #f.readline().replace("\n", '')
			print data
			if data == '': # we hit the end of the file
				f.close()
				readFile = True
		if readFile == True:
			data = raw_input("")
		if "show-all" in data: #the show-all command.
			for a in key_value:
				sys.stdout.write(str(a) + ': ' + str(key_value[a][0]) + '  ')	
			sys.stdout.write("\n")
			continue
		if "search" in data: #search command
			data += " B"
			server.send(data + "\n")
			continue			
		if "send" in data.lower(): #normal message passing from part 1 of the mp
			server.send(data + "\n")
			print "Sent \"" + data[5:len(data)-2] + "\" to " + data[len(data)-1] + ", System time is: " + str(time.time())
			continue	
		if "get" in data.lower() and int(data[len(data)-1]) == 2: #get command for sequential consistency so read value and return
			parsed = data.split(' ')
			key = int(parsed[1])
			get(key)
			continue
		if readFile == True: #gets the delay typed in by the user		
			delay = raw_input("").split()	
		else:
			delay = f.readline().replace("\n", '').split()
		thread.start_new_thread(sleep_and_send, (data, 0))
		global recieved
		while recieved == False: #wait
			b = 1
		recieved = False		
		time.sleep(float(delay[1]))	#sleep to allow for delay between commands.			

def total_order(): #method which gets messages from sequencer and central server and executes them in a totally ordered manner
	global s
	global recieved
	match = 1
	while match == 1:
		if (s+1) in from_sequencer.keys(): #checks to see if the next message in sequence has already been received
			message = from_sequencer[s + 1]
			for string in from_server:
				if message == string:
					parsed = message.split(' ')
					key = int(parsed[1])
					print message[0: len(message) - 2] + " completed!"	
					if "insert" in message or "update" in message:
						value = int(parsed[2])
						insert_and_update(key, value)
						from_server.remove(message)
					if "get" in message:
						get(key)
					if "delete" in message:
						delete(key)		
					del from_sequencer[s + 1]				
					s = s + 1 #increments s so we can get the next message in sequence	
					recieved = True	
		else:
			match = 0		

#method to read data from the server.
def readData_server():
	global server
	while 1:
		data = server.recv(BUFFER_SIZE)
		data = data.replace("\n", "")
		if "received" in data.lower() and ("insert" in data or "get" in data or "update" in data or "delete" in data): #I sent the eventual request
			print data
			thread.start_new_thread(sent_eventual, (data,))
		elif "eventual request" in data: #I did not send, but received the eventual request
			received_eventual(data)	
		elif "received" in data.lower(): #normal message passing from part 1 of mp	
			print data[0:len(data) - 1]
		elif "search" in data: #search command
			parsed = data.split(' ')
			key = int(parsed[1])
			destination = parsed[2]
			if key in key_value.keys():
				message = "B has key " + str(key)
			else:
				message = "B does NOT have key " + str(key)
			server.send("Send " + message + " " + destination + "\n")
		elif "has" in data or "have" in data: #got reply from search command
			print data				
		elif len(data) > 0: #we send all other messages to server. This includes all messages from models 1 and 2
			#print "From server: " + data
			from_server.append(data)	
		data = ""

#reads data from sequencer
def readData_sequencer():
	global sequencer
	while 1:
		data = sequencer.recv(100)
		data = data.replace("\n", "")
		data = data.replace(".", "")
		if len(data) > 0:
			parsed = data.split(' ')
			if "insert" in data or "update" in data:
				#print "From sequencer: " + data
				if re.search('[a-zA-Z]', parsed[4]): #makes sure that string doesn't consist of any alpha characters
					continue
				sequence_num = int(parsed[4])
				message = parsed[0] + " " + parsed[1] + " " + parsed[2] + " " + parsed[3]
			elif "get" or "delete" in data:	
				sequence_num = int(parsed[3])
				message = parsed[0] + " " + parsed[1] + " " + parsed[2]
			from_sequencer[sequence_num] = message
			total_order() #once we get message from sequencer, we call totally ordered to see if we can remove any messages from the queue
			data = ""

def main():
	global server
	global sequencer
	global s
	global recieved
	global deleted
	deleted = False
	recieved = False
	s = 0
	#code to connect myself to server and sequencer
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server.connect((TCP_IP, TCP_PORT))
	sequencer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sequencer.connect((TCP_IP, TCP_PORT_SEQUENCER))
	server_connection = 0
	while server_connection == 0: #don't want to send any messages to server until my connection has been established
		data = server.recv(BUFFER_SIZE)
		if len(data) > 0:
			print data
			server_connection = 1

	#spawn threads to read commands from client/file, to read data from server, and to read data from sequencer.
	thread.start_new_thread(readInputs, ())
	thread.start_new_thread(readData_server, ())
	thread.start_new_thread(readData_sequencer, ())
		
	while 1:
		running = 1

main()		