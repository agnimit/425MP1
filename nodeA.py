#!/usr/bin/env python

import socket
import time
import thread 
import fileinput

TCP_IP = '127.0.0.1'
TCP_PORT = 4001
BUFFER_SIZE = 1024

def readInputs(socket):
	while 1:
		data = raw_input("")
		if len(data) > 0:
			socket.send(data + "\n")
			print "Sent \"" + data[5:len(data)-2] + "\" to " + data[len(data)-1] + ", System time is: " + str(time.time())


def readData():
	while 1:
		data = s.recv(BUFFER_SIZE)
		if len(data) > 0:
			print data
			data = ""




s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))

time.sleep(15)
f = open('nodeACommands.txt', 'r')

for line in f:
	s.send(line + "\n")
	print "Sent \"" + line[5:len(line)-2] + "\" to " + line[len(line)-1] + ", System time is: " + str(time.time())
f.close()

thread.start_new_thread(readInputs, (s,))
thread.start_new_thread(readData, ())
	
while 1:
	a = 3
	

















