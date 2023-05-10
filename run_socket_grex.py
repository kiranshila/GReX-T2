#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 11:20:32 2020

@author: liamconnor
"""

import socket 
from astropy.io import ascii 
import numpy as np
import  pandas as pd

from T2 import socket_grex

HOST = "127.0.0.1"
PORT = 12345

# Use roughly 8 seconds as a gulp size
gulpsize=16384*8

s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# Create a UDP socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# Ensure that you can reconnect 
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# Bind the socket to the port
server_address = (HOST, PORT)
s.bind(server_address)

print("Connected to socket %s:%d" % (HOST, PORT))

candsfile = ['','','','','']

while True:
    data, address = s.recvfrom(4096)
    candstr = data.decode('utf-8')

    # Read time sample to keep track of gulp number
    itime = int(candstr.split('\t')[2])
    gulp_ii = itime // gulpsize
    #print(gulp_ii)
    
    if candsfile==['','','','','']:
        gulp = gulp_ii
        print("Starting gulp is %d" % gulp)
    if gulp_ii-gulp < 0:
        print("Receiving candidates gulps from before current gulp")
        print(gulp_ii, gulp)
        continue
    if gulp_ii-gulp >= len(candsfile):
        print("Receiving candidates too far ahead of current gulp")
        print(gulp_ii, gulp)
        continue
    candsfile[gulp_ii-gulp] += candstr
    # If cand is received with gulp 2 or more than
    # current gulp, process current gulp
    if gulp_ii >= gulp+3:
        if candsfile[0]=='':
            candsfile.pop(0)
            candsfile.append('')
            continue
        print("Clustering gulp", gulp)
        gulp += 1
        socket_grex.filter_candidates(candsfile[0])
        candsfile.pop(0)
        candsfile.append('')
        continue
        
exit()