#!/usr/bin/env python
import sys

# --------------------------------------------------------------------------
#This reducer code will input a <word, value> input file, and join words together
# Note the input will come as a group of lines with same word (ie the key)
# As it reads words it will hold on to the value field
#
# It will keep track of current word and previous word, if word changes
#   then it will perform the 'join' on the set of held values by merely printing out 
#   the word and values.  In other words, there is no need to explicitly match keys b/c
#   Hadoop has already put them sequentially in the input 
#   
# At the end it will perform the last join
#read lines and split lines into key & value
#if a key has changed (and it's not the first input)
#then check if ABC had been found and print out key and running total,
#if value is ABC then set some variable to mark that ABC was found (like abc_found = True)
#otherwise keep a running total of viewer counts
# --------------------------------------------------------------------------

abc_found = False              #initialize these variables
running_total = 0
prev_key = ""

for line in sys.stdin:
    line       = line.strip()       #strip out carriage return   #split line, into key and value, returns a list
    this_key, value = line.split("\t")

    if this_key != prev_key:
        if abc_found:
            print( "{0}\t{1}".format(prev_key, running_total)) 
        abc_found = False
        running_total = 0

    prev_key = this_key
    if value.isdigit():
        running_total += int(value)
    else:
        abc_found = True

if abc_found:
    print( "{0}\t{1}".format(prev_key, running_total)) 
