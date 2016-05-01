#!/usr/bin/env python
import sys

# --------------------------------------------------------------------------
#This mapper code will input a <date word, value> input file, and move date into 
#  the value field for output
#  read lines, and split lines into key & value
#  if value is ABC or if value is a digit print it out
# --------------------------------------------------------------------------



for line in sys.stdin:
    line       = line.strip()   #strip out carriage return
    key_value  = line.split(",")   #split line, into key and value, returns a list
    key_in     = key_value[0].split(" ")   #key is first item in list
    value_in   = key_value[1]   #value is 2nd item 

    #print key_in
    if value_in.isdigit() or value_in == 'ABC':
        print( '%s\t%s' % (key_in[0], value_in) )
#Note that Hadoop expects a tab to separate key value
#but this program assumes the input file has a ',' separating key value