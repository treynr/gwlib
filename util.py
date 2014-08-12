#!/usr/bin/python

## file:    util.py
## desc:    A bunch of misc. utility functions.
## vers:    0.1
## auth:    TR
# 

## splitList
#
## Splits a comma separated string into a list. Removes any extraneous spaces
## from each word too.
#
## arg, strs, string to split
## ret, list of words
#
def splitList(strs):
    if strs is None: 
        return None
    return [l.strip() for l in strs.split(',')] 

## chunkList
#
## Takes a list and chunks it into lists of size n.
#
## arg, l, the list to chunk
## arg, n, the size of the newly generated sublists/chunks
## ret, list of lists
#
def chunkList(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

