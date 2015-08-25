#!/usr/bin/python

## file:    util.py
## desc:    A bunch of misc. utility functions.
## vers:    0.1
## auth:    TR
# 

from collections import defaultdict as dd
import datetime as dt
import json

## tup2dict
#
## Finally fucking wrote this because I need this functionality quite often 
## and writing a for loop every time was getting annoying.
## Converts a list of tuples into a dict. Use key to specifiy which tuple 
## index (0/1) should be used as the key (only supports two-element tuples 
## right now). If lst == True, then the dictionary values are treated as 
## lists instead of just some single variable.  
#
## arg, tup, list of tuples that will converted to a dict
## arg, key, tuple index to use as the key
## arg, lst, specifies if dict values should be treated as lists 
## ret, dictionary
#
def tup2dict(tup, key=0, lst=False):
	val = 0 # Tuple index to use as values

	if key == 0:
		val = 1
	if lst:
		tmap = dd(list)
	else:
		tmap = {}
	
	for t in tup:
		if lst:
			tmap[t[key]].append(t[val])
		else:
			tmap[t[key]] = t[val]

	return tmap

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

#### flatten
##
#### This ugly fucker of a list comprehension just takes a list of lists 
#### (inner list lengths don't have to be equal) and flattens everything so we
#### just have one giant list of whatever shit was in the inner lists.
##
def flattenList(outlst):
    return [a for inlst in outlst for a in inlst]

## exportJson
#
## Takes a filepath and some data structure (usually a list of objects) and 
## dumps the data to a JSON file at the given path. 
#
## arg, fp, filepath to write to
## arg, data, data to export
## arg, dtag, OPTIONAL data tag string used to tag the data w/ script arguments
#
def exportJson(fp, data, dtag=''):
	with open(fp, 'w') as fl:
		if dtag == '':
			print >> fl, json.dumps(data)
		else:
			print >> fl, json.dumps([dtag, data])

#### getToday
##
#### Returns today's date as a string in the format YYYY-MM-DD.
##
def getToday():
	now = dt.datetime.now()
	year = str(now.year)
	month = str(now.month)
	day = str(now.day)

	if len(month) == 1:
		month = '0' + month
	if len(day) == 1:
		day = '0' + day

	return year + '-' + month + '-' + day

