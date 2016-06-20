#!/usr/bin/env python2

## file:    util.py
## desc:    A bunch of misc. utility functions.
## auth:    TR
# 

from collections import defaultdict as dd
import datetime as dt
import json

def chunk_list(l, n):
    """
    Chunks a list into a list of list where each sublist has a size of n.

    :type l: list
    :arg l: the list being chunked

    :ret list: chunks
    """

    if n == 0:
        n = len(l)

    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def flatten(outlist):
    """
    Flattens a list of lists into a single list.

    :type outlist: list
    :arg outlist: 

    :ret list:
    """

    return [a for inlist in outlist for a in inlist]

def export_json(fp, data, dtag=''):
    """
    Exports a python data structure into a JSON string and saves the result to
    a given file.

    :type fp: str
    :arg fp: filepath

    :type data: ?
    :arg data: the shit being exported

    :type dtag: str
    :arg dtag: An optional tag prepended to the exported data
    """

    with open(fp, 'w') as fl:
        if dtag == '':
            print >> fl, json.dumps(data)
        else:
            print >> fl, json.dumps([dtag, data])

def get_today():
    """
    Returns today's date a string in the format YYYY.MM.DD.

    :ret str: the date
    """

    now = dt.datetime.now()
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)

    if len(month) == 1:
        month = '0' + month

    if len(day) == 1:
        day = '0' + day

    return year + '.' + month + '.' + day

