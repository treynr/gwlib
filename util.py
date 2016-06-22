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

def parse_generic_file(fp, delim='\t'):
    """
    Parses a file that uses the generic format I use for most projects. The
    text format:
        # denotes comments
        blank lines are skipped
        data is organized into columns
        each column is tab separated
    """

    data = []

    with open(fp, 'r') as fl:
        for ln in fl:
            ln = ln.strip()

            if ln[:1] == '#':
                continue
            elif ln == '':
                continue

            ln = ln.split(delim)

            data.append(ln)
    
    return data

def make_geneset(name, abbrev, desc, sp_id, pub_id, grps, score_type, thresh,
                 gene_type, gene_vals, usr_id, cur_id=5, file_id=0):
    """
    Given a shitload of arguments, this function returns a dictionary
    representation of a single geneset. Each key is a different column found
    in the geneset table. Not all columns are (or need to be) represented.

    :type name: str
    :arg name: geneset name

    :type abbrev: str
    :arg abbrev: geneset abbreviation

    :type desc: str
    :arg desc: geneset description

    :type sp_id: int
    :arg sp_id: species ID

    :type pub_id: int
    :arg pub_id: publication ID

    :type grps: str
    :arg grps: comma separated list of group ids (grp_id)

    :type score_type: int
    :arg score_type: geneset value score type

    :type thresh: float
    :arg thresh: geneset value threshold

    :type gene_type: int
    :arg gene_type: gene ID type

    :type gene_vals: list
    :arg gene_vals: tuples of ode_gene_ids and their values

    :type usr_id: int
    :arg usr_id: user ID

    :type cur_id: int
    :arg cur_id: curation tier

    :type file_id: int
    :arg file_id: file ID

    :ret dict: 
    """

    gs = {}

    ## If the geneset isn't private, neither should the group be
    if cur_id != 5 and grps.find('-1'):
        grps = grps.split(',')
        grps = map(lambda x: if x == '-1': '0' else x, grps)
        grps = ','.join(grps)

    gs['gs_name'] = name
    gs['gs_abbreviation'] = abbrev
    gs['gs_description'] = desc
    gs['sp_id'] = int(sp_id)
    gs['gs_groups'] = grps
    gs['pub_id'] = pub_id
    gs['gs_threshold_type'] = int(score_type)
    gs['gs_threshold'] = thresh
    gs['gs_gene_id_type'] = int(gene_type)
    gs['usr_id'] = int(usr_id)
    ## Not a column in the geneset table; but these are processed later since
    ## each geneset_value requires a gs_id
    gs['geneset_values'] = gene_vals

    ## Other fields we can fill out
    gs['gs_count'] = len(vals)
    gs['cur_id'] = cur_id

    return gs

