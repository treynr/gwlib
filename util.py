#!/usr/bin/env python2

## file:    util.py
## desc:    A bunch of misc. utility functions.
## auth:    TR
# 

from sys import argv
import datetime as dt
import json
import os
import subprocess

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

def make_export_tag():
    """
    Generates a string using script arguments. This is attached to output so we
    know the exact commands used to generate a particular data file.

    returns
        a string containing the script name and any arguments.
    """

    return reduce(lambda x, y: x + ' ' + y, argv)

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

def parse_generic_format(s, delim='\t'):
    """
    Parses a string that uses the generic format I use for most projects. The
    text format:
        # denotes comments
        blank lines are skipped
        data is organized into columns
        each column is tab separated

    arguments
        s:      the string being parsed
        delim:  the column delimiter to use during parsing

    returns
        a list of lists, each inner list a single row from the file
        e.g.
            [
                [col0, col1, col2],
                [col0, col1, col2]
            ]
    """
    data = []

    for ln in iter(s.splitlines()):
        ln = ln.strip()

        if ln[:1] == '#':
            continue
        elif ln == '':
            continue

        ln = ln.split(delim)

        data.append(ln)
    
    return data

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
        return parse_generic_format(fl.read(), delim)

def make_geneset(
    name, abbrev, desc, sp_id, pub_id, grps, score_type, thresh,
    gene_type, gene_vals, at_id=None, usr_id=0, cur_id=5, 
    file_id=0, pmid=None, annos=[], uri=None
):
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
        grps = map(lambda x: '0' if x == '-1' else x, grps)
        grps = ','.join(grps)

    gs['gs_name'] = name
    gs['gs_abbreviation'] = abbrev
    gs['gs_description'] = desc
    gs['sp_id'] = int(sp_id)
    gs['gs_groups'] = grps
    gs['pub_id'] = pub_id
    gs['pmid'] = pmid
    gs['gs_threshold_type'] = int(score_type)
    gs['gs_threshold'] = thresh
    gs['gs_gene_id_type'] = int(gene_type)
    gs['usr_id'] = int(usr_id)
    ## Not a column in the geneset table; but these are processed later since
    ## each geneset_value requires a gs_id
    gs['geneset_values'] = gene_vals
    gs['at_id'] = at_id

    ## Other fields we can fill out
    gs['gs_count'] = len(gene_vals)
    gs['cur_id'] = cur_id

    ## Ontology annotations
    gs['annotations'] = annos

    ## URI to another resource
    gs['gs_uri'] = uri

    return gs

def make_geneset2(gs):
    """
    Converts the fields in the given geneset dictionary into proper column
    names for the geneset table. Attempts to map casually named fields to
    proper column names and substitute missing fields with sensible defaults.

    arguments
        gs: gene set dict object

    returns
        an altered dict 
    """

    geneset = {}


    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])

    if 'name' in gs:
        geneset['gs_name'] = gs['name']
    elif 'gs_name' in gs:
        geneset['gs_name'] = gs['gs_name']

    if 'description' in gs:
        geneset['gs_description'] = gs['description']
    elif 'gs_description' in gs:
        geneset['gs_description'] = gs['gs_description']

    if 'abbreviation' in gs:
        geneset['gs_abbreviation'] = gs['abbreviation']
    elif 'gs_abbreviation' in gs:
        geneset['gs_abbreviation'] = gs['gs_abbreviation']

    if 'species' in gs:
        geneset['sp_id'] = int(gs['species'])
    elif 'sp_id' in gs:
        geneset['sp_id'] = int(gs['cur_id'])

    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])

    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])

    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])

    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])

    if 'tier' in gs:
        geneset['cur_id'] = int(gs['tier'])
    elif 'cur_id' in gs:
        geneset['cur_id'] = int(gs['cur_id'])
    ## If the geneset isn't private, neither should the group be
    if cur_id != 5 and grps.find('-1'):
        grps = grps.split(',')
        grps = map(lambda x: '0' if x == '-1' else x, grps)
        grps = ','.join(grps)

    gs['gs_name'] = name
    gs['gs_abbreviation'] = abbrev
    gs['gs_description'] = desc
    gs['sp_id'] = int(sp_id)
    gs['gs_groups'] = grps
    gs['pub_id'] = pub_id
    gs['pmid'] = pmid
    gs['gs_threshold_type'] = int(score_type)
    gs['gs_threshold'] = thresh
    gs['gs_gene_id_type'] = int(gene_type)
    gs['usr_id'] = int(usr_id)
    ## Not a column in the geneset table; but these are processed later since
    ## each geneset_value requires a gs_id
    gs['geneset_values'] = gene_vals
    gs['at_id'] = at_id

    ## Other fields we can fill out
    gs['gs_count'] = len(gene_vals)
    gs['cur_id'] = cur_id

    return gs

def manipulate_path(path, pre='', post='', ext='', delim='-'):
    """
    Takes a filepath and modifies it based on user provided parameters.

    arguments
        path:   the path being modified
        pre:    adds a prefix string to the filename of the given path
                e.g. 
                    path = /some/file.txt, prefix = 'pre' 
                    -> /some/pre-file.txt
        post:   adds a postfix string to the filename of the given path
                e.g. 
                    path = /some/file.txt, prefix = 'post' 
                    -> /some/file-post.txt
        ext:    if the file is lacking an extension, this specifies the
                extension to add
        delim:  the delimiter to use when pre/postfixing strings
    """

    path_dir, path_base = os.path.split(path)
    path_base, path_ext = os.path.splitext(path_base)

    if pre:
        path_base = pre + delim + path_base

    if post:
        path_base = path_base + delim + post

    if not path_ext and ext:
        if ext[0] != '.':
            ext = '.' + ext

        path_ext = ext

    return os.path.join(path_dir, path_base) + path_ext

def get_git_info():
    """
    Returns the current git branch and commit hash.

    returns
        a string containing the current git branch and short seven letter hash
    """

    cmd_hash = ['git', 'rev-parse', '--short', 'HEAD']
    cmd_branch = ['git', 'symbolic-ref', '--short', 'HEAD']
    git_hash = subprocess.check_output(cmd_hash).strip()
    git_branch = subprocess.check_output(cmd_branch).strip()

    return '%s@%s' % (git_branch, git_hash)

