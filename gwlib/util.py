#!/usr/bin/env python2
# -*- coding: utf-8 -*-

## file: util.py
## desc: A bunch of misc. utility functions.
## auth: TR
#

from functools import reduce
from sys import argv
import datetime as dt
import json
import os
import subprocess

def chunk_list(l, n):
    """
    Chunks a list into a list of list where each sublist has a size of n.

    arguments
        l: the list being chunked
        n: the max size of each sublist

    returns
        a list generator
    """

    if n == 0:
        n = len(l)

    for i in range(0, len(l), n):
        yield l[i:i + n]

def flatten(outlist):
    """
    Flattens a list of lists into a single list.

    arguments
        outlist: the list being flattened

    returns
        a list
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

    arguments
        fp:   filepath
        data: data being exported to json
        dtag: output string exported with the data
    """

    with open(fp, 'w') as fl:
        if dtag == '':
            print >> fl, json.dumps(data)
        else:
            print >> fl, json.dumps([dtag, data])

def get_today():
    """
    Returns today's date a string in the format YYYY.MM.DD.
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

    with open(fp, 'r') as fl:
        return parse_generic_format(fl.read(), delim)

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

    if ext:
        if ext[0] != '.':
            ext = '.' + ext

        path_ext = ext

    return os.path.join(path_dir, path_base) + path_ext

def get_git_info():
    """
    Returns the current git commit hash.

    returns
        a string containing the current git branch and short seven letter hash
    """

    cmd_hash = ['git', 'rev-parse', '--short', 'HEAD']

    try:
        git_hash = subprocess.check_output(cmd_hash).strip()
    except:
        return ''

    return git_hash

def make_export_header(exe, version):
    """
    Generates a list of strings that can be joined together to serve as a small,
    commented header for data that is saved to a file. The header includes date of
    export, script, arguments and metadata to aid with reproducibility.

    arguments
        exe:     the name of the script
        version: the version of the script
    """

    return [
        '## {} v. {}-'.format(exe, version, get_git_info()),
        '## {}'.format(make_export_tag()),
        '## last updated {}'.format(get_today()),
        '#'
    ]

