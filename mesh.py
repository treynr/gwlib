#!/usr/bin/python

## file:    mesh.py
## desc:    MeSH function library for retrieving MeSH and pubmed information
##          from NCBI. Uses the NCBI e-util URLs. 
## vers:    0.1
## auth:    TR
# 

import util as utl

## getArticleInfo
#
## Retrieves publication info (including MeSH terms) for a given PubMed 
## article ID. Uses NCBI's e-utils, specifically EFetch, to get the info.
#
## TODO: There has to be a better way of doing this--retrieving MeSH terms 
## that is. Using EFetch is slow and shit. And Biopython sucks too.
#
## arg, id, PubMed article ID
## ret, string of the publication info in whatever horrible format NCBI uses
#
def getArticleInfo(id):
    import urllib as ul # For NCBI e-utils crap

    # NCBI efetch util url, no xml because it's pig disgusting
    #url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
    #       'db=pubmed&id=')
    url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
           'tool=geneweaver_gene2mesh&email=timothy_reynolds@baylor.edu'
           '&db=pubmed&rettype=medline&retmode=text&id=')

    if id is None:
        return None
    else:
        for i in id:
            url += str(id) + ','# Just in case the uid is an int for some reason

    # Return the article information
    return ul.urlopen(url).read()

## isolateTerms
#
## Parses out the MeSH terms from a publication info string (see above function
## getArticleInfo). The format for pub info is the same as the ASCII MeSH 
## format (see data/mesh2014.bin). The function loops, checking for PMIDs and
## collects the MeSH terms (under the MH identifier) for each.
#
## arg, dat, string of the publication info in whatever horrible format NCBI 
##           uses
## ret, pubmed2mesh dict, keys are pubmed IDs and values are lists of terms
## ret, set of all MeSH terms
#
def isolateTerms(dat):
    dat = dat.split('\n')   # split into lines
    p2m = {}                # dict, pubmed2mesh
    pub = []                # temporary pubmed ID
    all = []                # set of all MesH terms

    for ln in dat:
        if ln[:4] == 'PMID':
            pub = ln.split('-')[1].strip()  # PMID-12345
            p2m[pub] = []

        elif ln[:3] == 'MH ':
            mh = ln.split('-')[1].strip()   # MH - some term
            # some MHs have / which indicates a sub-heading, I think, and some
            # also have *, I have no idea what the hell those signify
            mh = mh.split('/')[0].strip('*').strip(' ')
            p2m[pub].append(mh)
            all.extend(mh)
        
    return (p2m, set(mh))

def getArticleTerms(pids):
    import time
    # was initially reading 500 articles at a time, but the jerk software at 
    # NCBI kept closing the connection
    pids = utl.chunkList(pids, 300) 
    info = ''

    for p in pids:
        info = getArticleInfo(p)
        # NCBI requires you only perform three requests a second
        time.sleep(0.3)         

    return isolateTerms(info)
    
## readG2p
#
## Opens and reads in the gene2pubmed data file. The information is stored in
## a dict where each gene is a key and their values are a list of publications.
#
## arg, fp, path to the gene2pubmed file
## ret, dict of genes --> pubmed IDs
#
def readG2p(fp):
    g2p = {}

    with open(fp, 'r') as fh:
        # Reads each line in the file and splits gene and pubmed IDs
        for l in fh:
            gp = l.strip().split('\t')

            # Store in the dict, key = gene id and val = pubmed id list
            g2p.setdefault(gp[0], set())
            g2p[gp[0]].add(gp[1])

    return g2p

## readP2m
#
## Opens and reads in a pubmed2mesh data file. The information is stored in
## a dict where each pubmed ID is a key and their values are a list of 
## MeSH terms.
#
## arg, fp, path to the gene2pubmed file
## ret, dict of pubmed IDs --> MeSH terms
#
def readP2m(fp):
    p2m = {}    # pubmed2mesh associations
    all = set() # set of all MeSH terms

    # Reads each line in the file and splits the pubmed IDs and MeSH terms
    with open(fp, 'r') as fh:
        for l in fh:
            # m[0] = pubmed ID, everything else = MeSH terms
            m = l.strip().split('|')

            # Some null terms were getting into the lists and fucking things up
            if '' in m:
                m.remove('')
            # Store pubmed ID + term associations in a dict
            p2m[m[0]] = m[1:]
            # Store all MeSH terms
            all.update(m[1:])

    return (p2m, all)

## makeP2m
#
## Writes the pubmed2mesh data to a file.
#
## arg, fp, filepath being written to
## arg, p2m, pubmed2mesh dict generated from the function getArticleTerms
#
def saveP2m(fp, p2m):
    with open(fp, 'w') as f:
        for k, v in p2m.items():
            print >> f, (str(k) + '|' + '|'.join(v))

## saveG2m
#
## Writes the gene2mesh or mesh2gene data to a file. If the weight dictionary
## is given, it writes that too using the same file path but with the added
## extension '.wts'.
#
## arg, fp, filepath being written to
## arg, g2m, gene2mesh (or mesh2gene) dict
## arg, wts, weight for the gene-mesh associations
#
def saveG2m(fp, g2m, wts=None, tab=False, gnames=None):
    with open(fp, 'w') as f:
        for k, v in g2m.items():
            if len(v) <= 1:
                continue
            if tab:
                # This was added for Charles' mesh2gene data and can now
                # probably be deleted, the gnames argument above can also
                # be deleted
                print >> f, (str(k) + '\t' + '\t'.join(map(lambda x: gnames[x], filter(lambda x: gnames.has_key(x), v))))
            else:
                print >> f, (str(k) + '|' + '|'.join(v))

    if wts:
        with open(fp + '.wts', 'w') as f:
            for gene in wts.keys(): # Dict of dicts
                for term in wts[gene].keys():
                    print >> f, (str(gene) + '|' + str(term) + '|' + 
                                str(wts[gene][term]))

def readG2m(fp):
    import os

    g2m = {}    # gene2mesh / mesh2gene dict
    wts = None  # weights

    # Reads each line in the file and splits the pubmed IDs and MeSH terms
    with open(fp, 'r') as fh:
        for l in fh:
            # m[0] = gene, everything else = MeSH terms (opposite for m2g)
            m = l.strip().split('|')
            g2m[m[0]] = m[1:]#map(int, m[1:])

    # A weights file should probably maybe more than likely exist...?
    if os.path.exists(fp + '.wts'):
        with open(fp + '.wts', 'r') as fh:
            wts = {}
            for l in fh:
                # m[0] = gene, m[1] = term, m[2] = weight
                m = l.strip().split('|')
                if wts.has_key(m[0]):
                    wts[m[0]][m[1]] = m[2]
                else:
                    wts[m[0]] = {}
                    wts[m[0]][m[1]] = m[2]

    return (g2m, wts)

## makeG2m
#
## Creates the gene -> MeSH term mapping and returns the result as a dict. 
#
## 08/01/2014 - Now works a bit differently. First, the dict key/values are
## reversed (see below). Genes are now only associated with a MeSH term if they
## are referenced by a minimum of two publications. And finally, the function
## now requires MeSH tree data--if a gene is associated with two MeSH terms,
## one of which is a child of another, the most granular MeSH term is chosen
## (as it is the closure of the other) and the ancestor term is discarded. 
#
## arg, g2p, gene2pubmed dict
## arg, p2m, pubmed2mesh dict
## arg, tree, MeSH tree data
## arg, m2g, OPTIONAL argument, if true the function makes a 
##           mesh2gene dict instead of gene2mesh
## ret, gene2mesh dict, keys are terms, values are all genes associated with 
##      that term (used to be the other way around, but it's easier to insert 
##      data into the DB this way)
#
def makeG2m(g2p, p2m, tree, make_m2g=True, weight=False):
    from collections import defaultdict
    if (not g2p) or (not p2m): 
        return None

    # dict of gene -> dicts, second dict is mesh term -> int (a count)
    g2m_count = defaultdict(lambda: defaultdict(int))
    g2m = defaultdict(set)
    m2g = defaultdict(set)  # mesh --> gene set
    # Traverse the gene2pubmed dict. As a reminder, g = a particular gene and
    # p = [set of pubmed IDs]
    for gene, pubs in g2p.items():
        for p in pubs:
            for m in p2m[p]:
                g2m_count[gene][m] += 1 # gene -> mesh count

    # Checks the counts for gene -> mesh associations, anything less than two
    # is discarded. Creates a g2m mapping
    for gene, mdict in g2m_count.items():
        for mesh, count in mdict.items():
            gran = True
            if count > 1:
                #m2g[mesh].add(gene)
                # Checks to see if this mesh term is an ancestor to another 
                # term already associated with this gene. If it is, it isn't
                # added (we keep only the most granular, significant term)
                for term in g2m[gene]:
                    if g2m_count[gene][term] > 1:
                        if term in tree[mesh]['ancestors']:
                            gran = False
                if gran:
                    g2m[gene].add(mesh)

    # Create weights which will later be added to the geneset values. The 
    # weight is simply normalized percent publications the term-gene 
    # association is found in.
    if weight:
        wmin = 9999.0
        wmax = 0.0
        publen = len(p2m.keys())
        for gene in g2m_count.keys():
            for term in g2m_count[gene].keys():
                if g2m_count[gene][term] > 1:
                    w = float(g2m_count[gene][term]) / float(publen)
                    g2m_count[gene][term] = w

                    if w < wmin: 
                        wmin = w
                    if w > wmax:
                        wmax = w

        # Normalization
        for gene in g2m_count.keys():
            for term in g2m_count[gene].keys():
                g2m_count[gene][term] = (g2m_count[gene][term] - wmin) / float(wmax - wmin)

    if not make_m2g:
        if weight:
            return (g2m, g2m_count)
        else:
            return g2m

    # Convert the gene2mesh dict to mesh2gene--makes it easier to add 
    # everything to the DB
    for gene, meshes in g2m.items():
        for mesh in meshes:
            m2g[mesh].add(gene)
    # Iterates over the gene2mesh dict and keeps only the most granular term
    # associated with a gene, all other ancestral terms are discarded
    # TODO, think of a faster, more efficient way to do this
    #for gene in g2m.keys():
    #    terms = g2m[gene]




        #print m2g
        #exit()
            #if pubs in p2m:
                #g2m[g].update(p2m[pubs])
            # This statement is only called in the case of an incomplete pub2mesh
            # data file, which should never be the case.
            #else: 
            #    g2m[g].update([])

    #return g2m
    if weight:
        return (m2g, g2m_count)
    else:
        return m2g

## loadMeshData
#
## Loads all the MeSH data from the ASCII format, generating trees and closures
## for each term as well. Heavily based off Jeremy Jay's MeshTermLoader
## implementation, so creds to him for most of it. Returns two dicts: one of
## each MeSH term and any important data associated with that term. The
## second dict is the MeSH tree and closures for each term. Will be useful if
## we want to filter other terms below or above another term in the tree
## hierarchy.
## TODO: This monolith of a function should probably be modularized a bit more.
## Also, may want to look into some additional descriptor elements like
## (PRINT) ENTRY, FX (forward cross reference), etc...
#
## arg, fp,  path to the MeSH ASCII file
## arg, nmap, OPTIONAL 
## ret, dict of MeSH terms (keys) and their data (vals)
## ret, dict of term closures including ancestor, parent, and child terms
#

def loadMeshData(fp, nmap=False):
    with open(fp, 'r') as fh:
        # Remove \r\n and get a list of the stripped lines
        lines = [l.strip() for l in fh]

    # Returns indices for each location of *NEWRECORD in the lines list
    inds = [i for i, j in enumerate(lines) if j == '*NEWRECORD']

    # Slices the list of lines into individual records using the indices (inds)
    # of each *NEWRECORD string, and then appends the last record to the list
    # Fuck, this is some ugly python
    recs = [lines[inds[i]:inds[i + 1] - 1] for i in range(len(inds) - 1)]
    recs.append(lines[inds[-1]:])

    terms = {} # Dict. where key = term and val = data from MeSH dump
    treenums = {} # Dict. where key = tree number and val = MeSH term
    mtree = {} # Dict. of MeSH tree structure and closure, each key is a term

    import re
    # Iterate over all the records, extracting only the necessary information and
    # storing that in a dict
    for r in recs:
        data = {'MN' : list(), 'nodes' : list()}

        # for each descriptor in the record, store relevant information
        for s in r:
            if s[:3] == 'MH ': # MeSH Heading 
                data['MH'] = s.split(' = ')[1]

            elif s[:3] == 'UI ': # Unique ID
                data['UI'] = s.split(' = ')[1]

            elif s[:3] == 'MN ': # MeSH tree number, there may be more than one
                ts = s.split(' = ')[1]
                data['MN'].append(ts)
                # Should be safe as MH always comes before MN in the MeSH file...
                # Associates the mesh term (MH) with the newly added tree number
                treenums[data['MN'][-1]] = data['MH']

                # Create a list of each previous node in the tree (ancestors)
                # e.g A01.111.236 --> [A01, A01.111, A01.111.236]
                data['nodes'].extend(( # Lines longer than 80 chars are of the devil 
                    [ts[:i] for i in xrange(len(ts)) if ts.find('.', i) == i]))
                data['nodes'].append(ts)

            elif s[:3] == 'UI ': # Unique identifier
                data['UI'] = s.split(' = ')[1]

        # Each MeSH term stored in a dict where key = term and val = relevant data
        terms[data['MH']] = data

    # Maps MeSH tree node numbers (e.g A01.141) to terms
    node_map = {}
    # Generate the MeSH tree (sorta) while concurrently generating closures for
    # each term. I guess this could also be made by dl'ing the actual
    # MeSH trees in ASCII format... 
    for t in terms.keys():
        mtree[t] = {'children' : set(), 'parents' : set(), 
                    'ancestors' : set(), 'node' : set()}

        # Store the node
        mtree[t]['node'].update(terms[t]['MN'])

        for mn in terms[t]['MN']:
            node_map[mn] = t

        # Retrieves all the parents for each tree number. Parents are just the
        # tree number minus tha last node (three numbers)
        pars = [s.rsplit('.', 1)[0] for s in terms[t]['MN']]

        # The tree (and closures) are made using terms and not the actual 
        # tree numbers
        for node in terms[t]['nodes']:
            mtree[t]['ancestors'].add(treenums[node]) # Add each ancestor node
            # added 8/3/2014, remove the term itself from the ancestors list;
            # I don't think a term is its own ancestor and this fucks up 
            # gene2mesh creation
            mtree[t]['ancestors'].discard(t)

            # If it's a parent, add it to the parents set
            if node in pars:
                mtree[t]['parents'].add(treenums[node])

    # Then, add all the children for each term
    for t in mtree.keys():
        for p in mtree[t]['parents']:
            mtree[p]['children'].add(t)

    if nmap:
        return (terms, mtree, node_map)

    return (terms, mtree)


#g2p = readG2p('../data/ncbi.gene2pubmed.data')
##plst = map(lambda x: x[0], utl.chunkList(g2p.values(), 15))
#plst = []
#for v in g2p.values():
#    plst.extend(list(v))
#plst = list(set(plst))
#print 'here'
#for p in utl.chunkList(plst, 300):
#    getArticleInfo(p)
#    exit()
