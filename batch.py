#!/usr/bin/env python

## file:    batch.py
## desc:    Batch file reader and writer. This is a variant of the batch parser
##          found in the GW2 sauce. It works the same way but decouples most of
##          the DB code and adds support for specifying tiers, attributions, 
##          and user IDs in the batch file.
## auth:    Baker
##          TR
#

## multisets because regular sets remove duplicates, requires python 2.7
from collections import Counter as mset
from collections import defaultdict as dd
import datetime
import json
import random
import re
import urllib2 as url2

import db
import util

        ## UTILITY ##
        #############

def read_file(fp):
    """
    Reads a file and splits it into lines.
    """

    with open(fp, 'r') as fl:
        return fl.read().split('\n')

def make_digrams(s):
    """
    Recursively creates an exhaustive list of digrams from the given string.

    :type s: str
    :arg s: string to generate digrams with

    :ret list: list of digram strings
    """

    if len(s) <= 2:
        return [s]

    b = makeDigrams(s[1:])
    b.insert(0, s[:2])

    return b

def calculate_str_similarity(s1, s2):
    """
    Calculates the percent similarity between two strings. Meant to be a
    replacement for PHP's similar_text function, which old GeneWeaver uses
    to determine the right microarray platform to use.
    Couldn't find how similar_text was implemented (just that it used some
    algorithm in the book 'Programming Classics' by Oliver) so this function
    was written achieve similar results. This algorithm uses digrams and 
    their intersections to determine percent similarity. It is calculated
    as:

    sim(s1, s2) = (2 * intersection(digrams(s1), digrams(s2)) /
                   |digrams(s1) + digrams(s2)|

    :type s1: str
    :arg s1: string #1

    :type s2: str
    :arg s2: string #2

    :ret float: percent similarity
    """

    sd1 = makeDigrams(s1)
    sd2 = makeDigrams(s2)
    intersect = list((mset(sd1) & mset(sd2)).elements())

    return (2 * len(intersect)) / float(len(sd1) + len(sd2))

        ## PARSERS ##
        #############

def parse_score_type(s):
    """
    Attempts to parse out the score type and any threshold value
    from a given string.
    Acceptable score types and threshold values include:
        Binary
        P-Value < 0.05
        Q-Value < 0.05
        0.40 < Correlation < 0.50
        6.0 < Effect < 22.50
    The numbers listed above are only examples and can vary depending on
    geneset. If those numbers can't be parsed for some reason, default values
    (e.g. 0.05) are used. The score types are converted into a numeric
    representation:
        P-Value = 1
        Q-Value = 2
        Binary = 3
        Correlation = 4
        Effect = 5

    :type s: str
    :arg s: string containing score type and possibly threshold value(s)

    :return tuple: (gs_threshold_type, gs_threshold, errors)
    """

    ## The score type, a numeric value but currently stored as a string
    stype = ''
    ## Default theshold values
    thresh = '0.05'
    thresh2 = '0.05'
    error = ''

    ## Binary threshold is left at the default of 1
    if s.lower() == 'binary':
        stype = '3'
        thresh = '1'

    elif s.lower().find('p-value') != -1:
        ## Try to find the threshold, this regex is from the PHP func.
        ## my regex: ([0-9]?\.[0-9]+)
        m = re.search(r"([0-9.-]{2,})", s.lower())
        stype = '1'

        if m:
            thresh = m.group(1)
        else:
            error = 'No threshold specified for P-Value data. Using p < 0.05.'

    elif s.lower().find('q-value') != -1:
        m = re.search(r"([0-9.-]{2,})", s.lower())
        stype = '2'

        if m:
            thresh = m.group(1)
        else:
            error = 'No threshold specified for Q-Value data. Using q < 0.05.'

    elif s.lower().find('correlation') != -1:
        ## This disgusting regex is from the PHP function
        ## And it sucks. It breaks on some input, might have to change this
        ## later.
        m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
        stype = '4'

        if m:
            thresh = m.group(1) + ',' + m.group(2)
        else:
            thresh = '-0.75,0.75'
            error = ('No thresholds specified for Correlation data.'
                     ' Using -0.75 < value < 0.75.')

    elif s.lower().find('effect') != -1:
        ## Again, PHP regex
        m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
        stype = '5'

        if m:
            thresh = m.group(1) + ',' + m.group(2)
        else:
            thresh = '0,1'
            error = ('No thresholds specified for Effect data.'
                     ' Using 0 < value < 1.')

    else:
        error = 'An unknown score type (%s) was provided.' % s

    return (stype, thresh, error)


#### getPubmedInfo
##
#### Retrieves Pubmed article info from the NCBI servers using the NCBI eutils.
#### The result is a dictionary whose keys are the same as the publication
#### table. The actualy return value for this function though is a tuple. The
#### first member is the dict, the second is any error message.
##
def getPubmedInfo(pmid):
    ## URL for pubmed article summary info
    url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?'
           'retmode=json&db=pubmed&id=%s') % pmid
    ## NCBI eFetch URL that only retrieves the abstract
    url_abs = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
               '?rettype=abstract&retmode=text&db=pubmed&id=%s') % pmid

    ## Sometimes the NCBI servers shit the bed and return errors that kill
    ## the python script, we catch these and just return blank pubmed info
    try:
        res = url2.urlopen(url).read()
        res2 = url2.urlopen(url_abs).read()

    except url2.HTTPError:
        er = ('Error! There was a problem accessing the NCBI servers. No '
              'PubMed info for the PMID you provided could be retrieved.')
        return ({}, er)

    pinfo = {}
    res = json.loads(res)

    ## In case of KeyErrors...
    try:
        pub = res['result']
        pub = pub[pmid]

        pinfo['pub_title'] = pub['title']
        pinfo['pub_abstract'] = res2
        pinfo['pub_journal'] = pub['fulljournalname']
        pinfo['pub_volume'] = pub['volume']
        pinfo['pub_pages'] = pub['pages']
        pinfo['pub_pubmed'] = pmid
        pinfo['pub_authors'] = ''

        ## Author struct {name, authtype, clustid}
        for auth in pub['authors']:
            pinfo['pub_authors'] += auth['name'] + ', '

        ## Delete the last comma + space
        pinfo['pub_authors'] = pinfo['pub_authors'][:-2]

    except:
        er = ('Error! The PubMed info retrieved from NCBI was incomplete. No '
              'PubMed data will be attributed to this geneset.')
        return ({}, er)

    return (pinfo, '')

def parse_batch_syntax(lns):
    """
    Parses a batch file according to the format listed on
    http://geneweaver.org/index.php?action=manage&cmd=batchgeneset

    :type lns: list
    :arg lns: each of the lines found in a batch file

    :ret tuple: (geneset list, warning list, error list)
    """

    genesets = []
    ## geneset_values, here as a list of tuples (symbol, value)
    gsvals = []  
    ## geneset abbreviation
    abbr = ''
    ## geneset name
    name = ''
    ## geneset description
    desc = ''
    ## gene ID type (gs_gene_id_type)
    gene = ''
    ## PubMed ID, later converted to a GW pub_id
    pub = None
    ## Group identifier, default is private (gs_groups)
    group = '-1'
    ## Score type (gs_threshold_type)
    stype = ''
    ## Threshold value (gs_threshold)
    thresh = '0.05'
    ## Species name, later converted to a GW sp_id
    spec = ''
    ## Curation tier
    cur_id = 5
    usr_id = 0
    ## Attribution
    at_id = None
    ## Critical errors discovered during parsing
    cerr = ''
    ## Non-critical errors discovered during parsing
    ncerr = []
    ## Critical errors discovered during parsing
    errors = []
    ## Non-critical errors discovered during parsing
    warns = [] 

    gene_types = db.get_gene_types()
    species = db.get_species()
    platforms = db.get_microarray_types()

    def reset_add_geneset():
        gs = util.make_geneset(name, abbr, desc, spec, pub, group, stype, 
                               thresh, gene, gsvals, at_id, usr_id, cur_id)
        abbr = ''
        desc = ''
        name = ''
        gsvals = []
        genesets.append(gs)

    for gdb_name, gdb_id in gene_types.items():
        gene_types[gdb_name.lower()] = gdb_id

    for sp_name, sp_id in species.items():
        species[sp_name.lower()] = sp_id

    for pf_name, pf_id in platforms.items():
        platforms[pf_name.lower()] = pf_id

    for i in range(len(lns)):
        lns[i] = lns[i].strip()

        ## These are special additions to the batch file that allow curation
        ## tiers, user IDs, and attributions to be specified.
        #
        ## Lines beginning with 'T' are Tier IDs
        if lns[i][:2].lower() == 't ':
            if gsvals:
                reset_add_geneset()

            cur_id = int(lns[i][1:].strip())

        elif lns[i][:2].lower() == 'u ':
            if gsvals:
                reset_add_geneset()

            usr_id = int(lns[i][1:].strip())

        elif lns[i][:2].lower() == 'd ':
            if gsvals:
                reset_add_geneset()

            at_id = int(lns[i][1:].strip())

        ## :, =, + is required for each geneset in the batch file
        #
        ## Lines beginning with ':' are geneset abbreviations (REQUIRED)
        if lns[i][:1] == ':':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                reset_add_geneset()
                #gs = util.make_geneset(name, abbr, desc, spec, pub, group,
                #                       stype, thresh, gene, gsvals)
                ### Start a new dataset
                #abbr = ''
                #desc = ''
                #name = ''
                #gsvals = []
                #genesets.append(gs)

            abbr = lns[i][1:].strip()

        ## Lines beginning with '=' are geneset names (REQUIRED)
        elif lns[i][:1] == '=':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                reset_add_geneset()
                #gs = util.make_geneset(name, abbr, desc, spec, pub, group,
                #                       stype, thresh, gene, gsvals)
                ### Start a new dataset
                #abbr = ''
                #desc = ''
                #name = ''
                #gsvals = []
                #genesets.append(gs)

            name = lns[i][1:].strip()

        ## Lines beginning with '+' are geneset descriptions (REQUIRED)
        elif lns[i][:1] == '+':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                reset_add_geneset()
                #gs = util.make_geneset(name, abbr, desc, spec, pub, group,
                #                       stype, thresh, gene, gsvals)
                ### Start a new dataset
                #abbr = ''
                #desc = ''
                #name = ''
                #gsvals = []
                #genesets.append(gs)

            desc += lns[i][1:].strip()
            desc += ' '

        ## !, @, %, are required but can be omitted from later sections if
        ## they don't differ from the first.
        #
        ## Lines beginning with '!' are score types (REQUIRED)
        elif lns[i][:1] == '!':
            score = lns[i][1:].strip()
            score = parse_score_type(score)

            ## Indicates a critical error has occured (no score type w/ an
            ## error message)
            if not score[0] and score[2]:
                errors.append(score[2])

            else:
                stype = score[0]
                thresh = score[1]

            ## Any warnings
            if score[0] and score[2]:
                warns.append(score[2])

        ## Lines beginning with '@' are species types (REQUIRED)
        elif lns[i][:1] == '@':
            spec = lns[i][1:].strip()

            if spec.lower() not in species.keys():
                err = 'LINE %s: %s is an invalid species' % (i + 1, spec)
                errors.append(err)

            else:
                ## spec is now an integer (sp_id)
                spec = species[spec.lower()]

        ## Lines beginning with '%' are gene ID types (REQUIRED)
        elif lns[i][:1] == '%':
            gene = lns[i][1:].strip()

            ## Gene ID representation is fucking ass backwards. If a 
            ## microarray platform is specified, the best possible match above
            ## a given threshold is found and used. All other gene types are 
            ## retrieved from the DB and their ID types are negated. 
            if gene.lower().find('microarray') != -1:
                ## Remove 'microarray ' text
                gene = gene[len('microarray '):]
                original = gene

                ## Determine the closest microarry platform match above a 70%
                ## similarity threshold.
                best = 0.70

                for plat, pid in platforms.items():
                    sim = calc_str_similarity(plat.lower(), original.lower())

                    if sim > best:
                        best = sim
                        gene = plat

                ## Convert to the ID, gene will now be an integer
                gene = platforms.get(gene, 'unknown')

                if type(gene) != int:
                    err = 'LINE %s: %s is an invalid platform' % \
                          (i + 1, original)
                    errors.append(err)

            ## Otherwise the user specified one of the gene types, not a
            ## microarray platform
            ## :IMPORTANT: Expression platforms have positive (+)
            ## gs_gene_id_types while all other types (e.g. symbols) should
            ## have negative (-) integer ID types.
            else:

                if gene.lower() not in gene_types.keys():
                    err = 'LINE %s: %s is an invalid gene type' % (i + 1, gene)
                    errors.append(err)

                else:
                    ## gene is now an integer (gdb_id)
                    gene = gene_types[gene.lower()]
                    gene = -gene

        ## Lines beginning with 'P ' are PubMed IDs (OPTIONAL)
        elif (lns[i][:2].lower() == 'p ') and (len(lns[i].split('\t')) == 1):
            pub = lns[i][1:].strip()

        ## Lines beginning with 'A' are groups, default is private (OPTIONAL)
        elif lns[i][:2].lower() == 'a ' and (len(lns[i].split('\t')) == 1):
            group = lns[i][1:].strip()

            ## If the user gives something other than private/public,
            ## automatically make it private
            if group.lower() != 'private' and group.lower() != 'public':
                group = '-1'
                cur = 5

            ## Public data sets are initially thrown into the provisional
            ## Tier IV. Tier should never be null.
            elif group.lower() == 'public':
                group = '0'
                cur = 4

            else:  # private
                group = '-1'
                cur = 5

        ## If the lines are tab separated, we assume it's the gene data that
        ## will become apart of the geneset_values
        elif len(lns[i].split('\t')) == 2:

            ## First we check to see if all the required data was specified
            if ((not abbr) or (not name) or (not desc) or (not stype) or
                (not spec) or (not gene)):

                err = 'One or more of the required fields are missing.'
                ## Otherwise this string will get appended a bajillion times
                if err not in errors:
                    errors.append(err)

            else:
                lns[i] = lns[i].split()

                ## I don't think this code can ever be reached...
                if len(lns[i]) < 2:
                    err = 'LINE %s: Skipping invalid gene, value formatting' \
                          % (i + 1)
                    warns.append(err)

                else:
                    gsvals.append((lns[i][0], lns[i][1]))


        ## Lines beginning with '#' are comments
        elif lns[i][:1] == '#':
            continue

        ## Skip blank lines
        elif lns[i][:1] == '':
            continue

        ## Who knows what the fuck this line is, just skip it
        else:
            err = 'LINE %s: Skipping unknown identifiers' % (i + 1)
            warns.append(err)

    ## awwww shit, we're finally finished! Check for critical errors and
    ## if there were none, make the final parsed geneset and return
    if errors:
        return ([], warns, errors)

    else:
        gs = util.make_geneset(name, abbr, desc, spec, pub, group, stype,
                               thresh, gene, gsvals)
        genesets.append(gs)

        return (genesets, warns, errors)

#### buFile
##
#### Parses geneset content into the proper format and inserts it into the file
#### table. The proper format is gene\tvalue\n .
##
def create_geneset_file(genes):
    """
    Parses the geneset_values into the proper format for storage in the file
    table and inserts the result.

    :type genes: list
    :arg genes: geneset_value tuples (gene, value)

    :ret int: file_id of the newly inserted file
    """

    conts = ''
    ## Geneset values should be a list of tuples (symbol, pval)
    for tup in genes:
        conts += (tup[0] + '\t' + tup[1] + '\n')

    return db.insert_file(len(conts), conts, '')


def create_geneset_values(gs):
    """
    Maps the given reference IDs to ode_gene_ids and inserts them into the
    geneset_value table.

    :type gs: dict
    :arg gs: geneset dict
    """

    ## Geneset values should be a list of tuples (symbol, pval)
    ## First we attempt to map them to the internal ode_gene_ids
    symbols = filter(lambda x: not not x, gs['geneset_values'])
    symbols = map(lambda x: x[0], symbols)

    ## Negative numbers indicate normal gene types (found in genedb) while
    ## positive numbers indicate expression platforms and more work :(
    if gs['gs_gene_id_type'] < 0:
        sym2ode = db.get_gene_ids_by_species(symbols, gs['sp_id'])

    else:
        sym2probe = db.get_platform_probes(gs['gs_gene_id_type'], symbols)
        prb_ids = []

        for sym in symbols:
            prb_ids.append(sym2probe[sym])

        prb_ids = list(set(prb_ids))
        prb2odes = db.get_probe2gene(prbids)

    # non-critical errors we will inform the user about
    noncrit = []
    # duplicate detection
    dups = dd(str)
    total = 0

    for sym, value in gs['geneset_values']:

        ## Platform handling
        if gs['gs_gene_id_type'] > 0:
            prbid = sym2probe[sym]
            odes = prb2odes[prbid]

            if not prbid or not odes:
                err = ("Error! There doesn't seem to be any gene/locus data for "
                       "%s in the database." % sym)
                noncrit.append(err)
                continue

            for ode in odes:
                ## Check for duplicate ode_gene_ids, otherwise postgres bitches
                if not dups[ode]:
                    dups[ode] = sym

                else:
                    err = ('Error! Seems that %s is a duplicate of %s. %s was not '
                           'added to the geneset.' %
                           (sym, dups[ode], sym))
                    noncrit.append(err)
                    continue

                db.insert_geneset_value(gs['gs_id'], ode, value, sym,
                                        gs['gs_threshold'])

                total += 1

            continue

        ## Not platform stuff
        if not sym2ode[sym]:
            err = ("Error! There doesn't seem to be any gene/locus data for "
                   "%s in the database." % sym)
            noncrit.append(err)
            continue

        ## Check for duplicate ode_gene_ids, otherwise postgres bitches
        if not dups[sym2ode[sym]]:
            dups[sym2ode[sym]] = sym

        else:
            err = ('Error! Seems that %s is a duplicate of %s. %s was not '
                   'added to the geneset.' %
                   (sym, dups[sym2ode[sym]], sym))
            noncrit.append(err)
            continue

        db.insert_geneset_value(gs['gs_id'], sym2ode[sym], value,
                                sym, gs['gs_threshold'])

        total += 1

    return (total, noncrit)


def parse_batch_file(fp, cur_id=None, usr_id=0):
    """
    Parses a batch file to completion.

    :type fp: str
    :arg fp: filepath to a batch file

    :type fp: str
    :arg fp: filepath to a batch file
    """

    noncrits = []  # non-critical errors we will inform the user about
    added = []  # list of gs_ids successfully added to the db

    ## returns (genesets, non-critical errors, critical errors)
    b = parse_batch_syntax(read_file(fp))

    ## A critical error has occurred
    if b[2]:
        return b

    else:
        genesets = b[0]
        noncrits = b[1]

    ## Custom tier
    if cur_id:
        for gs in genesets:
            gs.update({'cur_id': cur_id})

    ## Custom usr_id or just use guest ID
    for gs in genesets:
        gs.update({'usr_id': usr_id})

    attributions = db.get_attributions()

    for abbrev, at_id in attributions.items():
        ## Fucking none type in the db
        if abbrev:
            attributions[abbrev.lower()] = at_id

    for gs in genesets:
        gs.update({'usr_id': usr_id})

    ## Geneset post-processing: PubMed retrieval, gene -> ode_gene_id mapping,
    ## attribution mapping, and file table insertion
    for gs in genesets:
        ## If a PMID was provided, we get the info from NCBI
        if gs['pub_id']:
            pub = getPubmedInfo(gs['pub_id'])
            gs['pub_id'] = pub[0]

            if not pub[0]:
                gs['pub_id'] = None

            else:
                gs['pub_id'] = db.insert_publication(gs['pub_id'])

                ## Non-critical pubmed retrieval errors
                if pub[1]:
                    noncrits.append(pub[1])

        else:
            gs['pub_id'] = None  # empty pub

        if gs['at_id']:
            gs['at_id'] = attributions.get(gs['at_id'], None)

        ## Insert the data into the file table
        gs['file_id'] = create_geneset_file(gs['geneset_values'])
        ## Insert new genesets...
        gs['gs_id'] = db.insert_geneset(gs)
        ## ...then geneset_values
        gsverr = create_geneset_values(gs)

        ## Update gs_count if some geneset_values were found to be invalid
        if gsverr[0] != len(gs['geneset_values']):
            db.update_geneset_count(gs['gs_id'], gsverr[0])

        added.append(gs['gs_id'])

        ## Non-critical errors discovered during geneset_value creation
        if gsverr[1]:
            noncrits.extend(gsverr[1])

    return (added, noncrits)


if __name__ == '__main__':
    from optparse import OptionParser
    from sys import argv

    # cmd line shit
    usage = 'usage: %s [options] <batch_file>' % argv[0]
    parse = OptionParser(usage=usage)

    parse.add_option('-u', action='store', type='string', dest='usr_id',
                     help='Specify a usr_id for newly added genesets')
    parse.add_option('-c', action='store', type='string', dest='cur_id',
                     help='Specify a cur_id for newly added genesets')

    (opts, args) = parse.parse_args(argv)

    if len(args) < 2:
        print '[!] You need to provide a batch geneset file.'
        parse.print_help()
        print ''
        exit()

    if not opts.usr_id:
        opts.usr_id = 0
    if not opts.cur_id:
        opts.cur_id = 5

    ## Where all the magic happens
    stuff = parse_batch_file(args[1], opts.cur_id, opts.usr_id)

    print '[+] The following genesets were added:'
    print ', '.join(map(str, stuff[0]))
    print ''

    if stuff[1]:
        print '[!] There were some non-critical errors with the batch file:'
        for er in stuff[1]:
            print er
        print ''

