#!/usr/bin/env python

## file:    batch.py
## desc:    Rewrite of the PHP batch geneset upload function.
## vers:    0.2.1
## auth:    Baker
##          TR
#
## TODO:    1. The regex taken from the PHP code for effect and correlation
##	    scores doesn't work on all input cases. It breaks for cases such
##	    as "0.75 < Correlation." For almost all cases now though, it works.
#
##	    2. Genesets still need to be associated with a usr_id. This isn't
## 	    done now because there's no point in getting usr_ids offline.
##	    However, one of the cmd line args allows you to specify a usr_id.
#
##	    3. Actually determine gsv_in_threshold insead of just setting it
##	    to be true lol.
#
##	    4. Better messages for duplicate/missing genes and pubmed errors
##	    (i.e. provide the gs_name these failures are associated with).


## multisets because regular sets remove duplicates, requires python 2.7
from collections import Counter as mset
from collections import defaultdict as dd
import datetime
import json
import random
import re
import urllib2 as url2

import db

        ## UTILITY ##
        #############

def readBatchFile(fp):
    """
    Reads the file at the given filepath and returns all the lines that
    comprise the file.

    :arg string: filepath to read
    :ret list: a list of strings--each line in the file
    """

    with open(fp, 'r') as fl:
        return fl.readlines()

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

def parseScoreType(s):
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
            thresh = m.group(1)  # parenthesized group
        else:
            error = 'No threshold specified for P-Value data. Using p < 0.05.'

    elif s.lower().find('q-value') != -1:
        m = re.search(r"([0-9.-]{2,})", s.lower())
        stype = '2'

        if m:
            thresh = m.group(1)  # parenthesized group
        else:
            error = 'No threshold specified for Q-Value data. Using q < 0.05.'

    elif s.lower().find('correlation') != -1:
        ## This disgusting regex is from the PHP function
        ## And it sucks. It breaks on some input, might have to change this
        ## later.
        m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
        stype = '4'

        if m:
            thresh = m.group(1) + ',' + m.group(2)  # parenthesized group
        else:
            thresh = '-0.75,0.75'
            error = ('No thresholds specified for Correlation data.'
                     ' Using -0.75 < value < 0.75.')

    elif s.lower().find('effect') != -1:
        ## Again, PHP regex
        m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
        stype = '5'

        if m:
            thresh = m.group(1) + ',' + m.group(2)  # parenthesized group
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


#### parseBatchFile
##
##
def parse_batch_file(lns):
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
    ## Critical errors discovered during parsing
    cerr = ''
    ## Non-critical errors discovered during parsing
    ncerr = []
    ## Critical errors discovered during parsing
    errors = []
    ## Non-critical errors discovered during parsing
    warns = [] 

    for i in range(len(lns)):
        lns[i] = lns[i].strip()

        ## :, =, + is required for each geneset in the batch file
        #
        ## Lines beginning with ':' are geneset abbreviations (REQUIRED)
        if lns[i][:1] == ':':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                gs = makeGeneset(name, abbr, desc, spec, pub, group, stype,
                                 thresh, gene, gsvals, usr, cur)
                ## Start a new dataset
                abbr = ''
                desc = ''
                name = ''
                gsvals = []
                genesets.append(gs)

            abbr = lns[i][1:].strip()

        ## Lines beginning with '=' are geneset names (REQUIRED)
        elif lns[i][:1] == '=':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                gs = makeGeneset(name, abbr, desc, spec, pub, group, stype,
                                 thresh, gene, gsvals, usr, cur)
                ## Start a new dataset
                abbr = ''
                desc = ''
                name = ''
                gsvals = []
                genesets.append(gs)

            name = lns[i][1:].strip()

        ## Lines beginning with '+' are geneset descriptions (REQUIRED)
        elif lns[i][:1] == '+':
            ## This checks to see if we've already read in some geneset_values
            ## If we have, that means we can save the geneset, clear out any
            ## REQUIRED fields before we do more parsing, and start over
            if gsvals:
                gs = makeGeneset(name, abbr, desc, spec, pub, group, stype,
                                 thresh, gene, gsvals, usr, cur)
                ## Start a new dataset
                abbr = ''
                desc = ''
                name = ''
                gsvals = []
                genesets.append(gs)

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
            specs = db.get_species()

            for sp_name, sp_id in specs.items():
                specs[sp_name.lower()] = sp_id

            if spec.lower() not in specs.keys():
                err = 'LINE %s: %s is an invalid species' % (i + 1, spec)
                errors.append(err)

            else:
                ## spec is now an integer (sp_id)
                spec = specs[spec.lower()]

        ## Lines beginning with '%' are gene ID types (REQUIRED)
        elif lns[i][:1] == '%':
            gene = lns[i][1:].strip()

            ## Gene ID representation is fucking ass backwards. If a 
            ## microarray platform is specified, the best possible match above
            ## a given threshold is found and used. All other gene types are 
            ## retrieved from the DB and their ID types are negated. 
            if gene.lower().find('microarray') != -1:
                plats = db.get_microarray_types()
                ## Remove 'microarray ' text
                gene = gene[len('microarray '):]
                original = gene

                ## Determine the closest microarry platform match above a 70%
                ## similarity threshold.
                best = 0.70

                for plat, pid in plats.items():
                    sim = calc_str_similarity(plat.lower(), original.lower())

                    if sim > best:
                        best = sim
                        gene = plat

                ## Convert to the ID, gene will now be an integer
                gene = plats.get(gene, 'unknown')

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
                types = db.getGeneTypes()

                if gene.lower() not in types.keys():
                    #cerr = ('Critical error! There is no data for the gene type '
                    #        '(%s) you specified.' % gene)
                    #break
                    err = 'LINE %s: %s is an invalid gene type' % (i + 1, gene)
                    errors.append(err)

                else:
                    ## gene is now an integer (gdb_id)
                    gene = types[gene.lower()]
                    ## Negate, see comment tagged important above
                    gene = -gene


        ## Lines beginning with 'P ' are PubMed IDs (OPTIONAL)
        #elif (ln[:2].lower() == 'p ') and (len(ln.split('\t')) == 1):
        elif (lns[i][:2].lower() == 'p ') and (len(lns[i].split('\t')) == 1):
            #pub = eatWhiteSpace(ln[1:])
            pub = eatWhiteSpace(lns[i][1:])

        ## Lines beginning with 'A' are groups, default is private (OPTIONAL)
        #elif ln[:2].lower() == 'a ' and (len(ln.split('\t')) == 1):
        elif lns[i][:2].lower() == 'a ' and (len(lns[i].split('\t')) == 1):
            #group = eatWhiteSpace(ln[1:])
            group = eatWhiteSpace(lns[i][1:])

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
        #elif len(ln.split('\t')) == 2:
        elif len(lns[i].split('\t')) == 2:

            ## First we check to see if all the required data was specified
            if ((not abbr) or (not name) or (not desc) or (not stype) or
                    (not spec) or (not gene)):
                #cerr = ('Critical error! Looks like one of the required '
                #        'fields is missing.')
                #break
                err = 'One or more of the required fields are missing.'
                ## Otherwise this string will get appended a bajillion times
                if err not in errors:
                    errors.append(err)
                #pass


            #ln = ln.split()
            else:
                lns[i] = lns[i].split()

                ## I don't think this code can ever be reached...
                if len(lns[i]) < 2:
                    err = 'LINE %s: Skipping invalid gene, value formatting' \
                          % (i + 1)
                    warns.append(err)

                else:
                    gsvals.append((lns[i][0], lns[i][1]))

            #if len(ln) < 2:
            #    cerr = ("Critical error! Looks like there isn't a value "
            #            "associated with the gene %s. Or maybe you forgot to "
            #            "use tabs." % ln[0])
            #    break

            #gsvals.append((ln[0], ln[1]))

        ## Lines beginning with '#' are comments
        #elif ln[:1] == '#':
        elif lns[i][:1] == '#':
            continue

        ## Skip blank lines
        #elif ln[:1] == '':
        elif lns[i][:1] == '':
            continue

        ## Who knows what the fuck this line is, just skip it
        else:
            #ncerr.append('BAD LINE: ' + ln)
            err = 'LINE %s: Skipping unknown identifiers' % (i + 1)
            warns.append(err)

    ## awwww shit, we're finally finished! Check for critical errors and
    ## if there were none, make the final geneset and return
    #if cerr:
    #    return ([], ncerr, cerr)
    if errors:
        return ([], warns, errors)

    else:
        gs = makeGeneset(name, abbr, desc, spec, pub, group, stype,
                         thresh, gene, gsvals, usr, cur)
        genesets.append(gs)

        #return (genesets, ncerr, [])
        return (genesets, warns, errors)


#### makeRandomFilename
##
#### Generates a random filename for the file_uri column in the file table. The
#### PHP version of this function (getRandomFilename) combines the user's
#### email, the string '_ODE_', the current date, and a random number. Since
#### this script is offline right now, I'm just using 'GW_' + date + '_' + a
#### random six letter alphanumeric string. Looking at the file_uri contents
#### currently in the db though, there seems to be a ton of variation in the
#### naming schemes.
##
def makeRandomFilename():
    lets = 'abcdefghijklmnopqrstuvwxyz1234567890'
    rstr = ''
    now = datetime.datetime.now()

    for i in range(6):
        rstr += random.choice('abcdefghijklmnopqrstuvwxyz1234567890')

    return ('GW_' + str(now.year) + '-' + str(now.month) + '-' +
            str(now.day) + '_' + rstr)


#### buFile
##
#### Parses geneset content into the proper format and inserts it into the file
#### table. The proper format is gene\tvalue\n .
##
def buFile(genes):
    conts = ''
    ## Geneset values should be a list of tuples (symbol, pval)
    for tup in genes:
        conts += (tup[0] + '\t' + tup[1] + '\n')

    return db.insertFile(len(conts), makeRandomFilename(), conts, '')


#### buGenesetValues
##
#### Batch upload geneset values.
##
def buGenesetValues(gs):
    ## Geneset values should be a list of tuples (symbol, pval)
    ## First we attempt to map them to the internal ode_gene_ids
    symbols = filter(lambda x: not not x, gs['values'])
    symbols = map(lambda x: x[0], symbols)

    ## Negative numbers indicate normal genetypes (found in genedb) while
    ## positive numbers indicate expression platforms and more work :(
    if gs['gs_gene_id_type'] < 0:
        sym2ode = db.getOdeGeneIds(gs['sp_id'], symbols)

    else:
        sym2probe = db.getPlatformProbes(gs['gs_gene_id_type'], symbols)
        prbids = []

        for sym in symbols:
            prbids.append(sym2probe[sym])

        prbids = list(set(prbids))
        prb2odes = db.getProbe2Gene(prbids)


    # non-critical errors we will inform the user about
    noncrit = []
    # duplicate detection
    dups = dd(str)
    total = 0

    for tup in gs['values']:

        ## Platform handling
        if gs['gs_gene_id_type'] > 0:
            sym = tup[0]
            value = tup[1]
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
                    dups[ode] = tup[0]

                else:
                    err = ('Error! Seems that %s is a duplicate of %s. %s was not '
                           'added to the geneset.' %
                           (sym, dups[ode], sym))
                    noncrit.append(err)
                    continue

                db.insertGenesetValue(gs['gs_id'], ode, value, sym,
                                      'true')
                                      #gs['gs_threshold'])

                total += 1

            continue

        ## Not platform stuff
        if not sym2ode[tup[0].lower()]:
            err = ("Error! There doesn't seem to be any gene/locus data for "
                   "%s in the database." % tup[0])
            noncrit.append(err)
            continue

        ## Check for duplicate ode_gene_ids, otherwise postgres bitches
        if not dups[sym2ode[tup[0].lower()]]:
            dups[sym2ode[tup[0].lower()]] = tup[0]

        else:
            err = ('Error! Seems that %s is a duplicate of %s. %s was not '
                   'added to the geneset.' %
                   (tup[0], dups[sym2ode[tup[0].lower()]], tup[0]))
            noncrit.append(err)
            continue

        ## Remember to lower that shit, forgot earlier :(
        db.insertGenesetValue(gs['gs_id'], sym2ode[tup[0].lower()], tup[1],
                              tup[0], gs['gs_threshold'])

        total += 1

    return (total, noncrit)


#### buGenesets
##
#### Batch upload genesets. Requires the filepath to the batch upload file.
#### Takes two additional (optional) parameters, a usr_id and cur_id, which
#### are provided as command line arguments. This allows the person running
#### the script to change usr_ids and cur_ids, which are currently set to 0
#### and 5 (private) respectively, for this "offline" version of the script.
##
def buGenesets(fp, usr_id=0, cur_id=5):
    noncrits = []  # non-critical errors we will inform the user about
    added = []  # list of gs_ids successfully added to the db

    ## returns (genesets, non-critical errors, critical errors)
    b = parseBatchFile(readBatchFile(fp), usr_id, cur_id)

    ## A critical error has occurred
    if b[2]:
        print b[2]
        print ''
        exit()

    else:
        genesets = b[0]
        noncrits = b[1]

    for gs in genesets:
        ## If a PMID was provided, we get the info from NCBI
        if gs['pub_id']:
            pub = getPubmedInfo(gs['pub_id'])
            gs['pub_id'] = pub[0]

            ## Non-crit pubmed retrieval errors
            if pub[1]:
                noncrits.append(pub[1])

            ## New row in the publication table
            if gs['pub_id']:
                gs['pub_id'] = db.insertPublication(gs['pub_id'])
            else:
                gs['pub_id'] = None  # empty pub

        else:
            gs['pub_id'] = None  # empty pub

        ## Insert the data into the file table
        gs['file_id'] = buFile(gs['values'])
        ## Insert new genesets and geneset_values
        gs['gs_id'] = db.insertGeneset(gs)
        gsverr = buGenesetValues(gs)

        ## Update gs_count if some geneset_values were found to be invalid
        if gsverr[0] != len(gs['values']):
            db.updateGenesetCount(gs['gs_id'], gsverr[0])

        added.append(gs['gs_id'])

        ## Non-critical errors discovered during geneset_value creation
        if gsverr[1]:
            noncrits.extend(gsverr[1])

    db.commit()

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
    stuff = buGenesets(args[1], opts.usr_id, opts.cur_id)

    print '[+] The following genesets were added:'
    print ', '.join(map(str, stuff[0]))
    print ''

    if stuff[1]:
        print '[!] There were some non-critical errors with the batch file:'
        for er in stuff[1]:
            print er
        print ''

