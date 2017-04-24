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

def get_pubmed_info(pmid):
    """
    Retrieves PubMed article metadata for a given PMID.

    arguments:
        pmid: an int PubMed ID

    returns:
        a dict where each key corresponds to a column in the publication table.
    """
    ## Sumarry info includes everything but the abstract
    sum_url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?'
               'retmode=json&db=pubmed&id=%s') % pmid
    ## Abstract only
    abs_url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
               '?rettype=abstract&retmode=text&db=pubmed&id=%s') % pmid

    ## Sometimes the NCBI servers shit the bed and return errors that kill
    ## the python script, we catch these and just return blank pubmed info
    for attempt in range(5):
        try:
            sum_res = url2.urlopen(sum_url).read()
            abs_res = url2.urlopen(abs_url).read()

        except url2.HTTPError as e:
            #print 'Error! Failed to retrieve a set of UniGene IDs from NCBI:'
            #print e
            continue

        break

    ## for-else construct, if the loop doesn't break (in our case this
    ## indicates success) then this statement is executed
    else:
        return {}

    pinfo = {}
    sum_res = json.loads(sum_res)

    ## Key errors will indicate an crucial metadata component of the article is
    ## missing. If this occurs we won't insert any new pubs.
    try:
        pub = sum_res[u'result']

        ## fucking unicode errors
        #for k, v in pub.items():
        #    k = k.encode('ascii', 'ignore')

        #    if isinstance(v, str):
        #        v = v.encode('ascii', 'ignore')

        #    pub[k] = v

        pub = pub[pmid]

        pinfo['pub_title'] = pub['title']
        pinfo['pub_abstract'] = abs_res
        pinfo['pub_journal'] = pub['fulljournalname']
        pinfo['pub_volume'] = pub.get('volume', '')
        pinfo['pub_pages'] = pub.get('pages', '')
        pinfo['pub_pubmed'] = pmid
        pinfo['pub_authors'] = []

        ## Author struct {name, authtype, clustid}
        for auth in pub['authors']:
            #name = auth[u'name'].encode('ascii', 'ignore')
            #pinfo[u'pub_authors'].append(name)
            pinfo[u'pub_authors'].append(auth['name'])

        pinfo['pub_authors'] = ','.join(pinfo['pub_authors'])

    except KeyError:
        return {}

    return pinfo


def make_digrams(s):
    """
    Recursively creates an exhaustive list of digrams from the given string.

    arguments:
        s: string to generate digrams with

    returns:
        a list of digram strings.
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

    arguments:
        s1: string #1
        s2: string #2

    returns:
        a float indicating the percent similarity between two strings
    """

    sd1 = makeDigrams(s1)
    sd2 = makeDigrams(s2)
    intersect = list((mset(sd1) & mset(sd2)).elements())

    return (2 * len(intersect)) / float(len(sd1) + len(sd2))

        ## READER ##
        ############

class BatchReader(object):
    """
    Class used to read and parse batch geneset files.


    attributes:
        filepath: string indicating the location of a batch file
        genesets: list of dicts representing the parsed genesets
        errors: list of strings indicating critical errors found during parsing
        warns: list of strings indicating non-crit errors found during parsing
    """

    def __init__(self, filepath):

        self.filepath = filepath
        self.genesets = []
        self.errors = []
        self.warns = []

    def __read_file(self, fp=None):
        """
        Reads a file and splits it into lines.
        """

        if not fp:
            fp = self.filepath

        with open(fp, 'r') as fl:
            return fl.read().split('\n')

    def __parse_score_type(self, s):
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

        arguments:
            s: string containing score type and possibly threshold value(s)

        returns:
            a tuple containing the threshold type and threshold value. 
            E.g. (gs_threshold_type, gs_threshold)
        """

        ## The score type, a numeric value but currently stored as a string
        stype = ''
        ## Default theshold values
        thresh = '0.05'
        thresh2 = '0.05'

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
                self.warns.append('Invalid threshold. Using p < 0.05.')

        elif s.lower().find('q-value') != -1:
            m = re.search(r"([0-9.-]{2,})", s.lower())
            stype = '2'

            if m:
                thresh = m.group(1)
            else:
                self.warns.append('Invalid threshold. Using q < 0.05.')

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
                self.warns.append(('Invalid threshold.' 
                                  'Using -0.75 < Correlation < 0.75'))

        elif s.lower().find('effect') != -1:
            ## Again, PHP regex
            m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
            stype = '5'

            if m:
                thresh = m.group(1) + ',' + m.group(2)
            else:
                thresh = '0,1'
                self.warns.append('Invalid threshold. Using 0 < Effect < 1.')

        else:
            self.errors.append('An unknown score type (%s) was provided.' % s)

        return (stype, thresh)


    def __parse_batch_syntax(self, lns):
        """
        Parses a batch file according to the format listed on
        http://geneweaver.org/index.php?action=manage&cmd=batchgeneset
        The results (parsed genesets) and any errors or warnings are stored in
        their respective class attributes. 

        arguments:
            lns: list of strings, one for each line in the batch file
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

        gene_types = db.get_gene_types()
        species = db.get_species()
        platforms = db.get_platform_names()

        def reset_add_geneset():
            gs = util.make_geneset(
                name, 
                abbr, 
                desc, 
                spec,
                pub,
                group,
                stype,
                thresh,
                gene,
                gsvals,
                at_id,
                usr_id,
                cur_id
            )
            abbr = ''
            desc = ''
            name = ''
            gsvals = []
            self.genesets.append(gs)

        for gdb_name, gdb_id in gene_types.items():
            gene_types[gdb_name.lower()] = gdb_id

        for sp_name, sp_id in species.items():
            species[sp_name.lower()] = sp_id

        for pf_name, pf_id in platforms.items():
            platforms[pf_name.lower()] = pf_id

        for i in range(len(lns)):
            lns[i] = lns[i].strip()

            ## These are special (dev only) additions to the batch file that 
            ## allow tiers, user IDs, and attributions to be specified.
            #
            ## Lines beginning with 'T' are Tier IDs
            if lns[i][:2].lower() == 't ':
                if gsvals:
                    reset_add_geneset()

                cur_id = int(lns[i][1:].strip())

            ## Lines beginning with 'U' are user IDs
            elif lns[i][:2].lower() == 'u ':
                if gsvals:
                    reset_add_geneset()

                usr_id = int(lns[i][1:].strip())

            ## Lines beginning with 'D' are attribution abbrevations
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

                abbr = lns[i][1:].strip()

            ## Lines beginning with '=' are geneset names (REQUIRED)
            elif lns[i][:1] == '=':
                ## This checks to see if we've already read in some geneset_values
                ## If we have, that means we can save the geneset, clear out any
                ## REQUIRED fields before we do more parsing, and start over
                if gsvals:
                    reset_add_geneset()

                name = lns[i][1:].strip()

            ## Lines beginning with '+' are geneset descriptions (REQUIRED)
            elif lns[i][:1] == '+':
                ## This checks to see if we've already read in some geneset_values
                ## If we have, that means we can save the geneset, clear out any
                ## REQUIRED fields before we do more parsing, and start over
                if gsvals:
                    reset_add_geneset()

                desc += lns[i][1:].strip()
                desc += ' '

            ## !, @, %, are required but can be omitted from later sections if
            ## they don't differ from the first.
            #
            ## Lines beginning with '!' are score types (REQUIRED)
            elif lns[i][:1] == '!':
                score = lns[i][1:].strip()
                score = self.__parse_score_type(score)
                stype = score[0]
                thresh = score[1]

                ## An error ocurred 
                if not stype:
                    self.errors[-1] = 'LINE %s: %s' % (i + 1, self.errors[-1])

            ## Lines beginning with '@' are species types (REQUIRED)
            elif lns[i][:1] == '@':
                spec = lns[i][1:].strip()

                if spec.lower() not in species.keys():
                    self.errors.append(
                        'LINE %s: %s is an invalid species' % (i + 1, spec)
                    )

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
                        self.errors.append(
                            'LINE %s: %s is an invalid platform' % (i + 1, original)
                        )

                ## Otherwise the user specified one of the gene types, not a
                ## microarray platform
                ## :IMPORTANT: Expression platforms have positive (+)
                ## gs_gene_id_types while all other types (e.g. symbols) should
                ## have negative (-) integer ID types.
                else:
                    if gene.lower() not in gene_types.keys():
                        self.errors.append(
                            'LINE %s: %s is an invalid gene type' % (i + 1, gene)
                        )

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
            ## will become part of the geneset_values
            elif len(lns[i].split('\t')) == 2:

                ## First we check to see if all the required data was specified
                if ((not abbr) or (not name) or (not desc) or (not stype) or
                    (not spec) or (not gene)):

                    err = 'One or more of the required fields are missing.'
                    ## Otherwise this string will get appended a bajillion times
                    if err not in errors:
                        self.errors.append(err)

                else:
                    lns[i] = lns[i].split()

                    gsvals.append((lns[i][0], lns[i][1]))


            ## Lines beginning with '#' are comments
            elif lns[i][:1] == '#':
                continue

            ## Skip blank lines
            elif lns[i][:1] == '':
                continue

            ## Who knows what the fuck this line is, just skip it
            else:
                self.warns.append(
                    'LINE %s: Skipping unknown identifiers' % (i + 1)
                )

        ## awwww shit, we're finally finished! Make the final parsed geneset.
        gs = util.make_geneset(
            name, abbr, desc, spec, pub, group, stype, thresh, gene, gsvals
        )
        self.genesets.append(gs)

    def __create_geneset_file(self, genes):
        """
        Parses the geneset_values into the proper format for storage in the file
        table and inserts the result.

        arguments:
            genes: a list of tuples representing geneset values
            e.g. [('Mobp', 0.001), ('Daxx', 0.2)]

        returns:
            the file_id (int) of the newly inserted file
        """

        conts = ''
        ## Geneset values should be a list of tuples (symbol, pval)
        for tup in genes:
            conts += (tup[0] + '\t' + tup[1] + '\n')

        return db.insert_file(len(conts), conts, '')


    def __create_geneset_values(self, gs):
        """
        Maps the given reference IDs to ode_gene_ids and inserts them into the
        geneset_value table.

        arguments:
            gs: a dict representing a geneset

        returns:
            an int indicating the total number of geneset_values inserted into
            the DB.
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

        # duplicate detection
        dups = dd(str)
        total = 0

        for sym, value in gs['geneset_values']:

            ## Platform handling
            if gs['gs_gene_id_type'] > 0:
                prbid = sym2probe[sym]
                odes = prb2odes[prbid]

                if not prbid or not odes:
                    self.warns.append('No gene/locus data exists for %s' % sym)
                    continue

                for ode in odes:
                    ## Check for duplicate ode_gene_ids, otherwise postgres bitches
                    if not dups[ode]:
                        dups[ode] = sym

                    else:
                        self.warns.append((
                            '%s is a duplicate of %s and '
                            'was not added to the geneset' % (sym, dups[ode])
                        ))
                        continue

                    db.insert_geneset_value(
                        gs['gs_id'], ode, value, sym, gs['gs_threshold']
                    )

                    total += 1

                continue

            ## Not platform stuff
            if not sym2ode[sym]:
                self.warns.append('No gene/locus exists data for %s' % sym)
                continue

            ## Check for duplicate ode_gene_ids, otherwise postgres bitches
            if not dups[sym2ode[sym]]:
                dups[sym2ode[sym]] = sym

            else:
                self.warns.append((
                    '%s is a duplicate of %s and was not '
                    'added to the geneset' % (sym, dups[ode])
                ))
                continue

            db.insert_geneset_value(
                gs['gs_id'], sym2ode[sym], value, sym, gs['gs_threshold']
            )

            total += 1

        return total


    def parse_batch_file(self):
        """
        Parses a batch file to completion.

        arguments:
            fp: a string, the filepath to a batch file

        returns:
            A list of ints. Each int is the gs_id of an inserted geneset.
        """

        self.errors = []
        self.warns = []

        if not self.filepath:
            self.errors('No batch file was provided.')
            return []

        ## list of gs_ids successfully added to the db
        added = []  

        self.__parse_batch_syntax(self.__read_file())

        if self.errors:
            return []

        attributions = db.get_attributions()

        for abbrev, at_id in attributions.items():
            ## Fucking NULL row in the db, this needs to be removed
            if abbrev:
                attributions[abbrev.lower()] = at_id

        ## Geneset post-processing: PubMed retrieval, gene -> ode_gene_id mapping,
        ## attribution mapping, and file table insertion
        for gs in self.genesets:
            ## If a PMID was provided, we get the info from NCBI
            if gs['pub_id']:
                pub = get_pubmed_info(gs['pub_id'])

                if not pub:
                    gs['pub_id'] = None

                else:
                    gs['pub_id'] = db.insert_publication(pub)

            else:
                gs['pub_id'] = None

            if gs['at_id']:
                gs['at_id'] = attributions.get(gs['at_id'], None)

            gs['file_id'] = self.__create_geneset_file(gs['geneset_values'])
            gs['gs_id'] = db.insert_geneset(gs)
            gsv_count = self.__create_geneset_values(gs)

            ## Update gs_count if some geneset_values were found to be invalid
            if gsv_count != len(gs['geneset_values']):
                db.update_geneset_count(gs['gs_id'], gsv_count)

            added.append(gs['gs_id'])

        return added

class BatchWriter(object):
    """
    Serializes geneset data into the batch geneset format. 

    attributes:
        genesets: a list of geneset dicts to serialize
    """

    def __init__(self, filepath, genesets, is_dev=False):
        self.filepath = filepath
        self.genesets = genesets
        self.is_dev = is_dev
        self.errors = []
        self.species = db.get_species()
        self.gene_types = db.get_gene_types()
        self.platforms = db.get_platform_names()
        self.attributions = db.get_attributions()

        ## Reverse each of the mappings
        for sp_name, sp_id in self.species.items():
            self.species[sp_id] = sp_name
            del self.species[sp_name]

        for gdb_name, gdb_id in self.gene_types.items():
            self.gene_types[gdb_id] = gdb_name
            del self.gene_types[gdb_name]

        for pf_name, pf_id in self.platforms.items():
            self.platforms[pf_id] = pf_name
            del self.platforms[pf_name]

        for at_abbrev, at_id in self.attributions.items():
            self.attributions[at_id] = at_abbrev
            del self.attributions[at_abbrev]

    def __format_threshold(self, threshold_type, threshold=''):
        """
            """

        serial = ''

        if not threshold and (threshold_type == 1 or threshold_type == 2):
            threshold = '0.05'

        elif threshold_type == 4 and len(threshold.split(',')) < 2:
            threshold = ['-0.75', '0.75']

        elif threshold_type == 5 and len(threshold.split(',')) < 2:
            threshold = ['0', '1']

        elif threshold_type == 5 or threshold_type == 4:
            threshold = threshold.split(',')

        if threshold_type == 1:
            serial = '! P-Value < ' + threshold

        elif threshold_type == 2:
            serial = '! Q-Value < ' + threshold

        elif threshold_type == 3:
            serial = '! Binary'

        elif threshold_type == 4:
            serial = '! ' + threshold[0] + ' < Correlation < ' + threshold[1]

        elif threshold_type == 5:
            serial = '! ' + threshold[0] + ' < Effect < ' + threshold[1]

        else:
            self.errors.append('Invalid threshold type')

        return serial
    
    def __format_species(self, sp_id):
        """
        """

        if sp_id not in self.species:
            self.errors.append('Invalid species ID')
            return ''

        return '@ ' + self.species[sp_id]

    def __format_gene_type(self, gene_type):
        """
        """

        serial = ''

        ## (-) == normal gene types, (+) == expression platforms
        if gene_type < 0:
            gene_type = abs(gene_type)

            if gene_type not in self.gene_types:
                self.errors.append('Invalid gene type')
                return ''

            serial = '% ' + self.gene_types[gene_type]

        else:
            if gene_type not in self.platforms:
                self.errors.append('Invalid expression platform')
                return ''

            serial = '% microarray ' + self.platforms[gene_type]

        return serial

    def __format_access(self, groups, tier):
        """
        """

        groups = groups.split(',')

        if '-1' in groups and tier == 5:
            return 'A Private'

        else:
            return 'A Public'

    def __format_publication(self, pub_id, pmid=None):
        """
        """

        if pmid:
            return 'P ' + str(pmid)

        elif pub_id:
            pmid = db.get_publication_pmid(pub_id)

            if pmid:
                return 'P ' + pmid

            else:
                return ''

        else:
            return ''

    def __format_label(self, label):
        """
        """

        return ': ' + label

    def __format_name(self, name):
        """
        """

        return '= ' + name

    def __format_description(self, desc):
        """
        """
        desc = desc.split()
        desc = util.chunk_list(desc, 8)
        serial = []

        for line in desc:
            serial.append('+ ' + ' '.join(line))

        return '\n'.join(serial)

    def __format_geneset_values(self, values):
        """
        """

        serial = []

        for symbol, value in values:
            serial.append('%s\t%s' % (symbol, value))

        return '\n'.join(serial)

    def __format_tier(self, tier):
        """
        """

        return 'T ' + str(tier)

    def __format_user(self, usr_id):
        """
        """

        return 'U ' + str(usr_id)

    def __format_attribution(self, at_id):
        """
        """

        if at_id not in self.attributions:
            self.errors.append('Invalid attribution ID')
            return ''

        return 'D ' + self.attributions[at_id]

    def serialize(self):
        """
        Formats the list of genesets into a single batch file and outputs the
        result.
        """

        serial = []
        serial.append('## Auto generated BGF')
        serial.append('#')
        serial.append('')

        threshold_type = None
        threshold = None
        species = None
        gene_type = None
        tier = None
        groups = None
        access = None
        pmid = None
        usr_id = None
        pub_id = None
        at_id = None

        for gs in self.genesets:

            ## General parameters are allowed to change in between geneset
            ## definitions
            if threshold_type != gs['gs_threshold_type'] or\
               threshold != gs['gs_threshold']:

                threshold_type = gs['gs_threshold_type']
                threshold = gs['gs_threshold']

                serial.append(
                    self.__format_threshold(threshold_type, threshold))

            if species != gs['sp_id']:
                species = gs['sp_id']

                serial.append(self.__format_species(species))

            if gene_type != gs['gs_gene_id_type']:
                gene_type = gs['gs_gene_id_type']

                serial.append(self.__format_gene_type(gene_type))

            if gs.get('pmid', None) and pmid != gs['pmid']:
                pmid = gs['pmid']

                serial.append(self.__format_publication(None, pmid))

            elif gs['pub_id'] and pub_id != gs['pub_id']:
                pub_id = gs['pub_id']

                serial.append(self.__format_publication(pub_id))

            if not access:
                access = True

                serial.append(
                    self.__format_access(gs['gs_groups'], gs['cur_id']))

            ## Enable developer parameters
            if self.is_dev:
                if tier != gs['cur_id']:
                    tier = gs['cur_id']

                    serial.append(self.__format_tier(tier))

                if at_id != gs['at_id']:
                    at_id = gs['at_id']

                    serial.append(self.__format_attribution(at_id))

                if usr_id != gs['usr_id']:
                    usr_id = gs['usr_id']

                    serial.append(self.__format_user(usr_id))

            serial.append('')
            serial.append(self.__format_label(gs['gs_abbreviation']))
            serial.append(self.__format_name(gs['gs_name']))
            serial.append(self.__format_description(gs['gs_description']))
            serial.append('')
            serial.append(
                self.__format_geneset_values(gs['geneset_values']))
            serial.append('')

        if self.errors:
            return False

        with open(self.filepath, 'w') as fl:
            print >> fl, '\n'.join(serial)

        return True

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

