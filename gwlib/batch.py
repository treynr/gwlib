#!/usr/bin/env python
# -*- coding: utf-8 -*-

## file: batch.py
## desc: Batch file reader and writer. This is a variant of the batch parser
##       found in the GW2 sauce. It works the same way but decouples most of
##       the DB code and adds support for specifying tiers, attributions,
##       and user IDs in the batch file.
## auth: Baker
##       TR
#

from __future__ import print_function
from collections import Counter as mset
from collections import defaultdict as dd
from copy import deepcopy
import json
import re
import urllib2 as url2

from ncbi import get_pubmed_articles
import db
import util

def get_pubmed_info(pmid):
    """
    Retrieves PubMed article metadata for a given PMID.

    arguments
        pmid: an int PubMed ID

    returns
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

        except url2.HTTPError:
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
            pinfo[u'pub_authors'].append(auth['name'])

        pinfo['pub_authors'] = ','.join(pinfo['pub_authors'])

    except KeyError:
        return {}

    return pinfo


def make_digrams(s):
    """
    Recursively creates an exhaustive list of digrams from the given string.

    arguments
        s: string to generate digrams with

    returns
        a list of digram strings.
    """

    if len(s) <= 2:
        return [s]

    b = make_digrams(s[1:])
    b.insert(0, s[:2])

    return b

def calculate_str_similarity(s1, s2):
    """
    Calculates the percent similarity between two strings. Meant to be a
    replacement for PHP's similar_text function, which old GeneWeaver uses
    to determine the right microarray platform to use.
    This algorithm uses digram intersections determine percent similarity.
    It is calculated as:

    sim(s1, s2) = (2 * intersection(digrams(s1), digrams(s2)) /
                   |digrams(s1) + digrams(s2)|

    arguments
        s1: string #1
        s2: string #2

    returns
        a float indicating the percent similarity between two strings
    """

    sd1 = make_digrams(s1)
    sd2 = make_digrams(s2)
    intersect = list((mset(sd1) & mset(sd2)).elements())

    return (2 * len(intersect)) / float(len(sd1) + len(sd2))

class BatchReader(object):
    """
    Class used to read and parse batch geneset files.

    public
        filepath:   string indicating the location of a batch file
        genesets:   list of dicts representing the parsed genesets, each dict
                    has fields corresponding to columns in the geneset table
        errors:     list of strings indicating critical errors found during
                    parsing
        warns:      list of strings indicating non-crit errors found during
                    parsing

    private
        _parse_set:         the gene set currently being parsed
        _pub_map:           PMID -> pub_id mapping
        _symbol_cache:      a series of nested dicts used to map species
                            specific gene symbols to GW identifiers. The
                            structure is:
                                sp_id -> gdb_id -> ode_ref_id -> ode_gene_id
        _annotation_cache:  mapping of ontology term IDs -> ont_ids
    """

    def __init__(self, filepath):

        self.filepath = filepath
        self.genesets = []
        self.errors = []
        self.warns = []
        self._parse_set = {}
        self._pub_map = None
        self._symbol_cache = dd(lambda: dd(lambda: dd(int)))
        self._annotation_cache = dd(int)

    def __read_file(self, fp=None):
        """
        Reads a file and splits it into lines.
        """

        if not fp:
            fp = self.filepath

        with open(fp, 'r') as fl:
            return fl.read().split('\n')

    def __reset_parsed_set(self):
        """
        Clears and resets the fields of the gene set currently being parsed. If
        the parsed set contains gene values we can assume it is complete (this
        assumption is checked later) and store it in the list of parsed sets.
        """

        if 'values' in self._parse_set and self._parse_set['values']:
            self.genesets.append(deepcopy(self._parse_set))

        if 'pmid' not in self._parse_set:
            self._parse_set['pmid'] = ''

        self._parse_set['gs_name'] = ''
        self._parse_set['gs_description'] = ''
        self._parse_set['gs_abbreviation'] = ''
        self._parse_set['values'] = []
        self._parse_set['annotations'] = []
        self._parse_set['gs_uri'] = None

    def __check_parsed_set(self):
        """
        Checks to see if all the required fields are filled out for the gene
        set currently being parsed.

        returns
            true if all required fields are filled out otherwise false
        """

        if not self._parse_set['gs_name'] or\
           not self._parse_set['gs_description'] or\
           not self._parse_set['gs_abbreviation'] or\
           'gs_gene_id_type' not in self._parse_set or\
           'gs_threshold_type' not in self._parse_set or\
           'sp_id' not in self._parse_set:
                return False

        return True

    def __parse_score_type(self, s):
        """
        Attempts to parse out the score type and any threshold value
        from the given string.
        Acceptable score types and threshold values include:
            Binary
            P-Value < 0.05
            Q-Value < 0.05
            0.40 < Correlation < 0.50
            6.0 < Effect < 22.50

        The numbers listed above are only examples and can vary depending on
        geneset. If those numbers can't be parsed for some reason, default values
        (e.g. 0.05) are used. The score types are converted into a numeric
        representation used by the GW DB:

            P-Value = 1
            Q-Value = 2
            Binary = 3
            Correlation = 4
            Effect = 5

        arguments
            s: string containing score type and possibly threshold value(s)

        returns
            a tuple containing the threshold type and threshold value.
                i.e. (gs_threshold_type, gs_threshold)
        """

        ## The score type, a numeric value but currently stored as a string
        stype = ''
        ## Default theshold values
        thresh = '0.05'

        ## Binary threshold is left at the default of 1
        if s.lower() == 'binary':
            stype = '3'
            thresh = '1'

        elif s.lower().find('p-value') != -1:
            ## All the regexs used in this function are taken from the
            ## original GW1 code
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
            ## This regex doesn't work on some input. It doesn't properly parse
            ## integers (only floats) and you must have two threshold values
            ## (you can't do something like Correlation < 5.0).
            ## Too lazy to change it though since this score type isn't widely
            ## used.
            m = re.search(r"([0-9.-]{2,})[^0-9.-]*([0-9.-]{2,})", s.lower())
            stype = '4'

            if m:
                thresh = m.group(1) + ',' + m.group(2)
            else:
                thresh = '-0.75,0.75'

                self.warns.append(
                    'Invalid threshold. Using -0.75 < Correlation < 0.75'
                )

        elif s.lower().find('effect') != -1:
            ## Same comments as the correlation regex
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

    def __parse_gene_type(self, gtype, platforms, gene_types):
        """
        Parses and determines the gene type the user has provided. If the gene
        type is a microarray, this will attempt to find a matching platform.
        Otherwise it will use regular gene types from the genedb table.

        arguments
            gtype:      the gene type string being parsed
            platforms:  dict mapping platform names -> gene type ids (gdb_id)
            gene_types: dict mapping regular gene type names -> gene type ids

        returns
            an gdb_id (integer) or None if no gene type could be parsed/found
        """

        ## If a microarray platform is specified, we use string
        ## similarity to find the best possible platform in our DB. The
        ## best match above a threshold is used. This has to be done
        ## since naming conventions for the same platform can vary.
        ## All other gene types are easier to handle; they are
        ## retrieved from the DB and their ID types are negated.
        if gtype.lower().find('microarray') != -1:
            ## Remove 'microarray' text
            gtype = gtype[len('microarray'):].strip()
            original = gtype

            ## Determine the closest microarry platform match above a 70%
            ## similarity threshold.
            best = 0.65

            for plat, pid in platforms.items():
                sim = calculate_str_similarity(plat.lower(), original.lower())

                if sim > best:
                    best = sim
                    gtype = plat

            ## Convert to the parsed gene ID type to an actual ID.
            ## gene will now be an integer
            gtype = platforms.get(gtype, 'unknown')

            if type(gtype) != int:
                self.errors.append('%s is an invalid platform' % original)

            else:
                return gtype

        ## Otherwise the user specified one of the gene types, not a
        ## microarray platform
        ## :IMPORTANT: Gene ID representation is fucking ass backwards.
        ## Expression platforms have positive (+) gs_gene_id_types
        ## while all other types (e.g. symbols) should have negative
        ## (-) integer ID types despite their gdb_ids being positive.
        else:
            gtype = gtype.lower()

            if gtype not in gene_types.keys():
                self.errors.append('%s is an invalid gene type' % gtype)

            else:
                ## Convert to a negative integer (gdb_id)
                return -gene_types[gtype]

        return None

    def __parse_batch_syntax(self, lns):
        """
        Parses a batch file according to the format listed on
        http://geneweaver.org/index.php?action=manage&cmd=batchgeneset
        The results (gene set objects) and any errors or warnings are stored in
        their respective class attributes.

        arguments
            lns: list of strings, one for each line in the batch file
        """

        self.__reset_parsed_set()

        gene_types = db.get_gene_types()
        species = db.get_species()
        platforms = db.get_platform_names()

        ## Provide lower cased keys for gene types, species, and expression
        ## platformrs. Otherwise batch files must use case sensitive fields
        ## which would be annoying.
        for gdb_name, gdb_id in gene_types.items():
            gene_types[gdb_name.lower()] = gdb_id

        for sp_name, sp_id in species.items():
            species[sp_name.lower()] = sp_id

        for pf_name, pf_id in platforms.items():
            platforms[pf_name.lower()] = pf_id

        for i in range(len(lns)):
            lns[i] = lns[i].strip()

            ## These are special (dev only) additions to the batch file that
            ## allow tiers, user IDs, and attributions to be specified. These
            ## are only used in the public resource uploader scripts.
            #
            ## Lines beginning with 'T' are Tier IDs
            if lns[i][:2] == 'T ':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['cur_id'] = int(lns[i][1:].strip())

            ## Lines beginning with 'U' are user IDs
            elif lns[i][:2] == 'U ':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['usr_id'] = int(lns[i][1:].strip())

            ## Lines beginning with 'D' are attribution abbrevations
            elif lns[i][:2] == 'D ':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['at_id'] = lns[i][1:].strip()

            ## :, =, + is required for each geneset in the batch file
            #
            ## Lines beginning with ':' are geneset abbreviations (REQUIRED)
            elif lns[i][:1] == ':':
                ## This checks to see if we've already read, parsed, and stored
                ## some gene values. If we have, that means we can save the
                ## currently parsed geneset, clear out any REQUIRED fields before
                ## we do more parsing, and begin parsing this new set.
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['gs_abbreviation'] = lns[i][1:].strip()

            ## Lines beginning with '=' are geneset names (REQUIRED)
            elif lns[i][:1] == '=':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['gs_name'] = lns[i][1:].strip()

            ## Lines beginning with '+' are geneset descriptions (REQUIRED)
            elif lns[i][:1] == '+':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['gs_description'] += lns[i][1:].strip()
                self._parse_set['gs_description'] += ' '

            ## !, @, %, are required but can be omitted from later sections if
            ## they don't differ from the first. Meaning, these fields can be
            ## specified once and will apply to all gene sets in the file unless
            ## this field is encountered again.
            #
            ## Lines beginning with '!' are score types (REQUIRED)
            elif lns[i][:1] == '!':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                ttype, threshold = self.__parse_score_type(lns[i][1:].strip())

                ## An error ocurred
                if not ttype:
                    ## Appends the line number to the last error which should
                    ## be the error indicating an unknown score type was used
                    self.errors[-1] = 'LINE %s: %s' % (i + 1, self.errors[-1])

                else:
                    self._parse_set['gs_threshold_type'] = ttype
                    self._parse_set['gs_threshold'] = threshold

            ## Lines beginning with '@' are species types (REQUIRED)
            elif lns[i][:1] == '@':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                spec = lns[i][1:].strip()

                if spec.lower() not in species.keys():
                    self.errors.append(
                        'LINE %s: %s is an invalid species' % (i + 1, spec)
                    )

                else:
                    ## Convert to sp_id
                    self._parse_set['sp_id'] = species[spec.lower()]

            ## Lines beginning with '$' specify a genome build (REQUIRED)
            elif lns[i][:2] == '$ ':
                if self._parse_set.get('genome_build', None):
                    self.__reset_parsed_set()

                self._parse_set['genome_build'] = lns[i][1:].strip()

            ## Lines beginning with '%' are gene ID types (REQUIRED)
            elif lns[i][:1] == '%':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                gene = lns[i][1:].strip()
                gene = self.__parse_gene_type(gene, platforms, gene_types)

                ## An error ocurred
                if not gene:
                    ## Appends the line number to the last error which should
                    ## be the error indicating an invalid gene type
                    self.errors[-1] = 'LINE %s: %s' % (i + 1, self.errors[-1])

                else:
                    self._parse_set['gs_gene_id_type'] = gene

            ## Lines beginning with 'P ' are PubMed IDs (OPTIONAL)
            elif (lns[i][:2] == 'P ') and (len(lns[i].split('\t')) == 1):
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['pmid'] = lns[i][1:].strip()

            ## Lines beginning with 'A' are groups, default is private (OPTIONAL)
            elif lns[i][:2] == 'A ' and (len(lns[i].split('\t')) == 1):
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                group = lns[i][1:].strip()

                ## If the user gives something other than private/public,
                ## automatically make it private
                if group.lower() != 'private' and group.lower() != 'public':
                    self._parse_set['gs_groups'] = '-1'
                    self._parse_set['cur_id'] = 5

                ## Public data sets are initially thrown into the provisional
                ## Tier IV. Tier should never be null.
                elif group.lower() == 'public':
                    self._parse_set['gs_groups'] = '0'
                    self._parse_set['cur_id'] = 4

                ## Private
                else:
                    self._parse_set['gs_groups'] = '-1'
                    self._parse_set['cur_id'] = 5

            ## Lines beginning with '~' are ontology annotations (OPTIONAL)
            elif lns[i][:2] == '~ ':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['annotations'].append(lns[i][1:].strip())

            ## Lines beginning with '>' point to a URI (OPTIONAL)
            elif lns[i][:2] == '> ':
                if self._parse_set['values']:
                    self.__reset_parsed_set()

                self._parse_set['gs_uri'] = lns[i][1:].strip()

            ## If the lines are tab separated, we assume it's the gene data that
            ## will become part of the geneset_values
            elif len(lns[i].split('\t')) == 2:

                ## Check to see if all the required data was specified, if not
                ## this set can't get uploaded. Let the user figure out what the
                ## hell they're missing cause telling them is too much work on
                ## our part.
                if not self.__check_parsed_set():

                    err = 'One or more of the required fields are missing.'

                    ## Otherwise this string will get appended a bajillion times
                    if err not in self.errors:
                        self.errors.append(err)

                else:
                    lns[i] = lns[i].split('\t')

                    self._parse_set['values'].append((lns[i][0], lns[i][1]))

            ## Lines beginning with '#' are comments
            elif lns[i][:1] == '#':
                continue

            ## Skip blank lines
            elif lns[i][:1] == '':
                continue

            ## Who knows what the fuck this line is, just skip it
            else:
                self.warns.append(
                    'LINE %s: Skipping line with unknown identifiers (%s)' %
                    ((i + 1), lns[i])
                )

        ## awwww shit, we're finally finished! Make the final parsed geneset.
        self.genesets.append(self._parse_set)

    def __insert_geneset_file(self, genes):
        """
        Modifies the geneset_values into the proper format for storage in the file
        table and inserts the result.

        arguments
            genes: a list of tuples representing gene set values
                   e.g. [('Mobp', 0.001), ('Daxx', 0.2)]

        returns
            the file_id (int) of the newly inserted file
        """

        conts = ''

        ## Gene set values should be a list of tuples (symbol, value)
        for tup in genes:
            conts += '%s\t%s\n' % (tup[0], tup[1])

        return db.insert_file(len(conts), conts, '')

    def __map_ontology_annotations(self, gs):
        """
        If a gene set has ontology annotations, we map the ontology term IDs to
        the internal IDs used by GW (ont_ids) and save them in the gene set
        object.
        """

        gs['ont_ids'] = []

        for anno in gs['annotations']:

            ## Check the cache of retrieved annotations
            if self._annotation_cache[anno]:
                gs['ont_ids'].append(self._annotation_cache[anno])

            else:
                ont_id = db.get_annotation_by_ref(anno)

                if ont_id:
                    gs['ont_ids'].append(ont_id)

                    self._annotation_cache[anno] = ont_id

                else:
                    self.warns.append(
                        'The ontology term %s is missing from GW' % anno
                    )

    def __map_gene_identifiers(self, gs):
        """
        Maps the user provided gene symbols (ode_ref_ids) to ode_gene_ids.
        The mapped genes are added to the gene set object in the
        'geneset_values' key. This added key is a list of triplets containing
        the user uploaded symbol, the ode_gene_id, and the value.
            e.g. [('mobp', 1318, 0.03), ...]

        arguments
            gs: a dict representing a geneset. Contains fields with the same
                columns as the geneset table

        returns
            an int indicating the total number of geneset_values inserted into
            the DB.
        """

        ## Isolate gene symbols (ode_ref_ids)
        gene_refs = map(lambda x: x[0], gs['values'])
        gene_type = gs['gs_gene_id_type']
        sp_id = gs['sp_id']
        gs['geneset_values'] = []

        ## Check to see if we have cached copies of these references. If we do,
        ## we don't have to make any DB calls or build the mapping

        ## Negative numbers indicate normal gene types (found in genedb) while
        ## positive numbers indicate expression platforms and more work :(
        if gs['gs_gene_id_type'] < 0:

            ## Check to see if we have IDs for these refs in the cache
            miss = set(gene_refs) - set(self._symbol_cache[sp_id][gene_type].keys())

            ## Some are missing, so we need to get them from the DB
            if len(miss) > 0:
                ## A mapping of (symbols) ode_ref_ids -> ode_gene_ids. The
                ## ode_ref_ids returned by this function have all been lower
                ## cased.

                ## Variants are handled using a slightly different function
                if -gene_type == db.get_variant_gene_type():
                    if not gs.get('genome_build', None):
                        self.errors.append(
                            'Variant gene sets require a genome build.'
                        )

                        return 0

                    ## Modify gene references to remove the rs prefix if it
                    ## exists
                    for i in range(len(gs['values'])):
                        gene, value = gs['values'][i]
                        gene = gene.lstrip('rs')

                        gs['values'][i] = (gene, value)

                    gene_refs = map(lambda x: x[0], gs['values'])

                    ## Should check to see if the genome build is valid...
                    ref2ode = db.get_variant_odes_by_refs(gene_refs, gs['genome_build'])

                    ## The variant ode retrieval function returns variant
                    ## references as integers, we convert them back to strings
                    ref2ode = dict(
                        map(lambda t: (str(t[0]), t[1]), ref2ode.items())
                    )
                else:
                    ref2ode = db.get_gene_ids_by_refs(gene_refs, sp_id, -gene_type)

                self._symbol_cache[sp_id][gene_type].update(ref2ode)

        ## It's a damn expression platform :/
        else:
            ## This is a mapping of (symbols) prb_ref_ids -> prb_ids for the
            ## given platform
            ref2prbid = db.get_all_platform_probes(gene_type)
            ## This is a mapping of prb_ids -> ode_gene_ids
            prbid2ode = db.get_probe2gene(ref2prbid.values())

            ## Just throw everything in the same dict, shouldn't matter since
            ## the prb_refs will be strings and the prb_ids will be ints
            self._symbol_cache[sp_id][gene_type] = dd(int)
            self._symbol_cache[sp_id][gene_type].update(ref2prbid)
            self._symbol_cache[sp_id][gene_type].update(prbid2ode)

        ref2ode = self._symbol_cache[sp_id][gene_type]

        ## duplicate detection
        dups = dd(str)

        for ref, value in gs['values']:

            ## Platform handling
            if gs['gs_gene_id_type'] > 0:
                prb_id = ref2ode[ref]
                odes = ref2ode[prb_id]

                if not prb_id or not odes:
                    self.warns.append('No gene/locus data exists for %s' % ref)
                    continue

                ## Yeah one probe reference may be associated with more than
                ## one gene/ode_gene_id, it's fucking weird. I think this is
                ## specific to affymetrix platforms
                for ode in odes:
                    ## Check for duplicate ode_gene_ids, otherwise postgres
                    ## bitches during value insertion
                    if not dups[ode]:
                        dups[ode] = ref

                    else:
                        self.warns.append(
                            '%s and %s are duplicates, only %s was added'
                            % (ref, dups[ode], dups[ode])
                        )
                        continue

                    gs['geneset_values'].append((ref, ode, value))

            ## Not platform stuff
            else:

                ## Case insensitive symbol identification
                refl = ref.lower()

                if not ref2ode[refl]:
                    self.warns.append('No gene/locus exists data for %s' % ref)
                    continue

                ode = ref2ode[refl]

                ## Prevent postgres bitching again
                if not dups[ode]:
                    dups[ode] = ref

                else:
                    self.warns.append(
                        '%s and %s are duplicates, only %s was added'
                        % (ref, dups[ode], dups[ode])
                    )
                    continue

                gs['geneset_values'].append((ref, ode, value))

        return len(gs['geneset_values'])

    def __insert_geneset_values(self, gs):
        """
        """

        for ref, ode, value in gs['geneset_values']:
            try:
                db.insert_geneset_value(
                    gs['gs_id'], ode, value, ref, gs['gs_threshold']
                )
            except Exception as e:
                print(e)
                print(gs)
                exit()

    def __insert_annotations(self, gs):
        """
        Inserts gene set ontology annotations into the DB.

        arguments
            gs: gene set object
        """

        if 'ont_ids' in gs:
            for ont_id in set(gs['ont_ids']):
                db.insert_geneset_ontology(
                    gs['gs_id'], ont_id, 'GeneWeaver Primary Annotation'
                )

    def parse_batch_file(self):
        """
        Parses a batch file to completion.

        arguments
            fp: a string, the filepath to a batch file

        returns
            A list of gene set objects (dicts) with properly filled out fields,
            ready for insertion into the GW DB.
        """

        self.errors = []
        self.warns = []

        if not self.filepath:
            self.errors('No batch file was provided.')
            return []

        self.__parse_batch_syntax(self.__read_file())

        if self.errors:
            return []

        attributions = db.get_attributions()

        for abbrev, at_id in attributions.items():
            ## Fucking NULL row in the db, this needs to be removed
            if abbrev:
                attributions[abbrev.lower()] = at_id

        ## Geneset post-processing: mapping gene -> ode_gene_ids, attributions,
        ## and annotations
        for gs in self.genesets:

            gs['gs_count'] = self.__map_gene_identifiers(gs)

            if 'at_id' in gs and gs['at_id']:
                gs['gs_attribution'] = attributions.get(gs['at_id'], None)
            else:
                gs['gs_attribution'] = None

            self.__map_ontology_annotations(gs)

        return self.genesets

    def get_geneset_pubmeds(self):
        """
        """

        if not self._pub_map:
            self._pub_map = db.get_publication_mapping()

        found = filter(lambda g: g['pmid'] in self._pub_map, self.genesets)

        for gs in found:
            gs['pub_id'] = self._pub_map[gs['pmid']]
            gs['pub'] = gs['pmid']

        pubs = map(lambda g: g['pmid'], self.genesets)

        if pubs:
            pubs = get_pubmed_articles(map(lambda g: g['pmid'], self.genesets))
        else:
            pubs = {}

        for gs in self.genesets:
            if gs['pmid'] not in pubs:
                gs['pub_id'] = None
                gs['pub'] = None

            else:
                gs['pub_id'] = None
                gs['pub'] = pubs[gs['pmid']]

    def insert_genesets(self, genesets=None):
        """
        """

        ids = []

        if not genesets:
            genesets = self.genesets

        if not self._pub_map:
            self._pub_map = db.get_publication_mapping()

        for gs in genesets:

            if not gs['gs_count']:

                self.errors.append((
                    'No genes in the set %s mapped to GW identifiers so it '
                    'was not uploaded'
                ) % gs['gs_name'])

                continue

            if not gs['pub_id'] and gs['pub']:
                if gs['pub']['pub_pubmed'] not in self._pub_map:
                    gs['pub_id'] = db.insert_publication(gs['pub'])

                    self._pub_map[gs['pub']['pub_pubmed']] = gs['pub_id']

                else:
                    gs['pub_id'] = self._pub_map[gs['pub']['pub_pubmed']]

            gs['file_id'] = self.__insert_geneset_file(gs['values'])
            gs['gs_id'] = db.insert_geneset(gs)
            self.__insert_geneset_values(gs)
            self.__insert_annotations(gs)

            ids.append(gs['gs_id'])

        return ids

    def finalize(self):
        """
        Commits DB changes.
        """

        db.commit()

class BatchWriter(object):
    """
    Serializes geneset data into the batch geneset format. This class is mainly
    meant to take a gene set, as it exists in the GW DB, and convert it into
    the batch format. However, it the error checking can be disabled to format
    any data types/contents into a batch gene set file.

    attributes:
        genesets: a list of geneset dicts to serialize
    """

    def __init__(self, filepath, genesets, is_dev=False, no_db=False):
        """
        Initializes the BatchWriter class and retrieves GeneWeaver ID -> common
        name associations.

        arguments
            filepath: path to the batch file output
            genesets: gene sets to be formatted as a batch file
            is_dev:   output hidden dev attributes/syntax
            no_db:    don't use the DB to map various DB specific IDs and table
                      features to their plain text equivalent, just output the
                      user given gene set values as-is to the batch file
        """
        self.filepath = filepath
        self.genesets = genesets
        self.is_dev = is_dev
        self.no_db = no_db
        self.errors = []

        if not self.no_db:
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

        if self.no_db:
            return threshold_type

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

        if self.no_db:
            return '@ %s' % sp_id

        if sp_id not in self.species:
            self.errors.append('Invalid species ID')
            return ''

        return '@ ' + self.species[sp_id]

    def __format_gene_type(self, gene_type):
        """
        """

        serial = ''

        if self.no_db:
            return '% {0}'.format(gene_type)

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

        if '-1' in groups or tier == 5:
            return 'A Private'

        else:
            return 'A Public'

    def __format_publication(self, pub_id, pmid=None):
        """
        """

        if pmid:
            return 'P ' + str(pmid)

        elif pub_id and not self.no_db:
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

        if self.no_db:
            return 'D %s' % at_id

        if at_id not in self.attributions:
            self.errors.append('Invalid attribution ID')
            return ''

        return 'D ' + self.attributions[at_id]

    def __format_annotations(self, annos):
        """
        """

        annos = map(lambda s: '~ ' + str(s), annos)

        return '\n'.join(annos)

    def __format_uri(self, uri):
        """
        """

        return '> ' + str(uri)

    def __passes_sanity_check(self, gs):
        """
        Checks to make sure the required portions of gene set metadata are
        present. Whether or not this metadata is valid is determined by the
        __format* functions. If descriptors are missing the function sets
        the appropriate errors.
        """

        if 'gs_abbreviation' in gs:
            gs_ident = gs['gs_abbreviation']

        elif 'gs_name' in gs:
            gs_ident = gs['gs_name']

        else:
            gs_ident = ''

        passes = True

        if 'gs_name' not in gs:
            self.errors.append(
                '%s input gene set is missing gs_name' % gs_ident
            )

            passes = False

        if 'gs_abbreviation' not in gs:
            self.errors.append(
                '%s input gene set is missing gs_abbreviation' % gs_ident
            )

            passes = False

        if 'gs_description' not in gs:
            self.errors.append(
                '%s input gene set is missing gs_description' % gs_ident
            )

            passes = False

        if 'sp_id' not in gs:
            self.errors.append(
                '%s input gene set is missing sp_id' % gs_ident
            )

            passes = False

        if 'gs_threshold_type' not in gs:
            self.errors.append(
                '%s input gene set is missing gs_threshold_type' % gs_ident
            )

            passes = False

        if 'geneset_values' not in gs:
            self.errors.append(
                '%s input gene set is missing geneset_values' % gs_ident
            )

            passes = False

        return passes

    def serialize(self, versioning=''):
        """
        Formats the list of genesets into a single batch file and outputs the
        result.
        """

        serial = []
        serial.append('## Machine generated BGF file')

        if versioning:
            serial.append('## %s' % versioning)

        serial.append('#')
        serial.append('')

        threshold_type = None
        threshold = None
        species = None
        gene_type = None
        tier = None
        access = None
        pmid = None
        usr_id = None
        pub_id = None
        at_id = None
        annos = None
        gs_uri = None

        for gs in self.genesets:

            if not self.__passes_sanity_check(gs):
                continue

            ## General parameters are allowed to change in between geneset
            ## definitions
            if threshold_type != gs['gs_threshold_type'] or\
               threshold != gs.get('gs_threshold', threshold):

                threshold_type = gs['gs_threshold_type']
                threshold = gs.get('gs_threshold', threshold)

                serial.append(
                    self.__format_threshold(threshold_type, threshold)
                )

            if species != gs['sp_id']:
                species = gs['sp_id']

                serial.append(self.__format_species(species))

            if gene_type != gs['gs_gene_id_type']:
                gene_type = gs['gs_gene_id_type']

                serial.append(self.__format_gene_type(gene_type))

            if gs.get('pmid', None) and pmid != gs['pmid']:
                pmid = gs['pmid']

                serial.append(self.__format_publication(None, pmid))

            if gs.get('pub_id', None) and pub_id != gs['pub_id']:
                pub_id = gs['pub_id']

                serial.append(self.__format_publication(pub_id))

            if gs.get('annotations', None) and annos != gs['annotations']:
                annos = gs['annotations']

                serial.append(self.__format_annotations(annos))

            if gs.get('gs_uri', None) and gs_uri != gs['gs_uri']:
                gs_uri = gs['gs_uri']

                serial.append(self.__format_uri(gs_uri))

            if not access:
                access = True

                serial.append(self.__format_access(
                    gs.get('gs_groups', ''), gs.get('cur_id', None)
                ))

            ## Enable developer parameters
            if self.is_dev:
                if gs.get('cur_id', None) and tier != gs['cur_id']:
                    tier = gs['cur_id']

                    serial.append(self.__format_tier(tier))

                if gs.get('at_id', None) and at_id != gs['at_id']:
                    at_id = gs['at_id']

                    serial.append(self.__format_attribution(at_id))

                if gs.get('usr_id', None) and usr_id != gs['usr_id']:
                    usr_id = gs['usr_id']

                    serial.append(self.__format_user(usr_id))

            serial.append('')
            serial.append(self.__format_label(gs['gs_abbreviation']))
            serial.append(self.__format_name(gs['gs_name']))
            serial.append(self.__format_description(gs['gs_description']))
            serial.append('')
            serial.append(self.__format_geneset_values(gs['geneset_values']))
            serial.append('')

        if self.errors:
            return False

        with open(self.filepath, 'w') as fl:
            print('\n'.join(serial), file=fl)

        return True

if __name__ == '__main__':
    pass

