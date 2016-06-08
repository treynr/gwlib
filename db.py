#!/usr/bin/env python2

## file:    db.py
## desc:    Contains all the important functions for accessing and querying the
##          GeneWeaver DB.
## vers:    0.1.0
## auth:    TR
#

import ConfigParser
import datetime as dt
import os
import psycopg2
import random

## Config is in the same folder as the rest of the GW library
CONFIG_PATH = 'gwlib.cfg'

    ## CONFIGURATION ##
    ###################

def create_config():
    """
    Creates a default config containing database connection info.
    """

    with open(CONFIG_PATH, 'w') as fl:

        print >> fl, '## GWLib DB Configuration'
        print >> fl, '#'
        print >> fl, ''
        print >> fl, '[db]'
        print >> fl, '## Postgres server address'
        print >> fl, 'host = 127.0.0.1'
        print >> fl, '## Postgres database name'
        print >> fl, 'database = dbname'
        print >> fl, 'port = 5432'
        print >> fl, 'user = someguy'
        print >> fl, 'password = somepassword'
        print >> fl, '## Force psycopg2 to commit after every statement'
        print >> fl, 'autocommit = false'
        print >> fl, '## GeneWeaver usr_id to use when inserting genesets'
        print >> fl, 'usr_id = 3507787'
        print >> fl, ''

def load_config():
    """
    Attempts to load the lib config. If it doesn't exist, one is created. Makes
    no attempt to check if the config is correct since any errors will prevent
    the application from loading.

    :ret object: the parsed config as a ConfigParser object
    """

    if not os.path.exists(CONFIG_PATH):
        create_config()

        print '[!] A new config was created for the DB library.'
        exit()

    parser = ConfigParser.RawConfigParser()

    parser.read(CONFIG_PATH)

    return parser

def get(section, option):
    """
    Retrieves the value for an option under the specified section.

    :type section: str
    :arg section: config section an option falls under

    :type option: str
    :arg option: option 

    :ret str: the value associated with the given section, option combo
    """

    return PARSER.get(section, option)

## Only time this really ever fails is when the config is bad or the postgres
## server isn't running. Executed on import.
try:
    parser = load_config()
    ##
    host = parser.get('db', 'host')
    database = parser.get('db', 'database')
    user = parser.get('db', 'user')
    password = parser.get('db', 'password')
    port = parser.get('db', 'port')
    ##
    constr = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" 
    constr = constr % (host, database, user, password, port)
    conn = psycopg2.connect(constr)

    #conn.autocommit(parser.get('db', 'autocommit') == 'true')
    #conn.autocommit(parser.getboolean('db', 'autocommit'))

except Exception as e:
    print '[!] Oh noes, failed to connect to the db'
    print 'The exception:'
    print e

    exit()

        ## CLASSES ##
        #############

class PooledCursor(object):
    """
    Modeled after the PooledCursor object in GeneWeaver's source code.
    Encapsulates psycopg2 connections and cursors so they can be used with
    python features that are unsupported in older versions of psycopg2.
    """

    def __init__(self, conn=conn):
        self.connection = conn
        self.cursor = None

    def __enter__(self):
        self.cursor = self.connection.cursor()

        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()

            self.cursor = None


## This file attempts to follow psycopg best practices, outlined in its FAQ: 
## http://initd.org/psycopg/docs/faq.html
## A few notable designs:
#
## New cursors are generated for every query to minimize caching and client
## side memory usage. Using cursors in a with statement should automatically
## call their destructors.
#
## The config can set connections to autocommit mode in order to avoid
## littering the postgres server with "idle in transaction" sessions.
#

        ## HELPERS ##
        #############

def asciify(s):
    """
    Takes a string, which could be unicode or a regular ASCII string and
    forcibly converts it to ASCII. Any conversion errors are ignored during the
    process. If the given argument isn't a string, the function does nothing.

    :type s: str
    :arg s: string being converted to ASCII
    """

    return s.encode('ascii', 'ignore') if isinstance(s, basestring) else s

def dictify(cursor):
    """
    Converts each row returned by the cursor into a list of dicts, where  
    each key is a column name and each value is whatever is returned by the
    query.

    :type cursor: object
    :arg cursor: the psycopg cursor

    :ret list: dicts containing the results of the SQL query
    """

    dlist = []
    
    for row in cursor:
        ## Prevents unicode type errors from cropping up later. Convert to
        ## ascii, ignore any conversion errors.
        row = map(lambda s: asciify(s), row)
        d = {}

        for i, col in enumerate(cursor.description):
            d[col[0]] = row[i]

        dlist.append(d)

    return dlist

def listify(cursor):
    """
    """

    return map(lambda t: t[0], cursor.fetchall())

def tuplify(thing):
    """
    """

    if type(thing) == list:
        return tuple(thing)

    elif type(thing) == tuple:
        return thing

    else:
        return (thing,)

def associate(cursor):
    """
    Creates a simple mapping from all the rows returned by the cursor. The
    first tuple member serves as the key and the rest are the values. Unlike
    dictify, this does not use column names and generates a single dict from
    all returned rows. Be careful since duplicates are overwritten.

    :type cursor: object
    :arg cursor: the psycopg cursor

    :ret dict: mapping of tuple element #1 to the rest
    """

    d = {}

    for row in cursor:
        row = map(lambda s: asciify(s), row)

        ## 1:1
        if len(row) == 2:
            d[row[0]] = row[1]

        ## 1:many
        else:
            d[row[0]] = list(row[1:])

    return d

def commit():
    """
    Commits the transaction.
    """
    conn.commit()

        ## SELECTS ##
        #############

def get_species():
    """
    Returns a species name and ID mapping for all the species currently 
    supported by GW.

    :ret dict: mapping of sp_names to sp_ids
    """

    #with conn.cursor() as cursor:
    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_name, sp_id
            FROM    odestatic.species;
            '''
        )

        return associate(cursor)

def get_gene_ids(refs, sp_id=None):
    """
    Given a set of external reference IDs, this returns a mapping of 
    ode_ref_ids to ode_gene_ids. An optional species id list can be provided to
    limit gene results by species.

    Reference IDs are always strings (even if they're numeric) and should be
    properly capitalized. If duplicate references exist in the DB (unlikely)
    then they are overwritten in the return dict. The returned ode_gene_ids are
    longs.

    :type refs: list/tuple
    :arg refs: external DB reference IDs

    :ret dict: mapping of ode_ref_ids to ode_gene_ids
    """

    if type(refs) == list:
        refs = tuple(refs)

    #with conn.cursor() as cursor:
    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_ref_id, ode_gene_id
            FROM    extsrc.gene
            WHERE   ode_ref_id IN %s;
            ''', 
                (refs,)
        )

        return associate(cursor)

def get_gene_ids_by_species(refs, sp_id):
    """
    Exactly like get_gene_ids() above but allows for a species ID to be given
    and the results limited by species.

    :type refs: list/tuple
    :arg refs: external DB reference IDs

    :type sp_id: int
    :arg sp_id: GW species ID

    :ret dict: mapping of ode_ref_ids to ode_gene_ids
    """

    if type(refs) == list:
        refs = tuple(refs)

    #with conn.cursor() as cursor:
    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_ref_id, ode_gene_id
            FROM    extsrc.gene
            WHERE   sp_id = %s AND 
                    ode_ref_id IN %s;
            ''', 
                (sp_id, refs)
        )

        return associate(cursor)

def get_gene_refs(gene_ids):
    """
    Retrieves external reference IDs for the given list of ode_gene_ids. The
    inverse of get_gene_ids().

    :type gene_ids: list/tuple
    :arg gene_ids: internal GeneWeaver gene IDs (ode_gene_id)

    :ret dict: mapping of ode_gene_ids to a list of its reference IDs
    """

    if type(gene_ids) == list:
        gene_ids = tuple(gene_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   ode_gene_id IN %s;
            ''', 
                (gene_ids,)
        )

        maplist = dictify(cursor)
        mapping = {}

        for d in maplist:
            if d['ode_gene_id'] in mapping:
                mapping[d['ode_gene_id']].append(d['ode_ref_id'])

            else:
                mapping[d['ode_gene_id']] = [d['ode_ref_id']]

        return mapping

def get_gene_refs_by_type(gene_ids, gene_id_type):
    """
    Exactly like get_gene_refs() but allows for retrieval by specific gene ID
    types.

    :type gene_ids: list/tuple
    :arg gene_ids: internal GeneWeaver gene IDs (ode_gene_id)

    :type gene_id_type: int
    :arg gene_ids: gene ID type to retrieve

    :ret dict: mapping of ode_gene_ids to its ref ID of a specific type
    """

    if type(gene_ids) == list:
        gene_ids = tuple(gene_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   gdb_id = %s AND 
                    ode_gene_id IN %s;
            ''', 
                (gene_id_type, gene_ids)
        )

        maplist = dictify(cursor)
        mapping = {}

        for d in maplist:
            if d['ode_gene_id'] in mapping:
                mapping[d['ode_gene_id']].append(d['ode_ref_id'])

            else:
                mapping[d['ode_gene_id']] = [d['ode_ref_id']]

        return mapping

def get_preferred_gene_refs(gene_ids):
    """
    Exactly like get_gene_refs() but only retrieves preferred ode_ref_ids.
    There _should_ only be one preferred ID.

    :type gene_ids: list/tuple
    :arg gene_ids: internal GeneWeaver gene IDs (ode_gene_id)

    :ret dict: mapping of ode_gene_ids to its ref ID of a specific type
    """

    if type(gene_ids) == list:
        gene_ids = tuple(gene_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   ode_pref = 't' AND
                    ode_gene_id IN %s;
            ''', 
                (gene_ids,)
        )

        return associate(cursor)

def get_genesets_by_tier(tiers=[1,2,3,4,5], size=5000):
    """
    Returns a list of normal (i.e. their status is not deleted or deprecated) 
    geneset IDs that belong in a particular tier or set of tiers. Also allows
    the user to retrieve genesets under a particular size.

    :type tiers: list
    :arg tiers: tiers to retrieve genesets from

    :type size: int
    :arg size: geneset size (gs_count) to use as a filter
    """

    if type(tiers) == list:
        tiers = tuple(tiers)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    cur_id IN %s AND
                    gs_count < %s;
            ''', 
                (tiers, size)
        )

        return listify(cursor)

def get_genesets_by_attribute(at_id, size=5000):
    """
    Returns a list of normal (i.e. their status is not deleted or deprecated) 
    geneset IDs that belong to a particular attribution group. Also allows
    the user to retrieve genesets under a particular size.

    :type at_id: int
    :arg at_id: GeneWeaver attribution ID 

    :type size: int
    :arg size: geneset size (gs_count) to use a filter
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    at_id = %s AND
                    gs_count < %s;
            ''', 
                (at_id, size)
        )

        return listify(cursor)

def get_geneset_values(gs_ids):
    """
    Returns all the geneset_values from the given list of genesets.

    :type gs_ids: list
    :arg gs_ids: geneset IDs

    :ret list: geneset_value objects containing column names as keys
    """

    if type(gs_ids) == list:
            gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, ode_gene_id, gsv_value
            FROM    extsrc.geneset_value
            WHERE   gs_id IN %s;
            ''', 
                (gs_ids,)
        )

        return dictify(cursor)

def get_gene_homologs(gene_ids):
    """
    Returns all homology IDs for the given list of gene IDs.

    :type gene_ids: list
    :arg gene_ids: ode_gene_ids

    :ret dict: mapping of ode_gene_ids to homology IDs (hom_id)
    """

    if type(gene_ids) == list:
        gene_ids = tuple(gene_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_gene_id, hom_id
            FROM    extsrc.homology
            WHERE   ode_gene_id IN %s;
            ''', 
                (gene_ids,)
        )

        return associate(cursor)

    ## INSERTS ##
    #############

def insert_geneset(gs):
    """
    Inserts a new geneset into the database. 

    :type gs: dict
    :arg gs: each key in the dict corresponds to a column in the geneset table

    :ret long: if insertion is successfull the new gs_id is returned
    """

    ## The following fields should not be null but aren't checked by the DB
    if ('cur_id' not in gs) or 
       ('gs_description' not in gs) or
       ('sp_id' not in gs):
        return 0

    ## Sensible defaults
    if ('file_id' not in gs):
        gs['file_id'] = 0

    if ('gs_created' not in gs):
        gs['gs_created'] = 'NOW()'

    if ('pub_id' not in gs):
        gs['pub_id'] = None

    ## 3 = binary threshold
    if ('gs_threshold_type' not in gs):
        gs['gs_threshold_type'] = 3
        gs['gs_threshold'] = 1

    if ('gs_groups' not in gs):
        gs['gs_groups'] = 0

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO geneset

                (usr_id, file_id, gs_name, gs_abbreviation, pub_id, cur_id,
                gs_description, sp_id, gs_count, gs_threshold_type,
                gs_threshold, gs_groups, gs_gene_id_type, gs_created, gsv_qual,
                gs_attribution)

            VALUES
                
                (%(usr_id)s, %(file_id)s, %(gs_name)s, %(gs_abbreviation)s, 
                %(pub_id)s, %(cur_id)s, %(gs_description)s, %(sp_id)s, 
                %(gs_count)s, %(gs_threshold_type)s, %(gs_threshold)s, 
                %(gs_groups)s, %(gs_gene_id_type)s, %(gs_created)s, 
                %(gsv_qual)s, %(gs_attribution))
            
            RETURNING gs_id;
            ''', 
                gs
        )

        ## Returns a list of tuples [(gs_id)]
        res = g_cur.fetchone()

        return cursor.fetchone()[0]

def insert_geneset_value(gs_id, gene_id, value, name, threshold):
    """
    Inserts a new geneset_value into the database. 

    :type gs_id: long
    :arg gs_id: gs_id the value is associated with

    :type gene_id: long
    :arg gene_id: ode_gene_id

    :type value: int/long/float/double
    :arg value: some numeric value associated with the gene (e.g. p-value)

    :type name: str
    :arg name: an ode_ref_id for the given ode_gene_id

    :type threshold: int/long/float/double
    :arg threshold: the threshold for the geneset associated with this value

    :ret long: if insertion is successfull the gs_id for this value is returned
    """

    ## thresh will eventually specify the value for gsv_in_threshold
    thresh = 't' if value <= thresh else 'f'

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO geneset_value

                (gs_id, ode_gene_id, gsv_value, gsv_source_list,
                gsv_value_list, gsv_in_threshold, gsv_hits, gsv_date)

            VALUES
                
                (%s, %s, %s, %s, %s, %s, 0, NOW());

            RETURNING gs_id;
            ''', 
                (gs_id, gene_id, value, [name], [float(value)], thresh)
        )

        ## Returns a list of tuples [(gs_id)]
        res = g_cur.fetchone()

        return cursor.fetchone()[0]

def insert_gene(gene_id, ref_id, gdb_id, sp_id, pref='f'):
    """
    Inserts a new gene (ode_gene_id, ode_ref_id pair) into the database. The
    gene should already be associated with an existing ode_gene_id; if one
    doesn't exist, a new one should be created using insert_new_gene().

    :type gene_id: long
    :arg gene_id: ode_gene_id

    :type ref_id: str
    :arg ref_id: external reference (ode_ref_id)

    :type gdb_id: int
    :arg gdb_id: an ID specifying the type of gene being inserted (see genedb)

    :type sp_id: int
    :arg sp_id: an ID specifying the species this gene belongs to

    :type pref: str, 't' or 'f'
    :arg pref: if true, sets this as the preferred gene (should almost always
               be false)

    :ret tuple: the (ode_gene_id, ode_ref_id) tuple serving as the primary key
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO gene

                (ode_gene_id, ode_ref_id, gdb_id, sp_id, ode_pref, ode_date)

            VALUES
                
                (%s, %s, %s, %s, %s, %s, NOW());

            RETURNING (ode_gene_id, ode_ref_id);
            ''', 
                (gene_id, ref_id, gdb_id, sp_id, pref)
        )

        return cursor.fetchone()[0]

    ## UPDATES ##
    #############

    ## DELETES ##
    #############

#### findAncientMeshSets (DEPRECATED)
##
#### Returns the gs_ids of MeSH genesets from the time before gene2mesh.
##
def findAncientMeshSets():
    query = ("SELECT gs_id FROM production.geneset WHERE cur_id IS NULL AND "
              "gs_name NOT ILIKE '%%in ctd%%' AND gs_name ILIKE '%%mesh%%';")

    g_cur.execute(query)

    res = g_cur.fetchall()

    return map(lambda x: x[0], res)

#### deleteGeneset
##
#### Marks a geneset as deleted. Since nothing is ever deleted, it's
#### simply marked as such.
##
def deleteGeneset(gs_id):
    updateGenesetStatus(gs_id, 'deleted')

## There's a subtle difference between getGeneIds and the "sensitive" version
## below it. getGeneIds requires gene symbols to exactly match their
## counterparts in the DB. The SQL query considers the genes BRCA1 and Brca1 as
## different. The sensitive version, doesn't require proper capitalization BUT
## this comes at the expense of run time. The SQL query takes for-fucking-ever
## and should only be used in certain cases.

def getGenesetSizes(gsids):
        if type(gsids) == list:
                gsids = tuple(gsids)

        query = '''SELECT gs_id, gs_count
                           FROM production.geneset
                           WHERE gs_id IN %s;'''

        g_cur.execute(query, [gsids])

        ## Returns a list of tuples [(gs_id, gs_count)]
        res = g_cur.fetchall()
        d = {}

        ## We return a dict of gs_id --> gs_count
        for tup in res:
                d[tup[0]] = tup[1]

        return d



#### getGenesetNames
##
#### Returns all gs_names for the given gs_ids. The results are returned
#### as a mapping of gs_ids -> gs_name.
##
#### arg: [integer],  list of gs_ids
#### ret: dict, mapping of gs_ids (int) to a gs_name (string)
##
def getGenesetNames(gsids):
        if type(gsids) == list:
                gsids = tuple(gsids)

        query = '''SELECT gs_id, gs_name 
                           FROM production.geneset
                           WHERE gs_id IN %s;'''
        d = {}

        g_cur.execute(query, [gsids])

        res = g_cur.fetchall()

        ## We return a dict, k: gs_id; v: gs_name
        for tup in res:
                        d[tup[0]] = tup[1]

        return d

#### getGenesetAbbreviations
##
#### Returns all gs_abbreviations for the given gs_ids. The results are
#### returned as a mapping of gs_ids -> gs_abbreviation.
##
#### arg: [integer],  list of gs_ids
#### ret: dict, mapping of gs_ids (int) to a gs_name (string)
##
def getGenesetAbbreviations(gsids):
        if not gsids:
                return {}
        if type(gsids) == list:
                gsids = tuple(gsids)

        query = '''SELECT gs_id, gs_abbreviation 
                           FROM production.geneset
                           WHERE gs_id IN %s;'''
        d = {}

        g_cur.execute(query, [gsids])

        res = g_cur.fetchall()

        ## We return a dict, k: gs_id; v: gs_name
        for tup in res:
                        d[tup[0]] = tup[1]

        return d

#### getGeneType
##
#### Returns the gdb_id for the given short name.
##
#### arg: string, gdb_shortname to use for retrieving the gdb_id
#### ret: integer, gdb_id for SNP type. None if it doesn't exist in the DB
##
def getGeneType(short):
        query = '''SELECT gdb_id
                           FROM odestatic.genedb
                           WHERE gdb_shortname LIKE %s;'''

        g_cur.execute(query, [short])

        res = g_cur.fetchone()

        if not res:
                return None
        else:
                return res[0]

#### getSnpGenes
##
#### Returns a mapping of all rolled up SNPs in the DB. 
##
#### arg: [int], gdb_id for the SNP gene type
#### ret: dict, mapping of SNP ID (ode_ref_id) -> ode_gene_id
##
def getSnpGenes(gdbid):
        query = '''SELECT ode_ref_id, ode_gene_id
                           FROM extsrc.gene
                           WHERE gdb_id = %s;''' 
        
        g_cur.execute(query, [gdbid])

        ## Returns a list of tuples [(ode_ref_id, ode_gene_id)]
        res = g_cur.fetchall()
        d = {}

        for tup in res:
                d[tup[0]] = tup[1]

        return d

#### getMeshIdsOld
##
#### Returns a list of gs_ids for all MeSH sets generated by gene2mesh. 
##
#### This uses the old MeSH geneset format for searching. The MeSH genesets
#### created by an older version of gene2mesh uses 'MeSH Set (...' as the
#### gs_name. 
#### This function is deprecated and will be removed from future versions.
##
#### arg, int list of gs_ids
#### ret, dict mapping gs_ids (int) to list of ode_gene_ids ([int])
##
def getMeshIdsOld():

        query = ("SELECT gs_id FROM production.geneset WHERE "
                         "gs_status NOT LIKE 'de%%' AND "
                         "gs_name ilike 'mesh set (%%';")
        d = {}

        g_cur.execute(query, [])

        res = g_cur.fetchall()

        # Strip out the tuples, only returning a list
        return map(lambda x: x[0], res)

#### getMeshIds
##
#### Returns a list of gs_ids for all current, non-deprecated MeSH sets 
#### generated by gene2mesh. 
##
#### ret: [integer], list of gs_ids corresponding to MeSH sets 
##
def getMeshIds():

        query = ("SELECT gs_id FROM production.geneset WHERE "
                         "gs_status NOT LIKE 'de%%' AND "
                         "gs_name like '[MeSH] %%:%%';")
        d = {}

        g_cur.execute(query, [])

        res = g_cur.fetchall()

        # Strip out the tuples, only returning a list
        return map(lambda x: x[0], res)

def getMeshSetsByName(names):

        if type(names) == list:
                names = tuple(names)

        query = '''SELECT gs_abbreviation, gs_id
                           FROM production.geneset
                           WHERE gs_status NOT ILIKE 'de%%' AND
                                         gs_name LIKE '[MeSH] %%' AND
                                         gs_abbreviation IN %s'''

        g_cur.execute(query, [names])

        res = g_cur.fetchall()
        d = {}

        for tup in res:
                d[tup[0]] = tup[1]
                                         
        return d

#### getMeshSetsOld
##
#### Returns all current MeSH genesets. Result is returned as a dict, gs_ids ->
#### [ode_gene_id]. 
##
#### Older version, see getMeshIdsOld comments above. Deprecated and will be
#### removed in a future release.
##
#### ret, dict mapping gs_ids (int) to list of ode_gene_ids ([int])
##
def getMeshSetsOld():
        return getGenesetGeneIds(getMeshIdsOld())

#### getMeshSets
##
#### Returns the contents (ode_gene_ids) of all current, non-deprecated MeSH genesets. 
##
#### ret: dict, mapping gs_ids (int) to list of ode_gene_ids ([int])
##
def getMeshSets():
        return getGenesetGeneIds(getMeshIds())

#### getMeshSetNames
##
#### Returns all current, non-deprecated MeSH terms. The latest version of
#### gene2mesh puts the MeSH term (by itself) as the gs_abbreviation. The term
#### can also be found in the gs_name and gs_description, but it would have to
#### be parsed out.
##
#### ret: dict, mapping gs_ids (int) to MeSH term (string)
##
def getMeshSetNames():
        return getGenesetAbbreviations(getMeshIds())

#### getMeshSetNamesOld
##
#### Returns all current MeSH geneset naames (terms). Result is returned as a 
#### dict, gs_ids -> names. The names are the MeSH terms themselves.
##
#### Old, deprecated and removed in a future release.
##
#### ret, dict mapping gs_ids (int) to list of ode_gene_ids ([int])
##
def getMeshSetNamesOld():
        return getGenesetNames(getMeshIds())

#### parseMeshTerm
##
#### Given a mesh geneset name, parses out the mesh term.
##
def parseMeshTerm(s):
        import re

        return re.match('MeSH Set \("(.+)"')[1]

#### getAttributionId
##
#### Given an attribution abbreviation, this function retrieves the attribution
#### ID (at_id) for that abbreviation.
##
#### arg: string, abbr, the attribution abbreviation to search for
#### ret: int, at_id for the given abbrev. returns 0 if nothing is found
##
def getAttributionId(abbr):
        query = '''SELECT at_id
                           FROM odestatic.attribution
                           WHERE at_abbrev ilike %s;'''

        g_cur.execute(query, [abbr])

        res = g_cur.fetchall()

        if not res:
                return 0
        else:
                return res[0][0]

#### makeRandomFilename
##
#### Generates a random filename for the file_uri column in the file table.
#### The string returned is 'GW_' + date + '_' + a random six letter
#### alphanumeric string.
##
def makeRandomFilename():
        lets = 'abcdefghijklmnopqrstuvwxyz1234567890'
        rstr = ''
        now = dt.datetime.now()

        for i in range(6):
                rstr += random.choice(lets)

        return ('GW_' + str(now.year) + '-' + str(now.month) + '-' +
                        str(now.day) + '_' + rstr)

#### makeGeneset
##
#### Given a shitload of arguments, this function returns a dictionary
#### representation of a single geneset. Each key is a different column
#### found in the geneset table. Not all columns are represented.
#### Just a note: grp should (usually) be '-1'.
##
##
def makeGeneset(name, abbr, desc, spec, pub, grp, ttype, thresh, gtype, vals,
                                usr=0, cur_id=5, file_id=0, at_id=0):
        gs = {}

        gs['gs_name'] = name
        gs['gs_abbreviation'] = abbr
        gs['gs_description'] = desc
        gs['sp_id'] = int(spec)
        gs['gs_groups'] = grp
        gs['pub_id'] = pub      # The pubmed article still needs to retrieved
        gs['gs_threshold_type'] = int(ttype)
        gs['gs_threshold'] = thresh
        gs['gs_gene_id_type'] = int(gtype)
        gs['usr_id'] = int(usr)
        gs['values'] = vals # Not a column in the geneset table; processed later
        gs['file_id'] = file_id
        gs['gs_attribution'] = at_id

        ## Other fields we can fill out
        gs['gs_count'] = len(vals)
        gs['cur_id'] = cur_id                   # auto private tier?

        return gs

#### insertFile
##
#### Inserts a new row into the file table. Most of the columns for the file
#### table are required as arguments.
##
def insertFileIntoDb(size, uri, contents, comments):
        query = '''INSERT INTO production.file 
                           (file_size, file_uri, file_contents, file_comments, 
                           file_created, file_changes)
                           VALUES (%s, %s, %s, %s, NOW(), \'\') 
                           RETURNING file_id;'''
        vals = [size, uri, contents, comments]

        g_cur.execute('set search_path = extsrc,production,odestatic;')
        g_cur.execute(query, vals)

        ## Returns a list of tuples [(file_id)]
        res = g_cur.fetchall()

        return res[0][0]

## score type 5
def insertFile(gsv):
        contents = ''

        for t in gsv:
                contents += (str(t[0]) + '\t' + str(t[1]) + '\n')

        return insertFileIntoDb(len(gsv), makeRandomFilename(), contents, '')


#### insertGeneset
##
#### Given a dict whose keys refer to columns of the geneset table,
#### this function inserts a new geneset into the db. 
#### Don't forget to commit changes after calling this function.
##
def insertGeneset(gd):
        query = ('INSERT INTO geneset (file_id, usr_id, cur_id, sp_id, '
                         'gs_threshold_type, gs_threshold, gs_created, gs_updated, '
                         'gs_status, gs_count, gs_uri, gs_gene_id_type, gs_name, '
                         'gs_abbreviation, gs_description, gs_attribution, gs_groups, '
                         'pub_id) '
                         'VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), \'normal\', '
                         '%s, \'\', %s, %s, %s, %s, %s, %s, %s) RETURNING gs_id;')

        vals = [gd['file_id'], gd['usr_id'], gd['cur_id'], gd['sp_id'], 
                        gd['gs_threshold_type'], gd['gs_threshold'], gd['gs_count'], 
                        gd['gs_gene_id_type'], gd['gs_name'], gd['gs_abbreviation'],
                        gd['gs_description'], gd['gs_attribution'], gd['gs_groups'], 
                        gd['pub_id']]

        g_cur.execute('set search_path = extsrc,production,odestatic;')
        g_cur.execute(query, vals)

        ## Returns a list of tuples [(gs_id)]
        res = g_cur.fetchall()

        return res[0][0]

#### insertGenesetValue
##
#### Inserts a new row into the geneset_value table using the given gs_id. 
##
def insertGenesetValue(gs_id, gene_id, value, name, thresh):
        query = '''INSERT INTO extsrc.geneset_value 
                           (gs_id, ode_gene_id, gsv_value, gsv_hits, gsv_source_list, 
                           gsv_value_list, gsv_in_threshold, gsv_date) 
                           VALUES (%s, %s, %s, 0, %s, ARRAY[0], %s, NOW());'''
        vals = [gs_id, gene_id, value, [name], thresh]

        g_cur.execute(query, vals)

def insertGene(gene_id, ref_id, gdb_id, sp_id, pref='f'):
        query = '''INSERT INTO extsrc.gene
                           (ode_gene_id, ode_ref_id, gdb_id, sp_id, ode_pref, ode_date)
                           VALUES
                           (%s, %s, %s, %s, %s, NOW());'''
        vals = [gene_id, ref_id, gdb_id, sp_id, pref]

        g_cur.execute(query, vals)

#### updateGenesetCount
##
#### Updates gs_count for a given gs_id.
##
def updateGenesetCount(gs_id, count):
        query = 'UPDATE production.geneset SET gs_count = %s WHERE gs_id = %s;'

        g_cur.execute(query, [count, gs_id])

#### updateGenesetStatus
##
#### Updates gs_count for a given gs_id.
##
def updateGenesetStatus(gs_id, status):
        query = 'UPDATE production.geneset SET gs_status = %s WHERE gs_id = %s;'

        g_cur.execute(query, [status, gs_id])

#### deprecateGeneset
##
#### Marks a geneset for deprecation. Since nothing is ever deleted, it's
#### simply marked as such.
##
def deprecateGeneset(gs_id):
        updateGenesetStatus(gs_id, 'deprecated')

#### deleteGenesetValues
##
#### Removes all geneset_values for a given gs_id.
##
def deleteGenesetValues(gs_id):
        if not gs_id:
                return

        query = 'DELETE FROM extsrc.geneset_value WHERE gs_id = %s;'

        g_cur.execute(query, [gs_id])


## query_ontol_ids
#
## Returns all ontology IDs (ont_id) associated with a particular gene set ID
#
## arg0, a gene set ID (int)
## ret, list of all ontology IDs associated with the given gene set
#
def query_ontol_ids(id):
        if (id is None) or (id == 0):
                return []

        query = "SELECT ont_id FROM extsrc.geneset_ontology WHERE gs_id=%s;"
        g_cur.execute(query, [id])

        res = g_cur.fetchall();
        # Iterates over the list and moves the gs_id from the tuple to a new list
        return map(lambda x: x[0], res)

## findblahblah...
#
## Given an ontology term, returns all the genesets annotated to that term.
#
def findGenesetsWithOntology(ont, tiers=[3,4,5]):
        if not ont:
                return []

        # Limit to MeSH 
        #query = ('SELECT ego.gs_id, ego.ont_id, eo.ont_name FROM '
        #                 'extsrc.geneset_ontology AS ego JOIN extsrc.ontology AS eo ON '
        #                 'ego.ont_id=eo.ont_id WHERE eo.ont_name=\'%s\' AND eo.ontdb_id=4 '
        #                 ';')
        #query = ('SELECT ego.gs_id, ego.ont_id, eo.ont_name FROM '
        query = ('SELECT ego.gs_id, pg.gs_name FROM '
                         'extsrc.geneset_ontology AS ego JOIN extsrc.ontology AS eo ON '
                         'ego.ont_id=eo.ont_id JOIN production.geneset AS pg ON '
                         #'pg.gs_id=ego.gs_id WHERE eo.ont_name=%s ' #AND eo.ontdb_id=4 '
                         'pg.gs_id=ego.gs_id WHERE eo.ont_name IN %s ' #AND eo.ontdb_id=4 '
                         'AND pg.gs_count < 1000 AND pg.cur_id=ANY(%s);')

        g_cur.execute(query, [ont, tiers])

        res = g_cur.fetchall();

        return res
        #return map(lambda x: x[0], res)

def genericOntologySearch(ont, name=False):
        #query = ('SELECT ont_name, ont_description FROM extsrc.ontology WHERE '
        #                 'ont_description LIKE %%%s%%;')
        if name:
                query = ('SELECT ont_name, ont_description FROM extsrc.ontology WHERE '
                                 'ont_name ILIKE \'%%\'||%s||\'%%\';')
        else:
                query = ('SELECT ont_name, ont_description FROM extsrc.ontology WHERE '
                                 'ont_description ILIKE \'%%\'||%s||\'%%\';')

        g_cur.execute(query, [ont])

        return g_cur.fetchall();

## Given a tuple of gs_ids, returns the species for each as a (gs_id, sp_id)
## tuple.
def queryGenesetSpecies(ids):
        query = 'SELECT gs_id, sp_id FROM production.geneset WHERE gs_id IN %s;'

        g_cur.execute(query, [ids])

        return g_cur.fetchall()

def queryGenesetNames(ids):
        if type(ids) == str or type(ids) == int or type(ids) == long:
                ids = [long(ids)]
        if type(ids) == list:
                ids = tuple(ids)

        query = 'SELECT gs_id, gs_name FROM production.geneset WHERE gs_id IN %s;'

        g_cur.execute(query, [ids])

        return g_cur.fetchall()

## query_ontols
#
## Returns all the ontologies associated with a particular gene set.
## maybe edit this function to return different crap later?
## TODO: limit by ontology types (e.g. GO or MeSH)
#
## arg0, a gene set ID (int)
## ret, list of tuples containing the ont_id and ont_name
#
def queryOntologies(id, ont=None):
        if (id is None) or (id == 0):
                return []

        onts = {1:'GO', 2:'MP', 3:'MA', 5:'EDAM', 4:'MeSH', 
                        'GO':1, 'MP':2, 'MA':3, 'EDAM':5, 'MeSH':4}
        query = ("SELECT eo.ont_id, eo.ont_name FROM extsrc.ontology eo JOIN "
                         "extsrc.geneset_ontology ego ON eo.ont_id=ego.ont_id JOIN "
                         "production.geneset pg ON pg.gs_id=ego.gs_id WHERE pg.gs_id=%s")

        # If the ontology type isn't found in the above dict...
        if (ont is not None) and (ont not in onts):
                ont = None
        # Check if the ontology type is a number, if not, convert (using dict)
        if (ont is not None) and (not isinstance(ont, int)):
                ont = onts[ont]
        # If we want to limit by ontology types
        if ont is not None:
                query += " AND eo.ontdb_id=%s;"
                g_cur.execute(query, [id, ont])
        else:
                query += ";"
                g_cur.execute(query, [id])

        return g_cur.fetchall()

## Same function as above but is passed a list of IDs to query
def queryOntologiesList(ids, ont=None):
        if (ids is None):
                return []

        onts = {1:'GO', 2:'MP', 3:'MA', 5:'EDAM', 4:'MeSH', 
                        'GO':1, 'MP':2, 'MA':3, 'EDAM':5, 'MeSH':4}
        query = ("SELECT eo.ont_id, eo.ont_name FROM extsrc.ontology eo JOIN "
                         "extsrc.geneset_ontology ego ON eo.ont_id=ego.ont_id JOIN "
                         "production.geneset pg ON pg.gs_id=ego.gs_id WHERE (")

        # If the ontology type isn't found in the above dict...
        if (ont is not None) and (ont not in onts):
                ont = None
        # Check if the ontology type is a number, if not, convert (using dict)
        if (ont is not None) and (not isinstance(ont, int)):
                ont = onts[ont]

        # Add the list of gene IDs
        for i in range(len(ids)):
                if i != (len(ids) - 1):
                        query += 'pg.gs_id=%s OR '
                else:
                        query += 'pg.gs_id=%s)'


        # If we want to limit by ontology types
        if ont is not None:
                query += " AND eo.ontdb_id=%s;"
                ids.append(ont)
                g_cur.execute(query, ids)#[ids, ont])
        else:
                query += ";"
                g_cur.execute(query, [ids])

        return g_cur.fetchall()

def query_ontol_type(id):
        if (id is None) or (id == 0):
                return None

        query = ("SELECT eo.ontdb_id FROM extsrc.ontology eo WHERE eo.ont_id=%s")

        g_cur.execute(query, [id])

        return g_cur.fetchall()


#def updateMeshSet
## commitChanges
#
## Makes any changes to the database permanent. Needed after database 
## alterations (e.g. INSERT, DELETE, etc.).
#
def commitChanges():
        conn.commit()

if __name__ == '__main__':

    ## Simple tests
    ## Selections
    print get_species()
    print get_gene_ids(['Daxx', 'Mobp', 'Ccr4'])
    print get_gene_ids_by_species(['Daxx', 'Mobp', 'Ccr4'], 1)
    print get_gene_refs([83882, 85988])
    print get_gene_refs_by_type([83882, 85988], 7)
    print get_preferred_gene_refs([83882, 85988])
    print get_genesets_by_tier(tiers=[3], size=10)
    print get_geneset_values([720])
    #print findMeshSet('Thromboplastin')
    #print findMeshSet('Hypothalamus, Posterior')
    #print findMeshSet('Encephalitis')
    #updateMeshSet('Thromboplastin', 0)

    #gsid = createGeneset(2, 2, 1, 0.5, 0, 'Test MeSH Set Test', 'mesh set testing', 'mesh set testing')
    #createGenesetValue(gsid[0], 53023, 1.0, 'JAK3', 't')
    #print gsid
    #commitChanges()
    #print len(queryGenes((14921, 14923)))
    #print queryGenes((14921, 14923))
    #terms = queryJaccards(31361, [2,3])
    #print terms[0][0]
    #print queryGenesetSize(31361)

    #print len(set(terms))

