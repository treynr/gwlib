#!/usr/bin/env python2

## file:    db.py
## desc:    Contains all the important functions for accessing and querying the
##          GeneWeaver DB.
## auth:    TR
#

from collections import OrderedDict as od
from sys import maxint
import os
import psycopg2
import random

## Global connection variable
conn = None

        ## CLASSES ##
        #############

class PooledCursor(object):
    """
    Modeled after the PooledCursor object in GeneWeaver's source code.
    Encapsulates psycopg2 connections and cursors so they can be used with
    python features that are unsupported in older versions of psycopg2.
    """

    def __init__(self, new_conn=None):
        if not new_conn:
            global conn
        else:
            conn = new_conn

        self.connection = conn
        self.cursor = None

    def __enter__(self):
        self.cursor = self.connection.cursor()

        self.cursor.execute('SET search_path = extsrc,odestatic,production;')

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
## littering the postgres server with "idle in transaction" sessions. Commit()
## should be called after every statement (including SELECTs) but this is
## doesn't work well for dry runs and testing.
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

    #return s.encode('ascii', 'ignore') if isinstance(s, basestring) else s
    #return s.encode('utf-8', 'ignore') if isinstance(s, basestring) else s
    if isinstance(s, basestring):
        return s.decode('utf-8').encode('utf-8', 'ignore')

    return s

def dictify(cursor, ordered=False):
    """
    Converts each row returned by the cursor into a list of dicts, where  
    each key is a column name and each value is whatever is returned by the
    query.

    arguments
        cursor: an active psycopg cursor
        ordered: a boolean indicating whether to use a regular or ordered dict

    returns
        a list of dicts containing the results of the SQL query
    """

    dlist = []
    
    for row in cursor:
        ## Prevents unicode type errors from cropping up later. Convert to
        ## ascii, ignore any conversion errors.
        row = map(lambda s: asciify(s), row)
        d = od() if ordered else {}

        for i, col in enumerate(cursor.description):
            d[col[0]] = row[i]

        dlist.append(d)

    return dlist

def dictify_and_map(cursor):
    """
    Converts each row returned by the cursor into a dicts, then maps those
    dicts according to the first column.

    e.g. SELECT sp_name, sp_id, sp_taxid FROM species; would return a mapping
    of sp_name -> {sp_name, sp_id, sp_taxid}

    arguments
        cursor: an active psycopg cursor
        ordered: a boolean indicating whether to use a regular or ordered dict

    returns
        a list of dicts containing the results of the SQL query
    """

    d = {}
    
    for row in cursor:
        ## Prevents unicode type errors from cropping up later. Convert to
        ## ascii, ignore any conversion errors.
        row = map(lambda s: asciify(s), row)
        drow = {}

        for i, col in enumerate(cursor.description):
            drow[col[0]] = row[i]

        d[row[0]] = drow

    return d

def listify(cursor):
    """
    Converts each cursor row into a list. Only the first tuple member is saved
    to the list.

    :type cursor: object
    :arg cursor: the psycopg cursor

    :ret list: the results of the SQL query
    """

    return map(lambda t: t[0], cursor.fetchall())

def tuplify(thing):
    """
    Converts a list, list like object or scalar value into a tuple.

    :type thing: something
    :arg thing: the thing being converted

    :ret list: tupled value
    """

    if type(thing) == list or type(thing) == set:
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

def associate_duplicate(cursor):
    """
    Like associate(), creates a simple mapping from all the rows returned by
    the cursor. The first tuple member serves as the key and the rest are the 
    values. This function correctly handles duplicate entries.

    arguments
        cursor: a psycopg cursor object

    returns
        a dict mapping tuple elemnt #1 to the rest
    """

    d = {}

    for row in cursor:
        row = map(lambda s: asciify(s), row)

        ## 1:1
        if len(row) == 2:
            if row[0] in d:
                d[row[0]].append(row[1])
            else:
                d[row[0]] = [row[1]]

        ## 1:many
        else:
            if row[0] in d:
                d[row[0]].extend(list(row[1:]))
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

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_name, sp_id
            FROM    odestatic.species;
            '''
        )

        return associate(cursor)

def get_species2():
    """
    Returns a species name and column mapping for all the species currently 
    supported by GW.

    returns
        a dict mapping sp_name to the specified entries in the table
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_name, sp_id, sp_taxid
            FROM    odestatic.species;
            '''
        )

        return dictify_and_map(cursor)

def get_species_by_taxid():
    """
    Returns a mapping of species taxids (NCBI taxonomy ID) to their sp_id.

    :ret dict: mapping of sp_taxids to sp_ids
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_taxid, sp_id
            FROM    odestatic.species;
            '''
        )

        return associate(cursor)

def get_species_gene_id():
    """
    Returns a species name and the ID corresponding to which type of gene
    identifiers the species uses by default.

    returns
        a dict mapping sp_name to gdb_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_name, sp_ref_gdb_id
            FROM    odestatic.species;
            '''
        )

        return associate(cursor)

def get_attributions():
    """
    Returns all the attributions (at_id and at_abbrev) found in the DB.

    :ret dict: mapping of abbreviations to IDs
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  at_abbrev, at_id
            FROM    odestatic.attribution;
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

def get_gene_ids_by_spid_type(sp_id, gdb_id):
    """
    Exactly like get_gene_ids() above but filters results by species and
    gene type.

    :type refs: list/tuple
    :arg refs: external DB reference IDs

    :type sp_id: int
    :arg sp_id: GW species ID

    :ret dict: mapping of ode_ref_ids to ode_gene_ids
    """

    ## There is an issue with gene symbols. Some species have duplicate symbols
    ## with different ode_gene_ids. One is the correct entry, the other is an
    ## entry for another gene with the same symbol (incorrectly attributed).
    ## One e.g. is the mouse Ccr4 gene. So, when symbols are searched for the
    ## ode_pref tag MUST be used.
    gene_types = get_short_gene_types()

    if gene_types['symbol'] == gdb_id:
        use_pref = True

    else:
        use_pref = False

    with PooledCursor() as cursor:

        if use_pref:
            cursor.execute(
                '''
                SELECT  lower(ode_ref_id), ode_gene_id
                FROM    extsrc.gene
                WHERE   sp_id = %s AND 
                        gdb_id = %s AND
                        ode_pref = 't';
                ''', 
                    (sp_id, gdb_id)
            )

        else:
            cursor.execute(
                '''
                SELECT  lower(ode_ref_id), ode_gene_id
                FROM    extsrc.gene
                WHERE   sp_id = %s;
                ''', 
                    (sp_id,)
            )

        return associate(cursor)

def get_species_genes(sp_id):
    """
    Similar to the above get_gene_ids() but an ode_ref_id -> ode_gene_id
    mapping for all genes for the given species.

    arguments
        sp_id: species ID

    returns
        a dict mapping of ode_ref_id -> ode_gene_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_ref_id, ode_gene_id
            FROM    extsrc.gene
            WHERE   sp_id = %s;
            ''', 
                (sp_id,)
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

def get_genesets(gs_ids):
    """
    Returns a list of genesets for the given list of gs_ids.

    arguments
        gs_ids: a list of gs_ids (long)

    returns
        a list of geneset objects that contain all columns in the geneset table
    """

    if type(gs_ids) == list:
        gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  *
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''', 
                (gs_ids,)
        )

        return dictify(cursor, ordered=True)

def get_genesets_by_tier(tiers=[1,2,3,4,5], size=maxint):
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

def get_genesets_by_attribute(at_id, size=maxint):
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
                    gs_attribution = %s AND
                    gs_count < %s;
            ''', 
                (at_id, size)
        )

        return listify(cursor)

def get_genesets_by_attribute_species(at_id, sp_id):
    """
    Returns a list of normal (i.e. their status is not deleted or deprecated) 
    geneset IDs that belong to a particular attribution group and species.

    arguments
        at_id: GeneWeaver attribution ID
        sp_id: GeneWeaver species ID

    returns
        a list of gene set IDs matching the given criteria
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    gs_attribution = %s AND
                    sp_id = %s;
            ''', 
                (at_id, sp_id)
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

def get_homolog_species(hom_ids):
    """
    Returns all the species associated with a given hom_id.

    arguments
        hom_ids: a list of hom_ids

    returns
        a mapping of ode_gene_ids to homology IDs (hom_id)
    """

    if type(hom_ids) == list:
        hom_ids = tuple(hom_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  hom_id, sp_id
            FROM    extsrc.homology
            WHERE   hom_id IN %s;
            ''', 
                (hom_ids,)
        )

        return associate_duplicate(cursor)

def get_publication(pmid):
    """
    Returns the GW publication ID associated with the gived PubMed ID.

    :type pmid: int
    :arg pmid: a PubMed ID

    :ret int: a GW pub_id, or 0 if one doesn't exist
    """

    with PooledCursor() as cursor:

        ## Ordered in case there exists more than one for the same publication.
        ## The lowest pub_id should be used and the others eventually deleted.
        cursor.execute(
            '''
            SELECT      pub_id
            FROM        production.publication
            WHERE       pub_pubmed = %s
            ORDER BY    pub_id ASC;
            ''',
                (pmid,)
        )

        result = cursor.fetchone()

        if result:
            return result[0]

        else:
            return 0

def get_publication_mapping():
    """
    Returns a mapping of PMID -> pub_id for all publications in the DB.

    returns
        a dict mapping PMIDs -> pub_ids
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT DISTINCT ON  (pub_pubmed) pub_pubmed, pub_id
            FROM                production.publication
            ORDER BY            pub_pubmed, pub_id;
            '''
        )

        return associate(cursor)

def get_publication_pmid(pub_id):
    """
    Returns the PMID associated with a GW publication ID.

    arguments:
        pub_id: int publication ID

    returns:
        a string representing the article's PMID
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT      pub_pubmed
            FROM        production.publication
            WHERE       pub_id = %s;
            ''',
                (pub_id,)
        )

        result = cursor.fetchone()

        if result:
            return result[0]

        else:
            return 0

def get_geneset_metadata(gs_ids):
    """
    Returns names, descriptions, and abbreviations for each geneset in the
    provided list.

    :type gs_ids: list
    :arg gs_ids: geneset IDs

    :ret list: dicts with column names as keys
    """
    if type(gs_ids) == list:
        gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, gs_name, gs_description, gs_abbreviation
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''',
                (gs_ids,)
        )

        return dictify(cursor)

def get_geneset_size(gs_ids):
    """
    Returns geneset sizes for the given genesets.

    :type gs_ids: list
    :arg gs_ids: geneset IDs

    :ret dict: mapping of gs_id to size (gs_count)
    """
    if type(gs_ids) == list:
        gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, gs_count
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''',
                (gs_ids,)
        )

        return associate(cursor)

def get_geneset_species(gs_ids):
    """
    Returns geneset species IDs for the given genesets.

    arguments
        gs_ids: list of gs_ids to get species data for

    returns
        a dict mapping gs_id -> sp_id
    """

    if type(gs_ids) == list:
        gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, sp_id
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''',
                (gs_ids,)
        )

        return associate(cursor)

def get_gene_types():
    """
    Returns a mapping of gene type names to their IDs.

    :ret dict: mapping of gdb_name -> gdb_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gdb_name, gdb_id
            FROM    odestatic.genedb;
            '''
        )

        return associate(cursor)

def get_short_gene_types():
    """
    Returns a mapping of gene type short names to their IDs.

    :ret dict: mapping of gdb_shortname -> gdb_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gdb_shortname, gdb_id
            FROM    odestatic.genedb;
            '''
        )

        return associate(cursor)

def get_platforms():
    """
    Returns the list of supported microarray platforms as objects containing
    any and all information associated with a particular platform.

    returns
        a list of objects whose keys match the platform table
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  pf_id, pf_name, pf_shortname, pf_gpl_id
            FROM    odestatic.platform;
            '''
        )

        return dictify(cursor)

def get_platform_names():
    """
    Returns the list of supported microarray platforms as a mapping of platform
    names -> IDs.

    :ret dict: mapping of pf_name -> pf_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  pf_name, pf_id
            FROM    odestatic.platform;
            '''
        )

        return associate(cursor)

def get_platform_probes(pf_id, refs):
    """
    Returns a mapping of probe names (prb_ref_ids from a particular microarray
    platform) to their IDs.

    :type pf_id: int
    :arg pf_id: platform ID

    :type refs: list
    :arg refs: list of probe references/names

    :ret dict: mapping of pf_id -> prb_ref_id
    """

    if type(refs) == list:
        refs = tuple(refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  prb_ref_id, prb_id
            FROM    odestatic.probe
            WHERE   pf_id = %s AND
                    prb_ref_id IN %s;
            ''',
                (pf_id, refs)
        )

        return associate(cursor)

def get_all_platform_probes(pf_id):
    """
    Returns a mapping of all current probe names (prb_ref_ids) to their IDs for
    a particular platform.

    arguments
        pf_id: platform ID

    returns
        a dict maping of prb_id -> prb_ref_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  prb_ref_id, prb_id
            FROM    odestatic.probe
            WHERE   pf_id = %s;
            ''',
                (pf_id,)
        )

        return associate(cursor)

def get_probe2gene(prb_ids):
    """
    Returns a mapping of prb_ids -> ode_gene_ids for the given set of prb_ids.

    arguments
        prb_ids: a list of probe IDs

    returns
        a dict mapping of prb_id -> prb_ref_id
    """

    if type(prb_ids) == list:
        prb_ids = tuple(prb_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  prb_id, ode_gene_id
            FROM    extsrc.probe2gene
            WHERE   prb_id in %s;
            ''',
                (prb_ids,)
        )

        #return associate(cursor)
        return associate_duplicate(cursor)

def get_group_by_name(name):
    """
    Returns the grp_id for the given grp_name.

    arguments
        name: the name of group

    returns
        a grp_id (int)
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  grp_id
            FROM    production.grp
            WHERE   grp_name = %s
            ''',
                (name,)
        )

        result = listify(cursor)

        if not result:  
            return None

        return result[0]

def get_projects():
    """
    Returns all projects in the DB.

    returns
        a list of dicts representing projects
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  pj_id, pj_name, pj_groups
            FROM    production.project;
            '''
        )

        return dictify(cursor)

def get_genesets_by_project(pj_ids):
    """
    Returns all genesets associated with the given project IDs.

    returns
        a mapping of pj_id -> gs_ids
    """

    if type(pj_ids) == list:
        pj_ids = tuple(pj_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  pj_id, gs_id
            FROM    production.project2geneset
            WHERE   pj_id IN %s;
            ''',
                (pj_ids,)
        )

        return associate_duplicate(cursor)

def get_geneset_annotations(gs_ids):
    """
    Returns the set of ontology annotations for each given gs_id.

    arguments
        gs_ids: list of gs_ids to retrieve annotations for

    returns
        a dict mapping gs_ids to a list of tuples containing the ont_id and
        ont_ref_id.
            e.g. {123456: (7890, 'GO:1234567')}
    """

    gs_ids = tuplify(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT      go.gs_id, go.ont_id, o.ont_ref_id
            FROM        extsrc.geneset_ontology AS go
            INNER JOIN  extsrc.ontology AS o
            ON          go.ont_id = o.ont_id
            WHERE       gs_id IN %s;
            ''',
                (gs_ids,)
        )

        gs2ann = {}

        for row in cursor:
            gs_id = row[0]

            if gs_id in gs2ann:
                gs2ann[gs_id].append(tuple(row[1:]))
            else:
                gs2ann[gs_id] = [tuple(row[1:])]

        return gs2ann

def get_annotation_by_refs(ont_refs):
    """
    Maps ontology reference IDs (e.g. GO:0123456, MP:0123456) to the internal
    ontology IDs used by GW.

    returns
        a mapping of ont_ref_id -> ont_id
    """

    if type(ont_refs) == list:
        ont_refs = tuple(ont_refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ont_ref_id, ont_id
            FROM    extsrc.ontology
            WHERE   ont_ref_id IN %s
            ''',
                (ont_refs,)
        )

        return associate(cursor)

def get_annotation_by_ref(ont_ref):
    """
    Returns the internal ontology ID used by GW for a single ontology reference
    ID.

    returns
        an ont_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ont_id
            FROM    extsrc.ontology
            WHERE   ont_ref_id = %s
            ''',
                (ont_ref,)
        )

        result = cursor.fetchone()

        if not result:
            return None

        return result[0]

def get_ontologies():
    """
    Returns the list of ontologies supported by GeneWeaver for use with gene 
    set annotations.

    returns
        a list of dicts whose fields match the ontologydb table. Each entry in
        the list is a row in the table.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ontdb_id, ontdb_name, ontdb_prefix, ontdb_date
            FROM    odestatic.ontologydb;
            '''
        )

        return dictify(cursor)

def get_ontdb_id(name):
    """
    Retrieves the ontologydb ID for the given ontology name.

    args
        name: ontology name
        
    returns
        an int ID for the corresponding ontologydb entry. None is returned if
        the ontology name is not found in the database.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ontdb_id
            FROM    odestatic.ontologydb
            WHERE   LOWER(ontdb_name) = LOWER(%s);
            ''',
                (name,)
        )

        if not cursor.rowcount:
            return None

        return cursor.fetchone()[0]

def get_ontology_terms_by_ontdb(ontdb_id):
    """
    Retrieves all ontology terms for the given ontdb_id.

    args
        ontdb_id: the ontologydb ID

    returns
        a list of dicts whose fields match the columns in the ontology table.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  *
            FROM    extsrc.ontology
            WHERE   ontdb_id = %s;
            ''',
                (ontdb_id,)
        )

        return dictify(cursor)


def get_user_map():
    """
    returns
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  usr_id, usr_email
            FROM    usr;
            '''
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
    if ('cur_id' not in gs) or\
       ('gs_description' not in gs) or\
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

    if 'gs_attribution' not in gs:
        gs['gs_attribution'] = None

    if 'gs_uri' not in gs:
        gs['gs_uri'] = None

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO geneset

                (usr_id, file_id, gs_name, gs_abbreviation, pub_id, cur_id,
                gs_description, sp_id, gs_count, gs_threshold_type,
                gs_threshold, gs_groups, gs_gene_id_type, gs_created,
                gs_attribution, gs_uri)

            VALUES
                
                (%(usr_id)s, %(file_id)s, %(gs_name)s, %(gs_abbreviation)s, 
                %(pub_id)s, %(cur_id)s, %(gs_description)s, %(sp_id)s, 
                %(gs_count)s, %(gs_threshold_type)s, %(gs_threshold)s, 
                %(gs_groups)s, %(gs_gene_id_type)s, %(gs_created)s, 
                %(gs_attribution)s, %(gs_uri)s)
            
            RETURNING gs_id;
            ''', 
                gs
        )

        return cursor.fetchone()[0]

def insert_geneset_value(gs_id, gene_id, value, name, threshold):
    """
    Inserts a new geneset_value into the database. 

    arguments
        gs_id:      gene set ID
        gene_id:    ode_gene_id
        value:      value associated with this gene
        name:       a gene name or symbol (typically an ode_ref_id)
        threshold:  a threshold value for the gene set 

    returns
        the gs_id associated with this gene set value
    """

    threshold = 't' if value <= threshold else 'f'

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO geneset_value

                (gs_id, ode_gene_id, gsv_value, gsv_source_list,
                gsv_value_list, gsv_in_threshold, gsv_hits, gsv_date)

            VALUES
                
                (%s, %s, %s, %s, %s, %s, 0, NOW())

            RETURNING gs_id;
            ''', 
                (gs_id, gene_id, value, [name], [float(value)], threshold)
        )

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

def insert_publication(pub):
    """
    Inserts a new publication into the database.

    arguments
        pub: a dict with fields matching the columns in the publication table

    returns
        a pub_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO publication

                (pub_authors, pub_title, pub_abstract, pub_journal,
                pub_volume, pub_pages, pub_month, pub_year, pub_pubmed)

            VALUES
                
                (%(pub_authors)s, %(pub_title)s, %(pub_abstract)s, 
                %(pub_journal)s, %(pub_volume)s, %(pub_pages)s, %(pub_month)s, 
                %(pub_year)s, %(pub_pubmed)s)

            RETURNING pub_id;
            ''', 
                pub
        )

        return cursor.fetchone()[0]

def insert_file(size, contents, comments):
    """
    Inserts a new file into the database. 

    arguments
        size:       size of the file in bytes
        contents:   file contents which MUST be in the format:
                        gene\tvalue\n
        comments:   misc. comments about this file

    returns
        a file_id
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO file

                (file_size, file_contents, file_comments, file_created)

            VALUES
                
                (%s, %s, %s, NOW())

            RETURNING file_id;
            ''', 
                (size, contents, comments)
        )

        return cursor.fetchone()[0]

def insert_platform(platform):
    """
    Inserts a new platform into the database using the given platform object.
    Some fields are skipped because I have no fucking clue what they're used
    for. As soon as I figure it out, they'll get added.

    arguments
        platform: a dict whose keys should match those of the platform table

    returns
        an int representing the newly inserted platform ID
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO platform
                (pf_gpl_id, pf_shortname, pf_name, sp_id, pf_date)
            VALUES
                (%(pf_gpl_id)s, %(pf_shortname)s, %(pf_name)s, %(sp_id)s, NOW())
            RETURNING pf_id;
            ''', 
                platform
        )

        return cursor.fetchone()[0]

def insert_probe(prb_ref, pf_id):
    """
    Inserts a new probe reference ID for the given platform ID.

    arguments
        prb_ref: a string probe reference ID
        pf_id: a platform ID

    returns
        an unique int for the given (prb_ref_id, pf_id) combo
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO probe
                (prb_ref_id, pf_id)
            VALUES
                (%s, %s)
            RETURNING prb_id;
            ''', 
                (prb_ref, pf_id)
        )

        return cursor.fetchone()[0]

def insert_probe2gene(prb_id, ode_id):
    """
    Inserts a new prb_id, ode_gene_id combination into the database.

    arguments
        prb_id: a probe ID
        ode_id: an ODE gene ID

    returns
        the prb_id of the newly inserted probe/gene ID combo
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO probe2gene
                (prb_id, ode_gene_id)
            VALUES
                (%s, %s)
            RETURNING prb_id;
            ''', 
                (prb_id, ode_id)
        )

        return cursor.fetchone()[0]

def insert_jaccard(lid, rid, jac):
    """
    Inserts an entry into the jaccard table.

    arguments
        lid: left gs_id
        rid: right gs_id
        jac: jaccard value
    """

    ## This is a constraint in production
    if lid >= rid:
        lid, rid = rid, lid

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.geneset_jaccard
                (gs_id_left, gs_id_right, jac_value)
            VALUES
                (%s, %s, %s);
            ''', 
                (lid, rid, jac)
        )

        return cursor.rowcount

def insert_ontologydb_entry(name, prefix):
    """
    Inserts a new ontology into the ontologydb table.

    arguments
        name:   the ontology name
        prefix: the ontology ID prefix (e.g. GO, MP)

    returns
        the ontdb_id of the newly inserted ontology
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO odestatic.ontologydb
                (ontdb_name, ontdb_prefix, ontdb_date)
            VALUES
                (%s, %s, NOW())
            RETURNING ontdb_id;
            ''', 
                (name, prefix)
        )

        return cursor.fetchone()[0]

def insert_ontology(ref_id, name, desc, children, parents, ontdb_id):
    """
    Inserts a new ontology term into the ontology table.

    arguments
        ref_id:     the ontology ID for this term
        name:       the ontology term
        desc:       description of the term
        children:   number of children this term has
        parents:    number of parents this term has

    returns
        the ont_id of the newly inserted term
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.ontology (
                ont_ref_id, ont_name, ont_description, ont_children, 
                ont_parents, ontdb_id

            ) VALUES (
                %s, %s, %s, %s, %s, %s

            ) RETURNING ont_id;
            ''', 
                (ref_id, name, desc, children, parents, ontdb_id)
        )

        return cursor.fetchone()[0]

def insert_ontology_relation(left, right, relation):
    """
    Inserts a new relationship into the ontology_relation table.
    The left ID should always be the more granular term, i.e. the left term is
    the child of the right term.

    arguments
        left:       the ontology ID for the child term
        right:      the ontology ID for the parent term
        relation:   the type of relationship (e.g. is_a, part_of, regulates)
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.ontology_relation
                (left_ont_id, right_ont_id, or_type)
            VALUES
                (%s, %s, %s);
            ''', 
                (left, right, relation)
        )

def insert_geneset_ontology(gs_id, ont_id, ref_type):
    """
    Annotates a gene set with an ontology term.

    arguments
        gs_id:      gs_id to annotate
        ont_id:     ont_id of the ontology term
        ref_type:   the type of annotation (string that varies in value)
                    e.g. "GeneWeaver Primary Manual" or "GW Primary Inferred"
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.geneset_ontology
                (gs_id, ont_id, gso_ref_type)
            VALUES
                (%s, %s, %s);
            ''', 
                (gs_id, ont_id, ref_type)
        )

        ## UPDATES ##
        #############

def update_geneset_status(gs_id, status):
    """
    Update the status of a geneset. The only statuses currently used are
    'normal', 'deleted', and 'deprecated'.

    :ret int: the number of rows affected by the update
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE  production.geneset
            SET     gs_status = %s
            WHERE   gs_id = %s;
            ''', 
                (status, gs_id)
        )

        return cursor.rowcount

def update_geneset_date(gsid):
    """
    Resets the "last updated" date for a gene set to now.

    arguments
        gsid: GS ID for the gene set being changed

    returns
        the number of rows affected by the update
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE  production.geneset
            SET     gs_updated = NOW()
            WHERE   gs_id = %s;
            ''', 
                (gsid,)
        )

        return cursor.rowcount

def update_geneset_dates(gsids):
    """
    Resets the "last updated" date for many gene sets to now.

    arguments
        gsids: list or tuple of gsids to update

    returns
        the number of rows affected by the update
    """
    if type(gsids) == list:
        gsids = tuple(gsids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE  production.geneset
            SET     gs_updated = NOW()
            WHERE   gs_id IN %s;
            ''', 
                (gsids,)
        )

        return cursor.rowcount

def update_geneset_count(gs_id, gs_count):
    """
    Update the size of a geneset.

    :ret int: the number of rows affected by the update
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE  production.geneset
            SET     gs_count = %s
            WHERE   gs_id = %s;
            ''', 
                (gs_count, gs_id)
        )

        return cursor.rowcount

def update_ontology_term_by_ref(ref_id, name, description, children, parents):
    """
    Updates an ontology term using its reference ID.

    args
        ref_id:         the ontology term reference ID
        name:           the name of the ontology term
        description:    a description of the term
        children:       the number of immediate child terms this term has
        parents:        the number of immediate parent terms this term has
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE      extsrc.ontology
            SET         ont_name = %s,
                        ont_description = %s,
                        ont_children = %s,
                        ont_parents = %s
            WHERE       ont_ref_id = %s
            RETURNING   ont_id;
            ''',
                (name, description, children, parents, ref_id)
        )

        if cursor.rowcount == 0:
            return None
        
        return cursor.fetchone()[0]

def commit():
    """
    Commit any DB changes. Must be called if the connection is not set to
    autocommit.
    """

    conn.commit()


    ## DELETES ##
    #############

def delete_jaccard(lid, rid):
    """
    Deletes an entry from the jaccard table.

    arguments
        lid: left gs_id
        rid: right gs_id
    """

    if lid >= rid:
        lid, rid = rid, lid

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            DELETE
            FROM   extsrc.geneset_jaccard
            WHERE  gs_id_left = %s AND
                   gs_id_right = %s;
            ''', 
                (lid, rid)
        )

        return cursor.rowcount

def delete_ontology_relations(ont_ids):
    """
    Deletes all ontology relations for the given set of ont_ids.

    args
        ont_ids: list of ont_ids 

    returns
        the number of rows deleted
    """

    ont_ids = tuplify(ont_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            DELETE
            FROM   extsrc.ontology_relation
            WHERE  left_ont_id IN %s OR
                   right_ont_id IN %s;
            ''', 
                (ont_ids, ont_ids)
        )

        return cursor.rowcount

    ## VARIANT SCHEMA ADDITIONS ##
    ##############################

def get_genome_builds():
    """
    Retrieves the list of genome builds supported by GW.

    returns
        a list of objects representing rows from the genome_build table
    """

    with PooledCursor() as cursor:

        cursor.execute('''SELECT * FROM odestatic.genome_build;''')

        return dictify(cursor)

def get_variant_type_by_effect(effect):
    """
    Retrieves the list of genome builds supported by GW.

    returns
        a list of objects representing rows from the genome_build table
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT * 
            FROM   odestatic.variant_type
            WHERE  vt_effect = %s;
            ''',
                (effect,))

        return dictify(cursor)

def insert_variant(var):
    """
    Inserts a new variant into the database. This function does not check to
    see if the insertion would violate any DB consistency checks.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.variant

                (var_ref_id, var_allele, var_chromosome, var_position, vt_id,
                var_ref_cur, var_obs_alleles, var_ma, var_maf, var_clinsig,
                gb_id)

            VALUES

                (%(var_ref_id)s, %(var_allele)s, %(var_chromosome)s, 
                %(var_position)s, %(vt_id)s, %(var_ref_cur)s, 
                %(var_obs_alleles)s, %(var_ma)s, %(var_maf)s, %(var_clinsig)s,
                %(gb_id)s)
            
            RETURNING var_id;
            ''', 
                var
        )

        return cursor.fetchone()[0]

def insert_variant_with_id(var):
    """
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO extsrc.variant

                (var_id, var_ref_id, var_allele, var_chromosome, var_position,
                vt_id, var_ref_cur, var_obs_alleles, var_ma, var_maf,
                var_clinsig, gb_id)

            VALUES

                (%(var_id)s, %(var_ref_id)s, %(var_allele)s, %(var_chromosome)s, 
                %(var_position)s, %(vt_id)s, %(var_ref_cur)s, 
                %(var_obs_alleles)s, %(var_ma)s, %(var_maf)s, %(var_clinsig)s,
                %(gb_id)s)
            
            RETURNING var_id;
            ''', 
                var
        )

        return cursor.fetchone()[0]

if __name__ == '__main__':

    ## Simple tests
    ## Selections
    print get_species()
    print get_attributions()
    print get_gene_ids(['Daxx', 'Mobp', 'Ccr4'])
    print get_gene_ids_by_species(['Daxx', 'Mobp', 'Ccr4'], 1)
    print get_gene_refs([83882, 85988])
    print get_gene_refs_by_type([83882, 85988], 7)
    print get_preferred_gene_refs([83882, 85988])
    print get_genesets_by_tier(tiers=[3], size=10)
    print get_genesets_by_attribute(11, size=2)
    print get_geneset_values([720])
    print get_gene_homologs([135283, 135642])

