#!/usr/bin/env python2

## file:    db.py
## desc:    Contains all the important functions for accessing and querying the
##          GeneWeaver DB.
## auth:    TR
#

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
    Converts each cursor row into a list. Only the first tuple member is saved
    to the list.

    :type cursor: object
    :arg cursor: the psycopg cursor

    :ret list: the results of the SQL query
    """

    return map(lambda t: t[0], cursor.fetchall())

def tuplify(thing):
    """
    Converts a list or scalor value into a tuple.

    :type thing: something
    :arg thing: the thing being converted

    :ret list: tupled value
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

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  sp_name, sp_id
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
                    gs_attribution = %s AND
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
            WHERE       pub_pubmed = %s;
            ORDER BY    pub_id ASC
            ''',
                (pmid,)
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

    :ret list: mapping of gs_id to size (gs_count)
    """

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

def insert_publication(pub):
    """
    Inserts a new publication into the database. If a publication with the same
    PMID already exists, that pub_id is returned instead.

    :type pub: dict
    :arg pub: a object whose fields match all columns in the publication table

    :ret int: a GW publication ID (pub_id)
    """

    if 'pub_pubmed' not in pub:
        pub['pub_pubmed'] = None

    else:
        pub_id = get_publication(pub['pub_pubmed'])

        if pub_id != 0:
            return pub_id

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            INSERT INTO publication

                (pub_id, pub_authors, pub_title, pub_abstract, pub_journal,
                pub_volume, pub_pages, pub_month, pub_year, pub_pubmed)

            VALUES
                
                (%(pub_id)s, %(pub_authors)s, %(pub_title)s, %(pub_abstract)s, 
                %(pub_journal)s, %(pub_volume)s, %(pub_pages)s, %(pub_month)s, 
                %(pub_year)s, %(pub_pubmed))

            RETURNING pub_id;
            ''', 
                pub
        )

        return cursor.fetchone()[0]

def insert_file(size, contents, comments):
    """
    Inserts a new file into the database. 

    :type size: int
    :arg size: file size in bytes

    :type contents: str
    :arg contents: contents of the file which _MUST_ be in the format:
        gene\tvalue\n

    :type comments: str
    :arg comments: misc. comments about this file

    :ret int: a GW file ID (file_id)
    """

    if 'pub_pubmed' not in pub:
        pub['pub_pubmed'] = None

    else:
        pub_id = get_publication(pub['pub_pubmed'])

        if pub_id != 0:
            return pub_id

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

def commit():
    """
    Commit any DB changes. Must be called if the connection is not set to
    autocommit.
    """

    conn.commit()


    ## DELETES ##
    #############


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

