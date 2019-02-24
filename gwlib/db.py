#!/usr/bin/env python2

## file: db.py
## desc: Contains all the important functions for accessing and querying the
##       GeneWeaver DB.
## auth: TR
#

from collections import OrderedDict as od
from psycopg2.extras import execute_values
import psycopg2

## Global connection variable
conn = None

class PooledCursor(object):
    """
    Small class that encapsulates psycopg2's connection and cursor objects.
    On instantiation the class will create a new DB connection if one doesn't exist and
    creates a new cursor when entered (e.g. using in a with statement).
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

        self.cursor.execute('SET search_path = curation,extsrc,odestatic,production;')

        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.cursor:
            self.cursor.close()

            self.cursor = None

    ## UTILITY ##
    #############

def connect(host, db, user, password, port=5432):
    """
    Connect to a database using the given credentials.

    arguments
        host:     DB host/server
        db:       DB name
        user:     user name
        password: password
        port:     optional port the DB server is using

    returns
        a tuple indicating success. The first element is a boolean which indicates
        whether the connection was successful or not. In the case of an unsuccessful
        connection, the second element contains the error or exception.
    """

    global conn

    try:
        conn = psycopg2.connect(
            host=host, dbname=db, user=user, password=password, port=port
        )

    except Exception as e:

        return (False, e)

    return (True, '')

def dictify(cursor, ordered=False):
    """
    Converts each row returned by the cursor into a list of dicts, where
    each key is a column name and each value is whatever is returned by the
    query.

    arguments
        cursor:  an active psycopg cursor
        ordered: a boolean indicating whether to use a regular or ordered dict

    returns
        a list of dicts containing the results of the SQL query
    """

    dlist = []

    for row in cursor:

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
        cursor:  an active psycopg cursor
        ordered: a boolean indicating whether to use a regular or ordered dict

    returns
        a list of dicts containing the results of the SQL query
    """

    d = {}

    for row in cursor:

        drow = {}

        for i, col in enumerate(cursor.description):
            drow[col[0]] = row[i]

        d[row[0]] = drow

    return d

def listify(cursor):
    """
    Converts each cursor row into a list. Only the first tuple member is saved
    to the list.

    arguments
        cursor: an active psycopg cursor

    returns
        a list containing the query results
    """

    return [t[0] for t in cursor.fetchall()]

def tuplify(thing):
    """
    Converts a list, list like object or scalar value into a tuple.

    arguments
        thing: some object being converted into a tuple

    returns
        a tuple
    """

    if hasattr(thing, '__iter__'):
        return tuple(thing)

    return (thing,)

def associate(cursor):
    """
    Creates a simple mapping from all the rows returned by the cursor. The
    first tuple member serves as the key and the rest are the values. Unlike
    dictify, this does not use column names and generates a single dict from
    all returned rows. Be careful since duplicates are overwritten.

    arguments
        cursor: an active psycopg cursor

    returns
        mapping of tuple element #1 to the rest
    """

    d = {}

    for row in cursor:

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
                d[row[0]] = [list(row[1:])]

    return d

def commit():
    """
    Commits the transaction.
    """

    conn.commit()

    ## SELECTIONS ##
    ################

def get_species(lower=False):
    """
    Returns a species name and ID mapping for all the species currently
    supported by GW.

    arguments
        lower: if true, returns lowercased species names
    returns
        a mapping of sp_names to sp_ids
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  CASE WHEN %s THEN LOWER(sp_name) ELSE sp_name END, sp_id
            FROM    odestatic.species;
            ''', (lower,)
        )

        return associate(cursor)

def get_species_with_taxid():
    """
    Returns a species name and column mapping for all the species currently
    supported by GW.

    returns
        a dict mapping sp_name to the specified entries in the table
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT sp_name, sp_id, sp_taxid
            FROM   odestatic.species;
            '''
        )

        return dictify(cursor)

def get_species_by_taxid():
    """
    Returns a mapping of species taxids (NCBI taxonomy ID) to their sp_id.

    returns
        a mapping of sp_taxids to sp_ids
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT sp_taxid, sp_id
            FROM   odestatic.species;
            '''
        )

        return associate(cursor)

## Might delete this
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
            SELECT sp_name, sp_ref_gdb_id
            FROM   odestatic.species;
            '''
        )

        return associate(cursor)

def get_attributions():
    """
    Returns all the attributions (at_id and at_abbrev) found in the DB.
    These represent third party data resources integrated into GeneWeaver.

    returns
        mapping of attribution abbreviations to IDs
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT at_abbrev, at_id
            FROM   odestatic.attribution;
            '''
        )

        return associate(cursor)

def get_gene_ids(refs, sp_id=None, gdb_id=None):
    """
    Given a set of external reference IDs, this returns a mapping of
    reference gene identifiers to the IDs used internally by GeneWeaver (ode_gene_id).
    An optional species id can be provided to limit gene results by species.
    An optional gene identifier type can be provided to limit mapping by ID type (useful
    when identifiers from different resources overlap).

    Reference IDs are always strings (even if they're numeric) and should be
    properly capitalized. If duplicate references exist in the DB (unlikely)
    then they are overwritten in the return dict. Reference IDs can be any valid
    identifier supported by GeneWeaver (e.g. Ensembl, NCBI Gene, MGI, HGNC, etc.).

    arguments
        refs:   a list of reference identifiers to convert
        sp_id:  an optional species identifier used to limit the ID mapping process
        gdb_id: an optional gene type identifier used to limit the ID mapping process

    returns
        a bijection of reference identifiers to GW IDs
    """

    refs = tuplify(refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            WITH symbol_type AS (
                SELECT gdb_id
                FROM   odestatic.genedb
                WHERE  gdb_name = 'Gene Symbol'
                LIMIT  1
            ), variant_type AS (
                SELECT COALESCE(
                    (SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant'),
                    0
                ) LIMIT 1
            )
            SELECT  ode_ref_id, ode_gene_id
            FROM    extsrc.gene
            WHERE   ode_ref_id IN %(refs)s AND
                    CASE
                        WHEN %(spid)s IS NOT NULL AND %(gdbid)s IS NOT NULL
                        THEN sp_id = %(spid)s AND gdb_id = %(gdbid)s

                        WHEN %(spid)s IS NOT NULL
                        THEN sp_id = %(spid)s

                        WHEN %(gdbid)s IS NOT NULL
                        THEN gdb_id = %(gdbid)s

                        ELSE true
                    END AND
                    CASE
                        --
                        -- We have to use ode_pref when gene symbol types are
                        -- specified. Some species have genes with duplicate
                        -- symbols (synonyms) that are no longer used but still
                        -- exist. w/out ode_pref, we retrieve incorrect genes.
                        -- For an e.g. see the mouse Ccr4 gene.
                        --
                        WHEN %(gdbid)s = (SELECT * FROM symbol_type)
                        THEN ode_pref = true

                        ELSE true
                    END AND
                    --
                    -- We don't want to match gene IDs that are representing variants
                    --
                    gdb_id <> (SELECT * FROM variant_type);
            ''', {'refs': refs, 'spid': sp_id, 'gdbid': gdb_id}
        )

        return associate(cursor)

def get_species_genes(sp_id, gdb_id=None, symbol=True):
    """
    Similar to the above get_gene_ids() but returns a reference to GW ID mapping for all
    genes for the given species (as a warning, this will be a lot of data).
    This query does not include genomic variants.
    If a gdb_id is provided then this will return all genes covered by the given gene
    type.
    If symbol is true, then the function returns genes covered by the symbol gene type to
    limit the amount of data returned.
    gdb_id will always override the symbol argument.

    arguments
        sp_id:  species identifier
        gdb_id: an optional gene type identifier used to limit the ID mapping process
        symbol: if true limits results to genes covered by the symbol gene type

    returns
        an N:1 mapping of reference identifiers to GW IDs
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_ref_id, ode_gene_id
            FROM    extsrc.gene
            WHERE   sp_id = %(sp_id)s AND
                    gdb_id NOT IN (
                        SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant'
                    ) AND
                    CASE
                        WHEN %(gdb_id)s IS NOT NULL
                        THEN gdb_id = %(gdb_id)s

                        WHEN %(symbol)s = TRUE
                        THEN gdb_id = (
                            SELECT gdb_id
                            FROM   odestatic.genedb
                            WHERE  gdb_name = 'Gene Symbol'
                        ) AND ode_pref = TRUE

                        ELSE TRUE
                    END;
            ''', {'sp_id': sp_id, 'gdb_id': gdb_id, 'symbol': symbol}
        )

        return associate(cursor)

def get_gene_refs(genes, type_id=None):
    """
    The inverse of the get_gene_refs() function. For the given list of internal GW gene
    identifiers, this function returns a mapping of internal to external
    (e.g. MGI, HGNC, Ensembl) reference identifiers.
    The mapping is 1:N since many external references may exist for a single, condensed
    GW identifier.

    arguments
        genes:   a list of internal GW gene identifiers (ode_gene_id)
        type_id: an optional gene type ID to limit the mapping to a specific gene type

    returns
        a 1:N mapping of GW IDs to reference identifiers
    """

    genes = tuplify(genes)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  DISTINCT ON (ode_gene_id, ode_ref_id) ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   ode_gene_id IN %(genes)s AND
                    CASE
                        WHEN %(type_id)s IS NOT NULL THEN gdb_id = %(type_id)s
                        ELSE true
                    END;
            ''', {'genes': genes, 'type_id': type_id}
        )

        return associate_duplicate(cursor)

def get_preferred_gene_refs(genes):
    """
    Exactly like get_gene_refs() but only retrieves preferred ode_ref_ids.
    There _should_ only be one preferred ID and it _should_ always be the gene symbol
    type.

    arguments
        genes: a list of internal GW gene identifiers (ode_gene_id)

    returns
        a bijection of GW IDs to reference identifiers
    """

    genes = tuplify(genes)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   ode_pref = TRUE AND
                    ode_gene_id IN %s;
            ''', (genes,)
        )

        return associate(cursor)

## Reminder to delete this function. Don't remember writing it but don't wanna remove it
## just yet in case doing so breaks something.
def get_preferred_gene_refs_from_homology(hom_ids, sp_id=2):
    """
    Returns a mapping of hom_id -> gene symbol using the given list of hom_ids and
    species. Defaults to a species ID of 2 which is usually human on almost all GW
    instances.

    arguments
        hom_ids: an iterator type containing the list of homology IDs
        sp_id    the species ID

    returns
        a dict mapping hom_id -> gene symbol
    """

    hom_ids = tuple(list(hom_ids))

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT     hom_id, ode_ref_id
            FROM       extsrc.homology h
            INNER JOIN extsrc.gene g
            USING      (ode_gene_id)
            WHERE      h.hom_id IN %s AND
                       g.sp_id = %s AND
                       g.ode_pref;
            ''', (hom_ids, sp_id)
        )

        return associate(cursor)

def get_genesets(gs_ids):
    """
    Returns a list of gene set metadata for the given list of gene set IDs.

    arguments
        gs_ids: a list of gs_ids

    returns
        a list of geneset objects that contain all columns in the geneset table
    """

    gs_ids = tuplify(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  *
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''', (gs_ids,)
        )

        return dictify(cursor, ordered=True)

def get_geneset_ids_by_tier(tiers=[1, 2, 3, 4, 5], size=0, sp_id=0):
    """
    Returns a list of normal (i.e. their status is not deleted or deprecated)
    gene set IDs that belong in a particular tier or set of tiers. Allows filtering of
    returned sets based on size and species.

    arguments
        tiers: a list of curation tiers
        size:  indicates the maximum size a set should be during retrieval
        sp_id: species identifier

    returns
        a list of gene set IDs
    """

    tiers = tuplify(tiers)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    cur_id IN %(tiers)s AND
                    CASE
                        WHEN %(size)s > 0 THEN gs_count < %(size)s
                        ELSE true
                    END AND
                    CASE
                        WHEN %(sp_id)s > 0 THEN sp_id = %(sp_id)s
                        ELSE true
                    END;
            ''', {'tiers': tiers, 'size': size, 'sp_id': sp_id}
        )

        return listify(cursor)

def get_geneset_ids_by_attribute(attrib, size=0, sp_id=0):
    """
    Returns a list of normal (i.e. their status is not deleted or deprecated)
    geneset IDs that belong to a particular attribution group. Allows filtering of
    returned sets based on size and species.

    arguments
        attrib: GW attribution ID
        size:   indicates the maximum size a set should be during retrieval
        sp_id: species identifier

    returns
        a list of gene set IDs
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    gs_attribution = %(attrib)s AND
                    CASE
                        WHEN %(size)s > 0 THEN gs_count < %(size)s
                        ELSE true
                    END AND
                    CASE
                        WHEN %(sp_id)s > 0 THEN sp_id = %(sp_id)s
                        ELSE true
                    END;
            ''', {'attrib': attrib, 'size': size, 'sp_id': sp_id}
        )

        return listify(cursor)

def get_geneset_values(gs_ids):
    """
    Returns all gene set values (genes and scores) for the given list of gene set IDs.

    arguments
        gs_ids: a list of gs_ids

    returns
        a list of dicts, each dict contains the gene set id, gene id, and gene score
    """

    gs_ids = tuplify(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT gs_id, ode_gene_id, gsv_value
            FROM   extsrc.geneset_value
            WHERE  gs_id IN %s;
            ''', (gs_ids,)
        )

        results = dictify(cursor)

        ## Convert Decimal values to floats
        for i in range(len(results)):
            results[i]['gsv_value'] = float(results[i]['gsv_value'])

        return results

def get_gene_homologs(genes, source='Homologene'):
    """
    Returns all homology IDs for the given list of gene IDs.

    arguments
        genes:  list of internal GeneWeaver gene identifiers
        source: the homology mapping data source to use

    returns
        a bijection of gene identifiers to homology identifiers

    TODO: might want to consider making this return a 1:N mapping to take into account
          paralogs, etc.
    """

    genes = tuplify(genes)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT ode_gene_id, hom_id
            FROM   extsrc.homology
            WHERE  ode_gene_id IN %s AND
                   hom_source_name = %s;
            ''', (genes, source)
        )

        return associate(cursor)

## Idk why this is here but can probably be removed?
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
            ''', (hom_ids,)
        )

        return associate_duplicate(cursor)

def get_publication(pmid):
    """
    Returns the GW publication ID associated with the gived PubMed ID.

    arguments
        pmid: PubMed ID

    returns
        a GW publication ID or None one doesn't exist
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
            ''', (pmid,)
        )

        result = cursor.fetchone()

        return result[0] if result else None

def get_publications(pmids):
    """
    Returns a mapping of PubMed IDs to their GW publication IDs.

    arguments
        pmids: a list of PubMed IDs

    returns
        a dict mapping PubMed IDs to GW publication IDs
    """

    pmids = tuplify(pmids)

    with PooledCursor() as cursor:

        ## The lowest pub_id should be used and the others eventually deleted.
        cursor.execute(
            '''
            SELECT      pub_pubmed, MIN(pub_id) as pub_id
            FROM        production.publication
            WHERE       pub_pubmed IN %s
            GROUP BY    pub_pubmed;
            ''', (pmids,)
        )

        return associate(cursor)

## I think this can be deleted
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
        a string representing the article's PMID or None if one doesn't exist
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT pub_pubmed
            FROM   production.publication
            WHERE  pub_id = %s;
            ''', (pub_id,)
        )

        result = cursor.fetchone()

        return result[0] if result else None

def get_geneset_pmids(gs_ids):
    """
    Returns a bijection of gene set identifiers to the PubMed IDs they are associated
    with.

    arguments
        gs_ids: list of gene set IDs to retrieve PMIDs for

    returns
        a dict that maps the GS ID to the PMID. If a GS ID doesn't have an associated
        publication, then it will be missing from results.
    """

    gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT      g.gs_id, p.pub_pubmed
            FROM        production.publication p
            INNER JOIN  production.geneset g
            USING       (pub_id)
            WHERE       gs_id IN %s;
            ''', (gs_ids,)
        )

        return associate(cursor)

def get_geneset_metadata(gs_ids):
    """
    Returns names, descriptions, and abbreviations for each geneset in the
    provided list.

    arguments
        gs_ids: list of gene set IDs to retrieve PMIDs for

    returns
        a list of dicts containing gene set IDs, names, descriptions, and abbreviations
    """

    gs_ids = tuplify(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, gs_name, gs_description, gs_abbreviation
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''', (gs_ids,)
        )

        return dictify(cursor)

## Might get rid of this
def get_geneset_size(gs_ids):
    """
    Returns geneset sizes for the given genesets.

    """
    if type(gs_ids) == list:
        gs_ids = tuple(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  gs_id, gs_count
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''', (gs_ids,)
        )

        return associate(cursor)

## and get rid of this
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
            ''', (gs_ids,)
        )

        return associate(cursor)

def get_gene_types(short=False):
    """
    Returns a bijection of gene type names to their associated type identifier.
    If short is true, returns "short names" which are condensed or abbreviated names.

    arguments
        short: optional argument to return short names

    returns
        a bijection of type names to type IDs
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  CASE WHEN %s THEN gdb_shortname ELSE gdb_name END,
                    gdb_id
            FROM    odestatic.genedb;
            ''', (short,)
        )

        return associate(cursor)

def get_score_types():
    """
    Returns a list of score types supported by GeneWeaver. This data isn't currently
    stored in the DB but it should be.

    returns
        a bijection of score types to type IDs
    """

    return {
        'p-value': 1,
        'q-value': 2,
        'binary': 3,
        'correlation': 4,
        'effect': 5
    }

def get_platforms():
    """
    Returns the list of GW supported microarray platform and gene expression
    technologies.

    returns
        a list of objects whose keys match the platform table. These attributes include
        the unique platform identifier, the platform name, a condensed name, and the GEO
        GPL identifier.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT pf_id, pf_name, pf_shortname, pf_gpl_id
            FROM   odestatic.platform;
            '''
        )

        return dictify(cursor)

def get_platform_names():
    """
    Returns the list of GW supported microarray platform and gene expression
    technologies.

    returns
        a bijection of platform names to identifiers.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT pf_name, pf_id
            FROM   odestatic.platform;
            '''
        )

        return associate(cursor)

def get_platform_probes(pf_id, refs):
    """
    Retrieves internal GW probe identifiers for the given list probe reference
    identifiers. Requires a platform ID since some expression platforms reuse probe
    references.

    arguments
        pf_id: platform identifier
        refs:  list of probe reference identifiers belonging to a platform

    returns
        a bijection of probe references to GW probe identifiers for the given platform
    """

    refs = tuplify(refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT prb_ref_id, prb_id
            FROM   odestatic.probe
            WHERE  pf_id = %s AND
                   prb_ref_id IN %s;
            ''', (pf_id, refs)
        )

        return associate(cursor)

def get_all_platform_probes(pf_id):
    """
    Returns all the probe reference identifiers (these are provided by the manufacturer
    and stored in the GW DB) for the given platform.

    arguments
        pf_id: platform ID

    returns
        a list of probe references
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  prb_ref_id, prb_id
            FROM    odestatic.probe
            WHERE   pf_id = %s;
            ''', (pf_id,)
        )

        return listify(cursor)

## Idk if this is ever used anywhere
def get_all_platform_genes(pf_id):
    """
    For the given platform, retrieves the genes each probe is supposed to map to.

    arguments
        pf_id: platform ID

    returns
        the list of genes targeted by all probes for the given platform
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT      ode_gene_id
            FROM        odestatic.probe p
            INNER JOIN  extsrc.probe2gene p2g
            USING       (prb_id)
            WHERE       pf_id = %s;
            ''', (pf_id,)
        )

        return listify(cursor)

def get_probe2gene(prb_ids):
    """
    For the given list of GW probe identifiers, retrieves the genes each probe is
    supposed to map to. Retrieves a 1:N mapping since some platforms map a single probe
    to multiple genes.

    arguments
        prb_ids: a list of probe IDs

    returns
        a 1:N mapping of probe IDs to genes (ode_gene_ids)
    """

    prb_ids = tuplify(prb_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  prb_id, ode_gene_id
            FROM    extsrc.probe2gene
            WHERE   prb_id in %s;
            ''', (prb_ids,)
        )

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
            ''', (name,)
        )

        return None if not cursor.rowcount else cursor.fetchone()[0]

## I think I can delete this
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

    arguments
        pj_ids: a list of project IDs

    returns
        a 1:N mapping of project IDs to gene set IDs
    """

    pj_ids = tuplify(pj_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT  pj_id, gs_id
            FROM    production.project2geneset
            WHERE   pj_id IN %s;
            ''', (pj_ids,)
        )

        return associate_duplicate(cursor)

def get_geneset_annotations(gs_ids):
    """
    Returns the set of ontology annotations for each given gene set.

    arguments
        gs_ids: list of gene set ids to retrieve annotations for

    returns
        a 1:N mapping of gene set IDs to ontology annotations.
        The value of each key in the returned dict is a list of tuples.
        Each tuple comprises a single annotation and contains two elements:
        1, an internal GW ID which represents an ontology term (ont_id);
        2, the external ontology term id used by the source ontology.
            e.g. {123456: (7890, 'GO:1234567')}
    """

    gs_ids = tuplify(gs_ids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT      go.gs_id, go.ont_id, o.ont_ref_id
            FROM        extsrc.geneset_ontology AS go
            INNER JOIN  extsrc.ontology AS o
            ON          USING (ont_id)
            WHERE       gs_id IN %s;
            ''', (gs_ids,)
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
        a bijection of ontology term references to GW ontology IDs
    """

    ont_refs = tuplify(ont_refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT ont_ref_id, ont_id
            FROM   extsrc.ontology
            WHERE  ont_ref_id IN %s
            ''', (ont_refs,)
        )

        return associate(cursor)

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
            SELECT ontdb_id, ontdb_name, ontdb_prefix, ontdb_date
            FROM   odestatic.ontologydb;
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
            SELECT ontdb_id
            FROM   odestatic.ontologydb
            WHERE  LOWER(ontdb_name) = LOWER(%s);
            ''', (name,)
        )

        return None if not cursor.rowcount else cursor.fetchone()[0]

def get_ontology_terms_by_ontdb(ontdb_id):
    """
    Retrieves all ontology terms associated with the given ontology.

    args
        ontdb_id: the ID representing an ontology

    returns
        a list of dicts whose fields match the columns in the ontology table.
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT *
            FROM   extsrc.ontology
            WHERE  ontdb_id = %s;
            ''', (ontdb_id,)
        )

        return dictify(cursor)

def get_threshold_types(lower=False):
    """
    Returns a bijection of threshold type names to their IDs.
    This data should be stored in the DB but it's not so we hardcode it here.

    arguments
        lower: optional argument which lower cases names if it is set to True

    returns
        a mapping of threshold types to IDs (gs_threshold_type)
    """

    types = ['P-value', 'Q-value', 'Binary', 'Correlation', 'Effect']
    type_ids = [1, 2, 3, 4, 5]

    if lower:
        return dict(zip([t.lower() for t in types], type_ids))

    return dict(zip(types, type_ids))

    ## INSERTIONS ##
    ################

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
            ''', gs
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
            ''', (gs_id, gene_id, value, [name], [float(value)], threshold)
        )

        return cursor.fetchone()[0]

def insert_geneset_values(values):
    """
    Like insert_geneset_value but inserts a list of values.

    arguments
        values: a list of geneset values, where each element is a tuple.
                The elements in the tuple should be in the following order:

                    gs_id, ode_gene_id, value, name, in_threshold
    """

    ## Some preprocessing: the source and value list fields need to generated and
    ## formatted as arrays
    for i, vs in enumerate(values):

        values[i] = (vs[0], vs[1], vs[2], [vs[3]], [vs[2]], vs[4])

    with PooledCursor() as cursor:

        execute_values(
            cursor,
            '''
            INSERT INTO extsrc.geneset_value (
                gs_id,
                ode_gene_id,
                gsv_value,
                gsv_source_list,
                gsv_value_list,
                gsv_in_threshold,
                gsv_hits,
                gsv_date
            ) VALUES %s;
            ''',
            values,
            '''
            (%s, %s, %s, %s, %s, %s, 0, NOW())
            '''
        )


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
            ''', (gene_id, ref_id, gdb_id, sp_id, pref)
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
            ''', pub
        )

        return cursor.fetchone()[0]

def insert_file(size, contents, comments=''):
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
            ''', (size, contents, comments)
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
            ''', platform
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
            ''', (prb_ref, pf_id)
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
            ''', (prb_id, ode_id)
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
            ''', (lid, rid, jac)
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
            ''', (name, prefix)
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
            ''', (ref_id, name, desc, children, parents, ontdb_id)
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
            ''', (left, right, relation)
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
            ''', (gs_id, ont_id, ref_type)
        )

    ## UPDATES ##
    #############

def update_geneset_status(gsid, status='normal'):
    """
    Update the status of a geneset. The only statuses currently used are
    'normal', 'deleted', and 'deprecated'.

    arguments
        gsid:   gene set ID
        status: gene set status

    returns
        the number of rows affected by the update
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE production.geneset
            SET    gs_status = %s
            WHERE  gs_id = %s;
            ''', (status, gsid)
        )

        return cursor.rowcount

def update_geneset_dates(gsids):
    """
    Resets the "last updated" date for many gene sets to now.

    arguments
        gsids: list of gsids to update

    returns
        the number of rows affected by the update
    """

    gsids = tuplify(gsids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE production.geneset
            SET    gs_updated = NOW()
            WHERE  gs_id IN %s;
            ''', (gsids,)
        )

        return cursor.rowcount

def update_geneset_size(gsid, size):
    """
    Update the size of a geneset.

    arguments
        gsid: gene set ID
        size: new size of the gene set

    returns
        the number of rows affected by the update
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE production.geneset
            SET    gs_count = %s
            WHERE  gs_id = %s;
            ''', (size, gsid)
        )

        return cursor.rowcount

def update_ontology_term_by_ref(ref_id, name, description, children, parents):
    """
    Updates an ontology term using its reference identifier. The reference identifier
    is supplied by the ontology resource (e.g. GO:12345, MP:000123).

    args
        ref_id:      the ontology term reference ID
        name:        the name of the ontology term
        description: a description of the term
        children:    the number of immediate child terms this term has
        parents:     the number of immediate parent terms this term has

    returns
        the internal GW ontology ID of the term that was updated
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            UPDATE    extsrc.ontology
            SET       ont_name = CASE WHEN %(name)s
                                 THEN %(name)s
                                 ELSE ont_name,
                      ont_description = CASE WHEN %(description)s
                                        THEN %(description)s ELSE
                                        ont_description,
                      ont_children = CASE WHEN %(children)s
                                        THEN %(children)s ELSE
                                        ont_children,
                      ont_parents = CASE WHEN %(parents)s
                                        THEN %(parents)s ELSE
                                        ont_parents,
            WHERE     ont_ref_id = %(ref_id)s
            RETURNING ont_id;
            ''',
            {
                'name': name,
                'description': description,
                'children': children,
                'parents': parents,
                'ref_id': ref_id
            }
        )

        return None if not cursor.rowcount else cursor.fetchone()[0]

    ## DELETES ##
    #############

def delete_jaccard(lid, rid):
    """
    Deletes an entry from the jaccard cache tabletable.

    arguments
        lid: left gs_id
        rid: right gs_id
    """

    ## There's a table constrait specifying that the left ID should be smaller than the
    ## right ID
    if lid >= rid:
        lid, rid = rid, lid

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            DELETE
            FROM   extsrc.geneset_jaccard
            WHERE  gs_id_left = %s AND
                   gs_id_right = %s;
            ''', (lid, rid)
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
            ''', (ont_ids, ont_ids)
        )

        return cursor.rowcount

    ## VARIANT RELATED ##
    #####################

def get_variant_gene_type():
    """
    Returns the gene type ID for the variant gene type.
    None is returned if the variant gene type can't be found.
    """

    with PooledCursor() as cursor:
        cursor.execute(
            '''
            SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant';
            '''
        )

        return None if not cursor.rowcount else cursor.fetchone()[0]

def get_genome_builds():
    """
    Retrieves the list of genome builds supported by GW.

    returns
        a list of objects representing rows from the genome_build table
    """

    with PooledCursor() as cursor:

        cursor.execute('''SELECT * FROM odestatic.genome_build;''')

        return dictify(cursor)

def get_genome_builds_by_ref(build):
    """
    Retrieves the genome build ID for the given genome build reference identifier.

    returns
        a gb_id or None if no ID exists for the given reference
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT gb_id
            FROM   odestatic.genome_build
            WHERE  gb_ref_id = %s;
            ''', (build,)
        )

        return None if not cursor.rowcount else cursor.fetchone()[0]

## I'm not sure if this is needed
def get_variant_type_by_effect(effect):
    """
    """

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT *
            FROM   odestatic.variant_type
            WHERE  vt_effect = %s;
            ''', (effect,)
        )

        return dictify(cursor)

def get_variants_by_refs(refs, build):
    """
    Retrieves a 1:1 mapping of variant reference identifiers--which are canonical
    reference SNPs (rsIDs)--and internal GW variant IDs.

    arguments
        refs:  a list of reference SNP identifiers
        build: genome build

    returns
        a bijection of reference IDs to GW variant IDs
    """

    ## Reference SNPs are prefixed with 'rs', we remove these if they exist
    refs = map(lambda s: str(s)[2:] if str(s)[:2] == 'rs' else s, refs)
    ## Convert to integers since we store rsIDs as ints
    refs = map(int, refs)
    refs = tuplify(refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT     v.var_ref_id, v.var_id
            FROM       extsrc.variant v
            INNER JOIN extsrc.variant_info vi
            USING      (vri_id)
            WHERE      vi.gb_id = (
                            SELECT gb_id
                            FROM   odestatic.genome_build
                            WHERE  gb_ref_id = %s
                        ) AND
                        v.var_ref_id IN %s;
            ''', (build, refs)
        )

        return associate(cursor)

def get_variant_odes_by_refs(refs, build):
    """
    Retrieves a 1:1 mapping of variant reference identifiers--which are canonical
    reference SNPs (rsIDs)--and internal GW variant gene IDs (ode_gene_id).
    Variant gene IDs are stored in the gene table and are used to map genetic
    variants to gene features through intragenic, upstream, downstream, or regulatory
    associations.

    arguments
        refs:  a list of reference SNP identifiers
        build: genome build

    returns
        a bijection of reference IDs to GW variant gene IDs
    """

    ## Reference SNPs are prefixed with 'rs', we remove these if they exist
    refs = map(lambda s: str(s)[2:] if str(s)[:2] == 'rs' else s, refs)
    ## Convert to integers since we store rsIDs as ints
    refs = map(int, refs)
    refs = tuplify(refs)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT     v.var_ref_id, g.ode_gene_id
            FROM       extsrc.variant v
            INNER JOIN extsrc.variant_info vi
            USING      (vri_id)
            INNER JOIN odestatic.genome_build gb
            USING      (gb_id)
            INNER JOIN extsrc.gene g
            --
            ---- We convert to a varchar so we can take advantage of the ode_ref_id
            ---- index on the gene table
            --
            ON         v.var_id :: varchar = g.ode_ref_id
            WHERE      gb.gb_ref_id = %s AND
                       g.sp_id = gb.sp_id AND
                       v.var_ref_id IN %s;
            ''', (build, refs)
        )

        return associate(cursor)

def get_variant_refs_by_odes(odes, build):
    """
    Returns a mapping of canonical reference SNP identifier (rsID) to the given
    variant gene IDs (ode_gene_id).

    arguments
        odes: ode_gene_id list

        returns
            a bijection of variant gene IDs to variant reference identifiers
    """

    odes = tuple(odes)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT     g.ode_gene_id, v.var_ref_id
            FROM       extsrc.gene g
            INNER JOIN extsrc.variant v
            ON         v.var_id = g.ode_ref_id :: BIGINT
            INNER JOIN extsrc.variant_info vi
            USING      (vri_id)
            INNER JOIN odestatic.genome_build gb
            USING      (gb_id)
            WHERE      gb.gb_ref_id = %s AND
                       g.sp_id = gb.sp_id AND
                       g.ode_gene_id IN %s;
            ''', (build, odes)
        )

        return associate(cursor)

def roll_up_variants_from_odes(odes, mapping=('Variant',)):
    """
    Rolls the given list of variants up to the gene level using the given variant mapping
    type.

    arguments
        odes:  a list of variant ode_gene_ids
        build: string specifying the genome build to use (e.g. hg38)

    returns
        a mapping of variant ode_gene_ids to gene ode_gene_ids. If the variant is not
        found in a gene, or no mapping exists, then it will be missing from the dict of
        returned associations. Similarly, if the genome build given is incorrect, no
        mapping will be returned.
    """

    odes = tuplify(odes)
    mapping = tuplify(mapping)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            SELECT DISTINCT ON (hom_source_id, ode_gene_id)
                   hom_source_id, ode_gene_id
            FROM   extsrc.homology h
            WHERE  hom_source_id IN %s AND
                   hom_source_name IN %s;
            ''', (odes, mapping)
        )

        return associate_duplicate(cursor)

def is_variant_set(gsids):
    """
    Determines if the given gene sets are variant sets.

    arguments
        gsids: list of gene set IDs

    returns
        a GSID-bool bijection, as a dict, where the bool is true if the set is a
        variant set and false otherwise.
    """

    gsids = tuplify(gsids)

    with PooledCursor() as cursor:

        cursor.execute(
            '''
            WITH variant_ids AS (
                SELECT gdb_id FROM odestatic.genedb WHERE gdb_name ILIKE 'variant'
            )
            SELECT   gs_id,
                     CASE
                        -- We must check that the id type is negative otherwise we could
                        -- inadvertently match against expression platforms
                        --
                        WHEN g.gs_gene_id_type < 0 AND
                             gdb.gdb_id IN (SELECT * FROM variant_ids) THEN TRUE
                        ELSE FALSE
                      END
            FROM      production.geneset g
            --
            -- We must left join because expression platforms are not found in the genedb table
            --
            LEFT JOIN odestatic.genedb gdb
            ON        gdb.gdb_id = @g.gs_gene_id_type
            WHERE     g.gs_id IN %s;
            ''', (gsids,)
        )

        return associate(cursor)


## Can get rid of all the insert variant functions
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
            ''', var
        )

        return cursor.fetchone()[0]

def insert_variants_and_info(variants):
    """
    """
    with PooledCursor() as cursor:
        execute_values(
            cursor,
            '''
            INSERT INTO extsrc.variant
                (
                    var_ref_id, var_allelel, var_obs_alleles, var_ma, var_maf,
                    vt_id, var_clinsig, vri_id
                )
            VALUES %s;
            ''',
            variants,
            '''
            (%(rsid)s, %(allele)s, %(observed)s, %(ma)s, %(maf)s, %(effect)s,
            %(clinsig)s,
            (
                INSERT INTO extsrc.variant_info
                    (vri_chromosome, vri_position, gb_id)
                VALUES
                    (%(chrom)s, %(coord)s, %(build)s)
                RETURNING vri_id
            ))
            '''
        )

def insert_variants(variants):
    """
    """
    with PooledCursor() as cursor:
        execute_values(
            cursor,
            '''
            INSERT INTO extsrc.variant
                (
                    var_ref_id, var_allelel, var_obs_alleles, var_ma, var_maf,
                    vt_id, var_clinsig, vri_id
                )
            VALUES %s;
            ''',
            variants,
            '''
            (%(rsid)s, %(allele)s, %(observed)s, %(ma)s, %(maf)s, %(effect)s,
            %(clinsig)s, %(vri_id)s)
            ''',
            5000
        )

def insert_variant_infos(variants):
    """
    """
    with PooledCursor() as cursor:
        execute_values(
            cursor,
            '''
            INSERT INTO extsrc.variant_info
                    (vri_chromosome, vri_position, gb_id)
            VALUES %s
            RETURNING vri_id;
            ''',
            variants,
            '''
            (%(chrom)s, %(coord)s, %(build)s)
            '''
        )

        return map(lambda t: t[0], cursor.fetchall())

def insert_variant_info(variant):
    """
    """
    with PooledCursor() as cursor:
        cursor.execute(
            '''
            INSERT INTO extsrc.variant_info
                (vri_chromosome, vri_position, gb_id)
            VALUES
                (%(chrom)s, %(coord)s, %(build)s)
            RETURNING vri_id;
            ''', variant
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
            ''', var
        )

        return cursor.fetchone()[0]

if __name__ == '__main__':

    pass

