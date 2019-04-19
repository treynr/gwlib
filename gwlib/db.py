#!/usr/bin/env python2

## file: db.py
## desc: Contains all the important functions for accessing and querying the
##       GeneWeaver DB.
## auth: TR
#

from collections import OrderedDict as od
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
import pandas as pd

## Global connection variable
conn = None
CONNPOOL = None

class PooledConnection(ThreadedConnectionPool):
    """
    Derives psycopg2's ThreadedConnectionPool class and allows connection pools
    to be creating using python's with statement.
    Can also be instantiated normally and implements the same methods as
    ThreadedConnectionPool.
    """

    def __init__(self, minconn=2, maxconn=10, pool=None, *args, **kwargs):

        if pool:
            self.pool = pool
        else:
            self.pool = super(PooledConnection, self).__init__(
                minconn, maxconn, *args, **kwargs
            )

        self.connections = []

    def __enter__(self):

        self.connections.append(self.getconn())

        return self.connections[-1]

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.connections:
            self.putconn(self.connections.pop())

    def getconn(self, key=None):
        return super(PooledConnection, self).getconn(key=key)

    def putconn(self, conn, key=None, close=False):
        return super(PooledConnection, self).putconn(conn, key=key, close=close)

    def closeall(self):
        return super(PooledConnection, self).closeall()

class PooledCursor(object):
    """
    Small class that encapsulates psycopg2's connection and cursor objects.
    Makes use of the global connection pool and retrieves a new connection from
    the pool when instatiated normally or using the with statement.
    """

    def __init__(self, pool=None):

        if not pool:
            global CONNPOOL
            pool = CONNPOOL

        self.pool = pool
        self.connection = pool.getconn()
        self.cursor = None

        self.connection.set_client_encoding('UTF-8')

    def __enter__(self):

        self.cursor = self.connection.cursor()

        self.cursor.execute('SET search_path = curation,extsrc,odestatic,production;')

        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.cursor:
            self.cursor.close()

            self.cursor = None

        self.pool.putconn(self.connection)

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

    global CONNPOOL

    try:
        CONNPOOL = PooledConnection(
            2, 10, host=host, dbname=db, user=user, password=password, port=port
        )

    except Exception as e:

        return (False, e)

    return (True, '')

def dictify(df):
    """
    Converts rows return by a query into a list of dictionaries.

    arguments
        df: dataframe returned by a DB query

    returns
        a list of dicts containing the results of the query.
        In the form [{column: value, ...}, ...]
    """

    return df.to_dict(orient='records')

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

def listify(df, key=None):
    """
    Converts the query result into a 2D numpy array.
    If key is given, then produces a 1D array using values from the column 
    specified by key.

    arguments
        df:  dataframe
        key: optional, return a list of values from this column

    returns
        an array
    """

    if key:
        return df[key].values

    return df.values

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

def biject(df, key=None, val=None):
    """
    Creates a simple mapping (bijection) of values from one column to another.
    If key or val are not supplied, generates a bijection between values from
    the first and second columns of the given dataframe.
    Duplicates are overwritten.

    arguments
        df:  dataframe returned by one of the DB queries
        key: optional, the column that should be used as the dict key
        val: optional, the column that should be used as the value

    returns
        a bijection of values in one column to another
    """

    if key and val:
        pass

    ## Only key was provided, then assume the value is just the first column after
    ## the key column is removed
    elif key:
        val = df.iloc[:, ~df.columns.isin([key])].columns[0]

    ## Only val was provided, then assume the key is the first column after the
    ## val column is removed
    elif val:
        key = df.iloc[:, ~df.columns.isin([val])].columns[0]

    ## Assume the key is the first column and the value is the second
    else:
        key, val = df.columns[:2]

    return df.set_index(key).to_dict(orient='dict')[val]

def associate_multiple(df, key=None):
    """
    Creates a mapping of values from one column to all others.
    If key is not supplied, generates a bijection between values from
    the first column to values of all others.

    arguments
        df:  dataframe returned by one of the DB queries
        key: optional, the column that should be used as the dict key

    returns
        a bijection of one column to all others
    """

    ## Only key was provided, then assume the value is just the first column after
    ## the key column is removed
    if not key:
        key = df.columns[0]

    return dict(
        [(row[key], row[row.index != key].tolist()) for (_, row) in df.iterrows()]
    )

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
        a dataframe containing species names (sp_name) and GW species IDs (sp_id)
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  CASE WHEN %s THEN LOWER(sp_name) ELSE sp_name END, sp_id
            FROM    odestatic.species;
            ''',
            conn,
            params=(lower,)
        )

def get_species_with_taxid(lower=False):
    """
    Returns a a list of species supported by GW. The returned list includes species
    names, identifiers, and NCBI taxon IDs.

    arguments
        lower: if true, returns lowercased species names

    returns
        a dataframe containing species names (sp_name), GW species IDs (sp_id), and
        NCBI taxon IDs (sp_taxid)
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT CASE WHEN %s THEN LOWER(sp_name) ELSE sp_name END, sp_id, sp_taxid
            FROM   odestatic.species;
            ''',
            conn,
            params=(lower,)
        )

## Get rid of this
def get_species_by_taxid():
    """
    Returns a mapping of species taxids (NCBI taxonomy ID) to their sp_id.

    returns
        a dataframe of NCBI taxon IDs (sp_taxid) and GW species IDs (sp_id)
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT sp_taxid, sp_id
            FROM   odestatic.species;
            ''',
            conn
        )

def get_attributions():
    """
    Returns all the attributions (at_id and at_abbrev) found in the DB.
    These represent third party data resources integrated into GeneWeaver.

    returns
        a dataframe of attribution abbreviations (at_abbrev) and their
        identifiers (at_id)
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT at_abbrev, at_id
            FROM   odestatic.attribution;
            ''',
            conn
        )

def get_gene_ids(refs, sp_id=None, gdb_id=None):
    """
    Given a set of external reference IDs, this returns a dataframe of
    reference gene identifiers and their IDs used internally by GeneWeaver
    (ode_gene_id).
    An optional species id can be provided to limit gene results by species.
    An optional gene identifier type can be provided to limit mapping by ID type
    (useful when identifiers from different resources overlap).
    This query does not incude genomic variants.

    Reference IDs are always strings (even if they're numeric) and should be
    properly capitalized. If duplicate references exist in the DB (unlikely for
    anything except symbols) then they are overwritten in the return dict.
    Reference IDs can be any valid identifier supported by GeneWeaver (e.g.
    Ensembl, NCBI Gene, MGI, HGNC, etc.).
    See the get_gene_types function for gene types supported by GW.

    arguments
        refs:   a list of reference identifiers to convert
        sp_id:  an optional species identifier used to limit the ID mapping process
        gdb_id: an optional gene type identifier used to limit the ID mapping process

    returns
        a dataframe containing reference identifiers (ode_ref_id) and GW gene
        IDs (ode_gene_id)
    """

    refs = tuplify(refs)

    with CONNPOOL as conn:
        return pd.read_sql_query(
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
            ''',
            conn,
            params={'refs': refs, 'spid': sp_id, 'gdbid': gdb_id}
        )

def get_species_genes(sp_id, gdb_id=None, symbol=True):
    """
    Similar to the above get_gene_ids() but returns a dataframe containing reference
    and GW IDs mapping for every single gene associated with a given species (warning,
    this might be a lot of data).
    This query does not include genomic variants.
    If a gdb_id is provided then this will return all genes covered by the given gene
    type.
    If symbol is true, then the function returns gene entities that have an official
    gene symbol to limit the amount of data returned.

    limit the amount of data returned.
    gdb_id will always override the symbol argument.

    arguments
        sp_id:  species identifier
        gdb_id: an optional gene type identifier used to limit the ID mapping process
        symbol: if true limits results to genes covered by the symbol gene type

    returns
        a dataframe containing all gene reference ID (ode_ref_id) and GW gene ID
        (ode_gene_id) pairs for a species.
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
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
            ''',
            conn,
            params={'sp_id': sp_id, 'gdb_id': gdb_id, 'symbol': symbol}
        )

def get_gene_refs(genes, type_id=None):
    """
    The inverse of the get_gene_refs() function. For the given list of internal GW gene
    identifiers, this function returns a datafrome of external (e.g. MGI, HGNC, Ensembl)
    reference identifiers.

    arguments
        genes:   a list of internal GW gene identifiers (ode_gene_id)
        type_id: an optional gene type ID to limit the mapping to a specific gene type

    returns
        a dataframe containing GW gene IDs (ode_gene_id) and gene reference IDs
        (ode_ref_id)
    """

    genes = tuplify(genes)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  DISTINCT ON (ode_gene_id, ode_ref_id) ode_gene_id, ode_ref_id
            FROM    extsrc.gene
            WHERE   ode_gene_id IN %(genes)s AND
                    CASE
                        WHEN %(type_id)s IS NOT NULL THEN gdb_id = %(type_id)s
                        ELSE true
                    END;
            ''',
            conn,
            params={'genes': genes, 'type_id': type_id}
        )


def get_genesets(gs_ids):
    """
    Returns a dataframe of gene set metadata for the given list of gene set IDs.

    arguments
        gs_ids: a list of gs_ids

    returns
        a dataframe of geneset metadata
    """

    gs_ids = tuplify(gs_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  *
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''',
            conn,
            params=(gs_ids,)
        )

def get_geneset_ids(tiers=[1, 2, 3, 4, 5], at_id=None, size=0, sp_id=0):
    """
    Returns an array of normal (i.e. their status is not deleted or deprecated)
    gene set IDs.
    IDs can be filtered based on tiers, gene set size, species, and public resource
    attribution.

    arguments
        at_id: public resource attribution ID
        tiers: a list of curation tiers
        size:  indicates the maximum size a set should be during retrieval
        sp_id: species identifier

    returns
        an array of gene set IDs (gs_id)
    """

    tiers = tuplify(tiers)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  gs_id
            FROM    production.geneset
            WHERE   gs_status NOT LIKE 'de%%' AND
                    cur_id IN %(tiers)s AND
                    CASE
                        WHEN %(at_id)s IS NOT NULL THEN gs_attribution = %(at_id)s
                        ELSE TRUE
                    END AND
                    CASE
                        WHEN %(size)s > 0 THEN gs_count < %(size)s
                        ELSE TRUE
                    END AND
                    CASE
                        WHEN %(sp_id)s > 0 THEN sp_id = %(sp_id)s
                        ELSE TRUE
                    END;
            ''',
            conn,
            params={'tiers': tiers, 'at_id': at_id, 'size': size, 'sp_id': sp_id}
        ).gs_id.to_numpy()

## Remove this
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
    Returns a dataframe containing gene set values (genes and scores) for the given
    list of gene set IDs.

    arguments
        gs_ids: a list of gene set identifiers

    returns
        a dataframe containing gene set IDs (gs_id), GW gene IDs (ode_gene_id), and
        scores (gsv_value)
    """

    gs_ids = tuplify(gs_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT gs_id, ode_gene_id, gsv_value
            FROM   extsrc.geneset_value
            WHERE  gs_id IN %s;
            ''',
            conn,
            params=(gs_ids,)
        )

def get_gene_homologs(genes, source='Homologene'):
    """
    Returns a dataframe contaiing internal GW homology IDs for the given
    list of gene IDs.

    arguments
        genes:  list of internal GeneWeaver gene identifiers (ode_gene_id)
        source: the homology mapping data source to use, default is Homologene

    returns
        a dataframe containing GW gene IDs (ode_gene_id) and their associated
        homology IDs (hom_id)
    """

    genes = tuplify(genes)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT ode_gene_id, hom_id
            FROM   extsrc.homology
            WHERE  ode_gene_id IN %s AND
                   hom_source_name = %s;
            ''',
            conn,
            params=(genes, source)
        )

def get_publication(pmid):
    """
    Returns the GW publication ID associated with the given PubMed ID.

    arguments
        pmid: PubMed ID

    returns
        a GW publication ID (pub_id) or None one doesn't exist
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
    Returns a dataframe containing PubMed IDs and their respective GW publication
    IDs.
    In cases where there are duplicate entries for the same PubMed ID, the minimum
    GW publication ID (pub_id) is returned.

    arguments
        pmids: a list of PubMed IDs

    returns
        a dataframe containing PubMed IDs (pub_pubmed) and their associated
        GW publication IDs (pub_id).
    """

    pmids = tuplify(pmids)

    with CONNPOOL as conn:
        ## The lowest pub_id should be used and the others eventually deleted.
        return pd.read_sql_query(
            '''
            SELECT      pub_pubmed, MIN(pub_id) as pub_id
            FROM        production.publication
            WHERE       pub_pubmed IN %s
            GROUP BY    pub_pubmed;
            ''',
            conn,
            params=(pmids,)
        )

def get_publication_pmid(pub_id):
    """
    Returns the PMID associated with a GW publication ID.

    arguments:
        pub_id: the GW publication ID

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
    Returns a dataframe of gene set identifiers and the PubMed IDs they are
    associated with.

    arguments
        gs_ids: list of gene set IDs to retrieve PMIDs for

    returns
        a dataframe containing gene set IDs (gs_id) and PubMed IDs (pub_pubmed)
    """

    gs_ids = tuple(gs_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT      g.gs_id, p.pub_pubmed
            FROM        production.publication p
            INNER JOIN  production.geneset g
            USING       (pub_id)
            WHERE       gs_id IN %s;
            ''',
            conn,
            params=(gs_ids,)
        )

def get_geneset_text(gs_ids):
    """
    Returns a dataframe containing gene set names, descriptions, and abbreviations
    for each geneset in the provided list.

    arguments
        gs_ids: list of gene set IDs to retrieve metadata for

    returns
        a dataframe of containing gene set IDs, names, descriptions, and
        abbreviations
    """

    gs_ids = tuplify(gs_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  gs_id, gs_name, gs_description, gs_abbreviation
            FROM    production.geneset
            WHERE   gs_id IN %s;
            ''',
            conn,
            params=(gs_ids,)
        )

def get_gene_types(short=False):
    """
    Returns a dataframe with gene type names and their associated type identifier.
    If short is true, returns "short names" which are condensed or abbreviated names.

    arguments
        short: optional argument to return short names

    returns
        a dataframe containing gene types and their GW IDs (gdb_id).
        If short == True, then gdb_shortname is returned otherwise the column is
        gdb_name.
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  CASE WHEN %s THEN gdb_shortname ELSE gdb_name END,
                    gdb_id
            FROM    odestatic.genedb;
            ''',
            conn,
            params=(short,)
        )

def get_platforms():
    """
    Returns the list of GW supported microarray platform and gene expression
    technologies.

    returns
        a  dataframe containing gene expression platforms supported by GW.
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT pf_id, pf_name, pf_shortname, pf_gpl_id
            FROM   odestatic.platform;
            ''',
            conn
        )

def get_platform_probes(pf_id, refs):
    """
    Retrieves internal GW probe identifiers for the given list of probe reference
    identifiers. Requires a platform ID since some expression platforms reuse probe
    references.

    arguments
        pf_id: platform identifier
        refs:  list of probe reference identifiers belonging to a platform

    returns
        a dataframe containing probe references (prb_ref_id) and GW probe
        identifiers (prb_id)
    """

    refs = tuplify(refs)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT prb_ref_id, prb_id
            FROM   odestatic.probe
            WHERE  pf_id = %s AND
                   prb_ref_id IN %s;
            ''',
            conn,
            params=(pf_id, refs)
        )

def get_all_platform_probes(pf_id):
    """
    Returns all the probe reference identifiers (these are provided by the manufacturer
    and stored in the GW DB) for the given platform.

    arguments
        pf_id: platform ID

    returns
        a dataframe containing probe references (prb_ref_id) and their GW probe
        IDs (prb_id)
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  prb_ref_id, prb_id
            FROM    odestatic.probe
            WHERE   pf_id = %s;
            ''',
            conn,
            params=(pf_id,)
        )

def get_probe_genes(prb_ids):
    """
    For the given list of GW probe identifiers (prb_id), retrieves a dataframe containing
    the genes each probe is supposed to map to.
    This ends up being a N:1 mapping since some platforms map a multiple probes to a
    single gene.

    arguments
        prb_ids: a list of probe IDs

    returns
        a dataframe containing probe IDs (prb_id) to and the genes (ode_gene_id) they
        represent
    """

    prb_ids = tuplify(prb_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  prb_id, ode_gene_id
            FROM    extsrc.probe2gene
            WHERE   prb_id in %s;
            ''',
            conn,
            params=(prb_ids,)
        )

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

def get_genesets_by_project(pj_ids):
    """
    Returns all genesets associated with the given GW project IDs (pj_id).

    arguments
        pj_ids: a list of project IDs

    returns
        a dataframe containing project IDs (pj_id) and their associated gene set
        IDs (gs_id)
    """

    pj_ids = tuplify(pj_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT  pj_id, gs_id
            FROM    production.project2geneset
            WHERE   pj_id IN %s;
            ''',
            conn,
            (pj_ids,)
        )

def get_geneset_annotations(gs_ids):
    """
    Returns gene set annotations for the given list of gene set IDs.

    arguments
        gs_ids: list of gene set ids to retrieve annotations for

    returns
        a dataframe containing geneset annotations which include the internal GW
        ontology id (ont_id) and the ontology term reference (ont_ref_id)
    """

    gs_ids = tuplify(gs_ids)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT      go.gs_id, go.ont_id, o.ont_ref_id
            FROM        extsrc.geneset_ontology AS go
            INNER JOIN  extsrc.ontology AS o
            USING       (ont_id)
            WHERE       gs_id IN %s;
            ''',
            conn,
            params=(gs_ids,)
        )

def get_ontology_ids_by_refs(ont_refs):
    """
    Returns a dataframe containing internal GW ontology IDs and the external
    reference IDs (e.g. GO:0123456, MP:0123456) they are associated with.

    arguments
        ont_refs: a list of external ontology reference IDs

    returns
        a dataframe containing GW ontology IDs (ont_id) and reference IDs (ont_ref_id)
    """

    ont_refs = tuplify(ont_refs)

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT ont_ref_id, ont_id
            FROM   extsrc.ontology
            WHERE  ont_ref_id IN %s
            ''',
            conn,
            params=(ont_refs,)
        )

def get_ontologies():
    """
    Returns the list of ontologies supported by GeneWeaver for use with gene
    set annotations.

    returns
        a dataframe containing ontologies supported by GW
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT ontdb_id, ontdb_name, ontdb_prefix, ontdb_linkout_url, ontdb_date
            FROM   odestatic.ontologydb;
            ''',
            conn
        )

def get_ontology_terms_by_ontdb(ontdb_id):
    """
    Retrieves all ontology terms associated with the given ontology.

    args
        ontdb_id: the ID representing an ontology

    returns
        a dataframe containing ontology term metadata for a given ontology
    """

    with CONNPOOL as conn:
        return pd.read_sql_query(
            '''
            SELECT *
            FROM   extsrc.ontology
            WHERE  ontdb_id = %s;
            ''',
            conn,
            params=(ontdb_id,)
        )

def get_threshold_types(lower=False):
    """
    Returns a dataframe of threshold type names to their IDs.
    This data should be stored in the DB but it's not so we hardcode it here.

    arguments
        lower: optional argument which lower cases names if it is set to True

    returns
        a dataframe containing threshold types and their IDs (gs_threshold_type)
    """

    types = ['P-value', 'Q-value', 'Binary', 'Correlation', 'Effect']
    type_ids = [1, 2, 3, 4, 5]

    if lower:
        types = [t.lower() for t in types]

    return pd.DataFrame(zip(types, type_ids), columns=['type_name', 'gs_threshold_type'])

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

