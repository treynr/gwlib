#!/usr/bin/python

## db.py
## v 0.1
#
## Contains all the important functions for accessing and querying the 
## GeneWeaver DB.
#

import psycopg2

# Attempt db connection
try:
    conn = psycopg2.connect(("dbname='geneweaver' user='odeadmin' "
                             "password='odeadmin'"))
except:
    print "[!] Oh noes, failed to connect to the db"
    exit()

# Get db_cursor
g_cur = conn.cursor()

## query_genesets
#
## Returns all gene set IDs (gs_id) that meet the following criteria: < 1000 
## genes in a set and have g_curation tiers specified by the user. 
#
## ret, list of IDs for all gene sets that meet the above criteria
#
def queryGenesets(tiers=None, size=1000):
    import re

    if not tiers:
        tiers = [x for x in range(1, 6)]
    else:
        # Remove anything that isn't an actual tier (should only be #'s 1 - 5)
        tiers = [x for x in tiers if (x >= 1) and (x <= 5)]

    query = ('SELECT gs_id FROM production.geneset WHERE gs_count < %s AND '
             'cur_id = ANY(%s);')

    g_cur.execute(query, [size, tiers])

    res = g_cur.fetchall()

    # Iterates over the list and moves the gs_id from the tuple to a new list
    return map(lambda x: x[0], res)

def queryGenesetSize(id):
    query = 'SELECT gs_count FROM production.geneset WHERE gs_id=%s;'

    g_cur.execute(query, [id])

    # Only get the first result
    return g_cur.fetchall()[0][0] # [(value,)] --> value

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
def findGenesetsWithOntology(ont):
    if not ont:
        return []

    # Limit to MeSH 
    #query = ('SELECT ego.gs_id, ego.ont_id, eo.ont_name FROM '
    #         'extsrc.geneset_ontology AS ego JOIN extsrc.ontology AS eo ON '
    #         'ego.ont_id=eo.ont_id WHERE eo.ont_name=\'%s\' AND eo.ontdb_id=4 '
    #         ';')
    query = (' SELECT ego.gs_id, ego.ont_id, eo.ont_name FROM '
             'extsrc.geneset_ontology AS ego JOIN extsrc.ontology AS eo ON '
             'ego.ont_id=eo.ont_id JOIN production.geneset AS pg ON '
             'pg.gs_id=ego.gs_id WHERE eo.ont_name=%s AND eo.ontdb_id=4 '
             'AND pg.gs_count < 1000 AND (pg.cur_id=3 OR pg.cur_id=4 OR pg.cur_id=5);')

    g_cur.execute(query, [ont])

    res = g_cur.fetchall();

    return map(lambda x: x[0], res)

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

## query_genes
#
## Returns all genes (their IDs and names) for a given gene set. The gene name
## that is returned is the preferred (ode_pref) name.
#
## arg0, a tuple of gene set IDs
## ret, list of tuples containing the ode_gene_id and ode_ref_id.
#
def queryGenes(id):
    if (id is None) or (id == 0):
        return []

    #query = ("SELECT eg.ode_gene_id, eg.ode_ref_id FROM extsrc.gene eg JOIN "
    #         "extsrc.geneset_value egv ON eg.ode_gene_id=egv.ode_gene_id "
    #         "WHERE eg.ode_pref='t' AND egv.gs_id=%s;")
    #query = ("SELECT eg.ode_gene_id FROM extsrc.gene eg FULL OUTER JOIN "
    #         "extsrc.geneset_value egv ON eg.ode_gene_id=egv.ode_gene_id "
    #         "WHERE eg.ode_pref='t' and egv.gs_id IN %s;")
    query = ("SELECT DISTINCT(eg.ode_gene_id) FROM extsrc.gene eg, "
             "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
             "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")
    g_cur.execute(query, [id])

    res = g_cur.fetchall()

    return map(lambda x: x[0], res)

def queryGenesAsName(id):
    if (id is None) or (id == 0):
        return []

    #query = ("SELECT DISTINCT(eg.ode_ref_id) FROM extsrc.gene eg, "
    #         "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
    #         "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")
    query = ("SELECT eg.ode_ref_id, egv.gs_id FROM extsrc.gene eg, "
             "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
             "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")

    g_cur.execute(query, [id])

    res = g_cur.fetchall()

    # Returns a list of tuples
    return res
    #return map(lambda x: x[0], res)

## find_geneset_with_ontol
#
## Returns all gene set IDs (gs_id) associated with a given ontology id 
## (ont_id). Results can be limited by ontology type (e.g. GO or MeSH).
## Capitalization counts (for the limiters)! 
## Also, limits sets by gene count and g_curation tier.
## TODO: pig disgusting function name that needs to be changed
#
## arg0, an ontology id (ont_id)
## arg1 (optional, defaults to GO), an ontology db id (ontdb_id) or prefix
## ret, list of gene set IDs associated with the given ontology
#
def find_geneset_with_ontol(id, ont=None):
    onts = {1:'GO', 2:'MP', 3:'MA', 4:'EDAM', 5:'MeSH', 
            'GO':1, 'MP':2, 'MA':3, 'EDAM':4, 'MeSH':5}
    query = ("SELECT ego.gs_id FROM extsrc.geneset_ontology ego JOIN "
             "extsrc.ontology eo ON eo.ont_id=ego.ont_id JOIN "
             "production.geneset pg ON pg.gs_id=ego.gs_id WHERE "
             "pg.gs_count < 1000 AND (pg.cur_id=3 OR pg.cur_id=4) ") #WHERE eo.ont_id=%s")

    # If the ontology type isn't found in the above dict...
    if (ont is not None) and (ont not in onts):
        ont = None
    # Check if the ontology type is a number, if not, convert (using dict)
    if (ont is not None) and (not isinstance(ont, int)):
        ont = onts[ont]
    if ont is None:
        #query += "WHERE eo.ont_id=%s;"
        query += "AND eo.ont_id=%s;"
        g_cur.execute(query, [id])
    else:
        #query += "WHERE eo.ont_id=%s AND eo.ontdb_id=%s;"
        query += "AND eo.ont_id=%s AND eo.ontdb_id=%s;"
        g_cur.execute(query, [id, ont])

    return g_cur.fetchall()

## queryJaccards
#
## Returns all Jaccard coefficients for the given gene set ID. Can be filtered
## via gene set size and tiers as well. 
#
## TODO, the return value is really convoluted. Need to change it.
#
def queryJaccards(id, tiers=None, size=1000):
    import re

    # Remove anything that isn't an actual tier (should only be #'s 1 - 5)
    tiers = [x for x in tiers if (x >= 1) and (x <= 5)]

    # Two queries, one for the left and the other for the right
    # There are no duplicates (i.e two rows, where gs_id_left in row one is 
    # equal to gs_id_right in the other and vice versa)
    #queryl = ('SELECT gs_id_left, gs_id_right, cur_id, gs_id, jac_value, '
    #         'gs_count FROM extsrc.geneset_jaccard AS jac JOIN '
    queryl = ('SELECT gs_id_left, gs_id_right, jac_value '
             'FROM extsrc.geneset_jaccard AS jac JOIN '
             'production.geneset AS pg ON jac.gs_id_right=pg.gs_id WHERE '
             'jac.gs_id_left=%s AND pg.gs_count < %s')
    #queryr = ('SELECT gs_id_left, gs_id_right, cur_id, gs_id, jac_value, '
    #         'gs_count FROM extsrc.geneset_jaccard AS jac JOIN '
    queryr = ('SELECT gs_id_left, gs_id_right, jac_value '
             'FROM extsrc.geneset_jaccard AS jac JOIN '
             'production.geneset AS pg ON jac.gs_id_left=pg.gs_id WHERE '
             'jac.gs_id_right=%s AND pg.gs_count < %s')

    if not tiers:
        queryl += ';'
        queryr += ';'
    else:
        queryl += ' AND ( '
        queryr += ' AND ( '

        for t in tiers:
            queryl += 'cur_id=' + str(t) + ' OR '
            queryr += 'cur_id=' + str(t) + ' OR '

        queryl += ');'
        queryr += ');'
        queryl = re.sub('OR \);', ');', queryl)
        queryr = re.sub('OR \);', ');', queryr)

    g_cur.execute(queryl, [id, size])
    resl = g_cur.fetchall()

    g_cur.execute(queryr, [id, size])
    resr = g_cur.fetchall()

    return (resl, resr)

## queryAllMeshTerms
#
## Returns all the MeSH terms in the database. These are retrieved from my 
## (Tim's) gene2mesh data set. 
#
def queryAllMeshTerms():
    # Eventually will change from public to mesh schema
    query = 'SELECT id, name FROM public.term;'

    g_cur.execute(query)

    return g_cur.fetchall()

def queryAllG2m():
    query = 'SELECT term_id FROM public.gene2mesh;'
    #query = 'SELECT t.name from public.gene2mesh AS g2m JOIN public.term AS t ON g2m.term_id=id;'

    g_cur.execute(query)

    res = g_cur.fetchall()

    return map(lambda x: x[0], res)

## Returns an ode_gene_id for a given list of symbols. Assumes humans as 
## the species, specifies a gdb_id of 7 (gene symbol) for the query, and
## ensures the ID is ode_pref(erred).
#
def geneSymbolToId(symbols):
    # For some reason this query returns duplicate values without the DISTINCT
    # clause. Must be a bug in psycopg because this query doesn't return 
    # duplicates without the DISTINCT when entered from psql.
    query = ('SELECT DISTINCT ode_gene_id, ode_ref_id FROM extsrc.gene WHERE gdb_id=7 '
             'AND sp_id=2 AND ode_pref=true AND (')

    for i in range(len(symbols)):
        if i == (len(symbols) - 1):
            query += 'ode_ref_id=%s);'
        else:
            query += 'ode_ref_id=%s OR '

    g_cur.execute(query, symbols)

    return g_cur.fetchall()

## queryGsName
#
## Given a list of geneset IDs, returns a dict mapping gs_id --> gs_name.
#
def queryGsName(ids):
    if not ids:
        return {}

    query = ('SELECT gs_id, gs_name FROM production.geneset WHERE gs_id = '
             'ANY(%s);')

    # Python's disgusting type system doesn't catch any text -> int errors, so
    # we need to manually convert any ids provided as strings to ints
    ids = map(int, ids)

    g_cur.execute(query, [ids])

    res = g_cur.fetchall()
    gmap = {}

    # The result is a list of tuples: fst = gs_id, snd = gs_name
    for r in res:
        gmap[str(r[0])] = r[1]

    return gmap

if __name__ == '__main__':

    print len(queryGenes((14921, 14923)))
    print queryGenes((14921, 14923))
    #terms = queryJaccards(31361, [2,3])
    #print terms[0][0]
    #print queryGenesetSize(31361)

    #print len(set(terms))

