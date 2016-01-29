#!/usr/bin/python

#### file:	db.py
#### desc:	Contains all the important functions for accessing and querying the
####		GeneWeaver DB.
#### vers:	0.1.0
#### auth:	TR
##

import datetime as dt
import psycopg2
import random

## Attempt local db connection; only time this really ever fails is when the
## postgres server isn't running.
try:
	conn = psycopg2.connect(("host='crick' dbname='geneweaver' user='odeadmin' "
							 "password='odeadmin'"))
except:
	print "[!] Oh noes, failed to connect to the db"
	exit()

## Globals are bad, mmkay?
g_cur = conn.cursor()

###############################################################################
################################### Queries ###################################

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

#### getSpecies
##
#### Returns all species in the DB as a mapping, sp_name -> sp_id.
##
#### ret: dict, mapping of species names to their internal IDs
##
def getSpecies():
	query = '''SELECT sp_name, sp_id
			   FROM odestatic.species;'''

	g_cur.execute(query)

	## Returns a list of tuples [(sp_name, sp_id)]
	res = g_cur.fetchall()
	d = {}

	## We return a dict of sp_name --> sp_id
	for tup in res:
		d[tup[0]] = tup[1]

	return d

## There's a subtle difference between getGeneIds and the "sensitive" version
## below it. getGeneIds requires gene symbols to exactly match their
## counterparts in the DB. The SQL query considers the genes BRCA1 and Brca1 as
## different. The sensitive version, doesn't require proper capitalization BUT
## this comes at the expense of run time. The SQL query takes for-fucking-ever
## and should only be used in certain cases.

#### getGeneIds
##
#### Given a list of external references for genes (e.g. symbols), this 
#### function returns a mapping, ode_ref_id --> ode_gene_ids. 
#### If the symbol doesn't exist in the DB or can't be found, it is mapped
#### to None.
##
#### arg: [string], list of external gene refs
#### ret: dict, ode_ref_id -> ode_gene_id mapping
##
def getGeneIds(refs, pref=True):
	if type(refs) == list:
		refs = tuple(refs)

	query = '''SELECT DISTINCT ode_ref_id, ode_gene_id 
			   FROM extsrc.gene
			   WHERE ode_ref_id IN %s'''

	g_cur.execute(query, [refs])

	## Returns a list of tuples [(ode_ref_id, ode_gene_id)]
	res = g_cur.fetchall()
	d = {}

	found = map(lambda x: x[0], res)

	## Map symbols that weren't found to None
	for nf in (set(refs) - set(found)):
		res.append((nf, None))

	## We return a dict of ode_ref_id --> ode_gene_ids
	for tup in res:
		d[tup[0]] = tup[1]

	return d

#### getGeneIdsSensitive
##
#### Given a list of external references for genes (e.g. symbols), this 
#### function returns a mapping, ode_ref_id --> ode_gene_ids. 
#### If the symbol doesn't exist in the DB or can't be found, it is mapped
#### to None.
##
#### arg: [string], list of external gene refs
#### ret: dict, ode_ref_id -> ode_gene_id mapping
##
def getGeneIdsSensitive(refs, pref=True):
	query = '''SELECT DISTINCT ode_ref_id, ode_gene_id 
			   FROM extsrc.gene
			   WHERE ode_ref_id LIKE ANY (%s)'''

	g_cur.execute(query, [refs])

	## Returns a list of tuples [(ode_ref_id, ode_gene_id)]
	res = g_cur.fetchall()
	d = {}

	found = map(lambda x: x[0], res)

	## Map symbols that weren't found to None
	for nf in (set(refs) - set(found)):
		res.append((nf, None))

	## We return a dict of ode_ref_id --> ode_gene_ids
	for tup in res:
		d[tup[0]] = tup[1]

	return d

#### getGeneIdsBySpecies
##
#### Given a list of external references for genes (ode_ref_ids), this 
#### function returns a symbol mapping, ode_ref_id --> ode_gene_ids, but
#### only for a single species. If the symbol doesn't exist in the DB or 
#### can't be found, it is mapped to None. humans = 2
##
#### arg: [string], list of external gene refs
#### arg: integer, GW species ID
#### ret: dict, ode_ref_id -> ode_gene_id mapping
##
def getGeneIdsBySpecies(syms, spec, pref=True):
	if type(syms) == list:
		syms = tuple(syms)

	query = '''SELECT DISTINCT ode_ref_id, ode_gene_id 
			   FROM extsrc.gene
			   WHERE sp_id = %s AND '''
	if pref:
		query += 'ode_pref = true AND ode_ref_id IN %s;'
	else:
		query += 'ode_ref_id IN %s;'

	g_cur.execute(query, [spec, syms])

	## Returns a list of tuples [(ode_ref_id, ode_gene_id)]
	res = g_cur.fetchall()
	d = {}

	found = map(lambda x: x[0], res)

	## Map symbols that weren't found to None
	for nf in (set(syms) - set(found)):
		res.append((nf, None))

	## We return a dict of ode_ref_id --> ode_gene_ids
	for tup in res:
		d[tup[0]] = tup[1]

	return d

#### getGeneIdsBySpecies2 (sql query changes)
##
#### Given a list of external references for genes (ode_ref_ids), this 
#### function returns a symbol mapping, ode_ref_id --> ode_gene_ids, but
#### only for a single species. If the symbol doesn't exist in the DB or 
#### can't be found, it is mapped to None. humans = 2
##
#### arg: [string], list of external gene refs
#### arg: integer, GW species ID
#### ret: dict, ode_ref_id -> ode_gene_id mapping
##
def getGeneIdsBySpecies2(syms, spec, pref=True):
	query = '''SELECT DISTINCT ode_ref_id, ode_gene_id 
			   FROM extsrc.gene
			   WHERE sp_id = %s AND ode_ref_id LIKE ANY (%s);'''

	#if pref:
	#	query += 'ode_pref = true AND ode_ref_id IN %s;'
	#else:
	#	query += 'ode_ref_id IN %s;'

	g_cur.execute(query, [spec, [syms]])

	## Returns a list of tuples [(ode_ref_id, ode_gene_id)]
	res = g_cur.fetchall()
	d = {}

	found = map(lambda x: x[0], res)

	## Map symbols that weren't found to None
	for nf in (set(syms) - set(found)):
		res.append((nf, None))

	## We return a dict of ode_ref_id --> ode_gene_ids
	for tup in res:
		d[tup[0]] = tup[1]

	return d

def getGenesetValues(gsids):
	if type(gsids) == list:
		gsids = tuple(gsids)

	query = '''SELECT gs_id, ode_gene_id, gsv_value
			   FROM extsrc.geneset_value
			   WHERE gs_id IN %s;'''

	g_cur.execute(query, [gsids])

#### getGenesetsByTier
##
#### Returns all gs_ids in the given tier(s) and that are smaller than a
#### certain size. Calling the function with no arguments will return all
#### genesets in all tiers that have less than 1000 members.
##
#### arg: [integer], list of tiers to use when querying genesets
#### arg: integer, size limit (default: 1000)
#### ret, list of IDs for all gene sets that meet the above criteria
##
def getGenesetsByTier(tiers=None, size=1000):

	if not tiers:
		tiers = (1, 2, 3, 4, 5)

	else:
		tiers = tuple(tiers)


	query = '''SELECT gs_id 
			   FROM production.geneset 
			   WHERE gs_status NOT LIKE 'de%%' AND 
			   		 gs_count < %s AND 
			   		 cur_id IN %s;'''

	g_cur.execute(query, [size, tiers])

	res = g_cur.fetchall()

	# Strip out the tuples, only returning a list
	return map(lambda x: x[0], res)

#### getGenesetGeneIds
##
#### Returns the contents (ode_gene_ids) of a given list of genesets.
#### The results are returned as a mapping of gs_ids -> ode_gene_ids.
##
#### arg: [integer],  a list of gs_ids
#### ret: dict, a mapping of gs_ids (int) to list of ode_gene_ids ([int])
##
def getGenesetGeneIds(gsids):
	if type(gsids) == list:
		gsids = tuple(gsids)

	query = '''SELECT gs_id, ode_gene_id 
			   FROM extsrc.geneset_value
			   WHERE gs_id IN %s;'''
	d = {}

	g_cur.execute(query, [gsids])

	res = g_cur.fetchall()

	## We return a dict, k: gs_id; v: [ode_gene_id]
	for tup in res:
		if d.get(tup[0], None):
			d[tup[0]].append(tup[1])
		else:
			d[tup[0]] = [tup[1]]

	return d

#### getGeneRefs
##
#### Returns an ode_ref_id (where ode_pref = true) for the given ode_gene_ids.
#### The results are returned as mapping of ode_gene_id -> ode_ref_id.
##
#### arg: [integer], list of ode_gene_ids
#### ret: dict, mapping of ode_gene_ids (int) to an ode_ref_id (string)
##
def getGeneRefs(gids):
	if type(gids) == list:
		gids = tuple(gids)

	query = '''SELECT ode_gene_id, ode_ref_id 
			   FROM extsrc.gene 
			   WHERE ode_pref = 't' AND ode_gene_id IN %s;'''

	g_cur.execute(query, [gids])

	res = g_cur.fetchall()
	d = {}

	found = map(lambda x: x[0], res)

	## Map ode_gene_ids with no preferred ode_ref_id to itself
	for nf in (set(gids) - set(found)):
		res.append((nf, str(nf)))

	## We return a dict, k: ode_gene_id; v: ode_ref_id
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

def getGenesetDescriptions(gsids):
	"""
	Returns all gs_descriptions for the given gs_ids.

	:arg list: list of gs_ids
	:ret dict: mapping of gs_id -> gs_description
	"""

	if type(gsids) == list:
		gsids = tuple(gsids)

	query = '''SELECT gs_id, gs_description 
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
	gs['pub_id'] = pub	# The pubmed article still needs to retrieved
	gs['gs_threshold_type'] = int(ttype)
	gs['gs_threshold'] = thresh
	gs['gs_gene_id_type'] = int(gtype)
	gs['usr_id'] = int(usr)
	gs['values'] = vals # Not a column in the geneset table; processed later
	gs['file_id'] = file_id
	gs['gs_attribution'] = at_id

	## Other fields we can fill out
	gs['gs_count'] = len(vals)
	gs['cur_id'] = cur_id			# auto private tier?

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

#### query_genesets
##
#### Returns all gene set IDs (gs_id) that meet the following criteria: < 1000 
#### genes in a set and have g_curation tiers specified by the user. 
##
#### ret, list of IDs for all gene sets that meet the above criteria
def queryGenesets(tiers=None, size=1000):
	import re

	if not tiers:
		tiers = [x for x in range(1, 6)]
	else:
		# Remove anything that isn't an actual tier (should only be #'s 1 - 5)
		tiers = [x for x in tiers if (x >= 1) and (x <= 5)]

	query = ("SELECT gs_id FROM production.geneset WHERE "
			 "gs_status NOT LIKE 'de%%' AND gs_count < %s AND "
			 'cur_id = ANY(%s);')

	g_cur.execute(query, [size, tiers])

	res = g_cur.fetchall()

	# Iterates over the list and moves the gs_id from the tuple to a new list
	return map(lambda x: x[0], res)

def queryGenesAsName(id):
	query = ("SELECT eg.ode_ref_id, egv.gs_id FROM extsrc.gene eg, "
			 "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
			 "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")

	g_cur.execute(query, [id])

	res = g_cur.fetchall()

	# Returns a list of tuples
	return res

def queryGenesAsId(id):
	if type(id) == list:
		id = tuple(id)
	query = ("SELECT eg.ode_gene_id, egv.gs_id FROM extsrc.gene eg, "
			 "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
			 "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")

	g_cur.execute(query, [id])

	res = g_cur.fetchall()

	# Returns a list of tuples
	return res

## Given a gs_id, returns a list of tuples containing the ode_gene_id and 
## gsv_value of all geneset_values associated with the gs_id.
def queryGeneValues(id):
	query = ('SELECT ode_gene_id, gsv_value FROM extsrc.geneset_value '
			 'WHERE gs_id=%s;')

	g_cur.execute(query, [id])

	res = g_cur.fetchall()

	# Returns a list of tuples
	return res

def queryGenesetSize(id):
	if type(id) == list:
		id = tuple(id)

	query = 'SELECT gs_id, gs_count FROM production.geneset WHERE gs_id IN %s;'

	g_cur.execute(query, [id])

	# Only get the first result
	#return g_cur.fetchall()[0][0] # [(value,)] --> value
	return g_cur.fetchall()

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
	#		  'extsrc.geneset_ontology AS ego JOIN extsrc.ontology AS eo ON '
	#		  'ego.ont_id=eo.ont_id WHERE eo.ont_name=\'%s\' AND eo.ontdb_id=4 '
	#		  ';')
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
	#		  'ont_description LIKE %%%s%%;')
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
	#		  "extsrc.geneset_value egv ON eg.ode_gene_id=egv.ode_gene_id "
	#		  "WHERE eg.ode_pref='t' AND egv.gs_id=%s;")
	#query = ("SELECT eg.ode_gene_id FROM extsrc.gene eg FULL OUTER JOIN "
	#		  "extsrc.geneset_value egv ON eg.ode_gene_id=egv.ode_gene_id "
	#		  "WHERE eg.ode_pref='t' and egv.gs_id IN %s;")
	query = ("SELECT DISTINCT(eg.ode_gene_id) FROM extsrc.gene eg, "
			 "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
			 "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")
	g_cur.execute(query, [id])

	res = g_cur.fetchall()

	return map(lambda x: x[0], res)

## queryGenesAsName
#
## Returns a list of tuples (gs_id, gene_name) for list of geneset IDs. The
## list of geneset IDs is actually a giant tuple. 
def queryGenesAsName(id):
	if (id is None) or (id == 0):
		return []
	if type(id) == list:
		id = tuple(id)

	#query = ("SELECT DISTINCT(eg.ode_ref_id) FROM extsrc.gene eg, "
	#		  "extsrc.geneset_value egv WHERE eg.ode_pref='t' and "
	#		  "eg.ode_gene_id=egv.ode_gene_id AND egv.gs_id IN %s; ")
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
	#		  'gs_count FROM extsrc.geneset_jaccard AS jac JOIN '
	queryl = ('SELECT gs_id_left, gs_id_right, jac_value '
			 'FROM extsrc.geneset_jaccard AS jac JOIN '
			 'production.geneset AS pg ON jac.gs_id_right=pg.gs_id WHERE '
			 'jac.gs_id_left=%s AND pg.gs_count < %s')
	#queryr = ('SELECT gs_id_left, gs_id_right, cur_id, gs_id, jac_value, '
	#		  'gs_count FROM extsrc.geneset_jaccard AS jac JOIN '
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

## Returns jaccard values for the given gs_id and all MeSH genesets.
#
def meshSetJaccards(gs_id):
	query = ('SELECT gs_id_right, jac_value FROM extsrc.geneset_jaccard '
			 'WHERE gs_id_left = %s AND gs_id_right IN (SELECT gs_id FROM '
			 'production.geneset WHERE gs_name LIKE \'Mesh Set ("%%\');')

	g_cur.execute(query, [gs_id])

	return g_cur.fetchall()

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

def geneSymbolToId(symbols, sp_id=2):
	if type(symbols) == list:
		symbols = tuple(symbols)
	# For some reason this query returns duplicate values without the DISTINCT
	# clause. Must be a bug in psycopg because this query doesn't return
	# duplicates without the DISTINCT when entered from psql.
	# 9/11/14 - a late note, but I fixed this problem in July, turned out to
	# be a shit ton of duplicates in the GW DB that was causing it. No idea how
	# the fuck those duplicates got in there since they weren't there in May.
	# talking about the bepo db btw, I now consider it the retarded child of 
	# Erich's machines.
	##query = ('SELECT DISTINCT ode_gene_id, ode_ref_id FROM extsrc.gene WHERE gdb_id=7 '
	##		   'AND sp_id=2 AND ode_pref=true AND (')
	query = ('SELECT DISTINCT ode_gene_id, ode_ref_id FROM extsrc.gene WHERE gdb_id=7 '
			 'AND sp_id=%s AND ode_pref=true AND ode_ref_id IN %s;')

	##for i in range(len(symbols)):
	##	  if i == (len(symbols) - 1):
	##		  query += 'ode_ref_id=%s);'
	##	  else:
	##		  query += 'ode_ref_id=%s OR '

	g_cur.execute(query, [sp_id, symbols])

	return g_cur.fetchall()


## queryGeneFromRef
## DEPRECATED -- REPLACED BY getGeneIds
#
## Will probably replace the above querySymbolToId function. This function 
## takes a list of ode_ref_ids and returns a list of tuples 
## (ode_gene_id, ode_ref_id). Can be used to map gene IDs from other DBs
## (like NCBI) to the internal identifiers GeneWeaver uses.
#
def queryGeneFromRef(ids, asdict=True):
	if type(ids) is list:
		ids = tuple(ids)

	query = ('SELECT ode_ref_id, ode_gene_id FROM extsrc.gene WHERE '
			'ode_ref_id IN %s;')
	g_cur.execute(query, [ids])
	res = g_cur.fetchall()

	if asdict:
		d = {}
		for t in res:
			d[t[0]] = t[1]

		res = d

	return res

	#return g_cur.fetchall()
	#return map(lambda x: x[0], res)

## queryGeneFromRef2
#
## Will probably replace the above querySymbolToId function. This function 
## takes a list of ode_ref_ids and returns a list of tuples 
## (ode_gene_id, ode_ref_id). Can be used to map gene IDs from other DBs
## (like NCBI) to the internal identifiers GeneWeaver uses.
#
## new function so changing the old one wouldn't break any scripts. I'll have
## to update the scripts eventually. Added ability to query based on 
## species (sp_id). sp_id = 2 is humans, 1 = mus musculus
#
def queryGeneFromRef2(ids, sp=2, asdict=True):
	if type(ids) is list:
		ids = tuple(ids)

	query = ('SELECT ode_ref_id, ode_gene_id FROM extsrc.gene WHERE '
			'sp_id=%s AND ode_ref_id IN %s;')
	g_cur.execute(query, [sp, ids])
	res = g_cur.fetchall()

	if asdict:
		d = {}
		for t in res:
			d[t[0]] = t[1]

		res = d

	return res


## Convert ode_gene_id -> symbol/entity name/whatever it's called
## DEPRECATED -- REPLACED BY getGeneNames
def queryGeneName(ids):
	if type(ids) is list:
		ids = tuple(ids)

	query = ('SELECT ode_gene_id, ode_ref_id FROM extsrc.gene WHERE '
			'ode_pref=\'t\' AND ode_gene_id IN %s')

	g_cur.execute(query, [ids])

	return g_cur.fetchall()

#### getGeneNames
##
#### Returns an ode_ref_id (where ode_pref = true) for the given ode_gene_ids.
#### The results are returned as a dict, mapping ode_gene_ids --> ode_ref_id.
##
#### arg, int list of ode_gene_ids
#### ret, dict mapping ode_gene_ids (int) to an ode_ref_id (string)
##
def getGeneNames(gids):
    if type(gids) == list:
        gids = tuple(gids)

    query = ("SELECT ode_gene_id, ode_ref_id FROM extsrc.gene WHERE "
             "ode_pref = 't' AND ode_gene_id IN %s;")
    d = {}

    g_cur.execute(query, [gids])

    res = g_cur.fetchall()

    found = map(lambda x: x[0], res)

    ## Map ode_gene_ids with no preferred ode_ref_id to itself
    for nf in (set(gids) - set(found)):
        res.append((nf, str(nf)))

    ## We return a dict, k: ode_gene_id; v: ode_ref_id
    for tup in res:
        d[tup[0]] = tup[1]

    return d

## queryGsName
#
## Given a list of geneset IDs, returns a dict mapping gs_id --> gs_name.
#
def queryGsName(ids):
	if not ids:
		return {}
	if type(ids) == list:
		ids = tuple(ids)

	query = ('SELECT gs_id, gs_name FROM production.geneset WHERE gs_id = '
			 'ANY(%s);')
	query = ('SELECT gs_id, gs_name FROM production.geneset WHERE gs_id IN '
			 '%s;')

	# Python's disgusting type system doesn't catch any text -> int errors, so
	# we need to manually convert any ids provided as strings to ints
	#ids = map(int, ids)

	g_cur.execute(query, [ids])

	res = g_cur.fetchall()
	gmap = {}

	# The result is a list of tuples: fst = gs_id, snd = gs_name
	for r in res:
		gmap[r[0]] = r[1]

	return gmap

## createGeneset
#
## Creates a geneset.
#
def createGeneset(cur_id, sp_id, thresh_type, thresh, cnt, name, abbrev, desc):
	usr = 3507787 # My usr_id
	query = ('INSERT INTO geneset (file_id, usr_id, cur_id, sp_id, '
			#'gs_threshold_type, gs_threshold, gs_groups, gs_created, '
			'gs_threshold_type, gs_threshold, gs_created, '
			'gs_updated, gs_status, gs_count, gs_uri, gs_gene_id_type, '
			'gs_name, gs_abbreviation, gs_description, gs_attribution) VALUES '
			'(0, %s, %s, %s, %s, %s, NOW(), NOW(), \'normal\', %s, \'\', -7, '
			'%s, %s, %s, 0) RETURNING gs_id;')
	g_cur.execute('set search_path = extsrc,production,odestatic;')
	g_cur.execute(query, [usr, cur_id, sp_id, thresh_type, thresh, cnt, name, abbrev, desc])

	# Make the changes permanent
	#conn.commit()

	return map(lambda x: x[0], g_cur.fetchall())[0]

def createGenesetValue(gs_id, gene_id, value, name, thresh):
	query = ('INSERT INTO extsrc.geneset_value (gs_id, ode_gene_id, '
			'gsv_value, gsv_hits, gsv_source_list, gsv_value_list, '
			'gsv_in_threshold, gsv_date) VALUES (%s, %s, %s, 0, %s, ARRAY[0], '
			'%s, %s);')

	g_cur.execute(query, [gs_id, gene_id, value, [name], thresh, 
		dt.date.today()])

## findMeshSet
#
## Finds a specific MeSH geneset using the MeSH term and string matching. Not
## really sure if there's a better way to do it. If the gs_name strings for
## these sets ever change, this will have to be updated. 
#
def findMeshSet(term):
	term = 'MeSH Set ("' + term + '"%%'
	query = ('SELECT gs_id FROM production.geneset WHERE gs_name LIKE %s AND '
			 'gs_status LIKE \'normal\';')

	g_cur.execute(query, [term])

	res = g_cur.fetchall()

	if not res:
		return None
	if res[0]:
		return res[0][0]
	else:
		return None

## getMeshSets
#
## Returns the gs_ids for all the MeSH sets in the database. Finds MeSH sets
## using simple string matching. If the MeSH set names ever change this will
## need to be updated. 
#
def getMeshSets():
	query = ('SELECT gs_id FROM production.geneset WHERE gs_name LIKE '
			 '\'MeSH Set ("%%\' AND gs_status LIKE \'normal\';')

	g_cur.execute(query)

	res = g_cur.fetchall()

	return map(lambda x: x[0], res)

## getAbaSets
#
## Returns the gs_ids for all the ABA sets in the database.
#
def getAbaSets():
	query = ('SELECT gs_id FROM production.geneset WHERE gs_name LIKE '
			 '\'ABA Set - %%\' AND gs_status LIKE \'normal\';')

	g_cur.execute(query)

	res = g_cur.fetchall()

	return map(lambda x: x[0], res)

## updateMeshSet
#
## Updates a MeSH geneset by marking the old one as deprecated, creating a 
## new one, and pointing the old one to the new. The function has to search
## for the MeSH set--this is accomplished using the findMeshSet function 
## above.
#
def updateMeshSet(term, cnt, terms): 
	title = 'MeSH Set ("' + term + '" : ' + terms[term]['UI'] + ')'
	desc = ('MeSH geneset generated by gene2mesh - "' + term + '" : ' + 
			terms[term]['UI'])
	abbr = '"' + term + '" : ' + terms[term]['UI']

	oid = findMeshSet(term)
	nid = createGeneset(2, 2, 1, 1.0, cnt, title, abbr, desc)

	if oid:
		query = 'UPDATE production.geneset SET gs_status = \'deprecated:%s\' WHERE gs_id=%s;'
		g_cur.execute(query, [nid, oid])

	return nid

## calcJaccard
#
## Calculates Jaccard indices for a given gs_id. Code essentially ripped from 
## the OfflineSimilarity tool. This is just done so I can target particular
## genesets (or update Jaccards after adding new genesets) and don't have to 
## wait for the script to finish.
#
## arg, gs_id, the geneset ID to use in the calculate_jaccard function
#
def calcJaccard(gs_id):
	g_cur.execute('SET search_path TO production,extsrc,odestatic;')
	g_cur.execute('SELECT calculate_jaccard(%s);' % (gs_id,))
	conn.commit()

## Functions related to python-based jaccard calculation

## updateJacStart
#
## Updates the date a jaccard calculation was started for a particular 
## geneset by altering the gsi_jac_started column found in geneset_info. Sets
## the time started to now().
#
## arg, 
#
def updateJacStart(gsids):
	if type(gsids) == list:
		gsids = tuple(gsids)
	
	query = ('UPDATE production.geneset_info SET gsi_jac_started=NOW() WHERE '
			 'gs_id IN %s;')

	#g_cur.execute('SET search_path TO production,extsrc,odestatic;')
	g_cur.execute(query, [gsids])

## addJaccards
#
## Given a list of tuples (id_left, id_right, jac) this function adds the
## jaccard values to the DB.
#
def addJaccards(jacs):
	query = ('INSERT INTO extsrc.geneset_jaccard '
			 '(gs_id_left, gs_id_right, jac_value) VALUES (%s, %s, %s);')

	for j in jacs:
		g_cur.execute(query, list(j))

## deleteJaccards
#
## Deletes all jaccard values for a given list of genesets.
#
def deleteJaccards(gsids):
	if type(gsids) == list:
		gsids = tuple(gsids)

	query = 'DELETE FROM extsrc.geneset_jaccard WHERE gs_id_left IN %s;'
	#query = ('DELETE FROM extsrc.geneset_jaccard WHERE gs_id_left IN %s OR '
	#		 'gs_id_right IN %s;')

	#g_cur.execute(query, [gsids, gsids])
	g_cur.execute(query, [gsids])

## findGenesetWithGenes
#
## Returns a list of genesets that contain at least one gene from a given list
## of genes. Assuming ode_gene_ids are given.
#
def findGenesetsWithGenes(genes):
	if type(genes) == list:
		genes = tuple(genes)

	query = ("SELECT DISTINCT pg.gs_id FROM production.geneset AS pg, "
			 "extsrc.geneset_value AS egv WHERE pg.gs_status NOT LIKE 'de%%' "
			 "AND egv.ode_gene_id IN %s;")
	#query = ("SELECT DISTINCT gs_id FROM extsrc.geneset_value WHERE "
	#		 "ode_gene_id IN %s;")

	g_cur.execute(query, [genes])

	# de-tuple the results
	return map(lambda x: x[0], g_cur.fetchall())

#### This query takes awhile. Like awhile. You've been warned.
def getSetsWithoutJaccards():
	queryl = '''SELECT gs_id
			    FROM production.geneset
			    WHERE gs_status NOT LIKE 'de%%' AND
			   		  gs_id NOT IN (SELECT DISTINCT gs_id_left
					  	FROM extsrc.geneset_jaccard);'''
	queryr = '''SELECT gs_id
			    FROM production.geneset
			    WHERE gs_status NOT LIKE 'de%%' AND
			   		  gs_id NOT IN (SELECT DISTINCT gs_id_right
					  	FROM extsrc.geneset_jaccard);'''

	g_cur.execute(queryl)

	left = map(lambda x: x[0], g_cur.fetchall())

	g_cur.execute(queryr)

	right = map(lambda x: x[0], g_cur.fetchall())

	return list(set(left) | set(right))

## getGenesForJaccard
#
## Given a list of gs_ids, returns all the genes (ode_gene_ids) associatd 
## with each geneset that are within the geneset_value threshold. Returns the
## results as a dictionary, gs_ids -> [ode_gene_id]. 
#
def getGenesForJaccard(gsids):
	from collections import defaultdict as dd

	if type(gsids) == list:
		gsids = tuple(gsids)

	query = ("SELECT gs_id, ode_gene_id FROM extsrc.geneset_value WHERE "
			 "gsv_in_threshold = 't' AND gs_id IN %s;")
	#query = ('SELECT gv.gs_id, gv.ode_gene_id FROM extsrc.geneset_value AS gv '
	#		 'INNER JOIN production.geneset AS gs ON gv.gs_id = gs.gs_id '
	#		 'WHERE gsv_in_threshold = \'t\' AND gs.gs_id IN %s;') #AND gs.gs_status NOT LIKE \'de%%\';')

	#query = ('SELECT gv.gs_id, gv.ode_gene_id FROM extsrc.geneset_value AS gv, '
	#		 'production.geneset as gs WHERE gv.gs_id = gs.gs_id AND '
	#		 'gv.gsv_in_threshold = \'t\' AND gs.gs_id IN %s;') #AND gs.gs_status NOT LIKE \'de%%\';')

	g_cur.execute(query, [gsids])

	res = g_cur.fetchall()
	gmap = dd(list)

	for r in res:
		gmap[r[0]].append(r[1])

	return gmap

## gene2snp
#
##
#
def gene2snp(gids):
	if type(gids) == list:
		gids = tuple(gids)

	query = ('SELECT ode_gene_id, snp_ref_id FROM extsrc.snp WHERE '
			 'ode_gene_id IN %s;')

	g_cur.execute(query, [gids])

	#res = g_cur.fetchall()
	return g_cur.fetchall()

## getHomologySourceId
#
##
#
def getHomologySourceId(ids):
	if type(ids) == list:
		ids = tuple(ids)

	query = ('SELECT ode_gene_id, hom_source_id FROM extsrc.homology WHERE '
			 'hom_source_name LIKE \'Homologene\' AND ode_gene_id IN %s;')

	g_cur.execute(query, [ids])

	return g_cur.fetchall()

## getGeneHomologs
#
## Gets all homologous ode_gene_ids for a list of ode_gene_ids. Can return
## the results as a dict.
#
def getGeneHomologs(ids, asdict=False):
	from collections import defaultdict as dd

	if type(ids) == list:
		ids = tuple(ids)
	
	query = ('SELECT b.ode_gene_id, a.ode_gene_id FROM extsrc.homology AS a, '
			 'extsrc.homology AS b WHERE a.hom_id=b.hom_id AND '
			 'b.ode_gene_id IN %s;')

	g_cur.execute(query, [ids])
	res = g_cur.fetchall()

	if not asdict:
		return res

	hmap = dd(list)
	for tup in res:
		#hmap[tup[0]] = tup[1]
		hmap[tup[0]].append(tup[1])

	# if there weren't any hom_ids for an ode_gene_id, map it to itself
	for i in ids:
		if not hmap.has_key(i):
			hmap[i] = [i]

	return hmap

## getHomologyId
#
## gets the hom_id for the list of ode_gene_ids
#
def getHomologyIds(ids, asdict=False):
	if type(ids) == list:
		ids = tuple(ids)

	query = ('SELECT ode_gene_id, hom_id FROM extsrc.homology WHERE '
			 'hom_source_name LIKE \'Homologene\' AND ode_gene_id IN %s;')

	g_cur.execute(query, [ids])
	res = g_cur.fetchall()

	if not asdict:
		return res

	hmap = {}
	for tup in res:
		hmap[tup[0]] = tup[1]

	# if there weren't any hom_ids for an ode_gene_id, map it to none
	for i in ids:
		if not hmap.has_key(i):
			hmap[i] = None

	return hmap

#def updateMeshSet
## commitChanges
#
## Makes any changes to the database permanent. Needed after database 
## alterations (e.g. INSERT, DELETE, etc.).
#
def commitChanges():
	conn.commit()

#if __name__ == '__main__':

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

