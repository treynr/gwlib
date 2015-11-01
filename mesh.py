#!/usr/bin/python

## file:	mesh.py
## desc:	MeSH function library for retrieving MeSH and pubmed information
##			from NCBI. Uses the NCBI e-util URLs. 
## vers:	0.1.0
## auth:	TR
# 

from collections import defaultdict as dd
from itertools import groupby
import util as utl

class Tree(dd):
	"""
	Generic tree structure used to represent the MeSH hierarchy. Each node can
	have an arbitrary number of children. Leaf nodes are represented by some
	value other than a dict, while the structure (and subtrees) are series of
	nested dicts. 
	Nodes can be accessed as a dict would (e.g. tree['A01']) or using '.' 
	(e.g. tree.A01).
	"""

	def __getattr__(self, key):

		return self[key]

	def __setattr__(self, key, val):

		self[key] = val

	def __walkTree(self, path):
		"""
		Given a path of nodes, the function walks the tree while creating empty
		nodes that are referenced in the path list.

		args:
			list, a node path--each element further in the list is found
						 further in the tree. If the arg is a string, it is
						 split at '.' characters to produce a node path list.

		ret:
			Tree, the last node added
		"""

		t = self
		allpath = ''

		for i, p in enumerate(path):

			t = t[p]
			t['path'] = '.'.join(map(str, path)[:i+1])

		return t

	def addNode(self, path):
		"""
		Given a path of nodes, the function adds each node along the path to
		the tree.

		args:
			list, a node path--each element further in the list is found
				  further in the tree
		"""

		self.__walkTree(path)

	def addValue(self, path, key, val):
		"""
		Adds a key/value pair to the node referenced in the given path list.
		Note, the key should not have any periods ('.') since these are used
		internally by the tree's path variable.

		args:
			list, a node path--the key/value pair is added to the final node
			key, any key type that can be used in a dict
			val, any value type that can be used in a dict

		"""

		self.__walkTree(path)[key] = val

	def getNode(self, path):
		"""
		Returns the node found at the given path.

		args:
			list, a node path

		ret:
			Tree, final node in a path
		"""

		return self.__walkTree(path)

	def getValue(self, path, key):
		"""
		Returns the value portion of a key/value pair at a specific node.

		args:
			list, a node path
			key, get the value of this key

		ret:
			val, some value
		"""

		return self.__walkTree(path)[key]

	def getKeys(self, path):
		"""
		Returns all the keys a particular node has.

		args:
			list, a node path

		ret:
			list, list of keys
		"""

		return self.__walkTree(path).keys()

	def getChildren(self, path=None):
		"""
		Returns all the child paths for a given node. If no path is specified,
		returns all children of the root node.

		args:
			list, a node path

		ret:
			list, list of path strings
		"""

		childs = []

		if not path:
			tree = self

		else:
			tree = self.__walkTree(path)

		for k, n in tree.items():
			if type(n) == Tree:
				childs.extend(self.__getChildren(n))
				childs.append(n.path)

		return sorted(childs)

	def __getChildren(self, tree):

		childs = []

		for k, n in tree.items():
			if type(n) == Tree:
				childs.extend(self.__getChildren(n))
				childs.append(n.path)

		return childs

def tree():
	return Tree(tree)

## getArticleInfo
#
## Retrieves publication info (including MeSH terms) for a given PubMed 
## article ID. Uses NCBI's e-utils, specifically EFetch, to get the info.
#
## TODO: There has to be a better way of doing this--retrieving MeSH terms 
## that is. Using EFetch is slow and shit. And Biopython sucks too.
#
## arg, id, PubMed article ID
## ret, string of the publication info in whatever horrible format NCBI uses
#
def getArticleInfo(id):
	import urllib as ul # For NCBI e-utils crap

	# NCBI efetch util url, no xml because it's pig disgusting
	url = ('http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?'
		   'tool=geneweaver_gene2mesh&email=timothy_reynolds@baylor.edu'
		   '&db=pubmed&rettype=medline&retmode=text&id=')

	if id is None:
		return None
	else:
		for i in id:
			# Just in case the uid is an int for some reason
			url += str(id) + ','

	# Return the article information
	return ul.urlopen(url).read()

## isolateTerms
#
## Parses out the MeSH terms from a publication info string (see above function
## getArticleInfo). The format for pub info is the same as the ASCII MeSH 
## format (see data/mesh2014.bin). The function loops, checking for PMIDs and
## collects the MeSH terms (under the MH identifier) for each.
#
## arg, dat, string of the publication info in whatever horrible format NCBI 
##			 uses
## ret, pubmed2mesh dict, keys are pubmed IDs and values are lists of terms
## ret, set of all MeSH terms
#
def isolateTerms(dat):
	dat = dat.split('\n')	# split into lines
	p2m = {}				# dict, pubmed2mesh
	pub = []				# temporary pubmed ID
	all = []				# set of all MesH terms

	for ln in dat:
		if ln[:4] == 'PMID':
			pub = ln.split('-')[1].strip()	# PMID-12345
			p2m[pub] = []

		elif ln[:3] == 'MH ':
			mh = ln.split('-')[1].strip()	# MH - some term
			# some MHs have / which indicates a sub-heading, I think, and some
			# also have *, I have no idea what the hell those signify
			mh = mh.split('/')[0].strip('*').strip(' ')
			p2m[pub].append(mh)
			all.extend(mh)
		
	return (p2m, set(mh))

def getArticleTerms(pids):
	import time
	# was initially reading 500 articles at a time, but the jerk software at 
	# NCBI kept closing the connection
	pids = utl.chunkList(pids, 300) 
	info = ''

	for p in pids:
		info = getArticleInfo(p)
		# NCBI requires you only perform three requests a second
		time.sleep(0.3)			

	return isolateTerms(info)
	
## readG2p
#
## Opens and reads in the gene2pubmed data file. The information is stored in
## a dict where each gene is a key and their values are a list of publications.
#
## arg, fp, path to the gene2pubmed file
## ret, dict of genes --> pubmed IDs
#
def readG2p(fp):
	g2p = {}

	with open(fp, 'r') as fh:
		# Reads each line in the file and splits gene and pubmed IDs
		for l in fh:
			gp = l.strip().split('\t')

			# Store in the dict, key = gene id and val = pubmed id list
			g2p.setdefault(gp[0], set())
			g2p[gp[0]].add(gp[1])

	return g2p

## readP2m
#
## Opens and reads in a pubmed2mesh data file. The information is stored in
## a dict where each pubmed ID is a key and their values are a list of 
## MeSH terms.
#
## arg, fp, path to the gene2pubmed file
## ret, dict of pubmed IDs --> MeSH terms
#
def readP2m(fp):
	p2m = {}	# pubmed2mesh associations
	all = set() # set of all MeSH terms

	# Reads each line in the file and splits the pubmed IDs and MeSH terms
	with open(fp, 'r') as fh:
		for l in fh:
			# m[0] = pubmed ID, everything else = MeSH terms
			m = l.strip().split('|')

			# Some null terms were getting into the lists and fucking things up
			if '' in m:
				m.remove('')
			# Store pubmed ID + term associations in a dict
			p2m[m[0]] = m[1:]
			# Store all MeSH terms
			all.update(m[1:])

	return (p2m, all)

## makeP2m
#
## Writes the pubmed2mesh data to a file.
#
## arg, fp, filepath being written to
## arg, p2m, pubmed2mesh dict generated from the function getArticleTerms
#
def saveP2m(fp, p2m):
	with open(fp, 'w') as f:
		for k, v in p2m.items():
			print >> f, (str(k) + '|' + '|'.join(v))

## saveG2m
#
## Writes the gene2mesh or mesh2gene data to a file. If the weight dictionary
## is given, it writes that too using the same file path but with the added
## extension '.wts'.
#
## arg, fp, filepath being written to
## arg, g2m, gene2mesh (or mesh2gene) dict
## arg, wts, weight for the gene-mesh associations
#
def saveG2m(fp, g2m, wts=None, tab=False, gnames=None):
	with open(fp, 'w') as f:
		for k, v in g2m.items():
			if len(v) <= 1:
				continue
			if tab:
				# This was added for Charles' mesh2gene data and can now
				# probably be deleted, the gnames argument above can also
				# be deleted
				print >> f, (str(k) + '\t' + '\t'.join(map(lambda x: gnames[x], filter(lambda x: gnames.has_key(x), v))))
			else:
				print >> f, (str(k) + '|' + '|'.join(v))

	if wts:
		with open(fp + '.wts', 'w') as f:
			for gene in wts.keys(): # Dict of dicts
				for term in wts[gene].keys():
					print >> f, (str(gene) + '|' + str(term) + '|' + 
								str(wts[gene][term]))

def readG2m(fp):
	import os

	g2m = {}	# gene2mesh / mesh2gene dict
	wts = None	# weights

	# Reads each line in the file and splits the pubmed IDs and MeSH terms
	with open(fp, 'r') as fh:
		for l in fh:
			# m[0] = gene, everything else = MeSH terms (opposite for m2g)
			m = l.strip().split('|')
			g2m[m[0]] = m[1:]#map(int, m[1:])

	# A weights file should probably maybe more than likely exist...?
	if os.path.exists(fp + '.wts'):
		with open(fp + '.wts', 'r') as fh:
			wts = {}
			for l in fh:
				# m[0] = gene, m[1] = term, m[2] = weight
				m = l.strip().split('|')
				if wts.has_key(m[0]):
					wts[m[0]][m[1]] = m[2]
				else:
					wts[m[0]] = {}
					wts[m[0]][m[1]] = m[2]

	return (g2m, wts)

## makeG2m
#
## Creates the gene -> MeSH term mapping and returns the result as a dict. 
#
## 08/01/2014 - Now works a bit differently. First, the dict key/values are
## reversed (see below). Genes are now only associated with a MeSH term if they
## are referenced by a minimum of two publications. And finally, the function
## now requires MeSH tree data--if a gene is associated with two MeSH terms,
## one of which is a child of another, the most granular MeSH term is chosen
## (as it is the closure of the other) and the ancestor term is discarded. 
#
## arg, g2p, gene2pubmed dict
## arg, p2m, pubmed2mesh dict
## arg, tree, MeSH tree data
## arg, m2g, OPTIONAL argument, if true the function makes a 
##			 mesh2gene dict instead of gene2mesh
## ret, gene2mesh dict, keys are terms, values are all genes associated with 
##		that term (used to be the other way around, but it's easier to insert 
##		data into the DB this way)
#
def makeG2m(g2p, p2m, tree, make_m2g=True, weight=False, closure=False):
	from collections import defaultdict
	if (not g2p) or (not p2m): 
		return None

	# dict of gene -> dicts, second dict is mesh term -> int (a count)
	g2m_count = defaultdict(lambda: defaultdict(int))
	g2m = defaultdict(set)
	m2g = defaultdict(set)	# mesh --> gene set
	# Traverse the gene2pubmed dict. As a reminder, g = a particular gene and
	# p = [set of pubmed IDs]
	for gene, pubs in g2p.items():
		for p in pubs:
			for m in p2m[p]:
				g2m_count[gene][m] += 1 # gene -> mesh count

	# Checks the counts for gene -> mesh associations, anything less than two
	# is discarded. Creates a g2m mapping
	for gene, mdict in g2m_count.items():
		for mesh, count in mdict.items():
			if count > 1:
				if g2m_count[gene][mesh] > 1:
					# add the MeSH to this gene
					g2m[gene].add(mesh)
					# add the term to all ancestor nodes (closure)
					for anc in tree[mesh]['ancestors']:
						if anc not in g2m[gene]:
							g2m[gene].add(anc)
							# alter the count too since we calc weights later on
							g2m_count[gene][anc] += 1

	# Create weights which will later be added to the geneset values. The 
	# weight is simply normalized percent publications the term-gene 
	# association is found in.
	if weight:
		wmin = 9999.0
		wmax = 0.0
		publen = len(p2m.keys())
		for gene in g2m_count.keys():
			for term in g2m_count[gene].keys():
				if g2m_count[gene][term] > 1:
					w = float(g2m_count[gene][term]) / float(publen)
					g2m_count[gene][term] = w

					if w < wmin: 
						wmin = w
					if w > wmax:
						wmax = w

		# Normalization
		for gene in g2m_count.keys():
			for term in g2m_count[gene].keys():
				g2m_count[gene][term] = (g2m_count[gene][term] - wmin) / float(wmax - wmin)

	if not make_m2g:
		if weight:
			return (g2m, g2m_count)
		else:
			return g2m

	# Convert the gene2mesh dict to mesh2gene--makes it easier to add 
	# everything to the DB
	for gene, meshes in g2m.items():
		for mesh in meshes:
			m2g[mesh].add(gene)

	if weight:
		return (m2g, g2m_count)
	else:
		return m2g

def loadMeshData(fp):
	with open(fp, 'r') as fh:
		lines = [l.strip() for l in fh]

	return lines

def parseMeshData(lines):
	## Returns indices for each location of *NEWRECORD in the lines list
	inds = [i for i, j in enumerate(lines) if j == '*NEWRECORD']

	## Slices the list of lines into individual records using the indices
	## of each *NEWRECORD string, and then appends the last record to the list
	## Fuck, this is some ugly python
	recs = [lines[inds[i]:inds[i + 1] - 1] for i in range(len(inds) - 1)]
	recs.append(lines[inds[-1]:])
	
	## Dict where key = term and the values are another dict (keys in parens): 
	## MeSH heading (MH), description (MS), ID (UI), tree node ID (MN)
	terms = {} 

	for r in recs:
		data = {'MN' : list(), 'nodes' : list()}

		for s in r:
			## MH: MeSH Heading, i.e. the MeSH term
			## MS: Scope note, a description of the term
			## UI: unique ID
			if s[:3] == 'MH ' or s[:2] == 'MS ' or s[:2] == 'UI ':
			   data[s[:2]] = s.split(' = ')[1]

			## Tree number, the ID of this term's node in the MeSH tree. There
			## may be more than one, and the ID includes the terms ancestors
			elif s[:3] == 'MN ':
				ts = s.split(' = ')[1]

				data['MN'].append(ts)
				# Should be safe as MH always comes before MN in the MeSH file...
				# Associates the mesh term (MH) with the newly added tree number
				#treenums[data['MN'][-1]] = data['MH']

				## Generate all ancestral nodes
				## e.g A01.111.236 --> [A01, A01.111, A01.111.236]
				ts = ts.split('.')
				ts = map(lambda i: '.'.join(ts[:i]), range(1, 1 + len(ts)))

				data['nodes'].extend(ts)

		terms[data['MH']] = data
	
	return terms

def addRootPath(path):
	"""
	Helper function that checks if the root letter in a MeSH tree path exists.
	If it doesn't exist, the letter is prepended to the path list.

	args:
		list, node path list

	ret:
		list, node path list with the root letter of the tree added
	"""

	if not path:
		return path

	if len(path[0]) == 1 and path[0].isalpha():
		return path

	letter = path[0][:1]

	return [letter] + path


def buildMeshTrees(terms):
	"""
	Builds each of the mesh trees (A - N, V, & Z) from node IDs, returning the
	root of the tree. Each node in the tree contains it's path and MeSH term.

	args:
		dict, term data generated from parseMeshData()

	ret:
		Tree, root of the newly build MeSH tree
	"""	

	nodes = []

	## Separate terms that have multiple node IDs
	for term, v in terms.items():
		if not v['MN']:
			continue

		for nid in v['MN']:
			nodes.append((term, nid))

	nodes = sorted(nodes, key=lambda n: n[1][:1])
	grps = [[]]

	## Group by MeSH tree letter (e.g. A, B, etc...) so tree building occurs
	## one subtree at a time
	for n in range(len(nodes)):
		if n > 0 and nodes[n][1][:1] != nodes[n - 1][1][:1]:
			grps.append([])

		grps[-1].append(nodes[n])
	
	mtree = tree()
	term2node = dd(list)

	## This actually builds each subtree
	for grp in grps:
		subtree = tree()

		for node in grp:
			path = node[1].split('.')

			term2node[node[0]].append(node[1])
			
			subtree.addNode(path)
			subtree.addValue(path, 'term', node[0])

		letter = grp[0][1][:1]
		mtree[letter] = subtree

	return (mtree, term2node)


if __name__ == '__main__':

	dat = parseMeshData(loadMeshData('/home/csi/r/reynolds/gw_mesh/data/mesh2014.bin'))
	tree, term2node = buildMeshTrees(dat)

	print addRootPath(['F01', '222', '333'])
	#mtree = tree()

	#mtree.term.shit = 'lol'
	#mtree.term.uid = '1111'
	#mtree.addNode([1, 2, 3])
	#mtree.addNode([1, 2, 4])
	#mtree.addNode([1, 2, 5])

	#print mtree.getChildren([1,2])
	#print mtree.getChildren([1,2,3])
	#print mtree.getChildren([1])

	#print mtree.getNode([1]).path
	#print mtree.getNode([1,2]).path
	#print mtree.getNode([1,2,4]).path
	#print mtree.getNode([1,2,5]).path
	#mtree.addNode([00, 01, 11])
	#mtree.addValue([1,2], 'key', 'val')

	#print mtree.getValue([1,2], 'key')
	#print mtree.term.shit
	#print mtree.term.uid
	#print mtree

		# Each MeSH term stored in a dict where key = term and val = relevant data
## loadMeshData
#
## Loads all the MeSH data from the ASCII format, generating trees and closures
## for each term as well. Heavily based off Jeremy Jay's MeshTermLoader
## implementation, so creds to him for most of it. Returns two dicts: one of
## each MeSH term and any important data associated with that term. The
## second dict is the MeSH tree and closures for each term. Will be useful if
## we want to filter other terms below or above another term in the tree
## hierarchy.
## TODO: This monolith of a function should probably be modularized a bit more.
## Also, may want to look into some additional descriptor elements like
## (PRINT) ENTRY, FX (forward cross reference), etc...
#
## arg, fp,  path to the MeSH ASCII file
## arg, nmap, OPTIONAL 
## ret, dict of MeSH terms (keys) and their data (vals)
## ret, dict of term closures including ancestor, parent, and child terms
#

def loadMeshData_DEPRECATED(fp, nmap=False):
	with open(fp, 'r') as fh:
		# Remove \r\n and get a list of the stripped lines
		lines = [l.strip() for l in fh]

	# Returns indices for each location of *NEWRECORD in the lines list
	inds = [i for i, j in enumerate(lines) if j == '*NEWRECORD']

	# Slices the list of lines into individual records using the indices (inds)
	# of each *NEWRECORD string, and then appends the last record to the list
	# Fuck, this is some ugly python
	recs = [lines[inds[i]:inds[i + 1] - 1] for i in range(len(inds) - 1)]
	recs.append(lines[inds[-1]:])

	terms = {} # Dict. where key = term and val = data from MeSH dump
	treenums = {} # Dict. where key = tree number and val = MeSH term
	mtree = {} # Dict. of MeSH tree structure and closure, each key is a term

	import re
	# Iterate over all the records, extracting only the necessary information and
	# storing that in a dict
	for r in recs:
		data = {'MN' : list(), 'MS' : '', 'nodes' : list()}

		# for each descriptor in the record, store relevant information
		for s in r:
			if s[:3] == 'MH ': # MeSH Heading 
				data['MH'] = s.split(' = ')[1]

			elif s[:3] == 'MS ': # Scope note, basically a description
				data['MS'] = s.split(' = ')[1]

			elif s[:3] == 'UI ': # Unique ID
				data['UI'] = s.split(' = ')[1]

			elif s[:3] == 'MN ': # MeSH tree number, there may be more than one
				ts = s.split(' = ')[1]
				data['MN'].append(ts)
				# Should be safe as MH always comes before MN in the MeSH file...
				# Associates the mesh term (MH) with the newly added tree number
				treenums[data['MN'][-1]] = data['MH']

				# Create a list of each previous node in the tree (ancestors)
				# e.g A01.111.236 --> [A01, A01.111, A01.111.236]
				data['nodes'].extend(( # Lines longer than 80 chars are of the devil 
					[ts[:i] for i in xrange(len(ts)) if ts.find('.', i) == i]))
				data['nodes'].append(ts)

			elif s[:3] == 'UI ': # Unique identifier
				data['UI'] = s.split(' = ')[1]

		# Each MeSH term stored in a dict where key = term and val = relevant data
		terms[data['MH']] = data

	# Maps MeSH tree node numbers (e.g A01.141) to terms
	node_map = {}
	# Generate the MeSH tree (sorta) while concurrently generating closures for
	# each term. I guess this could also be made by dl'ing the actual
	# MeSH trees in ASCII format... 
	for t in terms.keys():
		mtree[t] = {'children' : set(), 'parents' : set(), 
					'ancestors' : set(), 'node' : set()}

		# Store the node
		mtree[t]['node'].update(terms[t]['MN'])

		for mn in terms[t]['MN']:
			node_map[mn] = t

		# Retrieves all the parents for each tree number. Parents are just the
		# tree number minus tha last node (three numbers)
		pars = [s.rsplit('.', 1)[0] for s in terms[t]['MN']]

		# The tree (and closures) are made using terms and not the actual 
		# tree numbers
		for node in terms[t]['nodes']:
			mtree[t]['ancestors'].add(treenums[node]) # Add each ancestor node
			# added 8/3/2014, remove the term itself from the ancestors list;
			# I don't think a term is its own ancestor and this fucks up 
			# gene2mesh creation
			mtree[t]['ancestors'].discard(t)

			# If it's a parent, add it to the parents set
			if node in pars:
				mtree[t]['parents'].add(treenums[node])

	# Then, add all the children for each term
	for t in mtree.keys():
		for p in mtree[t]['parents']:
			mtree[p]['children'].add(t)

	if nmap:
		return (terms, mtree, node_map)

	return (terms, mtree)

