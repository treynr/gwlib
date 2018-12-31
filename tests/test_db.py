#!/usr/bin/env python

## file: test_db.py
## desc: Unit tests for db.py.
## auth: TR

import warnings

## Ignore binary wheel warnings from psycopg2
warnings.filterwarnings('ignore', module='psycopg2')

from gwlib import config
from gwlib import db

config.load_config('tests/test.cfg')

def test_connect():

    success, err = db.connect(
        config.get_db('host'),
        config.get_db('database'),
        config.get_db('user'),
        config.get_db('password'),
        config.get_db('port')
    )

    assert success

def test_get_species():

    res = db.get_species()

    assert res == {
        'Mus musculus': 1,
        'Homo sapiens': 2,
        'Rattus norvegicus': 3
    }

def test_get_species_with_taxid():

    res = db.get_species_with_taxid()

    assert res == [
        {'sp_id': 1, 'sp_taxid': 10090, 'sp_name': 'Mus musculus'},
        {'sp_id': 2, 'sp_taxid': 9606, 'sp_name': 'Homo sapiens'},
        {'sp_id': 3, 'sp_taxid': 10116, 'sp_name': 'Rattus norvegicus'}
    ]

def test_get_species_by_taxid():

    res = db.get_species_by_taxid()

    assert res == {
        10090: 1,
        9606: 2,
        10116: 3
    }

def test_get_attributions():

    res = db.get_attributions()

    assert res == {
        'GO': 8,
        'GWAS': 6,
        'CTD': 2
    }

#    100 INSERT INTO odestatic.genedb (gdb_id, gdb_name, gdb_shortname, sp_id)
#  1 VALUES      (2, 'Ensembl Gene', 'ensembl', 0),
#  2             (7, 'Gene Symbol', 'symbol', 0),
#  3             (10, 'MGI', 'mgi', 1),
#  4             (11, 'HGNC', 'hgnc', 2),
#  5             (12, 'RGD', 'rgd', 3),
#  6             (25, 'Variant', 'variant', 0);
#  7
#  8 INSERT INTO extsrc.gene (ode_gene_id, ode_ref_id, gdb_id, sp_id, ode_pref)
#  9 VALUES      (5105, 'MGI:108511', 10, 1, FALSE),
# 10             (5105, 'Mobp', 7, 1, TRUE),
# 11             (5105, 'ENSMUSG00000032517', 2, 1, FALSE),
# 12             (66945, 'HGNC:7189', 11, 2, FALSE),
# 13             (66945, 'MOBP', 7, 2, TRUE),
# 14             (66945, 'ENSG00000168314', 2, 2, FALSE),
# 15             (124272, 'RGD3101', 12, 3, FALSE),
# 16             (124272, 'Mobp', 7, 3, TRUE),
# 17             (124272, 'ENSRNOG00000018700', 2, 3, FALSE);


def test_get_gene_ids_1():

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'])

    assert res == {
        'MGI:108511': 5105,
        'HGNC:7189': 66945,
        'ENSRNOG00000018700': 124272
    }

def test_get_gene_ids_2():

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'], sp_id=1)

    assert res == {'MGI:108511': 5105}

def test_get_gene_ids_3():

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'], gdb_id=11)

    assert res == {'HGNC:7189': 66945}

def test_get_gene_ids_4():

    res = db.get_gene_ids(['MOBP'], gdb_id=7, sp_id=2)

    assert res == {'MOBP': 66945}

def test_get_gene_ids_5():

    res = db.get_gene_ids(['mobp'], gdb_id=7, sp_id=2)

    assert res == {}

def test_get_species_genes():

    res = db.get_species_genes(1, symbol=False)

    assert res == {
        'MGI:108511': 5105,
        'Mobp': 5105,
        'ENSMUSG00000032517': 5105,
        'MGI:87948': 73,
        'MGI:88336': 323
    }

def test_get_gene_refs_1():

    res = db.get_gene_refs([5105])

    assert 5105 in res
    assert 'MGI:108511' in res[5105]
    assert 'Mobp' in res[5105]
    assert 'ENSMUSG00000032517' in res[5105]

def test_get_gene_refs_2():

    res = db.get_gene_refs([5105, 66945, 124272], type_id=7)

    assert 5105 in res
    assert 66945 in res
    assert 124272 in res
    assert res[5105] == ['Mobp']
    assert res[66945] == ['MOBP']
    assert res[124272] == ['Mobp']

def test_get_preferred_gene_refs():

    res = db.get_preferred_gene_refs([5105, 66945, 124272])

    assert res == {
        5105: 'Mobp',
        66945: 'MOBP',
        124272: 'Mobp'
    }

def test_get_genesets():

    res = db.get_genesets([185236])

    assert len(res) == 1
    assert res[0]['gs_id'] == 185236
    assert res[0]['gs_abbreviation'] == 'GO:0006184'

def test_get_geneset_ids_by_tier_1():

    res = db.get_geneset_ids_by_tier(tiers=[1])

    assert len(res) == 2
    assert set(res) == set([185236, 270867])

def test_get_geneset_ids_by_tier_2():

    res = db.get_geneset_ids_by_tier(tiers=[1, 3])

    assert len(res) == 3
    assert set(res) == set([185236, 270867, 219234])

def test_get_geneset_ids_by_attribute_1():

    res = db.get_geneset_ids_by_attribute(attrib=2)

    assert len(res) == 0

def test_get_geneset_ids_by_attribute_2():

    res = db.get_geneset_ids_by_attribute(attrib=8)

    assert len(res) == 1
    assert res == [185236]

def test_get_geneset_ids_by_attribute_3():

    res = db.get_geneset_ids_by_attribute(attrib=6, sp_id=2)

    assert len(res) == 1
    assert res == [270867]

def test_get_gene_homologs():

    res = db.get_gene_homologs([5105, 124272, 66945])

    assert res == {
        5105: 32040,
        124272: 32040,
        66945: 32040
    }

def test_get_publication():

    res = db.get_publication('17440432')

    assert res == 2312

def test_get_publication_pmid():

    res = db.get_publication_pmid(2312)

    assert res == '17440432'

def test_get_geneset_pmids():

    res = db.get_geneset_pmids([219234, 270867])

    assert res == {
        219234: '17440432',
        270867: '26077402'
    }

def test_get_geneset_metadata():

    res = db.get_geneset_metadata([219234, 270867])

    assert len(res) == 2
    assert 'gs_id' in res[0]
    assert 'gs_name' in res[0]
    assert 'gs_description' in res[0]
    assert 'gs_abbreviation' in res[0]

def test_get_gene_types_1():

    res = db.get_gene_types()

    assert res == {
        'Ensembl Gene': 2,
        'Gene Symbol': 7,
        'MGI': 10,
        'HGNC': 11,
        'RGD': 12,
        'Variant': 25,
    }

def test_get_gene_types_2():

    res = db.get_gene_types(short=True)

    assert res == {
        'ensembl': 2,
        'symbol': 7,
        'mgi': 10,
        'hgnc': 11,
        'rgd': 12,
        'variant': 25,
    }

