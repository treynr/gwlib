#!/usr/bin/env python
# -*- coding: utf-8 -*-

## file: test_db.py
## desc: Unit tests for db.py.
## auth: TR

import datetime
import numpy as np
import pandas as pd
import pytest
import warnings

## Ignore binary wheel warnings from psycopg2
warnings.filterwarnings('ignore', module='psycopg2')

from gwlib import config
from gwlib import db

config.load_config('tests/test.cfg')

@pytest.fixture(scope='module')
def connect():

    return db.connect(
        config.get_db('host'),
        config.get_db('database'),
        config.get_db('user'),
        config.get_db('password'),
        config.get_db('port')
    )

def test_PooledConnection_1():
    with db.PooledConnection(
        host=config.get_db('host'),
        dbname=config.get_db('database'),
        user=config.get_db('user'),
        password=config.get_db('password'),
        port=config.get_db('port')
    ) as pc:

        cursor = pc.cursor()

        cursor.execute('''SELECT 1;''')

        assert cursor.fetchone()[0] == 1

def test_PooledConnection_2():

    pc = db.PooledConnection(
        host=config.get_db('host'),
        dbname=config.get_db('database'),
        user=config.get_db('user'),
        password=config.get_db('password'),
        port=config.get_db('port')
    )

    conn = pc.getconn()
    cursor = conn.cursor()

    cursor.execute('''SELECT 1;''')

    assert cursor.fetchone()[0] == 1

def test_PooledCursor(connect):

    with db.PooledCursor() as cursor:

        cursor.execute('''SELECT 1;''')

        assert cursor.fetchone()[0] == 1

def test_connect(connect):
    success, err = connect
    assert success

def test_get_species(connect):

    res = db.get_species()

    assert res.equals(pd.DataFrame(
        [
            ['Mus musculus', 1],
            ['Homo sapiens', 2],
            ['Rattus norvegicus', 3]
        ],
        columns=['sp_name', 'sp_id']
    ))

def test_get_species_with_taxid(connect):

    res = db.get_species_with_taxid()

    assert res.equals(pd.DataFrame(
        [
            ['Mus musculus', 1, 10090],
            ['Homo sapiens', 2, 9606],
            ['Rattus norvegicus', 3, 10116]
        ],
        columns=['sp_name', 'sp_id', 'sp_taxid']
    ))

#def test_get_species_by_taxid():
#
#    res = db.get_species_by_taxid()
#
#    assert res == {
#        10090: 1,
#        9606: 2,
#        10116: 3
#    }

def test_get_attributions(connect):

    res = db.get_attributions()

    assert res.equals(pd.DataFrame(
        [
            ['GO', 8],
            ['GWAS', 6],
            ['CTD', 2]
        ],
        columns=['at_abbrev', 'at_id']
    ))

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


def test_get_gene_ids_1(connect):

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'])

    assert res.equals(pd.DataFrame(
        [
            ['MGI:108511', 5105],
            ['HGNC:7189', 66945],
            ['ENSRNOG00000018700', 124272]
        ],
        columns=['ode_ref_id', 'ode_gene_id']
    ))

def test_get_gene_ids_2(connect):

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'], sp_id=1)

    assert res.equals(pd.DataFrame(
        [
            ['MGI:108511', 5105],
        ],
        columns=['ode_ref_id', 'ode_gene_id']
    ))

def test_get_gene_ids_3(connect):

    res = db.get_gene_ids(['MGI:108511', 'HGNC:7189', 'ENSRNOG00000018700'], gdb_id=11)

    assert res.equals(pd.DataFrame(
        [
            ['HGNC:7189', 66945],
        ],
        columns=['ode_ref_id', 'ode_gene_id']
    ))

def test_get_gene_ids_4(connect):

    res = db.get_gene_ids(['MOBP'], gdb_id=7, sp_id=2)

    assert res.equals(pd.DataFrame(
        [
            ['MOBP', 66945]
        ],
        columns=['ode_ref_id', 'ode_gene_id']
    ))

def test_get_gene_ids_5(connect):

    res = db.get_gene_ids(['mobp'], gdb_id=7, sp_id=2)

    assert res.empty

def test_get_species_genes(connect):

    res = db.get_species_genes(1, symbol=False)

    assert res.equals(pd.DataFrame(
        [
            ['MGI:108511', 5105],
            ['Mobp', 5105],
            ['ENSMUSG00000032517', 5105],
            ['MGI:87948', 73],
            ['MGI:88336', 323]
        ],
        columns=['ode_ref_id', 'ode_gene_id']
    ))

def test_get_gene_refs_1(connect):

    res = db.get_gene_refs([5105])

    assert res.equals(pd.DataFrame(
        [
            [5105, 'ENSMUSG00000032517'],
            [5105, 'MGI:108511'],
            [5105, 'Mobp'],
        ],
        columns=['ode_gene_id', 'ode_ref_id']
    ))

def test_get_gene_refs_2(connect):

    res = db.get_gene_refs([5105, 66945, 124272], type_id=7)

    assert res.equals(pd.DataFrame(
        [
            [5105, 'Mobp'],
            [66945, 'MOBP'],
            [124272, 'Mobp'],
        ],
        columns=['ode_gene_id', 'ode_ref_id']
    ))

"""
def test_get_preferred_gene_refs():

    res = db.get_preferred_gene_refs([5105, 66945, 124272])

    assert res == {
        5105: 'Mobp',
        66945: 'MOBP',
        124272: 'Mobp'
    }
"""

def test_get_genesets(connect):

    res = db.get_genesets([185236])

    assert len(res.index) == 1
    assert res.loc[0, 'gs_id'] == 185236
    assert res.loc[0, 'gs_abbreviation'] == 'GO:0006184'

def test_get_geneset_ids_by_tier_1(connect):

    res = db.get_geneset_ids(tiers=[1])

    assert len(res) == 2
    assert (np.sort(res) == np.sort(np.array([185236, 270867]))).all()

def test_get_geneset_ids_by_tier_2(connect):

    res = db.get_geneset_ids(tiers=[1, 3])

    assert len(res) == 3
    assert (np.sort(res) == np.sort(np.array([185236, 270867, 219234]))).all()

def test_get_geneset_ids_by_attribute_1(connect):

    res = db.get_geneset_ids(at_id=2)

    assert len(res) == 0

def test_get_geneset_ids_by_attribute_2(connect):

    res = db.get_geneset_ids(at_id=8)

    assert len(res) == 1
    assert res == np.array([185236])

def test_get_geneset_ids_by_attribute_3(connect):

    res = db.get_geneset_ids(at_id=6, sp_id=2)

    assert len(res) == 1
    assert res == np.array([270867])

def test_get_geneset_values(connect):

    res = db.get_geneset_values([219234, 185236])

    assert res.equals(pd.DataFrame(
        [
            [185236, 73, 1.0],
            [185236, 323, 1.0],
            [219234, 66945, 2.6],
        ],
        columns=['gs_id', 'ode_gene_id', 'gsv_value']
    ))

def test_get_gene_homologs(connect):

    res = db.get_gene_homologs([5105, 124272, 66945])

    assert res.equals(pd.DataFrame(
        [
            [5105, 32040],
            [124272, 32040],
            [66945, 32040]
        ],
        columns=['ode_gene_id', 'hom_id',]
    ))

def test_get_publication(connect):

    res = db.get_publication('17440432')

    assert res == 2312

def test_get_publication_pmid(connect):

    res = db.get_publication_pmid(2312)

    assert res == '17440432'

def test_get_geneset_pmids(connect):

    res = db.get_geneset_pmids([219234, 270867])

    assert res.equals(pd.DataFrame(
        [
            [219234, '17440432'],
            [270867, '26077402']
        ],
        columns=['gs_id', 'pub_pubmed',]
    ))

def test_get_geneset_text(connect):

    res = db.get_geneset_text([219234, 270867])

    assert len(res.index) == 2
    assert res.gs_id.notnull().all()
    assert res.gs_name.notnull().all()
    assert res.gs_description.notnull().all()
    assert res.gs_abbreviation.notnull().all()

def test_get_gene_types(connect):

    res = db.get_gene_types()

    assert res.equals(pd.DataFrame(
        [
            ['Ensembl Gene', 2],
            ['Gene Symbol', 7],
            ['MGI', 10],
            ['HGNC', 11],
            ['RGD', 12],
            ['Variant', 25]
        ],
        columns=['gdb_name', 'gdb_id',]
    ))

def test_get_gene_types_short(connect):

    res = db.get_gene_types(short=True)

    assert res.equals(pd.DataFrame(
        [
            ['ensembl', 2],
            ['symbol', 7],
            ['mgi', 10],
            ['hgnc', 11],
            ['rgd', 12],
            ['variant', 25]
        ],
        columns=['gdb_name', 'gdb_id',]
    ))

def test_get_threshold_types_1(connect):

    res = db.get_threshold_types()

    assert res.equals(pd.DataFrame(
        zip(
            ['P-value', 'Q-value', 'Binary', 'Correlation', 'Effect'],
            [1, 2, 3, 4, 5]
        ),
        columns=['type_name', 'gs_threshold_type']
    ))

def test_get_threshold_types_2(connect):

    res = db.get_threshold_types(lower=True)

    assert res.equals(pd.DataFrame(
        zip(
            ['p-value', 'q-value', 'binary', 'correlation', 'effect'],
            [1, 2, 3, 4, 5]
        ),
        columns=['type_name', 'gs_threshold_type']
    ))

def test_get_platforms(connect):

    res = db.get_platforms()

    print(res)
    assert res.equals(pd.DataFrame(
        [
            [92,  'Affymetrix Human Gene 1.0 ST', 'HuGene-1_0-st', 'GPL10739'],
            [82, 'Illumina MouseWG-6 v2.0', 'MouseWG-6 v2.0', 'GPL6887'],
            [1, 'Affymetrix Murine Genome U74A', 'MG_U74A', 'GPL32']
        ],
        columns=['pf_id', 'pf_name', 'pf_shortname', 'pf_gpl_id']
    ))

def test_get_platform_probes(connect):

    res = db.get_platform_probes(1, ['104722_at'])

    assert res.equals(pd.DataFrame(
        [
            ['104722_at',  4723]
        ],
        columns=['prb_ref_id', 'prb_id']
    ))

def test_get_all_platform_probes(connect):

    res = db.get_all_platform_probes(1)

    assert res.equals(pd.DataFrame(
        [
            ['104722_at',  4723],
            ['92624_r_at',  5213]
        ],
        columns=['prb_ref_id', 'prb_id']
    ))

def test_get_probe_genes(connect):

    res = db.get_probe_genes([4723, 5213])

    assert res.equals(pd.DataFrame(
        [
            [4723, 6566],
            [5213, 1658]
        ],
        columns=['prb_id', 'ode_gene_id']
    ))

def test_get_group_by_name_1(connect):

    res = db.get_group_by_name('A group name')

    assert res == 1

def test_get_group_by_name_2(connect):

    res = db.get_group_by_name('A Group Name')

    assert res is None

def test_get_geneset_annotations(connect):

    res = db.get_geneset_annotations([185236])

    assert res.equals(pd.DataFrame(
        [
            [185236, 60455, 'GO:0006184'],
        ],
        columns=['gs_id', 'ont_id', 'ont_ref_id']
    ))

def test_get_ontology_ids_by_refs(connect):

    res = db.get_ontology_ids_by_refs(['GO:0006184', 'GO:0043088'])

    assert res.equals(pd.DataFrame(
        [
            ['GO:0006184', 60455],
            ['GO:0043088', 50019],
        ],
        columns=['ont_ref_id', 'ont_id']
    ))

def test_get_ontologies(connect):

    res = db.get_ontologies()

    assert res.equals(pd.DataFrame(
        [
            [1, 'Gene Ontology', 'GO', 'url', datetime.date.today()],
            [4, 'MeSH Terms', 'MESH', 'url', datetime.date.today()],
        ],
        columns=['ontdb_id', 'ontdb_name', 'ontdb_prefix', 'ontdb_linkout_url', 'ontdb_date']
    ))

def test_get_ontology_terms_by_ontdb(connect):

    res = db.get_ontology_terms_by_ontdb(1)

    assert res.equals(pd.DataFrame(
        [
	    [60455, 'GO:0006184', 'GTP catabolic process', 'The chemical...', 4, 2, 1],
	    [50019, 'GO:0043088', 'regulation of Cdc42 GTPase activity', 'Any process...', 1, 2, 1],
        ],
        columns=[
            'ont_id',
            'ont_ref_id',
            'ont_name',
            'ont_description',
            'ont_children',
            'ont_parents',
            'ontdb_id'
        ]
    ))

"""
def test_insert_geneset_values():

    db.insert_geneset_values([
        (1, 100, 1.2, 'Mobp', True),
        (1, 200, -1.2, 'Aplp2', True)
    ])

    values = db.get_geneset_values([1])
    values = sorted(values, key=lambda g: g['ode_gene_id'])

    assert values[0]['gs_id'] == 1
    assert values[0]['ode_gene_id'] == 100
    assert values[0]['gsv_value'] == 1.2
    assert values[1]['gs_id'] == 1
    assert values[1]['ode_gene_id'] == 200
    assert values[1]['gsv_value'] == -1.2
"""
