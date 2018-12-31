
-- file: setup-db.py
-- desc: Sets up basic tables and sample datasets for testing. Doesn't bother with
--       table constraints. Most tables are minimal versions of the one used in
--       production.
-- auth: TR
--

CREATE SCHEMA extsrc;
CREATE SCHEMA odestatic;
CREATE SCHEMA production;

-- Minimal species table, 3 of 7 columns represented
--
CREATE TABLE odestatic.species (

    sp_id    SERIAL NOT NULL,
    sp_name  VARCHAR NOT NULL,
    sp_taxid INTEGER NOT NULL
);

CREATE TABLE odestatic.attribution (

    at_id          SERIAL NOT NULL,
    at_abbrev      VARCHAR(32),
    gs_attribution TEXT
);

-- Minimal genedb table, 4 of 7 columns represented
--
CREATE TABLE odestatic.genedb (
    gdb_id          INTEGER NOT NULL,
    gdb_name        VARCHAR NOT NULL,
    sp_id           INTEGER,
    gdb_shortname   VARCHAR
);

-- Minimal gene table, 5 of 7 columns represented
--
CREATE TABLE extsrc.gene (
    ode_gene_id      BIGSERIAL NOT NULL,
    ode_ref_id       VARCHAR NOT NULL,
    gdb_id           INTEGER,
    sp_id            INTEGER,
    ode_pref         BOOLEAN DEFAULT FALSE NOT NULL
);

-- Minimal gene table, 13 of 27 columns represented
--
CREATE TABLE production.geneset (

    gs_id           BIGSERIAL NOT NULL,
    gs_name         VARCHAR NOT NULL,
    gs_abbreviation VARCHAR NOT NULL,
    gs_description  VARCHAR NOT NULL,
    cur_id          INTEGER NOT NULL,
    sp_id           INTEGER NOT NULL,
    gs_count        INTEGER DEFAULT 0,
    gs_gene_id_type INTEGER NOT NULL,
    gs_created      TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    gs_updated      TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
    gs_status       VARCHAR DEFAULT 'normal',
    gs_attribution  INTEGER,
    pub_id          INTEGER
);

-- Minimal publication table, 2 of 11 columns represented
--
CREATE TABLE production.publication (
    pub_id     SERIAL NOT NULL,
    pub_pubmed VARCHAR
);

-- Minimal geneset_value table, 5 of 8 columns represented
--
CREATE TABLE extsrc.geneset_value (
    gs_id            BIGINT NOT NULL,
    ode_gene_id      BIGINT NOT NULL,
    gsv_value        NUMERIC,
    gsv_in_threshold BOOLEAN DEFAULT false NOT NULL,
    gsv_date         DATE DEFAULT NOW()
);

-- Minimal homology table, 5 of 6 columns represented
--
CREATE TABLE extsrc.homology (
    hom_id          INTEGER NOT NULL,
    hom_source_id   VARCHAR NOT NULL,
    hom_source_name VARCHAR,
    ode_gene_id     BIGINT NOT NULL,
    sp_id           INTEGER
);

CREATE TABLE extsrc.ontology (
    ont_id          SERIAL NOT NULL,
    ont_ref_id      VARCHAR,
    ont_name        VARCHAR NOT NULL,
    ont_description VARCHAR,
    ont_children    INTEGER DEFAULT 0,
    ont_parents     INTEGER DEFAULT 0,
    ontdb_id        INTEGER
);

CREATE TABLE extsrc.probe2gene (
    prb_id      BIGINT NOT NULL,
    ode_gene_id BIGINT NOT NULL
);

CREATE TABLE odestatic.probe (
    prb_id      BIGINT NOT NULL,
    prb_ref_id  VARCHAR,
    pf_id       INTEGER NOT NULL
);

INSERT INTO odestatic.species (sp_name, sp_taxid) 
VALUES      ('Mus musculus', 10090), 
            ('Homo sapiens', 9606),
            ('Rattus norvegicus', 10116);

INSERT INTO odestatic.attribution (at_id, at_abbrev, gs_attribution)
VALUES      (8, 'GO', 'Gene Ontology'),
            (6, 'GWAS', 'NHGRI-EBI GWAS Catalog. A catalog of published genome-wide association studies.'),
            (2, 'CTD', 'Curated chemical gene interaction data from the Comparative Toxicogenomics Database.');

INSERT INTO odestatic.genedb (gdb_id, gdb_name, gdb_shortname, sp_id)
VALUES      (2, 'Ensembl Gene', 'ensembl', 0),
            (7, 'Gene Symbol', 'symbol', 0),
            (10, 'MGI', 'mgi', 1),
            (11, 'HGNC', 'hgnc', 2),
            (12, 'RGD', 'rgd', 3),
            (25, 'Variant', 'variant', 0);

INSERT INTO extsrc.gene (ode_gene_id, ode_ref_id, gdb_id, sp_id, ode_pref)
VALUES      (5105, 'MGI:108511', 10, 1, FALSE),
            (5105, 'Mobp', 7, 1, TRUE),
            (5105, 'ENSMUSG00000032517', 2, 1, FALSE),
            (66945, 'HGNC:7189', 11, 2, FALSE),
            (66945, 'MOBP', 7, 2, TRUE),
            (66945, 'ENSG00000168314', 2, 2, FALSE),
            (124272, 'RGD3101', 12, 3, FALSE),
            (124272, 'Mobp', 7, 3, TRUE),
            (124272, 'ENSRNOG00000018700', 2, 3, FALSE),
            (73, 'MGI:87948', 10, 1, FALSE),
            (323, 'MGI:88336', 10, 1, FALSE),
            (82788, 'SCAF4', 7, 2, TRUE),
            (82788, 'TPT1P1', 7, 2, TRUE);

INSERT INTO production.geneset (gs_id, gs_name, gs_abbreviation, gs_description, cur_id, sp_id, gs_gene_id_type, gs_attribution)
VALUES                         (185236, 'GO:0006184 GTP catabolic process', 'GO:0006184', 'The chemical reactions and pathways...', 1, 1, -10, 8),
                               (219234, 'Differentially Expressed in bipolar and control...', 'Bipolar twin study', '82 transcripts with a 1.3-fold change...', 3, 2, -2, NULL),
                               (270867, 'GWAS Catalog Data for rheumatoid arthritis...', 'GWAS: rheumatoid arthritis', 'List of positional candidate...', 1, 2, -7, 6);

INSERT INTO extsrc.geneset_value (gs_id, ode_gene_id, gsv_value)
VALUES                           (185236, 73, 1.0),
                                 (185236, 323, 1.0),
                                 (219234, 66945, 2.6),
                                 (270867, 82788, 0.00004),
                                 (270867, 63564, 0.00004);

INSERT INTO extsrc.homology (hom_id, hom_source_id, hom_source_name, ode_gene_id, sp_id)
VALUES                      (32040, 32040, 'Homologene', 5105, 1),
                            (32040, 32040, 'Homologene', 124272, 3),
                            (32040, 32040, 'Homologene', 66945, 2);

UPDATE production.geneset SET pub_id = 2312 WHERE gs_id = 219234;
UPDATE production.geneset SET pub_id = 7841 WHERE gs_id = 270867;

INSERT INTO production.publication (pub_id, pub_pubmed)
VALUES                             (2312, 17440432),
                                   (7841, 26077402);

