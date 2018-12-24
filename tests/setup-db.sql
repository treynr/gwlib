
---- file: setup-db.py
---- desc: Sets up basic tables and sample datasets for testing. Doesn't bother with
----       table constraints.
---- auth: TR
--

CREATE SCHEMA extsrc;
CREATE SCHEMA odestatic;
CREATE SCHEMA production;

CREATE TABLE odestatic.species (
    sp_id           SERIAL NOT NULL,
    sp_name         VARCHAR NOT NULL,
    sp_taxid        INTEGER NOT NULL,
    sp_ref_gdb_id   INTEGER,
    sp_date         DATE,
    sp_biomart_info VARCHAR,
    sp_source_data  TEXT
);

CREATE TABLE odestatic.attribution (
    at_id          SERIAL NOT NULL,
    at_abbrev      VARCHAR(32),
    gs_attribution TEXT
);

CREATE TABLE extsrc.gene (
    ode_gene_id      BIGSERIAL NOT NULL,
    ode_ref_id       VARCHAR NOT NULL,
    gdb_id           INTEGER,
    sp_id            INTEGER,
    ode_pref         BOOLEAN DEFAULT false NOT NULL,
    ode_date         DATE,
    old_ode_gene_ids BIGINT[]
);

CREATE TABLE extsrc.geneset_value (
    gs_id            BIGINT NOT NULL,
    ode_gene_id      BIGINT NOT NULL,
    gsv_value        NUMERIC,
    gsv_hits         BIGINT,
    gsv_source_list  VARCHAR[],
    gsv_value_list   NUMERIC[],
    gsv_in_threshold BOOLEAN DEFAULT false NOT NULL,
    gsv_date         DATE
);

CREATE TABLE extsrc.homology (
    hom_id          INTEGER NOT NULL,
    hom_source_id   VARCHAR NOT NULL,
    hom_source_name VARCHAR,
    ode_gene_id     BIGINT NOT NULL,
    sp_id           INTEGER,
    hom_date        DATE
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

CREATE TABLE odestatic.genedb (
    gdb_id          INTEGER NOT NULL,
    gdb_name        VARCHAR NOT NULL,
    sp_id           INTEGER,
    gdb_shortname   VARCHAR,
    gdb_date        TIMESTAMP WITHOUT TIME ZONE,
    gdb_precision   INTEGER DEFAULT 4,
    gdb_linkout_url VARCHAR
);

INSERT INTO odestatic.species (sp_name, sp_taxid) 
VALUES      ('Mus musculus', 10090), 
            ('Homo sapiens', 9606),
            ('Rattus norvegicus', 10116);
