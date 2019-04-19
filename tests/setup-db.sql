
-- file: setup-db.py
-- desc: Sets up basic tables and sample datasets for testing. Doesn't bother with
--       table constraints. Most tables are minimal versions of the one used in
--       production.
-- auth: TR
--

DROP SCHEMA IF EXISTS extsrc CASCADE;
DROP SCHEMA IF EXISTS odestatic CASCADE;
DROP SCHEMA IF EXISTS production CASCADE;

CREATE SCHEMA extsrc;
CREATE SCHEMA odestatic;
CREATE SCHEMA production;

-- pg_dump

CREATE TABLE extsrc.gene (
    ode_gene_id bigint NOT NULL,
    ode_ref_id character varying NOT NULL,
    gdb_id integer,
    sp_id integer,
    ode_pref boolean DEFAULT false NOT NULL,
    ode_date date,
    old_ode_gene_ids bigint[]
);



CREATE SEQUENCE extsrc.gene_ode_gene_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE extsrc.gene_ode_gene_id_seq OWNED BY extsrc.gene.ode_gene_id;



CREATE TABLE extsrc.geneset_ontology (
    gs_id bigint NOT NULL,
    ont_id bigint NOT NULL,
    gso_ref_type character varying NOT NULL
);



CREATE TABLE extsrc.geneset_value (
    gs_id bigint NOT NULL,
    ode_gene_id bigint NOT NULL,
    gsv_value numeric,
    gsv_hits bigint,
    gsv_source_list character varying[],
    gsv_value_list numeric[],
    gsv_in_threshold boolean DEFAULT false NOT NULL,
    gsv_date date
);



CREATE TABLE extsrc.homology (
    hom_id bigint NOT NULL,
    hom_source_id character varying NOT NULL,
    hom_source_name character varying,
    ode_gene_id bigint NOT NULL,
    sp_id integer,
    hom_date date,
    hom_source_uid smallint
);



CREATE TABLE extsrc.ontology (
    ont_id integer NOT NULL,
    ont_ref_id character varying,
    ont_name character varying NOT NULL,
    ont_description character varying,
    ont_children integer DEFAULT 0,
    ont_parents integer DEFAULT 0,
    ontdb_id integer
);



CREATE SEQUENCE extsrc.ontology_ont_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE extsrc.ontology_ont_id_seq OWNED BY extsrc.ontology.ont_id;



CREATE TABLE extsrc.probe2gene (
    prb_id bigint NOT NULL,
    ode_gene_id bigint NOT NULL
);



CREATE TABLE odestatic.genedb (
    gdb_id integer NOT NULL,
    gdb_name character varying NOT NULL,
    sp_id integer,
    gdb_shortname character varying,
    gdb_date timestamp without time zone,
    gdb_precision integer DEFAULT 4,
    gdb_linkout_url character varying
);



CREATE TABLE odestatic.attribution (
    at_id integer NOT NULL,
    at_abbrev character varying(32),
    gs_attribution text
);



CREATE SEQUENCE odestatic.attribution_at_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.attribution_at_id_seq OWNED BY odestatic.attribution.at_id;



CREATE SEQUENCE odestatic.genedb_gdb_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.genedb_gdb_id_seq OWNED BY odestatic.genedb.gdb_id;



CREATE TABLE odestatic.ontologydb (
    ontdb_id integer NOT NULL,
    ontdb_name character varying NOT NULL,
    ontdb_prefix character varying,
    ontdb_ncbo_id integer,
    ontdb_date date,
    ontdb_linkout_url character varying,
    ontdb_ncbo_vid integer
);



CREATE SEQUENCE odestatic.newontologydb_ontdb_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.newontologydb_ontdb_id_seq OWNED BY odestatic.ontologydb.ontdb_id;



CREATE TABLE odestatic.probe (
    prb_id bigint NOT NULL,
    prb_ref_id character varying,
    pf_id integer NOT NULL
);



CREATE SEQUENCE odestatic.newprobe_prb_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.newprobe_prb_id_seq OWNED BY odestatic.probe.prb_id;



CREATE TABLE odestatic.platform (
    pf_id integer NOT NULL,
    pf_gpl_id character varying,
    pf_shortname character varying,
    pf_name character varying,
    sp_id integer NOT NULL,
    pf_identifier_info character varying,
    pf_gene_primary character varying,
    pf_gene_secondary character varying,
    pf_tempfile character varying,
    pf_set integer,
    old_ma_group integer,
    pf_date timestamp without time zone
);



CREATE SEQUENCE odestatic.platform_pf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.platform_pf_id_seq OWNED BY odestatic.platform.pf_id;



CREATE TABLE odestatic.species (
    sp_id integer NOT NULL,
    sp_name character varying NOT NULL,
    sp_taxid integer NOT NULL,
    sp_ref_gdb_id integer,
    sp_date date,
    sp_biomart_info character varying,
    sp_source_data text
);



CREATE SEQUENCE odestatic.species_sp_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE odestatic.species_sp_id_seq OWNED BY odestatic.species.sp_id;



CREATE TABLE production.file (
    file_id bigint NOT NULL,
    file_size bigint,
    file_uri character varying,
    file_contents text,
    file_comments character varying,
    file_created date,
    file_changes text
);



CREATE SEQUENCE production.file_file_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE production.file_file_id_seq OWNED BY production.file.file_id;



CREATE TABLE production.geneset (
    gs_id bigint NOT NULL,
    usr_id integer NOT NULL,
    file_id bigint NOT NULL,
    gs_name character varying NOT NULL,
    gs_abbreviation character varying DEFAULT 'GS000 [noAbbrev]'::character varying NOT NULL,
    pub_id integer,
    res_id integer,
    cur_id integer,
    gs_description character varying,
    sp_id integer NOT NULL,
    gs_count integer NOT NULL,
    gs_threshold_type integer,
    gs_threshold character varying,
    gs_groups character varying,
    gs_attribution_old character varying,
    gs_uri character varying,
    gs_gene_id_type integer DEFAULT 0 NOT NULL,
    _searchtext tsvector,
    gs_created date,
    admin_flag character varying,
    gs_updated timestamp without time zone DEFAULT now(),
    gs_status character varying DEFAULT 'normal'::character varying,
    gsv_qual character varying DEFAULT '{}'::character varying,
    _comments_author text,
    _comments_curator text,
    gs_attribution integer,
    gs_is_edgelist boolean DEFAULT false
);



CREATE SEQUENCE production.geneset_gs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE production.geneset_gs_id_seq OWNED BY production.geneset.gs_id;



CREATE TABLE production.grp (
    grp_id integer NOT NULL,
    grp_name character varying NOT NULL,
    grp_private boolean DEFAULT false,
    grp_created date
);



CREATE SEQUENCE production.grp_grp_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE production.grp_grp_id_seq OWNED BY production.grp.grp_id;



CREATE TABLE production.publication (
    pub_id integer NOT NULL,
    pub_authors character varying,
    pub_title character varying,
    pub_abstract text,
    pub_journal character varying,
    pub_volume character varying,
    pub_pages character varying,
    pub_month character varying,
    pub_year character varying,
    pub_pubmed character varying,
    _searchtext tsvector
);



CREATE SEQUENCE production.publication_pub_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



ALTER SEQUENCE production.publication_pub_id_seq OWNED BY production.publication.pub_id;



ALTER TABLE ONLY extsrc.gene ALTER COLUMN ode_gene_id SET DEFAULT nextval('extsrc.gene_ode_gene_id_seq'::regclass);



ALTER TABLE ONLY extsrc.ontology ALTER COLUMN ont_id SET DEFAULT nextval('extsrc.ontology_ont_id_seq'::regclass);



ALTER TABLE ONLY odestatic.attribution ALTER COLUMN at_id SET DEFAULT nextval('odestatic.attribution_at_id_seq'::regclass);



ALTER TABLE ONLY odestatic.genedb ALTER COLUMN gdb_id SET DEFAULT nextval('odestatic.genedb_gdb_id_seq'::regclass);



ALTER TABLE ONLY odestatic.ontologydb ALTER COLUMN ontdb_id SET DEFAULT nextval('odestatic.newontologydb_ontdb_id_seq'::regclass);



ALTER TABLE ONLY odestatic.platform ALTER COLUMN pf_id SET DEFAULT nextval('odestatic.platform_pf_id_seq'::regclass);



ALTER TABLE ONLY odestatic.probe ALTER COLUMN prb_id SET DEFAULT nextval('odestatic.newprobe_prb_id_seq'::regclass);



ALTER TABLE ONLY odestatic.species ALTER COLUMN sp_id SET DEFAULT nextval('odestatic.species_sp_id_seq'::regclass);



ALTER TABLE ONLY production.file ALTER COLUMN file_id SET DEFAULT nextval('production.file_file_id_seq'::regclass);



ALTER TABLE ONLY production.geneset ALTER COLUMN gs_id SET DEFAULT nextval('production.geneset_gs_id_seq'::regclass);



ALTER TABLE ONLY production.grp ALTER COLUMN grp_id SET DEFAULT nextval('production.grp_grp_id_seq'::regclass);



ALTER TABLE ONLY production.publication ALTER COLUMN pub_id SET DEFAULT nextval('production.publication_pub_id_seq'::regclass);



ALTER TABLE ONLY extsrc.gene
    ADD CONSTRAINT gene_pkey PRIMARY KEY (ode_gene_id, ode_ref_id);



ALTER TABLE ONLY extsrc.geneset_ontology
    ADD CONSTRAINT geneset_ontology_pkey PRIMARY KEY (gs_id, ont_id, gso_ref_type);



ALTER TABLE ONLY extsrc.geneset_value
    ADD CONSTRAINT geneset_value_pkey PRIMARY KEY (gs_id, ode_gene_id);



ALTER TABLE ONLY extsrc.ontology
    ADD CONSTRAINT ontology_ont_ref_id_key UNIQUE (ont_ref_id);



ALTER TABLE ONLY extsrc.ontology
    ADD CONSTRAINT ontology_pkey PRIMARY KEY (ont_id);



ALTER TABLE ONLY extsrc.probe2gene
    ADD CONSTRAINT probe2gene_pkey PRIMARY KEY (prb_id, ode_gene_id);



ALTER TABLE ONLY odestatic.attribution
    ADD CONSTRAINT attribution_pkey PRIMARY KEY (at_id);



ALTER TABLE ONLY odestatic.genedb
    ADD CONSTRAINT genedb_pkey PRIMARY KEY (gdb_id);



ALTER TABLE ONLY odestatic.ontologydb
    ADD CONSTRAINT newontologydb_pkey PRIMARY KEY (ontdb_id);



ALTER TABLE ONLY odestatic.platform
    ADD CONSTRAINT platform_pf_gpl_id_key UNIQUE (pf_gpl_id);



ALTER TABLE ONLY odestatic.platform
    ADD CONSTRAINT platform_pkey PRIMARY KEY (pf_id);



ALTER TABLE ONLY odestatic.probe
    ADD CONSTRAINT probe_pkey PRIMARY KEY (prb_id);



ALTER TABLE ONLY odestatic.species
    ADD CONSTRAINT species_pkey PRIMARY KEY (sp_id);



ALTER TABLE ONLY production.file
    ADD CONSTRAINT file_pkey PRIMARY KEY (file_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_pkey PRIMARY KEY (gs_id);



ALTER TABLE ONLY production.grp
    ADD CONSTRAINT grp_pkey PRIMARY KEY (grp_id);



ALTER TABLE ONLY production.publication
    ADD CONSTRAINT publication_pkey PRIMARY KEY (pub_id);



CREATE INDEX gene_lower_gdb_id_sp_id_expr_idx ON extsrc.gene USING btree (lower((ode_ref_id)::text), gdb_id, sp_id, ((((gdb_id)::text || '*'::text) || lower((ode_ref_id)::text))));



CREATE INDEX gene_ode_gene_id_gdb_id_idx ON extsrc.gene USING btree (ode_gene_id, gdb_id);



CREATE INDEX gene_ode_gene_id_idx ON extsrc.gene USING btree (ode_gene_id);



CREATE INDEX gene_ode_ref_id_idx ON extsrc.gene USING btree (ode_ref_id);



CREATE INDEX geneset_ontology_gs_id_ont_id_idx ON extsrc.geneset_ontology USING btree (gs_id, ont_id) WHERE ((gso_ref_type)::text <> 'Blacklist'::text);



CREATE INDEX geneset_value_gs_id ON extsrc.geneset_value USING btree (gs_id);



CREATE INDEX geneset_value_gs_id_ode_gene_id_gsv_in_threshold_idx ON extsrc.geneset_value USING btree (gs_id, ode_gene_id, gsv_in_threshold);



CREATE INDEX geneset_value_gs_id_ode_gene_id_idx ON extsrc.geneset_value USING btree (gs_id, ode_gene_id) WHERE gsv_in_threshold;



CREATE INDEX geneset_value_gsv_in_threshold_idx ON extsrc.geneset_value USING btree (gsv_in_threshold);



CREATE INDEX geneset_value_ode_gene_id_gsv_in_threshold_idx ON extsrc.geneset_value USING btree (ode_gene_id, gsv_in_threshold);



CREATE INDEX homology_hom_id_idx ON extsrc.homology USING btree (hom_id);



CREATE INDEX homology_hom_source_id_idx ON extsrc.homology USING btree (hom_source_id);



CREATE INDEX homology_hom_source_uid_idx ON extsrc.homology USING btree (hom_source_uid);



CREATE INDEX homology_ode_gene_id_idx ON extsrc.homology USING btree (ode_gene_id);



CREATE INDEX probe2gene_ode_gene_id_idx ON extsrc.probe2gene USING btree (ode_gene_id);



CREATE INDEX lower_probe_idx ON odestatic.probe USING btree (lower((prb_ref_id)::text));



CREATE INDEX probe_ref_idx ON odestatic.probe USING btree (prb_ref_id);



CREATE INDEX file_contents_md5 ON production.file USING btree (md5(file_contents));



CREATE INDEX geneset_byuser_idx ON production.geneset USING btree (usr_id);



CREATE INDEX geneset_notdeleted_idx ON production.geneset USING btree (gs_status) WHERE ((gs_status)::text <> 'deleted'::text);



CREATE INDEX geneset_pub_idx ON production.geneset USING btree (pub_id);



CREATE INDEX geneset_status_idx ON production.geneset USING btree (gs_status);



CREATE INDEX geneset_usr_idx ON production.geneset USING btree (usr_id);



CREATE INDEX gs_searchtext_gin ON production.geneset USING gin (_searchtext);



CREATE INDEX pub_searchtext_gin ON production.publication USING gin (_searchtext);



CREATE TRIGGER geneset_create_info_trg AFTER INSERT ON production.geneset FOR EACH ROW EXECUTE PROCEDURE production.geneset_create_info();



CREATE TRIGGER geneset_gs_updated BEFORE UPDATE ON production.geneset FOR EACH ROW EXECUTE PROCEDURE production.on_geneset_updated();



CREATE TRIGGER geneset_threshold_update_trigger AFTER UPDATE ON production.geneset FOR EACH ROW EXECUTE PROCEDURE production.geneset_threshold_update();



CREATE TRIGGER geneset_update_trigger BEFORE INSERT OR UPDATE ON production.geneset FOR EACH ROW EXECUTE PROCEDURE production.geneset_search_update();



CREATE TRIGGER publication_update_trigger BEFORE INSERT OR UPDATE ON production.publication FOR EACH ROW EXECUTE PROCEDURE production.publication_search_update();



ALTER TABLE ONLY odestatic.genedb
    ADD CONSTRAINT genedb_sp_id_fkey FOREIGN KEY (sp_id) REFERENCES odestatic.species(sp_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_cur_id_fkey FOREIGN KEY (cur_id) REFERENCES odestatic.curation_levels(cur_id) ON UPDATE CASCADE ON DELETE RESTRICT;



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_file_id_fkey FOREIGN KEY (file_id) REFERENCES production.file(file_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_pub_id_fkey FOREIGN KEY (pub_id) REFERENCES production.publication(pub_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_res_id_fkey FOREIGN KEY (res_id) REFERENCES production.result(res_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_sp_id_fkey FOREIGN KEY (sp_id) REFERENCES odestatic.species(sp_id);



ALTER TABLE ONLY production.geneset
    ADD CONSTRAINT geneset_usr_id_fkey FOREIGN KEY (usr_id) REFERENCES production.usr(usr_id);



INSERT INTO odestatic.species (sp_id, sp_name, sp_taxid) 
VALUES      (0, '', 0), 
            (1, 'Mus musculus', 10090), 
            (2, 'Homo sapiens', 9606),
            (3, 'Rattus norvegicus', 10116);

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

INSERT INTO production.file (file_id) VALUES (0);

INSERT INTO production.geneset (gs_id, gs_name, gs_abbreviation, gs_description, cur_id, sp_id, gs_gene_id_type, gs_attribution, usr_id, file_id, gs_count)
VALUES                         (185236, 'GO:0006184 GTP catabolic process', 'GO:0006184', 'The chemical reactions and pathways...', 1, 1, -10, 8, 0, 0, 20),
                               (219234, 'Differentially Expressed in bipolar and control...', 'Bipolar twin study', '82 transcripts with a 1.3-fold change...', 3, 2, -2, NULL, 0, 0, 30),
                               (270867, 'GWAS Catalog Data for rheumatoid arthritis...', 'GWAS: rheumatoid arthritis', 'List of positional candidate...', 1, 2, -7, 6, 0, 0, 5);

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

INSERT INTO production.publication (pub_id, pub_pubmed)
VALUES                             (2312, 17440432),
                                   (7841, 26077402);

UPDATE production.geneset SET pub_id = 2312 WHERE gs_id = 219234;
UPDATE production.geneset SET pub_id = 7841 WHERE gs_id = 270867;

INSERT INTO odestatic.platform (pf_id, pf_gpl_id, pf_shortname, pf_name, sp_id)
VALUES                         (92, 'GPL10739', 'HuGene-1_0-st', 'Affymetrix Human Gene 1.0 ST', 2),
                               (82, 'GPL6887', 'MouseWG-6 v2.0', 'Illumina MouseWG-6 v2.0', 1),
                               (1, 'GPL32', 'MG_U74A', 'Affymetrix Murine Genome U74A', 1);

INSERT INTO odestatic.probe (prb_id, prb_ref_id, pf_id)
VALUES                      (4723, '104722_at', 1),
                            (5213, '92624_r_at', 1),
                            (1014432, '1223735', 82),
                            (1014603, '2455830', 82),
                            (2132663, '7920865', 92),
                            (2132681, '8146657', 92);

INSERT INTO extsrc.probe2gene (prb_id, ode_gene_id)
VALUES                        (4723, 6566),
                              (5213, 1658);

INSERT INTO production.grp (grp_id, grp_name)
VALUES                     (1, 'A group name'),
                           (2, 'My group');

INSERT INTO odestatic.ontologydb (ontdb_id, ontdb_name, ontdb_prefix, ontdb_linkout_url, ontdb_date)
VALUES                     (1, 'Gene Ontology', 'GO', 'url', NOW()),
                           (4, 'MeSH Terms', 'MESH', 'url', NOW());

INSERT INTO extsrc.geneset_ontology (gs_id, ont_id, gso_ref_type)
VALUES                              (185236, 60455, 'Manual Association');

INSERT INTO extsrc.ontology (ont_id, ont_ref_id, ont_name, ont_description, ont_children, ont_parents, ontdb_id)
VALUES                     (60455, 'GO:0006184', 'GTP catabolic process', 'The chemical...', 4, 2, 1),
                           (50019, 'GO:0043088', 'regulation of Cdc42 GTPase activity', 'Any process...', 1, 2, 1),
                           (7507, 'D009062', 'Mouth Neoplasms', 'Tumors or cancer...', 6, 2, 4);

