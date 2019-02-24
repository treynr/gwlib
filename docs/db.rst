
db.py module
============

Documentation for functions in the ``db`` module.

API
---

db.\ **connect**\ (host, db, user, password, port=5432)

Connect to a Postgres database using the given credentials.

Arguments:

- **host**: DB host/server
- **db**: DB name
- **user**: user name
- **password**: password
- **port**: optional, port the DB server is user

Returns:

A tuple indicating success. The first element is a boolean which indicates
whether the connection was successful or not. In the case of an
unsuccessful connection, the second element contains the error or exception.


Selections
''''''''''

db.\ **get_species**\ (lower=False)

Returns a species name and ID mapping for all the species currently
supported by GW.

Arguments:

- **lower**: if true, returns lowercased species names

Returns:
    
A mapping of species names (sp_name) to species identifiers (sp_id).


db.\ **get_species_with_taxid**\ ()

Returns a a list of species supported by GW. The returned list includes species
names, identifiers, and NCBI taxon IDs.

Returns: 

A list of dicts, each dict field corresponds to the column name (sp_id, sp_name, 
and sp_taxid).


db.\ **get_species_with_taxid**\ ()

Returns a mapping of species tax IDs (NCBI taxonomy ID) to their GW sppecies ID.

Returns:

A mapping of taxon IDs (sp_taxid) to species IDs (sp_id).


db.\ **get_attributions**\ ()

Returns all the attributions (at_id and at_abbrev) found in the DB.
These represent third party data resources integrated into GeneWeaver.

Returns:

A mapping of attribution abbreviations to IDs.


db.\ **get_gene_ids**\ (refs, sp_id=None, gdb_id=None)

Given a set of external reference IDs, this returns a mapping of reference gene 
identifiers to the IDs used internally by GeneWeaver (ode_gene_id).
An optional species id can be provided to limit gene results by species.
An optional gene identifier type (gdb_id) can be provided to limit mapping by 
ID type (useful when identifiers from different resources overlap).
This query does not include genomic variants.

Reference IDs are always strings (even if they're numeric) and should be
properly capitalized. If duplicate references exist in the DB (unlikely)
then they are overwritten in the return dict. Reference IDs can be any valid
identifier supported by GeneWeaver (e.g. Ensembl, NCBI Gene, MGI, HGNC, etc.).
See the **get_gene_types** function for gene types supported by GW.

Arguments:

- refs: a list of reference identifiers to convert
- sp_id: an optional species identifier used to limit the ID mapping process
- gdb_id: an optional gene type identifier used to limit the ID mapping process

Returns:

A bijection of reference identifiers (ode_ref_id) to GW gene IDs (ode_gene_id).


db.\ **get_species_genes**\ (sp_id, gdb_id=None, symbol=True)

Similar to the above **get_gene_ids** but returns a reference ID to GW ID 
mapping for all genes for the given species (as a warning, this will be a lot 
of data).
This query does not include genomic variants.

If a gdb_id is provided then this will return all genes covered by the given gene
type.
If symbol is true, then the function returns gene entities that have an official
gene symbol to limit the amount of data returned.
gdb_id will always override the symbol argument.

Arguments:

- sp_id:  species identifier
- gdb_id: an optional gene type identifier used to limit the ID mapping process
- symbol: if true limits results to genes covered by the symbol gene type

Returns:

An N:1 mapping of reference identifiers to GW IDs


db.\ **get_gene_refs**\ (genes, type_id=None)

The inverse of the **get_gene_refs** function. For the given list of internal GW 
gene identifiers, this function returns a mapping of internal to external
(e.g. MGI, HGNC, Ensembl) reference identifiers.
The mapping is 1:N since many external references may exist for a single, condensed
GW identifier.

Arguments:

- genes:   a list of internal GW gene identifiers (ode_gene_id)
- type_id: an optional gene type ID to limit the mapping to a specific gene type

Returns:

A 1:N mapping of GW IDs to reference identifiers


db.\ **get_genesets**\ (gs_ids)

Returns a list of gene set metadata for the given list of gene set IDs.

Arguments:

- gs_ids: a list of gs_ids

Returns

A list of geneset objects. Each object is a dict where each field corresponds to 
the columns in the geneset table. 


db.\ **get_geneset_ids**\ (tiers=[1, 2, 3, 4, 5], at_id=0, size=0, sp_id=0)

Returns a list of normal (i.e. their status is not deleted or deprecated) gene 
set IDs.
IDs can be filtered based on tiers, gene set size, species, and public resource
attribution.

Arguments:

- at_id: public resource attribution ID
- tiers: a list of curation tiers
- size:  indicates the maximum size a set should be during retrieval
- sp_id: species identifier

Returns:

A list of gene set IDs.


db.\ **get_geneset_values**\ (gs_ids)

Returns all gene set values (genes and scores) for the given list of gene set IDs.

Arguments:

- gs_ids: a list of gs_ids

Returns:

A list of dicts, each dict contains the gene set id, gene id, and gene score.
Dictionary fields correspond to column names: gs_id, ode_gene_id, and gsv_value.


db.\ **get_gene_homologs**\ (genes, source='Homologene')

Returns all homology IDs for the given list of gene IDs.

Arguments:

- genes:  list of internal GeneWeaver gene identifiers (ode_gene_id)
- source: the homology mapping data source to use, default is Homologene

Returns:

    A bijection of gene identifiers to homology identifiers


db.\ **get_publication**\ (pmid)

Returns the GW publication ID associated with the given PubMed ID.

Arguments:

- pmid: PubMed ID

Returns:

A GW publication ID (pub_id) or None one doesn't exist.


db.\ **get_publications**\ (pmids)

Returns a mapping of PubMed IDs to their GW publication IDs.

Arguments:

- pmids: a list of PubMed IDs

Returns:

A dict mapping PubMed IDs to GW publication IDs.


db.\ **get_publication_pmid**\ (pub_id)

Returns the PMID associated with a GW publication ID (pub_id).

Arguments:

- pub_id: publication ID

Returns:

A string representing the article's PMID or None if one doesn't exist


db.\ **get_geneset_pmids**\ (gs_ids)

Returns a bijection of gene set identifiers (gs_id) to the PubMed IDs they 
are associated with.

Arguments:

- gs_ids: list of gene set IDs (gs_id) to retrieve PMIDs for

Returns:

A dict that maps the GS ID to the PMID. If a GS ID doesn't have an associated
publication, then it will be missing from results.


db.\ **get_geneset_metadata**\ (gs_ids)

Returns names, descriptions, and abbreviations for each geneset in the
provided list.

Arguments:

- gs_ids: list of gene set IDs to retrieve metadata for

Returns:

A list of dicts containing gene set IDs, names, descriptions, and abbreviations.
Each dict field corresponds to the column name (gs_id, gs_name, 
gs_description, gs_abbreviation).


db.\ **get_gene_types**\ (short=False)

Returns a bijection of gene type names to their associated type identifier.
If short is true, returns "short names" which are condensed or abbreviated names.

Arguments:

- short: optional argument to return short names

Returns:

A bijection of gene type names to type IDs.


db.\ **get_score_types**\ ()

Returns a list of score types supported by GeneWeaver. This data isn't currently
stored in the DB but it should be.

Returns:

A bijection of score types to type IDs.


db.\ **get_platforms**\ ()

Returns the list of GW supported microarray platform and gene expression
technologies.

Returns:

A list of objects whose keys match the platform table. These attributes include
the unique platform identifier, the platform name, a condensed name, and the GEO
GPL identifier (pf_id, pf_name, pf_shortname, and pf_gpl_id).


db.\ **get_platform_names**\ ()

Returns a mapping of microarray platform names (pf_name) to GW platform IDs (pf_id).

Returns:

A bijection of platform names (pf_name) to identifiers (pf_id).


db.\ **get_platform_probes**\ (pf_id, refs)

Retrieves internal GW probe identifiers for the given list of probe reference
identifiers. Requires a platform ID since some expression platforms reuse probe
references.

Arguments:

- pf_id: platform identifier
- refs:  list of probe reference identifiers belonging to a platform

Returns:

A bijection of probe references to GW probe identifiers for the given platform


db.\ **get_all_platform_probes**\ (pf_id)

Retrieves all the probe reference identifiers (these are provided by the 
manufacturer and stored in the GW DB) for the given platform.

Arguments:

- pf_id: platform ID

Returns:

A list of probe references


db.\ **get_probe2gene**\ (prb_ids)

For the given list of GW probe identifiers, retrieves the genes each probe is
supposed to map to. Retrieves a 1:N mapping since some platforms map a single probe
to multiple genes.

Arguments:

- prb_ids: a list of probe IDs

Returns:

A 1:N mapping of probe IDs (prb_id) to genes (ode_gene_id)


db.\ **get_group_by_name**\ (name)

Returns the group ID (grp_id) for the given group name (grp_name).

Arguments:

- name: the name of group

Returns:

A group ID (grp_id).


db.\ **get_genesets_by_project**\ (pj_ids)

Returns all geneset IDs (gs_id) associated with the given project IDs (pj_id).

Arguments:

- pj_ids: a list of project IDs

Returns:

A 1:N mapping of project IDs to gene set IDs


db.\ **get_genesets_annotations**\ (gs_ids)

Returns the set of ontology annotations for each given gene set.

Arguments:

- gs_ids: list of gene set ids to retrieve annotations for

Returns:

A 1:N mapping of gene set IDs to ontology annotations.
The value of each key in the returned dict is a list of tuples.
Each tuple comprises a single annotation and contains two elements:
1) an internal GW ID which represents an ontology term (ont_id) and, 2)
the external ontology term id used by the source ontology.
e.g. {123456: (7890, 'GO:1234567')}


db.\ **get_annotation_by_refs**\ (ont_refs)

Maps ontology reference IDs (e.g. GO:0123456, MP:0123456) to the internal
ontology IDs used by GW.

Arguments:

- ont_refs: a list of external ontology reference IDs

Returns:

A bijection of ontology term references to GW ontology IDs.


db.\ **get_ontologies**\ ()

Returns the list of ontologies supported by GeneWeaver for use with gene
set annotations.

Returns:

A list of dicts. Each dict contains fields that match the ontologydb table 
(ontdb_id, ontdb_name, ontdb_prefix, ontdb_date).


db.\ **get_ontdb_id**\ (name)

Retrieves the ontologydb ID for the given ontology name.

Arguments:

- name: ontology name

Returns:

The ontology ID (ont_id) for the given ontology name. None is returned if the
ontology name is not found in the database.

db.\ **get_ontology_terms_by_ontdb**\ (ontdb_id)

Retrieves all ontology terms associated with the given ontology.

Arguments:

- ontdb_id: the ID representing an ontology

Returns:

A list of dicts whose fields match the columns in the ontology table.


db.\ **get_threshold_types**\ (lower=False)

Returns a bijection of threshold type names to their IDs.
This data should be stored in the DB but it's not so we hardcode it here.

Arguments:

- lower: optional argument which returns lower cased names if it is set to True

Returns

A mapping of threshold types to IDs (gs_threshold_type)


