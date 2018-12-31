
gwlib
=====

A python library designed to help write backend services for GeneWeaver__ (GW).

.. __: https://ncbi.nlm.nih.gov/pubmed/26656951

The :code:`gwlib` package is comprised of four separate modules:

- :code:`batch.py`: classes to parse and output gene sets in GeneWeaver's batch format.

- :code:`config.py`: contains a simple configuration file parser based on python's
  :code:`ConfigParser`.

- :code:`config.py`: contains a simple configuration file parser based on python's
  :code:`ConfigParser`.

- :code:`db.py`: wrapper functions that encapsulate commonly used GW database queries.

- :code:`log.py`: output logging class based python's :code:`logging` module.

- :code:`util.py`: miscellaneous utility functions.


Usage
-----

Let's say we're interested in retrieving all the genes belonging to a single species and
removing any genes that don't have homologs__ in at least one other species.
We can do this in just a few lines of code using the :code:`db.py` module:

.. __: https://en.wikipedia.org/wiki/Sequence_homology

.. code:: python

    from gwlib import db

    db.connect('host', 'database', 'user', 'password')

    ## Returns a bijection of species names to unique species identifiers
    species = db.get_species()

    ## Retrieve all known genes in mice
    genes = db.get_species_genes(species['Mus musculus'])

    ## Discover genes that have homologs in >= 1 other species using NCBI Homologene
    homologs = db.get_gene_homologs(set(genes.values()), source='Homologene')

    ## Print the gene identifiers
    print homologs.keys()


API
---

See the individual module docs for API and usage.


Installation
------------
