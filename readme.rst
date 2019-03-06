
gwlib
=====

.. image:: https://img.shields.io/travis/treynr/gwlib.svg?style=flat-square
    :target: https://travis-ci.org/treynr/gwlib

A Python 2.7/3.6/3.7 library designed to help write backend services for GeneWeaver__ (GW).

.. __: https://ncbi.nlm.nih.gov/pubmed/26656951

The :code:`gwlib` package is comprised of five separate modules:

- :code:`batch.py`: classes to parse and output gene sets in GW's batch format.

- :code:`config.py`: contains a simple configuration file parser based on python's
  :code:`ConfigParser`.

- :code:`db.py`: wrapper functions that encapsulate commonly used GW database queries.

- :code:`log.py`: output logging customization based python's :code:`logging` module.

- :code:`util.py`: miscellaneous utility functions.


Usage
-----

As an example, let's say we're interested in retrieving all the genes belonging to a 
single species and removing any genes that don't have homologs__ in at least one other 
species.
We can do this in just a few lines of code using the :code:`db.py` module:

.. __: https://en.wikipedia.org/wiki/Sequence_homology

.. code:: python

    from gwlib import db

    ## Replace these with your own credentials
    db.connect('host', 'database', 'user', 'password')

    ## Returns a bijection of species names to unique species identifiers
    species = db.get_species()

    ## Retrieve all known genes in mice
    genes = db.get_species_genes(species['Mus musculus'])

    ## Discover genes that have homologs in >= 1 other species using NCBI Homologene
    homologs = db.get_gene_homologs(set(genes.values()), source='Homologene')

    ## Print the gene identifiers
    print(homologs.keys())


API
---

See the individual module docs__ for API and usage.

.. __: docs/


Installation
------------

Installation can be accomplished by either retrieving the latest release and installing 
via pip or by cloning this repository and installing from source.
To install the latest version via pip:

.. code:: bash

    $ pip install https://github.com/treynr/gwlib/archive/v1.2.1.tar.gz

To install from source:

.. code:: bash

    $ git clone https://github.com/treynr/gwlib.git
    $ cd gwlib
    $ python setup.py install


Requirements
''''''''''''

- Python 2.7/3.6/3.7
- configparser__
- psycopg2__

.. __: https://github.com/jaraco/configparser/
.. __: http://initd.org/psycopg/

