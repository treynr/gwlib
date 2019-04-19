
Changelog
=========

Unreleased
----------

Added
'''''

- Add PooledConnection class


Changed
'''''''

- Update SELECT queries to return uniform datatypes (dataframes)

- Update PooledCursor to actually use pooled connections


1.2.1 - 2019.02.27
------------------

Added
'''''

- Add default_color parameter to logging messages


1.2.0 - 2019.02.16
------------------

Added
'''''

- Add function for retrieving gene set threshold type identifiers

- Add function for publication metadata based on PMIDs.

- Add function for inserting multiple gene set values at once.

Changed
'''''''

- Update ``get_geneset_values`` to ignore variant gene types.


1.1.0 - 2019.01.31
------------------

Added
'''''

- Add :code:`get_score_types` function to :code:`db.py` which returns a list of 
  supported GW score types. 

- Add :code:`configpraser` to required module list.

Changed
'''''''

- Update :code:`get_species` function to optionally return lowercased species names.

- Update :code:`get_git_hash` to handle failure properly.

- Change versioning scheme.

- Redo logging functionality

Fixed
'''''

- Fixed ImportError in :code:`setup.py`.


1.0.0 - 2019.01.05
------------------

- Initial public release.

