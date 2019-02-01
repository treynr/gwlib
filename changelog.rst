
Changelog
=========

1.1.0 - 2019.01.31
----------

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
