# ndbm_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a dbm.ndbm database without deferring index updates.

The ndbm_module module provides the database interface.

Prefer to use the ndbmdu_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

"""

from . import ndbm_module
from .core import _nosql


class Database(_nosql.Database):
    """Define Database class using ndbm_module module.

    Behaviour comes from the _nosql.Database class.

    The dbm.ndbm engine comes from the dbm.ndbm module.  It's functions
    are accessed via the ndbm_module which provides an interface
    similar to unqlite and vedis.
    """

    def open_database(self, **k):
        """Delegate to superclass with dbm.ndbm as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(ndbm_module, ndbm_module.Ndbm, None, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(ndbm_module, ndbm_module.Ndbm, None, **k)
