# lmdb_database.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Symas LMMD database without deferring index updates.

The lmdb module provides the database interface.

"""

import lmdb

from .core import _lmdb


class Database(_lmdb.Database):
    """Define Database class using lmdb module.

    Behaviour comes from the _lmdb.Database class.

    The Symas LMMD engine comes from the lmdb module.
    """

    def open_database(self, **k):
        """Delegate to superclass with bsddb3.db as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(lmdb, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(lmdb, **k)
