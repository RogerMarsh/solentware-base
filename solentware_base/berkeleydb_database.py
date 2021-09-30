# berkeleydb_database.py
# Copyright 2021 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database without deferring index updates.

The berkeleydb module provides the database interface.

Prefer to use the bsddb3du_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

"""

# Must be berkeleydb, not berkeleydb.db, to meet external references the
# way done in apsw_database, sqlite3_database, unqlite_database, and
# vedis_database.
import berkeleydb

from .core import _db


class Database(_db.Database):
    """Define Database class using berkeleydb module.

    Behaviour comes from the _db.Database class.

    The Berkeley DB engine comes from the berkeleydb module.
    """

    def open_database(self, **k):
        """Delegate with berkeleydb.db as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(berkeleydb.db, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(berkeleydb.db, **k)
