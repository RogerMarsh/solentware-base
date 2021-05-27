# bsddb3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley database created from a FileSpec() definition with the
bsddb3 module.

"""

# Must be bsddb3, not bsddb3.db, to meet external references the way done in
# apsw_database, sqlite3_database, unqlite_database, and vedis_database.
import bsddb3

from .core import _db


class Database(_db.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.
    """

    def open_database(self, **k):
        """Use bsddb3.db to access Berkeley DB and delegate to superclass."""

        # The first super().open_database() call in a run will raise a
        # SegmentSizeError, if the actual segment size is not the size given in
        # the FileSpec, after setting segment size to that found in database.
        # Then the super().open_database() call in except path should succeed
        # because segment size is now same as that on the database.
        try:
            super().open_database(bsddb3.db, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(bsddb3.db, **k)
