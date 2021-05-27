# sqlite3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database created from a FileSpec() definition with the
sqlite3 module.

The Python version must be 3.6 or later.  Use apsw_database on earlier versions
of Python.

"""
import sqlite3

from .core import _sqlite


class Database(_sqlite.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.
    """

    def open_database(self, **k):
        """Use sqlite3 to access SQLite and delegate to superclass."""

        # The first super().open_database() call in a run will raise a
        # SegmentSizeError, if the actual segment size is not the size given in
        # the FileSpec, after setting segment size to that found in database.
        # Then the super().open_database() call in except path should succeed
        # because segment size is now same as that on the database.
        try:
            super().open_database(sqlite3, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(sqlite3, **k)
