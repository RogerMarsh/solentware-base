# sqlite3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database without deferring index updates.

The sqlite3 module provides the database interface.

Prefer to use the sqlitedu_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

The Python version must be 3.6 or later.  Use apsw_database on earlier
versions of Python.

"""
import sqlite3

from .core import _sqlite


class Database(_sqlite.Database):
    """Define Database class using sqlite3 module.

    Behaviour comes from the _sqlite.Database class.

    The SQL engine comes from the sqlite3 module.
    """

    def open_database(self, **k):
        """Delegate to superclass with sqlite3 as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(sqlite3, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(sqlite3, **k)
