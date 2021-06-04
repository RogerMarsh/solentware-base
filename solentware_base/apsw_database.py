# apsw_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database without deferring index updates.

The apsw module provides the database interface.

Prefer to use the apswdu_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

"""
import apsw

from .core import _sqlite


class Database(_sqlite.Database):
    """Define Database class using apsw module.

    Behaviour comes from the _sqlite.Database class.

    The SQL engine comes from the apsw module.
    """

    def open_database(self, **k):
        """Delegate to superclass with apsw as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(apsw, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(apsw, **k)
