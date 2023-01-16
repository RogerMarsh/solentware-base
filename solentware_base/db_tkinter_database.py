# db_tkinter_database.py
# Copyright 2022 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database without deferring index updates.

The tkinter module provides the database interface.

Prefer to use the db_tkinterdu_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

"""

from solentware_base import db_tcl

from .core import _db_tkinter


class Database(_db_tkinter.Database):
    """Define Database class using bsddb3 module.

    Behaviour comes from the _db_tkinter.Database class.

    The Berkeley DB engine comes from the db_tkinter.Database class using
    the Tcl interface to Berkeley DB.
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
            super().open_database(db_tcl.tcl, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(db_tcl.tcl, **k)
