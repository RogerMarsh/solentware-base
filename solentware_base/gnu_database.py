# gnu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a dbm.gnu database without deferring index updates.

The gnu_module module provides the database interface.

Prefer to use the gnudu_database module when adding lots of new
records.  It will be a lot quicker because it defers index updates.

"""
from . import gnu_module
from .core import _nosql


class Database(_nosql.Database):
    """Define Database class using gnu_module module.

    Behaviour comes from the _nosql.Database class.

    The dbm.gnu engine comes from the dbm.gnu module.  It's functions
    are accessed via the gnu_module which provides an interface similar
    to unqlite and vedis.
    """

    def open_database(self, **k):
        """Delegate to superclass with dbm.gnu as database engine module.

        The first super().open_database() call in a run will raise a
        SegmentSizeError, if the actual segment size is not the size given in
        the FileSpec, after setting segment size to that found in database.
        Then the super().open_database() call in except path should succeed
        because segment size is now same as that on the database.
        """
        try:
            super().open_database(gnu_module, gnu_module.Gnu, None, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(gnu_module, gnu_module.Gnu, None, **k)
