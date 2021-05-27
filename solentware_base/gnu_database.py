# gnu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a NoSQL database created from a FileSpec() definition with Python's
dbm.gnu module.

"""
from . import gnu_module
from .core import _nosql


class Database(_nosql.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.
    """

    def open_database(self, **k):
        """Use dbm.ndbm to access a ndbm NoSQL database and delegate to
        superclass."""

        # The first super().open_database() call in a run will raise a
        # SegmentSizeError, if the actual segment size is not the size given in
        # the FileSpec, after setting segment size to that found in database.
        # Then the super().open_database() call in except path should succeed
        # because segment size is now same as that on the database.
        try:
            super().open_database(gnu_module, gnu_module.Gnu, None, **k)
        except self.__class__.SegmentSizeError:
            super().open_database(gnu_module, gnu_module.Gnu, None, **k)
