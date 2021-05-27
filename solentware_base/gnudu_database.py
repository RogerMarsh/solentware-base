# gnudu_database.py
# Copyright (c) 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a NoSQL database created from a FileSpec() definition with Python's
dbm.gnu module in deferred update mode.

"""
from . import gnu_database
from .core import _nosqldu


class Database(_nosqldu.Database, gnu_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.

    Default methods using the gnu interface are taken from the
    gnu_database.Database class, overridden or supplemented where necessary
    by methods from the core._nosqldu.Database class.
    """
