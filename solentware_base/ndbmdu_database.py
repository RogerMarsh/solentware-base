# ndbmdu_database.py
# Copyright (c) 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a NoSQL database created from a FileSpec() definition with Python's
dbm.ndbm module in deferred update mode.

"""
from . import ndbm_database
from .core import _nosqldu


class Database(_nosqldu.Database, ndbm_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.

    Default methods using the ndbm interface are taken from the
    ndbm_database.Database class, overridden or supplemented where necessary
    by methods from the core._nosqldu.Database class.
    """
