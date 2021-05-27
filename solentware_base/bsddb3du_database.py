# bsddb3du_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley database created from a FileSpec() definition with the
bsddb3 module in deferred update mode.

"""
from . import bsddb3_database
from .core import _dbdu


class Database(_dbdu.Database, bsddb3_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.

    Default methods for Berkeley DB using the bsddb3 interface are taken from
    the bsddb3_database.Database class, overridden or supplemented where
    necessary by methods from the core._dbdu.Database class.
    """
