# vedisdu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a NoSQL database created from a FileSpec() definition with the vedis
module in deferred update mode.

"""
from . import vedis_database
from .core import _nosqldu


class Database(_nosqldu.Database, vedis_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.

    Default methods using the vedis interface are taken from the
    vedis_database.Database class, overridden or supplemented where necessary
    by methods from the core._nosqldu.Database class.
    """
