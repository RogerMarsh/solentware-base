# apswdu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database created from a FileSpec() definition with the apsw
module in deferred update mode.

"""
from . import apsw_database
from .core import _sqlitedu


class Database(_sqlitedu.Database, apsw_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.

    Default methods for SQLite using the apsw interface are taken from the
    apsw_database.Database class, overridden or supplemented where necessary
    by methods from the core._sqlitedu.Database class.
    """
