# sqlite3duapi.py
# Copyright (c) 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API for bulk insertion of records, implemented using sqlite3,
where indicies are represented as lists or bitmaps of record numbers.
    
sqlite3 is an interface to SQLite3.

The database is accessed as if made from Berkeley DB primary and secondary
databases.

The _sqlitedu.Primary and _sqlitedu.Secondary classes provide the behaviour.

"""

from . import sqlite3api
from . import _sqlitedu


class Sqlite3duapiError(_sqlitedu.Sqlite3duapiError):
    pass


class Sqlite3duapi(_sqlitedu.Sqlite3duapi, sqlite3api.Database):
    
    """Access database with sqlite3.  See superclass for *args and **kargs.
    
    sqlite3 is an interface to SQLite3.
    
    _sqlitedu.Primary instances are used to do bulk data insertions, and
    _sqlitedu.Secondary instances are used to update indicies for the bulk
    data insertions.

    There will be one _sqlitedu.Primary instance for each SQLite3 table, used
    approximately like a Berkeley DB primary database.

    There will be one _sqlitedu.Secondary instance for each SQLite3 index, used
    approximately like a Berkeley DB secondary database.

    Primary and secondary terminology comes from Berkeley DB documentation.
    
    """

    def __init__(self, database_specification, *args, **kargs):
        """Use _sqlitedu.Primary and _sqlitedu.Secondary classes."""
        super().__init__(
            _sqlitedu.Primary,
            _sqlitedu.Secondary,
            database_specification,
            *args,
            **kargs)
