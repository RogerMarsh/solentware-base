# sqlite3api.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API, implemented using sqlite3, where indicies are represented as
lists or bitmaps of record numbers.
    
sqlite3 is an interface to SQLite3.

The database is accessed as if made from Berkeley DB primary and secondary
databases.

The _sqlite.Primary and _sqlite.Secondary classes provide the behaviour.

"""

import sqlite3

from . import _sqlite


class Sqlite3apiError(_sqlite.Sqlite3apiError):
    pass


class Database(_sqlite.Database):
    
    """Use sqlite3 module to access SQLite3 databases."""

    def make_connection(self):
        """Connect to an SQLite3 database with sqlite3 module."""
        if self._dbservices is None:
            self._dbservices = sqlite3.Connection(self._home)
            # Remove the following statement to convert to unicode strings
            #self._dbservices.text_factory = str


class Sqlite3api(_sqlite.Sqlite3api, Database):
    
    """Access database with sqlite3.  See superclass for *args and **kargs.
    
    sqlite3 is an interface to SQLite3.
    
    _sqlite.Primary instances are used to access data, and _sqlite.Secondary
    instances are used to access indicies on the data.

    There will be one _sqlite.Primary instance for each SQLite3 table, used
    approximately like a Berkeley DB primary database.

    There will be one _sqlite.Secondary instance for each SQLite3 index, used
    approximately like a Berkeley DB secondary database.

    Primary and secondary terminology comes from Berkeley DB documentation.
    
    """

    def __init__(self, database_specification, *args, **kargs):
        """Use _sqlite.Primary and _sqlite.Secondary classes."""
        super().__init__(
            _sqlite.Primary,
            _sqlite.Secondary,
            database_specification,
            *args,
            **kargs)
