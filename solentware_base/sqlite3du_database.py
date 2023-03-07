# sqlite3du_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database deferring index updates.

The sqlite3_database module provides the database interface.

Prefer to use the sqlite3_database module normally.

"""
from . import sqlite3_database
from .core import _sqlitedu
from .core import _sqlite


class Database(
    sqlite3_database.Database, _sqlitedu.Database, _sqlite.Database
):
    """Define deferred update Database class using sqlite3 module.

    Deferred update behaviour comes from the _sqlitedu.Database class.

    The SQL engine comes from the sqlite3_database.Database class.
    """
