# apswdu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite database deferring index updates.

The apsw_database module provides the database interface.

Prefer to use the apsw_database module normally.

"""
from . import apsw_database
from .core import _sqlitedu


class Database(_sqlitedu.Database, apsw_database.Database):
    """Define deferred update Database class using apsw module.

    Deferred update behaviour comes from the _sqlitedu.Database class.

    The SQL engine comes from the apsw_database.Database class.
    """
