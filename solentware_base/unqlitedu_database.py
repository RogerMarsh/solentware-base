# unqlitedu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access an UnQLite database deferring index updates.

The unqlite_database module provides the database interface.

Prefer to use the unqlite_database module normally.

"""
from . import unqlite_database
from .core import _nosqldu


class Database(_nosqldu.Database, unqlite_database.Database):
    """Define deferred update Database class using unqlite_database module.

    Deferred update behaviour comes from the _nosqldu.Database class.

    The UnQLite engine comes from the unqlite_database.Database class.
    """
