# berkeleydbdu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database deferring index updates.

The berkeleydb_database module provides the database interface.

Prefer to use the berkeleydb_database module normally.

"""
from . import berkeleydb_database
from .core import _dbdu


class Database(_dbdu.Database, berkeleydb_database.Database):
    """Define deferred update Database class using berkeleydb module.

    Deferred update behaviour comes from the _dbdu.Database class.

    The Berkeley DB engine comes from the berkeleydb_database.Database
    class.
    """
