# lmdbdu_database.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Symas LMMD database deferring index updates.

The lmdb_database module provides the database interface.

"""
from . import lmdb_database
from .core import _lmdbdu
from .core import _lmdb


class Database(lmdb_database.Database, _lmdbdu.Database, _lmdb.Database):
    """Define deferred update Database class using lmdb module.

    Deferred update behaviour comes from the _lmdbdu.Database class.

    The Symas LMMD engine comes from the lmdb_database.Database class.
    """
