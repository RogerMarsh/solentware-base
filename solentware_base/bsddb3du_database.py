# bsddb3du_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database deferring index updates.

The bsddb3_database module provides the database interface.

Prefer to use the bsddb3_database module normally.

"""
from . import bsddb3_database
from .core import _dbdu


class Database(_dbdu.Database, bsddb3_database.Database):
    """Define deferred update Database class using bsddb3 module.

    Deferred update behaviour comes from the _dbdu.Database class.

    The Berkeley DB engine comes from the bsddb3_database.Database class.
    """
