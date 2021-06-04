# ndbmdu_database.py
# Copyright (c) 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a dbm.ndbm database deferring index updates.

The ndbm_database module provides the database interface.

Prefer to use the ndbm_database module normally.

"""
from . import ndbm_database
from .core import _nosqldu


class Database(_nosqldu.Database, ndbm_database.Database):
    """Define deferred update Database class using ndbm_database module.

    Deferred update behaviour comes from the _nosqldu.Database class.

    The dbm.ndbm engine comes from the ndbm_database.Database class.
    """
