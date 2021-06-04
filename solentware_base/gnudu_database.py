# gnudu_database.py
# Copyright (c) 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a dbm.gnu database deferring index updates.

The gnu_database module provides the database interface.

Prefer to use the gnu_database module normally.

"""
from . import gnu_database
from .core import _nosqldu


class Database(_nosqldu.Database, gnu_database.Database):
    """Define deferred update Database class using gnu_database module.

    Deferred update behaviour comes from the _nosqldu.Database class.

    The dbm.gnu engine comes from the gnu_database.Database class.
    """
