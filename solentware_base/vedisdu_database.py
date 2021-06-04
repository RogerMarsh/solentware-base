# vedisdu_database.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Vedis database deferring index updates.

The vedis_database module provides the database interface.

Prefer to use the vedis_database module normally.

"""
from . import vedis_database
from .core import _nosqldu


class Database(_nosqldu.Database, vedis_database.Database):
    """Define deferred update Database class using vedis_database module.

    Deferred update behaviour comes from the _nosqldu.Database class.

    The Vedis engine comes from the vedis_database.Database class.
    """
