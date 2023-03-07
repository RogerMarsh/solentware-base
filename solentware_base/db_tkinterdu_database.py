# db_tkinterdu_database.py
# Copyright (c) 2022 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database deferring index updates.

The db_tkinter_database module provides the database interface.

Prefer to use the tkinter_database module normally.

"""
from . import db_tkinter_database
from .core import _dbdu_tkinter
from .core import _db_tkinter


class Database(
    db_tkinter_database.Database, _dbdu_tkinter.Database, _db_tkinter.Database
):
    """Define deferred update Database class using tkinter module.

    Deferred update behaviour comes from the _dbdu_tkinter.Database class.

    The Berkeley DB engine comes from the db_tkinter_database.Database
    class using the Tcl interface to Berkeley DB.
    """
