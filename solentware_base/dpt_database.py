# dpt_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database without deferring index updates.

Index updates are not deferred.  Use dptdu_database for deferred
updates, which will be a lot quicker when adding lots of new
records.

"""
from .core import _dpt


class Database(_dpt.Database):
    """Provide normal mode access to a DPT database.

    Normal mode means transactions and checkpoints are enabled, and
    index updates are not deferred.
    """
