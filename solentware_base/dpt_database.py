# dpt_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database created from a FileSpec() definition with the dptdb
module.

Index updates are not deferred.  Use dptdu_database for deferred updates, which
will be a lot quicker when adding lots of new records.

"""
from .core import _dpt


class Database(_dpt.Database):
    
    """Normal mode access to a DPT database.

    Method do_deferred_updates() is intended to run a deferred update job in
    a separate process using the dptdu_database.Database class.

    """
