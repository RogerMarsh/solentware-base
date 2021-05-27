# ndbm_module.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide dummy functions to fit ndbm API to UnQLite and Vedis API.

"""
from dbm.ndbm import *


class NdbmError(Exception):
    pass


class Ndbm:

    def __init__(self, path=None):
        """Delegate to ndbm.open after trapping memory-only database requests.

        path defaults to None for compatibility with the unqlite.UnQLite(...)
        and vedis.Vedis(...) calls, where memory-only databases are possible.

        """
        if path is None:
            raise NdbmError('Memory-only databases not supported by dbm.ndbm')
        self._ndbm = open(path, 'c')

    def begin(self):
        """Do nothing: ndbm does not support transactions."""

    def rollback(self):
        """Do nothing: ndbm does not support transactions."""

    def commit(self):
        """Do nothing: ndbm does not support transactions.

        Python interface does not support explicit synchronization with disk.
        """

    def exists(self, key):
        """Return True if key is in ndbm database.

        Fit API provided by UnQLite and Vedis.

        """
        return key in self._ndbm

    def close(self):
        self._ndbm.close()

    def __contains__(self, item):
        return item in self._ndbm

    def __getitem__(self, key):
        return self._ndbm[key]

    def __setitem__(self, key, value):
        self._ndbm[key] = value

    def __delitem__(self, key):
        del self._ndbm[key]
