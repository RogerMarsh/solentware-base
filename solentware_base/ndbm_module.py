# ndbm_module.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide Ndbm class to fit dbm.ndbm API to UnQLite and Vedis API."""
import dbm.ndbm


class NdbmError(Exception):
    """Report an attempt to open a 'memory-only' database."""


class Ndbm:
    """Provide an interface to dbm.ndbm similar to UnQLite and Vedis."""

    def __init__(self, path=None):
        """Trap memory-only database request and call dbm.ndbm.open.

        path defaults to None for compatibility with the unqlite.UnQLite(...)
        and vedis.Vedis(...) calls, where memory-only databases are possible.

        """
        if path is None:
            raise NdbmError("Memory-only databases not supported by dbm.ndbm")
        self._ndbm = dbm.ndbm.open(path, "c")

    def begin(self):
        """Do nothing: dbm.ndbm does not support transactions."""

    def rollback(self):
        """Do nothing: dbm.ndbm does not support transactions."""

    def commit(self):
        """Do nothing: dbm.ndbm does not support transactions.

        Python interface does not support explicit synchronization with disk.
        """

    def exists(self, key):
        """Return True if key is in dbm.ndbm database.

        Fit API provided by UnQLite and Vedis.

        """
        return key in self._ndbm

    def close(self):
        """Close dbm.ndbm database."""
        self._ndbm.close()

    def __contains__(self, item):
        """Return True if item is in dbm.ndbm database."""
        return item in self._ndbm

    def __getitem__(self, key):
        """Return value associated with key in dbm.ndbm database."""
        return self._ndbm[key]

    def __setitem__(self, key, value):
        """Associate key with value in dbm.ndbm database."""
        self._ndbm[key] = value

    def __delitem__(self, key):
        """Delete key, and associated value, from dbm.ndbm database."""
        del self._ndbm[key]
