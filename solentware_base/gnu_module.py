# gnu_module.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide Gnu class to fit dbm.gnu API to UnQLite and Vedis API."""
import dbm.gnu


class GnuError(Exception):
    """Report an attempt to open a 'memory-only' database."""


class Gnu:
    """Provide an interface to dbm.gnu similar to UnQLite and Vedis."""

    def __init__(self, path=None):
        """Trap memory-only database request and call dbm.gnu.open.

        path defaults to None for compatibility with the unqlite.UnQLite(...)
        and vedis.Vedis(...) calls, where memory-only databases are possible.

        """
        # Flags 'cu' not 'c' so test_01_do_database_task is passed.
        if path is None:
            raise GnuError("Memory-only databases not supported by dbm.gnu")
        self._gnu = dbm.gnu.open(path, "cu")

    def begin(self):
        """Do nothing: dbm.gnu does not support transactions."""

    def rollback(self):
        """Do nothing: dbm.gnu does not support transactions."""

    def commit(self):
        """Delegate to dbm.gnu.sync: dbm.gnu does not support transactions.

        Synchronize database with memory.

        """
        self._gnu.sync()

    def exists(self, key):
        """Return True if key is in dbm.gnu database.

        Fit API provided by UnQLite and Vedis.

        """
        return key in self._gnu

    def close(self):
        """Close dbm.gnu database."""
        self._gnu.close()

    def __contains__(self, item):
        """Return True if item is in dbm.gnu database."""
        return item in self._gnu

    def __getitem__(self, key):
        """Return value associated with key in dbm.gnu database."""
        return self._gnu[key]

    def __setitem__(self, key, value):
        """Associate key with value in dbm.gnu database."""
        self._gnu[key] = value

    def __delitem__(self, key):
        """Delete key, and associated value, from dbm.gnu database."""
        del self._gnu[key]
