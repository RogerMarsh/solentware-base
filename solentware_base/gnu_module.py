# gnu_module.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide dummy functions to fit gnu API to UnQLite and Vedis API.

"""
from dbm.gnu import *


class GnuError(Exception):
    pass


class Gnu:

    def __init__(self, path=None):
        """Delegate to gnu.open after trapping memory-only database requests.

        path defaults to None for compatibility with the unqlite.UnQLite(...)
        and vedis.Vedis(...) calls, where memory-only databases are possible.

        """
        # Flags 'cu' not 'c' so test_01_do_database_task is passed.
        if path is None:
            raise GnuError('Memory-only databases not supported by dbm.gnu')
        self._gnu = open(path, 'cu')

    def begin(self):
        """Do nothing: _gnu does not support transactions."""

    def rollback(self):
        """Do nothing: _gnu does not support transactions."""

    def commit(self):
        """Delegate to dbm.gnu.gdbm.sync: _gnu does not support transactions.

        Synchronize database with memory.

        """
        self._gnu.sync()

    def exists(self, key):
        """Return True if key is in ndbm database.

        Fit API provided by UnQLite and Vedis.

        """
        return key in self._gnu

    def close(self):
        self._gnu.close()

    def __contains__(self, item):
        return item in self._gnu

    def __getitem__(self, key):
        return self._gnu[key]

    def __setitem__(self, key, value):
        self._gnu[key] = value

    def __delitem__(self, key):
        del self._gnu[key]
