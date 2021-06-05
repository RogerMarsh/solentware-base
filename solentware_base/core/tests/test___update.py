# test___update.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Update databases with sample real data."""

import unittest
import os
from ast import literal_eval

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis = None
try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
try:
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None
try:
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None
try:
    from dptdb import dptapi
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptapi = None

from . import _data_generator
from ..segmentsize import SegmentSize

try:
    from .. import _nosql
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    _nosql = None
try:
    from .. import _sqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    _sqlite = None
try:
    from .. import _db
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    _db = None
try:
    from .. import _dpt
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    _dpt = None
try:
    from ... import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from ... import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None


class _Database(unittest.TestCase):
    def setUp(self):
        self._folder = "___update_test"
        if dbe_module is unqlite:
            self._oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
            self._engine = _nosql
        elif dbe_module is vedis:
            self._oda = dbe_module, dbe_module.Vedis, None
            self._engine = _nosql
        elif dbe_module in {berkeleydb, bsddb3}:
            self._oda = (dbe_module.db,)
            self._engine = _db
        elif dbe_module is sqlite3:
            self._oda = (dbe_module,)
            self._engine = _sqlite
        elif dbe_module is apsw:
            self._oda = (dbe_module,)
            self._engine = _sqlite
        elif dbe_module is dptapi:
            self._oda = ()
            self._engine = _dpt
        elif dbe_module is ndbm_module:
            self._oda = dbe_module, dbe_module.Ndbm, None
            self._engine = _nosql
        elif dbe_module is gnu_module:
            self._oda = dbe_module, dbe_module.Gnu, None
            self._engine = _nosql
        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(self._engine.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb
        if dbe_module in {berkeleydb, bsddb3}:
            logdir = "___memlogs_memory_db"
            if os.path.exists(logdir):
                for f in os.listdir(logdir):
                    if f.startswith("log."):
                        os.remove(os.path.join(logdir, f))
                os.rmdir(logdir)
        if os.path.exists(self._folder):
            if dbe_module in {berkeleydb, bsddb3}:
                logdir = os.path.join(self._folder, "___logs_" + self._folder)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        if f.startswith("log."):
                            os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            if dbe_module is dptapi:
                for dptsys in os.path.join("dptsys", "dptsys"), "dptsys":
                    logdir = os.path.join(self._folder, dptsys)
                    if os.path.exists(logdir):
                        for f in os.listdir(logdir):
                            os.remove(os.path.join(logdir, f))
                        os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)

    def test_01_open_database__no_files(self):
        # DPT, ndbm, and gnu, do not do memory databases.
        if dbe_module not in (dptapi, ndbm_module, gnu_module):
            self.database = self._D({}, segment_size_bytes=None)
            self.database.open_database(*self._oda)
            try:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
                self.assertEqual(self.database.home_directory, None)
                self.assertEqual(self.database.database_file, None)
            finally:
                self.database.close_database()

    def test_02_open_database__in_memory_txn_generated_filespec(self):
        # The default cachesize in Berkeley DB is too small for the number of
        # DB objects created: a Segmentation fault (core dumped) occurs when
        # the 13th index one is being opened.  See call to set_cachesize().
        # The environment argument is ignored for the other engines.
        # DPT, ndbm, and gnu, do not do memory databases.
        if dbe_module not in (dptapi, ndbm_module, gnu_module):
            self.database = self._D(
                generated_filespec,
                segment_size_bytes=None,
                environment={"bytes": 20000000},
            )
            self.database.open_database(*self._oda)
            try:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
                self.assertEqual(self.database.home_directory, None)
                self.assertEqual(self.database.database_file, None)
                _data_generator.populate(self.database, dg)
            finally:
                self.database.close_database()

    def test_03_open_database__in_memory_no_txn_generated_filespec(self):
        # The default cachesize in Berkeley DB is too small for the number of
        # DB objects created: a Segmentation fault (core dumped) occurs when
        # the 13th index one is being opened.  See call to set_cachesize().
        # The environment argument is ignored for the other engines.
        # DPT, ndbm, and gnu, do not do memory databases.
        if dbe_module not in (dptapi, ndbm_module, gnu_module):
            self.database = self._D(
                generated_filespec,
                segment_size_bytes=None,
                environment={"bytes": 20000000},
            )
            self.database.open_database(*self._oda)
            try:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
                self.assertEqual(self.database.home_directory, None)
                self.assertEqual(self.database.database_file, None)
                _data_generator.populate(
                    self.database,
                    dg,
                    transaction=True
                    if dbe_module in {berkeleydb, bsddb3}
                    else False,
                )
            finally:
                self.database.close_database()

    def test_04_open_database__in_directory_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # Transaction for each record.
        self.database = self._D(
            generated_filespec,
            folder=self._folder,
            segment_size_bytes=None,
        )
        self.database.open_database(*self._oda)
        try:
            self.assertEqual(
                self.database.home_directory,
                os.path.join(os.getcwd(), self._folder),
            )
            if dbe_module is not dptapi:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
                self.assertEqual(
                    self.database.database_file,
                    os.path.join(os.getcwd(), self._folder, self._folder),
                )
            _data_generator.populate(self.database, dg)
        finally:
            self.database.close_database()

    def test_05_open_database__in_directory_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # Transaction for all records.
        self.database = self._D(
            generated_filespec,
            folder=self._folder,
            segment_size_bytes=None,
        )
        self.database.open_database(*self._oda)
        try:
            self.assertEqual(
                self.database.home_directory,
                os.path.join(os.getcwd(), self._folder),
            )
            if dbe_module is not dptapi:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
                self.assertEqual(
                    self.database.database_file,
                    os.path.join(os.getcwd(), self._folder, self._folder),
                )
            self.database.start_transaction()
            _data_generator.populate(self.database, dg, transaction=False)
            self.database.commit()
        finally:
            self.database.close_database()


if __name__ == "__main__":
    dg = _data_generator._DataGenerator()
    generated_filespec = _data_generator.generate_filespec(dg)
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (
        unqlite,
        vedis,
        berkeleydb,
        bsddb3,
        sqlite3,
        apsw,
        dptapi,
        ndbm_module,
        gnu_module,
    ):
        if dbe_module is None:
            continue
        runner().run(loader(_Database))
