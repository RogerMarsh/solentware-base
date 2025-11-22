# test___deferred_update.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Update databases in deferred update mode with sample real data."""

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
    import lmdb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdb = None
try:
    from dptdb import dptapi
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptapi = None

from . import _data_generator
from ..segmentsize import SegmentSize

try:
    from ... import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from ... import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
try:
    from ... import unqlitedu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlitedu_database = None
try:
    from ... import vedisdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedisdu_database = None
try:
    from ... import sqlite3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3du_database = None
try:
    from ... import apswdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apswdu_database = None
try:
    from ... import lmdbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdbdu_database = None
try:
    from ... import berkeleydbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydbdu_database = None
try:
    from ... import bsddb3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3du_database = None
try:
    from ... import dptdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptdu_database = None
try:
    from ... import ndbmdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbmdu_database = None
try:
    from ... import gnudu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnudu_database = None


class _Database(unittest.TestCase):
    def setUp(self):
        self._folder = "___update_test"
        self.dg = _data_generator._DataGenerator()
        self.generated_filespec = _data_generator.generate_filespec(self.dg)
        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(self._engine.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


def _open_database__no_files(self):
    # DPT, lmdb, ndbm, and gnu, do not do memory databases.
    self.database = self._D({}, segment_size_bytes=None)
    self.database.open_database()
    try:
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
    finally:
        self.database.close_database()


def _open_database__in_memory_no_txn_generated_filespec(self):
    # The default cachesize in Berkeley DB is too small for the number of
    # DB objects created: a Segmentation fault (core dumped) occurs when
    # the 13th index one is being opened.  See call to set_cachesize().
    # The environment argument is ignored for the other engines.
    # DPT, lmdb, ndbm, and gnu, do not do memory databases.
    self.database = self._D(
        self.generated_filespec,
        segment_size_bytes=None,
        environment={"bytes": 20000000},
    )
    self.database.open_database()
    try:
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.database.set_defer_update()
        _data_generator.populate(self.database, self.dg, transaction=False)
        self.database.unset_defer_update()
    finally:
        self.database.close_database()


def _open_database__in_file_no_txn_generated_filespec(self):
    # No cachesize problem for bsddb3 when database is not in memory.
    # Transaction for each record.
    self.database = self._D(
        self.generated_filespec,
        folder=self._folder,
        segment_size_bytes=None,
    )
    self.database.open_database()
    try:
        self.assertEqual(
            self.database.home_directory,
            os.path.join(os.getcwd(), self._folder),
        )
        if self._engine is not dptdu_database:
            self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
            self.assertEqual(
                self.database.database_file,
                os.path.join(os.getcwd(), self._folder, self._folder),
            )
        self.database.set_defer_update()
        _data_generator.populate(self.database, self.dg, transaction=False)
        self.database.unset_defer_update()
    finally:
        self.database.close_database()


class _DatabaseBerkeley(_Database):
    def tearDown(self):
        super().tearDown()
        logdir = "___memlogs_memory_db"
        if os.path.exists(logdir):
            for f in os.listdir(logdir):
                if f.startswith("log."):
                    os.remove(os.path.join(logdir, f))
            os.rmdir(logdir)
        if os.path.exists(self._folder):
            logdir = os.path.join(self._folder, "___logs_" + self._folder)
            if os.path.exists(logdir):
                for f in os.listdir(logdir):
                    if f.startswith("log."):
                        os.remove(os.path.join(logdir, f))
                os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


class _DatabaseDpt(_Database):
    def tearDown(self):
        super().tearDown()
        if os.path.exists(self._folder):
            for dptsys in os.path.join("dptsys", "dptsys"), "dptsys":
                logdir = os.path.join(self._folder, dptsys)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


class _DatabaseOther(_Database):
    def tearDown(self):
        super().tearDown()
        if os.path.exists(self._folder):
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


if unqlite:

    class _DatabaseUnqlite(_DatabaseOther):
        def setUp(self):
            self._engine = unqlitedu_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if vedis:

    class _DatabaseVedis(_DatabaseOther):
        def setUp(self):
            self._engine = vedisdu_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if berkeleydb:

    class _DatabaseBerkeleydb(_DatabaseBerkeley):
        def setUp(self):
            self._engine = berkeleydbdu_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if bsddb3:

    class _DatabaseBsddb3(_DatabaseBerkeley):
        def setUp(self):
            self._engine = bsddb3du_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if sqlite3:

    class _DatabaseSqlite3(_DatabaseOther):
        def setUp(self):
            self._engine = sqlite3du_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if apsw:

    class _DatabaseApsw(_DatabaseOther):
        def setUp(self):
            self._engine = apswdu_database
            super().setUp()

        test_01 = _open_database__no_files
        test_02 = _open_database__in_memory_no_txn_generated_filespec
        test_03 = _open_database__in_file_no_txn_generated_filespec


if lmdb:

    class _DatabaseLmdb(_DatabaseOther):
        def setUp(self):
            self._engine = lmdbdu_database
            super().setUp()

        test_03 = _open_database__in_file_no_txn_generated_filespec


if dptapi:

    class _DatabaseDptapi(_DatabaseDpt):
        def setUp(self):
            self._engine = dptdu_database
            super().setUp()

        test_03 = _open_database__in_file_no_txn_generated_filespec


if ndbm_module:

    class _DatabaseNdbm(_DatabaseOther):
        def setUp(self):
            self._engine = ndbmdu_database
            super().setUp()

        test_03 = _open_database__in_file_no_txn_generated_filespec


if gnu_module:

    class _DatabaseGnu(_DatabaseOther):
        def setUp(self):
            self._engine = gnudu_database
            super().setUp()

        test_03 = _open_database__in_file_no_txn_generated_filespec


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if unqlite:
        runner().run(loader(_DatabaseUnqlite))
    if vedis:
        runner().run(loader(_DatabaseVedis))
    if berkeleydb:
        runner().run(loader(_DatabaseBerkeleydb))
    if bsddb3:
        runner().run(loader(_DatabaseBsddb3))
    if sqlite3:
        runner().run(loader(_DatabaseSqlite3))
    if apsw:
        runner().run(loader(_DatabaseApsw))
    if lmdb:
        runner().run(loader(_DatabaseLmdb))
    if dptapi:
        runner().run(loader(_DatabaseDptapi))
    if ndbm_module:
        runner().run(loader(_DatabaseNdbm))
    if gnu_module:
        runner().run(loader(_DatabaseGnu))
