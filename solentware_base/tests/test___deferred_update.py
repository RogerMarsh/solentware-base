# test___deferred_update.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Update databases in deferred update mode with sample real data.

The one tests done is a copy of the similar test in ..core.tests.test___update
except that the default segement size is used rather than the 'testing' one.

The imported modules, apswdu_database for _sqlitedu and so forth, take care of
some minor differences exposed in the lower lever tests.
"""

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
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None
try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
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

from ..core.tests import _data_generator
from ..core.segmentsize import SegmentSize

try:
    from .. import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from .. import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
try:
    from .. import unqlitedu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlitedu_database = None
try:
    from .. import vedisdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedisdu_database = None
try:
    from .. import sqlite3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3du_database = None
try:
    from .. import apswdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apswdu_database = None
try:
    from .. import lmdbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdbdu_database = None
try:
    from .. import bsddb3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3du_database = None
try:
    from .. import berkeleydbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydbdu_database = None
try:
    from .. import dptdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptdu_database = None
try:
    from .. import ndbmdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbmdu_database = None
try:
    from .. import gnudu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnudu_database = None


class _Database(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(self._engine.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb
        if os.path.exists(self._folder):
            if self._folder in (
                "___update_test_bsddb3",
                "___update_test_berkeleydb",
            ):
                logdir = os.path.join(self._folder, "___logs_" + self._folder)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        if f.startswith("log."):
                            os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            if self._folder == "___update_test_dpt":
                for dptsys in os.path.join("dptsys", "dptsys"), "dptsys":
                    logdir = os.path.join(self._folder, dptsys)
                    if os.path.exists(logdir):
                        for f in os.listdir(logdir):
                            os.remove(os.path.join(logdir, f))
                        os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)

    def t01_open_database__in_directory_no_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # No problem on OpenBSD for vedis as in .test___update module.
        # Transaction for each record.
        self.database = self._D(
            generated_filespec,
            folder=self._folder,
        )
        self.database.open_database()
        try:
            self.assertEqual(
                self.database.home_directory,
                os.path.join(os.getcwd(), self._folder),
            )
            if self._folder != "___update_test_dpt":
                self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
                self.assertEqual(
                    self.database.database_file,
                    os.path.join(os.getcwd(), self._folder, self._folder),
                )
            self.database.set_defer_update()
            _data_generator.populate(self.database, dg, transaction=False)
            self.database.unset_defer_update()
        finally:
            self.database.close_database()


if unqlite:

    class _DatabaseUnqlite(_Database):
        def setUp(self):
            self._folder = "___update_test_unqlite"
            self._engine = unqlitedu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if vedis:

    class _DatabaseVedis(_Database):
        def setUp(self):
            self._folder = "___update_test_vedis"
            self._engine = vedisdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if bsddb3:

    class _DatabaseBsddb3(_Database):
        def setUp(self):
            self._folder = "___update_test_bsddb3"
            self._engine = bsddb3du_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if berkeleydb:

    class _DatabaseBerkeleydb(_Database):
        def setUp(self):
            self._folder = "___update_test_berkeleydb"
            self._engine = berkeleydbdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if sqlite3:

    class _DatabaseSqlite3(_Database):
        def setUp(self):
            self._folder = "___update_test_sqlite3"
            self._engine = sqlite3du_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if apsw:

    class _DatabaseApsw(_Database):
        def setUp(self):
            self._folder = "___update_test_apsw"
            self._engine = apswdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if lmdb:

    class _DatabaseLmdb(_Database):
        def setUp(self):
            self._folder = "___update_test_lmdb"
            self._engine = lmdbdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if dptapi:

    class _DatabaseDptapi(_Database):
        def setUp(self):
            self._folder = "___update_test_dpt"
            self._engine = dptdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if ndbm_module:

    class _DatabaseNdbm(_Database):
        def setUp(self):
            self._folder = "___update_test_ndbm"
            self._engine = ndbmdu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


if gnu_module:

    class _DatabaseGnu(_Database):
        def setUp(self):
            self._folder = "___update_test_gnu"
            self._engine = gnudu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_no_txn_generated_filespec
        )


dg = _data_generator._DataGenerator()
generated_filespec = _data_generator.generate_filespec(dg)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if unqlite:
        runner().run(loader(_DatabaseUnqlite))
    if vedis:
        runner().run(loader(_DatabaseVedis))
    if bsddb3:
        runner().run(loader(_DatabaseBsddb3))
    if berkeleydb:
        runner().run(loader(_DatabaseBerkeleydb))
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
