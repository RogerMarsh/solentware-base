# test__nosql_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosql _database tests for gnudbm, ndbm, unqlite, and vedis, interfaces.

Open and close a database on a file, not in memory.

"""

import unittest
import os

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis = None

try:
    from ... import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from ... import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
from .. import _nosql
from .. import tree
from .. import filespec
from ..segmentsize import SegmentSize


class _NoSQL(unittest.TestCase):
    def setUp(self):
        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set differently for these engines.

        class _D(_nosql.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None


class Database_open_database:
    def t09(self):
        self.detail_does_not_exist_t09()
        self.database = self._D(
            filespec.FileSpec(
                **{"file1": {"field1"}, "file2": (), "file3": {"field2"}}
            ),
            folder=self._directory,
        )
        # No tree for field2 in file3 (without a full FileSpec instance).
        self.database.specification["file3"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.detail_created_t09()
        self.assertEqual(
            self.database.table,
            {
                "file1": "1",
                "___control": "0",
                "file1_field1": "1_1",
                "file2": "2",
                "file3": "3",
                "file3_field2": "3_1",
            },
        )
        self.assertEqual(
            self.database.segment_table,
            {"file1_field1": "1_1_0", "file3_field2": "3_1_0"},
        )
        self.assertEqual(
            self.database.segment_records,
            {"file1_field1": "1_1_1", "file3_field2": "3_1_1"},
        )
        self.assertEqual(
            [k for k in self.database.trees.keys()], ["file1_field1"]
        )
        self.assertIsInstance(self.database.trees["file1_field1"], tree.Tree)
        self.assertEqual(self.database.ebm_control["file1"]._file, "1")
        self.assertEqual(
            self.database.ebm_control["file1"].ebm_table, "1_0__ebm"
        )
        self.assertEqual(self.database.ebm_control["file2"]._file, "2")
        self.assertEqual(
            self.database.ebm_control["file2"].ebm_table, "2_0__ebm"
        )
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _nosql.ExistenceBitmapControl)
        self.database.close_database()
        self.detail_exists_t09()
        self.database = self._D(
            filespec.FileSpec(
                **{"file1": {"field1"}, "file2": (), "file3": {"field2"}}
            ),
            folder=self._directory,
        )
        self.database.specification["file3"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)
        self.database.close_database()

    def detail_does_not_exist(self, module):
        self.assertEqual(
            os.path.exists(os.path.join(self._directory, module.__name__)),
            False,
        )

    def detail_created(self, module):
        self.assertEqual(
            self.database.home_directory,
            os.path.join(os.path.dirname(__file__), module.__name__),
        )
        self.assertEqual(
            self.database.database_file,
            os.path.join(
                os.path.dirname(__file__),
                module.__name__,
                module.__name__,
            ),
        )


if gnu_module:

    class _NoSQLGnu(_NoSQL):
        def setUp(self):
            self._oda = gnu_module, gnu_module.Gnu, None
            super().setUp()
            self._directory = os.path.join(
                os.path.dirname(__file__), gnu_module.__name__
            )

        def tearDown(self):
            super().tearDown()
            os.remove(os.path.join(self._directory, gnu_module.__name__))
            os.rmdir(os.path.join(self._directory))

    class Database_open_databaseGnu(_NoSQLGnu):
        test_09 = Database_open_database.t09

        def detail_exists_t09(self):
            path = os.path.join(self._directory, gnu_module.__name__)
            self.assertEqual(os.path.exists(path), True)

        def detail_does_not_exist_t09(self):
            Database_open_database.detail_does_not_exist(self, gnu_module)

        def detail_created_t09(self):
            Database_open_database.detail_created(self, gnu_module)


if ndbm_module:

    class _NoSQLNdbm(_NoSQL):
        def setUp(self):
            self._oda = ndbm_module, ndbm_module.Ndbm, None
            super().setUp()
            self._directory = os.path.join(
                os.path.dirname(__file__), ndbm_module.__name__
            )

        def tearDown(self):
            super().tearDown()
            os.remove(
                os.path.join(
                    self._directory, ".".join((ndbm_module.__name__, "db"))
                )
            )
            os.rmdir(os.path.join(self._directory))

    class Database_open_databaseNdbm(_NoSQLNdbm):
        test_09 = Database_open_database.t09

        def detail_exists_t09(self):
            path = os.path.join(
                self._directory, ".".join((ndbm_module.__name__, "db"))
            )
            self.assertEqual(os.path.exists(path), True)

        def detail_does_not_exist_t09(self):
            Database_open_database.detail_does_not_exist(self, ndbm_module)

        def detail_created_t09(self):
            Database_open_database.detail_created(self, ndbm_module)


if unqlite:

    class _NoSQLUnqlite(_NoSQL):
        def setUp(self):
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()
            self._directory = os.path.join(
                os.path.dirname(__file__), unqlite.__name__
            )

        def tearDown(self):
            super().tearDown()
            os.remove(os.path.join(self._directory, unqlite.__name__))
            os.rmdir(os.path.join(self._directory))

    class Database_open_databaseUnqlite(_NoSQLUnqlite):
        test_09 = Database_open_database.t09

        def detail_exists_t09(self):
            path = os.path.join(self._directory, unqlite.__name__)
            self.assertEqual(os.path.exists(path), True)

        def detail_does_not_exist_t09(self):
            Database_open_database.detail_does_not_exist(self, unqlite)

        def detail_created_t09(self):
            Database_open_database.detail_created(self, unqlite)


if vedis:

    class _NoSQLVedis(_NoSQL):
        def setUp(self):
            self._oda = vedis, vedis.Vedis, None
            super().setUp()
            self._directory = os.path.join(
                os.path.dirname(__file__), vedis.__name__
            )

        def tearDown(self):
            super().tearDown()
            os.remove(os.path.join(self._directory, vedis.__name__))
            os.rmdir(os.path.join(self._directory))

    class Database_open_databaseVedis(_NoSQLVedis):
        test_09 = Database_open_database.t09

        def detail_exists_t09(self):
            path = os.path.join(self._directory, vedis.__name__)
            self.assertEqual(os.path.exists(path), True)

        def detail_does_not_exist_t09(self):
            Database_open_database.detail_does_not_exist(self, vedis)

        def detail_created_t09(self):
            Database_open_database.detail_created(self, vedis)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if gnu_module:
        runner().run(loader(Database_open_databaseGnu))
    if ndbm_module:
        runner().run(loader(Database_open_databaseNdbm))
    if unqlite:
        runner().run(loader(Database_open_databaseUnqlite))
    if vedis:
        runner().run(loader(Database_open_databaseVedis))
