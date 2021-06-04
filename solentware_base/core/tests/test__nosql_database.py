# test__nosql_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosql _database tests

Open and close a database on a file, not in memory.

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
from .. import recordset
from ..segmentsize import SegmentSize
from ..wherevalues import ValuesClause


class _NoSQL(unittest.TestCase):
    def setUp(self):

        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set differently for these engines.
        if dbe_module is unqlite:
            self._oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
        elif dbe_module is vedis:
            self._oda = dbe_module, dbe_module.Vedis, None
        elif dbe_module is ndbm_module:
            self._oda = dbe_module, dbe_module.Ndbm, None
        elif dbe_module is gnu_module:
            self._oda = dbe_module, dbe_module.Gnu, None

        class _D(_nosql.Database):
            pass

        self._D = _D
        self._directory = os.path.join(
            os.path.dirname(__file__), dbe_module.__name__
        )

    def tearDown(self):
        self.database = None
        self._D = None
        if dbe_module is ndbm_module:
            os.remove(
                os.path.join(
                    self._directory, ".".join((dbe_module.__name__, "db"))
                )
            )
        elif dbe_module is gnu_module:
            os.remove(os.path.join(self._directory, dbe_module.__name__))
        else:
            os.remove(os.path.join(self._directory, dbe_module.__name__))
        os.rmdir(os.path.join(self._directory))


class Database_open_database(_NoSQL):
    def test_09(self):
        self.assertEqual(
            os.path.exists(os.path.join(self._directory, dbe_module.__name__)),
            False,
        )
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
        self.assertEqual(
            self.database.home_directory,
            os.path.join(os.path.dirname(__file__), dbe_module.__name__),
        )
        self.assertEqual(
            self.database.database_file,
            os.path.join(
                os.path.dirname(__file__),
                dbe_module.__name__,
                dbe_module.__name__,
            ),
        )
        self.assertEqual(
            self.database.table,
            {
                "file1": ["1"],
                "___control": "0",
                "file1_field1": ["1_1"],
                "file2": ["2"],
                "file3": ["3"],
                "file3_field2": ["3_1"],
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
        self.assertEqual(self.database.ebm_segment_count, {})
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _nosql.ExistenceBitmapControl)
        self.database.close_database()
        if dbe_module is ndbm_module:
            path = os.path.join(
                self._directory, ".".join((dbe_module.__name__, "db"))
            )
        elif dbe_module is gnu_module:
            path = os.path.join(self._directory, dbe_module.__name__)
        else:
            path = os.path.join(self._directory, dbe_module.__name__)
        self.assertEqual(os.path.exists(path), True)
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


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in unqlite, vedis, ndbm_module, gnu_module:
        if dbe_module is None:
            continue
        runner().run(loader(Database_open_database))
