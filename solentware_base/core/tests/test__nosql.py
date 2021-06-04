# test__nosql.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosql _database tests

The rest of this docstring probably belongs a lot higher up the package tree.

Originally unit tests were fitted to the packages long after initial write,
because proof of testing is a good thing.

However I have ended up using them in shortish 'code-test' cycles to check the
method just newly written or amended actually succceeds in running, once the
module has a fairly stable structure and enough of it exists to run.  Largely
as a consequence of going for relative imports within a package whereever
possible: then python -m <test> is a convenient universal way of seeing if it
runs.  I avoided relative imports for a long time because they do not fit well
with idle.

Sometimes a unit test will have an attempt at exhaustive testing too.
"""
# The _nosql and test__nosql  modules are written by copying _sqlite and
# test__sqlite, then change test__nosql to do unqlite or vedis things one test
# at a time and replace the SQLite things in _nosql as they get hit.

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

_NDBM_TEST_ROOT = "___ndbm_test_nosql"
_GNU_TEST_ROOT = "___gnu_test_nosql"


if ndbm_module:

    class Ndbm(ndbm_module.Ndbm):
        # test__nosql assumes database modules support memory-only databases,
        # but ndbm does not support them.
        def __init__(self, path=None):
            if path is None:
                path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            super().__init__(path=path)


if gnu_module:

    class Gnu(gnu_module.Gnu):
        # test__nosql assumes database modules support memory-only databases,
        # but gnu does not support them.
        def __init__(self, path=None):
            if path is None:
                path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            super().__init__(path=path)


class _NoSQL(unittest.TestCase):
    # The sets of tests are run inside a loop for unqlite and vedis, and some
    # tests change SegmentSize.db_segment_size_bytes, so reset it to the
    # initial value in tearDown().

    def setUp(self):

        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set differently for these engines.
        if dbe_module is unqlite:
            self._oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
        elif dbe_module is vedis:
            self._oda = dbe_module, dbe_module.Vedis, None
        elif dbe_module is ndbm_module:
            self._oda = dbe_module, Ndbm, None
        elif dbe_module is gnu_module:
            self._oda = dbe_module, Gnu, None

        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(_nosql.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb

        # I have no idea why the database teardown for ndbm has to be like so:
        if dbe_module is ndbm_module:
            path = os.path.join(
                os.path.dirname(__file__), ".".join((_NDBM_TEST_ROOT, "db"))
            )
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)
            elif os.path.isfile(
                path
            ):  # Most tests, other two each have a few.
                os.remove(path)
            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

        # I have no idea why the database teardown for gnu has to be like so:
        if dbe_module is gnu_module:
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            if os.path.isfile(path):
                os.remove(path)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)


class Database___init__(_NoSQL):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 2 to 5 positional arguments ",
                    "but 6 were given",
                )
            ),
            self._D,
            *(None, None, None, None, None),
        )

    def test_02(self):
        # Matches 'type object' before Python 3.9 but class name otherwise.
        t = r"(?:type object|solentware_base\.core\.filespec\.FileSpec\(\))"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    t,
                    " argument after \*\* must be a mapping, ",
                    "not NoneType",
                )
            ),
            self._D,
            *(None,),
        )
        self.assertIsInstance(self._D({}), self._D)
        self.assertIsInstance(self._D(filespec.FileSpec()), self._D)

    def test_03(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database folder name {} is not valid",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def test_04(self):
        database = self._D({}, folder="a")
        self.assertEqual(
            sorted(database.__dict__.keys()),
            [
                "_initial_segment_size_bytes",
                "_real_segment_size_bytes",
                "_use_specification_items",
                "database_file",
                "dbenv",
                "ebm_control",
                "ebm_segment_count",
                "home_directory",
                "segment_records",
                "segment_size_bytes",
                "segment_table",
                "specification",
                "table",
                "table_data",
                "trees",
            ],
        )
        self.assertIsInstance(database, self._D)
        self.assertEqual(os.path.basename(database.home_directory), "a")
        self.assertEqual(os.path.basename(database.database_file), "a")
        self.assertEqual(
            os.path.basename(os.path.dirname(database.database_file)), "a"
        )
        self.assertEqual(database.specification, {})
        self.assertEqual(database.segment_size_bytes, 4000)
        self.assertEqual(database.dbenv, None)
        self.assertEqual(database.table, {})
        self.assertEqual(database.segment_table, {})
        self.assertEqual(database.segment_records, {})
        self.assertEqual(database.ebm_control, {})
        self.assertEqual(database.ebm_segment_count, {})
        self.assertEqual(database.trees, {})
        self.assertEqual(database._real_segment_size_bytes, False)
        self.assertEqual(database._initial_segment_size_bytes, 4000)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)

    def test_05(self):
        database = self._D({})
        self.assertEqual(database.home_directory, None)
        self.assertEqual(database.database_file, None)

    # This combination of folder and segment_size_bytes arguments is used for
    # unittests, except for one to see a non-memory database with a realistic
    # segment size.
    def test_06(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)


# Transaction methods do not raise exceptions if called when no database open
# but do nothing.
class Database_transaction_methods(_NoSQL):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_01_start_transaction(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.start_transaction()

    def test_02_backout(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.backout()

    def test_03_commit(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.commit()

    def test_04(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "start_transaction\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.database.start_transaction,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "backout\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.database.backout,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "commit\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.database.commit,
            *(None,),
        )


# Methods which do not require database to be open.
class DatabaseInstance(_NoSQL):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_01_validate_segment_size_bytes(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_validate_segment_size_bytes\(\) missing 1 required ",
                    "positional argument: 'segment_size_bytes'",
                )
            ),
            self.database._validate_segment_size_bytes,
        )
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database segment size must be an int",)),
            self.database._validate_segment_size_bytes,
            *("a",),
        )
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database segment size must be more than 0",)),
            self.database._validate_segment_size_bytes,
            *(0,),
        )
        self.assertEqual(
            self.database._validate_segment_size_bytes(None), None
        )
        self.assertEqual(self.database._validate_segment_size_bytes(1), None)

    def test_02_encode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "encode_record_number\(\) missing 1 required ",
                    "positional argument: 'key'",
                )
            ),
            self.database.encode_record_number,
        )
        self.assertEqual(self.database.encode_record_number(1), "1")

    def test_03_decode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "decode_record_number\(\) missing 1 required ",
                    "positional argument: 'skey'",
                )
            ),
            self.database.decode_record_number,
        )
        self.assertEqual(self.database.decode_record_number("1"), 1)

    def test_04_encode_record_selector(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "encode_record_selector\(\) missing 1 required ",
                    "positional argument: 'key'",
                )
            ),
            self.database.encode_record_selector,
        )
        self.assertEqual(self.database.encode_record_selector("a"), "a")

    def test_05_make_recordset(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_nil\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.database.recordlist_nil,
            *(None, None, None),
        )
        self.assertIsInstance(
            self.database.recordlist_nil("a"), recordset.RecordList
        )


# Memory databases are used for these tests.
class Database_open_database(_NoSQL):
    def test_01(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "open_database\(\) takes from 4 to 5 positional arguments ",
                    "but 6 were given",
                )
            ),
            self.database.open_database,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "close_database\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.database.close_database,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "close_database_contexts\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.database.close_database_contexts,
            *(None, None),
        )

    def test_02(self):
        self.database = self._D({})
        self.database.open_database(*self._oda)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self._oda[1])

    def test_03(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self._oda[1])

    def test_04_close_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)

    def test_05_close_database_contexts(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)

    def test_06(self):
        self.database = self._D({"file1": {"field1"}})
        self.database.open_database(*self._oda)
        self.check_specification()

    def test_07(self):
        self.database = self._D(filespec.FileSpec(**{"file1": {"field1"}}))
        self.database.open_database(*self._oda)
        self.check_specification()

    def test_08(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}})
        )
        self.database.open_database(*self._oda, files={"file1"})
        self.check_specification()

    def test_09(self):
        self.database = self._D(
            filespec.FileSpec(
                **{"file1": {"field1"}, "file2": (), "file3": {"field2"}}
            )
        )
        # No tree for field2 in file3 (without a full FileSpec instance).
        self.database.specification["file3"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)
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

    # Comment in _sqlite.py suggests this method is not needed.
    def test_12_is_database_file_active(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        self.assertEqual(d.is_database_file_active("file1"), False)
        d.open_database(*self._oda)
        self.assertEqual(d.is_database_file_active("file1"), True)

    def check_specification(self):
        self.assertEqual(
            self.database.table,
            {
                "file1": ["1"],
                "___control": "0",
                "file1_field1": ["1_1"],
            },
        )
        self.assertEqual(
            self.database.segment_table, {"file1_field1": "1_1_0"}
        )
        self.assertEqual(
            self.database.segment_records, {"file1_field1": "1_1_1"}
        )
        self.assertEqual(
            [k for k in self.database.trees.keys()], ["file1_field1"]
        )
        self.assertIsInstance(self.database.trees["file1_field1"], tree.Tree)
        self.assertEqual(self.database.ebm_control["file1"]._file, "1")
        self.assertEqual(
            self.database.ebm_control["file1"].ebm_table, "1_0__ebm"
        )
        self.assertEqual(self.database.ebm_segment_count, {})
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _nosql.ExistenceBitmapControl)


# Memory databases are used for these tests.
# This one has to look like a real application (almost).
# Do not need to catch the self.__class__.SegmentSizeError exception in
# _ED.open_database() method.
class Database_do_database_task(unittest.TestCase):
    # The sets of tests are run inside a loop for sqlite3 and apsw, and some
    # tests in this set change SegmentSize.db_segment_size_bytes, so reset it
    # to the initial value in tearDown().
    # _NoSQL does this, but Database_do_database_task is not based on it.

    def setUp(self):

        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set diferrently for these engines.
        if dbe_module is unqlite:
            _oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
        elif dbe_module is vedis:
            _oda = dbe_module, dbe_module.Vedis, None
        elif dbe_module is ndbm_module:
            _oda = dbe_module, dbe_module.Ndbm, None
        elif dbe_module is gnu_module:
            _oda = dbe_module, dbe_module.Gnu, None

        self._ssb = SegmentSize.db_segment_size_bytes

        class _ED(_nosql.Database):
            def open_database(self, **k):
                super().open_database(*_oda, **k)

        class _AD(_ED):
            def __init__(self, folder, **k):
                super().__init__({}, folder, **k)

        self._AD = _AD

    def tearDown(self):
        self.database = None
        self._AD = None
        SegmentSize.db_segment_size_bytes = self._ssb

        # I have no idea why the database teardown for gnu has to be like so:
        if dbe_module is gnu_module:
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            if os.path.isfile(path):
                os.remove(path)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

    def test_01_do_database_task(self):
        def m(*a, **k):
            pass

        if dbe_module in (ndbm_module,):
            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
        elif dbe_module in (gnu_module,):
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
        else:
            path = None
        self.database = self._AD(path)
        d = self.database
        d.open_database()
        self.assertEqual(d.do_database_task(m), None)

    def test_02_do_database_task(self):
        def m(*a, **k):
            pass

        if dbe_module in (ndbm_module,):
            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
        elif dbe_module in (gnu_module,):
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
        else:
            path = None
        self.database = self._AD(path)
        d = self.database
        self.assertEqual(d.do_database_task(m), None)


# Memory databases are used for these tests.
# Use the 'testing only' segment size for convenience of setup and eyeballing.
class _NoSQLOpen(_NoSQL):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}}),
            segment_size_bytes=None,
        )
        self.database.specification["file2"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class DatabaseTransactions(_NoSQLOpen):
    def test_01(self):
        self.database.start_transaction()
        self.assertEqual(self.database.start_transaction(), None)

    def test_02(self):
        self.database.start_transaction()
        self.assertEqual(self.database.backout(), None)

    def test_03(self):
        self.database.start_transaction()
        self.assertEqual(self.database.commit(), None)

    def test_04(self):
        self.assertEqual(self.database.backout(), None)

    def test_05(self):
        self.assertEqual(self.database.commit(), None)


class Database_put_replace_delete(_NoSQLOpen):
    # These tests are copied and modified from test__sqlite.
    # The tests on put assume a correct add_record_to_ebm method, and those on
    # delete assume a correct remove_record_from_ebm() method because the
    # bitmaps are used to identify the highest record number allocated.
    # UnQLite and Vedis do not have the notion of a record number like the
    # rowid in a SQLite3 table, or the key of a Recno database in Berkeley DB,
    # or the record number of a DPT file.

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "put\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'",
                )
            ),
            self.database.put,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "replace\(\) missing 4 required positional arguments: ",
                    "'file', 'key', 'oldvalue', and 'newvalue'",
                )
            ),
            self.database.replace,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'",
                )
            ),
            self.database.delete,
        )

    def test_02_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)

    def test_03_put(self):
        self.assertEqual("1__2" in self.database.dbenv, False)
        self.assertEqual(self.database.put("file1", 2, "new value"), None)
        self.database.add_record_to_ebm("file1", 2)
        self.assertEqual("1_0_2" in self.database.dbenv, True)
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 3)

    def test_04_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)
        self.database.add_record_to_ebm("file1", 0)
        self.assertEqual(self.database.put("file1", 0, "renew value"), None)
        recno = self.database.put("file1", None, "other value")
        self.assertEqual(recno, 1)

    def test_05_replace(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("old value"), repr("new value")
            ),
            None,
        )
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.database.dbenv["1_1"] = repr(None)
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("old value"), repr("new value")
            ),
            None,
        )
        self.assertEqual("1_1" in self.database.dbenv, True)

    def test_06_replace(self):
        self.database.dbenv["1_0_1"] = repr("old value")
        self.assertEqual(self.database.dbenv["1_0_1"], b"'old value'")
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("old value"), repr("new value")
            ),
            None,
        )
        self.assertEqual(self.database.dbenv["1_0_1"], b"'new value'")

    def test_07_replace(self):
        self.database.dbenv["1_1"] = repr("old value")
        self.assertEqual(self.database.dbenv["1_1"], b"'old value'")
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("new value"), repr("same value")
            ),
            None,
        )
        self.assertEqual(self.database.dbenv["1_1"], b"'old value'")

    def test_08_delete(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual(
            self.database.delete("file1", 1, repr("new value")), None
        )
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.database.dbenv["1_1"] = repr(None)
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.delete("file1", 1, repr("new value")), None
        )
        self.assertEqual("1_1" in self.database.dbenv, True)

    def test_09_delete(self):
        self.database.dbenv["1_0_1"] = repr("new value")
        self.database.add_record_to_ebm("file1", 0)
        self.assertEqual("1_0_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.delete("file1", 1, repr("new value")), None
        )
        self.database.remove_record_from_ebm("file1", 0)
        self.assertEqual("1_0_1" in self.database.dbenv, False)

    def test_10_delete(self):
        self.database.dbenv["1_1"] = repr("new value")
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.delete("file1", 1, repr("old value")), None
        )
        self.assertEqual("1_1" in self.database.dbenv, True)


# These tests need fully working put, replace, and delete, methods.
class Database_methods(_NoSQLOpen):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_primary_record\(\) missing 2 required positional ",
                    "arguments: 'file' and 'key'",
                )
            ),
            self.database.get_primary_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "remove_record_from_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'deletekey'",
                )
            ),
            self.database.remove_record_from_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "add_record_to_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'putkey'",
                )
            ),
            self.database.add_record_to_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_high_record\(\) missing 1 required ",
                    "positional argument: 'file'",
                )
            ),
            self.database.get_high_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_record_number\(\) takes from 2 to 4 ",
                    "positional arguments but 5 were given",
                )
            ),
            self.database.recordlist_record_number,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_record_number_range\(\) takes from 2 to 5 ",
                    "positional arguments but 6 were given",
                )
            ),
            self.database.recordlist_record_number_range,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_ebm\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given",
                )
            ),
            self.database.recordlist_ebm,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_table_connection\(\) missing 1 required positional ",
                    "argument: 'file'",
                )
            ),
            self.database.get_table_connection,
        )

    def test_02_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", None), None)

    def test_03_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", 1), None)

    def test_04_get_primary_record(self):
        self.database.put("file1", None, repr("new value"))
        self.assertEqual(
            self.database.get_primary_record("file1", 0), (0, "'new value'")
        )

    def test_05_remove_record_from_ebm(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "Existence bit map for segment does not exist",
            self.database.remove_record_from_ebm,
            *("file1", 2),
        )

    def test_06_remove_record_from_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(
            self.database.remove_record_from_ebm("file1", 2), (0, 2)
        )

    def test_07_add_record_to_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(self.database.add_record_to_ebm("file1", 4), (0, 4))

    def test_08_get_high_record(self):
        self.assertEqual(self.database.get_high_record("file1"), None)

    def test_14_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1"),
            recordset.RecordList,
        )

    def test_15_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1", key=500),
            recordset.RecordList,
        )

    def test_16_recordset_record_number(self):
        dbenv = self.database.dbenv
        self.assertEqual(dbenv.exists("1_0"), False)
        self.assertEqual(dbenv["1_0__ebm"], b"[]")
        self.assertEqual(dbenv.exists("1_0__ebm_0"), False)
        dbenv["1_0"] = repr("Some value")
        self.database.ebm_control["file1"].append_ebm_segment(
            b"\x80" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1),
            self.database.dbenv,
        )
        rl = self.database.recordlist_record_number("file1", key=0)
        self.assertIsInstance(rl, recordset.RecordList)
        self.assertEqual(rl.count_records(), 1)

    def test_17_recordset_record_number_range(self):
        self.assertIsInstance(
            self.database.recordlist_record_number_range("file1"),
            recordset.RecordList,
        )

    def test_18_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=0, keyend=2000
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        )

    def test_19_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keystart=10)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"\x00\x3f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        )

    def test_20_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keyend=35)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"\xff\xff\xff\xff\xe0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )

    def test_21_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=10, keyend=35
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"\x00\x3f\xff\xff\xe0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )

    def test_22_recordset_record_number_range(self):
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=170, keyend=350
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(
            rs[1].tobytes(),
            b"\x00\x00\x00\x00\x00\x3f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        )
        self.assertEqual(
            rs[2].tobytes(),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfc\x00\x00\x00\x00",
        )

    def test_23_recordset_record_number_range(self):
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=350, keyend=170
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_24_recordset_ebm(self):
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def test_25_recordset_ebm(self):
        self.create_ebm()
        rlebm = self.database.recordlist_ebm("file1")
        self.assertIsInstance(rlebm, recordset.RecordList)
        self.assertEqual(rlebm.sorted_segnums, [0])

    def test_26_get_table_connection(self):
        if dbe_module is unqlite:
            object_class = unqlite.UnQLite
        elif dbe_module is vedis:
            object_class = vedis.Vedis
        elif dbe_module is ndbm_module:
            object_class = ndbm_module.Ndbm
        elif dbe_module is gnu_module:
            object_class = gnu_module.Gnu
        self.assertIsInstance(
            self.database.get_table_connection("file1"), object_class
        )

    def create_ebm(self):
        self.database.ebm_control["file1"].append_ebm_segment(
            b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1),
            self.database.dbenv,
        )


class Database_find_values__empty(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"

    def test_01_find_values(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "find_values\(\) missing 2 required positional arguments: ",
                    "'valuespec' and 'file'",
                )
            ),
            self.database.find_values,
        )

    def test_02_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_03_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_04_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_05_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_06_find_values(self):
        self.valuespec.above_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_07_find_values(self):
        self.valuespec.from_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_08_find_values(self):
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_09_find_values(self):
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_10_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )


class Database_find_values__populated(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"
        self.database.trees["file1_field1"].insert("c")
        self.database.trees["file1_field1"].insert("d")
        self.database.trees["file1_field1"].insert("dk")
        self.database.trees["file1_field1"].insert("e")
        self.database.trees["file1_field1"].insert("f")

    def test_01_find_values(self):
        self.valuespec.above_value = "d"
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk"],
        )

    def test_02_find_values(self):
        self.valuespec.above_value = "d"
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk", "e"],
        )

    def test_03_find_values(self):
        self.valuespec.from_value = "d"
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk", "e"],
        )

    def test_04_find_values(self):
        self.valuespec.from_value = "d"
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk"],
        )

    def test_05_find_values(self):
        self.valuespec.above_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk", "e", "f"],
        )

    def test_06_find_values(self):
        self.valuespec.from_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk", "e", "f"],
        )

    def test_07_find_values(self):
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk", "e"],
        )

    def test_08_find_values(self):
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk"],
        )

    def test_09_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk", "e", "f"],
        )


class Database_add_record_to_field_value(_NoSQLOpen):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "add_record_to_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', 'segment', ",
                    "and 'record_number'",
                )
            ),
            self.database.add_record_to_field_value,
        )

    def test_02__assumptions(self):
        # Nothing exists yet, but tree is available for (file1, field1) only.
        db = self.database.dbenv
        self.assertEqual(db.exists("1_1_0_indexvalue"), False)
        self.assertEqual(db.exists("1_1_1_2_indexvalue"), False)
        self.assertEqual(db.exists("1_1"), False)  # tree root
        self.assertEqual(db.exists("1_1_2_0"), False)  # a node
        self.assertEqual("file1_field1" in self.database.trees, True)
        self.assertEqual(db.exists("2_1_0_indexvalue"), False)
        self.assertEqual(db.exists("2_1_1_2_indexvalue"), False)
        self.assertEqual(db.exists("2_1"), False)  # tree root
        self.assertEqual(db.exists("2_1_2_0"), False)  # a node
        self.assertEqual("file2_field1" in self.database.trees, False)
        self.assertEqual(
            self.database.specification["file2"]["fields"]["Field2"][
                "access_method"
            ],
            "hash",
        )
        self.assertEqual(
            self.database.specification["file1"]["fields"]["Field1"][
                "access_method"
            ],
            "btree",
        )

    def test_03_add_record_to_tree_field_value(self):
        db = self.database.dbenv
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("1_1"), True)
        self.assertEqual(db.exists("1_1_0_indexvalue"), True)
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()), {2: (0, 1)}
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 5
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: (5, 1)},
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 5
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: (5, 1)},
        )
        self.assertEqual(db.exists("1_1_1_3_indexvalue"), False)
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 6
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("L", 2)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x00\x05\x00\x06",
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 2
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("L", 3)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x00\x02\x00\x05\x00\x06",
        )
        for i in 10, 20, 30, 40:
            self.database.add_record_to_field_value(
                "file1", "field1", "indexvalue", 3, i
            )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("L", 7)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x00\x02\x00\x05\x00\x06\x00\x0a\x00\x14\x00\x1e\x00\x28",
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 50
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 8)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x80\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 50
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 8)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x80\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 3, 51
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 9)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x80\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )

    def test_04_add_record_to_hash_field_value(self):
        db = self.database.dbenv
        self.database.add_record_to_field_value(
            "file2", "field2", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("2_1"), False)  # This record never exists.
        self.assertEqual(db.exists("2_1_0_indexvalue"), True)
        self.assertEqual(
            literal_eval(db["2_1_0_indexvalue"].decode()), {2: (0, 1)}
        )


class Database_remove_record_from_field_value(_NoSQLOpen):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "remove_record_from_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', 'segment', ",
                    "and 'record_number'",
                )
            ),
            self.database.remove_record_from_field_value,
        )

    def test_02_remove_record_from_tree_field_value(self):
        db = self.database.dbenv
        for i in 5, 6, 2, 10, 20, 30, 40, 50, 51:
            self.database.add_record_to_field_value(
                "file1", "field1", "indexvalue", 3, i
            )
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 2, 0
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 9)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x80\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 4, 40
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 9)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x80\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 3, 40
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 8)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x08\x02\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        for i in 50, 51, 20:
            self.database.remove_record_from_field_value(
                "file1", "field1", "indexvalue", 3, i
            )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("B", 5)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x26\x20\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 3, 10
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("L", 4)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x00\x02\x00\x05\x00\x06\x00\x1e",
        )
        for i in 2, 6:
            self.database.remove_record_from_field_value(
                "file1", "field1", "indexvalue", 3, i
            )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: ("L", 2)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_3_indexvalue"].decode()),
            b"\x00\x05\x00\x1e",
        )
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 3, 5
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()),
            {2: (0, 1), 3: (30, 1)},
        )
        self.assertEqual(db.exists("1_1_1_3_indexvalue"), False)
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 3, 30
        )
        self.assertEqual(
            literal_eval(db["1_1_0_indexvalue"].decode()), {2: (0, 1)}
        )
        self.assertEqual(db.exists("1_1_1_3_indexvalue"), False)
        self.assertEqual(db.exists("1_1_1_2_indexvalue"), False)
        self.assertEqual(db.exists("1_1"), True)
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("1_1_0_indexvalue"), False)
        self.assertEqual(db.exists("1_1_1_3_indexvalue"), False)
        self.assertEqual(db.exists("1_1_1_2_indexvalue"), False)
        self.assertEqual(db.exists("1_1"), False)

    def test_03_remove_record_from_hash_field_value(self):
        db = self.database.dbenv
        self.database.add_record_to_field_value(
            "file2", "field2", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("2_1"), False)  # This record never exists.
        self.assertEqual(db.exists("2_1_0_indexvalue"), True)
        self.assertEqual(
            literal_eval(db["2_1_0_indexvalue"].decode()), {2: (0, 1)}
        )
        self.database.remove_record_from_field_value(
            "file2", "field2", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("2_1"), False)
        self.assertEqual(db.exists("2_1_0_indexvalue"), False)


class Database_populate_segment(_NoSQLOpen):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "populate_segment\(\) missing 3 required ",
                    "positional arguments: ",
                    "'segment_number', 'segment_reference', and 'file'",
                )
            ),
            self.database.populate_segment,
        )

    def test_02_populate_segment(self):
        s = self.database.populate_segment(2, 3, "file1")
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def test_04_populate_segment(self):
        s = self.database.populate_segment(2, b"\x00\x40\x00\x41", "file1")
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def test_06_populate_segment(self):
        s = self.database.populate_segment(
            0,
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00",
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)


class _NoSQLOpenPopulated(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        segments = (
            b"\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff",
            b"\x00\x40\x00\x41",
            b"\x00\x42\x00\x43\x00\x44",
        )
        keys = (
            "a_o",
            "aa_o",
            "ba_o",
            "bb_o",
            "c_o",
            "cep",
            "deq",
        )
        db = self.database.dbenv
        for e, k in enumerate(keys):
            self.database.trees["file1_field1"].insert(k)
            db["1_1_1_0_" + k] = repr(segments[e])
            db["1_1_0_" + k] = repr({0: ("B", 24 if e else 32)})
        self.database.trees["file1_field1"].insert("tww")
        db["1_1_1_0_" + "tww"] = repr(segments[7])
        db["1_1_0_" + "tww"] = repr({0: ("L", 2)})
        self.database.trees["file1_field1"].insert("twy")
        db["1_1_1_0_" + "twy"] = repr(segments[8])
        db["1_1_0_" + "twy"] = repr({0: ("L", 3)})
        self.database.trees["file1_field1"].insert("one")
        db["1_1_0_" + "one"] = repr({0: (50, 1)})
        self.database.trees["file1_field1"].insert("nin")
        db["1_1_0_" + "nin"] = repr({0: (100, 1)})
        self.database.trees["file1_field1"].insert("www")
        db["1_1_1_0_" + "www"] = repr(segments[8])
        db["1_1_1_1_" + "www"] = repr(segments[8])
        db["1_1_0_" + "www"] = repr({0: ("L", 3), 1: ("L", 3)})


class Database_make_recordset(_NoSQLOpenPopulated):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_key_like\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given",
                )
            ),
            self.database.recordlist_key_like,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_key\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given",
                )
            ),
            self.database.recordlist_key,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_key_startswith\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given",
                )
            ),
            self.database.recordlist_key_startswith,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_key_range\(\) takes from 3 to 8 ",
                    "positional arguments but 9 were given",
                )
            ),
            self.database.recordlist_key_range,
            *(None, None, None, None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_all\(\) takes from 3 to 4 ",
                    "positional arguments but 5 were given",
                )
            ),
            self.database.recordlist_all,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "recordlist_nil\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given",
                )
            ),
            self.database.recordlist_nil,
            *(None, None, None),
        )

    def test_02_make_recordset_key_like(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered",
            self.database.recordlist_key_like,
            *("file2", "field2"),
        )

    def test_03_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_04_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="z")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_05_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="n")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_06_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="w")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_07_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="e")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 41)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_08_make_recordset_key(self):
        rs = self.database.recordlist_key("file2", "field2")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_09_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_10_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_11_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="tww")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)

    def test_12_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 32)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_13_make_recordset_key_startswith(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered",
            self.database.recordlist_key_startswith,
            *("file2", "field2"),
        )

    def test_14_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_15_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="ppp"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_16_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="o"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_17_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="tw"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_18_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="d"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 24)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_19_make_recordset_key_range(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered",
            self.database.recordlist_key_range,
            *("file2", "field2"),
        )

    def test_20_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 128)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_21_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", le="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_22_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_23_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_24_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="c", le="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_25_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", ge="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_26_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", le="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 112)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_27_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_28_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_29_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_30_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_31_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="c", lt="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_33_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", lt="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 112)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_34_make_recordset_all(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered",
            self.database.recordlist_all,
            *("file2", "field2"),
        )

    def test_35_make_recordset_all(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 128)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_36_make_recordset_nil(self):
        rs = self.database.recordlist_nil("file1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)


class Database_file_unfile_records(_NoSQLOpenPopulated):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "unfile_records_under\(\) missing 3 required ",
                    "positional arguments: 'file', 'field', and 'key'",
                )
            ),
            self.database.unfile_records_under,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "file_records_under\(\) missing 4 required positional ",
                    "arguments: 'file', 'field', 'recordset', and 'key'",
                )
            ),
            self.database.file_records_under,
        )

    def test_02_unfile_records_under(self):
        db = self.database.dbenv
        self.assertEqual(
            "aa_o"
            in self.database.trees["file1_field1"].search("aa_o")[-1].node[4],
            True,
        )
        self.assertEqual(db.exists("1_1_0_aa_o"), True)
        self.assertEqual(db.exists("1_1_1_0_aa_o"), True)
        self.database.unfile_records_under("file1", "field1", "aa_o")
        self.assertEqual(db.exists("1_1_0_aa_o"), False)
        self.assertEqual(db.exists("1_1_1_0_aa_o"), False)
        self.assertEqual(
            "aa_o"
            in self.database.trees["file1_field1"].search("aa_o")[-1].node[4],
            False,
        )

    def test_03_unfile_records_under(self):
        db = self.database.dbenv
        self.assertEqual(
            "kkkk"
            in self.database.trees["file1_field1"].search("aa_o")[-1].node[4],
            False,
        )
        self.assertEqual(db.exists("1_1_0_kkkk"), False)
        self.database.unfile_records_under("file1", "field1", "kkkk")
        self.assertEqual(db.exists("1_1_0_kkkk"), False)
        self.assertEqual(
            "kkkk"
            in self.database.trees["file1_field1"].search("aa_o")[-1].node[4],
            False,
        )

    def test_04_file_records_under(self):
        db = self.database.dbenv
        rs = self.database.recordlist_all("file1", "field1")
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.database.file_records_under("file1", "field1", rs, "aa_o")
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()),
            {0: ("B", 128), 1: ("L", 3)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        )
        self.assertEqual(
            literal_eval(db["1_1_1_1_aa_o"].decode()), b"\x00B\x00C\x00D"
        )

    def test_05_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(db.exists("1_1_0_rrr"), False)
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, "rrr")
        self.assertEqual(
            literal_eval(db["1_1_0_rrr"].decode()),
            {0: ("B", 128), 1: ("L", 3)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_rrr"].decode()),
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
        )
        self.assertEqual(
            literal_eval(db["1_1_1_1_rrr"].decode()), b"\x00B\x00C\x00D"
        )

    def test_06_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_twy"].decode()), {0: ("L", 3)})
        self.assertEqual(
            literal_eval(db["1_1_1_0_twy"].decode()), b"\x00B\x00C\x00D"
        )
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        rs = self.database.recordlist_key("file1", "field1", key="twy")
        self.database.file_records_under("file1", "field1", rs, "aa_o")
        self.assertEqual(literal_eval(db["1_1_0_twy"].decode()), {0: ("L", 3)})
        self.assertEqual(
            literal_eval(db["1_1_1_0_twy"].decode()), b"\x00B\x00C\x00D"
        )
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("L", 3)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()), b"\x00B\x00C\x00D"
        )

    def test_07_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_twy"].decode()), {0: ("L", 3)})
        self.assertEqual(
            literal_eval(db["1_1_1_0_twy"].decode()), b"\x00B\x00C\x00D"
        )
        rs = self.database.recordlist_key("file1", "field1", key="twy")
        self.assertEqual(db.exists("1_1_0_rrr"), False)
        self.database.file_records_under("file1", "field1", rs, "rrr")
        self.assertEqual(literal_eval(db["1_1_0_twy"].decode()), {0: ("L", 3)})
        self.assertEqual(
            literal_eval(db["1_1_1_0_twy"].decode()), b"\x00B\x00C\x00D"
        )
        self.assertEqual(literal_eval(db["1_1_0_rrr"].decode()), {0: ("L", 3)})
        self.assertEqual(
            literal_eval(db["1_1_1_0_rrr"].decode()), b"\x00B\x00C\x00D"
        )

    def test_08_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "aa_o")
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(literal_eval(db["1_1_0_aa_o"].decode()), {0: (50, 1)})
        self.assertEqual(db.exists("1_1_1_0_aa_o"), False)

    def test_09_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(db.exists("1_1_0_rrr"), False)
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "rrr")
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(literal_eval(db["1_1_0_rrr"].decode()), {0: (50, 1)})

    def test_10_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(
            literal_eval(db["1_1_0_ba_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_ba_o"].decode()),
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.assertEqual(
            literal_eval(db["1_1_0_www"].decode()), {0: ("L", 3), 1: ("L", 3)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_www"].decode()), b"\x00B\x00C\x00D"
        )
        self.assertEqual(
            literal_eval(db["1_1_1_1_www"].decode()), b"\x00B\x00C\x00D"
        )
        rs = self.database.recordlist_key("file1", "field1", key="ba_o")
        self.database.file_records_under("file1", "field1", rs, "www")
        self.assertEqual(
            literal_eval(db["1_1_0_ba_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_ba_o"].decode()),
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.assertEqual(
            literal_eval(db["1_1_0_www"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_www"].decode()),
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.assertEqual(db.exists("1_1_1_1_www"), False)


class Database_database_create_cursors(_NoSQLOpen):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "database_cursor\(\) takes from 3 to 4 ",
                    "positional arguments but 5 were given",
                )
            ),
            self.database.database_cursor,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "create_recordset_cursor\(\) missing 1 required positional ",
                    "argument: 'recordset'",
                )
            ),
            self.database.create_recordset_cursor,
        )

    def test_02_database_cursor_primary(self):
        self.assertIsInstance(
            self.database.database_cursor("file1", "file1"),
            _nosql.CursorPrimary,
        )

    def test_03_database_cursor_secondary_tree(self):
        self.assertIsInstance(
            self.database.database_cursor("file1", "field1"),
            _nosql.CursorSecondary,
        )

    def test_04_database_cursor_secondary_hash(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered",
            self.database.database_cursor,
            *("file2", "field2"),
        )

    def test_05_create_recordset_cursor(self):
        d = self.database
        rs = d.recordlist_key("file1", "field1", key="ba_o")
        self.assertIsInstance(
            d.create_recordset_cursor(rs), recordset.RecordsetCursor
        )


class Database_freed_record_number(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        for i in range(SegmentSize.db_segment_size * 3):
            self.database.dbenv["_".join(("1_0", str(i)))] = repr(
                "_".join((str(i), "value"))
            )
            self.database.add_record_to_ebm("file1", i)
        self.high_record = self.database.get_high_record("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record[0], SegmentSize.db_segment_size
        )[0]

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_lowest_freed_record_number\(\) missing 1 required ",
                    "positional argument: 'dbset'",
                )
            ),
            self.database.get_lowest_freed_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "note_freed_record_number_segment\(\) missing 4 required ",
                    "positional arguments: 'dbset', 'segment', ",
                    "'record_number_in_segment', and 'high_record'",
                )
            ),
            self.database.note_freed_record_number_segment,
        )

    def test_02_note_freed_record_number_segment(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (
            100,
            101,
            200,
            300,
        ):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages,
            [0, 1, 2],
        )
        self.database.ebm_control["file1"].freed_record_number_pages = None
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (201,):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages,
            [0, 1, 2],
        )

    def test_03_get_lowest_freed_record_number(self):
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def test_04_get_lowest_freed_record_number(self):
        for i in (
            100,
            101,
            200,
            300,
        ):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 100)

    def test_05_get_lowest_freed_record_number(self):
        for i in (380,):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def test_06_get_lowest_freed_record_number(self):
        for i in (110,):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 110)

    # The freed record number in segment number 2, 'divmod(380, 128)', is not
    # seen until segment number 4 has records.
    # Segment 2 is not deleted from the 'freed record number' list until the
    # first search of the segment after all freed record numbers have been
    # re-used.
    def test_07_get_lowest_freed_record_number(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (380,):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)
        i = self.high_record[0]
        for i in range(i, i + 129):
            self.database.dbenv["_".join(("1_0", str(i)))] = repr(
                "_".join((str(i), "value"))
            )
            self.database.add_record_to_ebm("file1", i)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        self.high_record = self.database.get_high_record("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record[0], SegmentSize.db_segment_size
        )[0]
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 380)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        self.database.add_record_to_ebm("file1", 380)
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            0,
        )


# Does this test add anything beyond Database_freed_record_number?
class Database_empty_freed_record_number(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        self.high_record = self.database.get_high_record("file1")

    def test_01(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        self.database.note_freed_record_number_segment(
            "file1", 0, 100, self.high_record
        )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        self.assertEqual(
            self.database.get_high_record("file1"), self.high_record
        )


class RecordsetCursor(_NoSQLOpen):
    def setUp(self):
        super().setUp()
        segments = (
            b"\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        key = "a_o"
        for i in range(380):
            self.database.dbenv["_".join(("1", "0", str(i)))] = repr(
                str(i) + "Any value"
            )
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.dbenv["_".join(("1", "0", "_ebm", "1"))] = repr(bits)
        self.database.dbenv["_".join(("1", "0", "_ebm", "2"))] = repr(bits)
        self.database.dbenv["_".join(("1", "0", "_ebm"))] = repr((0, 1, 2))
        for e, s in enumerate(segments):
            self.database.dbenv["_".join(("1", "1", "1", str(e), key))] = repr(
                s
            )
        self.database.dbenv["_".join(("1", "1", "0", key))] = repr(
            {0: "B", 1: "B", 2: "B"}
        )

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required ",
                    "positional arguments: 'recordset' and 'engine'",
                )
            ),
            _nosql.RecordsetCursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_get_record\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            _nosql.RecordsetCursor(None, None)._get_record,
        )

    def test_02___init__01(self):
        rc = _nosql.RecordsetCursor(None, True)
        self.assertEqual(rc.engine, True)

    def test_03___init__02(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        rc = _nosql.RecordsetCursor(rs, self.database.dbenv)
        self.assertIs(rc.engine, self.database.dbenv)
        self.assertIs(rc._dbset, rs)

    def test_04__get_record(self):
        rc = _nosql.RecordsetCursor(
            self.database.recordlist_key("file1", "field1", key="a_o"),
            self.database.dbenv,
        )
        self.assertEqual(rc._get_record(4000), None)
        self.assertEqual(rc._get_record(120), None)
        self.assertEqual(rc._get_record(10), (10, "'10Any value'"))
        self.assertEqual(rc._get_record(155), (155, "'155Any value'"))


class ExistenceBitmapControl(_NoSQLOpen):
    def setUp(self):
        super().setUp()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "read_exists_segment\(\) missing 2 required ",
                    "positional arguments: 'segment_number' and 'dbenv'",
                )
            ),
            self.database.ebm_control["file1"].read_exists_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'key' and 'dbenv'",
                )
            ),
            self.database.ebm_control["file1"].get_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'key' and 'dbenv'",
                )
            ),
            self.database.ebm_control["file1"].delete_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "put_ebm_segment\(\) missing 3 required ",
                    "positional arguments: 'key', 'value', and 'dbenv'",
                )
            ),
            self.database.ebm_control["file1"].put_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "append_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'value' and 'dbenv'",
                )
            ),
            self.database.ebm_control["file1"].append_ebm_segment,
        )

    def test_02_read_exists_segment_01(self):
        self.assertEqual(self.database.ebm_control["file1"]._segment_count, 0)
        self.assertEqual(
            self.database.ebm_control["file1"].read_exists_segment(0, None),
            None,
        )

    def test_03_read_exists_segment_02(self):
        self.assertEqual(self.database.ebm_control["file1"]._segment_count, 0)
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.dbenv["_".join(("1", "0", "_ebm", "1"))] = repr(bits)
        self.database.dbenv["_".join(("1", "0", "_ebm", "2"))] = repr(bits)
        self.database.ebm_control["file1"]._segment_count = 3
        self.database.ebm_control["file1"].table_ebm_segments = [0, 1, 2]
        seg = self.database.ebm_control["file1"].read_exists_segment(
            0, self.database.dbenv
        )
        self.assertEqual(seg.count(), 128)
        seg = self.database.ebm_control["file1"].read_exists_segment(
            1, self.database.dbenv
        )
        self.assertEqual(seg.count(), 128)

    def test_04_get_ebm_segment_01(self):
        sr = self.database.ebm_control["file1"].get_ebm_segment(
            0, self.database.dbenv
        )
        self.assertEqual(sr, None)

    def test_05_get_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        sr = self.database.ebm_control["file1"].get_ebm_segment(
            0, self.database.dbenv
        )
        self.assertEqual(sr, bits)

    def test_06_delete_ebm_segment_01(self):
        self.database.ebm_control["file1"].delete_ebm_segment(
            0, self.database.dbenv
        )

    def test_07_delete_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        self.database.ebm_control["file1"].delete_ebm_segment(
            0, self.database.dbenv
        )

    def test_08_put_ebm_segment_01(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits, self.database.dbenv
        )
        self.assertEqual(
            "_".join(("1", "0", "_ebm", "0")) in self.database.dbenv, False
        )

    def test_09_put_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits, self.database.dbenv
        )
        self.assertEqual(
            self.database.dbenv["_".join(("1", "0", "_ebm", "0"))],
            repr(bits).encode(),
        )

    def test_10_append_ebm_segment(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].append_ebm_segment(
            bits, self.database.dbenv
        )

    def test_11_set_high_record_number_01(self):
        self.database.ebm_control["file1"].set_high_record_number(
            self.database.dbenv
        )
        self.assertEqual(
            self.database.ebm_control["file1"].high_record_number, -1
        )

    def test_12_set_high_record_number_02(self):
        bits0 = b"\x00" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        bits1 = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].table_ebm_segments = [0, 1, 2]
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits0, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            1, bits1, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            2, bits1, self.database.dbenv
        )
        self.database.ebm_control["file1"].set_high_record_number(
            self.database.dbenv
        )
        self.assertEqual(
            self.database.ebm_control["file1"].high_record_number, 383
        )

    def test_13_set_high_record_number_03(self):
        bits0 = b"\x00" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        bits1 = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].table_ebm_segments = [0, 1, 2]
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits0, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            1, bits1, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            2, bits0, self.database.dbenv
        )
        self.database.ebm_control["file1"].set_high_record_number(
            self.database.dbenv
        )
        self.assertEqual(
            self.database.ebm_control["file1"].high_record_number, 255
        )

    def test_14_set_high_record_number_04(self):
        bits0 = b"\x00" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        bits1 = b"\xff" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].table_ebm_segments = [0, 1, 2]
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits0, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            1, bits1, self.database.dbenv
        )
        self.database.ebm_control["file1"].put_ebm_segment(
            2, bits0, self.database.dbenv
        )
        self.database.ebm_control["file1"].set_high_record_number(
            self.database.dbenv
        )
        self.assertEqual(
            self.database.ebm_control["file1"].high_record_number, 135
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in unqlite, vedis, ndbm_module, gnu_module:
        if dbe_module is None:
            continue
        runner().run(loader(Database___init__))
        runner().run(loader(Database_transaction_methods))
        runner().run(loader(DatabaseInstance))
        runner().run(loader(Database_open_database))
        runner().run(loader(Database_do_database_task))
        runner().run(loader(DatabaseTransactions))
        runner().run(loader(Database_put_replace_delete))
        runner().run(loader(Database_methods))
        runner().run(loader(Database_find_values__empty))
        runner().run(loader(Database_find_values__populated))
        runner().run(loader(Database_add_record_to_field_value))
        runner().run(loader(Database_remove_record_from_field_value))
        runner().run(loader(Database_populate_segment))
        runner().run(loader(Database_make_recordset))
        runner().run(loader(Database_file_unfile_records))
        runner().run(loader(Database_database_create_cursors))
        runner().run(loader(Database_freed_record_number))
        runner().run(loader(Database_empty_freed_record_number))
        runner().run(loader(RecordsetCursor))
        runner().run(loader(ExistenceBitmapControl))
