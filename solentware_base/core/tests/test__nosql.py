# test__nosql.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosql _database tests with gnudbm, ndbm, unqlite, and vedis, interfaces.

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
import shutil

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
from .. import recordsetcursor
from .. import recordsetbasecursor
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

        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(_nosql.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


class Database___init__:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 5 positional arguments ",
                    "but 6 were given$",
                )
            ),
            self._D,
            *(None, None, None, None, None),
        )

    def t02(self):
        # Matches 'type object' before Python 3.9 but class name otherwise.
        t = r"(?:type object|solentware_base\.core\.filespec\.FileSpec\(\))"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    t,
                    r" argument after \*\* must be a mapping, ",
                    "not NoneType$",
                )
            ),
            self._D,
            *(None,),
        )
        self.assertIsInstance(self._D({}), self._D)
        self.assertIsInstance(self._D(filespec.FileSpec()), self._D)

    def t03(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database folder name {} is not valid$",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def t04(self):
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
        self.assertEqual(database.trees, {})
        self.assertEqual(database._real_segment_size_bytes, False)
        self.assertEqual(database._initial_segment_size_bytes, 4000)
        # Following test may not pass when run by unittest discovery
        # because other test modules may change the tested value.
        # self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)

    def t05(self):
        database = self._D({})
        self.assertEqual(database.home_directory, None)
        self.assertEqual(database.database_file, None)

    # This combination of folder and segment_size_bytes arguments is used for
    # unittests, except for one to see a non-memory database with a realistic
    # segment size.
    def t06(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)


# Transaction methods do not raise exceptions if called when no database open
# but do nothing.
class Database_transaction_methods:

    def t01_start_transaction(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.start_transaction()

    def t02_backout(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.backout()

    def t03_commit(self):
        self.assertEqual(self.database.dbenv, None)
        self.database.commit()

    def t04(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_transaction\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.start_transaction,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"backout\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.backout,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"commit\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.commit,
            *(None,),
        )


# Methods which do not require database to be open.
class DatabaseInstance:

    def t01_validate_segment_size_bytes(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_validate_segment_size_bytes\(\) missing 1 required ",
                    "positional argument: 'segment_size_bytes'$",
                )
            ),
            self.database._validate_segment_size_bytes,
        )
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database segment size must be an int$",)),
            self.database._validate_segment_size_bytes,
            *("a",),
        )
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(("Database segment size must be more than 0$",)),
            self.database._validate_segment_size_bytes,
            *(0,),
        )
        self.assertEqual(
            self.database._validate_segment_size_bytes(None), None
        )
        self.assertEqual(self.database._validate_segment_size_bytes(1), None)

    def t02_encode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_record_number\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.database.encode_record_number,
        )
        self.assertEqual(self.database.encode_record_number(1), "1")

    def t03_decode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"decode_record_number\(\) missing 1 required ",
                    "positional argument: 'skey'$",
                )
            ),
            self.database.decode_record_number,
        )
        self.assertEqual(self.database.decode_record_number("1"), 1)

    def t04_encode_record_selector(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_record_selector\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.database.encode_record_selector,
        )
        self.assertEqual(self.database.encode_record_selector("a"), "a")

    def t05_make_recordset(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_nil\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.recordlist_nil,
            *(None, None, None),
        )
        self.assertIsInstance(
            self.database.recordlist_nil("a"), recordset.RecordList
        )

    # Attribute database file is None at this point.
    def t06__generate_database_file_name(self):
        self.assertEqual(self.database._generate_database_file_name("a"), None)


# Memory databases are used for these tests.
class Database_open_database:
    def t01(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"open_database\(\) takes from 4 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.open_database,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close_database\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.close_database,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close_database_contexts\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.database.close_database_contexts,
            *(None, None),
        )

    def t02(self):
        self.database = self._D({})
        self.database.open_database(*self._oda)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self._oda[1])

    def t03(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self._oda[1])

    def t04_close_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)

    def t05_close_database_contexts(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(*self._oda)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)

    def t06(self):
        self.database = self._D({"file1": {"field1"}})
        self.database.open_database(*self._oda)
        self.check_specification()

    def t07(self):
        self.database = self._D(filespec.FileSpec(**{"file1": {"field1"}}))
        self.database.open_database(*self._oda)
        self.check_specification()

    def t08(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}})
        )
        self.database.open_database(*self._oda, files={"file1"})
        self.check_specification()

    def t09(self):
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

    # Comment in _sqlite.py suggests this method is not needed.
    def t12_is_database_file_active(self):
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
                "file1": "1",
                "___control": "0",
                "file1_field1": "1_1",
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
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _nosql.ExistenceBitmapControl)


# Memory databases cannot be used for these tests.
class DatabaseAddFieldToExistingDatabase:

    def t13_add_field_to_open_database(self):
        folder = "aaaa"
        database = self._D({"file1": {"field1"}}, folder=folder)
        database.open_database(*self._oda)
        database.close_database()
        database = None
        database = self._D({"file1": {"field1", "newfield"}}, folder=folder)
        self.assertRaisesRegex(
            filespec.FileSpecError,
            "".join(
                (
                    "Specification does not have same fields for each ",
                    "file as defined in this FileSpec$",
                )
            ),
            database.open_database,
            *(*self._oda,),
        )
        shutil.rmtree(folder)


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
        _oda = self._oda

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


# Memory databases are used for these tests.
# Use the 'testing only' segment size for convenience of setup and eyeballing.
class _NoSQLOpen(_NoSQL):
    def setup_detail(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}}),
            segment_size_bytes=None,
        )
        self.database.specification["file2"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)

    def teardown_detail(self):
        self.database.close_database()


class DatabaseTransactions:
    def t01(self):
        self.database.start_transaction()
        self.assertEqual(self.database.start_transaction(), None)

    def t02(self):
        self.database.start_transaction()
        self.assertEqual(self.database.backout(), None)

    def t03(self):
        self.database.start_transaction()
        self.assertEqual(self.database.commit(), None)

    def t04(self):
        self.assertEqual(self.database.backout(), None)

    def t05(self):
        self.assertEqual(self.database.commit(), None)


class Database_put_replace_delete:
    # These tests are copied and modified from test__sqlite.
    # The tests on put assume a correct add_record_to_ebm method, and those on
    # delete assume a correct remove_record_from_ebm() method because the
    # bitmaps are used to identify the highest record number allocated.
    # UnQLite and Vedis do not have the notion of a record number like the
    # rowid in a SQLite3 table, or the key of a Recno database in Berkeley DB,
    # or the record number of a DPT file.

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"put\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'$",
                )
            ),
            self.database.put,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"replace\(\) missing 4 required positional arguments: ",
                    "'file', 'key', 'oldvalue', and 'newvalue'$",
                )
            ),
            self.database.replace,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"delete\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'$",
                )
            ),
            self.database.delete,
        )

    def t02_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)

    def t03_put(self):
        self.assertEqual("1__2" in self.database.dbenv, False)
        self.assertEqual(self.database.put("file1", 2, "new value"), None)
        self.database.add_record_to_ebm("file1", 2)
        self.assertEqual("1_0_2" in self.database.dbenv, True)
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 3)

    def t04_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)
        self.database.add_record_to_ebm("file1", 0)
        self.assertEqual(self.database.put("file1", 0, "renew value"), None)
        recno = self.database.put("file1", None, "other value")
        self.assertEqual(recno, 1)

    def t05_replace(self):
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

    def t06_replace(self):
        self.database.dbenv["1_0_1"] = repr("old value")
        self.assertEqual(self.database.dbenv["1_0_1"], b"'old value'")
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("old value"), repr("new value")
            ),
            None,
        )
        self.assertEqual(self.database.dbenv["1_0_1"], b"'new value'")

    def t07_replace(self):
        self.database.dbenv["1_1"] = repr("old value")
        self.assertEqual(self.database.dbenv["1_1"], b"'old value'")
        self.assertEqual(
            self.database.replace(
                "file1", 1, repr("new value"), repr("same value")
            ),
            None,
        )
        self.assertEqual(self.database.dbenv["1_1"], b"'old value'")

    def t08_delete(self):
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

    def t09_delete(self):
        self.database.dbenv["1_0_1"] = repr("new value")
        self.database.add_record_to_ebm("file1", 0)
        self.assertEqual("1_0_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.delete("file1", 1, repr("new value")), None
        )
        self.database.remove_record_from_ebm("file1", 0)
        self.assertEqual("1_0_1" in self.database.dbenv, False)

    def t10_delete(self):
        self.database.dbenv["1_1"] = repr("new value")
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual(
            self.database.delete("file1", 1, repr("old value")), None
        )
        self.assertEqual("1_1" in self.database.dbenv, True)


# These tests need fully working put, replace, and delete, methods.
class Database_methods:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_primary_record\(\) missing 2 required positional ",
                    "arguments: 'file' and 'key'$",
                )
            ),
            self.database.get_primary_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"remove_record_from_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'deletekey'$",
                )
            ),
            self.database.remove_record_from_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"add_record_to_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'putkey'$",
                )
            ),
            self.database.add_record_to_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_high_record_number\(\) missing 1 required ",
                    "positional argument: 'file'$",
                )
            ),
            self.database.get_high_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_record_number\(\) takes from 2 to 4 ",
                    "positional arguments but 5 were given$",
                )
            ),
            self.database.recordlist_record_number,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_record_number_range\(\) takes from 2 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_record_number_range,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_ebm\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.recordlist_ebm,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_table_connection\(\) missing 1 required ",
                    "positional argument: 'file'$",
                )
            ),
            self.database.get_table_connection,
        )

    def t02_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", None), None)

    def t03_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", 1), None)

    def t04_get_primary_record(self):
        self.database.put("file1", None, repr("new value"))
        self.assertEqual(
            self.database.get_primary_record("file1", 0), (0, "'new value'")
        )

    def t05_remove_record_from_ebm(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "Existence bit map for segment does not exist$",
            self.database.remove_record_from_ebm,
            *("file1", 2),
        )

    def t06_remove_record_from_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(
            self.database.remove_record_from_ebm("file1", 2), (0, 2)
        )

    def t07_add_record_to_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(self.database.add_record_to_ebm("file1", 4), (0, 4))

    def t08_get_high_record(self):
        self.assertEqual(self.database.get_high_record_number("file1"), None)

    def t14_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1"),
            recordset.RecordList,
        )

    def t15_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1", key=500),
            recordset.RecordList,
        )

    def t16_recordset_record_number(self):
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

    def t17_recordset_record_number_range(self):
        self.assertIsInstance(
            self.database.recordlist_record_number_range("file1"),
            recordset.RecordList,
        )

    def t18_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=0, keyend=2000
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )

    def t19_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keystart=10)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x00\x3f\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )

    def t20_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keyend=35)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\xff\xff\xff\xff\xf0\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )

    def t21_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=10, keyend=35
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x00\x3f\xff\xff\xf0\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )

    def t22_recordset_record_number_range(self):
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
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x3f\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )
        self.assertEqual(
            rs[2].tobytes(),
            b"".join(
                (
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xfe\x00\x00\x00\x00",
                )
            ),
        )

    def t23_recordset_record_number_range(self):
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=350, keyend=170
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t24_recordset_ebm(self):
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def t25_recordset_ebm(self):
        self.create_ebm()
        rlebm = self.database.recordlist_ebm("file1")
        self.assertIsInstance(rlebm, recordset.RecordList)
        self.assertEqual(rlebm.sorted_segnums, [0])

    def create_ebm(self):
        self.database.ebm_control["file1"].append_ebm_segment(
            b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1),
            self.database.dbenv,
        )


class Database_find_values__empty:
    def setup_detail(self):
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"

    def t01_find_values(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"find_values\(\) missing 2 required ",
                    "positional arguments: 'valuespec' and 'file'$",
                )
            ),
            self.database.find_values,
        )

    def t02_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t03_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t04_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t05_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t06_find_values(self):
        self.valuespec.above_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t07_find_values(self):
        self.valuespec.from_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t08_find_values(self):
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t09_find_values(self):
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def t10_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )


class Database_find_values__populated:
    def setup_detail(self):
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"
        self.database.trees["file1_field1"].insert("c")
        self.database.trees["file1_field1"].insert("d")
        self.database.trees["file1_field1"].insert("dk")
        self.database.trees["file1_field1"].insert("e")
        self.database.trees["file1_field1"].insert("f")

    def t01_find_values(self):
        self.valuespec.above_value = "d"
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk"],
        )

    def t02_find_values(self):
        self.valuespec.above_value = "d"
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk", "e"],
        )

    def t03_find_values(self):
        self.valuespec.from_value = "d"
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk", "e"],
        )

    def t04_find_values(self):
        self.valuespec.from_value = "d"
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk"],
        )

    def t05_find_values(self):
        self.valuespec.above_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["dk", "e", "f"],
        )

    def t06_find_values(self):
        self.valuespec.from_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d", "dk", "e", "f"],
        )

    def t07_find_values(self):
        self.valuespec.to_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk", "e"],
        )

    def t08_find_values(self):
        self.valuespec.below_value = "e"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk"],
        )

    def t09_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["c", "d", "dk", "e", "f"],
        )


class DatabaseAddRecordToFieldValue:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"add_record_to_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', ",
                    "'segment', and 'record_number'$",
                )
            ),
            self.database.add_record_to_field_value,
        )

    def t02__assumptions(self):
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

    def t03_add_record_to_tree_field_value(self):
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x80\x20\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x80\x20\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x80\x30\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )

    def t04_add_record_to_hash_field_value(self):
        db = self.database.dbenv
        self.database.add_record_to_field_value(
            "file2", "field2", "indexvalue", 2, 0
        )
        self.assertEqual(db.exists("2_1"), False)  # This record never exists.
        self.assertEqual(db.exists("2_1_0_indexvalue"), True)
        self.assertEqual(
            literal_eval(db["2_1_0_indexvalue"].decode()), {2: (0, 1)}
        )


class DatabaseRemoveRecordFieldValue:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"remove_record_from_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', ",
                    "'segment', and 'record_number'$",
                )
            ),
            self.database.remove_record_from_field_value,
        )

    def t02_remove_record_tree_field_value(self):
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x80\x30\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x80\x30\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x26\x20\x08\x02\x00\x00\x30\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x26\x20\x00\x02\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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

    def t03_remove_record_hash_field_value(self):
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


class Database_populate_segment:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"populate_segment\(\) missing 3 required ",
                    "positional arguments: ",
                    "'segment_number', 'segment_reference', and 'file'$",
                )
            ),
            self.database.populate_segment,
        )

    def t02_populate_segment(self):
        s = self.database.populate_segment(2, 3, "file1")
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def t04_populate_segment(self):
        s = self.database.populate_segment(2, b"\x00\x40\x00\x41", "file1")
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def t06_populate_segment(self):
        s = self.database.populate_segment(
            0,
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\xff\xff\xff\x00\x00\x00\x00",
                )
            ),
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)


class _NoSQLOpenPopulated:
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\xff",
                    b"\xff\xff\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\xff\xff\xff\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                )
            ),
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


class Database_make_recordset:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_like\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key_like,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_startswith\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key_startswith,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_range\(\) takes from 3 to 8 ",
                    "positional arguments but 9 were given$",
                )
            ),
            self.database.recordlist_key_range,
            *(None, None, None, None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_all\(\) takes from 3 to 4 ",
                    "positional arguments but 5 were given$",
                )
            ),
            self.database.recordlist_all,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_nil\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.recordlist_nil,
            *(None, None, None),
        )

    def t02_make_recordset_key_like(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered$",
            self.database.recordlist_key_like,
            *("file2", "field2"),
        )

    def t03_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t04_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="z")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t05_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="n")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t06_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="w")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t07_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="e")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 41)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t08_make_recordset_key(self):
        rs = self.database.recordlist_key("file2", "field2")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t09_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t10_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def t11_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="tww")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)

    def t12_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 32)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t13_make_recordset_key_startswith(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered$",
            self.database.recordlist_key_startswith,
            *("file2", "field2"),
        )

    def t14_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t15_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="ppp"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t16_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="o"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def t17_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="tw"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t18_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="d"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 24)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t19_make_recordset_key_range(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered$",
            self.database.recordlist_key_range,
            *("file2", "field2"),
        )

    def t20_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 128)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t21_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", le="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t22_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t23_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t24_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="c", le="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t25_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", ge="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t26_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", le="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 112)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t27_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t28_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t29_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t30_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t31_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="c", lt="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t32_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t33_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", lt="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 112)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t34_make_recordset_all(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered$",
            self.database.recordlist_all,
            *("file2", "field2"),
        )

    def t35_make_recordset_all(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 128)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t36_make_recordset_nil(self):
        rs = self.database.recordlist_nil("file1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)


class Database_file_unfile_records:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"unfile_records_under\(\) missing 3 required ",
                    "positional arguments: 'file', 'field', and 'key'$",
                )
            ),
            self.database.unfile_records_under,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"file_records_under\(\) missing 4 required positional ",
                    "arguments: 'file', 'field', 'recordset', and 'key'$",
                )
            ),
            self.database.file_records_under,
        )

    def t02_unfile_records_under(self):
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

    def t03_unfile_records_under(self):
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

    def t04_file_records_under(self):
        db = self.database.dbenv
        rs = self.database.recordlist_all("file1", "field1")
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        self.database.file_records_under("file1", "field1", rs, "aa_o")
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()),
            {0: ("B", 128), 1: ("L", 3)},
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"".join(
                (
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )
        self.assertEqual(
            literal_eval(db["1_1_1_1_aa_o"].decode()), b"\x00B\x00C\x00D"
        )

    def t05_file_records_under(self):
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
            b"".join(
                (
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )
        self.assertEqual(
            literal_eval(db["1_1_1_1_rrr"].decode()), b"\x00B\x00C\x00D"
        )

    def t06_file_records_under(self):
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
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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

    def t07_file_records_under(self):
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

    def t08_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(
            literal_eval(db["1_1_0_aa_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_aa_o"].decode()),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "aa_o")
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(literal_eval(db["1_1_0_aa_o"].decode()), {0: (50, 1)})
        self.assertEqual(db.exists("1_1_1_0_aa_o"), False)

    def t09_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(db.exists("1_1_0_rrr"), False)
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "rrr")
        self.assertEqual(literal_eval(db["1_1_0_one"].decode()), {0: (50, 1)})
        self.assertEqual(literal_eval(db["1_1_0_rrr"].decode()), {0: (50, 1)})

    def t10_file_records_under(self):
        db = self.database.dbenv
        self.assertEqual(
            literal_eval(db["1_1_0_ba_o"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_ba_o"].decode()),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        self.assertEqual(
            literal_eval(db["1_1_0_www"].decode()), {0: ("B", 24)}
        )
        self.assertEqual(
            literal_eval(db["1_1_1_0_www"].decode()),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        self.assertEqual(db.exists("1_1_1_1_www"), False)


class Database_database_create_cursors:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"database_cursor\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.database_cursor,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"create_recordset_cursor\(\) missing 1 ",
                    "required positional argument: 'recordset'$",
                )
            ),
            self.database.create_recordset_cursor,
        )

    def t02_database_cursor_primary(self):
        self.assertIsInstance(
            self.database.database_cursor("file1", "file1"),
            _nosql.CursorPrimary,
        )

    def t03_database_cursor_secondary_tree(self):
        self.assertIsInstance(
            self.database.database_cursor("file1", "field1"),
            _nosql.CursorSecondary,
        )

    def t04_database_cursor_secondary_hash(self):
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "'field2' field in 'file2' file is not ordered$",
            self.database.database_cursor,
            *("file2", "field2"),
        )

    def t05_create_recordset_cursor(self):
        d = self.database
        rs = d.recordlist_key("file1", "field1", key="ba_o")
        self.assertIsInstance(
            d.create_recordset_cursor(rs), recordsetcursor.RecordsetCursor
        )

    def t06_database_cursor_recordset(self):
        rs = recordset.RecordList(self.database, "field1")
        self.assertIsInstance(
            self.database.database_cursor("file1", "file1", recordset=rs),
            recordsetbasecursor.RecordSetBaseCursor,
        )


class Database_freed_record_number:
    def setup_detail(self):
        for i in range(SegmentSize.db_segment_size * 3):
            self.database.dbenv["_".join(("1_0", str(i)))] = repr(
                "_".join((str(i), "value"))
            )
            self.database.add_record_to_ebm("file1", i)
        self.high_record = self.database.get_high_record_number("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record, SegmentSize.db_segment_size
        )[0]

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_lowest_freed_record_number\(\) missing 1 required ",
                    "positional argument: 'dbset'$",
                )
            ),
            self.database.get_lowest_freed_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"note_freed_record_number_segment\(\) missing 4 ",
                    "required positional arguments: 'dbset', 'segment', ",
                    "'record_number_in_segment', and 'high_record_number'$",
                )
            ),
            self.database.note_freed_record_number_segment,
        )

    def t02_note_freed_record_number_segment(self):
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

    def t03_get_lowest_freed_record_number(self):
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def t04_get_lowest_freed_record_number(self):
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

    def t05_get_lowest_freed_record_number(self):
        for i in (380,):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def t06_get_lowest_freed_record_number(self):
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
    def t07_get_lowest_freed_record_number(self):
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
        i = self.high_record
        for i in range(i, i + 129):
            self.database.dbenv["_".join(("1_0", str(i)))] = repr(
                "_".join((str(i), "value"))
            )
            self.database.add_record_to_ebm("file1", i)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        self.high_record = self.database.get_high_record_number("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record, SegmentSize.db_segment_size
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

    def t08_get_lowest_freed_record_number(self):
        for i in (0, 1):
            self.database.delete("file1", i, repr("_".join((str(i), "value"))))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 0)


# Does this test add anything beyond Database_freed_record_number?
class Database_empty_freed_record_number:
    def t01(self):
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
            self.database.get_high_record_number("file1"), self.high_record
        )


class RecordsetCursor:
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
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

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 2 required ",
                    "positional arguments: 'recordset' and 'engine'$",
                )
            ),
            _nosql.RecordsetCursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_get_record\(\) missing 1 required ",
                    "positional argument: 'record_number'$",
                )
            ),
            _nosql.RecordsetCursor(None, None)._get_record,
        )

    def t02___init__01(self):
        rc = _nosql.RecordsetCursor(None, True)
        self.assertEqual(rc.engine, True)

    def t03___init__02(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        rc = _nosql.RecordsetCursor(rs, self.database.dbenv)
        self.assertIs(rc.engine, self.database.dbenv)
        self.assertIs(rc._dbset, rs)

    def t04__get_record(self):
        rc = _nosql.RecordsetCursor(
            self.database.recordlist_key("file1", "field1", key="a_o"),
            self.database.dbenv,
        )
        self.assertEqual(rc._get_record(4000), None)
        self.assertEqual(rc._get_record(120), None)
        self.assertEqual(rc._get_record(10), (10, "'10Any value'"))
        self.assertEqual(rc._get_record(155), (155, "'155Any value'"))


class ExistenceBitmapControl:
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"read_exists_segment\(\) missing 2 required ",
                    "positional arguments: 'segment_number' and 'dbenv'$",
                )
            ),
            self.database.ebm_control["file1"].read_exists_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'key' and 'dbenv'$",
                )
            ),
            self.database.ebm_control["file1"].get_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"delete_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'key' and 'dbenv'$",
                )
            ),
            self.database.ebm_control["file1"].delete_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"put_ebm_segment\(\) missing 3 required ",
                    "positional arguments: 'key', 'value', and 'dbenv'$",
                )
            ),
            self.database.ebm_control["file1"].put_ebm_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"append_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'value' and 'dbenv'$",
                )
            ),
            self.database.ebm_control["file1"].append_ebm_segment,
        )

    def t02_read_exists_segment_01(self):
        self.assertEqual(self.database.ebm_control["file1"]._segment_count, 0)
        self.assertEqual(
            self.database.ebm_control["file1"].read_exists_segment(0, None),
            None,
        )

    def t03_read_exists_segment_02(self):
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

    def t04_get_ebm_segment_01(self):
        sr = self.database.ebm_control["file1"].get_ebm_segment(
            0, self.database.dbenv
        )
        self.assertEqual(sr, None)

    def t05_get_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        sr = self.database.ebm_control["file1"].get_ebm_segment(
            0, self.database.dbenv
        )
        self.assertEqual(sr, bits)

    def t06_delete_ebm_segment_01(self):
        self.database.ebm_control["file1"].delete_ebm_segment(
            0, self.database.dbenv
        )

    def t07_delete_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbenv["_".join(("1", "0", "_ebm", "0"))] = repr(bits)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        self.database.ebm_control["file1"].delete_ebm_segment(
            0, self.database.dbenv
        )

    def t08_put_ebm_segment_01(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits, self.database.dbenv
        )
        self.assertEqual(
            "_".join(("1", "0", "_ebm", "0")) in self.database.dbenv, False
        )

    def t09_put_ebm_segment_02(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].table_ebm_segments = [0]
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits, self.database.dbenv
        )
        self.assertEqual(
            self.database.dbenv["_".join(("1", "0", "_ebm", "0"))],
            repr(bits).encode(),
        )

    def t10_append_ebm_segment(self):
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].append_ebm_segment(
            bits, self.database.dbenv
        )

    def t11_set_high_record_number_01(self):
        self.database.ebm_control["file1"].set_high_record_number(
            self.database.dbenv
        )
        self.assertEqual(
            self.database.ebm_control["file1"].high_record_number, -1
        )

    def t12_set_high_record_number_02(self):
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

    def t13_set_high_record_number_03(self):
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

    def t14_set_high_record_number_04(self):
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


if gnu_module:

    class _NoSQLGnu(_NoSQL):
        def setUp(self):
            self._oda = gnu_module, Gnu, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            if os.path.isfile(path):
                os.remove(path)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

    class Database___init__Gnu(_NoSQLGnu):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset
        test_06 = DatabaseInstance.t06__generate_database_file_name

    class Database_open_databaseGnu(_NoSQLGnu):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02
        test_03 = Database_open_database.t03
        test_04 = Database_open_database.t04_close_database
        test_05 = Database_open_database.t05_close_database_contexts
        test_06 = Database_open_database.t06
        test_07 = Database_open_database.t07
        test_08 = Database_open_database.t08
        test_09 = Database_open_database.t09
        test_12 = Database_open_database.t12_is_database_file_active
        check_specification = Database_open_database.check_specification

    class Database_add_field_to_existing_databaseGnu(_NoSQLGnu):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskGnu(Database_do_database_task):
        def setUp(self):
            self._oda = gnu_module, Gnu, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
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

            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            self.database = self._AD(path)
            d = self.database
            d.open_database()
            self.assertEqual(d.do_database_task(m), None)

        def test_02_do_database_task(self):
            def m(*a, **k):
                pass

            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            self.database = self._AD(path)
            d = self.database
            self.assertEqual(d.do_database_task(m), None)

    class _NoSQLOpenGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            _NoSQLOpen.setup_detail(self)

        def tearDown(self):
            _NoSQLOpen.teardown_detail(self)
            super().tearDown()

    class DatabaseTransactionsGnu(_NoSQLOpenGnu):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteGnu(_NoSQLOpenGnu):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_replace
        test_08 = Database_put_replace_delete.t08_delete
        test_09 = Database_put_replace_delete.t09_delete
        test_10 = Database_put_replace_delete.t10_delete

    class Database_methodsGnu(_NoSQLOpenGnu):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_14 = Database_methods.t14_recordset_record_number
        test_15 = Database_methods.t15_recordset_record_number
        test_16 = Database_methods.t16_recordset_record_number
        test_17 = Database_methods.t17_recordset_record_number_range
        test_18 = Database_methods.t18_recordset_record_number_range
        test_19 = Database_methods.t19_recordset_record_number_range
        test_20 = Database_methods.t20_recordset_record_number_range
        test_21 = Database_methods.t21_recordset_record_number_range
        test_22 = Database_methods.t22_recordset_record_number_range
        test_23 = Database_methods.t23_recordset_record_number_range
        test_24 = Database_methods.t24_recordset_ebm
        test_25 = Database_methods.t25_recordset_ebm

        def test26_get_table_connection(self):
            self.assertIsInstance(
                self.database.get_table_connection("file1"), gnu_module.Gnu
            )

        create_ebm = Database_methods.create_ebm

    class Database_find_values__emptyGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            Database_find_values__empty.setup_detail(self)

        test_01 = Database_find_values__empty.t01_find_values
        test_02 = Database_find_values__empty.t02_find_values
        test_03 = Database_find_values__empty.t03_find_values
        test_04 = Database_find_values__empty.t04_find_values
        test_05 = Database_find_values__empty.t05_find_values
        test_06 = Database_find_values__empty.t06_find_values
        test_07 = Database_find_values__empty.t07_find_values
        test_08 = Database_find_values__empty.t08_find_values
        test_09 = Database_find_values__empty.t09_find_values
        test_10 = Database_find_values__empty.t10_find_values

    class Database_find_values__populatedGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            Database_find_values__populated.setup_detail(self)

        test_01 = Database_find_values__populated.t01_find_values
        test_02 = Database_find_values__populated.t02_find_values
        test_03 = Database_find_values__populated.t03_find_values
        test_04 = Database_find_values__populated.t04_find_values
        test_05 = Database_find_values__populated.t05_find_values
        test_06 = Database_find_values__populated.t06_find_values
        test_07 = Database_find_values__populated.t07_find_values
        test_08 = Database_find_values__populated.t08_find_values
        test_09 = Database_find_values__populated.t09_find_values

    class Database_add_record_to_field_valueGnu(_NoSQLOpenGnu):
        test_01 = DatabaseAddRecordToFieldValue.t01
        test_02 = DatabaseAddRecordToFieldValue.t02__assumptions
        test_03 = (
            DatabaseAddRecordToFieldValue.t03_add_record_to_tree_field_value
        )
        test_04 = (
            DatabaseAddRecordToFieldValue.t04_add_record_to_hash_field_value
        )

    class Database_remove_record_from_field_valueGnu(_NoSQLOpenGnu):
        test_01 = DatabaseRemoveRecordFieldValue.t01
        test_02 = (
            DatabaseRemoveRecordFieldValue.t02_remove_record_tree_field_value
        )
        test_03 = (
            DatabaseRemoveRecordFieldValue.t03_remove_record_hash_field_value
        )

    class Database_populate_segmentGnu(_NoSQLOpenGnu):
        test_01 = Database_populate_segment.t01
        test_02 = Database_populate_segment.t02_populate_segment
        test_04 = Database_populate_segment.t04_populate_segment
        test_06 = Database_populate_segment.t06_populate_segment

    class _NoSQLOpenPopulatedGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            _NoSQLOpenPopulated.setup_detail(self)

    class Database_make_recordsetGnu(_NoSQLOpenPopulatedGnu):
        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_make_recordset_key_like
        test_03 = Database_make_recordset.t03_make_recordset_key_like
        test_04 = Database_make_recordset.t04_make_recordset_key_like
        test_05 = Database_make_recordset.t05_make_recordset_key_like
        test_06 = Database_make_recordset.t06_make_recordset_key_like
        test_07 = Database_make_recordset.t07_make_recordset_key_like
        test_08 = Database_make_recordset.t08_make_recordset_key
        test_09 = Database_make_recordset.t09_make_recordset_key
        test_10 = Database_make_recordset.t10_make_recordset_key
        test_11 = Database_make_recordset.t11_make_recordset_key
        test_12 = Database_make_recordset.t12_make_recordset_key
        test_13 = Database_make_recordset.t13_make_recordset_key_startswith
        test_14 = Database_make_recordset.t14_make_recordset_key_startswith
        test_15 = Database_make_recordset.t15_make_recordset_key_startswith
        test_16 = Database_make_recordset.t16_make_recordset_key_startswith
        test_17 = Database_make_recordset.t17_make_recordset_key_startswith
        test_18 = Database_make_recordset.t18_make_recordset_key_startswith
        test_19 = Database_make_recordset.t19_make_recordset_key_range
        test_20 = Database_make_recordset.t20_make_recordset_key_range
        test_21 = Database_make_recordset.t21_make_recordset_key_range
        test_22 = Database_make_recordset.t22_make_recordset_key_range
        test_23 = Database_make_recordset.t23_make_recordset_key_range
        test_24 = Database_make_recordset.t24_make_recordset_key_range
        test_25 = Database_make_recordset.t25_make_recordset_key_range
        test_26 = Database_make_recordset.t26_make_recordset_key_range
        test_27 = Database_make_recordset.t27_make_recordset_key_range
        test_28 = Database_make_recordset.t28_make_recordset_key_range
        test_29 = Database_make_recordset.t29_make_recordset_key_range
        test_30 = Database_make_recordset.t30_make_recordset_key_range
        test_31 = Database_make_recordset.t31_make_recordset_key_range
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_all
        test_35 = Database_make_recordset.t35_make_recordset_all
        test_36 = Database_make_recordset.t36_make_recordset_nil

    class Database_file_unfile_recordsGnu(_NoSQLOpenPopulatedGnu):
        test_01 = Database_file_unfile_records.t01
        test_02 = Database_file_unfile_records.t02_unfile_records_under
        test_03 = Database_file_unfile_records.t03_unfile_records_under
        test_04 = Database_file_unfile_records.t04_file_records_under
        test_05 = Database_file_unfile_records.t05_file_records_under
        test_06 = Database_file_unfile_records.t06_file_records_under
        test_07 = Database_file_unfile_records.t07_file_records_under
        test_08 = Database_file_unfile_records.t08_file_records_under
        test_09 = Database_file_unfile_records.t09_file_records_under
        test_10 = Database_file_unfile_records.t10_file_records_under

    class Database_database_create_cursorsGnu(_NoSQLOpenGnu):
        test_01 = Database_database_create_cursors.t01
        test_02 = Database_database_create_cursors.t02_database_cursor_primary
        test_03 = (
            Database_database_create_cursors.t03_database_cursor_secondary_tree
        )
        test_04 = (
            Database_database_create_cursors.t04_database_cursor_secondary_hash
        )
        test_05 = Database_database_create_cursors.t05_create_recordset_cursor
        test_06 = (
            Database_database_create_cursors.t06_database_cursor_recordset
        )

    class Database_freed_record_numberGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            Database_freed_record_number.setup_detail(self)

        test_01 = Database_freed_record_number.t01
        test_02 = (
            Database_freed_record_number.t02_note_freed_record_number_segment
        )
        test_03 = (
            Database_freed_record_number.t03_get_lowest_freed_record_number
        )
        test_04 = (
            Database_freed_record_number.t04_get_lowest_freed_record_number
        )
        test_05 = (
            Database_freed_record_number.t05_get_lowest_freed_record_number
        )
        test_06 = (
            Database_freed_record_number.t06_get_lowest_freed_record_number
        )
        test_07 = (
            Database_freed_record_number.t07_get_lowest_freed_record_number
        )
        test_08 = (
            Database_freed_record_number.t08_get_lowest_freed_record_number
        )

    class Database_empty_freed_record_numberGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorGnu(_NoSQLOpenGnu):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlGnu(_NoSQLOpenGnu):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment_01
        test_09 = ExistenceBitmapControl.t09_put_ebm_segment_02
        test_10 = ExistenceBitmapControl.t10_append_ebm_segment
        test_11 = ExistenceBitmapControl.t11_set_high_record_number_01
        test_12 = ExistenceBitmapControl.t12_set_high_record_number_02
        test_13 = ExistenceBitmapControl.t13_set_high_record_number_03
        test_14 = ExistenceBitmapControl.t14_set_high_record_number_04


if ndbm_module:

    class _NoSQLNdbm(_NoSQL):
        def setUp(self):
            self._oda = ndbm_module, Ndbm, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
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

    class Database___init__Ndbm(_NoSQLNdbm):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset
        test_06 = DatabaseInstance.t06__generate_database_file_name

    class Database_open_databaseNdbm(_NoSQLNdbm):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02
        test_03 = Database_open_database.t03
        test_04 = Database_open_database.t04_close_database
        test_05 = Database_open_database.t05_close_database_contexts
        test_06 = Database_open_database.t06
        test_07 = Database_open_database.t07
        test_08 = Database_open_database.t08
        test_09 = Database_open_database.t09
        test_12 = Database_open_database.t12_is_database_file_active
        check_specification = Database_open_database.check_specification

    class Database_add_field_to_existing_databaseNdbm(_NoSQLNdbm):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskNdbm(Database_do_database_task):
        def setUp(self):
            self._oda = ndbm_module, ndbm_module.Ndbm, None
            super().setUp()

        def test_01_do_database_task(self):
            def m(*a, **k):
                pass

            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            self.database = self._AD(path)
            d = self.database
            d.open_database()
            self.assertEqual(d.do_database_task(m), None)

        def test_02_do_database_task(self):
            def m(*a, **k):
                pass

            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            self.database = self._AD(path)
            d = self.database
            self.assertEqual(d.do_database_task(m), None)

    class _NoSQLOpenNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            _NoSQLOpen.setup_detail(self)

        def tearDown(self):
            _NoSQLOpen.teardown_detail(self)
            super().tearDown()

    class DatabaseTransactionsNdbm(_NoSQLOpenNdbm):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteNdbm(_NoSQLOpenNdbm):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_replace
        test_08 = Database_put_replace_delete.t08_delete
        test_09 = Database_put_replace_delete.t09_delete
        test_10 = Database_put_replace_delete.t10_delete

    class Database_methodsNdbm(_NoSQLOpenNdbm):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_14 = Database_methods.t14_recordset_record_number
        test_15 = Database_methods.t15_recordset_record_number
        test_16 = Database_methods.t16_recordset_record_number
        test_17 = Database_methods.t17_recordset_record_number_range
        test_18 = Database_methods.t18_recordset_record_number_range
        test_19 = Database_methods.t19_recordset_record_number_range
        test_20 = Database_methods.t20_recordset_record_number_range
        test_21 = Database_methods.t21_recordset_record_number_range
        test_22 = Database_methods.t22_recordset_record_number_range
        test_23 = Database_methods.t23_recordset_record_number_range
        test_24 = Database_methods.t24_recordset_ebm
        test_25 = Database_methods.t25_recordset_ebm

        def test26_get_table_connection(self):
            self.assertIsInstance(
                self.database.get_table_connection("file1"), ndbm_module.Ndbm
            )

        create_ebm = Database_methods.create_ebm

    class Database_find_values__emptyNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            Database_find_values__empty.setup_detail(self)

        test_01 = Database_find_values__empty.t01_find_values
        test_02 = Database_find_values__empty.t02_find_values
        test_03 = Database_find_values__empty.t03_find_values
        test_04 = Database_find_values__empty.t04_find_values
        test_05 = Database_find_values__empty.t05_find_values
        test_06 = Database_find_values__empty.t06_find_values
        test_07 = Database_find_values__empty.t07_find_values
        test_08 = Database_find_values__empty.t08_find_values
        test_09 = Database_find_values__empty.t09_find_values
        test_10 = Database_find_values__empty.t10_find_values

    class Database_find_values__populatedNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            Database_find_values__populated.setup_detail(self)

        test_01 = Database_find_values__populated.t01_find_values
        test_02 = Database_find_values__populated.t02_find_values
        test_03 = Database_find_values__populated.t03_find_values
        test_04 = Database_find_values__populated.t04_find_values
        test_05 = Database_find_values__populated.t05_find_values
        test_06 = Database_find_values__populated.t06_find_values
        test_07 = Database_find_values__populated.t07_find_values
        test_08 = Database_find_values__populated.t08_find_values
        test_09 = Database_find_values__populated.t09_find_values

    class Database_add_record_to_field_valueNdbm(_NoSQLOpenNdbm):
        test_01 = DatabaseAddRecordToFieldValue.t01
        test_02 = DatabaseAddRecordToFieldValue.t02__assumptions
        test_03 = (
            DatabaseAddRecordToFieldValue.t03_add_record_to_tree_field_value
        )
        test_04 = (
            DatabaseAddRecordToFieldValue.t04_add_record_to_hash_field_value
        )

    class Database_remove_record_from_field_valueNdbm(_NoSQLOpenNdbm):
        test_01 = DatabaseRemoveRecordFieldValue.t01
        test_02 = (
            DatabaseRemoveRecordFieldValue.t02_remove_record_tree_field_value
        )
        test_03 = (
            DatabaseRemoveRecordFieldValue.t03_remove_record_hash_field_value
        )

    class Database_populate_segmentNdbm(_NoSQLOpenNdbm):
        test_01 = Database_populate_segment.t01
        test_02 = Database_populate_segment.t02_populate_segment
        test_04 = Database_populate_segment.t04_populate_segment
        test_06 = Database_populate_segment.t06_populate_segment

    class _NoSQLOpenPopulatedNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            _NoSQLOpenPopulated.setup_detail(self)

    class Database_make_recordsetNdbm(_NoSQLOpenPopulatedNdbm):
        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_make_recordset_key_like
        test_03 = Database_make_recordset.t03_make_recordset_key_like
        test_04 = Database_make_recordset.t04_make_recordset_key_like
        test_05 = Database_make_recordset.t05_make_recordset_key_like
        test_06 = Database_make_recordset.t06_make_recordset_key_like
        test_07 = Database_make_recordset.t07_make_recordset_key_like
        test_08 = Database_make_recordset.t08_make_recordset_key
        test_09 = Database_make_recordset.t09_make_recordset_key
        test_10 = Database_make_recordset.t10_make_recordset_key
        test_11 = Database_make_recordset.t11_make_recordset_key
        test_12 = Database_make_recordset.t12_make_recordset_key
        test_13 = Database_make_recordset.t13_make_recordset_key_startswith
        test_14 = Database_make_recordset.t14_make_recordset_key_startswith
        test_15 = Database_make_recordset.t15_make_recordset_key_startswith
        test_16 = Database_make_recordset.t16_make_recordset_key_startswith
        test_17 = Database_make_recordset.t17_make_recordset_key_startswith
        test_18 = Database_make_recordset.t18_make_recordset_key_startswith
        test_19 = Database_make_recordset.t19_make_recordset_key_range
        test_20 = Database_make_recordset.t20_make_recordset_key_range
        test_21 = Database_make_recordset.t21_make_recordset_key_range
        test_22 = Database_make_recordset.t22_make_recordset_key_range
        test_23 = Database_make_recordset.t23_make_recordset_key_range
        test_24 = Database_make_recordset.t24_make_recordset_key_range
        test_25 = Database_make_recordset.t25_make_recordset_key_range
        test_26 = Database_make_recordset.t26_make_recordset_key_range
        test_27 = Database_make_recordset.t27_make_recordset_key_range
        test_28 = Database_make_recordset.t28_make_recordset_key_range
        test_29 = Database_make_recordset.t29_make_recordset_key_range
        test_30 = Database_make_recordset.t30_make_recordset_key_range
        test_31 = Database_make_recordset.t31_make_recordset_key_range
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_all
        test_35 = Database_make_recordset.t35_make_recordset_all
        test_36 = Database_make_recordset.t36_make_recordset_nil

    class Database_file_unfile_recordsNdbm(_NoSQLOpenPopulatedNdbm):
        test_01 = Database_file_unfile_records.t01
        test_02 = Database_file_unfile_records.t02_unfile_records_under
        test_03 = Database_file_unfile_records.t03_unfile_records_under
        test_04 = Database_file_unfile_records.t04_file_records_under
        test_05 = Database_file_unfile_records.t05_file_records_under
        test_06 = Database_file_unfile_records.t06_file_records_under
        test_07 = Database_file_unfile_records.t07_file_records_under
        test_08 = Database_file_unfile_records.t08_file_records_under
        test_09 = Database_file_unfile_records.t09_file_records_under
        test_10 = Database_file_unfile_records.t10_file_records_under

    class Database_database_create_cursorsNdbm(_NoSQLOpenNdbm):
        test_01 = Database_database_create_cursors.t01
        test_02 = Database_database_create_cursors.t02_database_cursor_primary
        test_03 = (
            Database_database_create_cursors.t03_database_cursor_secondary_tree
        )
        test_04 = (
            Database_database_create_cursors.t04_database_cursor_secondary_hash
        )
        test_05 = Database_database_create_cursors.t05_create_recordset_cursor
        test_06 = (
            Database_database_create_cursors.t06_database_cursor_recordset
        )

    class Database_freed_record_numberNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            Database_freed_record_number.setup_detail(self)

        test_01 = Database_freed_record_number.t01
        test_02 = (
            Database_freed_record_number.t02_note_freed_record_number_segment
        )
        test_03 = (
            Database_freed_record_number.t03_get_lowest_freed_record_number
        )
        test_04 = (
            Database_freed_record_number.t04_get_lowest_freed_record_number
        )
        test_05 = (
            Database_freed_record_number.t05_get_lowest_freed_record_number
        )
        test_06 = (
            Database_freed_record_number.t06_get_lowest_freed_record_number
        )
        test_07 = (
            Database_freed_record_number.t07_get_lowest_freed_record_number
        )
        test_08 = (
            Database_freed_record_number.t08_get_lowest_freed_record_number
        )

    class Database_empty_freed_record_numberNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorNdbm(_NoSQLOpenNdbm):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlNdbm(_NoSQLOpenNdbm):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment_01
        test_09 = ExistenceBitmapControl.t09_put_ebm_segment_02
        test_10 = ExistenceBitmapControl.t10_append_ebm_segment
        test_11 = ExistenceBitmapControl.t11_set_high_record_number_01
        test_12 = ExistenceBitmapControl.t12_set_high_record_number_02
        test_13 = ExistenceBitmapControl.t13_set_high_record_number_03
        test_14 = ExistenceBitmapControl.t14_set_high_record_number_04


if unqlite:

    class _NoSQLUnqlite(_NoSQL):
        def setUp(self):
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()

    class Database___init__Unqlite(_NoSQLUnqlite):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset
        test_06 = DatabaseInstance.t06__generate_database_file_name

    class Database_open_databaseUnqlite(_NoSQLUnqlite):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02
        test_03 = Database_open_database.t03
        test_04 = Database_open_database.t04_close_database
        test_05 = Database_open_database.t05_close_database_contexts
        test_06 = Database_open_database.t06
        test_07 = Database_open_database.t07
        test_08 = Database_open_database.t08
        test_09 = Database_open_database.t09
        test_12 = Database_open_database.t12_is_database_file_active
        check_specification = Database_open_database.check_specification

    class Database_add_field_to_existing_databaseUnqlite(_NoSQLUnqlite):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskUnqlite(Database_do_database_task):
        def setUp(self):
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()

        def test_01_do_database_task(self):
            def m(*a, **k):
                pass

            path = None
            self.database = self._AD(path)
            d = self.database
            d.open_database()
            self.assertEqual(d.do_database_task(m), None)

        def test_02_do_database_task(self):
            def m(*a, **k):
                pass

            path = None
            self.database = self._AD(path)
            d = self.database
            self.assertEqual(d.do_database_task(m), None)

    class _NoSQLOpenUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            _NoSQLOpen.setup_detail(self)

        def tearDown(self):
            _NoSQLOpen.teardown_detail(self)
            super().tearDown()

    class DatabaseTransactionsUnqlite(_NoSQLOpenUnqlite):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteUnqlite(_NoSQLOpenUnqlite):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_replace
        test_08 = Database_put_replace_delete.t08_delete
        test_09 = Database_put_replace_delete.t09_delete
        test_10 = Database_put_replace_delete.t10_delete

    class Database_methodsUnqlite(_NoSQLOpenUnqlite):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_14 = Database_methods.t14_recordset_record_number
        test_15 = Database_methods.t15_recordset_record_number
        test_16 = Database_methods.t16_recordset_record_number
        test_17 = Database_methods.t17_recordset_record_number_range
        test_18 = Database_methods.t18_recordset_record_number_range
        test_19 = Database_methods.t19_recordset_record_number_range
        test_20 = Database_methods.t20_recordset_record_number_range
        test_21 = Database_methods.t21_recordset_record_number_range
        test_22 = Database_methods.t22_recordset_record_number_range
        test_23 = Database_methods.t23_recordset_record_number_range
        test_24 = Database_methods.t24_recordset_ebm
        test_25 = Database_methods.t25_recordset_ebm

        def test26_get_table_connection(self):
            self.assertIsInstance(
                self.database.get_table_connection("file1"), unqlite.UnQLite
            )

        create_ebm = Database_methods.create_ebm

    class Database_find_values__emptyUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            Database_find_values__empty.setup_detail(self)

        test_01 = Database_find_values__empty.t01_find_values
        test_02 = Database_find_values__empty.t02_find_values
        test_03 = Database_find_values__empty.t03_find_values
        test_04 = Database_find_values__empty.t04_find_values
        test_05 = Database_find_values__empty.t05_find_values
        test_06 = Database_find_values__empty.t06_find_values
        test_07 = Database_find_values__empty.t07_find_values
        test_08 = Database_find_values__empty.t08_find_values
        test_09 = Database_find_values__empty.t09_find_values
        test_10 = Database_find_values__empty.t10_find_values

    class Database_find_values__populatedUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            Database_find_values__populated.setup_detail(self)

        test_01 = Database_find_values__populated.t01_find_values
        test_02 = Database_find_values__populated.t02_find_values
        test_03 = Database_find_values__populated.t03_find_values
        test_04 = Database_find_values__populated.t04_find_values
        test_05 = Database_find_values__populated.t05_find_values
        test_06 = Database_find_values__populated.t06_find_values
        test_07 = Database_find_values__populated.t07_find_values
        test_08 = Database_find_values__populated.t08_find_values
        test_09 = Database_find_values__populated.t09_find_values

    class Database_add_record_to_field_valueUnqlite(_NoSQLOpenUnqlite):
        test_01 = DatabaseAddRecordToFieldValue.t01
        test_02 = DatabaseAddRecordToFieldValue.t02__assumptions
        test_03 = (
            DatabaseAddRecordToFieldValue.t03_add_record_to_tree_field_value
        )
        test_04 = (
            DatabaseAddRecordToFieldValue.t04_add_record_to_hash_field_value
        )

    class Database_remove_record_from_field_valueUnqlite(_NoSQLOpenUnqlite):
        test_01 = DatabaseRemoveRecordFieldValue.t01
        test_02 = (
            DatabaseRemoveRecordFieldValue.t02_remove_record_tree_field_value
        )
        test_03 = (
            DatabaseRemoveRecordFieldValue.t03_remove_record_hash_field_value
        )

    class Database_populate_segmentUnqlite(_NoSQLOpenUnqlite):
        test_01 = Database_populate_segment.t01
        test_02 = Database_populate_segment.t02_populate_segment
        test_04 = Database_populate_segment.t04_populate_segment
        test_06 = Database_populate_segment.t06_populate_segment

    class _NoSQLOpenPopulatedUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            _NoSQLOpenPopulated.setup_detail(self)

    class Database_make_recordsetUnqlite(_NoSQLOpenPopulatedUnqlite):
        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_make_recordset_key_like
        test_03 = Database_make_recordset.t03_make_recordset_key_like
        test_04 = Database_make_recordset.t04_make_recordset_key_like
        test_05 = Database_make_recordset.t05_make_recordset_key_like
        test_06 = Database_make_recordset.t06_make_recordset_key_like
        test_07 = Database_make_recordset.t07_make_recordset_key_like
        test_08 = Database_make_recordset.t08_make_recordset_key
        test_09 = Database_make_recordset.t09_make_recordset_key
        test_10 = Database_make_recordset.t10_make_recordset_key
        test_11 = Database_make_recordset.t11_make_recordset_key
        test_12 = Database_make_recordset.t12_make_recordset_key
        test_13 = Database_make_recordset.t13_make_recordset_key_startswith
        test_14 = Database_make_recordset.t14_make_recordset_key_startswith
        test_15 = Database_make_recordset.t15_make_recordset_key_startswith
        test_16 = Database_make_recordset.t16_make_recordset_key_startswith
        test_17 = Database_make_recordset.t17_make_recordset_key_startswith
        test_18 = Database_make_recordset.t18_make_recordset_key_startswith
        test_19 = Database_make_recordset.t19_make_recordset_key_range
        test_20 = Database_make_recordset.t20_make_recordset_key_range
        test_21 = Database_make_recordset.t21_make_recordset_key_range
        test_22 = Database_make_recordset.t22_make_recordset_key_range
        test_23 = Database_make_recordset.t23_make_recordset_key_range
        test_24 = Database_make_recordset.t24_make_recordset_key_range
        test_25 = Database_make_recordset.t25_make_recordset_key_range
        test_26 = Database_make_recordset.t26_make_recordset_key_range
        test_27 = Database_make_recordset.t27_make_recordset_key_range
        test_28 = Database_make_recordset.t28_make_recordset_key_range
        test_29 = Database_make_recordset.t29_make_recordset_key_range
        test_30 = Database_make_recordset.t30_make_recordset_key_range
        test_31 = Database_make_recordset.t31_make_recordset_key_range
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_all
        test_35 = Database_make_recordset.t35_make_recordset_all
        test_36 = Database_make_recordset.t36_make_recordset_nil

    class Database_file_unfile_recordsUnqlite(_NoSQLOpenPopulatedUnqlite):
        test_01 = Database_file_unfile_records.t01
        test_02 = Database_file_unfile_records.t02_unfile_records_under
        test_03 = Database_file_unfile_records.t03_unfile_records_under
        test_04 = Database_file_unfile_records.t04_file_records_under
        test_05 = Database_file_unfile_records.t05_file_records_under
        test_06 = Database_file_unfile_records.t06_file_records_under
        test_07 = Database_file_unfile_records.t07_file_records_under
        test_08 = Database_file_unfile_records.t08_file_records_under
        test_09 = Database_file_unfile_records.t09_file_records_under
        test_10 = Database_file_unfile_records.t10_file_records_under

    class Database_database_create_cursorsUnqlite(_NoSQLOpenUnqlite):
        test_01 = Database_database_create_cursors.t01
        test_02 = Database_database_create_cursors.t02_database_cursor_primary
        test_03 = (
            Database_database_create_cursors.t03_database_cursor_secondary_tree
        )
        test_04 = (
            Database_database_create_cursors.t04_database_cursor_secondary_hash
        )
        test_05 = Database_database_create_cursors.t05_create_recordset_cursor
        test_06 = (
            Database_database_create_cursors.t06_database_cursor_recordset
        )

    class Database_freed_record_numberUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            Database_freed_record_number.setup_detail(self)

        test_01 = Database_freed_record_number.t01
        test_02 = (
            Database_freed_record_number.t02_note_freed_record_number_segment
        )
        test_03 = (
            Database_freed_record_number.t03_get_lowest_freed_record_number
        )
        test_04 = (
            Database_freed_record_number.t04_get_lowest_freed_record_number
        )
        test_05 = (
            Database_freed_record_number.t05_get_lowest_freed_record_number
        )
        test_06 = (
            Database_freed_record_number.t06_get_lowest_freed_record_number
        )
        test_07 = (
            Database_freed_record_number.t07_get_lowest_freed_record_number
        )
        test_08 = (
            Database_freed_record_number.t08_get_lowest_freed_record_number
        )

    class Database_empty_freed_record_numberUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorUnqlite(_NoSQLOpenUnqlite):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlUnqlite(_NoSQLOpenUnqlite):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment_01
        test_09 = ExistenceBitmapControl.t09_put_ebm_segment_02
        test_10 = ExistenceBitmapControl.t10_append_ebm_segment
        test_11 = ExistenceBitmapControl.t11_set_high_record_number_01
        test_12 = ExistenceBitmapControl.t12_set_high_record_number_02
        test_13 = ExistenceBitmapControl.t13_set_high_record_number_03
        test_14 = ExistenceBitmapControl.t14_set_high_record_number_04


if vedis:

    class _NoSQLVedis(_NoSQL):
        def setUp(self):
            self._oda = vedis, vedis.Vedis, None
            super().setUp()

    class Database___init__Vedis(_NoSQLVedis):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset
        test_06 = DatabaseInstance.t06__generate_database_file_name

    class Database_open_databaseVedis(_NoSQLVedis):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02
        test_03 = Database_open_database.t03
        test_04 = Database_open_database.t04_close_database
        test_05 = Database_open_database.t05_close_database_contexts
        test_06 = Database_open_database.t06
        test_07 = Database_open_database.t07
        test_08 = Database_open_database.t08
        test_09 = Database_open_database.t09
        test_12 = Database_open_database.t12_is_database_file_active
        check_specification = Database_open_database.check_specification

    class Database_add_field_to_existing_databaseVedis(_NoSQLVedis):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskVedis(Database_do_database_task):
        def setUp(self):
            self._oda = vedis, vedis.Vedis, None
            super().setUp()

        def test_01_do_database_task(self):
            def m(*a, **k):
                pass

            path = None
            self.database = self._AD(path)
            d = self.database
            d.open_database()
            self.assertEqual(d.do_database_task(m), None)

        def test_02_do_database_task(self):
            def m(*a, **k):
                pass

            path = None
            self.database = self._AD(path)
            d = self.database
            self.assertEqual(d.do_database_task(m), None)

    class _NoSQLOpenVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            _NoSQLOpen.setup_detail(self)

        def tearDown(self):
            _NoSQLOpen.teardown_detail(self)
            super().tearDown()

    class DatabaseTransactionsVedis(_NoSQLOpenVedis):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteVedis(_NoSQLOpenVedis):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_replace
        test_08 = Database_put_replace_delete.t08_delete
        test_09 = Database_put_replace_delete.t09_delete
        test_10 = Database_put_replace_delete.t10_delete

    class Database_methodsVedis(_NoSQLOpenVedis):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_14 = Database_methods.t14_recordset_record_number
        test_15 = Database_methods.t15_recordset_record_number
        test_16 = Database_methods.t16_recordset_record_number
        test_17 = Database_methods.t17_recordset_record_number_range
        test_18 = Database_methods.t18_recordset_record_number_range
        test_19 = Database_methods.t19_recordset_record_number_range
        test_20 = Database_methods.t20_recordset_record_number_range
        test_21 = Database_methods.t21_recordset_record_number_range
        test_22 = Database_methods.t22_recordset_record_number_range
        test_23 = Database_methods.t23_recordset_record_number_range
        test_24 = Database_methods.t24_recordset_ebm
        test_25 = Database_methods.t25_recordset_ebm

        def test26_get_table_connection(self):
            self.assertIsInstance(
                self.database.get_table_connection("file1"), vedis.Vedis
            )

        create_ebm = Database_methods.create_ebm

    class Database_find_values__emptyVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            Database_find_values__empty.setup_detail(self)

        test_01 = Database_find_values__empty.t01_find_values
        test_02 = Database_find_values__empty.t02_find_values
        test_03 = Database_find_values__empty.t03_find_values
        test_04 = Database_find_values__empty.t04_find_values
        test_05 = Database_find_values__empty.t05_find_values
        test_06 = Database_find_values__empty.t06_find_values
        test_07 = Database_find_values__empty.t07_find_values
        test_08 = Database_find_values__empty.t08_find_values
        test_09 = Database_find_values__empty.t09_find_values
        test_10 = Database_find_values__empty.t10_find_values

    class Database_find_values__populatedVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            Database_find_values__populated.setup_detail(self)

        test_01 = Database_find_values__populated.t01_find_values
        test_02 = Database_find_values__populated.t02_find_values
        test_03 = Database_find_values__populated.t03_find_values
        test_04 = Database_find_values__populated.t04_find_values
        test_05 = Database_find_values__populated.t05_find_values
        test_06 = Database_find_values__populated.t06_find_values
        test_07 = Database_find_values__populated.t07_find_values
        test_08 = Database_find_values__populated.t08_find_values
        test_09 = Database_find_values__populated.t09_find_values

    class Database_add_record_to_field_valueVedis(_NoSQLOpenVedis):
        test_01 = DatabaseAddRecordToFieldValue.t01
        test_02 = DatabaseAddRecordToFieldValue.t02__assumptions
        test_03 = (
            DatabaseAddRecordToFieldValue.t03_add_record_to_tree_field_value
        )
        test_04 = (
            DatabaseAddRecordToFieldValue.t04_add_record_to_hash_field_value
        )

    class Database_remove_record_from_field_valueVedis(_NoSQLOpenVedis):
        test_01 = DatabaseRemoveRecordFieldValue.t01
        test_02 = (
            DatabaseRemoveRecordFieldValue.t02_remove_record_tree_field_value
        )
        test_03 = (
            DatabaseRemoveRecordFieldValue.t03_remove_record_hash_field_value
        )

    class Database_populate_segmentVedis(_NoSQLOpenVedis):
        test_01 = Database_populate_segment.t01
        test_02 = Database_populate_segment.t02_populate_segment
        test_04 = Database_populate_segment.t04_populate_segment
        test_06 = Database_populate_segment.t06_populate_segment

    class _NoSQLOpenPopulatedVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            _NoSQLOpenPopulated.setup_detail(self)

    class Database_make_recordsetVedis(_NoSQLOpenPopulatedVedis):
        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_make_recordset_key_like
        test_03 = Database_make_recordset.t03_make_recordset_key_like
        test_04 = Database_make_recordset.t04_make_recordset_key_like
        test_05 = Database_make_recordset.t05_make_recordset_key_like
        test_06 = Database_make_recordset.t06_make_recordset_key_like
        test_07 = Database_make_recordset.t07_make_recordset_key_like
        test_08 = Database_make_recordset.t08_make_recordset_key
        test_09 = Database_make_recordset.t09_make_recordset_key
        test_10 = Database_make_recordset.t10_make_recordset_key
        test_11 = Database_make_recordset.t11_make_recordset_key
        test_12 = Database_make_recordset.t12_make_recordset_key
        test_13 = Database_make_recordset.t13_make_recordset_key_startswith
        test_14 = Database_make_recordset.t14_make_recordset_key_startswith
        test_15 = Database_make_recordset.t15_make_recordset_key_startswith
        test_16 = Database_make_recordset.t16_make_recordset_key_startswith
        test_17 = Database_make_recordset.t17_make_recordset_key_startswith
        test_18 = Database_make_recordset.t18_make_recordset_key_startswith
        test_19 = Database_make_recordset.t19_make_recordset_key_range
        test_20 = Database_make_recordset.t20_make_recordset_key_range
        test_21 = Database_make_recordset.t21_make_recordset_key_range
        test_22 = Database_make_recordset.t22_make_recordset_key_range
        test_23 = Database_make_recordset.t23_make_recordset_key_range
        test_24 = Database_make_recordset.t24_make_recordset_key_range
        test_25 = Database_make_recordset.t25_make_recordset_key_range
        test_26 = Database_make_recordset.t26_make_recordset_key_range
        test_27 = Database_make_recordset.t27_make_recordset_key_range
        test_28 = Database_make_recordset.t28_make_recordset_key_range
        test_29 = Database_make_recordset.t29_make_recordset_key_range
        test_30 = Database_make_recordset.t30_make_recordset_key_range
        test_31 = Database_make_recordset.t31_make_recordset_key_range
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_all
        test_35 = Database_make_recordset.t35_make_recordset_all
        test_36 = Database_make_recordset.t36_make_recordset_nil

    class Database_file_unfile_recordsVedis(_NoSQLOpenPopulatedVedis):
        test_01 = Database_file_unfile_records.t01
        test_02 = Database_file_unfile_records.t02_unfile_records_under
        test_03 = Database_file_unfile_records.t03_unfile_records_under
        test_04 = Database_file_unfile_records.t04_file_records_under
        test_05 = Database_file_unfile_records.t05_file_records_under
        test_06 = Database_file_unfile_records.t06_file_records_under
        test_07 = Database_file_unfile_records.t07_file_records_under
        test_08 = Database_file_unfile_records.t08_file_records_under
        test_09 = Database_file_unfile_records.t09_file_records_under
        test_10 = Database_file_unfile_records.t10_file_records_under

    class Database_database_create_cursorsVedis(_NoSQLOpenVedis):
        test_01 = Database_database_create_cursors.t01
        test_02 = Database_database_create_cursors.t02_database_cursor_primary
        test_03 = (
            Database_database_create_cursors.t03_database_cursor_secondary_tree
        )
        test_04 = (
            Database_database_create_cursors.t04_database_cursor_secondary_hash
        )
        test_05 = Database_database_create_cursors.t05_create_recordset_cursor
        test_06 = (
            Database_database_create_cursors.t06_database_cursor_recordset
        )

    class Database_freed_record_numberVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            Database_freed_record_number.setup_detail(self)

        test_01 = Database_freed_record_number.t01
        test_02 = (
            Database_freed_record_number.t02_note_freed_record_number_segment
        )
        test_03 = (
            Database_freed_record_number.t03_get_lowest_freed_record_number
        )
        test_04 = (
            Database_freed_record_number.t04_get_lowest_freed_record_number
        )
        test_05 = (
            Database_freed_record_number.t05_get_lowest_freed_record_number
        )
        test_06 = (
            Database_freed_record_number.t06_get_lowest_freed_record_number
        )
        test_07 = (
            Database_freed_record_number.t07_get_lowest_freed_record_number
        )
        test_08 = (
            Database_freed_record_number.t08_get_lowest_freed_record_number
        )

    class Database_empty_freed_record_numberVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorVedis(_NoSQLOpenVedis):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlVedis(_NoSQLOpenVedis):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment_01
        test_09 = ExistenceBitmapControl.t09_put_ebm_segment_02
        test_10 = ExistenceBitmapControl.t10_append_ebm_segment
        test_11 = ExistenceBitmapControl.t11_set_high_record_number_01
        test_12 = ExistenceBitmapControl.t12_set_high_record_number_02
        test_13 = ExistenceBitmapControl.t13_set_high_record_number_03
        test_14 = ExistenceBitmapControl.t14_set_high_record_number_04


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if gnu_module:
        runner().run(loader(Database___init__Gnu))
        runner().run(loader(Database_transaction_methodsGnu))
        runner().run(loader(DatabaseInstanceGnu))
        runner().run(loader(Database_open_databaseGnu))
        runner().run(loader(Database_add_field_to_existing_databaseGnu))
        runner().run(loader(Database_do_database_taskGnu))
        runner().run(loader(DatabaseTransactionsGnu))
        runner().run(loader(Database_put_replace_deleteGnu))
        runner().run(loader(Database_methodsGnu))
        runner().run(loader(Database_find_values__emptyGnu))
        runner().run(loader(Database_find_values__populatedGnu))
        runner().run(loader(Database_add_record_to_field_valueGnu))
        runner().run(loader(Database_remove_record_from_field_valueGnu))
        runner().run(loader(Database_populate_segmentGnu))
        runner().run(loader(Database_make_recordsetGnu))
        runner().run(loader(Database_file_unfile_recordsGnu))
        runner().run(loader(Database_database_create_cursorsGnu))
        runner().run(loader(Database_freed_record_numberGnu))
        runner().run(loader(Database_empty_freed_record_numberGnu))
        runner().run(loader(RecordsetCursorGnu))
        runner().run(loader(ExistenceBitmapControlGnu))
    if ndbm_module:
        runner().run(loader(Database___init__Ndbm))
        runner().run(loader(Database_transaction_methodsNdbm))
        runner().run(loader(DatabaseInstanceNdbm))
        runner().run(loader(Database_open_databaseNdbm))
        runner().run(loader(Database_add_field_to_existing_databaseNdbm))
        runner().run(loader(Database_do_database_taskNdbm))
        runner().run(loader(DatabaseTransactionsNdbm))
        runner().run(loader(Database_put_replace_deleteNdbm))
        runner().run(loader(Database_methodsNdbm))
        runner().run(loader(Database_find_values__emptyNdbm))
        runner().run(loader(Database_find_values__populatedNdbm))
        runner().run(loader(Database_add_record_to_field_valueNdbm))
        runner().run(loader(Database_remove_record_from_field_valueNdbm))
        runner().run(loader(Database_populate_segmentNdbm))
        runner().run(loader(Database_make_recordsetNdbm))
        runner().run(loader(Database_file_unfile_recordsNdbm))
        runner().run(loader(Database_database_create_cursorsNdbm))
        runner().run(loader(Database_freed_record_numberNdbm))
        runner().run(loader(Database_empty_freed_record_numberNdbm))
        runner().run(loader(RecordsetCursorNdbm))
        runner().run(loader(ExistenceBitmapControlNdbm))
    if unqlite:
        runner().run(loader(Database___init__Unqlite))
        runner().run(loader(Database_transaction_methodsUnqlite))
        runner().run(loader(DatabaseInstanceUnqlite))
        runner().run(loader(Database_open_databaseUnqlite))
        runner().run(loader(Database_add_field_to_existing_databaseUnqlite))
        runner().run(loader(Database_do_database_taskUnqlite))
        runner().run(loader(DatabaseTransactionsUnqlite))
        runner().run(loader(Database_put_replace_deleteUnqlite))
        runner().run(loader(Database_methodsUnqlite))
        runner().run(loader(Database_find_values__emptyUnqlite))
        runner().run(loader(Database_find_values__populatedUnqlite))
        runner().run(loader(Database_add_record_to_field_valueUnqlite))
        runner().run(loader(Database_remove_record_from_field_valueUnqlite))
        runner().run(loader(Database_populate_segmentUnqlite))
        runner().run(loader(Database_make_recordsetUnqlite))
        runner().run(loader(Database_file_unfile_recordsUnqlite))
        runner().run(loader(Database_database_create_cursorsUnqlite))
        runner().run(loader(Database_freed_record_numberUnqlite))
        runner().run(loader(Database_empty_freed_record_numberUnqlite))
        runner().run(loader(RecordsetCursorUnqlite))
        runner().run(loader(ExistenceBitmapControlUnqlite))
    if vedis:
        runner().run(loader(Database___init__Vedis))
        runner().run(loader(Database_transaction_methodsVedis))
        runner().run(loader(DatabaseInstanceVedis))
        runner().run(loader(Database_open_databaseVedis))
        runner().run(loader(Database_add_field_to_existing_databaseVedis))
        runner().run(loader(Database_do_database_taskVedis))
        runner().run(loader(DatabaseTransactionsVedis))
        runner().run(loader(Database_put_replace_deleteVedis))
        runner().run(loader(Database_methodsVedis))
        runner().run(loader(Database_find_values__emptyVedis))
        runner().run(loader(Database_find_values__populatedVedis))
        runner().run(loader(Database_add_record_to_field_valueVedis))
        runner().run(loader(Database_remove_record_from_field_valueVedis))
        runner().run(loader(Database_populate_segmentVedis))
        runner().run(loader(Database_make_recordsetVedis))
        runner().run(loader(Database_file_unfile_recordsVedis))
        runner().run(loader(Database_database_create_cursorsVedis))
        runner().run(loader(Database_freed_record_numberVedis))
        runner().run(loader(Database_empty_freed_record_numberVedis))
        runner().run(loader(RecordsetCursorVedis))
        runner().run(loader(ExistenceBitmapControlVedis))
