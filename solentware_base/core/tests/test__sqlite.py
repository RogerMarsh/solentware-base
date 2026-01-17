# test__sqlite.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_sqlite _database tests with apsw and sqlite3 interfaces."""

import unittest
import os
import shutil

try:
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None

from .. import _sqlite
from .. import filespec
from .. import recordset
from .. import recordsetcursor
from .. import recordsetbasecursor
from ..segmentsize import SegmentSize
from ..wherevalues import ValuesClause


class _SQLite(unittest.TestCase):
    # The sets of tests are run inside a loop for sqlite3 and apsw, and some
    # tests change SegmentSize.db_segment_size_bytes, so reset it to the
    # initial value in tearDown().

    def setUp(self):
        self._ssb = SegmentSize.db_segment_size_bytes

        class _D(_sqlite.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self._ssb


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
            _sqlite.DatabaseError,
            "".join(("Database folder name {} is not valid$",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def t04(self):
        database = self._D({}, folder="a")
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
        self.assertEqual(database.index, {})
        self.assertEqual(database.segment_table, {})
        self.assertEqual(database.ebm_control, {})
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
        ssb = self._ssb
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self._ssb = ssb


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
            _sqlite.DatabaseError,
            "".join(("Database segment size must be an int$",)),
            self.database._validate_segment_size_bytes,
            *("a",),
        )
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
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


# Memory databases are used for these tests.
class Database_open_database:
    def t01(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"open_database\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.open_database,
            *(None, None, None),
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
        self.open_database_temp(self.database)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self.get_connection_class())

    def t03(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.open_database_temp(self.database)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertIsInstance(self.database.dbenv, self.get_connection_class())

    def t04_close_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.open_database_temp(self.database)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)

    def t05_close_database_contexts(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.open_database_temp(self.database)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)

    def t06(self):
        self.database = self._D({"file1": {"field1"}})
        self.open_database_temp(self.database)
        self.check_specification()

    def t07(self):
        self.database = self._D(filespec.FileSpec(**{"file1": {"field1"}}))
        self.open_database_temp(self.database)
        self.check_specification()

    def t08(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}})
        )
        self.open_database_temp(self.database, files={"file1"})
        self.check_specification()

    def t09(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        self.open_database_temp(self.database)
        self.assertEqual(
            self.database.table,
            {
                "file1": "file1",
                "___control": "___control",
                "file1_field1": "file1_field1",
                "file2": "file2",
            },
        )
        self.assertEqual(
            self.database.index, {"file1_field1": "ixfile1_field1"}
        )

        self.assertEqual(
            self.database.segment_table,
            {"file1": "file1__segment", "file2": "file2__segment"},
        )
        self.assertEqual(self.database.ebm_control["file1"]._file, "file1")
        self.assertEqual(
            self.database.ebm_control["file1"].ebm_table, "file1__ebm"
        )
        self.assertEqual(self.database.ebm_control["file2"]._file, "file2")
        self.assertEqual(
            self.database.ebm_control["file2"].ebm_table, "file2__ebm"
        )
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _sqlite.ExistenceBitmapControl)

    # Comment in _sqlite.py suggests this method is not needed.
    def t12_is_database_file_active(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        self.assertEqual(d.is_database_file_active("file1"), False)
        self.open_database_temp(d)
        self.assertEqual(d.is_database_file_active("file1"), True)

    def check_specification(self):
        self.assertEqual(
            self.database.table,
            {
                "file1": "file1",
                "___control": "___control",
                "file1_field1": "file1_field1",
            },
        )
        self.assertEqual(
            self.database.index, {"file1_field1": "ixfile1_field1"}
        )
        self.assertEqual(
            self.database.segment_table, {"file1": "file1__segment"}
        )
        self.assertEqual(self.database.ebm_control["file1"]._file, "file1")
        self.assertEqual(
            self.database.ebm_control["file1"].ebm_table, "file1__ebm"
        )
        for v in self.database.ebm_control.values():
            self.assertIsInstance(v, _sqlite.ExistenceBitmapControl)


# Memory databases cannot be used for these tests.
class DatabaseAddFieldToExistingDatabase:

    def t13_add_field_to_open_database(self):
        folder = "aaaa"
        database = self._D({"file1": {"field1"}}, folder=folder)
        self.open_database_temp(database)
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
            self.open_database_temp,  # database.open_database,
            *(database,),  # *(dbe_module,),
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
    # _SQLite does this, but Database_do_database_task is not based on it.

    def tearDown(self):
        self.database = None
        self._AD = None
        SegmentSize.db_segment_size_bytes = self._ssb

    def t01_do_database_task(self):
        def m(*a, **k):
            pass

        self.database = self._AD(None)
        d = self.database
        d.open_database()
        self.assertEqual(d.do_database_task(m), None)


# Memory databases are used for these tests.
# Use the 'testing only' segment size for convenience of setup and eyeballing.
class _SQLiteOpen(_SQLite):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(dbe_module)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class DatabaseTransactions:
    # apsw exception is apsw.SQLError
    # sqlite3 exception is sqlite3.OperationalError
    def t01(self):
        self.database.start_transaction()
        self.assertRaisesRegex(
            Exception,
            "cannot start a transaction within a transaction$",
            self.database.start_transaction,
        )

    def t02(self):
        self.database.start_transaction()
        self.database.backout()

    def t03(self):
        self.database.start_transaction()
        self.database.commit()

    # apsw exception is apsw.SQLError
    # sqlite3 exception is sqlite3.OperationalError
    def t04(self):
        self.assertRaisesRegex(
            Exception,
            "cannot rollback - no transaction is active$",
            self.database.backout,
        )

    # apsw exception is apsw.SQLError
    # sqlite3 exception is sqlite3.OperationalError
    def t05(self):
        self.assertRaisesRegex(
            Exception,
            "cannot commit - no transaction is active$",
            self.database.commit,
        )


class Database_put_replace_delete:
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
        self.assertEqual(recno, 1)

    def t03_put(self):
        self.assertEqual(self.database.put("file1", 2, "new value"), None)
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 3)

    def t04_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 1)
        self.assertEqual(self.database.put("file1", 1, "renew value"), None)
        recno = self.database.put("file1", None, "other value")
        self.assertEqual(recno, 2)

    def t05_replace(self):
        self.assertEqual(
            self.database.replace("file1", 1, "new value", "renew value"), None
        )

    def t06_delete(self):
        self.assertEqual(self.database.delete("file1", 1, "new value"), None)


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
                    r"get_segment_records\(\) missing 2 required ",
                    "positional arguments: 'rownumber' and 'file'$",
                )
            ),
            self.database.get_segment_records,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_segment_records\(\) missing 2 required ",
                    "positional arguments: 'values' and 'file'$",
                )
            ),
            self.database.set_segment_records,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"delete_segment_records\(\) missing 2 required ",
                    "positional arguments: 'values' and 'file'$",
                )
            ),
            self.database.delete_segment_records,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"insert_segment_records\(\) missing 2 required ",
                    "positional arguments: 'values' and 'file'$",
                )
            ),
            self.database.insert_segment_records,
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
        self.database.put("file1", None, "new value")
        self.assertEqual(
            self.database.get_primary_record("file1", 1), (1, "new value")
        )

    def t05_remove_record_from_ebm(self):
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
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

    def t09_get_segment_records(self):
        self.database.insert_segment_records((12,), "file1")
        self.assertEqual(self.database.get_segment_records(1, "file1"), 12)

    def t10_get_segment_records(self):
        self.database.insert_segment_records((12,), "file1")
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
            "Segment record 2 missing in 'file1'$",
            self.database.get_segment_records,
            *(2, "file1"),
        )

    def t11_set_segment_records(self):
        self.database.insert_segment_records((12,), "file1")
        self.database.set_segment_records((13, 1), "file1")
        self.assertEqual(self.database.get_segment_records(1, "file1"), 13)

    def t12_delete_segment_records(self):
        self.database.delete_segment_records((12,), "file1")

    def t13_insert_segment_records(self):
        self.assertEqual(
            self.database.insert_segment_records((12,), "file1"), 1
        )

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
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1",
                "(",
                "file1",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = 1, "Some value"
        cursor.execute(statement, values)
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = 1, b"\x740" + b"\x00" * (
            SegmentSize.db_segment_size_bytes - 1
        )
        cursor.execute(statement, values)
        rl = self.database.recordlist_record_number("file1", key=1)
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
                    b"\x7f\xff\xff\xff\xff\xff\xff\xff",
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
                    b"\x7f\xff\xff\xff\xf0\x00\x00\x00",
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
        self.create_ebm_extra(2)
        self.create_ebm_extra(3)
        self.create_ebm_extra(4)
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
        self.create_ebm_extra(2)
        self.create_ebm_extra(3)
        self.create_ebm_extra(4)
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
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def t26_get_table_connection(self):
        self.assertIsInstance(
            self.database.get_table_connection("file1"),
            self.get_connection_class(),
        )

    def create_ebm(self):
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = 1, b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor.execute(statement, values)

    def create_ebm_extra(self, segment):
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = (
            segment,
            b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1),
        )
        cursor.execute(statement, values)


class Database_find_values_empty:
    def get_keys(self):
        return [i for i in self.database.find_values(self.valuespec, "file1")]

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
        self.assertEqual(self.get_keys(), [])

    def t03_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t04_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t05_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t06_find_values(self):
        self.valuespec.above_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t07_find_values(self):
        self.valuespec.from_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t08_find_values(self):
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t09_find_values(self):
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t10_find_values(self):
        self.assertEqual(self.get_keys(), [])


class Database_find_values:
    def get_keys(self):
        return [i for i in self.database.find_values(self.valuespec, "file1")]

    def t11_find_values_01(self):
        ae = self.assertEqual
        keys = self.get_keys()
        ae(len(keys), 5)
        ae(set(keys), set(("d", "e", "c", "dk", "f")))

    def t11_find_values_02_above_below(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.below_value = "f"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(set(keys), set(("d", "e", "dk")))

    def t11_find_values_03_above_to(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.to_value = "e"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(set(keys), set(("d", "e", "dk")))

    def t11_find_values_04_from_to(self):
        ae = self.assertEqual
        self.valuespec.from_value = "c"
        self.valuespec.to_value = "dr"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(set(keys), set(("d", "c", "dk")))

    def t11_find_values_05_from_below(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        self.valuespec.below_value = "e"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(set(keys), set(("d", "dk")))

    def t11_find_values_06_above(self):
        ae = self.assertEqual
        self.valuespec.above_value = "d"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(set(keys), set(("e", "dk", "f")))

    def t11_find_values_07_from(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        keys = self.get_keys()
        ae(len(keys), 4)
        ae(set(keys), set(("d", "e", "dk", "f")))

    def t11_find_values_08_to(self):
        ae = self.assertEqual
        self.valuespec.to_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(set(keys), set(("d", "c", "dk")))

    def t11_find_values_09_below(self):
        ae = self.assertEqual
        self.valuespec.below_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(set(keys), set(("d", "c")))


class Database_find_values_ascending_empty:
    def get_keys(self):
        return [
            i
            for i in self.database.find_values_ascending(
                self.valuespec, "file1"
            )
        ]

    def t01_find_values_ascending(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"find_values_ascending\(\) missing 2 required ",
                    "positional arguments: 'valuespec' and 'file'$",
                )
            ),
            self.database.find_values_ascending,
        )

    def t02_find_values_ascending(self):
        self.valuespec.above_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t03_find_values_ascending(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t04_find_values_ascending(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t05_find_values_ascending(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t06_find_values_ascending(self):
        self.valuespec.above_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t07_find_values_ascending(self):
        self.valuespec.from_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t08_find_values_ascending(self):
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t09_find_values_ascending(self):
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t10_find_values_ascending(self):
        self.assertEqual(self.get_keys(), [])


class Database_find_values_ascending:
    def get_keys(self):
        return [
            i
            for i in self.database.find_values_ascending(
                self.valuespec, "file1"
            )
        ]

    def t11_find_values_ascending_01(self):
        ae = self.assertEqual
        keys = self.get_keys()
        ae(len(keys), 5)
        ae(keys, ["c", "d", "dk", "e", "f"])

    def t11_find_values_ascending_02_above_below(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.below_value = "f"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["d", "dk", "e"])

    def t11_find_values_ascending_03_above_to(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.to_value = "e"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["d", "dk", "e"])

    def t11_find_values_ascending_04_from_to(self):
        ae = self.assertEqual
        self.valuespec.from_value = "c"
        self.valuespec.to_value = "dr"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["c", "d", "dk"])

    def t11_find_values_ascending_05_from_below(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        self.valuespec.below_value = "e"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(keys, ["d", "dk"])

    def t11_find_values_ascending_06_above(self):
        ae = self.assertEqual
        self.valuespec.above_value = "d"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["dk", "e", "f"])

    def t11_find_values_ascending_07_from(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        keys = self.get_keys()
        ae(len(keys), 4)
        ae(keys, ["d", "dk", "e", "f"])

    def t11_find_values_ascending_08_to(self):
        ae = self.assertEqual
        self.valuespec.to_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["c", "d", "dk"])

    def t11_find_values_ascending_09_below(self):
        ae = self.assertEqual
        self.valuespec.below_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(keys, ["c", "d"])


class Database_find_values_descending_empty:
    def get_keys(self):
        return [
            i
            for i in self.database.find_values_descending(
                self.valuespec, "file1"
            )
        ]

    def t01_find_values_descending(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"find_values_descending\(\) missing 2 required ",
                    "positional arguments: 'valuespec' and 'file'$",
                )
            ),
            self.database.find_values_descending,
        )

    def t02_find_values_descending(self):
        self.valuespec.above_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t03_find_values_descending(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t04_find_values_descending(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t05_find_values_descending(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t06_find_values_descending(self):
        self.valuespec.above_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t07_find_values_descending(self):
        self.valuespec.from_value = "b"
        self.assertEqual(self.get_keys(), [])

    def t08_find_values_descending(self):
        self.valuespec.to_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t09_find_values_descending(self):
        self.valuespec.below_value = "d"
        self.assertEqual(self.get_keys(), [])

    def t10_find_values_descending(self):
        self.assertEqual(self.get_keys(), [])


class Database_find_values_descending:
    def get_keys(self):
        return [
            i
            for i in self.database.find_values_descending(
                self.valuespec, "file1"
            )
        ]

    def t11_find_values_descending_01(self):
        ae = self.assertEqual
        keys = self.get_keys()
        ae(len(keys), 5)
        ae(keys, ["f", "e", "dk", "d", "c"])

    def t11_find_values_descending_02_above_below(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.below_value = "f"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["e", "dk", "d"])

    def t11_find_values_descending_03_above_to(self):
        ae = self.assertEqual
        self.valuespec.above_value = "c"
        self.valuespec.to_value = "e"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["e", "dk", "d"])

    def t11_find_values_descending_04_from_to(self):
        ae = self.assertEqual
        self.valuespec.from_value = "c"
        self.valuespec.to_value = "dr"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["dk", "d", "c"])

    def t11_find_values_descending_05_from_below(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        self.valuespec.below_value = "e"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(keys, ["dk", "d"])

    def t11_find_values_descending_06_above(self):
        ae = self.assertEqual
        self.valuespec.above_value = "d"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["f", "e", "dk"])

    def t11_find_values_descending_07_from(self):
        ae = self.assertEqual
        self.valuespec.from_value = "d"
        keys = self.get_keys()
        ae(len(keys), 4)
        ae(keys, ["f", "e", "dk", "d"])

    def t11_find_values_descending_08_to(self):
        ae = self.assertEqual
        self.valuespec.to_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 3)
        ae(keys, ["dk", "d", "c"])

    def t11_find_values_descending_09_below(self):
        ae = self.assertEqual
        self.valuespec.below_value = "dk"
        keys = self.get_keys()
        ae(len(keys), 2)
        ae(keys, ["d", "c"])


class Database_make_recordset:
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\x7f\xff\xff\xff\x00\x00\x00\x00",
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
        self.segments = {}
        keys = (
            "a_o",
            "aa_o",
            "ba_o",
            "bb_o",
            "c_o",
            "cep",
            "deq",
        )
        self.keyvalues = {}
        key_statement = " ".join(
            (
                "insert into file1_field1 (",
                "field1",
                ",",
                "Segment",
                ",",
                "RecordCount",
                ",",
                "file1",
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )
        cursor = self.database.dbenv.cursor()
        try:
            for s in segments:
                cursor.execute(
                    "".join(
                        (
                            "insert into file1__segment ",
                            "( RecordNumbers ) values ( ? )",
                        )
                    ),
                    (s,),
                )
                self.segments[
                    cursor.execute(
                        "select last_insert_rowid() from file1__segment"
                    ).fetchone()[0]
                ] = s
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                cursor.execute(
                    key_statement, (k, 0, 32 if e else 31, self.keyvalues[k])
                )
            self.keyvalues["tww"] = 8
            cursor.execute(key_statement, ("tww", 0, 2, self.keyvalues["tww"]))
            self.keyvalues["twy"] = 9
            cursor.execute(key_statement, ("twy", 0, 2, self.keyvalues["twy"]))
            cursor.execute(key_statement, ("one", 0, 1, 50))
            cursor.execute(key_statement, ("nin", 0, 1, 100))
            cursor.execute(key_statement, ("www", 0, 2, self.keyvalues["twy"]))
            cursor.execute(key_statement, ("www", 1, 2, self.keyvalues["twy"]))
        finally:
            cursor.close()

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
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"populate_segment\(\) missing 2 required ",
                    "positional arguments: 'segment_reference' and 'file'$",
                )
            ),
            self.database.populate_segment,
        )
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

    def t02_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 1, 0
        )

    def t03_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "nin", 0, 99
        )

    def t04_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "twy", 0, 99
        )

    def t05_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "aa_o", 0, 99
        )

    def t06_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 1, 0
        )

    def t07_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "nin", 0, 99
        )

    def t08_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "twy", 0, 68
        )

    def t09_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "bb_o", 0, 68
        )

    def t10_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "tww", 0, 65
        )

    def t11_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "one", 0, 50
        )

    def t12_populate_segment(self):
        s = self.database.populate_segment(("keyvalue", 2, 1, 3), "file1")
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def t13_populate_segment(self):
        ss = " ".join(
            (
                "select field1 , Segment , RecordCount , file1 from",
                "file1_field1 where field1 == 'one' and Segment == 0",
            )
        )
        s = self.database.populate_segment(
            self.database.dbenv.cursor().execute(ss).fetchone(), "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def t14_populate_segment(self):
        s = self.database.populate_segment(
            ("tww", 0, 2, self.keyvalues["tww"]), "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def t15_populate_segment(self):
        ss = " ".join(
            (
                "select field1 , Segment , RecordCount , file1 from",
                "file1_field1 where field1 == 'tww' and Segment == 0",
            )
        )
        s = self.database.populate_segment(
            self.database.dbenv.cursor().execute(ss).fetchone(), "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def t16_populate_segment(self):
        s = self.database.populate_segment(
            ("c_o", 0, 24, self.keyvalues["c_o"]), "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)

    def t17_populate_segment(self):
        ss = " ".join(
            (
                "select field1 , Segment , RecordCount , file1 from",
                "file1_field1 where field1 == 'c_o' and Segment == 0",
            )
        )
        s = self.database.populate_segment(
            self.database.dbenv.cursor().execute(ss).fetchone(), "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)

    def t18_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t19_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="z")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t20_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="n")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t21_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="w")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t22_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike="e")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 41)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t23_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t24_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def t25_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="tww")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)

    def t26_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 31)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t27_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t28_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="ppp"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t29_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="o"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def t30_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="tw"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t31_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart="d"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 24)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t32_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t33_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", le="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t34_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t35_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t36_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="c", le="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t37_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", ge="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t38_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", le="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t39_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t40_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="ppp", lt="qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def t41_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="n", le="q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t42_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="t", le="tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t43_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt="c", lt="cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t44_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt="c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t45_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", lt="cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t46_make_recordset_all(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def t47_unfile_records_under(self):
        self.database.unfile_records_under("file1", "field1", "aa_o")

    def t48_unfile_records_under(self):
        self.database.unfile_records_under("file1", "field1", "kkkk")

    def t49_file_records_under(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, "aa_o")

    def t50_file_records_under(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, "rrr")

    def t51_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key="twy")
        self.database.file_records_under("file1", "field1", rs, "aa_o")

    def t52_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key="twy")
        self.database.file_records_under("file1", "field1", rs, "rrr")

    def t53_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "aa_o")

    def t54_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key="one")
        self.database.file_records_under("file1", "field1", rs, "rrr")

    def t55_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key="ba_o")
        self.database.file_records_under("file1", "field1", rs, "www")

    def t56_database_cursor(self):
        d = self.database
        self.assertIsInstance(
            d.database_cursor("file1", "file1"), _sqlite.CursorPrimary
        )
        self.assertIsInstance(
            d.database_cursor("file1", "field1"), _sqlite.CursorSecondary
        )

    def t57_create_recordset_cursor(self):
        d = self.database
        rs = self.database.recordlist_key("file1", "field1", key=b"ba_o")
        self.assertIsInstance(
            d.create_recordset_cursor(rs), recordsetcursor.RecordsetCursor
        )


class Database_freed_record_number:
    def setup_detail(self):
        self.database.ebm_control["file1"] = _sqlite.ExistenceBitmapControl(
            "file1", self.database
        )
        self.statement = " ".join(
            (
                "insert into",
                "file1",
                "(",
                "file1",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        cursor = self.database.dbenv.cursor()
        for i in range(SegmentSize.db_segment_size * 3 - 1):
            cursor.execute(
                self.statement, (None, "_".join((str(i + 1), "value")))
            )
            self.database.add_record_to_ebm("file1", i + 1)
        cursor.close()
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
            self.database.delete("file1", i, "_".join((str(i), "value")))
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
            self.database.delete("file1", i, "_".join((str(i), "value")))
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
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 100)

    def t05_get_lowest_freed_record_number(self):
        for i in (380,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def t06_get_lowest_freed_record_number(self):
        for i in (110,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
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
            self.database.delete("file1", i, "_".join((str(i), "value")))
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
        cursor = self.database.dbenv.cursor()
        for i in range(i, i + 129):
            cursor.execute(
                self.statement, (None, "_".join((str(i + 1), "value")))
            )
            self.database.add_record_to_ebm("file1", i + 1)
        cursor.close()
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

    # Deletion of record number 0 is silently ignored.
    def t08_get_lowest_freed_record_number(self):
        for i in (0, 1):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 1)


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
                    b"\x7f\xff\xff\xff\x00\x00\x00\x00",
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
        keys = ("a_o",)
        key_statement = " ".join(
            (
                "insert into file1_field1 (",
                "field1",
                ",",
                "Segment",
                ",",
                "RecordCount",
                ",",
                "file1",
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )
        cursor = self.database.dbenv.cursor()
        try:
            for i in range(380):
                cursor.execute(
                    "insert into file1 ( Value ) values ( ? )",
                    (str(i + 1) + "Any value",),
                )
            statement = " ".join(
                (
                    "insert into",
                    "file1__ebm",
                    "(",
                    "file1__ebm",
                    ",",
                    "Value",
                    ")",
                    "values ( ? , ? )",
                )
            )
            bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
            cursor.execute(statement, (1, bits))
            bits = b"\xff" * SegmentSize.db_segment_size_bytes
            cursor.execute(statement, (2, bits))
            cursor.execute(statement, (3, bits))
            for s in segments:
                cursor.execute(
                    "".join(
                        (
                            "insert into file1__segment ",
                            "( RecordNumbers ) values ( ? )",
                        )
                    ),
                    (s,),
                )
            for e in range(len(segments)):
                cursor.execute(
                    key_statement, ("a_o", e, 32 if e else 31, e + 1)
                )
        finally:
            cursor.close()

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 2 required ",
                    "positional arguments: 'recordset' and 'engine'$",
                )
            ),
            _sqlite.RecordsetCursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_get_record\(\) missing 1 required ",
                    "positional argument: 'record_number'$",
                )
            ),
            _sqlite.RecordsetCursor(None, None)._get_record,
        )

    def t02___init__01(self):
        rc = _sqlite.RecordsetCursor(None, True)
        self.assertEqual(rc.engine, True)

    def t03___init__02(self):
        rs = self.database.recordlist_key("file1", "field1", key="a_o")
        rc = _sqlite.RecordsetCursor(rs, self.database.dbenv)
        self.assertIs(rc.engine, self.database.dbenv)
        self.assertIs(rc._dbset, rs)

    def t04__get_record(self):
        rc = _sqlite.RecordsetCursor(
            self.database.recordlist_key("file1", "field1", key="a_o"),
            self.database.dbenv,
        )
        self.assertEqual(rc._get_record(4000), None)
        self.assertEqual(rc._get_record(120), None)
        self.assertEqual(rc._get_record(10), (10, "10Any value"))
        self.assertEqual(rc._get_record(155), (155, "155Any value"))


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
            self.database.ebm_control["file1"].read_exists_segment(
                0, self.database.dbenv
            ),
            None,
        )

    def t03_read_exists_segment_02(self):
        self.assertEqual(self.database.ebm_control["file1"]._segment_count, 0)
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor.execute(statement, (1, bits))
        bits = b"\xff" * SegmentSize.db_segment_size_bytes
        cursor.execute(statement, (2, bits))
        cursor.execute(statement, (3, bits))
        self.database.ebm_control["file1"]._segment_count = 3
        seg = self.database.ebm_control["file1"].read_exists_segment(
            0, self.database.dbenv
        )
        self.assertEqual(seg.count(), 127)
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
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor.execute(statement, (1, bits))
        sr = self.database.ebm_control["file1"].get_ebm_segment(
            0, self.database.dbenv
        )
        self.assertEqual(sr, bits)

    def t06_delete_ebm_segment_01(self):
        self.database.ebm_control["file1"].delete_ebm_segment(
            0, self.database.dbenv
        )

    def t07_delete_ebm_segment_02(self):
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor.execute(statement, (1, bits))
        self.database.ebm_control["file1"].delete_ebm_segment(
            1, self.database.dbenv
        )

    def t08_put_ebm_segment(self):
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].put_ebm_segment(
            0, bits, self.database.dbenv
        )

    def t09_append_ebm_segment(self):
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].append_ebm_segment(
            bits, self.database.dbenv
        )


if sqlite3:

    class _SQLiteSqlite3(_SQLite):
        def open_database(self):
            self.database.open_database(sqlite3)

        def open_database_temp(self, temp, files=None):
            if files is None:
                temp.open_database(sqlite3)
            else:
                temp.open_database(sqlite3, files=files)

        def get_connection_class(self):
            return sqlite3.Connection

    class Database___init__Sqlite3(_SQLiteSqlite3):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsSqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceSqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset

    class Database_open_databaseSqlite3(_SQLiteSqlite3):
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

    class Database_add_field_to_existing_databaseSqlite3(_SQLiteSqlite3):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskSqlite3(Database_do_database_task):
        def setUp(self):
            self._ssb = SegmentSize.db_segment_size_bytes

            class _ED(_sqlite.Database):
                def open_database(self, **k):
                    super().open_database(sqlite3, **k)

            class _AD(_ED):
                def __init__(self, folder, **k):
                    super().__init__({}, folder, **k)

            self._AD = _AD

        test_01 = Database_do_database_task.t01_do_database_task

    class _SQLiteOpenSqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}),
                segment_size_bytes=None,
            )
            self.open_database()

        def tearDown(self):
            self.database.close_database()
            super().tearDown()

    class DatabaseTransactionsSqlite3(_SQLiteOpenSqlite3):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_delete

    class Database_methodsSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_09 = Database_methods.t09_get_segment_records
        test_10 = Database_methods.t10_get_segment_records
        test_11 = Database_methods.t11_set_segment_records
        test_12 = Database_methods.t12_delete_segment_records
        test_13 = Database_methods.t13_insert_segment_records
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
        test_26 = Database_methods.t26_get_table_connection
        create_ebm = Database_methods.create_ebm
        create_ebm_extra = Database_methods.create_ebm_extra

    class Database_find_values_emptySqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        get_keys = Database_find_values_empty.get_keys
        test_01 = Database_find_values_empty.t01_find_values
        test_02 = Database_find_values_empty.t02_find_values
        test_03 = Database_find_values_empty.t03_find_values
        test_04 = Database_find_values_empty.t04_find_values
        test_05 = Database_find_values_empty.t05_find_values
        test_06 = Database_find_values_empty.t06_find_values
        test_07 = Database_find_values_empty.t07_find_values
        test_08 = Database_find_values_empty.t08_find_values
        test_09 = Database_find_values_empty.t09_find_values
        test_10 = Database_find_values_empty.t10_find_values

    class Database_find_valuesSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        get_keys = Database_find_values.get_keys
        test_11_01 = Database_find_values.t11_find_values_01
        test_11_02 = Database_find_values.t11_find_values_02_above_below
        test_11_03 = Database_find_values.t11_find_values_03_above_to
        test_11_04 = Database_find_values.t11_find_values_04_from_to
        test_11_05 = Database_find_values.t11_find_values_05_from_below
        test_11_06 = Database_find_values.t11_find_values_06_above
        test_11_07 = Database_find_values.t11_find_values_07_from
        test_11_08 = Database_find_values.t11_find_values_08_to
        test_11_09 = Database_find_values.t11_find_values_09_below

    class Database_find_values_ascending_emptySqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvae = Database_find_values_ascending_empty
        get_keys = Dfvae.get_keys
        test_01 = Dfvae.t01_find_values_ascending
        test_02 = Dfvae.t02_find_values_ascending
        test_03 = Dfvae.t03_find_values_ascending
        test_04 = Dfvae.t04_find_values_ascending
        test_05 = Dfvae.t05_find_values_ascending
        test_06 = Dfvae.t06_find_values_ascending
        test_07 = Dfvae.t07_find_values_ascending
        test_08 = Dfvae.t08_find_values_ascending
        test_09 = Dfvae.t09_find_values_ascending
        test_10 = Dfvae.t10_find_values_ascending

    class Database_find_values_ascendingSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfva = Database_find_values_ascending
        get_keys = Dfva.get_keys
        test_11_01 = Dfva.t11_find_values_ascending_01
        test_11_02 = Dfva.t11_find_values_ascending_02_above_below
        test_11_03 = Dfva.t11_find_values_ascending_03_above_to
        test_11_04 = Dfva.t11_find_values_ascending_04_from_to
        test_11_05 = Dfva.t11_find_values_ascending_05_from_below
        test_11_06 = Dfva.t11_find_values_ascending_06_above
        test_11_07 = Dfva.t11_find_values_ascending_07_from
        test_11_08 = Dfva.t11_find_values_ascending_08_to
        test_11_09 = Dfva.t11_find_values_ascending_09_below

    class Database_find_values_descending_emptySqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvde = Database_find_values_descending_empty
        get_keys = Dfvde.get_keys
        test_01 = Dfvde.t01_find_values_descending
        test_02 = Dfvde.t02_find_values_descending
        test_03 = Dfvde.t03_find_values_descending
        test_04 = Dfvde.t04_find_values_descending
        test_05 = Dfvde.t05_find_values_descending
        test_06 = Dfvde.t06_find_values_descending
        test_07 = Dfvde.t07_find_values_descending
        test_08 = Dfvde.t08_find_values_descending
        test_09 = Dfvde.t09_find_values_descending
        test_10 = Dfvde.t10_find_values_descending

    class Database_find_values_descendingSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvd = Database_find_values_descending
        get_keys = Dfvd.get_keys
        test_11_01 = Dfvd.t11_find_values_descending_01
        test_11_02 = Dfvd.t11_find_values_descending_02_above_below
        test_11_03 = Dfvd.t11_find_values_descending_03_above_to
        test_11_04 = Dfvd.t11_find_values_descending_04_from_to
        test_11_05 = Dfvd.t11_find_values_descending_05_from_below
        test_11_06 = Dfvd.t11_find_values_descending_06_above
        test_11_07 = Dfvd.t11_find_values_descending_07_from
        test_11_08 = Dfvd.t11_find_values_descending_08_to
        test_11_09 = Dfvd.t11_find_values_descending_09_below

    class Database_make_recordsetSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            Database_make_recordset.setup_detail(self)

        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_add_record_to_field_value
        test_03 = Database_make_recordset.t03_add_record_to_field_value
        test_04 = Database_make_recordset.t04_add_record_to_field_value
        test_05 = Database_make_recordset.t05_add_record_to_field_value
        test_06 = Database_make_recordset.t06_remove_record_from_field_value
        test_07 = Database_make_recordset.t07_remove_record_from_field_value
        test_08 = Database_make_recordset.t08_remove_record_from_field_value
        test_09 = Database_make_recordset.t09_remove_record_from_field_value
        test_10 = Database_make_recordset.t10_remove_record_from_field_value
        test_11 = Database_make_recordset.t11_remove_record_from_field_value
        test_12 = Database_make_recordset.t12_populate_segment
        test_13 = Database_make_recordset.t13_populate_segment
        test_14 = Database_make_recordset.t14_populate_segment
        test_15 = Database_make_recordset.t15_populate_segment
        test_16 = Database_make_recordset.t16_populate_segment
        test_17 = Database_make_recordset.t17_populate_segment
        test_18 = Database_make_recordset.t18_make_recordset_key_like
        test_19 = Database_make_recordset.t19_make_recordset_key_like
        test_20 = Database_make_recordset.t20_make_recordset_key_like
        test_21 = Database_make_recordset.t21_make_recordset_key_like
        test_22 = Database_make_recordset.t22_make_recordset_key_like
        test_23 = Database_make_recordset.t23_make_recordset_key
        test_24 = Database_make_recordset.t24_make_recordset_key
        test_25 = Database_make_recordset.t25_make_recordset_key
        test_26 = Database_make_recordset.t26_make_recordset_key
        test_27 = Database_make_recordset.t27_make_recordset_key_startswith
        test_28 = Database_make_recordset.t28_make_recordset_key_startswith
        test_29 = Database_make_recordset.t29_make_recordset_key_startswith
        test_30 = Database_make_recordset.t30_make_recordset_key_startswith
        test_31 = Database_make_recordset.t31_make_recordset_key_startswith
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_key_range
        test_35 = Database_make_recordset.t35_make_recordset_key_range
        test_36 = Database_make_recordset.t36_make_recordset_key_range
        test_37 = Database_make_recordset.t37_make_recordset_key_range
        test_38 = Database_make_recordset.t38_make_recordset_key_range
        test_39 = Database_make_recordset.t39_make_recordset_key_range
        test_40 = Database_make_recordset.t40_make_recordset_key_range
        test_41 = Database_make_recordset.t41_make_recordset_key_range
        test_42 = Database_make_recordset.t42_make_recordset_key_range
        test_43 = Database_make_recordset.t43_make_recordset_key_range
        test_44 = Database_make_recordset.t44_make_recordset_key_range
        test_45 = Database_make_recordset.t45_make_recordset_key_range
        test_46 = Database_make_recordset.t46_make_recordset_all
        test_47 = Database_make_recordset.t47_unfile_records_under
        test_48 = Database_make_recordset.t48_unfile_records_under
        test_49 = Database_make_recordset.t49_file_records_under
        test_50 = Database_make_recordset.t50_file_records_under
        test_51 = Database_make_recordset.t51_file_records_under
        test_52 = Database_make_recordset.t52_file_records_under
        test_53 = Database_make_recordset.t53_file_records_under
        test_54 = Database_make_recordset.t54_file_records_under
        test_55 = Database_make_recordset.t55_file_records_under
        test_56 = Database_make_recordset.t56_database_cursor
        test_57 = Database_make_recordset.t57_create_recordset_cursor

    class Database_freed_record_numberSqlite3(_SQLiteOpenSqlite3):
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

    class Database_empty_freed_record_numberSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlSqlite3(_SQLiteOpenSqlite3):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment
        test_09 = ExistenceBitmapControl.t09_append_ebm_segment


if apsw:

    class _SQLiteApsw(_SQLite):
        def open_database(self):
            self.database.open_database(apsw)

        def open_database_temp(self, temp, files=None):
            if files is None:
                temp.open_database(apsw)
            else:
                temp.open_database(apsw, files=files)

        def get_connection_class(self):
            return apsw.Connection

    class Database___init__Apsw(_SQLiteApsw):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_transaction_methodsApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = Database_transaction_methods.t01_start_transaction
        test_02 = Database_transaction_methods.t02_backout
        test_03 = Database_transaction_methods.t03_commit
        test_04 = Database_transaction_methods.t04

    class DatabaseInstanceApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        test_01 = DatabaseInstance.t01_validate_segment_size_bytes
        test_02 = DatabaseInstance.t02_encode_record_number
        test_03 = DatabaseInstance.t03_decode_record_number
        test_04 = DatabaseInstance.t04_encode_record_selector
        test_05 = DatabaseInstance.t05_make_recordset

    class Database_open_databaseApsw(_SQLiteApsw):
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

    class Database_add_field_to_existing_databaseApsw(_SQLiteApsw):
        test_13 = (
            DatabaseAddFieldToExistingDatabase.t13_add_field_to_open_database
        )

    class Database_do_database_taskApsw(Database_do_database_task):
        def setUp(self):
            self._ssb = SegmentSize.db_segment_size_bytes

            class _ED(_sqlite.Database):
                def open_database(self, **k):
                    super().open_database(sqlite3, **k)

            class _AD(_ED):
                def __init__(self, folder, **k):
                    super().__init__({}, folder, **k)

            self._AD = _AD

        test_01 = Database_do_database_task.t01_do_database_task

    class _SQLiteOpenApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}),
                segment_size_bytes=None,
            )
            self.open_database()

        def tearDown(self):
            self.database.close_database()
            super().tearDown()

    class DatabaseTransactionsApsw(_SQLiteOpenApsw):
        test_01 = DatabaseTransactions.t01
        test_02 = DatabaseTransactions.t02
        test_03 = DatabaseTransactions.t03
        test_04 = DatabaseTransactions.t04
        test_05 = DatabaseTransactions.t05

    class Database_put_replace_deleteApsw(_SQLiteOpenApsw):
        test_01 = Database_put_replace_delete.t01
        test_02 = Database_put_replace_delete.t02_put
        test_03 = Database_put_replace_delete.t03_put
        test_04 = Database_put_replace_delete.t04_put
        test_05 = Database_put_replace_delete.t05_replace
        test_06 = Database_put_replace_delete.t06_delete

    class Database_methodsApsw(_SQLiteOpenApsw):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_get_primary_record
        test_03 = Database_methods.t03_get_primary_record
        test_04 = Database_methods.t04_get_primary_record
        test_05 = Database_methods.t05_remove_record_from_ebm
        test_06 = Database_methods.t06_remove_record_from_ebm
        test_07 = Database_methods.t07_add_record_to_ebm
        test_08 = Database_methods.t08_get_high_record
        test_09 = Database_methods.t09_get_segment_records
        test_10 = Database_methods.t10_get_segment_records
        test_11 = Database_methods.t11_set_segment_records
        test_12 = Database_methods.t12_delete_segment_records
        test_13 = Database_methods.t13_insert_segment_records
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
        test_26 = Database_methods.t26_get_table_connection
        create_ebm = Database_methods.create_ebm
        create_ebm_extra = Database_methods.create_ebm_extra

    class Database_find_values_emptyApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        get_keys = Database_find_values_empty.get_keys
        test_01 = Database_find_values_empty.t01_find_values
        test_02 = Database_find_values_empty.t02_find_values
        test_03 = Database_find_values_empty.t03_find_values
        test_04 = Database_find_values_empty.t04_find_values
        test_05 = Database_find_values_empty.t05_find_values
        test_06 = Database_find_values_empty.t06_find_values
        test_07 = Database_find_values_empty.t07_find_values
        test_08 = Database_find_values_empty.t08_find_values
        test_09 = Database_find_values_empty.t09_find_values
        test_10 = Database_find_values_empty.t10_find_values

    class Database_find_valuesApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        get_keys = Database_find_values.get_keys
        test_11_01 = Database_find_values.t11_find_values_01
        test_11_02 = Database_find_values.t11_find_values_02_above_below
        test_11_03 = Database_find_values.t11_find_values_03_above_to
        test_11_04 = Database_find_values.t11_find_values_04_from_to
        test_11_05 = Database_find_values.t11_find_values_05_from_below
        test_11_06 = Database_find_values.t11_find_values_06_above
        test_11_07 = Database_find_values.t11_find_values_07_from
        test_11_08 = Database_find_values.t11_find_values_08_to
        test_11_09 = Database_find_values.t11_find_values_09_below

    class Database_find_values_ascending_emptyApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvae = Database_find_values_ascending_empty
        get_keys = Dfvae.get_keys
        test_01 = Dfvae.t01_find_values_ascending
        test_02 = Dfvae.t02_find_values_ascending
        test_03 = Dfvae.t03_find_values_ascending
        test_04 = Dfvae.t04_find_values_ascending
        test_05 = Dfvae.t05_find_values_ascending
        test_06 = Dfvae.t06_find_values_ascending
        test_07 = Dfvae.t07_find_values_ascending
        test_08 = Dfvae.t08_find_values_ascending
        test_09 = Dfvae.t09_find_values_ascending
        test_10 = Dfvae.t10_find_values_ascending

    class Database_find_values_ascendingApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfva = Database_find_values_ascending
        get_keys = Dfva.get_keys
        test_11_01 = Dfva.t11_find_values_ascending_01
        test_11_02 = Dfva.t11_find_values_ascending_02_above_below
        test_11_03 = Dfva.t11_find_values_ascending_03_above_to
        test_11_04 = Dfva.t11_find_values_ascending_04_from_to
        test_11_05 = Dfva.t11_find_values_ascending_05_from_below
        test_11_06 = Dfva.t11_find_values_ascending_06_above
        test_11_07 = Dfva.t11_find_values_ascending_07_from
        test_11_08 = Dfva.t11_find_values_ascending_08_to
        test_11_09 = Dfva.t11_find_values_ascending_09_below

    class Database_find_values_descending_emptyApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvde = Database_find_values_descending_empty
        get_keys = Dfvde.get_keys
        test_01 = Dfvde.t01_find_values_descending
        test_02 = Dfvde.t02_find_values_descending
        test_03 = Dfvde.t03_find_values_descending
        test_04 = Dfvde.t04_find_values_descending
        test_05 = Dfvde.t05_find_values_descending
        test_06 = Dfvde.t06_find_values_descending
        test_07 = Dfvde.t07_find_values_descending
        test_08 = Dfvde.t08_find_values_descending
        test_09 = Dfvde.t09_find_values_descending
        test_10 = Dfvde.t10_find_values_descending

    class Database_find_values_descendingApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            cursor = self.database.dbenv.cursor()
            statement = " ".join(
                (
                    "insert into",
                    "file1_field1",
                    "(",
                    "field1",
                    ")",
                    "values ( ? )",
                )
            )
            for key in ("d", "e", "c", "dk", "f"):
                values = (key,)
                cursor.execute(statement, values)
            self.valuespec = ValuesClause()
            self.valuespec.field = "field1"

        Dfvd = Database_find_values_descending
        get_keys = Dfvd.get_keys
        test_11_01 = Dfvd.t11_find_values_descending_01
        test_11_02 = Dfvd.t11_find_values_descending_02_above_below
        test_11_03 = Dfvd.t11_find_values_descending_03_above_to
        test_11_04 = Dfvd.t11_find_values_descending_04_from_to
        test_11_05 = Dfvd.t11_find_values_descending_05_from_below
        test_11_06 = Dfvd.t11_find_values_descending_06_above
        test_11_07 = Dfvd.t11_find_values_descending_07_from
        test_11_08 = Dfvd.t11_find_values_descending_08_to
        test_11_09 = Dfvd.t11_find_values_descending_09_below

    class Database_make_recordsetApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            Database_make_recordset.setup_detail(self)

        test_01 = Database_make_recordset.t01
        test_02 = Database_make_recordset.t02_add_record_to_field_value
        test_03 = Database_make_recordset.t03_add_record_to_field_value
        test_04 = Database_make_recordset.t04_add_record_to_field_value
        test_05 = Database_make_recordset.t05_add_record_to_field_value
        test_06 = Database_make_recordset.t06_remove_record_from_field_value
        test_07 = Database_make_recordset.t07_remove_record_from_field_value
        test_08 = Database_make_recordset.t08_remove_record_from_field_value
        test_09 = Database_make_recordset.t09_remove_record_from_field_value
        test_10 = Database_make_recordset.t10_remove_record_from_field_value
        test_11 = Database_make_recordset.t11_remove_record_from_field_value
        test_12 = Database_make_recordset.t12_populate_segment
        test_13 = Database_make_recordset.t13_populate_segment
        test_14 = Database_make_recordset.t14_populate_segment
        test_15 = Database_make_recordset.t15_populate_segment
        test_16 = Database_make_recordset.t16_populate_segment
        test_17 = Database_make_recordset.t17_populate_segment
        test_18 = Database_make_recordset.t18_make_recordset_key_like
        test_19 = Database_make_recordset.t19_make_recordset_key_like
        test_20 = Database_make_recordset.t20_make_recordset_key_like
        test_21 = Database_make_recordset.t21_make_recordset_key_like
        test_22 = Database_make_recordset.t22_make_recordset_key_like
        test_23 = Database_make_recordset.t23_make_recordset_key
        test_24 = Database_make_recordset.t24_make_recordset_key
        test_25 = Database_make_recordset.t25_make_recordset_key
        test_26 = Database_make_recordset.t26_make_recordset_key
        test_27 = Database_make_recordset.t27_make_recordset_key_startswith
        test_28 = Database_make_recordset.t28_make_recordset_key_startswith
        test_29 = Database_make_recordset.t29_make_recordset_key_startswith
        test_30 = Database_make_recordset.t30_make_recordset_key_startswith
        test_31 = Database_make_recordset.t31_make_recordset_key_startswith
        test_32 = Database_make_recordset.t32_make_recordset_key_range
        test_33 = Database_make_recordset.t33_make_recordset_key_range
        test_34 = Database_make_recordset.t34_make_recordset_key_range
        test_35 = Database_make_recordset.t35_make_recordset_key_range
        test_36 = Database_make_recordset.t36_make_recordset_key_range
        test_37 = Database_make_recordset.t37_make_recordset_key_range
        test_38 = Database_make_recordset.t38_make_recordset_key_range
        test_39 = Database_make_recordset.t39_make_recordset_key_range
        test_40 = Database_make_recordset.t40_make_recordset_key_range
        test_41 = Database_make_recordset.t41_make_recordset_key_range
        test_42 = Database_make_recordset.t42_make_recordset_key_range
        test_43 = Database_make_recordset.t43_make_recordset_key_range
        test_44 = Database_make_recordset.t44_make_recordset_key_range
        test_45 = Database_make_recordset.t45_make_recordset_key_range
        test_46 = Database_make_recordset.t46_make_recordset_all
        test_47 = Database_make_recordset.t47_unfile_records_under
        test_48 = Database_make_recordset.t48_unfile_records_under
        test_49 = Database_make_recordset.t49_file_records_under
        test_50 = Database_make_recordset.t50_file_records_under
        test_51 = Database_make_recordset.t51_file_records_under
        test_52 = Database_make_recordset.t52_file_records_under
        test_53 = Database_make_recordset.t53_file_records_under
        test_54 = Database_make_recordset.t54_file_records_under
        test_55 = Database_make_recordset.t55_file_records_under
        test_56 = Database_make_recordset.t56_database_cursor
        test_57 = Database_make_recordset.t57_create_recordset_cursor

    class Database_freed_record_numberApsw(_SQLiteOpenApsw):
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

    class Database_empty_freed_record_numberApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            self.high_record = self.database.get_high_record_number("file1")

        test_01 = Database_empty_freed_record_number.t01

    class RecordsetCursorApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            RecordsetCursor.setup_detail(self)

        test_01 = RecordsetCursor.t01
        test_02 = RecordsetCursor.t02___init__01
        test_03 = RecordsetCursor.t03___init__02
        test_04 = RecordsetCursor.t04__get_record

    class ExistenceBitmapControlApsw(_SQLiteOpenApsw):
        test_01 = ExistenceBitmapControl.t01
        test_02 = ExistenceBitmapControl.t02_read_exists_segment_01
        test_03 = ExistenceBitmapControl.t03_read_exists_segment_02
        test_04 = ExistenceBitmapControl.t04_get_ebm_segment_01
        test_05 = ExistenceBitmapControl.t05_get_ebm_segment_02
        test_06 = ExistenceBitmapControl.t06_delete_ebm_segment_01
        test_07 = ExistenceBitmapControl.t07_delete_ebm_segment_02
        test_08 = ExistenceBitmapControl.t08_put_ebm_segment
        test_09 = ExistenceBitmapControl.t09_append_ebm_segment


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if sqlite3:
        runner().run(loader(Database___init__Sqlite3))
        runner().run(loader(Database_transaction_methodsSqlite3))
        runner().run(loader(DatabaseInstanceSqlite3))
        runner().run(loader(Database_open_databaseSqlite3))
        runner().run(loader(Database_add_field_to_existing_databaseSqlite3))
        runner().run(loader(Database_do_database_taskSqlite3))
        runner().run(loader(DatabaseTransactionsSqlite3))
        runner().run(loader(Database_put_replace_deleteSqlite3))
        runner().run(loader(Database_methodsSqlite3))
        runner().run(loader(Database_find_valuesSqlite3))
        runner().run(loader(Database_find_values_ascendingSqlite3))
        runner().run(loader(Database_find_values_descendingSqlite3))
        runner().run(loader(Database_make_recordsetSqlite3))
        runner().run(loader(Database_freed_record_numberSqlite3))
        runner().run(loader(Database_empty_freed_record_numberSqlite3))
        runner().run(loader(RecordsetCursorSqlite3))
        runner().run(loader(ExistenceBitmapControlSqlite3))
    if apsw:
        runner().run(loader(Database___init__Apsw))
        runner().run(loader(Database_transaction_methodsApsw))
        runner().run(loader(DatabaseInstanceApsw))
        runner().run(loader(Database_open_databaseApsw))
        runner().run(loader(Database_add_field_to_existing_databaseApsw))
        runner().run(loader(Database_do_database_taskApsw))
        runner().run(loader(DatabaseTransactionsApsw))
        runner().run(loader(Database_put_replace_deleteApsw))
        runner().run(loader(Database_methodsApsw))
        runner().run(loader(Database_find_valuesApsw))
        runner().run(loader(Database_find_values_ascendingApsw))
        runner().run(loader(Database_find_values_descendingApsw))
        runner().run(loader(Database_make_recordsetApsw))
        runner().run(loader(Database_freed_record_numberApsw))
        runner().run(loader(Database_empty_freed_record_numberApsw))
        runner().run(loader(RecordsetCursorApsw))
        runner().run(loader(ExistenceBitmapControlApsw))
