# test__db.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_db _database tests"""

import unittest
import os

try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
try:
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None

from .. import _db
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..wherevalues import ValuesClause
from ..bytebit import Bitarray


class _DB(unittest.TestCase):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for bsddb3.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        class _D(_db.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        logdir = "___memlogs_memory_db"
        if os.path.exists(logdir):
            for f in os.listdir(logdir):
                if f.startswith("log."):
                    os.remove(os.path.join(logdir, f))
            os.rmdir(logdir)


class Database___init__(_DB):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 2 to 7 positional arguments ",
                    "but 8 were given",
                )
            ),
            self._D,
            *(None, None, None, None, None, None, None),
        )

    def test_02(self):
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
            _db.DatabaseError,
            "".join(("Database folder name {} is not valid",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def test_04(self):
        self.assertRaisesRegex(
            _db.DatabaseError,
            "".join(("Database environment must be a dictionary",)),
            self._D,
            *({},),
            **dict(environment=[]),
        )

    def test_05(self):
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
        self.assertEqual(database.dbtxn, None)
        self.assertEqual(database._dbe, None)
        self.assertEqual(database.segment_table, {})
        self.assertEqual(database.ebm_control, {})
        self.assertEqual(database.ebm_segment_count, {})
        self.assertEqual(database._real_segment_size_bytes, False)
        self.assertEqual(database._initial_segment_size_bytes, 4000)
        self.assertEqual(database._file_per_database, False)
        self.assertEqual(database._initial_file_per_database, False)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)

    def test_06(self):
        database = self._D({})
        self.assertEqual(database.home_directory, None)
        self.assertEqual(database.database_file, None)

    # This combination of folder and segment_size_bytes arguments is used for
    # unittests, except for one to see a non-memory database with a realistic
    # segment size.
    def test_07(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)


# Transaction methods, except start_transaction, do not raise exceptions if
# called when no database open but do nothing.
class Database_transaction_methods(_DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_01_start_transaction(self):
        self.assertEqual(self.database.dbenv, None)
        self.assertRaisesRegex(
            AttributeError,
            "".join(("'NoneType' object has no attribute 'txn_begin'",)),
            self.database.start_transaction,
        )

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
class DatabaseInstance(_DB):
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
            _db.DatabaseError,
            "".join(("Database segment size must be an int",)),
            self.database._validate_segment_size_bytes,
            *("a",),
        )
        self.assertRaisesRegex(
            _db.DatabaseError,
            "".join(("Database segment size must be more than 0",)),
            self.database._validate_segment_size_bytes,
            *(0,),
        )
        self.assertEqual(
            self.database._validate_segment_size_bytes(None), None
        )
        self.assertEqual(self.database._validate_segment_size_bytes(1), None)

    def test_02_environment_flags(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "environment_flags\(\) missing 1 required ",
                    "positional argument: 'dbe'",
                )
            ),
            self.database.environment_flags,
        )
        dbe = dbe_module.db
        self.assertEqual(
            self.database.environment_flags(dbe),
            (
                dbe.DB_CREATE
                | dbe.DB_RECOVER
                | dbe.DB_INIT_MPOOL
                | dbe.DB_INIT_LOCK
                | dbe.DB_INIT_LOG
                | dbe.DB_INIT_TXN
                | dbe.DB_PRIVATE
            ),
        )

    def test_03_encode_record_number(self):
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
        self.assertEqual(self.database.encode_record_number(1), b"1")

    def test_04_decode_record_number(self):
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
        self.assertEqual(self.database.decode_record_number(b"1"), 1)

    def test_05_encode_record_selector(self):
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
        self.assertEqual(self.database.encode_record_selector("a"), b"a")

    def test_06_make_recordset(self):
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
class Database_open_database(_DB):
    def test_01(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "open_database\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.database.open_database,
            *(None, None, None),
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
        self.database.open_database(dbe_module.db)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertEqual(self.database.dbenv.__class__.__name__, "DBEnv")

    def test_03(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(dbe_module.db)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(self.database.home_directory, None)
        self.assertEqual(self.database.database_file, None)
        self.assertEqual(self.database.dbenv.__class__.__name__, "DBEnv")

    def test_04_close_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(dbe_module.db)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)

    def test_05_close_database_contexts(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(dbe_module.db)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database_contexts()
        self.assertEqual(self.database.dbenv, None)

    def test_06(self):
        self.database = self._D({"file1": {"field1"}})
        self.database.open_database(dbe_module.db)
        self.check_specification()

    def test_07(self):
        self.database = self._D(filespec.FileSpec(**{"file1": {"field1"}}))
        self.database.open_database(dbe_module.db)
        self.check_specification()

    def test_08(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}})
        )
        self.database.open_database(dbe_module.db, files={"file1"})
        self.check_specification()

    def test_09(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        d.open_database(dbe_module.db)
        self.assertEqual(
            set(d.table), {"file1", "___control", "file1_field1", "file2"}
        )
        self.assertEqual(set(d.segment_table), {"file1", "file2"})
        self.assertEqual(set(d.ebm_control), {"file1", "file2"})
        self.assertEqual(set(d.ebm_segment_count), set())
        for v in d.ebm_control.values():
            self.assertIsInstance(v, _db.ExistenceBitmapControl)
        c = 0
        o = set()
        for t in (
            d.table,
            d.segment_table,
            d.ebm_segment_count,
        ):
            for v in t.values():
                if isinstance(v, list):
                    for i in v:
                        self.assertEqual(i.__class__.__name__, "DB")
                        c += 1
                        o.add(i)
                else:
                    self.assertEqual(v.__class__.__name__, "DB")
                    c += 1
                    o.add(v)
        for t in (d.ebm_control,):
            for v in t.values():
                self.assertEqual(v.ebm_table.__class__.__name__, "DB")
                c += 1
                o.add(v)
        self.assertEqual(c, len(o))

    def test_10_file_name_for_database(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        self.assertEqual(d._file_per_database, False)
        self.assertEqual(d.database_file, None)
        self.assertEqual(d.file_name_for_database("file1"), None)
        d._file_per_database = True
        self.assertEqual(d.home_directory, None)
        self.assertEqual(d.file_name_for_database("file1"), "file1")
        d.home_directory = "home"
        self.assertEqual(
            d.file_name_for_database("file1"), os.path.join("home", "file1")
        )

    def test_11_checkpoint_before_close_dbenv(self):
        self.database = self._D(filespec.FileSpec())
        d = self.database
        d.open_database(dbe_module.db)
        self.assertEqual(d.checkpoint_before_close_dbenv(), None)

    # Comment in _db.py suggests this method is not needed.
    def test_12_is_database_file_active(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        self.assertRaisesRegex(
            KeyError,
            "'file1'",
            d.is_database_file_active,
            *("file1",),
        )
        d.open_database(dbe_module.db)
        self.assertEqual(d.is_database_file_active("file1"), True)
        x = d.table["file1"][0]
        d.table["file1"][0] = None
        self.assertEqual(d.is_database_file_active("file1"), False)
        d.table["file1"][0] = x

    def check_specification(self):
        d = self.database
        self.assertEqual(set(d.table), {"file1", "___control", "file1_field1"})
        self.assertEqual(set(d.segment_table), {"file1"})
        self.assertEqual(set(d.ebm_control), {"file1"})
        self.assertEqual(set(d.ebm_segment_count), set())
        for v in d.ebm_control.values():
            self.assertIsInstance(v, _db.ExistenceBitmapControl)
        c = 0
        o = set()
        for t in (
            d.table,
            d.segment_table,
            d.ebm_segment_count,
        ):
            for v in t.values():
                if isinstance(v, list):
                    for i in v:
                        self.assertEqual(i.__class__.__name__, "DB")
                        c += 1
                        o.add(i)
                else:
                    self.assertEqual(v.__class__.__name__, "DB")
                    c += 1
                    o.add(v)
        for t in (d.ebm_control,):
            for v in t.values():
                self.assertEqual(v.ebm_table.__class__.__name__, "DB")
                c += 1
                o.add(v)
        self.assertEqual(c, len(o))


# Memory databases are used for these tests.
# This one has to look like a real application (almost).
# Do not need to catch the self.__class__.SegmentSizeError exception in
# _ED.open_database() method.
class Database_do_database_task(unittest.TestCase):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for bsddb3.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        class _ED(_db.Database):
            def open_database(self, **k):
                super().open_database(dbe_module.db, **k)

        class _AD(_ED):
            def __init__(self, folder, **k):
                super().__init__({}, folder, **k)

        self._AD = _AD

    def tearDown(self):
        self.database = None
        self._AD = None
        logdir = "___memlogs_memory_db"
        if os.path.exists(logdir):
            for f in os.listdir(logdir):
                if f.startswith("log."):
                    os.remove(os.path.join(logdir, f))
            os.rmdir(logdir)

    def test_01_do_database_task(self):
        def m(*a, **k):
            pass

        self.database = self._AD(None)
        d = self.database
        d.open_database()
        self.assertEqual(d.do_database_task(m), None)


# Memory databases are used for these tests.
# Use the 'testing only' segment size for convenience of setup and eyeballing.
class _DBOpen(_DB):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(dbe_module.db)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class DatabaseTransactions(_DBOpen):

    # Second start_transaction does nothing.
    def test_01(self):
        self.database.start_transaction()
        self.database.start_transaction()

    def test_02(self):
        self.database.start_transaction()
        self.database.backout()

    def test_03(self):
        self.database.start_transaction()
        self.database.commit()

    # Bare backout does nothing.
    def test_04(self):
        self.database.backout()

    # Bare commit does nothing.
    def test_05(self):
        self.database.commit()


class Database_put_replace_delete(_DBOpen):
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
        self.assertEqual(recno, 1)

    def test_03_put(self):
        self.assertEqual(self.database.put("file1", 2, "new value"), None)
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 3)

    def test_04_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 1)
        self.assertEqual(self.database.put("file1", 1, "renew value"), None)
        recno = self.database.put("file1", None, "other value")
        self.assertEqual(recno, 2)

    def test_05_replace(self):
        self.assertEqual(
            self.database.replace("file1", 1, "new value", "renew value"), None
        )

    def test_06_delete(self):
        self.assertEqual(self.database.delete("file1", 1, "new value"), None)


class Database_methods(_DBOpen):
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
        self.database.put("file1", None, "new value")
        self.assertEqual(
            self.database.get_primary_record("file1", 1), (1, "new value")
        )

    def test_05_remove_record_from_ebm(self):
        self.assertRaisesRegex(
            _db.DatabaseError,
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
            self.database.recordlist_record_number("file1", key=2),
            recordset.RecordList,
        )

    def test_16_recordset_record_number(self):
        self.database.table["file1"][0].put(1, encode("Some value"))
        values = b"\x40" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].ebm_table.put(1, values)
        rl = self.database.recordlist_record_number("file1", key=1)
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
            b"\x7f\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
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
            b"\x7f\xff\xff\xff\xe0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
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
        self.create_ebm_extra()
        self.create_ebm_extra()
        self.create_ebm_extra()
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
        self.create_ebm_extra()
        self.create_ebm_extra()
        self.create_ebm_extra()
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
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def test_26_get_table_connection(self):
        self.assertEqual(
            self.database.get_table_connection("file1").__class__.__name__,
            "DB",
        )

    def create_ebm(self):
        values = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].ebm_table.put(1, values)

    def create_ebm_extra(self):
        values = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].ebm_table.append(values)


class Database_find_values(_DBOpen):
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

    def test_11_find_values(self):
        self.database.table["file1_field1"][0].put(b"d", encode("values"))
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d"],
        )


class Database_make_recordset(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
        segments = (
            b"\x7f\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff",
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
        self.references = {}
        for s in segments:
            self.segments[self.database.segment_table["file1"].append(s)] = s
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                self.references[k] = b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(32 if e else 31).to_bytes(2, byteorder="big"),
                        self.keyvalues[k].to_bytes(4, byteorder="big"),
                    )
                )
                cursor.put(
                    k.encode(), self.references[k], dbe_module.db.DB_KEYLAST
                )
            self.keyvalues["tww"] = 8
            self.references["tww"] = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(2).to_bytes(2, byteorder="big"),
                    self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                )
            )
            cursor.put(
                "tww".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            self.keyvalues["twy"] = 9
            cursor.put(
                "twy".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "one".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(50).to_bytes(2, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "nin".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(100).to_bytes(2, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )

            # This pair of puts wrote their records to different files before
            # solentware-base-4.0, one for lists and one for bitmaps.
            # At solentware-base-4.0 the original test_55_file_records_under
            # raises a bsddb3.db.DBKeyEmptyError exception when attempting to
            # delete the second record referred to by self.keyvalues['twy'].
            # The test is changed to expect the exception.
            cursor.put(
                "www".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "www".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x01",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )

        finally:
            cursor.close()

    def tearDown(self):
        self.database.commit()
        super().tearDown()

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
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "populate_segment\(\) missing 2 required ",
                    "positional arguments: 'segment_reference' and 'file'",
                )
            ),
            self.database.populate_segment,
        )
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

    def test_02_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "indexvalue", 1, 0
        )

    def test_03_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "nin", 0, 99
        )

    def test_04_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "twy", 0, 99
        )

    def test_05_add_record_to_field_value(self):
        self.database.add_record_to_field_value(
            "file1", "field1", "aa_o", 0, 99
        )

    def test_06_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "indexvalue", 1, 0
        )

    def test_07_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "nin", 0, 99
        )

    def test_08_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "twy", 0, 68
        )

    def test_09_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "bb_o", 0, 68
        )

    def test_10_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "tww", 0, 65
        )

    def test_11_remove_record_from_field_value(self):
        self.database.remove_record_from_field_value(
            "file1", "field1", "one", 0, 50
        )

    def test_12_populate_segment(self):
        s = self.database.populate_segment(
            b"\x00\x00\x00\x02\x00\x03", "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def test_13_populate_segment(self):
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            while True:
                k, v = cursor.next()
                if k.decode() == "one":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
                        break
        finally:
            cursor.close()

    def test_14_populate_segment(self):
        s = self.database.populate_segment(
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x02",
                    self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                )
            ),
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def test_15_populate_segment(self):
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            while True:
                k, v = cursor.next()
                if k.decode() == "tww":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(
                            s, recordset.RecordsetSegmentList
                        )
                        self.assertEqual(s.count_records(), 2)
                        break
        finally:
            cursor.close()

    def test_16_populate_segment(self):
        s = self.database.populate_segment(
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x18",
                    self.keyvalues["c_o"].to_bytes(4, byteorder="big"),
                )
            ),
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)

    def test_17_populate_segment(self):
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            while True:
                k, v = cursor.next()
                if k.decode() == "c_o":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(
                            s, recordset.RecordsetSegmentBitarray
                        )
                        self.assertEqual(s.count_records(), 24)
                        break
        finally:
            cursor.close()

    def test_18_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_19_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"z")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_20_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"n")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_21_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"w")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_22_make_recordset_key_like(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"e")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 41)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_23_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_24_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_25_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"tww")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)

    def test_26_make_recordset_key(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"a_o")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 31)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_27_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_28_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"ppp"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_29_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"o"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_30_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"tw"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_31_make_recordset_key_startswith(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"d"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 24)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_33_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"ppp", le=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_34_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"n", le=b"q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_35_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"t", le=b"tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_36_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"c", le=b"cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_37_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", ge=b"c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_38_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", le=b"cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_39_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"ppp", lt=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_40_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"ppp", lt=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_41_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"n", le=b"q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_42_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"t", le=b"tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_43_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"c", lt=b"cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_44_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt=b"c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_45_make_recordset_key_range(self):
        rs = self.database.recordlist_key_range("file1", "field1", lt=b"cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_46_make_recordset_all(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_47_unfile_records_under(self):
        self.database.unfile_records_under("file1", "field1", b"aa_o")

    def test_48_unfile_records_under(self):
        self.database.unfile_records_under("file1", "field1", b"kkkk")

    def test_49_file_records_under(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_50_file_records_under(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    def test_51_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"twy")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_52_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"twy")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    def test_53_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_54_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    # Changed at solentware-base-4.0, see comments in setUp() for put records.
    # Did I really miss the change in error message? Or change something which
    # causes a different error?  Spotted while working on _nosql.py.
    # There has been a FreeBSD OS and ports upgrade since solentware-base-4.0.
    # Changed back after rebuild at end of March 2020.
    # When doing some testing on OpenBSD in September 2020 see that the -30997
    # exception is raised.
    def test_55_file_records_under(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"ba_o")
        # self.database.file_records_under('file1', 'field1', rs, b'www')
        self.assertRaisesRegex(
            bdb.DBKeyEmptyError,
            r"".join(
                (
                    r"(?:\(-30995, 'BDB0066 |\(-30997, ')",
                    r"DB_KEYEMPTY: Non-existent key/data pair'\)",
                )
            ),
            # "\(-30995, 'BDB0066 DB_KEYEMPTY: Non-existent key/data pair'\)",
            # "\(-30997, 'DB_KEYEMPTY: Non-existent key/data pair'\)",
            self.database.file_records_under,
            *("file1", "field1", rs, b"www"),
        )

    # Did I really miss the change in error message? Or change something which
    # causes a different error?  Spotted while working on _nosql.py.
    # There has been a FreeBSD OS and ports upgrade since solentware-base-4.0.
    # Changed back after rebuild at end of March 2020.
    # When doing some testing on OpenBSD in September 2020 see that BDB1002
    # is omitted from the exception text.
    def test_56__get_segment_record_numbers(self):
        self.assertIsInstance(
            self.database._get_segment_record_numbers("file1", 7), Bitarray
        )
        self.assertIsInstance(
            self.database._get_segment_record_numbers("file1", 8), list
        )
        self.assertRaisesRegex(
            bdb.DBInvalidArgError,
            r"".join(
                (
                    r"\(22, 'Invalid argument -- (?:BDB1002 )?",
                    r"illegal record number of 0'\)",
                )
            ),
            # "\(22, 'Invalid argument -- BDB1002 illegal record number of 0'\)",
            # "\(22, 'Invalid argument -- illegal record number of 0'\)",
            self.database._get_segment_record_numbers,
            *("file1", 0),
        )
        self.assertRaisesRegex(
            TypeError,
            "object of type 'NoneType' has no len\(\)",
            self.database._get_segment_record_numbers,
            *("file1", 10),
        )

    def test_57__populate_recordset_segment(self):
        d = self.database
        bs = self.references["c_o"]
        self.assertEqual(len(bs), 10)
        bl = self.references["tww"]
        self.assertEqual(len(bl), 10)
        rs = recordset.RecordList(d, "file1")
        self.assertEqual(len(rs), 0)
        self.assertEqual(d.populate_recordset_segment(rs, bl), None)
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)
        self.assertEqual(d.populate_recordset_segment(rs, bs), None)
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertEqual(
            d.populate_recordset_segment(rs, b"\x00\x00\x00\x00\x00\x01"), None
        )
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertEqual(
            d.populate_recordset_segment(rs, b"\x00\x00\x00\x01\x00\x05"), None
        )
        self.assertEqual(len(rs), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertIsInstance(rs[1], recordset.RecordsetSegmentInt)

    def test_58_database_cursor(self):
        d = self.database
        self.assertIsInstance(
            d.database_cursor("file1", "file1"), _db.CursorPrimary
        )
        self.assertIsInstance(
            d.database_cursor("file1", "field1"), _db.CursorSecondary
        )

    def test_59_create_recordset_cursor(self):
        d = self.database
        rs = self.database.recordlist_key("file1", "field1", key=b"ba_o")
        self.assertIsInstance(
            d.create_recordset_cursor(rs), recordset.RecordsetCursor
        )


class Database_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
        self.database.ebm_control["file1"] = _db.ExistenceBitmapControl(
            "file1", self.database, dbe_module.db, dbe_module.db.DB_CREATE
        )
        for i in range(SegmentSize.db_segment_size * 3 - 1):
            self.database.add_record_to_ebm(
                "file1",
                self.database.table["file1"][0].append(
                    encode("value"), txn=self.database.dbtxn
                ),
            )
        self.high_record = self.database.get_high_record("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record[0], SegmentSize.db_segment_size
        )[0]

    def tearDown(self):
        self.database.commit()
        super().tearDown()

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
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 100)

    def test_05_get_lowest_freed_record_number(self):
        for i in (380,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def test_06_get_lowest_freed_record_number(self):
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
    def test_07_get_lowest_freed_record_number(self):
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
        i = self.high_record[0]
        for i in range(i, i + 129):
            self.database.add_record_to_ebm(
                "file1",
                self.database.table["file1"][0].append(
                    encode("value"), txn=self.database.dbtxn
                ),
            )
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
        # self.database.start_transaction()
        rn = self.database.get_lowest_freed_record_number("file1")
        # self.database.commit()
        self.assertEqual(rn, None)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            0,
        )


# Does this test add anything beyond Database_freed_record_number?
class Database_empty_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.ebm_control["file1"] = _db.ExistenceBitmapControl(
            "file1", self.database, dbe_module.db, dbe_module.db.DB_CREATE
        )
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


class RecordsetCursor(_DBOpen):
    def setUp(self):
        super().setUp()
        segments = (
            b"\x7f\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        keys = ("a_o",)
        for i in range(380):
            self.database.table["file1"][0].append(
                encode(str(i + 1) + "Any value")
            )
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].ebm_table.put(1, bits)
        bits = b"\xff" * SegmentSize.db_segment_size_bytes
        self.database.ebm_control["file1"].ebm_table.put(2, bits)
        self.database.ebm_control["file1"].ebm_table.put(3, bits)
        for s in segments:
            self.database.segment_table["file1"].append(s)
        self.database.start_transaction()
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        for e in range(len(segments)):
            cursor.put(
                b"a_o",
                b"".join(
                    (
                        e.to_bytes(4, byteorder="big"),
                        (128 if e else 127).to_bytes(2, byteorder="big"),
                        (e + 1).to_bytes(4, byteorder="big"),
                    )
                ),
                self.database._dbe.DB_KEYLAST,
            )

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 1 required ",
                    "positional argument: 'recordset'",
                )
            ),
            _db.RecordsetCursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_get_record\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            _db.RecordsetCursor(None, None)._get_record,
        )

    def test_02___init__01(self):
        rc = _db.RecordsetCursor(None)
        self.assertEqual(rc._transaction, None)
        self.assertEqual(rc._database, None)

    def test_03___init__02(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"a_o")
        rc = _db.RecordsetCursor(rs)
        self.assertIs(rc._dbset, rs)

    def test_04__get_record(self):
        rc = _db.RecordsetCursor(
            self.database.recordlist_key("file1", "field1", key=b"a_o"),
            transaction=self.database.dbtxn,
            database=self.database.table["file1"][0],
        )
        self.assertEqual(rc._get_record(4000), None)
        self.assertEqual(rc._get_record(120), None)
        self.assertEqual(rc._get_record(10), (10, "10Any value"))
        self.assertEqual(rc._get_record(155), (155, "155Any value"))


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (berkeleydb, bsddb3):
        if dbe_module is None:
            continue
        bdb = dbe_module.db
        if dbe_module is berkeleydb:

            def encode(value):
                return value.encode()

        else:

            def encode(value):
                return value

        runner().run(loader(Database___init__))
        runner().run(loader(Database_transaction_methods))
        runner().run(loader(DatabaseInstance))
        runner().run(loader(Database_open_database))
        runner().run(loader(Database_do_database_task))
        runner().run(loader(DatabaseTransactions))
        runner().run(loader(Database_put_replace_delete))
        runner().run(loader(Database_methods))
        runner().run(loader(Database_find_values))
        runner().run(loader(Database_make_recordset))
        runner().run(loader(Database_freed_record_number))
        runner().run(loader(Database_empty_freed_record_number))
        runner().run(loader(RecordsetCursor))
