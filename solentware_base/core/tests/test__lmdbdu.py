# test__lmdbdu.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_lmdbdu _database tests"""

import unittest
import os
import shutil

try:
    import lmdb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdb = None

from .. import _lmdb
from .. import _lmdbdu
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray

from .test__lmdb import _Specification
from .test__lmdb_setUp_tearDown import (
    HOME,
    HOME_DATA,
    DBdu,
)

_segment_sort_scale = SegmentSize._segment_sort_scale


# Same tests as test__sqlite.Database___init__ with relevant additions.
# Alternative is one test method with just the additional tests.
class Database___init__(DBdu):
    def test_01_exceptions_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given$",
                )
            ),
            self._D,
            *(None, None, None),
        )

    def test_01_exceptions_02(self):
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

    def test_01_exceptions_03(self):
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "".join(("Database folder name {} is not valid$",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def test_04___init___01(self):
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
        self.assertEqual(database.dbtxn.transaction, None)
        self.assertEqual(database._dbe, None)
        self.assertEqual(database.segment_table, {})
        self.assertEqual(database.ebm_control, {})
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)

        # These tests are only difference to test__sqlite.Database___init__
        self.assertEqual(database.deferred_update_points, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(database.deferred_update_points, frozenset({31999}))
        self.assertEqual(database.first_chunk, {})
        self.assertEqual(database.high_segment, {})
        self.assertEqual(database.initial_high_segment, {})
        self.assertEqual(database.existence_bit_maps, {})
        self.assertEqual(database.value_segments, {})

    def test_04___init___02(self):
        database = self._D({})
        self.assertEqual(database.home_directory, HOME)
        self.assertEqual(database.database_file, HOME_DATA)

    # This combination of folder and segment_size_bytes arguments is used for
    # unittests, except for one to see a non-memory database with a realistic
    # segment size.
    def test_04___init___03(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(database.deferred_update_points, frozenset({127}))


# Transaction methods, except start_transaction, do not raise exceptions if
# called when no database open but do nothing.
class Database_transaction_bad_calls(DBdu):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_02_transaction_bad_calls_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_transaction\(\) takes 1 positional ",
                    "argument but 2 were given$",
                )
            ),
            self.database.start_transaction,
            *(None,),
        )

    def test_02_transaction_bad_calls_02(self):
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

    def test_02_transaction_bad_calls_03(self):
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


class Database_start_transaction(DBdu):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)

    def test_01_start_transaction_01(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, True)

    def test_01_start_transaction_02(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, True)

    def test_01_start_transaction_03(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_read_only_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, False)


class Database_backout_and_commit(DBdu):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)
        self.database.dbtxn._transaction = self.database.dbenv.begin()

    def test_01_backout_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdb.backout()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertEqual(sdb.dbtxn._transaction, None)
        self.assertEqual(sdb.dbtxn._write_requested, False)

    def test_02_commit_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdb.commit()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertEqual(sdb.dbtxn._transaction, None)
        self.assertEqual(sdb.dbtxn._write_requested, False)


class Database_open_database(DBdu, _Specification):
    def test_01(self):
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

    def test_02(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(self.dbe_module)
        self.assertEqual(self.database.dbenv.__class__.__name__, "Environment")
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)


# Memory databases are used for these tests.
class _DBOpen(DBdu):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(self.dbe_module)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()

    def append_arbitrary_record(self):
        # Many tests do the commented statement in Berkeley DB.  Here the
        # expansion justifies a convenience method.
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1"].datastore
        ) as cursor:
            if cursor.last():
                key = int.from_bytes(cursor.key(), byteorder="big") + 1
            else:
                key = 0
            cursor.put(
                key.to_bytes(4, byteorder="big"),
                ("any value").encode(),
                overwrite=False,
            )


# These methods must be in transaction in py-lmdb.
class Database_methods(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            _lmdbdu.DatabaseError,
            "database_cursor not implemented$",
            self.database.database_cursor,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"unset_defer_update\(\) takes 1 ",
                    "positional argument but 2 were given$",
                )
            ),
            self.database.unset_defer_update,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_write_existence_bit_map\(\) missing 2 required ",
                    "positional arguments: 'file' and 'segment'$",
                )
            ),
            self.database._write_existence_bit_map,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"new_deferred_root\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'$",
                )
            ),
            self.database.new_deferred_root,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_defer_update\(\) takes 1 ",
                    "positional argument but 2 were given$",
                )
            ),
            self.database.set_defer_update,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_ebm_segment\(\) takes 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.get_ebm_segment,
            *(None, None, None),
        )

    def test_02_database_cursor(self):
        self.assertRaisesRegex(
            _lmdbdu.DatabaseError,
            "database_cursor not implemented$",
            self.database.database_cursor,
            *(None, None),
        )

    def test_03_unset_defer_update(self):
        self.database.start_transaction()
        self.database.unset_defer_update()

    def test_04_write_existence_bit_map(self):
        segment = 0
        b = b"\x7f\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        bs = recordset.RecordsetSegmentBitarray(segment, None, b)
        self.database.existence_bit_maps["file1"] = {}
        self.database.existence_bit_maps["file1"][segment] = bs
        self.database._write_existence_bit_map("file1", segment)

    def test_05_new_deferred_root(self):
        self.assertEqual(
            self.database.table["file1_field1"].__class__.__name__,
            "_Datastore",
        )
        self.database.new_deferred_root("file1", "field1")

    def test_06_set_defer_update_01(self):
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], None)
        self.assertEqual(self.database.high_segment["file1"], None)
        self.assertEqual(self.database.first_chunk["file1"], None)

    def test_07_set_defer_update_02(self):
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1"].datastore
        ) as cursor:
            if cursor.last():
                key = int.from_bytes(cursor.key(), byteorder="big") + 1
            else:
                key = 0
            cursor.put(
                key.to_bytes(4, byteorder="big"),
                ("any value").encode(),
                overwrite=False,
            )
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], 0)
        self.assertEqual(self.database.high_segment["file1"], 0)
        self.assertEqual(self.database.first_chunk["file1"], True)

    def test_08_get_ebm_segment(self):
        self.assertEqual(
            self.database.get_ebm_segment(
                self.database.ebm_control["file1"], 1
            ),
            None,
        )


class Database_do_final_segment_deferred_updates(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"do_final_segment_deferred_updates\(\) takes 1 ",
                    "positional argument but 2 were given$",
                )
            ),
            database.do_final_segment_deferred_updates,
            *(None,),
        )

    def test_02(self):
        self.assertEqual(len(self.database.existence_bit_maps), 0)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.database.do_final_segment_deferred_updates()

    def test_03(self):
        self.database.existence_bit_maps["file1"] = None
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.database.do_final_segment_deferred_updates()

    def test_04(self):
        self.append_arbitrary_record()
        self.database.existence_bit_maps["file1"] = None
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not subscriptable$",
            self.database.do_final_segment_deferred_updates,
        )

    def test_05(self):
        self.append_arbitrary_record()
        self.database.existence_bit_maps["file1"] = {}
        ba = Bitarray()
        ba.frombytes(
            b"\30" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        )
        self.database.existence_bit_maps["file1"][0] = ba
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )

        # The segment has one record, not the high record, in segment but no
        # index references.  See test_06 for opposite.
        self.database.value_segments["file1"] = {}
        self.database.do_final_segment_deferred_updates()
        self.assertEqual(
            self.database.dbtxn.transaction.get(
                (0).to_bytes(4, byteorder="big"),
                db=self.database.ebm_control["file1"].ebm_table.datastore,
            ),
            b"\x18" + b"\x00" * 15,
        )

    def test_06(self):
        for i in range(128):
            self.append_arbitrary_record()
        self.database.existence_bit_maps["file1"] = {}
        ba = Bitarray()
        ba.frombytes(
            b"\3f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        )
        self.database.existence_bit_maps["file1"][0] = ba
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )

        # The segment has high record, and in this case others, in segment but
        # no index references.  See test_05 for opposite.
        self.assertEqual(self.database.deferred_update_points, {i})
        self.database.do_final_segment_deferred_updates()
        self.assertEqual(
            self.database.dbtxn.transaction.get(
                (0).to_bytes(4, byteorder="big"),
                db=self.database.ebm_control["file1"].ebm_table.datastore,
            ),
            None,
        )


class Database__sort_and_write_high_or_chunk(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def test_13(self):
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {
            "field1": {"bits": ba, "list": [1, 2], "list": [9]}
        }
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x00"),
                (b"list", b"\x00\x00\x00\x05\x00\x09"),
            ],
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"\x00\x00\x00\x00", b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
            ],
        )

        # Catch 'do-nothing' in _sort_and_write_high_or_chunk().
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(self.database._path_marker, {"p1", "p6"})

    # This drives _sort_and_write_high_or_chunk() through all paths which do
    # something except where the segment created by merging existing and new
    # is a bitarray and the existing segment is a list or bitarray.
    # The case where created segment is neither bitarray, list, nor int, is
    # ignored.  This method should probably assume it cannot happen: I do not
    # see how to test it without adding code elsewhere to create an unwanted
    # segment type.
    # Original test had "... 'list': [1, 2] ..." which led to a mismatch
    # between record count in segment and list of record numbers because
    # _sort_and_write_high_or_chunk assumes existing and new segments do not
    # have record numbers in common.  Not a problem for test's purpose, but
    # confusing if one looks closely.
    def test_14(self):
        dt = self.database.table["file1_field1"]
        txn = self.database.dbtxn.transaction
        for key, value in (
            (b"int", b"\x00\x00\x00\x05\x00\x01"),
            (b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"),
            (b"bits", b"\x00\x00\x00\x05\x00\x02"),
        ):
            txn.put(key, value, db=dt.datastore)
        txn.put(
            b"\x00\x00\x00\x02",
            b"\x00\x01\x00\x04",
            db=self.database.segment_table["file1"].datastore,
        )
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {
            "field1": {"bits": ba, "list": [2, 3], "list": [9]}
        }
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x05\x00!\x00\x00\x00\x03"),
                (b"int", b"\x00\x00\x00\x05\x00\x01"),
                (b"list", b"\x00\x00\x00\x05\x00\x03\x00\x00\x00\x02"),
            ],
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"\x00\x00\x00\x02", b"\x00\x01\x00\x04\x00\t"),
                (b"\x00\x00\x00\x03", b"*\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
            ],
        )
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p5b-b",
                    "p5a",
                    "p4b",
                    "p2b",
                    "p5b-a",
                    "p5b",
                    "p2a",
                    "p5a-b",
                    "p6",
                    "p4a",
                },
            )

    # Force _sort_and_write_high_or_chunk through path 'p3'.
    # That is it's only merit.  Setting "high_segment['file1'] = 3" with the
    # put 'list' records referring to segment 5 is bound to cause problems,
    # like putting the new segment 5 records in a separate record.
    def test_15(self):
        dt = self.database.table["file1_field1"]
        txn = self.database.dbtxn.transaction
        for key, value in (
            (b"int", b"\x00\x00\x00\x05\x00\x01"),
            (b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"),
            (b"list", b"\x00\x00\x00\x06\x00\x02\x00\x00\x00\x07"),
            (b"bits", b"\x00\x00\x00\x05\x00\x02"),
        ):
            txn.put(key, value, db=dt.datastore)
        txn.put(
            b"\x00\x00\x00\x02",
            b"\x00\x01\x00\x04",
            db=self.database.segment_table["file1"].datastore,
        )
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {
            "field1": {"bits": ba, "list": [2, 3], "list": [9]}
        }
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x05\x00!\x00\x00\x00\x03"),
                (b"int", b"\x00\x00\x00\x05\x00\x01"),
                (b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"),
                (b"list", b"\x00\x00\x00\x05\x00\t"),
                (b"list", b"\x00\x00\x00\x06\x00\x02\x00\x00\x00\x07"),
            ],
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"\x00\x00\x00\x02", b"\x00\x01\x00\x04"),
                (b"\x00\x00\x00\x03", b"*\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
            ],
        )
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p6",
                    "p5b",
                    "p2b",
                    "p5b-a",
                    "p3",
                    "p2a",
                    "p4a",
                    "p5a-b",
                    "p5a",
                },
            )

    def test_16(self):
        dt = self.database.table["file1_field1"]
        txn = self.database.dbtxn.transaction
        for key, value in (
            (b"list", b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01"),
            (b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02"),
        ):
            txn.put(key, value, db=dt.datastore)
        for key, value in (
            (b"\x00\x00\x00\x01", b"\x00\x01\x00\x04"),
            (
                b"\x00\x00\x00\x02",
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
            ),
        ):
            txn.put(
                key,
                value,
                db=self.database.segment_table["file1"].datastore,
            )
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {"field1": {"bits": ba}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 5
        self.database.high_segment["file1"] = 5
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 2)
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x02\x00(\x00\x00\x00\x02"),
                (b"list", b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01"),
            ],
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"\x00\x00\x00\x01", b"\x00\x01\x00\x04"),
                (b"\x00\x00\x00\x02", b"\n\n\xff\n\n\n\n\n\n\n\n\n\n\n\n\n"),
            ],
        )
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p5b-b",
                    "p5a",
                    "p4b",
                    "p2b",
                    "p5b-a",
                    "p5b",
                    "p2a",
                    "p5a-b",
                    "p6",
                    "p4a",
                },
            )

    def test_17(self):
        dt = self.database.table["file1_field1"]
        txn = self.database.dbtxn.transaction
        for key, value in (
            (b"list", b"\x00\x00\x00\x01\x00\x02\x00\x00\x00\x01"),
            (b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02"),
        ):
            txn.put(key, value, db=dt.datastore)
        for key, value in (
            (b"\x00\x00\x00\x01", b"\x00\x01\x00\x04"),
            (
                b"\x00\x00\x00\x02",
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
            ),
        ):
            txn.put(
                key,
                value,
                db=self.database.segment_table["file1"].datastore,
            )
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {"field1": {"list": ba}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 5
        self.database.high_segment["file1"] = 5
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 1)
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02"),
                (b"list", b'\x00\x00\x00\x01\x00"\x00\x00\x00\x01'),
            ],
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra,
            [
                (b"\x00\x00\x00\x01", b"J\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
                (
                    b"\x00\x00\x00\x02",
                    b"".join(
                        (
                            b"\x00\x00\xff\x00\x00\x00\x00\x00",
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            ],
        )
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p5b-b",
                    "p5a",
                    "p4b",
                    "p2b",
                    "p5b-a",
                    "p5b",
                    "p2a",
                    "p5a-b",
                    "p6",
                    "p4a",
                },
            )


class Database_sort_and_write(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"sort_and_write\(\) missing 3 required ",
                    "positional arguments: 'file', 'field', and 'segment'$",
                )
            ),
            database.sort_and_write,
        )

    def test_02(self):
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "nofield", None),
        )

    def test_03(self):
        self.database.value_segments["file1"] = {}
        self.database.sort_and_write("file1", "nofield", None)
        self.database.sort_and_write("file1", "field1", None)

    def test_04(self):
        self.database.value_segments["file1"] = {"field1": None}
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not iterable$",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def test_05(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def test_06(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "field1", 4),
        )

    def test_07(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 4)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")

    def test_08(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")

    def test_09(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")

    def test_10(self):
        self.database.value_segments["file1"] = {"field1": {"list": [1]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(ra, [(b"list", b"\x00\x00\x00\x05\x00\x01")])
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(ra, [])

    def test_11(self):
        self.database.value_segments["file1"] = {"field1": {"list": [1, 4]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra, [(b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x00")]
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(ra, [(b"\x00\x00\x00\x00", b"\x00\x01\x00\x04")])

    def test_12(self):
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {"field1": {"bits": ba}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(dt.__class__.__name__, "_Datastore")
        ra = []
        with self.database.dbtxn.transaction.cursor(db=dt.datastore) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra, [(b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x00")]
        )
        ra = []
        with self.database.dbtxn.transaction.cursor(
            db=self.database.segment_table["file1"].datastore
        ) as cursor:
            while True:
                r = cursor.next()
                if not r:
                    break
                ra.append(cursor.item())
        self.assertEqual(
            ra, [(b"\x00\x00\x00\x00", b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")]
        )


# merge() does nothing.
class Database_merge(_DBOpen):
    def setUp(self):
        super().setUp()
        if SegmentSize._segment_sort_scale != _segment_sort_scale:
            SegmentSize._segment_sort_scale = _segment_sort_scale


class Database_delete_index(_DBOpen):
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"delete_index\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'$",
                )
            ),
            database.delete_index,
        )

    def test_delete_index_01(self):
        self.database.start_transaction()
        self.assertEqual(
            self.database.delete_index("file1", "field1") is None, True
        )
        self.database.backout()


class Database_find_value_segments(_DBOpen):
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"find_value_segments\(\) missing 2 required ",
                    "positional arguments: 'field' and 'file'$",
                )
            ),
            database.find_value_segments,
        )

    def test_find_value_segments_01(self):
        self.database.start_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.find_value_segments("field1", "file1"),),
        )
        self.database.backout()

    def test_find_value_segments_02(self):
        self.database.start_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.find_value_segments("field1", "file1"),),
        )
        self.database.commit()

    def test_find_value_segments_03(self):
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.find_value_segments("field1", "file1"),),
        )
        self.database.end_read_only_transaction()

    def test_find_value_segments_02(self):
        self.database.value_segments["file1"] = {"field1": {"int": [1]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.start_transaction()
        self.database.sort_and_write("file1", "field1", 5)
        self.database.commit()
        self.database.start_read_only_transaction()
        for item in self.database.find_value_segments("field1", "file1"):
            self.assertEqual(
                item,
                [b"int", b"\x00\x00\x00\x05", 0, b"\x00\x01", b"\x00\x01"],
            )
        self.database.end_read_only_transaction()

    def test_find_value_segments_03(self):
        self.database.value_segments["file1"] = {"field1": {"list": [1, 4]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.start_transaction()
        self.database.sort_and_write("file1", "field1", 5)
        self.database.commit()
        self.database.start_read_only_transaction()
        for item in self.database.find_value_segments("field1", "file1"):
            self.assertEqual(
                item,
                [
                    b"list",
                    b"\x00\x00\x00\x05",
                    0,
                    b"\x00\x02",
                    b"\x00\x00\x00\x00",
                ],
            )
        self.database.end_read_only_transaction()


# Not memory-only so the folder can hold the sorted index sequential files.
class _DBOpen(DBdu):
    def setUp(self):
        super().setUp()
        self.folder = os.path.join("/tmp", "merge_test_lmdbdu")
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}),
            folder=self.folder,
            segment_size_bytes=None,
        )
        self.database.open_database(self.dbe_module)
        self.sequential = os.path.join(self.folder, "sequential")
        os.mkdir(self.sequential)
        self.field = os.path.join(self.sequential, "field1")
        os.mkdir(self.field)

    def tearDown(self):
        self.database.close_database()
        shutil.rmtree(self.folder)
        super().tearDown()


class Database_merge_import(_DBOpen):
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"merge_import\(\) missing 4 required ",
                    "positional arguments: 'index_directory', ",
                    "'file', 'field', and 'commit_limit'$",
                )
            ),
            database.merge_import,
        )

    def test_merge_import_01(self):
        self.database.start_transaction()
        self.assertRaisesRegex(
            FileNotFoundError,
            "No such file or directory: 'ss'$",
            next,
            *(self.database.merge_import("ss", "file1", "field1", 10),),
        )
        self.database.commit()

    def test_merge_import_02(self):
        shutil.rmtree(self.field)
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(
                self.database.merge_import(
                    self.sequential, "file1", "field1", 10
                ),
            ),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_03(self):
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_04(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write("")
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_05(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write("1")
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            TypeError,
            r"object of type 'int' has no len\(\)$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_06(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", "b", "c", "d"]))
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            AssertionError,
            "",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_07(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", "b", "c", "d", "e", "f"]))
        self.database.start_read_only_transaction()
        self.assertRaisesRegex(
            AssertionError,
            "",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )
        self.database.end_read_only_transaction()

    def test_merge_import_08(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(
                repr([b"a", b"\x00\x00\x00\x01", 1, b"\x00\x01", b"\x00\x07"])
            )
        self.database.start_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 2),),
        )
        self.database.commit()

    def test_merge_import_09(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(
                repr([b"a", b"\x00\x00\x00\x01", 1, b"\x00\x01", b"\x00\x07"])
            )
            file.write("\n")
            file.write(
                repr([b"b", b"\x00\x00\x00\x01", 1, b"\x00\x01", b"\x00\x07"])
            )
            file.write("\n")
            file.write(
                repr([b"c", b"\x00\x00\x00\x01", 1, b"\x00\x01", b"\x00\x07"])
            )
        self.database.start_transaction()
        for count in self.database.merge_import(
            self.field, "file1", "field1", 2
        ):
            self.assertEqual(count, 2)
        self.database.commit()


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (lmdb,):
        if dbe_module is None:
            continue

        def encode(value):
            return value

        runner().run(loader(Database___init__))
        runner().run(loader(Database_transaction_bad_calls))
        runner().run(loader(Database_start_transaction))
        runner().run(loader(Database_backout_and_commit))
        runner().run(loader(Database_open_database))
        runner().run(loader(Database_methods))
        # runner().run(loader(Database__rows))
        runner().run(loader(Database_do_final_segment_deferred_updates))
        runner().run(loader(Database__sort_and_write_high_or_chunk))
        runner().run(loader(Database_sort_and_write))
        runner().run(loader(Database_merge))
        runner().run(loader(Database_delete_index))
        runner().run(loader(Database_find_value_segments))
        runner().run(loader(Database_merge_import))
