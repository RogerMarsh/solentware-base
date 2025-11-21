# test__dbdu.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_dbdu _database tests"""

import unittest
import os
import shutil

try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
try:
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None

from .. import _db
from .. import _dbdu
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray

_segment_sort_scale = SegmentSize._segment_sort_scale

if berkeleydb:


    class _DBdu(unittest.TestCase):
        def setUp(self):
            class _D(_dbdu.Database, _db.Database):
                def open_database(self, **k):
                    super().open_database(berkeleydb.db, **k)

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


    # Same tests as test__sqlite.Database___init__ with relevant additions.
    # Alternative is one test method with just the additional tests.
    class Database___init__(_DBdu):
        def test_01(self):
            self.assertRaisesRegex(
                TypeError,
                "".join(
                    (
                        r"__init__\(\) takes from 2 to 7 positional arguments ",
                        "but 8 were given$",
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
                        r" argument after \*\* must be a mapping, ",
                        "not NoneType$",
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
                "".join(("Database folder name {} is not valid$",)),
                self._D,
                *({},),
                **dict(folder={}),
            )

        def test_04(self):
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
            # Following test may not pass when run by unittest discovery
            # because other test modules may change the tested value.
            # self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)

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
            self.assertEqual(database.deferred_update_points, frozenset({127}))


    # Transaction methods, except start_transaction, do not raise exceptions if
    # called when no database open but do nothing.
    class Database_transaction_methods(_DBdu):
        def setUp(self):
            super().setUp()
            self.database = self._D({})

        def test_01_start_transaction(self):
            self.assertEqual(self.database.dbenv, None)
            self.assertRaisesRegex(
                AttributeError,
                r"'NoneType' object has no attribute 'txn_begin'$",
                self.database.start_transaction,
            )
            self.assertEqual(self.database.dbtxn, None)

        def test_02_environment_flags(self):
            self.assertEqual(
                self.database.environment_flags(berkeleydb.db),
                (
                    berkeleydb.db.DB_CREATE
                    | berkeleydb.db.DB_RECOVER
                    | berkeleydb.db.DB_INIT_MPOOL
                    | berkeleydb.db.DB_INIT_LOCK
                    | berkeleydb.db.DB_INIT_LOG
                    | berkeleydb.db.DB_INIT_TXN
                    | berkeleydb.db.DB_PRIVATE
                ),
            )

        def test_03_checkpoint_before_close_dbenv(self):
            self.database.checkpoint_before_close_dbenv()

        def test_04(self):
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
                        r"environment_flags\(\) missing 1 required ",
                        "positional argument: 'dbe'$",
                    )
                ),
                self.database.environment_flags,
            )
            self.assertRaisesRegex(
                TypeError,
                "".join(
                    (
                        r"checkpoint_before_close_dbenv\(\) takes 1 positional ",
                        "argument but 2 were given$",
                    )
                ),
                self.database.checkpoint_before_close_dbenv,
                *(None,),
            )


    # Memory databases are used for these tests.
    class Database_open_database(_DBdu):
        def test_01(self):
            self.database = self._D({})
            repr_main = "".join(
                (
                    "<bound method _DBdu.setUp.<locals>._D.open_database of ",
                    "<__main__._DBdu.setUp.<locals>._D object at ",
                )
            )
            repr_discover = "".join(
                (
                    "<bound method _DBdu.setUp.<locals>._D.open_database of ",
                    "<solentware_base.core.tests.test__dbdu._DBdu.setUp.",
                    "<locals>._D object at ",
                )
            )
            repr_open = repr(self.database.open_database)
            self.assertEqual(
                (
                    repr_open.startswith(repr_main) or
                    repr_open.startswith(repr_discover)
                ),
                True,
            )

        def test_02(self):
            self.database = self._D({}, segment_size_bytes=None)
            self.database.open_database()
            self.assertEqual(self.database.dbenv.__class__.__name__, "DBEnv")
            self.database.close_database()
            self.assertEqual(self.database.dbenv, None)


    # Memory databases are used for these tests.
    class _DBOpen(_DBdu):
        def setUp(self):
            super().setUp()
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
            )
            self.database.open_database()

        def tearDown(self):
            self.database.close_database()
            super().tearDown()


    class Database_methods(_DBOpen):
        def test_01(self):
            self.assertRaisesRegex(
                _dbdu.DatabaseError,
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
                _dbdu.DatabaseError,
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
                self.database.table["file1_field1"].__class__.__name__, "DB"
            )
            # self.assertEqual(
            #    self.database.index['file1_field1'],
            #    ['ixfile1_field1'])
            self.database.new_deferred_root("file1", "field1")
            self.assertEqual(
                self.database.table["file1_field1"].__class__.__name__, "DB"
            )
            # self.assertEqual(
            #    self.database.index['file1_field1'],
            #    ['ixfile1_field1', 'ixt_0_file1_field1'])
            self.assertEqual(len(self.database.table["file1_field1"]), 0)

        def test_06_set_defer_update_01(self):
            self.database.set_defer_update()
            self.assertEqual(self.database.initial_high_segment["file1"], None)
            self.assertEqual(self.database.high_segment["file1"], None)
            self.assertEqual(self.database.first_chunk["file1"], None)

        def test_07_set_defer_update_02(self):
            self.database.table["file1"].append(encode("any value"))
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
            self.database.table["file1"].append(b"Any value")
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
            self.database.table["file1"].append(b"Any value")
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
                self.database.ebm_control["file1"].ebm_table.get(1),
                b"\x18" + b"\x00" * 15,
            )

        def test_06(self):
            for i in range(127):
                self.database.table["file1"].append(b"Any value")
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
            self.assertEqual(self.database.deferred_update_points, {i + 1})
            self.database.do_final_segment_deferred_updates()
            self.assertEqual(
                self.database.ebm_control["file1"].ebm_table.get(1), None
            )


    class Database__sort_and_write_high_or_chunk(_DBOpen):
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            dt = self.database.table["file1_field1"]
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x01"),
                    (b"list", b"\x00\x00\x00\x05\x00\x09"),
                ],
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (1, b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
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
            dt.put(b"int", b"\x00\x00\x00\x05\x00\x01")
            dt.put(b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02")
            dt.put(b"bits", b"\x00\x00\x00\x05\x00\x02")
            self.database.segment_table["file1"].put(2, b"\x00\x01\x00\x04")
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            self.assertEqual(len(dt), 3)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (b"bits", b"\x00\x00\x00\x05\x00!\x00\x00\x00\x03"),
                    (b"int", b"\x00\x00\x00\x05\x00\x01"),
                    (b"list", b"\x00\x00\x00\x05\x00\x03\x00\x00\x00\x02"),
                ],
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (2, b"\x00\x01\x00\x04\x00\t"),
                    (3, b"*\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
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
            dt.put(b"int", b"\x00\x00\x00\x05\x00\x01")
            dt.put(b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02")
            dt.put(b"list", b"\x00\x00\x00\x06\x00\x02\x00\x00\x00\x07")
            dt.put(b"bits", b"\x00\x00\x00\x05\x00\x02")
            self.database.segment_table["file1"].put(2, b"\x00\x01\x00\x04")
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            self.assertEqual(len(dt), 5)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
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
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (2, b"\x00\x01\x00\x04"),
                    (3, b"*\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
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
            dt.put(b"list", b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01")
            dt.put(b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02")
            self.database.segment_table["file1"].put(1, b"\x00\x01\x00\x04")
            self.database.segment_table["file1"].put(
                2,
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 2)
            self.database.commit()
            self.assertEqual(len(dt), 2)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (b"bits", b"\x00\x00\x00\x02\x00(\x00\x00\x00\x02"),
                    (b"list", b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01"),
                ],
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (1, b"\x00\x01\x00\x04"),
                    (2, b"\n\n\xff\n\n\n\n\n\n\n\n\n\n\n\n\n"),
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
            dt.put(b"list", b"\x00\x00\x00\x01\x00\x02\x00\x00\x00\x01")
            dt.put(b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02")
            self.database.segment_table["file1"].put(1, b"\x00\x01\x00\x04")
            self.database.segment_table["file1"].put(
                2,
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 1)
            self.database.commit()
            self.assertEqual(len(dt), 2)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02"),
                    (b"list", b'\x00\x00\x00\x01\x00"\x00\x00\x00\x01'),
                ],
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra,
                [
                    (1, b"J\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),
                    (
                        2,
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
            self.assertEqual(len(dt), 0)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))

        def test_08(self):
            self.database.value_segments["file1"] = {"field1": {}}
            self.database.first_chunk["file1"] = True
            self.database.initial_high_segment["file1"] = 4
            self.database.high_segment["file1"] = 3
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            dt = self.database.table["file1_field1"]
            self.assertEqual(len(dt), 0)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))

        def test_09(self):
            self.database.value_segments["file1"] = {"field1": {}}
            self.database.first_chunk["file1"] = False
            self.database.initial_high_segment["file1"] = 4
            self.database.high_segment["file1"] = 3
            self.database.sort_and_write("file1", "field1", 5)
            dt = self.database.table["file1_field1"]
            self.assertEqual(len(dt), 0)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))

        def test_10(self):
            self.database.value_segments["file1"] = {"field1": {"list": [1]}}
            self.database.first_chunk["file1"] = False
            self.database.initial_high_segment["file1"] = 4
            self.database.high_segment["file1"] = 3
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            dt = self.database.table["file1_field1"]
            self.assertEqual(len(dt), 1)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(ra, [(b"list", b"\x00\x00\x00\x05\x00\x01")])
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
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
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            dt = self.database.table["file1_field1"]
            self.assertEqual(len(dt), 1)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra, [(b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x01")]
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(ra, [(1, b"\x00\x01\x00\x04")])

        def test_12(self):
            ba = Bitarray()
            ba.frombytes(b"\x0a" * 16)
            self.database.value_segments["file1"] = {"field1": {"bits": ba}}
            self.database.first_chunk["file1"] = False
            self.database.initial_high_segment["file1"] = 4
            self.database.high_segment["file1"] = 3
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            dt = self.database.table["file1_field1"]
            self.assertEqual(len(dt), 1)
            self.assertEqual(dt.__class__.__name__, "DB")
            self.assertEqual(dt.get_dbname(), (None, "file1_field1"))
            cursor = dt.cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(
                ra, [(b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x01")]
            )
            cursor = self.database.segment_table["file1"].cursor()
            ra = []
            while True:
                r = cursor.next()
                if r is None:
                    break
                ra.append(r)
            self.assertEqual(ra, [(1, b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")])


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
            self.assertEqual(
                self.database.delete_index("file1", "field1") is None, True
            )


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
            self.assertRaisesRegex(
                StopIteration,
                "$",
                next,
                *(self.database.find_value_segments("field1", "file1"),),
            )

        def test_find_value_segments_02(self):
            self.database.value_segments["file1"] = {"field1": {"int": [1]}}
            self.database.first_chunk["file1"] = False
            self.database.initial_high_segment["file1"] = 4
            self.database.high_segment["file1"] = 3
            self.database.start_transaction()
            self.database.sort_and_write("file1", "field1", 5)
            self.database.commit()
            for item in self.database.find_value_segments("field1", "file1"):
                self.assertEqual(
                    item,
                    [b"int", b"\x00\x00\x00\x05", 0, b"\x00\x01", b"\x00\x01"],
                )

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
            for item in self.database.find_value_segments("field1", "file1"):
                self.assertEqual(
                    item,
                    [
                        b"list",
                        b"\x00\x00\x00\x05",
                        0,
                        b"\x00\x02",
                        b"\x00\x00\x00\x01",
                    ],
                )


    # Not memory-only so the folder can hold the sorted index sequential files.
    class _DBMerge(_DBdu):
        def setUp(self):
            super().setUp()
            self.folder = os.path.join("/tmp", "merge_test_dbdu")
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}),
                folder=self.folder,
                segment_size_bytes=None,
            )
            self.database.open_database()
            self.sequential = os.path.join(self.folder, "sequential")
            os.mkdir(self.sequential)
            self.field = os.path.join(self.sequential, "field1")
            os.mkdir(self.field)

        def tearDown(self):
            self.database.close_database()
            shutil.rmtree(self.folder)
            super().tearDown()


    class Database_merge_import(_DBMerge):
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
            self.assertRaisesRegex(
                FileNotFoundError,
                "No such file or directory: 'ss'$",
                next,
                *(self.database.merge_import("ss", "file1", "field1", 10),),
            )

        def test_merge_import_02(self):
            shutil.rmtree(self.field)
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

        def test_merge_import_03(self):
            self.assertRaisesRegex(
                StopIteration,
                "$",
                next,
                *(self.database.merge_import(self.field, "file1", "field1", 10),),
            )

        def test_merge_import_04(self):
            with open(os.path.join(self.field, "0"), mode="w") as file:
                file.write("")
            self.assertRaisesRegex(
                StopIteration,
                "$",
                next,
                *(self.database.merge_import(self.field, "file1", "field1", 10),),
            )

        def test_merge_import_05(self):
            with open(os.path.join(self.field, "0"), mode="w") as file:
                file.write("1")
            self.assertRaisesRegex(
                TypeError,
                r"object of type 'int' has no len\(\)$",
                next,
                *(self.database.merge_import(self.field, "file1", "field1", 10),),
            )

        def test_merge_import_06(self):
            with open(os.path.join(self.field, "0"), mode="w") as file:
                file.write(repr(["a", "b", "c", "d"]))
            self.assertRaisesRegex(
                AssertionError,
                "",
                next,
                *(self.database.merge_import(self.field, "file1", "field1", 10),),
            )

        def test_merge_import_07(self):
            with open(os.path.join(self.field, "0"), mode="w") as file:
                file.write(repr(["a", "b", "c", "d", "e", "f"]))
            self.assertRaisesRegex(
                AssertionError,
                "",
                next,
                *(self.database.merge_import(self.field, "file1", "field1", 10),),
            )

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


    def encode(value):
        """Return encoded value.

        This method exists because the bsddb3 package required the value,
        and the tests served both berkeleydb and bsddb3 until Python 3.9
        reached End of Life.

        """
        return value.encode()


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if berkeleydb:
        runner().run(loader(Database___init__))
        runner().run(loader(Database_transaction_methods))
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
