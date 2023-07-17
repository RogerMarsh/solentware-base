# test__dbdu_tkinter.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_dbdu_tkinter _database tests."""

import unittest
import os

try:
    from ... import db_tcl
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    db_tcl = None
from .. import _db_tkinter
from .. import _dbdu_tkinter
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray

_segment_sort_scale = SegmentSize._segment_sort_scale


class _DBdu(unittest.TestCase):
    def setUp(self):
        class _D(_dbdu_tkinter.Database, _db_tkinter.Database):
            def open_database(self, **k):
                super().open_database(bdb, **k)

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
        for f in os.listdir():
            if f.startswith("__db."):
                os.remove(f)


# Same tests as test__sqlite.Database___init__ with relevant additions.
# Alternative is one test method with just the additional tests.
class Database___init__(_DBdu):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 7 positional arguments ",
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
                    r" argument after \*\* must be a mapping, ",
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
            _db_tkinter.DatabaseError,
            "".join(("Database folder name {} is not valid",)),
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
            _db_tkinter.DatabaseError,
            r"No environment for start transaction",
            self.database.start_transaction,
        )
        self.assertEqual(self.database.dbtxn, None)

    def test_02_environment_flags(self):
        self.assertEqual(
            self.database.environment_flags(bdb),
            ["-create", "-recover", "-txn"],
        )

    def test_03_checkpoint_before_close_dbenv(self):
        self.database.checkpoint_before_close_dbenv()

    def test_04(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_transaction\(\) takes 1 positional argument ",
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
                    r"environment_flags\(\) missing 1 required ",
                    "positional argument: 'dbe'",
                )
            ),
            self.database.environment_flags,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"checkpoint_before_close_dbenv\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.database.checkpoint_before_close_dbenv,
            *(None,),
        )


# Memory databases are used for these tests.
class Database_open_database(_DBdu):
    def test_01(self):
        self.database = self._D({})
        repr_open_database = "".join(
            (
                "<bound method _DBdu.setUp.<locals>._D.open_database of ",
                "<__main__._DBdu.setUp.<locals>._D object at ",
            )
        )
        self.assertEqual(
            repr(self.database.open_database).startswith(repr_open_database),
            True,
        )

    def test_02(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database()
        self.assertEqual(self.database.dbenv.__class__.__name__, "str")
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
            TypeError,
            "".join(
                (
                    r"database_cursor\(\) takes from 3 to 4 ",
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
                    r"unset_defer_update\(\) takes 1 ",
                    "positional argument but 2 were given",
                )
            ),
            self.database.unset_defer_update,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"write_existence_bit_map\(\) missing 2 required ",
                    "positional arguments: 'file' and 'segment'",
                )
            ),
            self.database.write_existence_bit_map,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"new_deferred_root\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'",
                )
            ),
            self.database.new_deferred_root,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_defer_update\(\) takes 1 ",
                    "positional argument but 2 were given",
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
                    "positional arguments but 4 were given",
                )
            ),
            self.database.get_ebm_segment,
            *(None, None, None),
        )

    def test_02_database_cursor(self):
        self.assertRaisesRegex(
            _dbdu_tkinter.DatabaseError,
            "database_cursor not implemented",
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
        self.database.write_existence_bit_map("file1", segment)

    # Commented code in this test retained from bsddb3 and berkeleydb
    # version (without conversion to Tcl API).
    def test_05_new_deferred_root(self):
        self.assertEqual(
            self.database.table["file1_field1"][0].__class__.__name__, "str"
        )
        # self.assertEqual(
        #    self.database.index['file1_field1'],
        #    ['ixfile1_field1'])
        self.database.new_deferred_root("file1", "field1")
        self.assertEqual(
            self.database.table["file1_field1"][0].__class__.__name__, "str"
        )
        # self.assertEqual(
        #    self.database.index['file1_field1'],
        #    ['ixfile1_field1', 'ixt_0_file1_field1'])
        self.assertEqual(len(self.database.table["file1_field1"]), 1)

    def test_06_set_defer_update_01(self):
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], None)
        self.assertEqual(self.database.high_segment["file1"], None)
        self.assertEqual(self.database.first_chunk["file1"], None)

    def test_07_set_defer_update_02(self):
        db_tcl.tcl_tk_call(
            (
                self.database.table["file1"][0],
                "put",
                "-append",
                encode("any value"),
            )
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
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"do_final_segment_deferred_updates\(\) takes 1 ",
                    "positional argument but 2 were given",
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
        db_tcl.tcl_tk_call(
            (
                self.database.table["file1"][0],
                "put",
                "-append",
                b"Any value",
            )
        )
        self.database.existence_bit_maps["file1"] = None
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not subscriptable",
            self.database.do_final_segment_deferred_updates,
        )

    def test_05(self):
        db_tcl.tcl_tk_call(
            (
                self.database.table["file1"][0],
                "put",
                "-append",
                b"Any value",
            )
        )
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
            db_tcl.tcl_tk_call(
                (self.database.ebm_control["file1"].ebm_table, "get", 1)
            )[0][1],
            b"\x18" + b"\x00" * 15,
        )

    def test_06(self):
        command = (
            self.database.table["file1"][0],
            "put",
            "-append",
            b"Any value",
        )
        for i in range(127):
            db_tcl.tcl_tk_call(command)
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
        # The empty Tcl list returned by the 'get' command becomes "" on
        # return to Python.
        # The berkeleydb and bsddb3 interfaces return None.
        self.assertEqual(
            db_tcl.tcl_tk_call(
                (self.database.ebm_control["file1"].ebm_table, "get", 1)
            ),
            "",
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
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x01"),
                (b"list", b"\x00\x00\x00\x05\x00\x09"),
            ],
        )
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        db_tcl.tcl_tk_call((dt[0], "put", b"int", b"\x00\x00\x00\x05\x00\x01"))
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"list",
                b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02",
            )
        )
        db_tcl.tcl_tk_call(
            (dt[0], "put", b"bits", b"\x00\x00\x00\x05\x00\x02")
        )
        db_tcl.tcl_tk_call(
            (
                self.database.segment_table["file1"],
                "put",
                2,
                b"\x00\x01\x00\x04",
            )
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
        self.database.start_transaction()
        self.database.sort_and_write("file1", "field1", 5)
        self.database.commit()
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        # cursor = dt[0].cursor()
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(
            ra,
            [
                (b"bits", b"\x00\x00\x00\x05\x00!\x00\x00\x00\x03"),
                (b"int", b"\x00\x00\x00\x05\x00\x01"),
                (b"list", b"\x00\x00\x00\x05\x00\x03\x00\x00\x00\x02"),
            ],
        )
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        db_tcl.tcl_tk_call((dt[0], "put", b"int", b"\x00\x00\x00\x05\x00\x01"))
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"list",
                b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02",
            )
        )
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"list",
                b"\x00\x00\x00\x06\x00\x02\x00\x00\x00\x07",
            )
        )
        db_tcl.tcl_tk_call(
            (dt[0], "put", b"bits", b"\x00\x00\x00\x05\x00\x02")
        )
        db_tcl.tcl_tk_call(
            (
                self.database.segment_table["file1"],
                "put",
                2,
                b"\x00\x01\x00\x04",
            )
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
        self.database.start_transaction()
        self.database.sort_and_write("file1", "field1", 5)
        self.database.commit()
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"list",
                b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01",
            )
        )
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"bits",
                b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02",
            )
        )
        dst = self.database.segment_table["file1"]
        db_tcl.tcl_tk_call((dst, "put", 1, b"\x00\x01\x00\x04"))
        db_tcl.tcl_tk_call(
            (
                dst,
                "put",
                2,
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
            )
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
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r)
        self.assertEqual(
            ra,
            [
                ((b"bits", b"\x00\x00\x00\x02\x00(\x00\x00\x00\x02"),),
                ((b"list", b"\x00\x00\x00\x02\x00\x02\x00\x00\x00\x01"),),
            ],
        )
        cursor = db_tcl.tcl_tk_call((dst, "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r)
        self.assertEqual(
            ra,
            [
                ((1, b"\x00\x01\x00\x04"),),
                ((2, b"\n\n\xff\n\n\n\n\n\n\n\n\n\n\n\n\n"),),
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
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"list",
                b"\x00\x00\x00\x01\x00\x02\x00\x00\x00\x01",
            )
        )
        db_tcl.tcl_tk_call(
            (
                dt[0],
                "put",
                b"bits",
                b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02",
            )
        )
        dst = self.database.segment_table["file1"]
        db_tcl.tcl_tk_call((dst, "put", 1, b"\x00\x01\x00\x04"))
        db_tcl.tcl_tk_call(
            (
                dst,
                "put",
                2,
                b"".join(
                    (
                        b"\x00\x00\xff\x00\x00\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                ),
            )
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
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r)
        self.assertEqual(
            ra,
            [
                ((b"bits", b"\x00\x00\x00\x02\x00\x08\x00\x00\x00\x02"),),
                ((b"list", b'\x00\x00\x00\x01\x00"\x00\x00\x00\x01'),),
            ],
        )
        cursor = db_tcl.tcl_tk_call((dst, "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r)
        self.assertEqual(
            ra,
            [
                ((1, b"J\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"),),
                (
                    (
                        2,
                        b"".join(
                            (
                                b"\x00\x00\xff\x00\x00\x00\x00\x00",
                                b"\x00\x00\x00\x00\x00\x00\x00\x00",
                            )
                        ),
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
                    "positional arguments: 'file', 'field', and 'segment'",
                )
            ),
            database.sort_and_write,
        )

    def test_02(self):
        self.assertRaisesRegex(
            KeyError,
            "'file1'",
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
            "'NoneType' object is not iterable",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def test_05(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.assertRaisesRegex(
            KeyError,
            "'file1'",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def test_06(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.assertRaisesRegex(
            KeyError,
            "'file1'",
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
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.

    def test_08(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # self.assertEqual(dt[1].get_dbname(), (None, "1_file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.

    def test_09(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        dt = self.database.table["file1_field1"]
        self.assertEqual(len(dt), 1)
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.

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
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(ra, [(b"list", b"\x00\x00\x00\x05\x00\x01")])
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(
            ra, [(b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x01")]
        )
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
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
        for t in dt:
            self.assertEqual(t.__class__.__name__, "str")
        # self.assertEqual(dt[0].get_dbname(), (None, "file1_field1"))
        # The Tcl API does not have an equivalent to get_dbname.
        # cursor = dt[0].cursor()
        cursor = db_tcl.tcl_tk_call((dt[0], "cursor"))
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(
            ra, [(b"bits", b"\x00\x00\x00\x05\x00\x20\x00\x00\x00\x01")]
        )
        cursor = db_tcl.tcl_tk_call(
            (self.database.segment_table["file1"], "cursor")
        )
        ra = []
        while True:
            r = db_tcl.tcl_tk_call((cursor, "get", "-next"))
            if not r:
                break
            ra.append(r[0])
        # Must close cursor explicitly in Tcl API.
        db_tcl.tcl_tk_call((cursor, "close"))
        self.assertEqual(ra, [(1, b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")])


class Database_merge(_DBOpen):
    def setUp(self):
        super().setUp()
        if SegmentSize._segment_sort_scale != _segment_sort_scale:
            SegmentSize._segment_sort_scale = _segment_sort_scale

    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"merge\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'",
                )
            ),
            database.merge,
        )

    def test_02(self):
        self.assertEqual(SegmentSize._segment_sort_scale, _segment_sort_scale)
        self.assertEqual(len(self.database.table["file1_field1"]), 1)
        dbo = set(t for t in self.database.table["file1_field1"])
        self.assertEqual(len(dbo), 1)
        for t in dbo:
            self.assertEqual(t.__class__.__name__, "str")
        self.database.merge("file1", "field1")
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(self.database._path_marker, {"p1"})

    def test_03(self):
        self.assertEqual(SegmentSize._segment_sort_scale, _segment_sort_scale)
        self.database.new_deferred_root("file1", "field1")
        dt = self.database.table["file1_field1"]
        self.assertEqual(len(dt), 1)
        dbo = set(t for t in dt)
        self.assertEqual(len(dbo), 1)
        for t in dbo:
            self.assertEqual(t.__class__.__name__, "str")
        for t in dt[1:]:
            # t.close()
            db_tcl.tcl_tk_call((t, "close"))
        del dt[1:]
        self.database.merge("file1", "field1")
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {"p3", "p5", "p6", "p9", "p8", "p2", "p7", "p10", "p19", "p4"},
            )

    # The combinations of _segment_sort_scale settings and 'insert into ...'
    # statements in tests 4, 5, and 6 force merge() method through all paths
    # where deferred updates have to be done.
    # 'p14' and 'p20' are not traversed.  Assume 'p18' is always traversed at
    # some point to close and delete cursors when no longer needed.
    # The remaining 'do-nothing' paths are traversed by tests 1, 2, and 3.

    def test_04(self):
        self.assertEqual(SegmentSize._segment_sort_scale, _segment_sort_scale)
        dt = self.database.table["file1_field1"]
        self.database.new_deferred_root("file1", "field1")
        command = [dt[-1], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"])
        db_tcl.tcl_tk_call(tuple(command))
        self.assertEqual(len(dt), 1)
        dbo = set(t for t in dt)
        self.assertEqual(len(dbo), 1)
        for t in dbo:
            self.assertEqual(t.__class__.__name__, "str")
        for t in dt[1:]:
            db_tcl.tcl_tk_call((t, "close"))
        del dt[1:]
        command = [self.database.segment_table["file1"], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([2, b"\x00\x01\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        self.database.merge("file1", "field1")
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p3",
                    "p15",
                    "p8",
                    "p16",
                    "p5",
                    "p7",
                    "p12",
                    "p13",
                    "p11",
                    "p10",
                    "p4",
                    "p19",
                    "p18",
                    "p6",
                    "p2",
                    "p17",
                },
            )

    def test_05(self):
        SegmentSize._segment_sort_scale = 1
        self.assertEqual(SegmentSize._segment_sort_scale, 1)
        dt = self.database.table["file1_field1"]
        self.database.new_deferred_root("file1", "field1")
        command = [dt[-1], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"])
        db_tcl.tcl_tk_call(tuple(command))
        self.database.new_deferred_root("file1", "field1")
        self.assertEqual(len(dt), 1)
        dbo = set(t for t in dt)
        self.assertEqual(len(dbo), 1)
        for t in dbo:
            self.assertEqual(t.__class__.__name__, "str")
        # The single 'dt[-1].close()' in bsddb3 and berkeleydb version is
        # wrong but those interfaces allow one to get away with not closing
        # the database.
        # With Tcl API a _tkinter.TclError exception is raised in tearDown():
        # 'env<n>: Database handles still open at environment close'.
        # Close and forget about all except dt[0].
        for t in dt[1:]:
            db_tcl.tcl_tk_call((t, "close"))
        del dt[1:]
        command = [self.database.segment_table["file1"], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([2, b"\x00\x01\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        self.database.merge("file1", "field1")
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {"p4", "p10", "p5", "p19", "p7", "p9", "p2", "p3"},
            )

    def test_06(self):
        SegmentSize._segment_sort_scale = 2
        self.assertEqual(SegmentSize._segment_sort_scale, 2)
        self.merge_06_07()
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p7",
                    "p2",
                    "p11",
                    "p6",
                    "p15",
                    "p19",
                    "p3",
                    "p16",
                    "p17",
                    "p18",
                    "p13",
                    "p12",
                    "p5",
                    "p10",
                    "p4",
                },
            )

    # Verify test_06 is passed with the default SegmentSize.segment_sort_scale.
    # Almost. 'p8' is additional path traversed.  I assume it is necessary to
    # potentially put a load of 'None's in buffer at 'p6' which get cleared out
    # at 'p8' immediatly.
    def test_07(self):
        self.assertEqual(SegmentSize._segment_sort_scale, _segment_sort_scale)
        self.merge_06_07()
        if hasattr(self.database, "_path_marker"):
            self.assertEqual(
                self.database._path_marker,
                {
                    "p7",
                    "p2",
                    "p11",
                    "p6",
                    "p15",
                    "p19",
                    "p3",
                    "p16",
                    "p17",
                    "p18",
                    "p13",
                    "p12",
                    "p5",
                    "p10",
                    "p4",
                    "p8",
                },
            )

    def merge_06_07(self):
        dt = self.database.table["file1_field1"]
        self.database.new_deferred_root("file1", "field1")
        command = [dt[-1], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([b"list", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x02"])
        db_tcl.tcl_tk_call(tuple(command))
        command = [dt[-1], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([b"list1", b"\x00\x00\x00\x05\x00\x02\x00\x00\x00\x03"])
        db_tcl.tcl_tk_call(tuple(command))
        self.database.new_deferred_root("file1", "field1")
        command = [dt[-1], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([b"list1", b"\x00\x00\x00\x06\x00\x02\x00\x00\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        self.assertEqual(len(dt), 1)
        dbo = set(t for t in dt)
        self.assertEqual(len(dbo), 1)
        for t in dbo:
            self.assertEqual(t.__class__.__name__, "str")
        # The single 'dt[-1].close()' in bsddb3 and berkeleydb version is
        # wrong but those interfaces allow one to get away with not closing
        # the database.
        # With Tcl API a _tkinter.TclError exception is raised in tearDown():
        # 'env<n>: Database handles still open at environment close'.
        # Close and forget about all except dt[0].
        for t in dt[1:]:
            db_tcl.tcl_tk_call((t, "close"))
        del dt[1:]
        command = [self.database.segment_table["file1"], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([2, b"\x00\x01\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        command = [self.database.segment_table["file1"], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([3, b"\x00\x01\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        command = [self.database.segment_table["file1"], "put"]
        if self.database.dbtxn:
            command.extend(["-txn", self.database.dbtxn])
        command.extend([4, b"\x00\x01\x00\x04"])
        db_tcl.tcl_tk_call(tuple(command))
        self.database.merge("file1", "field1")


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (db_tcl,):
        if dbe_module is None:
            continue
        # bdb = dbe_module.Tk(useTk=False)
        bdb = dbe_module.tcl
        if dbe_module is db_tcl:

            def encode(value):
                return value

        runner().run(loader(Database___init__))
        runner().run(loader(Database_transaction_methods))
        runner().run(loader(Database_open_database))
        runner().run(loader(Database_methods))
        # runner().run(loader(Database__rows))
        runner().run(loader(Database_do_final_segment_deferred_updates))
        runner().run(loader(Database__sort_and_write_high_or_chunk))
        runner().run(loader(Database_sort_and_write))
        runner().run(loader(Database_merge))
