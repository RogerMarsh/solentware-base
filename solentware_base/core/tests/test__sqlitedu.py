# test__sqlitedu.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_sqlitedu _database tests"""

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
from .. import _sqlitedu
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray

_segment_sort_scale = SegmentSize._segment_sort_scale


class _SQLitedu(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


# Same tests as test__sqlite.Database___init__ with relevant additions.
# Alternative is one test method with just the additional tests.
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
        self.assertEqual(database.deferred_update_points, frozenset({127}))


# Memory databases are used for these tests.
class Database_open_database:
    def t01(self):
        self.database = self._D({})
        self.assertEqual(
            repr(self.database.open_database).startswith(
                "<bound method _SQLitedu"
            ),
            True,
        )
        self.assertEqual(
            ".setUp.<locals>._D.open_database of "
            in repr(self.database.open_database),
            True,
        )
        self.assertEqual(
            ".setUp.<locals>._D object at "
            in repr(self.database.open_database),
            True,
        )

    def t02(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database()
        self.assertEqual(self.database.dbenv.__class__.__name__, "Connection")
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)


# Memory databases are used for these tests.
class _SQLiteOpen(_SQLitedu):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database()

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class Database_methods:
    def t01(self):
        self.assertRaisesRegex(
            _sqlitedu.DatabaseError,
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

    def t02_database_cursor(self):
        self.assertRaisesRegex(
            _sqlitedu.DatabaseError,
            "database_cursor not implemented$",
            self.database.database_cursor,
            *(None, None),
        )

    def t03_unset_defer_update(self):
        self.database.start_transaction()
        self.database.unset_defer_update()

    def t05_new_deferred_root(self):
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        self.assertEqual(self.database.index["file1_field1"], "ixfile1_field1")
        self.database.new_deferred_root("file1", "field1")
        self.assertEqual(
            self.database.table["file1_field1"],
            "file1_field1",
        )
        self.assertEqual(
            self.database.index["file1_field1"],
            "ixfile1_field1",
        )

    def t06_set_defer_update_01(self):
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], None)
        self.assertEqual(self.database.high_segment["file1"], None)
        self.assertEqual(self.database.first_chunk["file1"], None)

    def t07_set_defer_update_02(self):
        cursor = self.database.dbenv.cursor()
        try:
            cursor.execute(
                "insert into file1 ( Value ) values ( ? )", ("Any value",)
            )
        finally:
            cursor.close()

        # In apsw, at Python3.6 when creating these tests, the insert does not
        # start a transaction but in sqlite3, at Python3.7 when creating these
        # tests, it does.
        # The insert is there only to drive set_defer_update() through the
        # intended path.  In normal use the table will already be occupied or
        # not and taken as found.
        try:
            self.database.commit()
        except Exception as exc:
            if exc.__class__.__name__ != "SQLError":
                raise

        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], 0)
        self.assertEqual(self.database.high_segment["file1"], 0)
        self.assertEqual(self.database.first_chunk["file1"], True)

    def t08_set_defer_update_03(self):
        # Simulate normal use: the insert is not part of the deferred update.
        self.database.start_transaction()
        cursor = self.database.dbenv.cursor()
        try:
            cursor.execute(
                "insert into file1 ( Value ) values ( ? )", ("Any value",)
            )
        finally:
            cursor.close()
        self.database.commit()

        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], 0)
        self.assertEqual(self.database.high_segment["file1"], 0)
        self.assertEqual(self.database.first_chunk["file1"], True)

    def t09_get_ebm_segment(self):
        self.assertEqual(
            self.database.get_ebm_segment(
                self.database.ebm_control["file1"], 1
            ),
            None,
        )


class Database__rows:
    def t01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_rows\(\) missing 2 required ",
                    "positional arguments: 'segvalues' and 'segment'$",
                )
            ),
            database._rows,
        )

    def t02(self):
        database = self._D({}, segment_size_bytes=None)
        values = {"kv3": (2, b"dd"), "kv1": (56, b"lots"), "kv2": (1, b"l")}
        self.assertEqual(
            [r for r in database._rows(values, 5)],
            [
                ("kv1", 5, 56, b"lots"),
                ("kv2", 5, 1, b"l"),
                ("kv3", 5, 2, b"dd"),
            ],
        )


class Database_do_final_segment_deferred_updates:
    def t01(self):
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

    def t02(self):
        self.assertEqual(len(self.database.existence_bit_maps), 0)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.database.do_final_segment_deferred_updates()

    def t03(self):
        self.database.existence_bit_maps["file1"] = None
        self.assertEqual(len(self.database.existence_bit_maps), 1)
        self.assertIn(
            "field1", self.database.specification["file1"]["secondary"]
        )
        self.database.do_final_segment_deferred_updates()

    def t04(self):
        cursor = self.database.dbenv.cursor()
        try:
            cursor.execute(
                "insert into file1 ( Value ) values ( ? )", ("Any value",)
            )
        finally:
            cursor.close()
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

    def t05(self):
        cursor = self.database.dbenv.cursor()
        try:
            cursor.execute(
                "insert into file1 ( Value ) values ( ? )", ("Any value",)
            )
        finally:
            cursor.close()
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

    def t06(self):
        cursor = self.database.dbenv.cursor()
        try:
            for i in range(127):
                cursor.execute(
                    "insert into file1 ( Value ) values ( ? )", ("Any value",)
                )
        finally:
            cursor.close()
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


class Database_sort_and_write:
    def t01(self):
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

    def t02(self):
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "nofield", None),
        )

    def t03(self):
        self.database.value_segments["file1"] = {}
        self.database.sort_and_write("file1", "nofield", None)
        self.database.sort_and_write("file1", "field1", None)

    def t04(self):
        self.database.value_segments["file1"] = {"field1": None}
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not iterable$",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def t05(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )

    def t06(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            self.database.sort_and_write,
            *("file1", "field1", 4),
        )

    def t07(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 4)
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")

    def t08(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(
            self.database.table["file1_field1"],
            "file1_field1",
        )

    def t09(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")

    def t10(self):
        self.database.value_segments["file1"] = {"field1": {"list": [1]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        cursor = self.database.dbenv.cursor()
        self.assertEqual(
            cursor.execute("select * from file1_field1").fetchall(),
            [("list", 5, 1, 1)],
        )
        self.assertEqual(
            cursor.execute("select * from file1__segment").fetchall(), []
        )

    def t11(self):
        self.database.value_segments["file1"] = {"field1": {"list": [1, 4]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        cursor = self.database.dbenv.cursor()
        self.assertEqual(
            cursor.execute("select * from file1_field1").fetchall(),
            [("list", 5, 2, 1)],
        )
        self.assertEqual(
            cursor.execute("select * from file1__segment").fetchall(),
            [(b"\x00\x01\x00\x04",)],
        )

    def t12(self):
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {"field1": {"bits": ba}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        cursor = self.database.dbenv.cursor()
        self.assertEqual(
            cursor.execute("select * from file1_field1").fetchall(),
            [("bits", 5, 32, 1)],
        )
        self.assertEqual(
            cursor.execute("select * from file1__segment").fetchall(),
            [(b"\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n",)],
        )

    def t13(self):
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
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        cursor = self.database.dbenv.cursor()
        self.assertEqual(
            cursor.execute("select count ( * ) from file1_field1").fetchall(),
            [(2,)],
        )
        self.assertEqual(
            cursor.execute(
                "select count ( * ) from file1__segment"
            ).fetchall(),
            [(1,)],
        )

    def t14(self):
        cursor = self.database.dbenv.cursor()
        cursor.execute(
            " ".join(
                (
                    "insert into file1_field1",
                    "( field1 , Segment , RecordCount",
                    ", file1 )",
                    "values ( ? , ? , ? , ? )",
                )
            ),
            ("int", 5, 1, 1),
        )
        cursor.execute(
            " ".join(
                (
                    "insert into file1_field1",
                    "( field1 , Segment , RecordCount",
                    ", file1 )",
                    "values ( ? , ? , ? , ? )",
                )
            ),
            ("list", 5, 2, 2),
        )
        cursor.execute(
            " ".join(
                (
                    "insert into file1__segment ( rowid , RecordNumbers )",
                    "values ( ? , ? )",
                )
            ),
            (2, b"\x00\x01\x00\x04"),
        )
        cursor.execute(
            " ".join(
                (
                    "insert into file1_field1 ( field1 , Segment , RecordCount"
                    ", file1 )",
                    "values ( ? , ? , ? , ? )",
                )
            ),
            ("bits", 5, 1, 2),
        )
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
        self.assertEqual(self.database.table["file1_field1"], "file1_field1")
        self.assertEqual(
            cursor.execute("select count ( * ) from file1_field1").fetchall(),
            [(3,)],
        )
        self.assertEqual(
            cursor.execute(
                "select count ( * ) from file1__segment"
            ).fetchall(),
            [(2,)],
        )


# merge() does nothing.
class Database_merge(_SQLiteOpen):
    def setUp(self):
        super().setUp()
        if SegmentSize._segment_sort_scale != _segment_sort_scale:
            SegmentSize._segment_sort_scale = _segment_sort_scale


class Database_delete_index:
    def t01(self):
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

    def t02_delete_index(self):
        self.assertEqual(
            self.database.delete_index("file1", "field1") is None, True
        )


class Database_find_value_segments:
    def t01(self):
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

    def t02_find_value_segments(self):
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.find_value_segments("field1", "file1"),),
        )

    def t03_find_value_segments(self):
        self.database.value_segments["file1"] = {"field1": {"int": [1]}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.start_transaction()
        self.database.sort_and_write("file1", "field1", 5)
        self.database.commit()
        for item in self.database.find_value_segments("field1", "file1"):
            self.assertEqual(item, ["int", 5, 0, 1, 1])

    def t04_find_value_segments(self):
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
            self.assertEqual(item, ["list", 5, 0, 2, 1])


# Not memory-only so the folder can hold the sorted index sequential files.
class _SQLiteMerge(_SQLitedu):
    def setup_detail(self):
        self.folder = os.path.join("/tmp", "merge_test_sqlitedu")
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


class Database_merge_import:
    def t01(self):
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

    def t02_merge_import(self):
        self.assertRaisesRegex(
            FileNotFoundError,
            "No such file or directory: 'ss'$",
            next,
            *(self.database.merge_import("ss", "file1", "field1", 10),),
        )

    def t03_merge_import(self):
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

    def t04_merge_import(self):
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )

    def t05_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write("")
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )

    def t06_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write("1")
        self.assertRaisesRegex(
            TypeError,
            r"object of type 'int' has no len\(\)$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )

    def t07_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", "b", "c", "d"]))
        self.assertRaisesRegex(
            AssertionError,
            "",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )

    def t08_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", "b", "c", "d", "e", "f"]))
        self.assertRaisesRegex(
            AssertionError,
            "",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 10),),
        )

    def t09_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", b"\x00\x00\x00\x01", 1, 1, b"\x00\x07"]))
        self.database.start_transaction()
        self.assertRaisesRegex(
            StopIteration,
            "$",
            next,
            *(self.database.merge_import(self.field, "file1", "field1", 2),),
        )
        self.database.commit()

    def t10_merge_import(self):
        with open(os.path.join(self.field, "0"), mode="w") as file:
            file.write(repr(["a", b"\x00\x00\x00\x01", 1, 1, b"\x00\x07"]))
            file.write("\n")
            file.write(repr(["b", b"\x00\x00\x00\x01", 1, 1, b"\x00\x07"]))
            file.write("\n")
            file.write(repr(["c", b"\x00\x00\x00\x01", 1, 1, b"\x00\x07"]))
        self.database.start_transaction()
        for count in self.database.merge_import(
            self.field, "file1", "field1", 2
        ):
            self.assertEqual(count, 2)
        self.database.commit()


if sqlite3:

    class _SQLiteduSqlite3(_SQLitedu):
        def setUp(self):
            super().setUp()

            class _D(_sqlitedu.Database, _sqlite.Database):
                def open_database(self, **k):
                    super().open_database(sqlite3, **k)

            self._D = _D

    class Database___init__Sqlite3(_SQLiteduSqlite3):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_open_databaseSqlite3(_SQLiteduSqlite3):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02

    class _SQLiteOpenSqlite3(_SQLiteduSqlite3):
        def setUp(self):
            super().setUp()
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}),
                segment_size_bytes=None,
            )
            self.database.open_database()

        def tearDown(self):
            self.database.close_database()
            super().tearDown()

    class Database_methodsSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_database_cursor
        test_03 = Database_methods.t03_unset_defer_update
        test_05 = Database_methods.t05_new_deferred_root
        test_06 = Database_methods.t06_set_defer_update_01
        test_07 = Database_methods.t07_set_defer_update_02
        test_08 = Database_methods.t08_set_defer_update_03
        test_09 = Database_methods.t09_get_ebm_segment

    class Database__rowsSqlite3(_SQLiteduSqlite3):
        test_01 = Database__rows.t01
        test_02 = Database__rows.t02

    class Database_do_final_segment_deferred_updatesSqlite3(
        _SQLiteOpenSqlite3
    ):
        test_01 = Database_do_final_segment_deferred_updates.t01
        test_02 = Database_do_final_segment_deferred_updates.t02
        test_04 = Database_do_final_segment_deferred_updates.t04
        test_05 = Database_do_final_segment_deferred_updates.t05
        test_06 = Database_do_final_segment_deferred_updates.t06

    class Database_sort_and_writeSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_sort_and_write.t01
        test_02 = Database_sort_and_write.t02
        test_03 = Database_sort_and_write.t03
        test_04 = Database_sort_and_write.t04
        test_05 = Database_sort_and_write.t05
        test_06 = Database_sort_and_write.t06
        test_07 = Database_sort_and_write.t07
        test_08 = Database_sort_and_write.t08
        test_09 = Database_sort_and_write.t09
        test_10 = Database_sort_and_write.t10
        test_11 = Database_sort_and_write.t11
        test_12 = Database_sort_and_write.t12
        test_13 = Database_sort_and_write.t13
        test_14 = Database_sort_and_write.t14

    # merge() does nothing.
    class Database_mergeSqlite3(_SQLiteOpenSqlite3):
        def setUp(self):
            super().setUp()
            if SegmentSize._segment_sort_scale != _segment_sort_scale:
                SegmentSize._segment_sort_scale = _segment_sort_scale

    class Database_delete_indexSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_delete_index.t01
        test_02 = Database_delete_index.t02_delete_index

    class Database_find_value_segmentsSqlite3(_SQLiteOpenSqlite3):
        test_01 = Database_find_value_segments.t01
        test_02 = Database_find_value_segments.t02_find_value_segments
        test_03 = Database_find_value_segments.t03_find_value_segments
        test_04 = Database_find_value_segments.t04_find_value_segments

    class _SQLiteMergeSqlite3(_SQLiteduSqlite3):
        def tearDown(self):
            self.database.close_database()
            shutil.rmtree(self.folder)
            super().tearDown()

    class Database_merge_importSqlite3(_SQLiteMergeSqlite3):
        def setUp(self):
            super().setUp()
            _SQLiteMerge.setup_detail(self)

        test01 = Database_merge_import.t01
        test02 = Database_merge_import.t02_merge_import
        test03 = Database_merge_import.t03_merge_import
        test04 = Database_merge_import.t04_merge_import
        test05 = Database_merge_import.t05_merge_import
        test06 = Database_merge_import.t06_merge_import
        test07 = Database_merge_import.t07_merge_import
        test08 = Database_merge_import.t08_merge_import
        test09 = Database_merge_import.t09_merge_import
        test10 = Database_merge_import.t10_merge_import


if apsw:

    class _SQLiteduApsw(_SQLitedu):
        def setUp(self):
            super().setUp()

            class _D(_sqlitedu.Database, _sqlite.Database):
                def open_database(self, **k):
                    super().open_database(apsw, **k)

            self._D = _D

    class Database___init__Apsw(_SQLiteduApsw):
        test_01 = Database___init__.t01
        test_02 = Database___init__.t02
        test_03 = Database___init__.t03
        test_04 = Database___init__.t04
        test_05 = Database___init__.t05
        test_06 = Database___init__.t06

    class Database_open_databaseApsw(_SQLiteduApsw):
        test_01 = Database_open_database.t01
        test_02 = Database_open_database.t02

    class _SQLiteOpenApsw(_SQLiteduApsw):
        def setUp(self):
            super().setUp()
            self.database = self._D(
                filespec.FileSpec(**{"file1": {"field1"}}),
                segment_size_bytes=None,
            )
            self.database.open_database()

        def tearDown(self):
            self.database.close_database()
            super().tearDown()

    class Database_methodsApsw(_SQLiteOpenApsw):
        test_01 = Database_methods.t01
        test_02 = Database_methods.t02_database_cursor
        test_03 = Database_methods.t03_unset_defer_update
        test_05 = Database_methods.t05_new_deferred_root
        test_06 = Database_methods.t06_set_defer_update_01
        test_07 = Database_methods.t07_set_defer_update_02
        test_08 = Database_methods.t08_set_defer_update_03
        test_09 = Database_methods.t09_get_ebm_segment

    class Database__rowsApsw(_SQLiteduApsw):
        test_01 = Database__rows.t01
        test_02 = Database__rows.t02

    class Database_do_final_segment_deferred_updatesApsw(_SQLiteOpenApsw):
        test_01 = Database_do_final_segment_deferred_updates.t01
        test_02 = Database_do_final_segment_deferred_updates.t02
        test_04 = Database_do_final_segment_deferred_updates.t04
        test_05 = Database_do_final_segment_deferred_updates.t05
        test_06 = Database_do_final_segment_deferred_updates.t06

    class Database_sort_and_writeApsw(_SQLiteOpenApsw):
        test_01 = Database_sort_and_write.t01
        test_02 = Database_sort_and_write.t02
        test_03 = Database_sort_and_write.t03
        test_04 = Database_sort_and_write.t04
        test_05 = Database_sort_and_write.t05
        test_06 = Database_sort_and_write.t06
        test_07 = Database_sort_and_write.t07
        test_08 = Database_sort_and_write.t08
        test_09 = Database_sort_and_write.t09
        test_10 = Database_sort_and_write.t10
        test_11 = Database_sort_and_write.t11
        test_12 = Database_sort_and_write.t12
        test_13 = Database_sort_and_write.t13
        test_14 = Database_sort_and_write.t14

    # merge() does nothing.
    class Database_mergeApsw(_SQLiteOpenApsw):
        def setUp(self):
            super().setUp()
            if SegmentSize._segment_sort_scale != _segment_sort_scale:
                SegmentSize._segment_sort_scale = _segment_sort_scale

    class Database_delete_indexApsw(_SQLiteOpenApsw):
        test_01 = Database_delete_index.t01
        test_02 = Database_delete_index.t02_delete_index

    class Database_find_value_segmentsApsw(_SQLiteOpenApsw):
        test_01 = Database_find_value_segments.t01
        test_02 = Database_find_value_segments.t02_find_value_segments
        test_03 = Database_find_value_segments.t03_find_value_segments
        test_04 = Database_find_value_segments.t04_find_value_segments

    class _SQLiteMergeApsw(_SQLiteduApsw):
        def tearDown(self):
            self.database.close_database()
            shutil.rmtree(self.folder)
            super().tearDown()

    class Database_merge_importApsw(_SQLiteMergeApsw):
        def setUp(self):
            super().setUp()
            _SQLiteMerge.setup_detail(self)

        test01 = Database_merge_import.t01
        test02 = Database_merge_import.t02_merge_import
        test03 = Database_merge_import.t03_merge_import
        test04 = Database_merge_import.t04_merge_import
        test05 = Database_merge_import.t05_merge_import
        test06 = Database_merge_import.t06_merge_import
        test07 = Database_merge_import.t07_merge_import
        test08 = Database_merge_import.t08_merge_import
        test09 = Database_merge_import.t09_merge_import
        test10 = Database_merge_import.t10_merge_import


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if sqlite3:
        runner().run(loader(Database___init__Sqlite3))
        runner().run(loader(Database_open_databaseSqlite3))
        runner().run(loader(Database_methodsSqlite3))
        runner().run(loader(Database__rowsSqlite3))
        runner().run(loader(Database_do_final_segment_deferred_updatesSqlite3))
        runner().run(loader(Database_sort_and_writeSqlite3))
        runner().run(loader(Database_mergeSqlite3))
        runner().run(loader(Database_delete_indexSqlite3))
        runner().run(loader(Database_find_value_segmentsSqlite3))
        runner().run(loader(Database_merge_importSqlite3))
    if apsw:
        runner().run(loader(Database___init__Apsw))
        runner().run(loader(Database_open_databaseApsw))
        runner().run(loader(Database_methodsApsw))
        runner().run(loader(Database__rowsApsw))
        runner().run(loader(Database_do_final_segment_deferred_updatesApsw))
        runner().run(loader(Database_sort_and_writeApsw))
        runner().run(loader(Database_mergeApsw))
        runner().run(loader(Database_delete_indexApsw))
        runner().run(loader(Database_find_value_segmentsApsw))
        runner().run(loader(Database_merge_importApsw))
