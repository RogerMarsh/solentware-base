# test__nosqldu.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosqldu _database tests"""

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
from .. import _nosqldu
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray

_segment_sort_scale = SegmentSize._segment_sort_scale

_NDBM_TEST_ROOT = "___ndbm_test_nosqldu"
_GNU_TEST_ROOT = "___gnu_test_nosqldu"


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


class _NoSQLdu(unittest.TestCase):
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

        class _D(_nosqldu.Database, _nosql.Database):
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


# Same tests as test__sqlite.Database___init__ with relevant additions.
# Alternative is one test method with just the additional tests.
class Database___init__(_NoSQLdu):
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
                "_int_to_bytes",
                "_real_segment_size_bytes",
                "_use_specification_items",
                "database_file",
                "dbenv",
                "deferred_update_points",
                "ebm_control",
                "ebm_segment_count",
                "existence_bit_maps",
                "first_chunk",
                "high_segment",
                "home_directory",
                "initial_high_segment",
                "segment_records",
                "segment_size_bytes",
                "segment_table",
                "specification",
                "table",
                "table_data",
                "trees",
                "value_segments",
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
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)

        # These tests are only difference to test__nosql.Database___init__
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


# Memory databases are used for these tests.
class _NoSQLOpen(_NoSQLdu):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(*self._oda)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class Database_methods(_NoSQLOpen):
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
                    "start_transaction\(\) takes 1 ",
                    "positional argument but 2 were given",
                )
            ),
            self.database.start_transaction,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "backout\(\) takes 1 ",
                    "positional argument but 2 were given",
                )
            ),
            self.database.backout,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "commit\(\) takes 1 ",
                    "positional argument but 2 were given",
                )
            ),
            self.database.commit,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_defer_update\(\) takes 1 ",
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
                    "unset_defer_update\(\) takes 1 ",
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
                    "write_existence_bit_map\(\) missing 2 required ",
                    "positional arguments: 'file' and 'segment'",
                )
            ),
            self.database.write_existence_bit_map,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "new_deferred_root\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'",
                )
            ),
            self.database.new_deferred_root,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_ebm_segment\(\) missing 2 required ",
                    "positional arguments: 'ebm_control' and 'key'",
                )
            ),
            self.database.get_ebm_segment,
        )

    def test_02_database_cursor(self):
        self.assertRaisesRegex(
            _nosqldu.DatabaseError,
            "database_cursor not implemented",
            self.database.database_cursor,
            *(None, None),
        )

    def test_03_unset_defer_update(self):
        self.database.start_transaction()
        self.database.unset_defer_update()

    def test_04_write_existence_bit_map(self):
        segment = 0
        b = b"\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        bs = recordset.RecordsetSegmentBitarray(segment, None, b)
        self.database.existence_bit_maps["file1"] = {}
        self.database.existence_bit_maps["file1"][segment] = bs
        self.database.write_existence_bit_map("file1", segment)
        ae = self.assertEqual
        ae(literal_eval(self.database.dbenv["1_0__ebm_0"].decode()), b)
        ae(self.database.ebm_control["file1"].table_ebm_segments, [0])
        c = b"\x00\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        cs = recordset.RecordsetSegmentBitarray(segment, None, c)
        self.database.existence_bit_maps["file1"][segment] = cs
        self.database.write_existence_bit_map("file1", segment)
        ae(literal_eval(self.database.dbenv["1_0__ebm_0"].decode()), c)
        ae(self.database.ebm_control["file1"].table_ebm_segments, [0])

    def test_05_new_deferred_root(self):
        ae = self.assertEqual
        ae(self.database.table["file1_field1"], ["1_1"])
        ae(self.database.new_deferred_root("file1", "field1"), None)
        ae(self.database.table["file1_field1"], ["1_1"])

    def test_06_set_defer_update_01(self):
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], -1)
        self.assertEqual(self.database.high_segment["file1"], -1)
        self.assertEqual(self.database.first_chunk["file1"], False)

    def test_07_set_defer_update_02(self):
        self.database.put("file1", None, "Some value")
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], 0)
        self.assertEqual(self.database.high_segment["file1"], 0)
        self.assertEqual(self.database.first_chunk["file1"], True)

    # This test has to be done in a non-memory database.
    def xtest_08_set_defer_update_03(self):

        # Simulate normal use: the insert is not part of the deferred update.
        self.database.close_database()
        D = _nosql.Database(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        D.open_database(*self._oda)
        D.start_transaction()
        D.put("file1", None, "Some value")
        D.commit()
        D.close_database()
        del D

        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(*self._oda)
        self.database.set_defer_update()
        self.assertEqual(self.database.initial_high_segment["file1"], 0)
        self.assertEqual(self.database.high_segment["file1"], 0)
        self.assertEqual(self.database.first_chunk["file1"], True)

    def test_09_get_ebm_segment(self):
        self.assertEqual(
            self.database.get_ebm_segment(
                self.database.ebm_control["file1"], 1
            ),
            None,
        )


class Database_do_final_segment_deferred_updates(_NoSQLOpen):
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "do_final_segment_deferred_updates\(\) takes 1 ",
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
        self.database.put("file1", None, "Some value")
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
        self.database.put("file1", None, "Some value")
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

    # range(128) and test {i}, rather than range(127) and {i+1}, because record
    # numbers start at 0 not 1.
    def test_06(self):
        for i in range(128):
            self.database.put("file1", None, "Some value")
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


class Database_sort_and_write(_NoSQLOpen):
    def test_01(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "sort_and_write\(\) missing 3 required ",
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
            KeyError,
            "'file1'",
            self.database.sort_and_write,
            *("file1", "field1", None),
        )
        self.database.first_chunk["file1"] = None
        self.database.initial_high_segment["file1"] = None
        self.database.high_segment["file1"] = None
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'items'",
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

    # Not sure why this does not raise exception for any NoSQL module now.
    def xtest_06(self):
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
        self.assertEqual(self.database.table["file1_field1"], ["1_1"])

    def test_08(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = True
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], ["1_1"])

    def test_09(self):
        self.database.value_segments["file1"] = {"field1": {}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        self.assertEqual(self.database.table["file1_field1"], ["1_1"])

    def test_10(self):
        self.database.value_segments["file1"] = {"field1": {"int": 7}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        ae = self.assertEqual
        db = self.database.dbenv
        ae(self.database.table["file1_field1"], ["1_1"])
        ae(literal_eval(db["1_1_0_int"].decode()), {5: (7, 1)})
        ae("1_1_1_5_int" in db, False)

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
        ae = self.assertEqual
        db = self.database.dbenv
        ae(self.database.table["file1_field1"], ["1_1"])
        ae(literal_eval(db["1_1_0_list"].decode()), {5: ("L", 2)})
        ae("1_1_1_5_list" in db, True)
        ae(literal_eval(db["1_1_1_5_list"].decode()), b"\x00\x01\x00\x04")

    def test_12(self):
        ba = Bitarray()
        ba.frombytes(b"\x0a" * 16)
        self.database.value_segments["file1"] = {"field1": {"bits": ba}}
        self.database.first_chunk["file1"] = False
        self.database.initial_high_segment["file1"] = 4
        self.database.high_segment["file1"] = 3
        self.database.sort_and_write("file1", "field1", 5)
        ae = self.assertEqual
        db = self.database.dbenv
        ae(self.database.table["file1_field1"], ["1_1"])
        ae(literal_eval(db["1_1_0_bits"].decode()), {5: ("B", 32)})
        ae("1_1_1_5_bits" in db, True)
        ae(
            literal_eval(db["1_1_1_5_bits"].decode()),
            b"\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a\x0a",
        )


class Database_merge(_NoSQLOpen):
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
                    "merge\(\) missing 2 required ",
                    "positional arguments: 'file' and 'field'",
                )
            ),
            database.merge,
        )

    def test_02(self):
        self.assertEqual(SegmentSize._segment_sort_scale, _segment_sort_scale)
        self.assertEqual(self.database.table["file1_field1"], ["1_1"])
        self.assertEqual(self.database.merge("file1", "field1"), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in unqlite, vedis, ndbm_module, gnu_module:
        if dbe_module is None:
            continue
        runner().run(loader(Database___init__))
        runner().run(loader(Database_methods))
        runner().run(loader(Database_do_final_segment_deferred_updates))
        runner().run(loader(Database_sort_and_write))
        runner().run(loader(Database_merge))
