# test__database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_database tests"""

import unittest

from .. import _database
from .. import find
from .. import where
from .. import findvalues
from .. import wherevalues
from .. import recordset
from ..segmentsize import SegmentSize
from .. import record
from .. import _db
from .. import _sqlite


class Database_01(unittest.TestCase):
    def setUp(self):
        self.database = _database.Database()
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.segment_size_bytes = 300
        self.database.open_database = lambda files=None: None

    def tearDown(self):
        self.database = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete_instance\(\) missing 2 required positional arguments: ",
                    "'dbset' and 'instance'",
                )
            ),
            self.database.delete_instance,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "edit_instance\(\) missing 2 required positional arguments: ",
                    "'dbset' and 'instance'",
                )
            ),
            self.database.edit_instance,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "put_instance\(\) missing 2 required positional arguments: ",
                    "'dbset' and 'instance'",
                )
            ),
            self.database.put_instance,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "record_finder\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.database.record_finder,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "record_selector\(\) missing 1 required positional argument: ",
                    "'statement'",
                )
            ),
            self.database.record_selector,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "values_finder\(\) missing 1 required positional argument: ",
                    "'dbset'",
                )
            ),
            self.database.values_finder,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "values_selector\(\) missing 1 required positional argument: ",
                    "'statement'",
                )
            ),
            self.database.values_selector,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "make_segment\(\) missing 4 required positional arguments: ",
                    "'key', 'segment_number', 'record_count', and 'records'",
                )
            ),
            self.database.make_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_segment_size\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.database.set_segment_size,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "exists\(\) missing 2 required positional arguments: ",
                    "'file' and 'field'",
                )
            ),
            self.database.exists,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "is_primary\(\) missing 2 required positional arguments: ",
                    "'file' and 'field'",
                )
            ),
            self.database.is_primary,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "is_recno\(\) missing 2 required positional arguments: ",
                    "'file' and 'field'",
                )
            ),
            self.database.is_recno,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "repair_cursor\(\) missing 1 required positional argument: ",
                    "'oldcursor'",
                )
            ),
            self.database.repair_cursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "allocate_and_open_contexts\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.database.allocate_and_open_contexts,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "allocate_and_open_contexts\(\) got an unexpected keyword ",
                    "argument 'xxx'",
                )
            ),
            self.database.allocate_and_open_contexts,
            **dict(xxx=None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "open_database_contexts\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.database.open_database_contexts,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "open_database_contexts\(\) got an unexpected keyword ",
                    "argument 'xxx'",
                )
            ),
            self.database.open_database_contexts,
            **dict(xxx=None),
        )

    def test_record_finder(self):
        self.assertIsInstance(self.database.record_finder("file1"), find.Find)

    def test_record_selector(self):
        self.assertIsInstance(self.database.record_selector(""), where.Where)

    def test_values_finder(self):
        self.assertIsInstance(
            self.database.values_finder("file1"), findvalues.FindValues
        )

    def test_values_selector(self):
        self.assertIsInstance(
            self.database.values_selector(""), wherevalues.WhereValues
        )

    def test_make_segment_01(self):
        self.assertIsInstance(
            self.database.make_segment("", 1, 1, 5),
            recordset.RecordsetSegmentInt,
        )

    def test_make_segment_02(self):
        self.assertIsInstance(
            self.database.make_segment(
                "",
                1,
                SegmentSize.db_segment_size,
                b"\xff" * SegmentSize.db_segment_size_bytes,
            ),
            recordset.RecordsetSegmentBitarray,
        )

    def test_make_segment_03(self):
        self.assertIsInstance(
            self.database.make_segment("", 1, 2, b"\x00\x05\x00\xf0"),
            recordset.RecordsetSegmentList,
        )

    def test_set_segment_size(self):
        self.assertEqual(self.database.set_segment_size(), None)

    def test_exists(self):
        self.assertEqual(self.database.exists("file1", "file1"), True)
        self.assertEqual(self.database.exists("file2", "field1"), False)
        self.assertEqual(self.database.exists("file1", "field1"), True)

    def test_is_primary(self):
        self.assertEqual(self.database.is_primary("file1", "file1"), True)
        self.assertEqual(self.database.is_primary("file1", "field1"), False)

    def test_is_recno(self):
        self.assertEqual(self.database.is_recno("file1", "file1"), True)

    def test_repair_cursor(self):
        class A:
            pass

        a = A()
        self.assertIs(self.database.repair_cursor(a, "ignored"), a)

    def test_allocate_and_open_contexts_01(self):
        self.assertEqual(self.database.allocate_and_open_contexts(), None)

    def test_allocate_and_open_contexts_02(self):
        self.assertEqual(
            self.database.allocate_and_open_contexts(files=("a",)), None
        )

    def test_open_database_contexts_01(self):
        self.assertEqual(self.database.open_database_contexts(), None)

    def test_open_database_contexts_02(self):
        self.assertEqual(
            self.database.open_database_contexts(files=("a",)), None
        )


class Database_02_empty_instance(unittest.TestCase):
    def setUp(self):
        class D(_database.Database):
            def get_high_record(self, dbset):
                return 50000

            def delete(self, dbset, key, srvalue):
                pass

            def put(self, dbset, key, srvalue):
                pass

            def encode_record_number(self, key):
                return key

            def remove_record_from_ebm(self, dbset, key):
                return 0, 400

            def add_record_to_ebm(self, dbset, key):
                return 1, 17233

            def note_freed_record_number_segment(
                self, dbset, segment, record_number, high_record
            ):
                pass

            def get_lowest_freed_record_number(self, dbset):
                return None

        self.database = D()
        self.instance = record.Record()

    def tearDown(self):
        self.database = None

    def test_delete_instance(self):
        self.assertEqual(
            self.database.delete_instance("file1", self.instance), None
        )

    def test_edit_instance(self):
        self.instance.newrecord = record.Record()
        self.instance.key.recno = 10
        self.assertEqual(
            self.database.edit_instance("file1", self.instance), None
        )

    def test_put_instance(self):
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )


class Database_03_delete_instance(unittest.TestCase):
    def setUp(self):
        class R(record.Record):
            def packed_value(self):
                pv = super().packed_value()
                i = pv[1]
                i["field1"] = ["keyvalue1"]
                i["field2"] = ["keyvalue2"]
                i["field3"] = ["keyvalue3"]
                i["field4"] = []
                i["field5"] = []
                i["field6"] = []
                return pv

            def field2_delete_callback(self, srindex_values):
                pass

            def field5_delete_callback(self, srindex_values):
                pass

            deletecallbacks = {}
            deletecallbacks["field2"] = field2_delete_callback
            deletecallbacks["field5"] = field5_delete_callback

        class D(_database.Database):
            def get_high_record(self, dbset):
                return 50000

            def delete(self, dbset, key, srvalue):
                pass

            def encode_record_number(self, key):
                return key

            def remove_record_from_ebm(self, dbset, key):
                return 0, 400

            def remove_record_from_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

            def note_freed_record_number_segment(
                self, dbset, segment, record_number, high_record
            ):
                pass

        self.database = D()
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.specification["file2"] = {"secondary": {"field4": None}}
        self.database.specification["file3"] = {"secondary": {"field7": None}}
        self.instance = R()

    def tearDown(self):
        self.database = None

    def test_delete_instance(self):
        self.assertEqual(
            self.database.delete_instance("file1", self.instance), None
        )


# Why does edit_instance not use get_lowest_freed_record_number?
class Database_04_edit_instance(unittest.TestCase):
    def setUp(self):
        class R(record.Record):
            def packed_value(self):
                pv = super().packed_value()
                pv[1].update(self.indicies)
                return pv

            def field2_delete_callback(self, srindex_values):
                pass

            def field5_delete_callback(self, srindex_values):
                pass

            def fielda_delete_callback(self, srindex_values):
                pass

            deletecallbacks = {}
            deletecallbacks["field2"] = field2_delete_callback
            deletecallbacks["field5"] = field5_delete_callback
            deletecallbacks["fielda"] = fielda_delete_callback

            def field2_put_callback(self, srindex_values):
                pass

            def field5_put_callback(self, srindex_values):
                pass

            def fieldb_put_callback(self, srindex_values):
                pass

            putcallbacks = {}
            putcallbacks["field2"] = field2_put_callback
            putcallbacks["field5"] = field5_put_callback
            putcallbacks["fieldb"] = fieldb_put_callback

        class D(_database.Database):
            def get_high_record(self, dbset):
                return 50000

            def delete(self, dbset, key, srvalue):
                pass

            def put(self, dbset, key, srvalue):
                return 50001

            def replace(self, dbset, key, srvalue, new_srvalue):
                pass

            def encode_record_number(self, key):
                return key

            def remove_record_from_ebm(self, dbset, key):
                return 0, 400

            def add_record_to_ebm(self, dbset, key):
                return 1, 17233

            def remove_record_from_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

            def add_record_to_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

        self.database = D()
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.specification["file2"] = {"secondary": {"field4": None}}
        self.database.specification["file3"] = {"secondary": {"field7": None}}
        self.instance = R()
        self.newinstance = R()
        self.instance.newrecord = self.newinstance
        self.instance.indicies = {
            "field1": ["keyvalue11", "keyvalue12"],
            "field2": ["keyvalue21", "keyvalue22"],
            "field3": ["keyvalue31", "keyvalue32"],
            "field4": [],
            "field5": [],
            "field6": [],
            "fielda": ["keyvaluea1"],
        }
        self.newinstance.indicies = {
            "field1": ["keyvalue13", "keyvalue12"],
            "field2": ["keyvalue23", "keyvalue22"],
            "field3": ["keyvalue33", "keyvalue32"],
            "field4": [],
            "field5": [],
            "field6": [],
            "fieldb": ["keyvalueb1"],
        }

    def tearDown(self):
        self.database = None

    def test_edit_instance_01(self):
        self.instance.key.recno = 10
        self.newinstance.key.recno = 10
        self.assertEqual(
            self.database.edit_instance("file1", self.instance), None
        )

    def test_edit_instance_01(self):
        self.instance.key.recno = 10
        self.newinstance.key.recno = 10
        self.newinstance.value.__dict__["a"] = None
        self.assertEqual(
            self.database.edit_instance("file1", self.instance), None
        )

    def test_edit_instance_03(self):
        self.instance.key.recno = 10
        self.newinstance.key.recno = 11
        self.assertEqual(
            self.database.edit_instance("file1", self.instance), None
        )


class _Database_05_put_instance(unittest.TestCase):
    def setUp(self):
        class R(record.Record):
            def packed_value(self):
                pv = super().packed_value()
                i = pv[1]
                i["field1"] = ["keyvalue1"]
                i["field2"] = ["keyvalue2"]
                i["field3"] = ["keyvalue3"]
                i["field4"] = []
                i["field5"] = []
                i["field6"] = []
                return pv

            def field2_put_callback(self, srindex_values):
                pass

            def field5_put_callback(self, srindex_values):
                pass

            putcallbacks = {}
            putcallbacks["field2"] = field2_put_callback
            putcallbacks["field5"] = field5_put_callback

        class _D(_database.Database):
            def get_high_record(self, dbset):
                return 50000

            def encode_record_number(self, key):
                return key

            def add_record_to_ebm(self, dbset, key):
                return 1, 17233

            def add_record_to_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

        self._D = _D
        self.R = R

    def tearDown(self):
        self.database = None

    def create_specification_and_instance(self):
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.specification["file2"] = {"secondary": {"field4": None}}
        self.database.specification["file3"] = {"secondary": {"field7": None}}
        self.instance = self.R()


# Append record at end.
class Database_05_put_instance_01(_Database_05_put_instance):
    def setUp(self):
        super().setUp()

        class D(self._D):
            def put(self, dbset, key, srvalue):
                return None

            def get_lowest_freed_record_number(self, dbset):
                return None

        self.database = D()
        self.create_specification_and_instance()

    def test_put_instance(self):
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )


# Reuse record number.
class Database_05_put_instance_02(_Database_05_put_instance):
    def setUp(self):
        super().setUp()

        class D(self._D):
            def put(self, dbset, key, srvalue):
                return 20

            def get_lowest_freed_record_number(self, dbset):
                return 20

        self.database = D()
        self.create_specification_and_instance()

    def test_put_instance(self):
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )


# Overwrite record (put_instance is told what recno to use).
class Database_05_put_instance_03(_Database_05_put_instance):
    def setUp(self):
        super().setUp()

        class D(self._D):
            def put(self, dbset, key, srvalue):
                return 5

        self.database = D()
        self.create_specification_and_instance()
        self.instance.key.recno = 5

    def test_put_instance(self):
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )


class Database_06_subclass_methods(unittest.TestCase):
    def setUp(self):
        class D(_database.Database):
            # Define stub methods expected in _db.Database and _sqlite.Database,
            # but only test is existence by comparison of dir()s.
            def get_high_record(self, dbset):
                return 50000

            def delete(self, dbset, key, srvalue):
                pass

            def put(self, dbset, key, srvalue):
                return 50001

            def replace(self, dbset, key, srvalue, new_srvalue):
                pass

            def encode_record_number(self, key):
                return key

            def remove_record_from_ebm(self, dbset, key):
                return 0, 400

            def add_record_to_ebm(self, dbset, key):
                return 1, 17233

            def remove_record_from_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

            def add_record_to_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

            def note_freed_record_number_segment(
                self, dbset, segment, record_number, high_record
            ):
                pass

            def get_lowest_freed_record_number(self, dbset):
                return None

            def _validate_segment_size_bytes(self, segment_size_bytes):
                pass

            def start_transaction(self):
                pass

            def backout(self):
                pass

            def commit(self):
                pass

            def open_database(self, dbe, files=None):
                pass

            def close_database_contexts(self, files=None):
                pass

            def close_database(self):
                pass

            def get_primary_record(self, file, key):
                pass

            def decode_record_number(self, skey):
                pass

            def encode_record_selector(self, key):
                pass

            def populate_segment(self, segment_reference, file):
                pass

            def find_values(self, valuespec, file):
                pass

            def recordlist_record_number(self, file, key=None, cache_size=1):
                pass

            def recordlist_record_number_range(
                self, file, keystart=None, keyend=None, cache_size=1
            ):
                pass

            def recordlist_ebm(self, file, cache_size=1):
                pass

            def recordlist_key_like(
                self, file, field, keylike=None, cache_size=1
            ):
                pass

            def recordlist_key(self, file, field, key=None, cache_size=1):
                pass

            def recordlist_key_startswith(
                self, file, field, keystart=None, cache_size=1
            ):
                pass

            def recordlist_key_range(
                self,
                file,
                field,
                ge=None,
                gt=None,
                le=None,
                lt=None,
                cache_size=1,
            ):
                pass

            def recordlist_all(self, file, field, cache_size=1):
                pass

            def recordlist_nil(self, file, cache_size=1):
                pass

            def unfile_records_under(self, file, field, key):
                pass

            def file_records_under(self, file, field, recordset, key):
                pass

            def database_cursor(self, file, field, keyrange=None):
                pass

            def create_recordset_cursor(self, recordset):
                pass

            def is_database_file_active(self, file):
                pass

            def get_table_connection(self, file):
                pass

            def do_database_task(
                self,
                taskmethod,
                logwidget=None,
                taskmethodargs={},
                use_specification_items=None,
            ):
                pass

        self.D = D

    def tearDown(self):
        self.D = None

    def test__db_methods(self):
        dm = set(dir(_db.Database))
        for m in dir(self.D):
            self.assertEqual(m in dm, True)
        dm -= set(dir(self.D))
        self.assertEqual(
            sorted(dm),
            sorted(
                (
                    "populate_recordset_segment",
                    "environment_flags",
                    "_get_segment_record_numbers",
                    "file_name_for_database",
                    "checkpoint_before_close_dbenv",
                    "SegmentSizeError",
                )
            ),
        )

    def test__sqlite_methods(self):
        dm = set(dir(_sqlite.Database))
        for m in dir(self.D):
            self.assertEqual(m in dm, True)
        dm -= set(dir(self.D))
        self.assertEqual(
            sorted(dm),
            sorted(
                (
                    "delete_segment_records",
                    "get_segment_records",
                    "set_segment_records",
                    "insert_segment_records",
                    "SegmentSizeError",
                )
            ),
        )


class ExistenceBitmapControl(unittest.TestCase):
    def setUp(self):
        class D(_database.Database):
            def encode_record_selector(self, file):
                return "E" + file

        self.database = D()

    def tearDown(self):
        pass

    def test___init__01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'file' and 'database'",
                )
            ),
            _database.ExistenceBitmapControl,
        )

    def test___init__02(self):
        rrnc = _database.ExistenceBitmapControl("file", self.database)
        self.assertIsInstance(rrnc, _database.ExistenceBitmapControl)

    def test_segment_count(self):
        rrnc = _database.ExistenceBitmapControl("file", self.database)
        self.assertEqual(rrnc._segment_count, None)
        rrnc._segment_count = 0
        rrnc.segment_count = 3
        sc = rrnc.segment_count
        self.assertEqual(sc, 4)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Database_01))
    runner().run(loader(Database_02_empty_instance))
    runner().run(loader(Database_03_delete_instance))
    runner().run(loader(Database_04_edit_instance))
    runner().run(loader(Database_05_put_instance_01))
    runner().run(loader(Database_05_put_instance_02))
    runner().run(loader(Database_05_put_instance_03))
    runner().run(loader(Database_06_subclass_methods))
    runner().run(loader(ExistenceBitmapControl))
