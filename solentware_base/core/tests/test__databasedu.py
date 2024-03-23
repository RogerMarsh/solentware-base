# test__databasedu.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_database tests"""

import unittest

from .. import _databasedu
from ..segmentsize import SegmentSize
from .. import record
from .. import _bytebit


class _Database_put_instance(unittest.TestCase):
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

        class _D(_databasedu.Database):
            def encode_record_number(self, key):
                return key

            def _defer_add_record_to_ebm(self, dbset, segment, key):
                return 1, 17233

            def _defer_add_record_to_field_value(
                self, dbset, secondary, v, segment, record_number
            ):
                pass

            def _write_existence_bit_map(self, dbset, segment):
                return None

            def sort_and_write(self, dbset, secondary, segment):
                return None

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


class Database_put_instance_01(_Database_put_instance):
    def setUp(self):
        super().setUp()

        class D(self._D):
            def put(self, dbset, key, srvalue):
                return 20

        self.database = D()
        self.create_specification_and_instance()

    def test_put_instance_01(self):
        self.database.deferred_update_points = {19}
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )

    def test_put_instance_02(self):
        self.database.first_chunk = {}
        self.database.deferred_update_points = {10, 20}
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )

    def test_put_instance_03(self):
        self.database.first_chunk = {}
        self.database.high_segment = {}
        self.database.deferred_update_points = {40, 20}
        self.assertEqual(
            self.database.put_instance("file1", self.instance), None
        )


class Database_put_instance_02(_Database_put_instance):
    def setUp(self):
        super().setUp()

        class D(self._D):
            pass

        self.database = D()
        self.create_specification_and_instance()
        self.instance.key.recno = 5

    def test_put_instance(self):
        self.assertRaisesRegex(
            _databasedu.DatabaseduError,
            "".join((r"Cannot reuse record number in deferred update\.$",)),
            self.database.put_instance,
            *("file1", self.instance),
        )


class Database_defer_add_record_to_ebm(unittest.TestCase):
    def setUp(self):
        class EBMC:
            def get_ebm_segment(self, *a):
                return b"\x7f" + b"\x00" * (
                    SegmentSize.db_segment_size_bytes - 1
                )

        self.EBMC = EBMC

        class _D(_databasedu.Database):
            def get_ebm_segment(self, *a):
                return self.ebm_control["file1"].get_ebm_segment()

        self.database = _D()
        self.database.specification = {}
        self.database.specification["file1"] = {}
        self.database.existence_bit_maps = {}
        self.database.ebm_control = {}
        self.database.ebm_control["file1"] = {}
        self.database.dbenv = None

    def tearDown(self):
        self.database = None

    def test_defer_add_record_to_ebm_02(self):
        self.database.ebm_control["file1"] = self.EBMC()
        self.assertEqual(len(self.database.existence_bit_maps), 0)
        self.assertEqual(
            self.database._defer_add_record_to_ebm("file1", 1, 20), None
        )
        v = self.database.existence_bit_maps["file1"]
        self.assertIsInstance(v[1], _bytebit.Bitarray)
        self.assertEqual(
            self.database._defer_add_record_to_ebm("file1", 1, 20), None
        )
        self.assertIsInstance(v[1], _bytebit.Bitarray)


class Database_defer_add_record_to_field_value(unittest.TestCase):
    def setUp(self):
        class _D(_databasedu.Database):
            pass

        self.database = _D()
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.value_segments = {}

    def tearDown(self):
        self.database = None

    def test_defer_add_record_to_field_value_01(self):
        self.assertEqual(len(self.database.value_segments), 0)
        self.assertEqual(
            self.database._defer_add_record_to_field_value(
                "file1", "field1", "v1", 1, 20
            ),
            None,
        )
        v = self.database.value_segments["file1"]["field1"]
        self.assertIsInstance(v["v1"], list)
        self.assertEqual(
            self.database._defer_add_record_to_field_value(
                "file1", "field1", "v1", 1, 30
            ),
            None,
        )
        self.assertIsInstance(v["v1"], list)
        self.assertEqual(
            self.database._defer_add_record_to_field_value(
                "file1", "field1", "v1", 1, 3500
            ),
            None,
        )
        self.assertIsInstance(v["v1"], list)

    def test_defer_add_record_to_field_value_02(self):
        self.assertEqual(len(self.database.value_segments), 0)
        for i in range(SegmentSize.db_upper_conversion_limit):
            self.assertEqual(
                self.database._defer_add_record_to_field_value(
                    "file1", "field1", "v1", 1, i
                ),
                None,
            )
        v = self.database.value_segments["file1"]["field1"]
        self.assertIsInstance(v["v1"], list)
        self.assertEqual(
            self.database._defer_add_record_to_field_value(
                "file1", "field1", "v1", 1, 3500
            ),
            None,
        )
        self.assertIsInstance(v["v1"], _bytebit.Bitarray)
        self.assertEqual(
            self.database._defer_add_record_to_field_value(
                "file1", "field1", "v1", 1, 4000
            ),
            None,
        )
        self.assertIsInstance(v["v1"], _bytebit.Bitarray)


class Database__prepare_segment_record_list(unittest.TestCase):
    def setUp(self):
        class _D(_databasedu.Database):
            def __init__(self):
                self.value_segments = {"file": {"field": {"index": []}}}
                self._int_to_bytes = [
                    n.to_bytes(2, byteorder="big") for n in range(8)
                ]

            def _grl(self):
                return self.value_segments["file"]["field"]["index"]

            def _vsff(self):
                return self.value_segments["file"]["field"]

        self.database = _D()

    def tearDown(self):
        self.database = None

    def test__prepare_segment_record_list_01(self):
        database = self.database
        database._grl().append(4)
        database._prepare_segment_record_list("file", "field")
        self.assertEqual(database._grl(), [1, 4])

    def test__prepare_segment_record_list_02(self):
        database = self.database
        database._grl().extend([4, 7])
        database._prepare_segment_record_list("file", "field")
        self.assertEqual(database._grl(), [2, b"\x00\x04\x00\x07"])

    def test__prepare_segment_record_list_03(self):
        database = self.database
        database._vsff()["index"] = _bytebit.Bitarray(8)
        database._grl()[0] = True
        database._grl()[6] = True
        database._prepare_segment_record_list("file", "field")
        self.assertEqual(database._grl(), [2, b"\x82"])

    def test__prepare_segment_record_list_04(self):
        database = self.database
        database._vsff()["index"] = _bytebit.Bitarray(8)
        database._grl()[0] = True
        database._grl()[6] = True
        database._grl()[3] = True
        database._prepare_segment_record_list("file", "field")
        self.assertEqual(database._grl(), [3, b"\x92"])

    def test__prepare_segment_record_list_05(self):
        database = self.database
        database._vsff()["field"] = 10
        self.assertRaisesRegex(
            AttributeError,
            "".join((r"'int' object has no attribute 'count'$",)),
            database._prepare_segment_record_list,
            *("file", "field"),
        )

    def test__prepare_segment_record_list_06(self):
        # What happens: but will break somewhere.
        database = self.database
        database._vsff()["field"] = []
        database._prepare_segment_record_list("file", "field")
        self.assertEqual(database._grl(), [0, b""])


class Database_set_segment_size(unittest.TestCase):
    def setUp(self):
        class _D(_databasedu.Database):
            def set_segment_size(self):
                pass

        self.database = _D()
        self.database.specification = {}
        self.database.specification["file1"] = {"secondary": {"field1": None}}
        self.database.value_segments = {}

    def tearDown(self):
        self.database = None

    def test_set_segment_size(self):
        self.assertEqual(self.database.set_segment_size(), None)


class Database_deferred_update_housekeeping(unittest.TestCase):
    def setUp(self):
        class _D(_databasedu.Database):
            pass

        self.database = _D()

    def tearDown(self):
        self.database = None

    def test_deferred_update_housekeeping(self):
        self.assertEqual(self.database.deferred_update_housekeeping(), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Database_put_instance_01))
    runner().run(loader(Database_put_instance_02))
    runner().run(loader(Database_defer_add_record_to_ebm))
    runner().run(loader(Database_defer_add_record_to_field_value))
    runner().run(loader(Database__prepare_segment_record_list))
    runner().run(loader(Database_set_segment_size))
    runner().run(loader(Database_deferred_update_housekeeping))
