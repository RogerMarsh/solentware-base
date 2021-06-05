# test__db_cursor.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_db cursor tests"""

import unittest

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


class _DB(unittest.TestCase):
    def setUp(self):
        class _D(_db.Database):
            pass

        self._D = _D
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(dbe_module.db)

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None


class Cursor_db(_DB):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 2 to 4 positional arguments ",
                    "but 5 were given",
                )
            ),
            _db.Cursor,
            *(None, None, None, None),
        )
        cursor = _db.Cursor(self.database.table["file1"][0])

        # Superclass of _db.Cursor defines close().
        # Confirm self.database.table['file1'][0] object has close() method.
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("close\(\) takes 1 positional argument but 2 were given",)
            ),
            cursor.close,
            *(None,),
        )

        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_converted_partial\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            cursor.get_converted_partial,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_partial_with_wildcard\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            cursor.get_partial_with_wildcard,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_converted_partial_with_wildcard\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            cursor.get_converted_partial_with_wildcard,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            cursor.refresh_recordset,
            *(None, None),
        )

    def test_02___init__(self):
        cursor = _db.Cursor(self.database.table["file1"][0])
        self.assertEqual(cursor._transaction, None)
        self.assertEqual(cursor._current_segment, None)
        self.assertEqual(cursor.current_segment_number, None)
        self.assertEqual(cursor._current_record_number_in_segment, None)
        self.assertIsInstance(
            cursor._cursor, self.database.table["file1"][0].cursor().__class__
        )

    def test_03_close(self):

        # Superclass of _db.Cursor defines close().
        # Confirm self.database.table['file1'][0] object has close() method.
        cursor = _db.Cursor(self.database.table["file1"][0])
        cursor.close()
        self.assertEqual(cursor._cursor, None)

    def test_04_get_converted_partial(self):
        cursor = _db.Cursor(self.database.table["file1"][0])
        cursor._partial = ""
        self.assertEqual(cursor.get_converted_partial(), b"")

    def test_05_get_partial_with_wildcard(self):
        cursor = _db.Cursor(self.database.table["file1"][0])
        self.assertRaisesRegex(
            _db.DatabaseError,
            "get_partial_with_wildcard not implemented",
            cursor.get_partial_with_wildcard,
        )

    def test_06_get_converted_partial_with_wildcard(self):
        cursor = _db.Cursor(self.database.table["file1"][0])
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'encode'",
            cursor.get_converted_partial_with_wildcard,
        )
        cursor._partial = "part"
        self.assertEqual(cursor.get_converted_partial_with_wildcard(), b"part")

    def test_07_refresh_recordset(self):
        cursor = _db.Cursor(self.database.table["file1"][0])
        self.assertEqual(cursor.refresh_recordset(), None)


class Cursor_primary(_DB):
    def setUp(self):
        super().setUp()
        self.cursor = _db.CursorPrimary(
            self.database.table["file1"][0],
            ebm=self.database.ebm_control["file1"].ebm_table,
            engine=dbe_module.db,
        )

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 2 to 4 positional arguments ",
                    "but 5 were given",
                )
            ),
            _db.CursorPrimary,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "count_records\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_position_of_record,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_at_position\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_record_at_position,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "nearest\(\) missing 1 required ",
                    "positional argument: 'key'",
                )
            ),
            self.cursor.nearest,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "prev\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "setat\(\) missing 1 required ",
                    "positional argument: 'record'",
                )
            ),
            self.cursor.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.cursor.refresh_recordset,
            *(None, None),
        )

    def test_02___init__(self):
        self.assertEqual(self.cursor._ebm.__class__.__name__, "DB")

    def test_03_count_records(self):
        self.assertEqual(self.cursor.count_records(), 0)

    def test_04_first(self):
        self.assertEqual(self.cursor.first(), None)

    def test_05_get_position_of_record_01(self):
        self.assertEqual(self.cursor.get_position_of_record(), 0)

    def test_06_get_position_of_record_02(self):
        self.assertEqual(self.cursor.get_position_of_record((5, None)), 0)

    def test_07_get_position_of_record_03(self):
        # With a populated database.
        pass

    def test_08_get_record_at_position_01(self):
        self.assertEqual(self.cursor.get_record_at_position(), None)

    def test_09_get_record_at_position_02(self):
        self.assertEqual(self.cursor.get_record_at_position(-1), None)

    def test_10_get_record_at_position_03(self):
        self.assertEqual(self.cursor.get_record_at_position(0), None)

    def test_11_last(self):
        self.assertEqual(self.cursor.last(), None)

    def test_12_nearest(self):
        self.assertEqual(self.cursor.nearest(12), None)

    def test_13_next(self):
        self.assertEqual(self.cursor.next(), None)

    def test_14_prev(self):
        self.assertEqual(self.cursor.prev(), None)

    def test_15_setat(self):
        self.assertEqual(self.cursor.setat((10, None)), None)

    def test_16_refresh_recordset(self):
        self.cursor.refresh_recordset()


class Cursor_secondary(_DB):
    def setUp(self):
        super().setUp()
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
        for s in segments:
            self.segments[self.database.segment_table["file1"].append(s)] = s
        self.database.start_transaction()
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                cursor.put(
                    k.encode(),
                    b"".join(
                        (
                            b"\x00\x00\x00\x00",
                            int(24 if e else 31).to_bytes(2, byteorder="big"),
                            self.keyvalues[k].to_bytes(4, byteorder="big"),
                        )
                    ),
                    dbe_module.db.DB_KEYLAST,
                )
            cursor.put(
                "ba_o".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x01",
                        int(2).to_bytes(2, byteorder="big"),
                        int(8).to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            self.keyvalues["twy"] = 9
            cursor.put(
                "twy".encode(),  #'cep'
                b"".join(
                    (
                        b"\x00\x00\x00\x01",
                        int(3).to_bytes(2, byteorder="big"),
                        int(9).to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "ba_o".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x02",
                        int(50).to_bytes(2, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "cep".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x02",
                        int(100).to_bytes(2, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
        finally:
            cursor.close()
        self.cursor = _db.CursorSecondary(
            self.database.table["file1_field1"][0],
            segment=self.database.segment_table["file1"],
            transaction=self.database.dbtxn,
        )

    def tearDown(self):
        self.cursor.close()
        self.database.commit()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            _db.CursorSecondary,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "count_records\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_position_of_record,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_at_position\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_record_at_position,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "nearest\(\) missing 1 required ",
                    "positional argument: 'key'",
                )
            ),
            self.cursor.nearest,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "prev\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.cursor.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "setat\(\) missing 1 required ",
                    "positional argument: 'record'",
                )
            ),
            self.cursor.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_partial_key\(\) missing 1 required ",
                    "positional argument: 'partial'",
                )
            ),
            self.cursor.set_partial_key,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_get_segment\(\) missing 3 required ",
                    "positional arguments: 'key', 'segment_number', ",
                    "and 'reference'",
                )
            ),
            self.cursor._get_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_current_segment\(\) missing 2 required ",
                    "positional arguments: 'key' and 'reference'",
                )
            ),
            self.cursor.set_current_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.cursor.refresh_recordset,
            *(None, None),
        )

    def test_02___init__(self):
        self.assertEqual(self.cursor._segment.__class__.__name__, "DB")

    def test_05_count_records_01(self):
        s = self.cursor.count_records()
        self.assertEqual(s, 182)

    def test_06_count_records_02(self):
        self.cursor._partial = "b"
        s = self.cursor.count_records()
        self.assertEqual(s, 51)

    def test_07_first_01(self):
        self.cursor._partial = False
        s = self.cursor.first()
        self.assertEqual(s, None)

    def test_08_first_02(self):
        s = self.cursor.first()
        self.assertEqual(s, ("a_o", 1))

    def test_09_first_03(self):
        self.cursor._partial = "b"
        s = self.cursor.first()
        self.assertEqual(s, ("ba_o", 40))

    def test_10_first_04(self):
        self.cursor._partial = "A"
        s = self.cursor.first()
        self.assertEqual(s, None)

    def test_11_get_position_of_record_01(self):
        s = self.cursor.get_position_of_record()
        self.assertEqual(s, 0)

    # Tests 12, 13, and 14, drive method through all record paths in the loop
    # on 'cursor.execute(...)' between them.  Tried arguments till set that did
    # so was found.

    def test_12_get_position_of_record_02(self):
        s = self.cursor.get_position_of_record(("ba_o", 300))
        self.assertEqual(s, 81)

    def test_13_get_position_of_record_03(self):
        self.cursor._partial = "b"
        s = self.cursor.get_position_of_record(("bb_o", 20))
        self.assertEqual(s, 27)

    def test_14_get_position_of_record_04(self):
        s = self.cursor.get_position_of_record(("cep", 150))
        self.assertEqual(s, 154)

    def test_30_last_01(self):
        self.cursor._partial = False
        s = self.cursor.last()
        self.assertEqual(s, None)

    def test_31_last_02(self):
        s = self.cursor.last()
        self.assertEqual(s, ("twy", 196))  # ('deq', 127))

    def test_32_last_03(self):
        ae = self.assertEqual
        last = self.cursor.last
        self.cursor._partial = "c"
        ae(last(), ("cep", 356))
        self.cursor._partial = "d"
        ae(last(), ("deq", 127))
        self.cursor._partial = "b"
        ae(last(), ("bb_o", 79))

    def test_33_last_04(self):
        self.cursor._partial = "A"
        s = self.cursor.last()
        self.assertEqual(s, None)

    def test_34_nearest_01(self):
        self.assertEqual(self.cursor.nearest(b"d"), ("deq", 104))

    def test_35_nearest_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.nearest(b"d"), None)

    def test_36_nearest_03(self):
        self.cursor._partial = "b"
        self.assertEqual(self.cursor.nearest(b"bb"), ("bb_o", 56))

    def test_37_nearest_04(self):
        self.assertEqual(self.cursor.nearest(b"z"), None)

    def test_38_next_01(self):
        ae = self.assertEqual
        next_ = self.cursor.next
        for i in range(1, 32):
            ae(next_(), ("a_o", i))
        for i in range(24, 48):
            ae(next_(), ("aa_o", i))
        for i in range(40, 64):
            ae(next_(), ("ba_o", i))
        for i in range(192, 194):
            ae(next_(), ("ba_o", i))
        ae(next_(), ("ba_o", 306))
        for i in range(56, 80):
            ae(next_(), ("bb_o", i))
        for i in range(
            72,
            96,
        ):
            ae(next_(), ("c_o", i))
        for i in range(88, 112):
            ae(next_(), ("cep", i))
        ae(next_(), ("cep", 356))
        for i in range(104, 128):
            ae(next_(), ("deq", i))
        for i in range(194, 197):
            ae(next_(), ("twy", i))
        ae(next_(), None)

    def test_39_next_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.next(), None)

    def test_40_next_03(self):
        ae = self.assertEqual
        next_ = self.cursor.next
        ae(next_(), ("a_o", 1))
        self.cursor._partial = False
        ae(next_(), None)
        self.cursor._partial = None
        ae(next_(), ("a_o", 2))
        self.assertNotEqual(self.cursor._current_segment, None)
        self.assertNotEqual(self.cursor.current_segment_number, None)
        self.cursor._partial = "bb"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        ae(next_(), ("bb_o", 56))

    def test_41_next_04(self):
        ae = self.assertEqual
        next_ = self.cursor.next
        ae(self.cursor._current_segment, None)
        ae(self.cursor.current_segment_number, None)
        self.cursor._partial = "c"
        for i in range(72, 96):
            ae(next_(), ("c_o", i))
        for i in range(88, 112):
            ae(next_(), ("cep", i))
        ae(next_(), ("cep", 356))
        self.assertNotEqual(self.cursor._current_segment, None)
        self.assertNotEqual(self.cursor.current_segment_number, None)
        self.cursor._partial = "d"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        for i in range(104, 128):
            ae(next_(), ("deq", i))
        ae(next_(), None)
        self.cursor._partial = "b"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        ae(next_(), ("ba_o", 40))

    def test_44_prev_01(self):
        ae = self.assertEqual
        prev = self.cursor.prev
        for i in range(196, 193, -1):
            ae(prev(), ("twy", i))
        for i in range(127, 103, -1):
            ae(prev(), ("deq", i))
        ae(prev(), ("cep", 356))
        for i in range(111, 87, -1):
            ae(prev(), ("cep", i))
        for i in range(95, 71, -1):
            ae(prev(), ("c_o", i))
        for i in range(79, 55, -1):
            ae(prev(), ("bb_o", i))
        ae(prev(), ("ba_o", 306))
        for i in range(193, 191, -1):
            ae(prev(), ("ba_o", i))
        for i in range(63, 39, -1):
            ae(prev(), ("ba_o", i))
        for i in range(47, 23, -1):
            ae(prev(), ("aa_o", i))
        for i in range(31, 0, -1):
            ae(prev(), ("a_o", i))
        ae(prev(), None)

    def test_45_prev_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.prev(), None)

    def test_46_prev_05(self):
        ae = self.assertEqual
        prev = self.cursor.prev
        ae(prev(), ("twy", 196))
        self.cursor._partial = False
        ae(prev(), None)
        self.cursor._partial = None
        ae(prev(), ("twy", 195))
        self.assertNotEqual(self.cursor._current_segment, None)
        self.assertNotEqual(self.cursor.current_segment_number, None)
        self.cursor._partial = "a"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        ae(prev(), ("aa_o", 47))

    def test_47_prev_06(self):
        ae = self.assertEqual
        prev = self.cursor.prev
        ae(self.cursor._current_segment, None)
        ae(self.cursor.current_segment_number, None)
        self.cursor._partial = "c"
        ae(prev(), ("cep", 356))
        for i in range(111, 87, -1):
            ae(prev(), ("cep", i))
        for i in range(95, 71, -1):
            ae(prev(), ("c_o", i))
        self.assertNotEqual(self.cursor._current_segment, None)
        self.assertNotEqual(self.cursor.current_segment_number, None)
        self.cursor._partial = "d"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        for i in range(127, 103, -1):
            ae(prev(), ("deq", i))
        ae(prev(), None)
        self.cursor._partial = "b"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        ae(prev(), ("bb_o", 79))

    def test_48_setat_01(self):
        self.cursor._partial = False
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, None)

    def test_49_setat_02(self):
        self.cursor._partial = "a"
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, None)

    def test_50_setat_03(self):
        self.cursor._partial = "c"
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, ("cep", 100))

    def test_51_setat_04(self):
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, ("cep", 100))

    def test_52_setat_05(self):
        s = self.cursor.setat(("cep", 500))
        self.assertEqual(s, None)

    def test_53_setat_06(self):
        s = self.cursor.setat(("cep", 50))
        self.assertEqual(s, None)

    def test_54_set_partial_key(self):
        self.cursor.set_partial_key("ce")
        self.assertEqual(self.cursor._partial, "ce")

    def test_55__get_segment_01(self):
        s = self.cursor._get_segment(
            "cep",
            2,
            b"".join(
                (
                    int(2).to_bytes(4, byteorder="big"),
                    int(100).to_bytes(2, byteorder="big"),
                )
            ),
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def test_56__get_segment_02(self):
        self.assertEqual(self.cursor.current_segment_number, None)
        s = self.cursor._get_segment(
            "aa_o",
            0,
            b"".join(
                (
                    int(0).to_bytes(4, byteorder="big"),
                    int(24).to_bytes(2, byteorder="big"),
                    int(2).to_bytes(4, byteorder="big"),
                )
            ),
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)

    def test_57__get_segment_03(self):
        self.assertEqual(self.cursor.current_segment_number, None)
        s = self.cursor._get_segment(
            "cep",
            1,
            b"".join(
                (
                    int(1).to_bytes(4, byteorder="big"),
                    int(3).to_bytes(2, byteorder="big"),
                    int(9).to_bytes(4, byteorder="big"),
                )
            ),
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(self.cursor.current_segment_number, None)
        self.cursor._current_segment = s
        self.cursor.current_segment_number = 1
        t = self.cursor._get_segment(
            "cep",
            1,
            b"".join(
                (
                    int(1).to_bytes(4, byteorder="big"),
                    int(3).to_bytes(2, byteorder="big"),
                    int(9).to_bytes(4, byteorder="big"),
                )
            ),
        )
        self.assertIs(s, t)

    def test_58_set_current_segment(self):
        s = self.cursor.set_current_segment(
            "cep",
            b"".join(
                (
                    int(2).to_bytes(4, byteorder="big"),
                    int(100).to_bytes(2, byteorder="big"),
                )
            ),
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
        self.assertIs(s, self.cursor._current_segment)
        self.assertEqual(self.cursor.current_segment_number, 2)

    def test_59_refresh_recordset(self):
        self.cursor.refresh_recordset()


class Cursor_secondary__get_record_at_position(_DB):
    def setUp(self):
        super().setUp()
        segments = (
            b"\x7f\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x42\x00\x43\x00\x44",
            b"\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        )
        self.segments = {}
        key = "a_o"
        for s in segments:
            self.segments[self.database.segment_table["file1"].append(s)] = s
        self.database.start_transaction()
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            cursor.put(
                key.encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(31).to_bytes(2, byteorder="big"),
                        int(0 + 1).to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                key.encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x01",
                        int(3).to_bytes(2, byteorder="big"),
                        int(1 + 1).to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                key.encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x02",
                        int(24).to_bytes(2, byteorder="big"),
                        int(2 + 1).to_bytes(4, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                key.encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x03",
                        int(50).to_bytes(2, byteorder="big"),
                    )
                ),
                dbe_module.db.DB_KEYLAST,
            )
        finally:
            cursor.close()
        self.cursor = _db.CursorSecondary(
            self.database.table["file1_field1"][0],
            segment=self.database.segment_table["file1"],
            transaction=self.database.dbtxn,
        )

    def tearDown(self):
        self.cursor.close()
        self.database.commit()
        super().tearDown()

    def test_20_get_record_at_position_06(self):
        ae = self.assertEqual
        grat = self.cursor.get_record_at_position
        for i in range(31):
            ae(grat(i), ("a_o", i + 1))
        for i in range(31, 34):
            ae(grat(i), ("a_o", i + 163))
        for i in range(34, 58):
            ae(grat(i), ("a_o", i + 246))
        ae(grat(58), ("a_o", 434))
        ae(grat(59), None)
        ae(grat(-1), ("a_o", 434))
        for i in range(-2, -26, -1):
            ae(grat(i), ("a_o", i + 305))
        for i in range(-26, -29, -1):
            ae(grat(i), ("a_o", i + 222))
        for i in range(-29, -60, -1):
            ae(grat(i), ("a_o", i + 60))
        ae(grat(-60), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (berkeleydb, bsddb3):
        if dbe_module is None:
            continue
        runner().run(loader(Cursor_db))
        runner().run(loader(Cursor_primary))
        runner().run(loader(Cursor_secondary))
        runner().run(loader(Cursor_secondary__get_record_at_position))
