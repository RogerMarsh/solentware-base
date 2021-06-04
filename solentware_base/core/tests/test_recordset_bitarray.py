# test_recordset_bitarray.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for RecordsetSegmentBitarray class"""

import unittest
import copy

from .. import recordset
from ..segmentsize import SegmentSize


class RecordsetSegmentBitarray(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes
        SegmentSize.db_segment_size_bytes = None
        self.sbytes = b"".join(
            (
                b"\x01\x00\xff\x00\x00\x00\x00\x00",
                b"\x00\x00\x00\x00\x00\x00\x00\x00",
            )
        )
        self.rsi = recordset.RecordsetSegmentBitarray(
            2, "key", records=self.sbytes
        )

    def tearDown(self):
        SegmentSize.db_segment_size_bytes = self.__ssb
        self.rsi = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'segment_number' and 'key'",
                )
            ),
            recordset.RecordsetSegmentBitarray,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("__init__\(\) got an unexpected keyword argument 'xxxxx'",)
            ),
            recordset.RecordsetSegmentBitarray,
            *(None, None),
            **dict(xxxxx=None),
        )
        # Matches "'bool' ..." before Python 3.8 but "can't ..." otherwise.
        self.assertRaisesRegex(
            TypeError,
            "|".join(
                (
                    "'bool' object is not iterable",
                    "can't extend bytearray with bool",
                )
            ),
            recordset.RecordsetSegmentBitarray,
            *(None, None),
            **dict(records=False),
        )
        self.assertEqual(
            sorted(self.rsi.__dict__.keys()),
            [
                "_reversed",
                "bitarray",
                "current_position_in_segment",
                "index_key",
                "segment_number",
            ],
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "count_records\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "current\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi.current,
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
            self.rsi.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record_number\(\) missing 1 required ",
                    "positional argument: ",
                    "'recnum'",
                )
            ),
            self.rsi.get_position_of_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_number_at_position\(\) takes 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsi.get_record_number_at_position,
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
            self.rsi.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi.next,
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
            self.rsi.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("setat\(\) missing 1 required positional argument: 'record'",)
            ),
            self.rsi.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_empty_segment\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi._empty_segment,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__deepcopy__\(\) missing 1 required positional argument: ",
                    "'memo'",
                )
            ),
            self.rsi.__deepcopy__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__contains__\(\) missing 1 required positional argument: ",
                    "'relative_record_number'",
                )
            ),
            self.rsi.__contains__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "normalize\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsi.normalize,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "promote\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi.promote,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__or__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsi.__or__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__and__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsi.__and__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__xor__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsi.__xor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "tobytes\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsi.tobytes,
            *(None,),
        )

    def test___init__(self):
        s = self.rsi
        self.assertEqual(s.bitarray.bitarray_bytes, self.sbytes)
        self.assertEqual(s.index_key, "key")
        self.assertEqual(s.segment_number, 2)
        self.assertEqual(s.current_position_in_segment, None)
        self.assertEqual(s._reversed, None)

    def test_segment_number(self):
        self.assertEqual(self.rsi.segment_number, 2)

    def test_count_records(self):
        self.assertEqual(self.rsi.count_records(), 9)

    def test_current_01(self):
        self.assertEqual(self.rsi.current(), None)

    def test_current_02(self):
        self.rsi.current_position_in_segment = 1
        self.assertEqual(self.rsi.current(), ("key", 257))

    def test_current_03(self):
        # Different to RecordsetSegmentInt?
        self.rsi.current_position_in_segment = 200
        self.assertEqual(self.rsi.current(), ("key", 456))

    def test_first_01(self):
        self.assertEqual(self.rsi.first(), ("key", 263))
        self.assertEqual(self.rsi.current_position_in_segment, 7)

    def test_first_02(self):
        self.rsi.current_position_in_segment = 2
        self.assertEqual(self.rsi.first(), ("key", 263))

    def test_first_03(self):
        self.rsi.segment_number = "a"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("unsupported operand type\(s\) for \+: 'int' and 'str'",)
            ),
            self.rsi.first,
        )

    def test_get_position_of_record_number(self):
        self.assertEqual(self.rsi.get_position_of_record_number(16), 1)
        self.assertEqual(self.rsi.get_position_of_record_number(17), 2)
        self.assertEqual(self.rsi.get_position_of_record_number(18), 3)
        self.assertEqual(self.rsi.get_position_of_record_number(19), 4)
        self.assertEqual(self.rsi.get_position_of_record_number(23), 8)
        self.assertEqual(self.rsi.get_position_of_record_number(24), 9)
        self.assertEqual(self.rsi.get_position_of_record_number(7), 0)
        self.assertEqual(self.rsi.get_position_of_record_number(8), 1)

    def test_get_record_number_at_position_01(self):
        self.assertEqual(self.rsi.get_record_number_at_position(0), 263)
        self.assertEqual(self.rsi.get_record_number_at_position(1), 272)
        self.assertEqual(self.rsi.get_record_number_at_position(2), 273)
        self.assertEqual(self.rsi.get_record_number_at_position(3), 274)
        self.assertEqual(self.rsi.get_record_number_at_position(4), 275)
        self.assertEqual(self.rsi.get_record_number_at_position(5), 276)
        self.assertEqual(self.rsi.get_record_number_at_position(6), 277)
        self.assertEqual(self.rsi.get_record_number_at_position(7), 278)
        self.assertEqual(self.rsi.get_record_number_at_position(8), 279)
        self.assertEqual(self.rsi.get_record_number_at_position(9), None)

    def test_get_record_number_at_position_02(self):
        self.assertEqual(self.rsi.get_record_number_at_position(-1), 279)
        self.assertEqual(self.rsi.get_record_number_at_position(-2), 278)
        self.assertEqual(self.rsi.get_record_number_at_position(-3), 277)
        self.assertEqual(self.rsi.get_record_number_at_position(-4), 276)
        self.assertEqual(self.rsi.get_record_number_at_position(-5), 275)
        self.assertEqual(self.rsi.get_record_number_at_position(-6), 274)
        self.assertEqual(self.rsi.get_record_number_at_position(-7), 273)
        self.assertEqual(self.rsi.get_record_number_at_position(-8), 272)
        self.assertEqual(self.rsi.get_record_number_at_position(-9), 263)
        self.assertEqual(self.rsi.get_record_number_at_position(-10), None)

    def test_last_01(self):
        self.assertEqual(self.rsi.last(), ("key", 279))
        self.assertEqual(self.rsi.current_position_in_segment, 23)

    def test_last_02(self):
        self.rsi.current_position_in_segment = 2
        self.assertEqual(self.rsi.last(), ("key", 279))

    def test_last_03(self):
        self.rsi.segment_number = "a"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("unsupported operand type\(s\) for \+: 'int' and 'str'",)
            ),
            self.rsi.last,
        )

    def test_next(self):
        self.assertEqual(self.rsi.next(), ("key", 263))
        self.assertEqual(self.rsi.current_position_in_segment, 7)
        self.assertEqual(self.rsi.next(), ("key", 272))
        self.assertEqual(self.rsi.next(), ("key", 273))
        for i in range(5):
            self.rsi.next()
        self.assertEqual(self.rsi.next(), ("key", 279))
        self.assertEqual(self.rsi.next(), None)

    def test_prev(self):
        self.assertEqual(self.rsi.prev(), ("key", 279))
        self.assertEqual(self.rsi.current_position_in_segment, 23)
        self.assertEqual(self.rsi.prev(), ("key", 278))
        self.assertEqual(self.rsi.prev(), ("key", 277))
        for i in range(5):
            self.rsi.prev()
        self.assertEqual(self.rsi.prev(), ("key", 263))
        self.assertEqual(self.rsi.prev(), None)

    def test_setat_01(self):
        self.assertEqual(self.rsi.setat(263), ("key", 263))
        self.assertEqual(self.rsi.current_position_in_segment, 7)

    def test_setat_02(self):
        self.assertEqual(self.rsi.setat(68000), None)
        self.assertEqual(self.rsi.setat(600), None)

    def test_normalize_01(self):
        self.assertIs(self.rsi.normalize(), self.rsi)

    def test_normalize_02(self):
        for i in 272, 273, 274, 275, 276, 277, 278, 279:
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi.bitarray[o] = False
        self.assertIsInstance(
            self.rsi.normalize(), recordset.RecordsetSegmentInt
        )

    def test_normalize_03(self):
        for i in 273, 274, 275, 276, 277, 278, 279:
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi.bitarray[o] = False
        self.assertIsInstance(
            self.rsi.normalize(), recordset.RecordsetSegmentList
        )

    def test_normalize_04(self):
        for i in 278, 279:
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi.bitarray[o] = False
        self.assertIsInstance(
            self.rsi.normalize(), recordset.RecordsetSegmentList
        )

    def test_normalize_05(self):
        for i in range(65544, 65550):
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi.bitarray[o] = False
        self.assertIs(self.rsi.normalize(use_upper_limit=False), self.rsi)

    def test_promote(self):
        self.assertIs(self.rsi.promote(), self.rsi)

    def test__empty_segment(self):
        self.assertIsInstance(
            self.rsi._empty_segment(), recordset.RecordsetSegmentBitarray
        )

    def test___deepcopy__(self):
        self.assertIsInstance(
            self.rsi.__deepcopy__({}), recordset.RecordsetSegmentBitarray
        )

    def test___contains__(self):
        self.assertEqual(self.rsi.__contains__(6), False)
        self.assertEqual(self.rsi.__contains__(7), True)

    def test___or__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            "key",
            records=(
                b"".join(
                    (
                        b"\x03\x00\xf0\x00\xff\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                )
            ),
        )
        s = self.rsi | s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(5 in s, False)
        self.assertEqual(6 in s, True)
        self.assertEqual(7 in s, True)
        self.assertEqual(17 in s, True)
        self.assertEqual(21 in s, True)
        self.assertEqual(35 in s, True)

    def test___or__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            "key",
            records=(
                b"".join(
                    (
                        b"\x03\x00\xf0\x00\xff\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                )
            ),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'or' segments with different segment numbers",)
            ),
            self.rsi.__or__,
            *(s3,),
        )

    def test___and__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            "key",
            records=(
                b"".join(
                    (
                        b"\x03\x00\xf0\x00\xff\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                )
            ),
        )
        s = self.rsi & s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(5 in s, False)
        self.assertEqual(6 in s, False)
        self.assertEqual(7 in s, True)
        self.assertEqual(17 in s, True)
        self.assertEqual(21 in s, False)
        self.assertEqual(35 in s, False)

    def test___and__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            "key",
            records=(
                b"\x03" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
            ),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'and' segments with different segment numbers",)
            ),
            self.rsi.__and__,
            *(s3,),
        )

    def test___xor__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            "key",
            records=(
                b"".join(
                    (
                        b"\x03\x00\xf0\x00\xff\x00\x00\x00",
                        b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    )
                )
            ),
        )
        s = self.rsi ^ s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(6 in s, True)
        self.assertEqual(7 in s, False)
        self.assertEqual(8 in s, False)
        self.assertEqual(16 in s, False)
        self.assertEqual(19 in s, False)
        self.assertEqual(20 in s, True)
        self.assertEqual(23 in s, True)
        self.assertEqual(36 in s, True)

    def test___xor__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            "key",
            records=(
                b"\x03" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
            ),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'xor' segments with different segment numbers",)
            ),
            self.rsi.__xor__,
            *(s3,),
        )

    def test_tobytes(self):
        self.assertEqual(self.rsi.tobytes(), self.sbytes)

    def test___setitem__01(self):
        self.rsi[divmod(261, SegmentSize.db_segment_size)] = True
        self.assertEqual(5 in self.rsi, True)

    def test___setitem__02(self):
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                (
                    "'RecordsetSegmentBitarray' segment is not the one for ",
                    "this 'key'",
                )
            ),
            self.rsi.__setitem__,
            *(divmod(18541, SegmentSize.db_segment_size), True),
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(RecordsetSegmentBitarray))
