# test_recordset_bitarray.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for RecordsetSegmentBitarray class"""

import unittest
import copy

from .. import recordset
from ..segmentsize import SegmentSize

SEGBYTES = (b'\x01' +
            b'\xff' * (SegmentSize._db_upper_conversion_limit // 8) +
            b'\x00' * (SegmentSize.db_segment_size_bytes -
                       SegmentSize._db_upper_conversion_limit // 8 -
                       1))


class RecordsetSegmentBitarray(unittest.TestCase):

    def setUp(self):
        self.rsi = recordset.RecordsetSegmentBitarray(
            2, 'key', records=SEGBYTES)

    def tearDown(self):
        self.rsi = None

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 2 required positional arguments: ",
                "'segment_number' and 'key'",
                )),
            recordset.RecordsetSegmentBitarray,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) got an unexpected keyword argument 'xxxxx'",
                )),
            recordset.RecordsetSegmentBitarray,
            *(None, None),
            **dict(xxxxx=None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "'bool' object is not iterable",
                )),
            recordset.RecordsetSegmentBitarray,
            *(None, None),
            **dict(records=False),
            )
        self.assertEqual(sorted(self.rsi.__dict__.keys()),
                         ['_bitarray',
                          '_current_position_in_segment',
                          '_key',
                          '_reversed',
                          '_segment_number',
                          ])
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "count_records\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.count_records,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "current\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.current,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "first\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.first,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_position_of_record_number\(\) missing 1 required ",
                "positional argument: ",
                "'recnum'",
                )),
            self.rsi.get_position_of_record_number,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_record_number_at_position\(\) takes from 2 to 3 ",
                "positional arguments but 4 were given",
                )),
            self.rsi.get_record_number_at_position,
            *(None, None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "last\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.last,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "next\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.next,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "prev\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.prev,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "setat\(\) missing 1 required positional argument: 'record'",
                )),
            self.rsi.setat,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "_empty_segment\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi._empty_segment,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__deepcopy__\(\) missing 1 required positional argument: ",
                "'memo'",
                )),
            self.rsi.__deepcopy__,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__contains__\(\) missing 1 required positional argument: ",
                "'relative_record_number'",
                )),
            self.rsi.__contains__,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "normalize\(\) takes from 1 to 2 ",
                "positional arguments but 3 were given",
                )),
            self.rsi.normalize,
            *(None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "promote\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.promote,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__or__\(\) missing 1 required positional argument: ",
                "'other'",
                )),
            self.rsi.__or__,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__and__\(\) missing 1 required positional argument: ",
                "'other'",
                )),
            self.rsi.__and__,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__xor__\(\) missing 1 required positional argument: ",
                "'other'",
                )),
            self.rsi.__xor__,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "tobytes\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.rsi.tobytes,
            *(None,),
            )

    def test___init__(self):
        s = self.rsi
        self.assertEqual(s._bitarray._ba, SEGBYTES)
        self.assertEqual(s._key, 'key')
        self.assertEqual(s._segment_number, 2)
        self.assertEqual(s._current_position_in_segment, None)
        self.assertEqual(s._reversed, None)

    def test_segment_number(self):
        self.assertEqual(self.rsi.segment_number, 2)

    def test_count_records(self):
        self.assertEqual(self.rsi.count_records(), 2001)

    def test_current_01(self):
        self.assertEqual(self.rsi.current(), None)

    def test_current_02(self):
        self.rsi._current_position_in_segment = 1
        self.assertEqual(self.rsi.current(), ('key', 65537))

    def test_current_03(self):
        # Different to RecordsetSegmentInt?
        self.rsi._current_position_in_segment = 20000
        self.assertEqual(self.rsi.current(), ('key', 85536))

    def test_first_01(self):
        self.assertEqual(self.rsi.first(), ('key', 65543))
        self.assertEqual(self.rsi._current_position_in_segment, 7)

    def test_first_02(self):
        self.rsi._current_position_in_segment = 2
        self.assertEqual(self.rsi.first(), ('key', 65543))

    def test_first_03(self):
        self.rsi._segment_number = 'a'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "unsupported operand type\(s\) for \+: 'int' and 'str'",
                )),
            self.rsi.first,
            )

    def test_get_position_of_record_number(self):
        self.assertEqual(self.rsi.get_position_of_record_number(64), 57)
        self.assertEqual(self.rsi.get_position_of_record_number(65), 58)
        self.assertEqual(self.rsi.get_position_of_record_number(66), 59)
        self.assertEqual(self.rsi.get_position_of_record_number(67), 60)
        self.assertEqual(self.rsi.get_position_of_record_number(68), 61)
        self.assertEqual(self.rsi.get_position_of_record_number(63), 56)

    def test_get_record_number_at_position(self):
        self.assertEqual(self.rsi.get_record_number_at_position(0), 65543)

    def test_last_01(self):
        self.assertEqual(self.rsi.last(), ('key', 67543))
        self.assertEqual(self.rsi._current_position_in_segment, 2007)

    def test_last_02(self):
        self.rsi._current_position_in_segment = 2
        self.assertEqual(self.rsi.last(), ('key', 67543))

    def test_last_03(self):
        self.rsi._segment_number = 'a'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "unsupported operand type\(s\) for \+: 'int' and 'str'",
                )),
            self.rsi.last,
            )

    def test_next(self):
        self.assertEqual(self.rsi.next(), ('key', 65543))
        self.assertEqual(self.rsi._current_position_in_segment, 7)
        self.assertEqual(self.rsi.next(), ('key', 65544))
        self.assertEqual(self.rsi.next(), ('key', 65545))
        for i in range(1997):
            self.rsi.next()
        self.assertEqual(self.rsi.next(), ('key', 67543))
        self.assertEqual(self.rsi.next(), None)

    def test_prev(self):
        self.assertEqual(self.rsi.prev(), ('key', 67543))
        self.assertEqual(self.rsi._current_position_in_segment, 2007)
        self.assertEqual(self.rsi.prev(), ('key', 67542))
        self.assertEqual(self.rsi.prev(), ('key', 67541))
        for i in range(1997):
            self.rsi.prev()
        self.assertEqual(self.rsi.prev(), ('key', 65543))
        self.assertEqual(self.rsi.prev(), None)

    def test_setat_01(self):
        self.assertEqual(self.rsi.setat(65601), ('key', 65601))
        self.assertEqual(self.rsi._current_position_in_segment, 65)

    def test_setat_02(self):
        self.assertEqual(self.rsi.setat(68000), None)
        self.assertEqual(self.rsi.setat(600), None)

    def test_normalize_01(self):
        self.assertIs(self.rsi.normalize(), self.rsi)

    def test_normalize_02(self):
        for i in range(65544, 67544):
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi._bitarray[o] = False
        self.assertIsInstance(self.rsi.normalize(),
                              recordset.RecordsetSegmentInt)

    def test_normalize_03(self):
        for i in range(65544, 66144):
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi._bitarray[o] = False
        self.assertIsInstance(self.rsi.normalize(),
                              recordset.RecordsetSegmentList)

    def test_normalize_04(self):
        for i in range(65544, 65550):
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi._bitarray[o] = False
        self.assertIsInstance(self.rsi.normalize(),
                              recordset.RecordsetSegmentList)

    def test_normalize_05(self):
        for i in range(65544, 65550):
            s, o = divmod(i, SegmentSize.db_segment_size)
            self.rsi._bitarray[o] = False
        self.assertIs(self.rsi.normalize(use_upper_limit=False), self.rsi)

    def test_promote(self):
        self.assertIs(self.rsi.promote(), self.rsi)

    def test__empty_segment(self):
        self.assertIsInstance(self.rsi._empty_segment(),
                              recordset.RecordsetSegmentBitarray)

    def test___deepcopy__(self):
        self.assertIsInstance(self.rsi.__deepcopy__({}),
                              recordset.RecordsetSegmentBitarray)

    def test___contains__(self):
        self.assertEqual(self.rsi.__contains__(6), False)
        self.assertEqual(self.rsi.__contains__(7), True)

    def test___or__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        s = self.rsi | s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(5 in s, False)
        self.assertEqual(6 in s, True)
        self.assertEqual(2006 in s, True)
        self.assertEqual(2007 in s, True)
        self.assertEqual(2008 in s, False)

    def test___or__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join((
                "Attempt to 'or' segments with different segment numbers",
                )),
            self.rsi.__or__,
            *(s3,),
            )

    def test___and__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        s = self.rsi & s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(5 in s, False)
        self.assertEqual(6 in s, False)
        self.assertEqual(7 in s, True)
        self.assertEqual(8 in s, False)
        self.assertEqual(2007 in s, False)
        self.assertEqual(2008 in s, False)

    def test___and__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join((
                "Attempt to 'and' segments with different segment numbers",
                )),
            self.rsi.__and__,
            *(s3,),
            )

    def test___xor__01(self):
        s2 = recordset.RecordsetSegmentBitarray(
            2,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        s = self.rsi ^ s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(5 in s, False)
        self.assertEqual(6 in s, True)
        self.assertEqual(7 in s, False)
        self.assertEqual(8 in s, True)
        self.assertEqual(2007 in s, True)
        self.assertEqual(2008 in s, False)

    def test___xor__02(self):
        s3 = recordset.RecordsetSegmentBitarray(
            3,
            'key',
            records=(
                b'\x03' +
                b'\x00' * (SegmentSize.db_segment_size_bytes - 1))
            )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join((
                "Attempt to 'xor' segments with different segment numbers",
                )),
            self.rsi.__xor__,
            *(s3,),
            )

    def test_tobytes(self):
        self.assertEqual(self.rsi.tobytes(), SEGBYTES)

    def test___setitem__01(self):
        self.rsi[divmod(65541, SegmentSize.db_segment_size)] = True
        self.assertEqual(5 in self.rsi, True)

    def test___setitem__02(self):
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join((
                "'RecordsetSegmentBitarray' segment is not the one for ",
                "this 'key'",
                )),
            self.rsi.__setitem__,
            *(divmod(18541, SegmentSize.db_segment_size), True),
            )


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(RecordsetSegmentBitarray))
