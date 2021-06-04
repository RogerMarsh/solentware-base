# test_recordset_int.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for RecordsetSegmentInt class"""

import unittest
import copy

from .. import recordset
from ..segmentsize import SegmentSize


class RecordsetSegmentInt(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes
        SegmentSize.db_segment_size_bytes = None
        self.rsi = recordset.RecordsetSegmentInt(2, "key", records=b"A")

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
            recordset.RecordsetSegmentInt,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("__init__\(\) got an unexpected keyword argument 'xxxxx'",)
            ),
            recordset.RecordsetSegmentInt,
            *(None, None),
            **dict(xxxxx=None),
        )

        # Python3.5 or earlier(?) raises '... is not iterable' exception.
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "(cannot convert 'NoneType' object to bytes)|",
                    "('NoneType' object is not iterable)",
                )
            ),
            recordset.RecordsetSegmentInt,
            *(None, None),
            **dict(records=None),
        )

        self.assertEqual(
            sorted(self.rsi.__dict__.keys()),
            [
                "current_position_in_segment",
                "index_key",
                "record_number",
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
        self.assertEqual(s.record_number, 65)
        self.assertEqual(s.index_key, "key")
        self.assertEqual(s.segment_number, 2)
        self.assertEqual(s.current_position_in_segment, None)

    def test_segment_number(self):
        self.assertEqual(self.rsi.segment_number, 2)

    def test_count_records(self):
        self.assertEqual(self.rsi.count_records(), 1)

    def test_current_01(self):
        self.assertEqual(self.rsi.current(), None)

    def test_current_02(self):
        self.rsi.current_position_in_segment = 5
        self.assertEqual(self.rsi.current(), ("key", 321))

    def test_first_01(self):
        self.assertEqual(self.rsi.first(), ("key", 321))
        self.assertEqual(self.rsi.current_position_in_segment, 0)

    def test_first_02(self):
        self.rsi.current_position_in_segment = 5
        self.assertEqual(self.rsi.first(), ("key", 321))

    def test_get_position_of_record_number(self):
        self.assertEqual(self.rsi.get_position_of_record_number(64), 0)
        self.assertEqual(self.rsi.get_position_of_record_number(65), 1)
        self.assertEqual(self.rsi.get_position_of_record_number(66), 1)

    def test_get_record_number_at_position_01(self):
        self.assertEqual(self.rsi.get_record_number_at_position(0), 321)
        self.assertEqual(self.rsi.get_record_number_at_position(1), None)

    def test_get_record_number_at_position_02(self):
        self.assertEqual(self.rsi.get_record_number_at_position(-1), 321)
        self.assertEqual(self.rsi.get_record_number_at_position(-2), None)

    def test_last_01(self):
        self.assertEqual(self.rsi.last(), ("key", 321))
        self.assertEqual(self.rsi.current_position_in_segment, 0)

    def test_last_02(self):
        self.rsi.current_position_in_segment = 5
        self.assertEqual(self.rsi.last(), ("key", 321))

    def test_next_01(self):
        self.assertEqual(self.rsi.next(), ("key", 321))
        self.assertEqual(self.rsi.current_position_in_segment, 0)

    def test_next_02(self):
        self.rsi.current_position_in_segment = 5
        self.assertEqual(self.rsi.next(), None)

    def test_prev_01(self):
        self.assertEqual(self.rsi.prev(), ("key", 321))
        self.assertEqual(self.rsi.current_position_in_segment, 0)

    def test_prev_02(self):
        self.rsi.current_position_in_segment = 5
        self.assertEqual(self.rsi.prev(), None)

    def test_setat_01(self):
        self.assertEqual(self.rsi.setat(321), ("key", 321))
        self.assertEqual(self.rsi.current_position_in_segment, 0)

    def test_setat_02(self):
        self.assertEqual(self.rsi.setat(322), None)

    def test__empty_segment(self):
        self.assertIsInstance(
            self.rsi._empty_segment(), recordset.RecordsetSegmentInt
        )

    def test___deepcopy__(self):
        self.assertIsInstance(
            self.rsi.__deepcopy__({}), recordset.RecordsetSegmentInt
        )

    def test___contains__(self):
        self.assertEqual(self.rsi.__contains__(66), False)
        self.assertEqual(self.rsi.__contains__(65), True)

    def test_normalize(self):
        self.assertIs(self.rsi.normalize(), self.rsi)

    def test_promote(self):
        s = self.rsi.promote()
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, False)
        self.assertEqual(65 in s, True)

    def test___or__01(self):
        s2 = recordset.RecordsetSegmentInt(2, "key", records=b"B")
        s = self.rsi | s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, True)
        self.assertEqual(65 in s, True)

    def test___or__02(self):
        s3 = recordset.RecordsetSegmentInt(3, "key", records=b"C")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'or' segments with different segment numbers",)
            ),
            self.rsi.__or__,
            *(s3,),
        )

    def test___and__01(self):
        s2 = recordset.RecordsetSegmentInt(2, "key", records=b"B")
        s = self.rsi & s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, False)
        self.assertEqual(65 in s, False)

    def test___and__02(self):
        s3 = recordset.RecordsetSegmentInt(3, "key", records=b"C")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'and' segments with different segment numbers",)
            ),
            self.rsi.__and__,
            *(s3,),
        )

    def test___xor__01(self):
        s2 = recordset.RecordsetSegmentInt(2, "key", records=b"B")
        s = self.rsi ^ s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, True)
        self.assertEqual(65 in s, True)

    def test___xor__02(self):
        s3 = recordset.RecordsetSegmentInt(3, "key", records=b"C")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'xor' segments with different segment numbers",)
            ),
            self.rsi.__xor__,
            *(s3,),
        )

    def test_tobytes(self):
        self.assertEqual(self.rsi.tobytes(), b"\x00A")


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(RecordsetSegmentInt))
