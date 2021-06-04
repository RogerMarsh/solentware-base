# test_recordset_list.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for RecordsetSegmentList class"""

import unittest
import copy

from .. import recordset
from ..segmentsize import SegmentSize


class RecordsetSegmentList(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes
        SegmentSize.db_segment_size_bytes = None
        self.rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00A\x00B\x00C"
        )

    def tearDown(self):
        SegmentSize.db_segment_size_bytes = self.__ssb
        self.rsl = None

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
            recordset.RecordsetSegmentList,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("__init__\(\) got an unexpected keyword argument 'xxxxx'",)
            ),
            recordset.RecordsetSegmentList,
            *(None, None),
            **dict(xxxxx=None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(("object of type 'NoneType' has no len()",)),
            recordset.RecordsetSegmentList,
            *(None, None),
            **dict(records=None),
        )
        self.assertEqual(
            sorted(self.rsl.__dict__.keys()),
            [
                "current_position_in_segment",
                "index_key",
                "list",
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
            self.rsl.count_records,
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
            self.rsl.current,
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
            self.rsl.first,
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
            self.rsl.get_position_of_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_number_at_position\(\) takes 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsl.get_record_number_at_position,
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
            self.rsl.last,
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
            self.rsl.next,
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
            self.rsl.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("setat\(\) missing 1 required positional argument: 'record'",)
            ),
            self.rsl.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_empty_segment\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsl._empty_segment,
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
            self.rsl.__deepcopy__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__contains__\(\) missing 1 required positional argument: ",
                    "'relative_record_number'",
                )
            ),
            self.rsl.__contains__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "normalize\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsl.normalize,
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
            self.rsl.promote,
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
            self.rsl.__or__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__and__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsl.__and__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__xor__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsl.__xor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "tobytes\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsl.tobytes,
            *(None,),
        )

    def test___init__(self):
        s = self.rsl
        self.assertEqual(s.list, [65, 66, 67])
        self.assertEqual(s.index_key, "key")
        self.assertEqual(s.segment_number, 2)
        self.assertEqual(s.current_position_in_segment, None)

    def test_segment_number(self):
        self.assertEqual(self.rsl.segment_number, 2)

    def test_count_records(self):
        self.assertEqual(self.rsl.count_records(), 3)

    def test_current_01(self):
        self.assertEqual(self.rsl.current(), None)

    def test_current_02(self):
        self.rsl.current_position_in_segment = 1
        self.assertEqual(self.rsl.current(), ("key", 322))

    def test_current_03(self):
        # Different to RecordsetSegmentInt?
        self.rsl.current_position_in_segment = 5
        self.assertRaisesRegex(
            IndexError,
            "".join(("list index out of range",)),
            self.rsl.current,
        )

    def test_first_01(self):
        self.assertEqual(self.rsl.first(), ("key", 321))
        self.assertEqual(self.rsl.current_position_in_segment, 0)

    def test_first_02(self):
        self.rsl.current_position_in_segment = 2
        self.assertEqual(self.rsl.first(), ("key", 321))

    def test_first_03(self):
        self.rsl.segment_number = None
        self.assertEqual(self.rsl.first(), None)

    def test_first_04(self):
        self.rsl.segment_number = "a"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("unsupported operand type\(s\) for \+: 'int' and 'str'",)
            ),
            self.rsl.first,
        )

    def test_get_position_of_record_number(self):
        self.assertEqual(self.rsl.get_position_of_record_number(64), 0)
        self.assertEqual(self.rsl.get_position_of_record_number(65), 0)
        self.assertEqual(self.rsl.get_position_of_record_number(66), 1)
        self.assertEqual(self.rsl.get_position_of_record_number(67), 2)
        self.assertEqual(self.rsl.get_position_of_record_number(68), 3)
        self.assertEqual(self.rsl.get_position_of_record_number(63), 0)

    def test_get_record_number_at_position_01(self):
        self.assertEqual(self.rsl.get_record_number_at_position(0), 321)
        self.assertEqual(self.rsl.get_record_number_at_position(1), 322)
        self.assertEqual(self.rsl.get_record_number_at_position(2), 323)
        self.assertEqual(self.rsl.get_record_number_at_position(3), None)

    def test_get_record_number_at_position_02(self):
        self.assertEqual(self.rsl.get_record_number_at_position(-1), 323)
        self.assertEqual(self.rsl.get_record_number_at_position(-2), 322)
        self.assertEqual(self.rsl.get_record_number_at_position(-3), 321)
        self.assertEqual(self.rsl.get_record_number_at_position(-4), None)

    def test_last_01(self):
        self.assertEqual(self.rsl.last(), ("key", 323))
        self.assertEqual(self.rsl.current_position_in_segment, 2)

    def test_last_02(self):
        self.rsl.current_position_in_segment = 2
        self.assertEqual(self.rsl.last(), ("key", 323))

    def test_last_03(self):
        self.rsl.segment_number = None
        self.assertEqual(self.rsl.last(), None)

    def test_last_04(self):
        self.rsl.segment_number = "a"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("unsupported operand type\(s\) for \+: 'int' and 'str'",)
            ),
            self.rsl.last,
        )

    def test_next(self):
        self.assertEqual(self.rsl.next(), ("key", 321))
        self.assertEqual(self.rsl.current_position_in_segment, 0)
        self.assertEqual(self.rsl.next(), ("key", 322))
        self.assertEqual(self.rsl.next(), ("key", 323))
        self.assertEqual(self.rsl.next(), None)

    def test_prev(self):
        self.assertEqual(self.rsl.prev(), ("key", 323))
        self.assertEqual(self.rsl.current_position_in_segment, 2)
        self.assertEqual(self.rsl.prev(), ("key", 322))
        self.assertEqual(self.rsl.prev(), ("key", 321))
        self.assertEqual(self.rsl.prev(), None)

    def test_setat_01(self):
        self.assertEqual(self.rsl.setat(321), ("key", 321))
        self.assertEqual(self.rsl.current_position_in_segment, 0)

    def test_setat_02(self):
        self.assertEqual(self.rsl.setat(320), None)
        self.assertEqual(self.rsl.setat(600), None)

    def test_insort_left_nodup(self):
        self.assertEqual(len(self.rsl.list), 3)
        self.rsl.insort_left_nodup(65)
        self.assertEqual(len(self.rsl.list), 3)
        self.rsl.insort_left_nodup(69)
        self.assertEqual(len(self.rsl.list), 4)

    def test___contains__(self):
        self.assertEqual(self.rsl.__contains__(69), False)
        self.assertEqual(self.rsl.__contains__(65), True)

    def test_normalize_01(self):
        self.assertIs(self.rsl.normalize(), self.rsl)

    def test_normalize_02(self):
        del self.rsl.list[:-1]
        self.assertIsInstance(
            self.rsl.normalize(), recordset.RecordsetSegmentInt
        )

    def test_normalize_03(self):
        for i in range(SegmentSize.db_upper_conversion_limit):
            self.rsl.list.append(i + 100)
        self.assertIsInstance(
            self.rsl.normalize(), recordset.RecordsetSegmentBitarray
        )

    def test_normalize_04(self):
        for i in range(SegmentSize.db_lower_conversion_limit):
            self.rsl.list.append(i + 100)
        self.assertIs(self.rsl.normalize(), self.rsl)

    def test_normalize_05(self):
        for i in range(SegmentSize.db_lower_conversion_limit):
            self.rsl.list.append(i + 100)
        self.assertIs(self.rsl.normalize(use_upper_limit=False), self.rsl)

    def test_promote(self):
        s = self.rsl.promote()
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, True)
        self.assertEqual(67 in s, True)
        self.assertEqual(68 in s, False)
        self.assertEqual(65 in s, True)

    def test___or__01(self):
        s2 = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        s = self.rsl | s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, True)
        self.assertEqual(65 in s, True)
        self.assertEqual(67 in s, True)
        self.assertEqual(68 in s, True)
        self.assertEqual(69 in s, True)

    def test___or__02(self):
        s3 = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'or' segments with different segment numbers",)
            ),
            self.rsl.__or__,
            *(s3,),
        )

    def test___and__01(self):
        s2 = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        s = self.rsl & s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, False)
        self.assertEqual(65 in s, False)
        self.assertEqual(67 in s, True)
        self.assertEqual(68 in s, False)
        self.assertEqual(69 in s, False)

    def test___and__02(self):
        s3 = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'and' segments with different segment numbers",)
            ),
            self.rsl.__and__,
            *(s3,),
        )

    def test___xor__01(self):
        s2 = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        s = self.rsl ^ s2
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(66 in s, True)
        self.assertEqual(65 in s, True)
        self.assertEqual(67 in s, False)
        self.assertEqual(68 in s, True)
        self.assertEqual(69 in s, True)

    def test___xor__02(self):
        s3 = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'xor' segments with different segment numbers",)
            ),
            self.rsl.__xor__,
            *(s3,),
        )

    def test__empty_segment(self):
        self.assertIsInstance(
            self.rsl._empty_segment(), recordset.RecordsetSegmentList
        )

    def test___deepcopy__(self):
        self.assertIsInstance(
            self.rsl.__deepcopy__({}), recordset.RecordsetSegmentList
        )

    def test_tobytes(self):
        self.assertEqual(self.rsl.tobytes(), b"\x00A\x00B\x00C")


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(RecordsetSegmentList))
