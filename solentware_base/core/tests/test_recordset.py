# test_recordset.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for _Recordset class"""

import unittest
from collections import deque
import sys

from .. import recordset
from ..segmentsize import SegmentSize


class _Recordset(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes
        SegmentSize.db_segment_size_bytes = None

        class DB:
            pass

        self.DB = DB

        class RC:
            pass

        self.RC = RC

        class D:
            def __init__(self):
                # The idiom best representing Berkeley DB and DPT is
                # "self.d = {'file1':DB(), 'file2':DB()}".
                # The idiom implemented best represents SQLite and allows the
                # bitwise operator tests, __or__ and so forth, to test cases
                # where more than one 'D' object exists.
                db = DB()
                self.d = {"file1": db, "file2": db}

            # Planned to become 'def get_table(self, file)'.
            # See .._db .._dpt and .._sqlite modules.
            # Need to look at 'exists' too.
            def get_table_connection(self, file):
                return self.d.get(file)

            def exists(self, file, field):
                return bool(self.get_table_connection(file))

            def create_recordset_cursor(self, rs):
                return RC()

        self.D = D
        self.d = D()
        self.rs = recordset._Recordset(self.d, "file1")
        self.rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00A\x00B\x00C"
        )

    def tearDown(self):
        self.rs = None
        self.rsl = None
        SegmentSize.db_segment_size_bytes = self.__ssb

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'dbhome' and 'dbset'",
                )
            ),
            recordset._Recordset,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("__init__\(\) got an unexpected keyword argument 'xxxxx'",)
            ),
            recordset._Recordset,
            *(None, None),
            **dict(xxxxx=None),
        )

        if sys.version_info[:2] < (3, 6):
            excmsg = "(unorderable types: str\(\) [<>] int\(\))"
        else:
            excmsg = "".join(
                (
                    "('[<>]' not supported between instances of ",
                    "'str' and 'int')|",
                )
            )
        self.assertRaisesRegex(
            TypeError,
            excmsg,
            recordset._Recordset,
            *(self.d, "file1"),
            **dict(cache_size="a"),
        )

        self.assertEqual(
            sorted(self.rs.__dict__.keys()),
            [
                "_current_segment",
                "_database",
                "_dbhome",
                "_dbset",
                "_rs_segments",
                "_sorted_segnums",
                "record_cache",
                "record_deque",
            ],
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "close\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "clear_recordset\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.clear_recordset,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__len__\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.__len__,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__getitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'",
                )
            ),
            self.rs.__getitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__setitem__\(\) missing 2 required ",
                    "positional arguments: ",
                    "'segment' and 'record_numbers'",
                )
            ),
            self.rs.__setitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__delitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'",
                )
            ),
            self.rs.__delitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__contains__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'",
                )
            ),
            self.rs.__contains__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "count_records\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record_number\(\) missing 1 required ",
                    "positional argument: 'recnum'",
                )
            ),
            self.rs.get_position_of_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_number_at_position\(\) missing 1 required ",
                    "positional argument: 'position'",
                )
            ),
            self.rs.get_record_number_at_position,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "insort_left_nodup\(\) missing 1 required positional argument: ",
                    "'segment'",
                )
            ),
            self.rs.insort_left_nodup,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.last,
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
            self.rs.next,
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
            self.rs.prev,
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
            self.rs.current,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("setat\(\) missing 1 required positional argument: 'record'",)
            ),
            self.rs.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__or__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__or__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ior__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__ior__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__and__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__and__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__iand__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__iand__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__xor__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__xor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ixor__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rs.__ixor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "normalize\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rs.normalize,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "is_record_number_in_record_set\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            self.rs.is_record_number_in_record_set,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_empty__recordset\(\) takes 0 positional arguments ",
                    "but 1 was given",
                )
            ),
            recordset._empty__recordset,
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
            self.rs.__deepcopy__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "place_record_number\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            self.rs.place_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "remove_record_number\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            self.rs.remove_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "create_recordset_cursor\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rs.create_recordset_cursor,
            *(None,),
        )

    def test___init__01(self):
        s = recordset._Recordset(self.d, "")
        self.assertEqual(s._rs_segments, {})
        self.assertEqual(s.record_cache, {})
        self.assertEqual(s.record_deque, deque(maxlen=1))
        self.assertEqual(s._current_segment, None)
        self.assertEqual(s._sorted_segnums, [])
        self.assertEqual(s._dbhome, None)
        self.assertEqual(s._dbset, None)
        self.assertEqual(s._database, None)

    def test___init__02(self):
        s = self.rs
        self.assertEqual(s._rs_segments, {})
        self.assertEqual(s.record_cache, {})
        self.assertEqual(s.record_deque, deque(maxlen=1))
        self.assertEqual(s._current_segment, None)
        self.assertEqual(s._sorted_segnums, [])
        self.assertIsInstance(s._dbhome, self.D)
        self.assertEqual(s._dbset, "file1")
        self.assertIsInstance(s._database, self.DB)

    def test_close(self):
        self.assertEqual(self.rs.close(), None)

    def test_clear_recordset(self):
        self.assertEqual(self.rs.clear_recordset(), None)

    def test___len__(self):
        self.assertEqual(self.rs.__len__(), 0)

    def test___getitem__(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0",)),
            self.rs.__getitem__,
            *(0,),
        )
        self.rs._rs_segments[0] = None
        self.assertEqual(self.rs.__getitem__(0), None)

    def test___setitem__(self):
        self.assertEqual(self.rs.__setitem__(0, True), None)
        self.assertEqual(self.rs._rs_segments[0], True)

    def test___delitem___01(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0",)),
            self.rs.__delitem__,
            *(0,),
        )

    def test___delitem___01(self):
        s = self.rs
        s._rs_segments[0] = 0
        s._rs_segments[1] = 1
        s._rs_segments[2] = 2
        s._rs_segments[3] = 3
        s._sorted_segnums = sorted(s._rs_segments)
        self.assertEqual(s._current_segment, None)
        for i in range(len(s._sorted_segnums)):
            self.assertEqual(s.__delitem__(i), None)
        self.assertEqual(s._current_segment, None)

    def test___delitem___02(self):
        s = self.rs
        s._rs_segments[0] = 0
        s._rs_segments[1] = 1
        s._rs_segments[2] = 2
        s._rs_segments[3] = 3
        s._sorted_segnums = sorted(s._rs_segments)
        self.assertEqual(s._current_segment, None)
        s._current_segment = 6
        for i in range(len(s._sorted_segnums)):
            self.assertEqual(s.__delitem__(i), None)
        self.assertEqual(s._current_segment, None)

    def test___delitem___03(self):
        s = self.rs
        s._rs_segments[0] = 0
        s._rs_segments[1] = 1
        s._rs_segments[2] = 2
        s._rs_segments[3] = 3
        s._sorted_segnums = sorted(s._rs_segments)
        self.assertEqual(s._current_segment, None)
        s._current_segment = 6
        self.assertEqual(s.__delitem__(1), None)
        self.assertEqual(s.__delitem__(2), None)
        self.assertEqual(s.__delitem__(0), None)
        self.assertEqual(s.__delitem__(3), None)
        self.assertEqual(s._current_segment, None)

    def test___contains__(self):
        self.assertEqual(0 in self.rs, False)

    def test_count_records(self):
        self.assertEqual(self.rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.count_records(), 3)

    def test_get_position_of_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "unsupported operand type\(s\) for divmod\(\): 'str' and 'int'",
                )
            ),
            self.rs.get_position_of_record_number,
            *("a",),
        )
        self.assertEqual(self.rs.get_position_of_record_number(5), 0)
        self.assertEqual(self.rs.get_position_of_record_number(3500), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.get_position_of_record_number(350), 3)
        self.assertEqual(self.rs.get_position_of_record_number(322), 1)
        self.rs[0] = recordset.RecordsetSegmentList(
            0, "key", records=b"\x00A\x00B\x00C"
        )
        self.assertEqual(self.rs.get_position_of_record_number(322), 4)

    def test_get_record_number_at_position_01(self):
        if sys.version_info[:2] < (3, 6):
            excmsg = "(unorderable types: str\(\) [<>] int\(\))"
        else:
            excmsg = "".join(
                (
                    "('[<>]' not supported between instances of ",
                    "'str' and 'int')|",
                )
            )
        self.assertRaisesRegex(
            TypeError,
            excmsg,
            self.rs.get_record_number_at_position,
            *("a",),
        )
        self.assertEqual(self.rs.get_record_number_at_position(2), None)
        self.assertEqual(self.rs.get_record_number_at_position(-2), None)

    def test_get_record_number_at_position_02(self):
        self.rs[self.rsl.segment_number] = self.rsl
        self.rs[0] = recordset.RecordsetSegmentList(
            0, "key", records=b"\x00A\x00B\x00C"
        )
        self.rs[1] = recordset.RecordsetSegmentBitarray(
            1, "key", records=b"\x00\x7e\xe0" + b"\x00" * 13
        )
        self.rs[3] = recordset.RecordsetSegmentInt(3, "key", records=b"\x04")
        self.assertEqual(self.rs.get_record_number_at_position(0), 65)
        self.assertEqual(self.rs.get_record_number_at_position(1), 66)
        self.assertEqual(self.rs.get_record_number_at_position(2), 67)
        self.assertEqual(self.rs.get_record_number_at_position(3), 137)
        self.assertEqual(self.rs.get_record_number_at_position(4), 138)
        self.assertEqual(self.rs.get_record_number_at_position(5), 139)
        self.assertEqual(self.rs.get_record_number_at_position(6), 140)
        self.assertEqual(self.rs.get_record_number_at_position(7), 141)
        self.assertEqual(self.rs.get_record_number_at_position(8), 142)
        self.assertEqual(self.rs.get_record_number_at_position(9), 144)
        self.assertEqual(self.rs.get_record_number_at_position(10), 145)
        self.assertEqual(self.rs.get_record_number_at_position(11), 146)
        self.assertEqual(self.rs.get_record_number_at_position(12), 321)
        self.assertEqual(self.rs.get_record_number_at_position(13), 322)
        self.assertEqual(self.rs.get_record_number_at_position(14), 323)
        self.assertEqual(self.rs.get_record_number_at_position(15), 388)
        self.assertEqual(self.rs.get_record_number_at_position(16), None)

    def test_get_record_number_at_position_03(self):
        self.rs[self.rsl.segment_number] = self.rsl
        self.rs[0] = recordset.RecordsetSegmentList(
            0, "key", records=b"\x00A\x00B\x00C"
        )
        self.rs[1] = recordset.RecordsetSegmentBitarray(
            1, "key", records=b"\x00\x7e\xe0" + b"\x00" * 13
        )
        self.rs[3] = recordset.RecordsetSegmentInt(3, "key", records=b"\x04")
        self.assertEqual(self.rs.get_record_number_at_position(-1), 388)
        self.assertEqual(self.rs.get_record_number_at_position(-2), 323)
        self.assertEqual(self.rs.get_record_number_at_position(-3), 322)
        self.assertEqual(self.rs.get_record_number_at_position(-4), 321)
        self.assertEqual(self.rs.get_record_number_at_position(-5), 146)
        self.assertEqual(self.rs.get_record_number_at_position(-6), 145)
        self.assertEqual(self.rs.get_record_number_at_position(-7), 144)
        self.assertEqual(self.rs.get_record_number_at_position(-8), 142)
        self.assertEqual(self.rs.get_record_number_at_position(-9), 141)
        self.assertEqual(self.rs.get_record_number_at_position(-10), 140)
        self.assertEqual(self.rs.get_record_number_at_position(-11), 139)
        self.assertEqual(self.rs.get_record_number_at_position(-12), 138)
        self.assertEqual(self.rs.get_record_number_at_position(-13), 137)
        self.assertEqual(self.rs.get_record_number_at_position(-14), 67)
        self.assertEqual(self.rs.get_record_number_at_position(-15), 66)
        self.assertEqual(self.rs.get_record_number_at_position(-16), 65)
        self.assertEqual(self.rs.get_record_number_at_position(-17), None)

    def test_insort_left_nodup(self):
        self.assertEqual(self.rs.insort_left_nodup(2), None)
        self.assertEqual(self.rs.insort_left_nodup(1), None)
        self.assertEqual(len(self.rs._sorted_segnums), 2)
        self.assertEqual(self.rs.insort_left_nodup(1), None)
        self.assertEqual(len(self.rs._sorted_segnums), 2)

    def test_first(self):
        self.assertEqual(self.rs.first(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.first(), ("key", 321))

    def test_last(self):
        self.assertEqual(self.rs.last(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.last(), ("key", 323))

    def test_next(self):
        self.assertEqual(self.rs.next(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.next(), ("key", 321))
        self.assertEqual(self.rs.next(), ("key", 322))
        self.assertEqual(self.rs.next(), ("key", 323))
        self.assertEqual(self.rs.next(), None)
        self.rs[4] = recordset.RecordsetSegmentList(
            4, "key", records=b"\x00A\x00B\x00C"
        )
        self.assertEqual(self.rs.next(), ("key", 577))

    def test_prev(self):
        self.assertEqual(self.rs.prev(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.prev(), ("key", 323))
        self.assertEqual(self.rs.prev(), ("key", 322))
        self.assertEqual(self.rs.prev(), ("key", 321))
        self.assertEqual(self.rs.prev(), None)
        self.rs[1] = recordset.RecordsetSegmentList(
            1, "key", records=b"\x00A\x00B\x00C"
        )
        self.assertEqual(self.rs.prev(), ("key", 195))

    def test_current(self):
        self.assertEqual(self.rs.current(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs._current_segment, None)
        self.assertEqual(self.rs.current(), None)
        self.rs.first()
        self.assertEqual(self.rs._current_segment, 0)
        self.assertEqual(self.rs.current(), ("key", 321))

    def test_setat(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "unsupported operand type\(s\) for divmod\(\): 'str' ",
                    "and 'int'",
                )
            ),
            self.rs.setat,
            *("a",),
        )
        self.assertEqual(self.rs.setat(195), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.setat(195), None)
        self.rs[1] = recordset.RecordsetSegmentList(
            1, "key", records=b"\x00A\x00B\x00C"
        )
        self.assertEqual(self.rs.setat(195), ("key", 195))

    def test___or___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'or' record sets for different databases",)),
            self.rs.__or__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'or' record sets for different tables",)),
            self.rs.__or__,
            *(rs2,),
        )
        rs = self.rs | rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs | rs1
        self.assertEqual(rs.count_records(), 5)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___or___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs = self.rs | rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs | rs1
        self.assertEqual(rs.count_records(), 6)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___ior___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'ior' record sets for different databases",)),
            self.rs.__ior__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'ior' record sets for different tables",)),
            self.rs.__ior__,
            *(rs2,),
        )
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 |= self.rs
        self.assertEqual(rs1.count_records(), 5)
        self.assertEqual(self.rs.count_records(), 3)

    def test___ior___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 |= self.rs
        self.assertEqual(rs1.count_records(), 6)
        self.assertEqual(self.rs.count_records(), 3)

    def test___and___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'and' record sets for different databases",)),
            self.rs.__and__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'and' record sets for different tables",)),
            self.rs.__and__,
            *(rs2,),
        )
        rs = self.rs & rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs & rs1
        self.assertEqual(rs.count_records(), 1)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___and___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs = self.rs & rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs & rs1
        self.assertEqual(rs.count_records(), 0)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___iand___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'iand' record sets for different databases",)
            ),
            self.rs.__iand__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'iand' record sets for different tables",)),
            self.rs.__iand__,
            *(rs2,),
        )
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 &= self.rs
        self.assertEqual(rs1.count_records(), 1)
        self.assertEqual(self.rs.count_records(), 3)

    def test___iand___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 &= self.rs
        self.assertEqual(rs1.count_records(), 0)
        self.assertEqual(self.rs.count_records(), 3)

    def test___xor___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'xor' record sets for different databases",)),
            self.rs.__xor__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'xor' record sets for different tables",)),
            self.rs.__xor__,
            *(rs2,),
        )
        rs = self.rs ^ rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs ^ rs1
        self.assertEqual(rs.count_records(), 4)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___xor___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs = self.rs ^ rs1
        self.assertEqual(rs.count_records(), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs = self.rs ^ rs1
        self.assertEqual(rs.count_records(), 6)
        self.assertEqual(rs1.count_records(), 3)
        self.assertEqual(self.rs.count_records(), 3)

    def test___ixor___01(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        rs2 = recordset._Recordset(self.d, "file2")
        rsd = recordset._Recordset(d, "file1")
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(
                ("Attempt to 'ixor' record sets for different databases",)
            ),
            self.rs.__ixor__,
            *(rsd,),
        )
        self.assertRaisesRegex(
            recordset.RecordsetError,
            "".join(("Attempt to 'ixor' record sets for different tables",)),
            self.rs.__ixor__,
            *(rs2,),
        )
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 ^= self.rs
        self.assertEqual(rs1.count_records(), 4)
        self.assertEqual(self.rs.count_records(), 3)

    def test___ixor___02(self):
        d = self.D()
        rsl = recordset.RecordsetSegmentList(
            3, "key", records=b"\x00C\x00D\x00E"
        )
        rs1 = recordset._Recordset(self.d, "file1")
        self.rs[self.rsl.segment_number] = self.rsl
        rs1[rsl.segment_number] = rsl
        rs1 ^= self.rs
        self.assertEqual(rs1.count_records(), 6)
        self.assertEqual(self.rs.count_records(), 3)

    def test_normalize_01(self):
        rss = set(self.rs._rs_segments.keys())
        self.assertEqual(self.rs.normalize(), None)
        self.assertEqual(rss, set(self.rs._rs_segments.keys()))
        self.assertEqual(len(self.rs._rs_segments), 0)

    def test_normalize_02(self):
        self.rs[self.rsl.segment_number] = self.rsl
        rss = set(self.rs._rs_segments.keys())
        self.assertEqual(self.rs.normalize(), None)
        self.assertEqual(rss, set(self.rs._rs_segments.keys()))
        self.assertEqual(len(self.rs._rs_segments), 1)

    def test_is_record_number_in_record_set_01(self):
        self.assertEqual(self.rs.is_record_number_in_record_set(1), False)
        self.assertEqual(self.rs.is_record_number_in_record_set(320), False)
        self.assertEqual(self.rs.is_record_number_in_record_set(321), False)

    def test_is_record_number_in_record_set_02(self):
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.is_record_number_in_record_set(1), False)
        self.assertEqual(self.rs.is_record_number_in_record_set(320), False)
        self.assertEqual(self.rs.is_record_number_in_record_set(321), True)

    def test__empty_recordset(self):
        self.assertIsInstance(
            recordset._empty__recordset(), recordset._Recordset
        )

    def test___deepcopy__(self):
        self.assertIsInstance(self.rs.__deepcopy__({}), recordset._Recordset)

    def test_place_record_number_01(self):
        self.assertEqual(len(self.rs._rs_segments), 0)
        self.assertEqual(self.rs.place_record_number(300), None)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(len(self.rs._rs_segments), 1)

    def test_place_record_number_02(self):
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(len(self.rs._rs_segments), 1)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentList
        )
        self.assertEqual(self.rs.count_records(), 3)
        self.assertEqual(self.rs.place_record_number(321), None)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(self.rs.count_records(), 3)
        self.assertEqual(self.rs.place_record_number(320), None)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(self.rs.count_records(), 4)
        self.assertEqual(len(self.rs._rs_segments), 1)
        self.assertEqual(self.rs.place_record_number(1), None)
        self.assertIsInstance(
            self.rs._rs_segments[0], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(self.rs.count_records(), 5)
        self.assertEqual(len(self.rs._rs_segments), 2)

    def test_remove_record_number_01(self):
        self.assertEqual(len(self.rs._rs_segments), 0)
        self.assertEqual(self.rs.remove_record_number(65600), None)
        self.assertEqual(len(self.rs._rs_segments), 0)

    def test_remove_record_number_02(self):
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(len(self.rs._rs_segments), 1)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentList
        )
        self.assertEqual(self.rs.count_records(), 3)
        self.assertEqual(self.rs.remove_record_number(320), None)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(self.rs.count_records(), 3)
        self.assertEqual(self.rs.remove_record_number(321), None)
        self.assertIsInstance(
            self.rs._rs_segments[2], recordset.RecordsetSegmentBitarray
        )
        self.assertEqual(self.rs.count_records(), 2)

    def test_create_recordset_cursor(self):
        self.assertIsInstance(self.rs.create_recordset_cursor(), self.RC)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(_Recordset))
