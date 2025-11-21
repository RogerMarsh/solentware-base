# test__nosql_cursor.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_nosql cursor tests."""

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
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize

_NDBM_TEST_ROOT = "___ndbm_test_nosql_cursor"
_GNU_TEST_ROOT = "___gnu_test_nosql_cursor"


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


class _NoSQL(unittest.TestCase):
    def setUp(self):
        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set differently for these engines.

        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(_nosql.Database):
            pass

        self._D = _D
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}}),
            segment_size_bytes=None,
        )
        self.database.specification["file2"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


class Cursor_nosql(_NoSQL):
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 4 positional arguments ",
                    "but 5 were given$",
                )
            ),
            _nosql.Cursor,
            *(None, None, None, None),
        )
        cursor = _nosql.Cursor(self.database)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (r"close\(\) takes 1 positional argument but 2 were given$",)
            ),
            cursor.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_converted_partial\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            cursor.get_converted_partial,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_partial_with_wildcard\(\) takes 1 ",
                    "positional argument but 2 were given$",
                )
            ),
            cursor.get_partial_with_wildcard,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_converted_partial_with_wildcard\(\) takes 1 ",
                    "positional argument but 2 were given$",
                )
            ),
            cursor.get_converted_partial_with_wildcard,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"refresh_recordset\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            cursor.refresh_recordset,
            *(None, None),
        )

    def t02___init__(self):
        cursor = _nosql.Cursor(self.database)
        self.assertEqual(cursor._file, None)
        self.assertEqual(cursor._current_segment, None)
        self.assertEqual(cursor.current_segment_number, None)
        self.assertEqual(cursor._current_record_number_in_segment, None)
        # CursorPrimary is cursor on existence bitmap.
        # CursorSecondary is cursor on tree for file and field.
        self.assertEqual(cursor._cursor, None)

    def t03_close(self):
        cursor = _nosql.Cursor(self.database)
        cursor.close()
        self.assertEqual(cursor._cursor, None)

    def t04_get_converted_partial(self):
        cursor = _nosql.Cursor(self.database)
        self.assertEqual(cursor.get_converted_partial(), None)

    def t05_get_partial_with_wildcard(self):
        cursor = _nosql.Cursor(self.database)
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "get_partial_with_wildcard not implemented$",
            cursor.get_partial_with_wildcard,
        )

    def t06_get_converted_partial_with_wildcard(self):
        cursor = _nosql.Cursor(self.database)
        cursor._partial = "part"
        self.assertEqual(cursor.get_converted_partial_with_wildcard(), "part")

    def t07_refresh_recordset(self):
        cursor = _nosql.Cursor(self.database)
        self.assertEqual(cursor.refresh_recordset(), None)


class Cursor_primary(_NoSQL):
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        db = self.database.dbenv
        tes = []
        for e, s in enumerate(segments):
            db["1_0__ebm_" + str(e)] = repr(s)
            tes.append(e)
        db["1_0__ebm"] = repr(tes)
        for i in range(24):
            db["1_0_" + str(i)] = repr("Data for record " + str(i))
            j = i + 128 + 24
            db["1_0_" + str(j)] = repr("Data for record " + str(j))
            j += 128 + 16
            db["1_0_" + str(j)] = repr("Data for record " + str(j))
        for i in range(8):
            db["1_0_" + str(i + 24)] = repr("Data for record " + str(i + 24))
        self.database.ebm_control["file1"] = _nosql.ExistenceBitmapControl(
            "1", "0", self.database
        )
        self.cursor = _nosql.CursorPrimary(self.database, file="file1")

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 2 positional arguments ",
                    "but 3 were given$",
                )
            ),
            _nosql.CursorPrimary,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"count_records\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"first\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_position_of_record\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.get_position_of_record,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_record_at_position\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.get_record_at_position,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"last\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"nearest\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.cursor.nearest,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"next\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"prev\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"setat\(\) missing 1 required ",
                    "positional argument: 'record'$",
                )
            ),
            self.cursor.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"refresh_recordset\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.refresh_recordset,
            *(None, None),
        )

    def t02___init__(self):
        cursor = _nosql.CursorPrimary(self.database, file="file2")
        self.assertEqual(cursor._ebm.ebm_table, "2_0__ebm")
        self.assertEqual(cursor._table, "2_0")

    def t03___init__(self):
        self.assertEqual(self.cursor._ebm.ebm_table, "1_0__ebm")
        self.assertEqual(self.cursor._table, "1_0")

    def t04_count_records(self):
        self.assertEqual(self.cursor.count_records(), 80)
        self.cursor.close()
        self.assertEqual(self.cursor.count_records(), None)

    def t05_first(self):
        self.assertEqual(self.cursor.first(), (0, repr("Data for record 0")))

    def t06_get_position_of_record_01(self):
        self.assertEqual(self.cursor.get_position_of_record(), 0)

    def t06_get_position_of_record_02(self):
        self.assertEqual(self.cursor.get_position_of_record((5, None)), 6)
        self.assertEqual(self.cursor.get_position_of_record((170, None)), 51)

    def t06_get_position_of_record_03(self):
        self.assertEqual(self.cursor.get_position_of_record((175, None)), 56)
        self.assertEqual(self.cursor.get_position_of_record((176, None)), 57)
        self.assertEqual(self.cursor.get_position_of_record((177, None)), 57)
        self.assertEqual(self.cursor.get_position_of_record((296, None)), 57)
        self.assertEqual(self.cursor.get_position_of_record((297, None)), 58)
        self.assertEqual(self.cursor.get_position_of_record((318, None)), 79)
        self.assertEqual(self.cursor.get_position_of_record((319, None)), 80)
        self.assertEqual(self.cursor.get_position_of_record((320, None)), 81)
        self.assertEqual(self.cursor.get_position_of_record((383, None)), 81)
        self.assertEqual(self.cursor.get_position_of_record((384, None)), 81)

    def t08_get_record_at_position_01(self):
        cgrap = self.cursor.get_record_at_position
        self.assertEqual(cgrap(), None)
        self.cursor._ebm.table_ebm_segments.clear()
        self.assertEqual(cgrap(30), None)

    def t08_get_record_at_position_02(self):
        cgrap = self.cursor.get_record_at_position
        self.assertEqual(cgrap(-1), (319, repr("Data for record 319")))
        self.assertEqual(cgrap(-23), (297, repr("Data for record 297")))
        self.assertEqual(cgrap(-24), (296, repr("Data for record 296")))
        self.assertEqual(cgrap(-25), (175, repr("Data for record 175")))
        self.assertEqual(cgrap(-48), (152, repr("Data for record 152")))
        self.assertEqual(cgrap(-49), (31, repr("Data for record 31")))
        self.assertEqual(cgrap(-80), (0, repr("Data for record 0")))
        self.assertEqual(cgrap(-81), None)

    def t08_get_record_at_position_03(self):
        cgrap = self.cursor.get_record_at_position
        self.assertEqual(cgrap(1), (0, "'Data for record 0'"))
        self.assertEqual(cgrap(30), (29, repr("Data for record 29")))
        self.assertEqual(cgrap(31), (30, repr("Data for record 30")))
        self.assertEqual(cgrap(32), (31, repr("Data for record 31")))
        self.assertEqual(cgrap(33), (152, repr("Data for record 152")))
        self.assertEqual(cgrap(56), (175, repr("Data for record 175")))
        self.assertEqual(cgrap(57), (296, repr("Data for record 296")))
        self.assertEqual(cgrap(80), (319, repr("Data for record 319")))
        self.assertEqual(cgrap(81), None)

    def t12_last(self):
        self.assertEqual(
            self.cursor.last(), (319, repr("Data for record 319"))
        )

    def t13_nearest(self):
        self.assertEqual(
            self.cursor.nearest(1), (1, repr("Data for record 1"))
        )
        self.assertEqual(
            self.cursor.nearest(40), (152, repr("Data for record 152"))
        )
        self.assertEqual(self.cursor.current_segment_number, 1)
        self.assertEqual(self.cursor._current_record_number_in_segment, 24)
        self.assertEqual(self.cursor.nearest(325), None)
        self.assertEqual(self.cursor.current_segment_number, 1)
        self.assertEqual(self.cursor._current_record_number_in_segment, 24)

    def t14_next_01(self):
        for i in range(32):
            self.assertEqual(
                self.cursor.next(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 0)
            self.assertEqual(self.cursor._current_record_number_in_segment, i)
        for i in range(152, 176):
            self.assertEqual(
                self.cursor.next(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 1)
            self.assertEqual(
                self.cursor._current_record_number_in_segment, i - 128
            )
        for i in range(296, 320):
            self.assertEqual(
                self.cursor.next(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 2)
            self.assertEqual(
                self.cursor._current_record_number_in_segment, i - 256
            )
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor.current_segment_number, 2)
        self.assertEqual(self.cursor._current_record_number_in_segment, 63)

    def t15_next_02(self):
        self.cursor.current_segment_number = 1
        self.cursor._current_record_number_in_segment = 100
        self.assertEqual(
            self.cursor.next(), (296, repr("Data for record 296"))
        )
        self.assertEqual(self.cursor.current_segment_number, 2)
        self.assertEqual(self.cursor._current_record_number_in_segment, 40)

    def t16_next_03(self):
        self.cursor.current_segment_number = 1
        self.cursor._current_record_number_in_segment = 10
        self.assertEqual(
            self.cursor.next(), (152, repr("Data for record 152"))
        )
        self.assertEqual(self.cursor.current_segment_number, 1)
        self.assertEqual(self.cursor._current_record_number_in_segment, 24)

    def t17_prev_01(self):
        for i in range(319, 295, -1):
            self.assertEqual(
                self.cursor.prev(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 2)
            self.assertEqual(
                self.cursor._current_record_number_in_segment, i - 256
            )
        for i in range(175, 151, -1):
            self.assertEqual(
                self.cursor.prev(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 1)
            self.assertEqual(
                self.cursor._current_record_number_in_segment, i - 128
            )
        for i in range(31, -1, -1):
            self.assertEqual(
                self.cursor.prev(), (i, repr("Data for record " + str(i)))
            )
            self.assertEqual(self.cursor.current_segment_number, 0)
            self.assertEqual(self.cursor._current_record_number_in_segment, i)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor.current_segment_number, 0)
        self.assertEqual(self.cursor._current_record_number_in_segment, 0)

    def t18_prev_02(self):
        self.cursor.current_segment_number = 1
        self.cursor._current_record_number_in_segment = 100
        self.assertEqual(
            self.cursor.prev(), (175, repr("Data for record 175"))
        )
        self.assertEqual(self.cursor.current_segment_number, 1)
        self.assertEqual(self.cursor._current_record_number_in_segment, 47)

    def t19_prev_03(self):
        self.cursor.current_segment_number = 1
        self.cursor._current_record_number_in_segment = 10
        self.assertEqual(self.cursor.prev(), (31, repr("Data for record 31")))
        self.assertEqual(self.cursor.current_segment_number, 0)
        self.assertEqual(self.cursor._current_record_number_in_segment, 31)

    def t20_setat(self):
        self.assertEqual(
            self.cursor.setat((10, None)), (10, repr("Data for record 10"))
        )
        self.assertEqual(self.cursor.current_segment_number, 0)
        self.assertEqual(self.cursor._current_record_number_in_segment, 10)
        self.assertEqual(self.cursor.setat((200, None)), None)
        self.assertEqual(self.cursor.current_segment_number, 0)
        self.assertEqual(self.cursor._current_record_number_in_segment, 10)

    def t21_refresh_recordset(self):
        self.cursor.refresh_recordset()


class CursorPrimaryGetRecordAtPosition(_NoSQL):
    # The Cursor_primary setup does not allow 'get_record_at_position(0)'
    # with 'record 0' absent; which was a problem in all other database
    # engines.
    # Other get_record_at_position tests remain in Cursor_primary.
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\x01\x80\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        db = self.database.dbenv
        tes = []
        for e, s in enumerate(segments):
            db["1_0__ebm_" + str(e)] = repr(s)
            tes.append(e)
        db["1_0__ebm"] = repr(tes)
        for i in range(7, 9):
            db["1_0_" + str(i)] = repr("Data for record " + str(i))
        for i in range(24):
            j = i + 128 + 24
            db["1_0_" + str(j)] = repr("Data for record " + str(j))
        self.database.ebm_control["file1"] = _nosql.ExistenceBitmapControl(
            "1", "0", self.database
        )
        self.cursor = _nosql.CursorPrimary(self.database, file="file1")

    def t01_get_record_at_position_01(self):
        cgrap = self.cursor.get_record_at_position
        self.assertEqual(cgrap(0), None)
        self.assertEqual(cgrap(1), (7, "'Data for record 7'"))
        self.assertEqual(cgrap(2), (8, "'Data for record 8'"))
        self.assertEqual(cgrap(3), (152, "'Data for record 152'"))
        self.assertEqual(cgrap(26), (175, "'Data for record 175'"))
        self.assertEqual(cgrap(27), None)
        self.assertEqual(cgrap(-1), (175, "'Data for record 175'"))
        self.assertEqual(cgrap(-26), (7, "'Data for record 7'"))
        self.assertEqual(cgrap(-27), None)


class Cursor_secondary(_NoSQL):
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\xff",
                    b"\xff\xff\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\xff\xff\xff\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                )
            ),
            b"\x00\x40\x00\x41",
            b"\x00\x42\x00\x43\x00\x44",
        )
        keys = (
            "a_o",
            "aa_o",
            "ba_o",
            "bb_o",
            "c_o",
            "cep",
            "deq",
        )
        db = self.database.dbenv
        for e, k in enumerate(keys):
            self.database.trees["file1_field1"].insert(k)
            db["1_1_1_0_" + k] = repr(segments[e])
            db["1_1_0_" + k] = repr({0: ("B", 32 if e == 0 else 24)})
        db["1_1_1_1_" + "ba_o"] = repr(segments[7])
        db["1_1_0_" + "ba_o"] = repr({0: ("B", 24), 1: ("L", 2)})
        self.database.trees["file1_field1"].insert("twy")
        db["1_1_1_1_" + "twy"] = repr(segments[8])
        db["1_1_0_" + "twy"] = repr({1: ("L", 3)})
        db["1_1_0_" + "ba_o"] = repr({0: ("B", 24), 1: ("L", 2), 2: (50, 1)})
        db["1_1_0_" + "cep"] = repr({0: ("B", 24), 2: (100, 1)})
        self.cursor = _nosql.CursorSecondary(
            self.database, file="file1", field="field1"
        )

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given$",
                )
            ),
            _nosql.CursorSecondary,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"count_records\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"first\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_position_of_record\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.get_position_of_record,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_record_at_position\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.get_record_at_position,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"last\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"nearest\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.cursor.nearest,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"next\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"prev\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.cursor.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"setat\(\) missing 1 required ",
                    "positional argument: 'record'$",
                )
            ),
            self.cursor.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_partial_key\(\) missing 1 required ",
                    "positional argument: 'partial'$",
                )
            ),
            self.cursor.set_partial_key,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_current_segment\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.cursor.set_current_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_current_segment_table\(\) missing 2 required ",
                    "positional arguments: 'key' and 'segment_table'$",
                )
            ),
            self.cursor.set_current_segment_table,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_current_segment_table\(\) takes from 3 to 4 ",
                    "positional arguments but 5 were given$",
                )
            ),
            self.cursor.set_current_segment_table,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"refresh_recordset\(\) takes from 1 to 2 ",
                    "positional arguments ",
                    "but 3 were given$",
                )
            ),
            self.cursor.refresh_recordset,
            *(None, None),
        )

    def t02___init__(self):
        self.assertEqual(self.cursor._field, "field1")
        self.assertEqual(self.cursor._table, "1_0")
        self.assertEqual(self.cursor._value_prefix, "1_1_1_")
        self.assertEqual(self.cursor._segment_table_prefix, "1_1_0_")
        self.assertEqual(self.cursor._segment_table, None)
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "".join(
                (
                    "Cannot create cursor because 'field2' field in 'file2' ",
                    "file is not ordered$",
                )
            ),
            _nosql.CursorSecondary,
            *(self.database,),
            **dict(file="file2", field="field2"),
        )

    def t05_count_records_01(self):
        s = self.cursor.count_records()
        self.assertEqual(s, 183)

    def t06_count_records_02(self):
        self.cursor._partial = "b"
        s = self.cursor.count_records()
        self.assertEqual(s, 51)

    def t07_first_01(self):
        self.cursor._partial = False
        s = self.cursor.first()
        self.assertEqual(s, None)

    def t08_first_02(self):
        s = self.cursor.first()
        self.assertEqual(s, ("a_o", 0))

    def t09_first_03(self):
        self.cursor._partial = "b"
        s = self.cursor.first()
        self.assertEqual(s, ("ba_o", 40))

    def t10_first_04(self):
        self.cursor._partial = "A"
        s = self.cursor.first()
        self.assertEqual(s, None)

    def t11_get_position_of_record_01(self):
        s = self.cursor.get_position_of_record()
        self.assertEqual(s, 0)

    def t12_get_position_of_record_02(self):
        s = self.cursor.get_position_of_record(("ba_o", 300))
        self.assertEqual(s, 82)

    # Same answer as _db, _lmdb and _sqlite because the a_o segemnt is not
    # counted: here it has 32 bits set rather than 31 bits set.  Compare with
    # other two get_position_of_record tests (12 and 14).
    def t13_get_position_of_record_03(self):
        self.cursor._partial = "b"
        s = self.cursor.get_position_of_record(("bb_o", 20))
        self.assertEqual(s, 28)

    def t14_get_position_of_record_04(self):
        s = self.cursor.get_position_of_record(("cep", 150))
        self.assertEqual(s, 155)

    def t30_last_01(self):
        self.cursor._partial = False
        s = self.cursor.last()
        self.assertEqual(s, None)

    def t31_last_02(self):
        s = self.cursor.last()
        self.assertEqual(s, ("twy", 196))  # ('deq', 127))

    def t32_last_03(self):
        self.cursor._partial = "b"
        s = self.cursor.last()
        self.assertEqual(s, ("bb_o", 79))

    def t33_last_04(self):
        self.cursor._partial = "A"
        s = self.cursor.last()
        self.assertEqual(s, None)

    def t34_nearest_01(self):
        self.assertEqual(self.cursor.nearest("d"), ("deq", 104))

    def t35_nearest_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.nearest("d"), None)

    def t36_nearest_03(self):
        self.cursor._partial = "b"
        self.assertEqual(self.cursor.nearest("bb"), ("bb_o", 56))

    def t37_nearest_04(self):
        self.assertEqual(self.cursor.nearest("z"), None)

    def t38_next_01(self):
        ae = self.assertEqual
        next_ = self.cursor.next
        for i in range(0, 32):
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

    def t39_next_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.next(), None)

    def t40_next_03(self):
        ae = self.assertEqual
        next_ = self.cursor.next
        ae(next_(), ("a_o", 0))
        self.cursor._partial = False
        ae(next_(), None)
        self.cursor._partial = None
        ae(next_(), ("a_o", 1))
        self.assertNotEqual(self.cursor._current_segment, None)
        self.assertNotEqual(self.cursor.current_segment_number, None)
        self.cursor._partial = "bb"
        self.cursor._current_segment = None
        self.cursor.current_segment_number = None
        ae(next_(), ("bb_o", 56))

    def t41_next_04(self):
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

    def t44_prev_01(self):
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
        for i in range(31, -1, -1):
            ae(prev(), ("a_o", i))
        ae(prev(), None)

    def t45_prev_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.prev(), None)

    def t46_prev_05(self):
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

    def t47_prev_06(self):
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

    def t50_setat_01(self):
        self.cursor._partial = False
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, None)

    def t51_setat_02(self):
        self.cursor._partial = "a"
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, None)

    def t52_setat_03(self):
        self.cursor._partial = "c"
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, ("cep", 100))

    def t53_setat_04(self):
        s = self.cursor.setat(("cep", 100))
        self.assertEqual(s, ("cep", 100))

    def t54_setat_05(self):
        s = self.cursor.setat(("cep", 500))
        self.assertEqual(s, None)

    def t55_setat_06(self):
        s = self.cursor.setat(("cep", 50))
        self.assertEqual(s, None)

    def t56_set_partial_key(self):
        self.cursor._current_segment = 2
        self.cursor.current_segment_number = 35
        self.cursor.set_partial_key("ce")
        self.assertEqual(self.cursor._partial, "ce")
        self.assertEqual(self.cursor._current_segment, None)
        self.assertEqual(self.cursor.current_segment_number, None)

    def t60_set_current_segment_table(self):
        ssc = _nosql.SegmentsetCursor(
            self.database.dbenv, "1_1_0_", "1_1_1_", "cep"
        )
        s = self.cursor.set_current_segment_table("cep", ssc, 1)
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
        self.assertIs(s, self.cursor._current_segment)
        self.assertEqual(self.cursor.current_segment_number, 2)

    def t61_refresh_recordset(self):
        self.cursor.refresh_recordset()

    def t70_get_unique_primary_for_index_key(self):
        ae = self.assertEqual
        gupfik = self.cursor.get_unique_primary_for_index_key
        self.assertRaisesRegex(
            _nosql.DatabaseError,
            "Index must refer to unique record$",
            gupfik,
            *("cep",),
        )


class CursorSecondaryGetRecordAtPosition(_NoSQL):
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"\x00\x42\x00\x43\x00\x44",
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        key = "a_o"
        db = self.database.dbenv
        self.database.trees["file1_field1"].insert(key)
        for e, s in enumerate(segments):
            db["1_1_1_" + str(e) + "_" + key] = repr(segments[e])
        db["1_1_0_" + key] = repr(
            {0: ("B", 32), 1: ("L", 3), 2: ("B", 24), 3: (50, 1)}
        )
        self.cursor = _nosql.CursorSecondary(
            self.database, file="file1", field="field1"
        )

    def t20_get_record_at_position_06(self):
        ae = self.assertEqual
        grap = self.cursor.get_record_at_position
        for i in range(32):
            ae(grap(i), ("a_o", i))
        for i in range(32, 35):
            ae(grap(i), ("a_o", i + 162))
        for i in range(35, 59):
            ae(grap(i), ("a_o", i + 245))
        ae(grap(59), ("a_o", 434))
        ae(grap(60), None)
        ae(grap(-1), ("a_o", 434))
        for i in range(-2, -26, -1):
            ae(grap(i), ("a_o", i + 305))
        for i in range(-26, -29, -1):
            ae(grap(i), ("a_o", i + 222))
        for i in range(-29, -61, -1):
            ae(grap(i), ("a_o", i + 60))
        ae(grap(-61), None)


if gnu_module:

    class _NoSQLGnu(_NoSQL):
        def setUp(self):
            self._oda = gnu_module, Gnu, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            if os.path.isfile(path):
                os.remove(path)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

    class Cursor_nosqlGnu(_NoSQLGnu):
        test_01 = Cursor_nosql.t01
        test_02 = Cursor_nosql.t02___init__
        test_03 = Cursor_nosql.t03_close
        test_04 = Cursor_nosql.t04_get_converted_partial
        test_05 = Cursor_nosql.t05_get_partial_with_wildcard
        test_06 = Cursor_nosql.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_nosql.t07_refresh_recordset

    class Cursor_primaryGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            Cursor_primary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03___init__
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_12 = Cursor_primary.t12_last
        test_13 = Cursor_primary.t13_nearest
        test_14 = Cursor_primary.t14_next_01
        test_15 = Cursor_primary.t15_next_02
        test_16 = Cursor_primary.t16_next_03
        test_17 = Cursor_primary.t17_prev_01
        test_18 = Cursor_primary.t18_prev_02
        test_19 = Cursor_primary.t19_prev_03
        test_20 = Cursor_primary.t20_setat
        test_21 = Cursor_primary.t21_refresh_recordset

    class Cursor_primary__get_record_at_positionGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            CursorPrimaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = (
            CursorPrimaryGetRecordAtPosition.t01_get_record_at_position_01
        )

    class Cursor_secondaryGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_05 = Cursor_secondary.t05_count_records_01
        test_06 = Cursor_secondary.t06_count_records_02
        test_07 = Cursor_secondary.t07_first_01
        test_08 = Cursor_secondary.t08_first_02
        test_09 = Cursor_secondary.t09_first_03
        test_10 = Cursor_secondary.t10_first_04
        test_11 = Cursor_secondary.t11_get_position_of_record_01
        test_12 = Cursor_secondary.t12_get_position_of_record_02
        test_13 = Cursor_secondary.t13_get_position_of_record_03
        test_14 = Cursor_secondary.t14_get_position_of_record_04
        test_30 = Cursor_secondary.t30_last_01
        test_31 = Cursor_secondary.t31_last_02
        test_32 = Cursor_secondary.t32_last_03
        test_33 = Cursor_secondary.t33_last_04
        test_34 = Cursor_secondary.t34_nearest_01
        test_35 = Cursor_secondary.t35_nearest_02
        test_36 = Cursor_secondary.t36_nearest_03
        test_37 = Cursor_secondary.t37_nearest_04
        test_38 = Cursor_secondary.t38_next_01
        test_39 = Cursor_secondary.t39_next_02
        test_40 = Cursor_secondary.t40_next_03
        test_41 = Cursor_secondary.t41_next_04
        test_44 = Cursor_secondary.t44_prev_01
        test_45 = Cursor_secondary.t45_prev_02
        test_46 = Cursor_secondary.t46_prev_05
        test_47 = Cursor_secondary.t47_prev_06
        test_50 = Cursor_secondary.t50_setat_01
        test_51 = Cursor_secondary.t51_setat_02
        test_52 = Cursor_secondary.t52_setat_03
        test_53 = Cursor_secondary.t53_setat_04
        test_54 = Cursor_secondary.t54_setat_05
        test_55 = Cursor_secondary.t55_setat_06
        test_56 = Cursor_secondary.t56_set_partial_key
        test_60 = Cursor_secondary.t60_set_current_segment_table
        test_61 = Cursor_secondary.t61_refresh_recordset
        test_70 = Cursor_secondary.t70_get_unique_primary_for_index_key

    class Cursor_secondary__get_record_at_positionGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            self.database.commit()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if ndbm_module:

    class _NoSQLNdbm(_NoSQL):
        def setUp(self):
            self._oda = ndbm_module, Ndbm, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
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

    class Cursor_nosqlNdbm(_NoSQLNdbm):
        test_01 = Cursor_nosql.t01
        test_02 = Cursor_nosql.t02___init__
        test_03 = Cursor_nosql.t03_close
        test_04 = Cursor_nosql.t04_get_converted_partial
        test_05 = Cursor_nosql.t05_get_partial_with_wildcard
        test_06 = Cursor_nosql.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_nosql.t07_refresh_recordset

    class Cursor_primaryNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            Cursor_primary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03___init__
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_12 = Cursor_primary.t12_last
        test_13 = Cursor_primary.t13_nearest
        test_14 = Cursor_primary.t14_next_01
        test_15 = Cursor_primary.t15_next_02
        test_16 = Cursor_primary.t16_next_03
        test_17 = Cursor_primary.t17_prev_01
        test_18 = Cursor_primary.t18_prev_02
        test_19 = Cursor_primary.t19_prev_03
        test_20 = Cursor_primary.t20_setat
        test_21 = Cursor_primary.t21_refresh_recordset

    class Cursor_primary__get_record_at_positionNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            CursorPrimaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = (
            CursorPrimaryGetRecordAtPosition.t01_get_record_at_position_01
        )

    class Cursor_secondaryNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_05 = Cursor_secondary.t05_count_records_01
        test_06 = Cursor_secondary.t06_count_records_02
        test_07 = Cursor_secondary.t07_first_01
        test_08 = Cursor_secondary.t08_first_02
        test_09 = Cursor_secondary.t09_first_03
        test_10 = Cursor_secondary.t10_first_04
        test_11 = Cursor_secondary.t11_get_position_of_record_01
        test_12 = Cursor_secondary.t12_get_position_of_record_02
        test_13 = Cursor_secondary.t13_get_position_of_record_03
        test_14 = Cursor_secondary.t14_get_position_of_record_04
        test_30 = Cursor_secondary.t30_last_01
        test_31 = Cursor_secondary.t31_last_02
        test_32 = Cursor_secondary.t32_last_03
        test_33 = Cursor_secondary.t33_last_04
        test_34 = Cursor_secondary.t34_nearest_01
        test_35 = Cursor_secondary.t35_nearest_02
        test_36 = Cursor_secondary.t36_nearest_03
        test_37 = Cursor_secondary.t37_nearest_04
        test_38 = Cursor_secondary.t38_next_01
        test_39 = Cursor_secondary.t39_next_02
        test_40 = Cursor_secondary.t40_next_03
        test_41 = Cursor_secondary.t41_next_04
        test_44 = Cursor_secondary.t44_prev_01
        test_45 = Cursor_secondary.t45_prev_02
        test_46 = Cursor_secondary.t46_prev_05
        test_47 = Cursor_secondary.t47_prev_06
        test_50 = Cursor_secondary.t50_setat_01
        test_51 = Cursor_secondary.t51_setat_02
        test_52 = Cursor_secondary.t52_setat_03
        test_53 = Cursor_secondary.t53_setat_04
        test_54 = Cursor_secondary.t54_setat_05
        test_55 = Cursor_secondary.t55_setat_06
        test_56 = Cursor_secondary.t56_set_partial_key
        test_60 = Cursor_secondary.t60_set_current_segment_table
        test_61 = Cursor_secondary.t61_refresh_recordset
        test_70 = Cursor_secondary.t70_get_unique_primary_for_index_key

    class Cursor_secondary__get_record_at_positionNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            self.database.commit()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if unqlite:

    class _NoSQLUnqlite(_NoSQL):
        def setUp(self):
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()

    class Cursor_nosqlUnqlite(_NoSQLUnqlite):
        test_01 = Cursor_nosql.t01
        test_02 = Cursor_nosql.t02___init__
        test_03 = Cursor_nosql.t03_close
        test_04 = Cursor_nosql.t04_get_converted_partial
        test_05 = Cursor_nosql.t05_get_partial_with_wildcard
        test_06 = Cursor_nosql.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_nosql.t07_refresh_recordset

    class Cursor_primaryUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            Cursor_primary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03___init__
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_12 = Cursor_primary.t12_last
        test_13 = Cursor_primary.t13_nearest
        test_14 = Cursor_primary.t14_next_01
        test_15 = Cursor_primary.t15_next_02
        test_16 = Cursor_primary.t16_next_03
        test_17 = Cursor_primary.t17_prev_01
        test_18 = Cursor_primary.t18_prev_02
        test_19 = Cursor_primary.t19_prev_03
        test_20 = Cursor_primary.t20_setat
        test_21 = Cursor_primary.t21_refresh_recordset

    class Cursor_primary__get_record_at_positionUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            CursorPrimaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = (
            CursorPrimaryGetRecordAtPosition.t01_get_record_at_position_01
        )

    class Cursor_secondaryUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_05 = Cursor_secondary.t05_count_records_01
        test_06 = Cursor_secondary.t06_count_records_02
        test_07 = Cursor_secondary.t07_first_01
        test_08 = Cursor_secondary.t08_first_02
        test_09 = Cursor_secondary.t09_first_03
        test_10 = Cursor_secondary.t10_first_04
        test_11 = Cursor_secondary.t11_get_position_of_record_01
        test_12 = Cursor_secondary.t12_get_position_of_record_02
        test_13 = Cursor_secondary.t13_get_position_of_record_03
        test_14 = Cursor_secondary.t14_get_position_of_record_04
        test_30 = Cursor_secondary.t30_last_01
        test_31 = Cursor_secondary.t31_last_02
        test_32 = Cursor_secondary.t32_last_03
        test_33 = Cursor_secondary.t33_last_04
        test_34 = Cursor_secondary.t34_nearest_01
        test_35 = Cursor_secondary.t35_nearest_02
        test_36 = Cursor_secondary.t36_nearest_03
        test_37 = Cursor_secondary.t37_nearest_04
        test_38 = Cursor_secondary.t38_next_01
        test_39 = Cursor_secondary.t39_next_02
        test_40 = Cursor_secondary.t40_next_03
        test_41 = Cursor_secondary.t41_next_04
        test_44 = Cursor_secondary.t44_prev_01
        test_45 = Cursor_secondary.t45_prev_02
        test_46 = Cursor_secondary.t46_prev_05
        test_47 = Cursor_secondary.t47_prev_06
        test_50 = Cursor_secondary.t50_setat_01
        test_51 = Cursor_secondary.t51_setat_02
        test_52 = Cursor_secondary.t52_setat_03
        test_53 = Cursor_secondary.t53_setat_04
        test_54 = Cursor_secondary.t54_setat_05
        test_55 = Cursor_secondary.t55_setat_06
        test_56 = Cursor_secondary.t56_set_partial_key
        test_60 = Cursor_secondary.t60_set_current_segment_table
        test_61 = Cursor_secondary.t61_refresh_recordset
        test_70 = Cursor_secondary.t70_get_unique_primary_for_index_key

    class Cursor_secondary__get_record_at_positionUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            self.database.commit()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if vedis:

    class _NoSQLVedis(_NoSQL):
        def setUp(self):
            self._oda = vedis, vedis.Vedis, None
            super().setUp()

    class Cursor_nosqlVedis(_NoSQLVedis):
        test_01 = Cursor_nosql.t01
        test_02 = Cursor_nosql.t02___init__
        test_03 = Cursor_nosql.t03_close
        test_04 = Cursor_nosql.t04_get_converted_partial
        test_05 = Cursor_nosql.t05_get_partial_with_wildcard
        test_06 = Cursor_nosql.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_nosql.t07_refresh_recordset

    class Cursor_primaryVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            Cursor_primary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03___init__
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_12 = Cursor_primary.t12_last
        test_13 = Cursor_primary.t13_nearest
        test_14 = Cursor_primary.t14_next_01
        test_15 = Cursor_primary.t15_next_02
        test_16 = Cursor_primary.t16_next_03
        test_17 = Cursor_primary.t17_prev_01
        test_18 = Cursor_primary.t18_prev_02
        test_19 = Cursor_primary.t19_prev_03
        test_20 = Cursor_primary.t20_setat
        test_21 = Cursor_primary.t21_refresh_recordset

    class Cursor_primary__get_record_at_positionVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            CursorPrimaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = (
            CursorPrimaryGetRecordAtPosition.t01_get_record_at_position_01
        )

    class Cursor_secondaryVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_05 = Cursor_secondary.t05_count_records_01
        test_06 = Cursor_secondary.t06_count_records_02
        test_07 = Cursor_secondary.t07_first_01
        test_08 = Cursor_secondary.t08_first_02
        test_09 = Cursor_secondary.t09_first_03
        test_10 = Cursor_secondary.t10_first_04
        test_11 = Cursor_secondary.t11_get_position_of_record_01
        test_12 = Cursor_secondary.t12_get_position_of_record_02
        test_13 = Cursor_secondary.t13_get_position_of_record_03
        test_14 = Cursor_secondary.t14_get_position_of_record_04
        test_30 = Cursor_secondary.t30_last_01
        test_31 = Cursor_secondary.t31_last_02
        test_32 = Cursor_secondary.t32_last_03
        test_33 = Cursor_secondary.t33_last_04
        test_34 = Cursor_secondary.t34_nearest_01
        test_35 = Cursor_secondary.t35_nearest_02
        test_36 = Cursor_secondary.t36_nearest_03
        test_37 = Cursor_secondary.t37_nearest_04
        test_38 = Cursor_secondary.t38_next_01
        test_39 = Cursor_secondary.t39_next_02
        test_40 = Cursor_secondary.t40_next_03
        test_41 = Cursor_secondary.t41_next_04
        test_44 = Cursor_secondary.t44_prev_01
        test_45 = Cursor_secondary.t45_prev_02
        test_46 = Cursor_secondary.t46_prev_05
        test_47 = Cursor_secondary.t47_prev_06
        test_50 = Cursor_secondary.t50_setat_01
        test_51 = Cursor_secondary.t51_setat_02
        test_52 = Cursor_secondary.t52_setat_03
        test_53 = Cursor_secondary.t53_setat_04
        test_54 = Cursor_secondary.t54_setat_05
        test_55 = Cursor_secondary.t55_setat_06
        test_56 = Cursor_secondary.t56_set_partial_key
        test_60 = Cursor_secondary.t60_set_current_segment_table
        test_61 = Cursor_secondary.t61_refresh_recordset
        test_70 = Cursor_secondary.t70_get_unique_primary_for_index_key

    class Cursor_secondary__get_record_at_positionVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            self.database.commit()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if gnu_module:
        runner().run(loader(Cursor_nosqlGnu))
        runner().run(loader(Cursor_primaryGnu))
        runner().run(loader(Cursor_primary__get_record_at_positionGnu))
        runner().run(loader(Cursor_secondaryGnu))
        runner().run(loader(Cursor_secondary__get_record_at_positionGnu))
    if ndbm_module:
        runner().run(loader(Cursor_nosqlNdbm))
        runner().run(loader(Cursor_primaryNdbm))
        runner().run(loader(Cursor_primary__get_record_at_positionNdbm))
        runner().run(loader(Cursor_secondaryNdbm))
        runner().run(loader(Cursor_secondary__get_record_at_positionNdbm))
    if unqlite:
        runner().run(loader(Cursor_nosqlUnqlite))
        runner().run(loader(Cursor_primaryUnqlite))
        runner().run(loader(Cursor_primary__get_record_at_positionUnqlite))
        runner().run(loader(Cursor_secondaryUnqlite))
        runner().run(loader(Cursor_secondary__get_record_at_positionUnqlite))
    if vedis:
        runner().run(loader(Cursor_nosqlVedis))
        runner().run(loader(Cursor_primaryVedis))
        runner().run(loader(Cursor_primary__get_record_at_positionVedis))
        runner().run(loader(Cursor_secondaryVedis))
        runner().run(loader(Cursor_secondary__get_record_at_positionVedis))
