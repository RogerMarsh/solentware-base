# test__sqlite_cursor.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_sqlite cursor tests."""

import unittest

try:
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None

from .. import _sqlite
from .. import filespec
from .. import recordset
from ..segmentsize import SegmentSize


class _SQLite(unittest.TestCase):
    def setUp(self):
        class _D(_sqlite.Database):
            pass

        self._D = _D
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.open_database()

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None


class Cursor_sqlite(_SQLite):
    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 5 positional arguments ",
                    "but 6 were given$",
                )
            ),
            _sqlite.Cursor,
            *(None, None, None, None, None),
        )
        cursor = _sqlite.Cursor(self.database.dbenv)
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
                    r"get_converted_partial_with_wildcard\(\) takes 1 "
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
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor._table, None)
        self.assertEqual(cursor._file, None)
        self.assertEqual(cursor._current_segment, None)
        self.assertEqual(cursor.current_segment_number, None)
        self.assertEqual(cursor._current_record_number_in_segment, None)
        self.assertIsInstance(
            cursor._cursor, self.database.dbenv.cursor().__class__
        )

    def t04_get_converted_partial(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor.get_converted_partial(), None)

    def t05_get_partial_with_wildcard(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
            "get_partial_with_wildcard not implemented$",
            cursor.get_partial_with_wildcard,
        )

    def t06_get_converted_partial_with_wildcard(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertRaisesRegex(
            TypeError,
            "sequence item 0: expected str instance, NoneType found$",
            cursor.get_converted_partial_with_wildcard,
        )
        cursor._partial = "part"
        self.assertEqual(cursor.get_converted_partial_with_wildcard(), "part*")

    def t07_refresh_recordset(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor.refresh_recordset(), None)


class Cursor_primary(_SQLite):
    def setUp(self):
        super().setUp()
        self.cursor = _sqlite.CursorPrimary(
            self.database.dbenv,
            table=self.database.table["file1"],
            ebm=self.database.ebm_control["file1"].ebm_table,
            file="file1",
        )

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given$",
                )
            ),
            _sqlite.CursorPrimary,
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
                    r"refresh_recordset\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.cursor.refresh_recordset,
            *(None, None),
        )

    def t02___init__(self):
        self.assertEqual(self.cursor._most_recent_row_read, False)
        self.assertEqual(self.cursor._ebm, "file1__ebm")

    def t03_close(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        cursor.close()
        self.assertEqual(cursor._cursor, None)

    def t04_count_records(self):
        self.assertEqual(self.cursor.count_records(), 0)

    def t05_first(self):
        self.assertEqual(self.cursor.first(), None)

    def t06_get_position_of_record_01(self):
        self.assertEqual(self.cursor.get_position_of_record(), 0)

    def t06_get_position_of_record_02(self):
        self.assertEqual(self.cursor.get_position_of_record((5, None)), 0)

    def t06_get_position_of_record_03(self):
        self.create_ebm()
        self.create_ebm_extra(2)
        # Records 304 and 317, in segment 3, have bits set.
        self.create_ebm_extra(
            3,
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x80\x04",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        self.assertEqual(self.cursor.get_position_of_record((304, None)), 256)
        self.assertEqual(self.cursor.get_position_of_record((310, None)), 257)
        self.assertEqual(self.cursor.get_position_of_record((317, None)), 257)
        self.assertEqual(self.cursor.get_position_of_record((319, None)), 258)
        self.assertEqual(self.cursor.get_position_of_record((320, None)), 258)

    def t08_get_record_at_position_01(self):
        self.assertEqual(self.cursor.get_record_at_position(), None)

    def t08_get_record_at_position_02(self):
        self.assertEqual(self.cursor.get_record_at_position(-1), None)

    def t08_get_record_at_position_03(self):
        self.assertEqual(self.cursor.get_record_at_position(0), None)

    def t08_get_record_at_position_04(self):
        # Records 1 to 255, 299, 304 and 317, in segment 3, exist.
        for start, stop in ((1, 256), (299, 300), (304, 305), (317, 318)):
            for record_number in range(start, stop):
                self.create_record(record_number)
        self.assertEqual(self.cursor.get_record_at_position(260), None)
        self.assertEqual(self.cursor.get_record_at_position(259), None)
        self.assertEqual(
            self.cursor.get_record_at_position(258), (317, str(317))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(257), (304, str(304))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(256), (299, str(299))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(255), (255, str(255))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(254), (254, str(254))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(128), (128, str(128))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(127), (127, str(127))
        )
        self.assertEqual(
            self.cursor.get_record_at_position(126), (126, str(126))
        )
        self.assertEqual(self.cursor.get_record_at_position(1), (1, str(1)))
        # Same as self.cursor.get_record_at_position(-259)
        self.assertEqual(self.cursor.get_record_at_position(0), None)

    def t08_get_record_at_position_05(self):
        # Records 1 to 255, 299, 304 and 317, in segment 3, exist.
        for start, stop in ((1, 256), (299, 300), (304, 305), (317, 318)):
            for record_number in range(start, stop):
                self.create_record(record_number)
        self.assertEqual(self.cursor.get_record_at_position(-260), None)
        self.assertEqual(self.cursor.get_record_at_position(-259), None)
        self.assertEqual(self.cursor.get_record_at_position(-258), (1, str(1)))
        self.assertEqual(self.cursor.get_record_at_position(-257), (2, str(2)))
        self.assertEqual(self.cursor.get_record_at_position(-256), (3, str(3)))
        self.assertEqual(
            self.cursor.get_record_at_position(-1), (317, str(317))
        )

    def t12_last(self):
        self.assertEqual(self.cursor.last(), None)

    def t13_nearest(self):
        self.assertEqual(self.cursor.nearest("d"), None)

    def t14_next_01(self):
        self.assertEqual(self.cursor._most_recent_row_read, False)
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t15_next_02(self):
        self.cursor._most_recent_row_read = None
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t16_next_03(self):
        self.cursor._most_recent_row_read = (10, None)
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t17_prev_01(self):
        self.assertEqual(self.cursor._most_recent_row_read, False)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t18_prev_02(self):
        self.cursor._most_recent_row_read = None
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t19_prev_03(self):
        self.cursor._most_recent_row_read = (10, None)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def t20_setat(self):
        self.assertEqual(self.cursor.setat((10, None)), None)

    def t21_refresh_recordset(self):
        self.cursor.refresh_recordset()

    def create_ebm(self, bmb=None):
        if bmb is None:
            bmb = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = 1, bmb
        cursor.execute(statement, values)

    def create_ebm_extra(self, segment, bmb=None):
        if bmb is None:
            bmb = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1__ebm",
                "(",
                "file1__ebm",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = (segment, bmb)
        cursor.execute(statement, values)

    def create_record(self, record_number):
        cursor = self.database.dbenv.cursor()
        statement = " ".join(
            (
                "insert into",
                "file1",
                "(",
                "file1",
                ",",
                "Value",
                ")",
                "values ( ? , ? )",
            )
        )
        values = record_number, str(record_number)
        cursor.execute(statement, values)


class Cursor_secondary(_SQLite):
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\x7f\xff\xff\xff\x00\x00\x00\x00",
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
        key_statement = " ".join(
            (
                "insert into file1_field1 (",
                "field1",
                ",",
                "Segment",
                ",",
                "RecordCount",
                ",",
                "file1",
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )
        cursor = self.database.dbenv.cursor()
        try:
            for s in segments:
                cursor.execute(
                    "".join(
                        (
                            "insert into file1__segment ",
                            "( RecordNumbers ) values ( ? )",
                        )
                    ),
                    (s,),
                )
                self.segments[
                    cursor.execute(
                        "select last_insert_rowid() from file1__segment"
                    ).fetchone()[0]
                ] = s
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                cursor.execute(
                    key_statement, (k, 0, 24 if e else 31, self.keyvalues[k])
                )
            cursor.execute(key_statement, ("ba_o", 1, 2, 8))
            self.keyvalues["twy"] = 9
            cursor.execute(key_statement, ("twy", 1, 3, 9))
            cursor.execute(key_statement, ("ba_o", 2, 1, 50))
            cursor.execute(key_statement, ("cep", 2, 1, 100))
        finally:
            cursor.close()
        self.cursor = _sqlite.CursorSecondary(
            self.database.dbenv,
            table=self.database.table["file1_field1"],
            segment=self.database.segment_table["file1"],
            file="file1",
            field="field1",
        )

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 4 positional arguments ",
                    "but 5 were given$",
                )
            ),
            _sqlite.CursorSecondary,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_segment_records\(\) missing 1 required ",
                    "positional argument: 'rownumber'$",
                )
            ),
            self.cursor.get_segment_records,
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
                    r"_get_segment\(\) missing 4 required ",
                    "positional arguments: 'key', 'segment_number', 'count', ",
                    "and 'record_number'$",
                )
            ),
            self.cursor._get_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"set_current_segment\(\) missing 1 required ",
                    "positional argument: 'segment_reference'$",
                )
            ),
            self.cursor.set_current_segment,
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
        self.assertEqual(self.cursor._field, "field1")
        self.assertEqual(self.cursor._segment, "file1__segment")

    def t03_get_segment_records_01(self):
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
            "Segment record 0 missing in 'file1__segment'$",
            self.cursor.get_segment_records,
            *(0,),
        )

    def t04_get_segment_records_02(self):
        s = self.cursor.get_segment_records(2)
        self.assertEqual(s, self.segments[2])

    def t05_count_records_01(self):
        s = self.cursor.count_records()
        self.assertEqual(s, 182)

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
        self.assertEqual(s, ("a_o", 1))

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

    # Tests 12, 13, and 14, drive method through all record paths in the loop
    # on 'cursor.execute(...)' between them.  Tried arguments till set that did
    # so was found.

    def t12_get_position_of_record_02(self):
        s = self.cursor.get_position_of_record(("ba_o", 300))
        self.assertEqual(s, 81)

    def t13_get_position_of_record_03(self):
        self.cursor._partial = "b"
        s = self.cursor.get_position_of_record(("ba_o", 20))
        self.assertEqual(s, 1)

    def t14_get_position_of_record_04(self):
        s = self.cursor.get_position_of_record(("cep", 150))
        self.assertEqual(s, 154)

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

    def t39_next_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.next(), None)

    def t40_next_03(self):
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
        for i in range(31, 0, -1):
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
        self.cursor.set_partial_key("ce")
        self.assertEqual(self.cursor._partial, "ce")

    def t57__get_segment_01(self):
        s = self.cursor._get_segment("cep", 2, 1, 100)
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def t58__get_segment_02(self):
        self.assertEqual(self.cursor.current_segment_number, None)
        s = self.cursor._get_segment("aa_o", 0, 24, 2)
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)

    def t59__get_segment_03(self):
        self.assertEqual(self.cursor.current_segment_number, None)
        s = self.cursor._get_segment("cep", 1, 3, 9)
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(self.cursor.current_segment_number, None)
        self.cursor._current_segment = s
        self.cursor.current_segment_number = 1
        t = self.cursor._get_segment("cep", 1, 3, 9)
        self.assertIs(s, t)

    def t60_set_current_segment(self):
        s = self.cursor.set_current_segment(("cep", 2, 1, 100))
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
        self.assertIs(s, self.cursor._current_segment)
        self.assertEqual(self.cursor.current_segment_number, 2)

    def t61_refresh_recordset(self):
        self.cursor.refresh_recordset()


class CursorSecondaryGetRecordAtPosition(_SQLite):
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\x7f\xff\xff\xff\x00\x00\x00\x00",
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
        self.segments = {}
        key = "a_o"
        key_statement = " ".join(
            (
                "insert into file1_field1 (",
                "field1",
                ",",
                "Segment",
                ",",
                "RecordCount",
                ",",
                "file1",
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )
        cursor = self.database.dbenv.cursor()
        try:
            for s in segments:
                cursor.execute(
                    "".join(
                        (
                            "insert into file1__segment ",
                            "( RecordNumbers ) values ( ? )",
                        )
                    ),
                    (s,),
                )
                self.segments[
                    cursor.execute(
                        "select last_insert_rowid() from file1__segment"
                    ).fetchone()[0]
                ] = s
            cursor.execute(key_statement, (key, 0, 31, 1))
            cursor.execute(key_statement, (key, 1, 3, 2))
            cursor.execute(key_statement, (key, 2, 24, 3))
            cursor.execute(key_statement, (key, 3, 1, 50))
        finally:
            cursor.close()
        self.cursor = _sqlite.CursorSecondary(
            self.database.dbenv,
            table=self.database.table["file1_field1"],
            segment=self.database.segment_table["file1"],
            file="file1",
            field="field1",
        )

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def t20_get_record_at_position_06(self):
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


if sqlite3:

    class _SQLiteSqlite3(_SQLite):
        def open_database(self):
            self.database.open_database(sqlite3)

    class Cursor_sqliteSqlite3(_SQLiteSqlite3):
        test_01 = Cursor_sqlite.t01
        test_02 = Cursor_sqlite.t02___init__
        test_04 = Cursor_sqlite.t04_get_converted_partial
        test_05 = Cursor_sqlite.t05_get_partial_with_wildcard
        test_06 = Cursor_sqlite.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_sqlite.t07_refresh_recordset

    class Cursor_primarySqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            self.cursor = _sqlite.CursorPrimary(
                self.database.dbenv,
                table=self.database.table["file1"],
                ebm=self.database.ebm_control["file1"].ebm_table,
                file="file1",
            )

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03_close
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_08 = Cursor_primary.t08_get_record_at_position_04
        test_08 = Cursor_primary.t08_get_record_at_position_05
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
        create_ebm = Cursor_primary.create_ebm
        create_ebm_extra = Cursor_primary.create_ebm_extra
        create_record = Cursor_primary.create_record

    class Cursor_secondarySqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_03 = Cursor_secondary.t03_get_segment_records_01
        test_04 = Cursor_secondary.t04_get_segment_records_02
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
        test_57 = Cursor_secondary.t57__get_segment_01
        test_58 = Cursor_secondary.t58__get_segment_02
        test_59 = Cursor_secondary.t59__get_segment_03
        test_60 = Cursor_secondary.t60_set_current_segment
        test_61 = Cursor_secondary.t61_refresh_recordset

    class Cursor_secondary__get_record_at_positionSqlite3(_SQLiteSqlite3):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if apsw:

    class _SQLiteApsw(_SQLite):
        def open_database(self):
            self.database.open_database(apsw)

    class Cursor_sqliteApsw(_SQLiteApsw):
        test_01 = Cursor_sqlite.t01
        test_02 = Cursor_sqlite.t02___init__
        test_04 = Cursor_sqlite.t04_get_converted_partial
        test_05 = Cursor_sqlite.t05_get_partial_with_wildcard
        test_06 = Cursor_sqlite.t06_get_converted_partial_with_wildcard
        test_07 = Cursor_sqlite.t07_refresh_recordset

    class Cursor_primaryApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            self.cursor = _sqlite.CursorPrimary(
                self.database.dbenv,
                table=self.database.table["file1"],
                ebm=self.database.ebm_control["file1"].ebm_table,
                file="file1",
            )

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_primary.t01
        test_02 = Cursor_primary.t02___init__
        test_03 = Cursor_primary.t03_close
        test_04 = Cursor_primary.t04_count_records
        test_05 = Cursor_primary.t05_first
        test_06 = Cursor_primary.t06_get_position_of_record_01
        test_06 = Cursor_primary.t06_get_position_of_record_02
        test_06 = Cursor_primary.t06_get_position_of_record_03
        test_08 = Cursor_primary.t08_get_record_at_position_01
        test_08 = Cursor_primary.t08_get_record_at_position_02
        test_08 = Cursor_primary.t08_get_record_at_position_03
        test_08 = Cursor_primary.t08_get_record_at_position_04
        test_08 = Cursor_primary.t08_get_record_at_position_05
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
        create_ebm = Cursor_primary.create_ebm
        create_ebm_extra = Cursor_primary.create_ebm_extra
        create_record = Cursor_primary.create_record

    class Cursor_secondaryApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            Cursor_secondary.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_01 = Cursor_secondary.t01
        test_02 = Cursor_secondary.t02___init__
        test_03 = Cursor_secondary.t03_get_segment_records_01
        test_04 = Cursor_secondary.t04_get_segment_records_02
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
        test_57 = Cursor_secondary.t57__get_segment_01
        test_58 = Cursor_secondary.t58__get_segment_02
        test_59 = Cursor_secondary.t59__get_segment_03
        test_60 = Cursor_secondary.t60_set_current_segment
        test_61 = Cursor_secondary.t61_refresh_recordset

    class Cursor_secondary__get_record_at_positionApsw(_SQLiteApsw):
        def setUp(self):
            super().setUp()
            CursorSecondaryGetRecordAtPosition.setup_detail(self)

        def tearDown(self):
            self.cursor.close()
            super().tearDown()

        test_20 = (
            CursorSecondaryGetRecordAtPosition.t20_get_record_at_position_06
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if sqlite3:
        runner().run(loader(Cursor_sqliteSqlite3))
        runner().run(loader(Cursor_primarySqlite3))
        runner().run(loader(Cursor_secondarySqlite3))
        runner().run(loader(Cursor_secondary__get_record_at_positionSqlite3))
    if apsw:
        runner().run(loader(Cursor_sqliteApsw))
        runner().run(loader(Cursor_primaryApsw))
        runner().run(loader(Cursor_secondaryApsw))
        runner().run(loader(Cursor_secondary__get_record_at_positionApsw))
