# test__sqlite_cursor.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_sqlite cursor tests"""

import unittest
try:
    import sqlite3
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None

from .. import _sqlite
from .. import filespec
from .. import recordset


class _SQLite(unittest.TestCase):

    def setUp(self):
        class _D(_sqlite.Database):
            pass
        self._D = _D
        self.database = self._D(
            filespec.FileSpec(**{'file1': {'field1'}}),
            segment_size_bytes=None)
        self.database.open_database(dbe_module)

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None


class Cursor_sqlite(_SQLite):

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) takes from 2 to 5 positional arguments ",
                "but 6 were given",
                )),
            _sqlite.Cursor,
            *(None, None, None, None, None),
            )
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "close\(\) takes 1 positional argument but 2 were given",
                )),
            cursor.close,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_converted_partial\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            cursor.get_converted_partial,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_partial_with_wildcard\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            cursor.get_partial_with_wildcard,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_converted_partial_with_wildcard\(\) takes 1 positional ",
                "argument but 2 were given",
                )),
            cursor.get_converted_partial_with_wildcard,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                "but 3 were given",
                )),
            cursor.refresh_recordset,
            *(None, None),
            )

    def test_02___init__(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor._most_recent_row_read, False)
        self.assertEqual(cursor._table, None)
        self.assertEqual(cursor._file, None)
        self.assertEqual(cursor._current_segment, None)
        self.assertEqual(cursor._current_segment_number, None)
        self.assertEqual(cursor._current_record_number_in_segment, None)
        self.assertIsInstance(cursor._cursor,
                              self.database.dbenv.cursor().__class__)

    def test_03_close(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        cursor.close()
        self.assertEqual(cursor._cursor, None)

    def test_04_get_converted_partial(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor.get_converted_partial(), None)

    def test_05_get_partial_with_wildcard(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
            "get_partial_with_wildcard not implemented",
            cursor.get_partial_with_wildcard,
            )

    def test_06_get_converted_partial_with_wildcard(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertRaisesRegex(
            TypeError,
            "sequence item 0: expected str instance, NoneType found",
            cursor.get_converted_partial_with_wildcard,
            )
        cursor._partial = 'part'
        self.assertEqual(cursor.get_converted_partial_with_wildcard(), 'part*')

    def test_07_refresh_recordset(self):
        cursor = _sqlite.Cursor(self.database.dbenv)
        self.assertEqual(cursor.refresh_recordset(), None)


class Cursor_primary(_SQLite):

    def setUp(self):
        super().setUp()
        self.cursor = _sqlite.CursorPrimary(
            self.database.dbenv,
            table=self.database.table['file1'][0],
            ebm=self.database.ebm_control['file1'].ebm_table,
            file='file1')

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) takes from 2 to 3 positional arguments ",
                "but 4 were given",
                )),
            _sqlite.CursorPrimary,
            *(None, None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "count_records\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.count_records,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "first\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.first,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_position_of_record\(\) takes from 1 to 2 positional ",
                "arguments but 3 were given",
                )),
            self.cursor.get_position_of_record,
            *(None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_record_at_position\(\) takes from 1 to 2 positional ",
                "arguments but 3 were given",
                )),
            self.cursor.get_record_at_position,
            *(None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "last\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.last,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "nearest\(\) missing 1 required ",
                "positional argument: 'key'",
                )),
            self.cursor.nearest,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "next\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.next,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "prev\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.prev,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "setat\(\) missing 1 required ",
                "positional argument: 'record'",
                )),
            self.cursor.setat,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                "but 3 were given",
                )),
            self.cursor.refresh_recordset,
            *(None, None),
            )

    def test_02___init__(self):
        self.assertEqual(self.cursor._ebm, 'file1__ebm')

    def test_03_count_records(self):
        self.assertEqual(self.cursor.count_records(), 0)

    def test_04_first(self):
        self.assertEqual(self.cursor.first(), None)

    def test_05_get_position_of_record_01(self):
        self.assertEqual(self.cursor.get_position_of_record(), 0)

    def test_06_get_position_of_record_02(self):
        self.assertEqual(self.cursor.get_position_of_record((5, None)), 0)

    def test_07_get_position_of_record_03(self):
        # With a populated databse.
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
        self.assertEqual(self.cursor.nearest('d'), None)

    def test_13_next_01(self):
        self.assertEqual(self.cursor._most_recent_row_read, False)
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_14_next_02(self):
        self.cursor._most_recent_row_read = None
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_15_next_03(self):
        self.cursor._most_recent_row_read = (10, None)
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_16_prev_01(self):
        self.assertEqual(self.cursor._most_recent_row_read, False)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_17_prev_02(self):
        self.cursor._most_recent_row_read = None
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_18_prev_03(self):
        self.cursor._most_recent_row_read = (10, None)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor._most_recent_row_read, None)

    def test_19_setat(self):
        self.assertEqual(self.cursor.setat((10, None)), None)

    def test_20_refresh_recordset(self):
        self.cursor.refresh_recordset()


class Cursor_secondary(_SQLite):

    def setUp(self):
        super().setUp()
        segments = (
            b'\x7f\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
            b'\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
            b'\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00',
            b'\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00\x00\x00',
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00\x00\x00',
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\x00\x00',
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff',
            b'\x00\x40\x00\x41',
            b'\x00\x42\x00\x43\x00\x44',
            )
        self.segments = {}
        keys = (
            'a_o',
            'aa_o',
            'ba_o',
            'bb_o',
            'c_o',
            'cep',
            'deq',
            )
        self.keyvalues = {}
        key_statement = " ".join((
            "insert into file1_field1 (",
            "field1", ",",
            "Segment", ",",
            "RecordCount", ",",
            "file1", ")",
            "values ( ? , ? , ? , ? )",
            ))
        cursor = self.database.dbenv.cursor()
        try:
            for s in segments:
                cursor.execute(
                    "insert into file1__segment ( RecordNumbers ) values ( ? )",
                    (s,))
                self.segments[
                    cursor.execute(
                        'select last_insert_rowid() from file1__segment'
                        ).fetchone()[0]] = s
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                cursor.execute(
                    key_statement,
                    (k, 0, 32 if e else 31, self.keyvalues[k]))
            cursor.execute(
                key_statement,
                ('ba_o', 1, 2, 8))
            self.keyvalues['twy'] = 9
            cursor.execute(
                key_statement,
                ('cep', 1, 3, 9))
            cursor.execute(
                key_statement,
                ('ba_o', 2, 1, 50))
            cursor.execute(
                key_statement,
                ('cep', 2, 1, 100))
        finally:
            cursor.close()
        self.cursor = _sqlite.CursorSecondary(
            self.database.dbenv,
            table=self.database.table['file1_field1'][0],
            segment=self.database.segment_table['file1'],
            file='file1',
            field='field1')

    def tearDown(self):
        self.cursor.close()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) takes from 2 to 4 positional arguments ",
                "but 5 were given",
                )),
            _sqlite.CursorSecondary,
            *(None, None, None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_segment_records\(\) missing 1 required ",
                "positional argument: 'rownumber'",
                )),
            self.cursor.get_segment_records,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "count_records\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.count_records,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "first\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.first,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_position_of_record\(\) takes from 1 to 2 positional ",
                "arguments but 3 were given",
                )),
            self.cursor.get_position_of_record,
            *(None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "get_record_at_position\(\) takes from 1 to 2 positional ",
                "arguments but 3 were given",
                )),
            self.cursor.get_record_at_position,
            *(None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "last\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.last,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "nearest\(\) missing 1 required ",
                "positional argument: 'key'",
                )),
            self.cursor.nearest,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "next\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.next,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "prev\(\) takes 1 positional argument ",
                "but 2 were given",
                )),
            self.cursor.prev,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "setat\(\) missing 1 required ",
                "positional argument: 'record'",
                )),
            self.cursor.setat,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "set_partial_key\(\) missing 1 required ",
                "positional argument: 'partial'",
                )),
            self.cursor.set_partial_key,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "_get_segment\(\) missing 4 required ",
                "positional arguments: 'key', 'segment_number', 'count', ",
                "and 'record_number'",
                )),
            self.cursor._get_segment,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "set_current_segment\(\) missing 1 required ",
                "positional argument: 'segment_reference'",
                )),
            self.cursor.set_current_segment,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                "but 3 were given",
                )),
            self.cursor.refresh_recordset,
            *(None, None),
            )

    def test_02___init__(self):
        self.assertEqual(self.cursor._field, 'field1')
        self.assertEqual(self.cursor._segment, 'file1__segment')

    def test_03_get_segment_records_01(self):
        self.assertRaisesRegex(
            _sqlite.DatabaseError,
            "Segment record 0 missing in 'file1__segment'",
            self.cursor.get_segment_records,
            *(0,),
            )

    def test_04_get_segment_records_02(self):
        s = self.cursor.get_segment_records(2)
        self.assertEqual(s, self.segments[2])

    def test_05_count_records_01(self):
        s = self.cursor.count_records()
        self.assertEqual(s, 230)

    def test_06_count_records_02(self):
        self.cursor._partial = 'b'
        s = self.cursor.count_records()
        self.assertEqual(s, 67)

    def test_07_first_01(self):
        self.cursor._partial = False
        s = self.cursor.first()
        self.assertEqual(s, None)

    def test_08_first_02(self):
        s = self.cursor.first()
        self.assertEqual(s, ('a_o', 1))

    def test_09_first_03(self):
        self.cursor._partial = 'b'
        s = self.cursor.first()
        self.assertEqual(s, ('ba_o', 40))

    def test_10_first_04(self):
        self.cursor._partial = 'A'
        s = self.cursor.first()
        self.assertEqual(s, None)

    def test_11_get_position_of_record_01(self):
        s = self.cursor.get_position_of_record()
        self.assertEqual(s, 0)

    # Tests 12, 13, and 14, drive method through all record paths in the loop
    # on 'cursor.execute(...)' between them.  Tried arguments till set that did
    # so was found.

    def test_12_get_position_of_record_02(self):
        s = self.cursor.get_position_of_record(('ba_o', 300))
        self.assertEqual(s, 97)

    def test_13_get_position_of_record_03(self):
        self.cursor._partial = 'b'
        s = self.cursor.get_position_of_record(('ba_o', 20))
        self.assertEqual(s, 0)

    def test_14_get_position_of_record_04(self):
        s = self.cursor.get_position_of_record(('cep', 150))
        self.assertEqual(s, 194)

    def test_15_get_record_at_position_01(self):
        s = self.cursor.get_record_at_position()
        self.assertEqual(s, None)

    # Tests 16, 17, 18, and 14, drive method through all record paths in the
    # loop on 'cursor.execute(...)' between them.  Tried arguments till set
    # that did so was found.

    def test_16_get_record_at_position_02(self):
        s = self.cursor.get_record_at_position(1)
        self.assertEqual(s, ('a_o', 2))

    def test_17_get_record_at_position_03(self):
        s = self.cursor.get_record_at_position(196)
        self.assertEqual(s, ('cep', 196))

    def test_18_get_record_at_position_04(self):
        s = self.cursor.get_record_at_position(198)
        self.assertEqual(s, ('cep', 356))

    def test_19_get_record_at_position_05(self):
        s = self.cursor.get_record_at_position(400)
        self.assertEqual(s, None)

    # Problem probably fixed.  See addition to get_record_number_at_position's
    # docstring on meaning of position argument.  Method get_record_at_position
    # in ._db and ._sqlite modules adjusted to fit.
    def test_20_get_record_at_position_06(self):
        s = self.cursor.get_record_at_position(-2)
        self.assertEqual(s, ('deq', 126))

    def test_30_last_01(self):
        self.cursor._partial = False
        s = self.cursor.last()
        self.assertEqual(s, None)

    def test_31_last_02(self):
        s = self.cursor.last()
        self.assertEqual(s, ('deq', 127))

    def test_32_last_03(self):
        self.cursor._partial = 'b'
        s = self.cursor.last()
        self.assertEqual(s, ('bb_o', 79))

    def test_33_last_04(self):
        self.cursor._partial = 'A'
        s = self.cursor.last()
        self.assertEqual(s, None)

    def test_34_nearest_01(self):
        self.assertEqual(self.cursor.nearest('d'), ('deq', 104))

    def test_35_nearest_02(self):
        self.cursor._partial = False
        self.assertEqual(self.cursor.nearest('d'), None)

    def test_36_nearest_03(self):
        self.cursor._partial = 'b'
        self.assertEqual(self.cursor.nearest('bb'), ('bb_o', 56))

    def test_37_nearest_04(self):
        self.assertEqual(self.cursor.nearest('z'), None)

    def test_38_next_01(self):
        s = self.cursor.next()
        self.assertEqual(s, ('a_o', 1))

    def test_39_next_02(self):
        self.cursor._most_recent_row_read = None
        s = self.cursor.next()
        self.assertEqual(s, None)

    def test_40_next_03(self):
        s = self.cursor.next()
        self.assertEqual(s, ('a_o', 1))
        s = self.cursor.next()
        self.assertEqual(s, ('a_o', 2))

    def test_41_next_04(self):
        while True:
            if not self.cursor.next():
                break

    def test_42_next_05(self):
        self.cursor.next()
        self.cursor._partial = False
        while True:
            if not self.cursor.next():
                break

    def test_43_next_06(self):
        self.cursor._partial = 'd'
        while True:
            if not self.cursor.next():
                break

    def test_44_prev_01(self):
        s = self.cursor.prev()
        self.assertEqual(s, ('deq', 127))

    def test_45_prev_02(self):
        self.cursor._most_recent_row_read = None
        s = self.cursor.prev()
        self.assertEqual(s, None)

    def test_46_prev_03(self):
        s = self.cursor.prev()
        self.assertEqual(s, ('deq', 127))
        s = self.cursor.prev()
        self.assertEqual(s, ('deq', 126))

    def test_47_prev_04(self):
        while True:
            if not self.cursor.prev():
                break

    def test_48_prev_05(self):
        self.cursor.prev()
        self.cursor._partial = False
        while True:
            if not self.cursor.prev():
                break

    def test_49_prev_06(self):
        self.cursor._partial = 'd'
        while True:
            if not self.cursor.prev():
                break

    def test_50_setat_01(self):
        self.cursor._partial = False
        s = self.cursor.setat(('cep', 100))
        self.assertEqual(s, None)

    def test_51_setat_02(self):
        self.cursor._partial = 'a'
        s = self.cursor.setat(('cep', 100))
        self.assertEqual(s, None)

    def test_52_setat_03(self):
        self.cursor._partial = 'c'
        s = self.cursor.setat(('cep', 100))
        self.assertEqual(s, ('cep', 100))

    def test_53_setat_04(self):
        s = self.cursor.setat(('cep', 100))
        self.assertEqual(s, ('cep', 100))

    def test_54_setat_05(self):
        s = self.cursor.setat(('cep', 500))
        self.assertEqual(s, None)

    def test_55_setat_06(self):
        s = self.cursor.setat(('cep', 50))
        self.assertEqual(s, None)

    def test_56_set_partial_key(self):
        self.cursor.set_partial_key('ce')
        self.assertEqual(self.cursor._partial, 'ce')

    def test_57__get_segment_01(self):
        s = self.cursor._get_segment('cep', 2, 1, 100)
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def test_58__get_segment_02(self):
        self.assertEqual(self.cursor._current_segment_number, None)
        s = self.cursor._get_segment('aa_o', 0, 24, 2)
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)

    def test_59__get_segment_03(self):
        self.assertEqual(self.cursor._current_segment_number, None)
        s = self.cursor._get_segment('cep', 1, 3, 9)
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(self.cursor._current_segment_number, None)
        self.cursor._current_segment = s
        self.cursor._current_segment_number = 1
        t = self.cursor._get_segment('cep', 1, 3, 9)
        self.assertIs(s, t)

    def test_60_set_current_segment(self):
        s = self.cursor.set_current_segment(('cep', 2, 1, 100))
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
        self.assertIs(s, self.cursor._current_segment)
        self.assertEqual(self.cursor._current_segment_number, 2)

    def test_61_refresh_recordset(self):
        self.cursor.refresh_recordset()


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in sqlite3, apsw:
        if dbe_module is None:
            continue
        runner().run(loader(Cursor_sqlite))
        runner().run(loader(Cursor_primary))
        runner().run(loader(Cursor_secondary))
