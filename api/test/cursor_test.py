# cursor_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""cursor tests"""

import unittest

from .. import cursor


class Cursor(unittest.TestCase):

    def setUp(self):
        class D:
            pass
        self.d = D()

    def tearDown(self):
        pass

    def test___init___01(self):
        c = cursor.Cursor(self.d)
        self.assertIsInstance(c, cursor.Cursor)
        self.assertEqual(c._cursor, None)
        self.assertIs(c._dbset, self.d)
        self.assertEqual(c._partial, None)
        self.assertEqual(len(c.__dict__), 3)

    def test___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'dbset'")),
            cursor.Cursor)

    def test_close_01(self):
        c = cursor.Cursor(self.d)
        self.assertEqual(c.close(), None)
        self.assertEqual(c._cursor, None)
        self.assertEqual(c._dbset, None)
        self.assertEqual(c._partial, None)
        self.assertEqual(len(c.__dict__), 3)

    def test_count_records_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'count_records not implemented',
            cursor.Cursor(self.d).count_records)

    def test_database_cursor_exists_01(self):
        self.assertEqual(cursor.Cursor(self.d).database_cursor_exists(), False)

    def test_first_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'first not implemented',
            cursor.Cursor(self.d).first)

    def test_get_position_of_record_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'get_position_of_record not implemented',
            cursor.Cursor(self.d).get_position_of_record)

    def test_get_record_at_position_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'get_record_at_position not implemented',
            cursor.Cursor(self.d).get_record_at_position)

    def test_last_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'last not implemented',
            cursor.Cursor(self.d).last)

    def test_nearest_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'nearest not implemented',
            cursor.Cursor(self.d).nearest,
            *(None,))

    def test_next_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'next not implemented',
            cursor.Cursor(self.d).next)

    def test_prev_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'prev not implemented',
            cursor.Cursor(self.d).prev)

    def test_refresh_recordset_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'refresh_recordset not implemented',
            cursor.Cursor(self.d).refresh_recordset)

    def test_setat_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'setat not implemented',
            cursor.Cursor(self.d).setat,
            *(None,))

    def test_set_partial_key_01(self):
        c = cursor.Cursor(self.d)
        self.assertEqual(c.set_partial_key('aa'), None)
        self.assertEqual(c._partial, None)

    def test_get_partial_01(self):
        c = cursor.Cursor(self.d)
        c._partial = 'aa'
        self.assertEqual(c.get_partial(), 'aa')

    def test_get_converted_partial_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'get_converted_partial not implemented',
            cursor.Cursor(self.d).get_converted_partial)

    def test_get_partial_with_wildcard_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'get_partial_with_wildcard not implemented',
            cursor.Cursor(self.d).get_partial_with_wildcard)

    def test_get_converted_partial_with_wildcard_01(self):
        self.assertRaisesRegex(
            cursor.CursorError,
            'get_converted_partial_with_wildcard not implemented',
            cursor.Cursor(self.d).get_converted_partial_with_wildcard)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Cursor))
