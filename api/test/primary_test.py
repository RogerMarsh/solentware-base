# primary_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""primary tests"""

import unittest

from .. import primary


class Primary(unittest.TestCase):

    def setUp(self):
        self.primary = primary.Primary()

    def tearDown(self):
        self.primary = None

    def test___init__(self):
        self.assertEqual(self.primary.__dict__,
                         {'_clientcursors': {},
                          '_recordsets': {},
                          })
        self.assertEqual(
            [a for a in dir(self.primary) if not a.startswith('__')],
            ['_clientcursors',
             '_recordsets',
             'close',
             'file_records_under',
             'find_values',
             'get_first_primary_key_for_index_key',
             'populate_recordset_key_like',
             'populate_recordset_key_startswith',
             'unfile_records_under',
             ])

    def test_close(self):
        class C:
            def close(self):
                pass
        class A(primary.Primary, C):
            pass
        a = A()
        a._clientcursors = {C(): None}
        a._recordsets = {C(): None}
        self.assertEqual(a.close(), None)
        self.assertEqual(a._clientcursors, {})
        self.assertEqual(a._recordsets, {})

    def test_get_first_primary_key_for_index_key_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("get_first_primary_key_for_index_key\(\) missing 1 ",
                     "required positional argument: 'key'")),
            self.primary.get_first_primary_key_for_index_key)

    def test_get_first_primary_key_for_index_key_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("get_first_primary_key_for_index_key not available ",
                     "on primary database")),
            self.primary.get_first_primary_key_for_index_key,
            *(None,))

    def test_populate_recordset_key_like_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("populate_recordset_key_like\(\) missing 2 ",
                     "required positional arguments: 'recordset' and 'key'")),
            self.primary.populate_recordset_key_like)

    def test_populate_recordset_key_like_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("populate_recordset_key_like not available ",
                     "on primary database")),
            self.primary.populate_recordset_key_like,
            *(None, None))

    def test_populate_recordset_key_startswith_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("populate_recordset_key_startswith\(\) missing 2 ",
                     "required positional arguments: 'recordset' and 'key'")),
            self.primary.populate_recordset_key_startswith)

    def test_populate_recordset_key_startswith_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("populate_recordset_key_startswith not available ",
                     "on primary database")),
            self.primary.populate_recordset_key_startswith,
            *(None, None))

    def test_file_records_under_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("file_records_under\(\) missing 2 ",
                     "required positional arguments: 'recordset' and 'key'")),
            self.primary.file_records_under)

    def test_file_records_under_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("file_records_under not available ",
                     "for primary database")),
            self.primary.file_records_under,
            *(None, None))

    def test_unfile_records_under_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("unfile_records_under\(\) missing 1 ",
                     "required positional argument: 'key'")),
            self.primary.unfile_records_under)

    def test_unfile_records_under_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("unfile_records_under not available ",
                     "for primary database")),
            self.primary.unfile_records_under,
            *(None,))

    def test_find_values_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("find_values\(\) missing 1 ",
                     "required positional argument: 'valuespec'")),
            self.primary.find_values)

    def test_find_values_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("find_values not implemented ",
                     "on primary database")),
            self.primary.find_values,
            *(None,))


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Primary))
