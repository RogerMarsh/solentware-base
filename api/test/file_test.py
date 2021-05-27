# file_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""file tests"""

import unittest

from .. import file


class File_00(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None, None, None, {}, None, None)

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, None)
        self.assertEqual(self.file._primary, True)
        self.assertEqual(self.file._fieldatts, {})
        self.assertEqual(self.file._table_link, None)

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), True)

    def test_table_link(self):
        self.assertEqual(self.file.table_link, None)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, None)


class File_01(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None, None, False, {}, None, None)

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, False)
        self.assertEqual(self.file._primary, False)
        self.assertEqual(self.file._fieldatts, {})
        self.assertEqual(self.file._table_link, None)

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), False)

    def test_table_link(self):
        self.assertEqual(self.file.table_link, None)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, None)


class File_02(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None, None, False, {}, None, None)
        self.file._table_link = [0, 1, 2]

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, False)
        self.assertEqual(self.file._primary, False)
        self.assertEqual(self.file._fieldatts, {})
        self.assertEqual(self.file._table_link, [0, 1, 2])

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), False)

    def test_table_link(self):
        self.assertEqual(self.file.table_link, 0)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, [0, 1, 2])


class File_03(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None, None, False, {}, None, None)
        self.file._table_link = 3

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, False)
        self.assertEqual(self.file._primary, False)
        self.assertEqual(self.file._fieldatts, {})
        self.assertEqual(self.file._table_link, 3)

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), False)

    def test_table_link(self):
        self.assertRaises(
            Exception,
            lambda: self.file.table_link)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, 3)


class File_04(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__01(self):
        self.assertRaisesRegex(
            file.FileError,
            ' '.join(['Attributes for', 'field',
                      'in', 'file',
                      'must be a dictionary or "None"']),
            file.File,
            *(None, 'description', None, {}, 'field', 'file'))

    def test___init__02(self):
        self.assertRaisesRegex(
            file.FileError,
            ' '.join(['Attribute', "'lll'",
                      'for', 'field',
                      'in', 'file',
                      'is not allowed']),
            file.File,
            *(None, {'lll': None}, None, {}, 'field', 'file'))

    def test___init__03(self):
        self.assertRaisesRegex(
            file.FileError,
            ' '.join(["'update_at_end'",
                      'for', 'field',
                      'in', 'file',
                      'is wrong type']),
            file.File,
            *(None, {'update_at_end': None}, None, {}, 'field', 'file'))

    def test___init__04(self):
        self.assertRaisesRegex(
            file.FileError,
            ' '.join(['Split percentage',
                      'for', 'field',
                      'in', 'file',
                      'is invalid']),
            file.File,
            *(None, {'splitpct': 120}, False, {}, 'field', 'file'))


class File_05(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None,
                              {'splitpct': 80},
                              False,
                              {},
                              'field',
                              'file')

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, False)
        self.assertEqual(self.file._primary, False)
        self.assertEqual(self.file._fieldatts, {})
        self.assertEqual(self.file._table_link, None)

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), False)

    def test_table_link(self):
        self.assertEqual(self.file.table_link, None)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, None)


class File_06(unittest.TestCase):

    def setUp(self):
        self.file = file.File(None,
                              {'splitpct': 80},
                              False,
                              set(('splitpct',)),
                              'field',
                              'file')

    def tearDown(self):
        self.file = None

    def test___init__(self):
        self.assertEqual(self.file._dataname, None)
        self.assertEqual(self.file._keyvalueset_name, False)
        self.assertEqual(self.file._primary, False)
        self.assertEqual(self.file._fieldatts, {'splitpct': 80})
        self.assertEqual(self.file._table_link, None)

    def test_is_primary(self):
        self.assertEqual(self.file.is_primary(), False)

    def test_table_link(self):
        self.assertEqual(self.file.table_link, None)

    def test_table_connection_list(self):
        self.assertEqual(self.file.table_connection_list, None)


if __name__ == '__main__':
    unittest.main()
