# secondaryfile_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""secondaryfile tests"""

import unittest

from .. import secondaryfile


class SecondaryFile(unittest.TestCase):

    def setUp(self):
        self.secondaryfile = secondaryfile.SecondaryFile()

    def tearDown(self):
        self.secondaryfile = None

    def test___init__(self):
        self.assertEqual(len(self.secondaryfile.__dict__), 1)
        self.assertEqual(
            [a for a in dir(self.secondaryfile) if not a.startswith('__')],
            ['_primary_database',
             'get_primary_database',
             'set_primary_database',
             ])
        self.assertEqual(self.secondaryfile._primary_database, None)

    def test_set_primary_database_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("set_primary_database\(\) missing 1 ",
                     "required positional argument: 'database'")),
            self.secondaryfile.set_primary_database)

    def test_set_primary_database_02(self):
        self.assertEqual(self.secondaryfile.set_primary_database(True), None)
        self.assertEqual(self.secondaryfile._primary_database, True)

    def test_get_primary_database_01(self):
        self.assertIs(self.secondaryfile.get_primary_database(),
                      self.secondaryfile._primary_database)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(SecondaryFile))
