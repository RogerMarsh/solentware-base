# primaryfile_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""primaryfile tests"""

import unittest

from .. import primaryfile


class PrimaryFile(unittest.TestCase):

    def setUp(self):
        class F:
            def __init__(self, a):
                pass
        class E:
            def __init__(self, file_reference=None):
                pass
        self.primaryfile = primaryfile.PrimaryFile(filecontrolprimary_class=F,
                                                   existencebitmap_class=E)
        self.e = E
        self.f = F

    def tearDown(self):
        self.primaryfile = None

    def test___init__(self):
        self.assertEqual(len(self.primaryfile.__dict__), 3)
        self.assertEqual(
            [a for a in dir(self.primaryfile) if not a.startswith('__')],
            ['_control_database',
             '_control_primary',
             '_existence_bits',
             'get_control_database',
             'get_control_primary',
             'get_existence_bits',
             'set_control_database',
             ])
        self.assertEqual(self.primaryfile._control_database, None)
        self.assertIsInstance(self.primaryfile._control_primary, self.f)
        self.assertIsInstance(self.primaryfile._existence_bits, self.e)

    def test_get_control_database_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'get_control_database'",
            self.primaryfile.get_control_database)

    def test_get_control_database_02(self):
        class C:
            def get_control_database(self):
                return 'gcd'
        self.primaryfile._control_database = C()
        self.assertEqual(self.primaryfile.get_control_database(), 'gcd')

    def test_get_existence_bits_01(self):
        self.assertIs(self.primaryfile.get_existence_bits(),
                      self.primaryfile._existence_bits)

    def test_set_control_database_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("set_control_database\(\) missing 1 ",
                     "required positional argument: 'database'")),
            self.primaryfile.set_control_database)

    def test_set_control_database_02(self):
        self.assertEqual(self.primaryfile.set_control_database(True), None)
        self.assertEqual(self.primaryfile._control_database, True)

    def test_get_control_primary_01(self):
        self.assertIs(self.primaryfile.get_control_primary(),
                      self.primaryfile._control_primary)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(PrimaryFile))
