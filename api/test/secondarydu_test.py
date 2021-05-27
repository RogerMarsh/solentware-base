# secondarydu_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""secondarydu tests"""

import unittest

from .. import secondarydu
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray


class Secondary(unittest.TestCase):

    def setUp(self):
        self.secondarydu = secondarydu.Secondary()

    def tearDown(self):
        self.secondarydu = None

    def test___init__(self):
        self.assertEqual(self.secondarydu.__dict__,
                         {'values': {},
                          })
        self.assertEqual(
            [a for a in dir(self.secondarydu) if not a.startswith('__')],
            ['defer_put',
             'delete',
             'replace',
             'values',
             ])

    def test_defer_put_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 3 required positional arguments: ",
                     "'key', 'segment', and 'record_number'")),
            self.secondarydu.defer_put)

    def test_defer_put_02(self):
        self.assertRaisesRegex(
            TypeError,
            "unhashable type: 'dict'",
            self.secondarydu.defer_put,
            *({}, None, None))

    def test_defer_put_03(self):
        self.assertEqual(self.secondarydu.defer_put('index', None, None), None)

    def test_defer_put_04(self):
        self.secondarydu.values['index'] = 1
        self.assertEqual(self.secondarydu.defer_put('index', None, None), None)
        self.assertEqual(self.secondarydu.values, {'index': [1, None]})

    # Allowing None as record number is wrong.
    def test_defer_put_05(self):
        for i in range(SegmentSize.db_upper_conversion_limit):
            self.assertEqual(self.secondarydu.defer_put('index', None, None),
                             None)
            self.assertEqual(self.secondarydu.values, {'index': None})

    def test_defer_put_06(self):
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            self.assertEqual(self.secondarydu.defer_put('index', None, i),
                             None)
        self.assertIsInstance(self.secondarydu.values['index'], Bitarray)

    def test_defer_put_07(self):
        for i in range(SegmentSize.db_upper_conversion_limit):
            self.assertEqual(self.secondarydu.defer_put('index', None, i),
                             None)
        self.assertIsInstance(self.secondarydu.values['index'], list)

    def test_defer_put_08(self):
        for i in range(1):
            self.assertEqual(self.secondarydu.defer_put('index', None, i),
                             None)
        self.assertIsInstance(self.secondarydu.values['index'], int)

    def test_defer_put_09(self):
        for i in range(1 + 1):
            self.assertEqual(self.secondarydu.defer_put('index', None, i),
                             None)
        self.assertIsInstance(self.secondarydu.values['index'], list)

    def test_delete_01(self):
        self.assertRaisesRegex(
            secondarydu.SecondaryduError,
            "delete not implemented",
            self.secondarydu.delete,
            *(None, None))

    def test_replace_01(self):
        self.assertRaisesRegex(
            secondarydu.SecondaryduError,
            "replace not implemented",
            self.secondarydu.replace,
            *(None, None, None))


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Secondary))
