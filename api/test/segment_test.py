# segment_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""segment tests"""

import unittest

from .. import segment


class Segment(unittest.TestCase):

    def setUp(self):
        self.segment = segment.Segment()

    def tearDown(self):
        self.segment = None

    def test___init__(self):
        self.assertEqual(self.segment.__dict__,
                         {'_segment_link': None,
                          })
        self.assertEqual(
            [a for a in dir(self.segment) if not a.startswith('__')],
            ['_segment_link',
             'append',
             'close',
             'delete',
             'get',
             'put',
             ])

    def test_close(self):
        self.assertEqual(self.segment.close(), None)

    def test_get_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("get\(\) missing 1 ",
                     "required positional argument: 'key'")),
            self.segment.get)

    def test_get_02(self):
        self.assertRaisesRegex(
            segment.SegmentError,
            "Segment.get not implemented",
            self.segment.get,
            *(None,))

    def test_delete_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("delete\(\) missing 1 ",
                     "required positional argument: 'key'")),
            self.segment.delete)

    def test_delete_02(self):
        self.assertRaisesRegex(
            segment.SegmentError,
            "Segment.delete not implemented",
            self.segment.delete,
            *(None,))

    def test_append_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("append\(\) missing 1 ",
                     "required positional argument: 'value'")),
            self.segment.append)

    def test_append_02(self):
        self.assertRaisesRegex(
            segment.SegmentError,
            "Segment.append not implemented",
            self.segment.append,
            *(None,))

    def test_put_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("put\(\) missing 2 ",
                     "required positional arguments: 'key' and 'value'")),
            self.segment.put)

    def test_put_02(self):
        self.assertRaisesRegex(
            segment.SegmentError,
            "Segment.put not implemented",
            self.segment.put,
            *(None, None))


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Segment))
