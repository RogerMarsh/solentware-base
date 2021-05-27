# primarydu_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""primarydu tests"""

import unittest

from .. import primarydu
from ..segmentsize import SegmentSize
from ..bytebit import Bitarray


class Primary(unittest.TestCase):

    def setUp(self):
        class P(primarydu.Primary):
            def _get_existence_bits(self, segment):
                return self.existence_bit_maps.get(segment)
        self.primarydu = primarydu.Primary()
        self.p = P()

    def tearDown(self):
        self.primarydu = None

    def test___init__(self):
        self.assertEqual(self.primarydu.__dict__,
                         {'existence_bit_maps': {},
                          'high_segment': None,
                          'first_chunk': None,
                          })
        self.assertEqual(
            [a for a in dir(self.primarydu) if not a.startswith('__')],
            ['defer_put',
             'existence_bit_maps',
             'first_chunk',
             'high_segment',
             ])

    def test_defer_put_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 2 required positional arguments: ",
                     "'segment' and 'record_number'")),
            self.primarydu.defer_put)

    def test_defer_put_02(self):
        self.primarydu.existence_bit_maps[1] = [False] * 10
        self.assertRaisesRegex(
            AttributeError,
            "'Primary' object has no attribute '_get_existence_bits'",
            self.primarydu.defer_put,
            *(0, None))
        self.assertRaisesRegex(
            IndexError,
            "list assignment index out of range",
            self.primarydu.defer_put,
            *(1, 12))
        self.assertEqual(self.primarydu.defer_put(1, 6), None)

    def test_defer_put_03(self):
        self.assertEqual(self.p.defer_put(1, 6), None)
        self.assertEqual(self.p.defer_put(1, 7), None)
        bm = SegmentSize.empty_bitarray.copy()
        bm[6] = True
        bm[7] = True
        self.assertEqual(bm.tobytes(), self.p.existence_bit_maps[1].tobytes())
        self.assertEqual(bm[8], False)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Primary))
