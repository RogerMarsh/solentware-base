# segmentsize_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""segmentsize tests"""

import unittest

from .. import segmentsize


class SegmentSize(unittest.TestCase):

    def setUp(self):
        self.s = segmentsize.SegmentSize

    def tearDown(self):
        pass

    def test_SegmentSize_01(self):
        self.assertIs(self.s.db_segment_size_bytes,
                      self.s._db_segment_size_bytes)
        self.assertIs(self.s.segment_sort_scale,
                      self.s._segment_sort_scale)
        self.assertIs(self.s.db_segment_size,
                      self.s._db_segment_size)
        self.assertIs(self.s.db_top_record_number_in_segment,
                      self.s._db_top_record_number_in_segment)
        self.assertIs(self.s.db_upper_conversion_limit,
                      self.s._db_upper_conversion_limit)
        self.assertIs(self.s.db_lower_conversion_limit,
                      self.s._db_lower_conversion_limit)
        self.assertIs(self.s.empty_bitarray_bytes,
                      self.s._empty_bitarray_bytes)
        self.assertIs(self.s.empty_bitarray,
                      self.s._empty_bitarray)
        self.assertEqual(self.s.db_segment_size_bytes, 4096)
        self.assertEqual(self.s.segment_sort_scale, 30000)
        self.assertEqual(self.s.db_segment_size, 32768)
        self.assertEqual(self.s.db_top_record_number_in_segment, 32767)
        self.assertEqual(self.s.db_upper_conversion_limit, 2000)
        self.assertEqual(self.s.db_lower_conversion_limit, 1950)
        self.assertEqual(self.s.empty_bitarray_bytes, b'\x00'*4096)
        for i in range(4096):
            self.assertEqual(self.s.empty_bitarray[i], False)

    def test_SegmentSize_02(self):
        self.s.db_segment_size_bytes = 1
        self.assertIs(self.s.db_segment_size_bytes,
                      self.s._db_segment_size_bytes)
        self.assertIs(self.s.segment_sort_scale,
                      self.s._segment_sort_scale)
        self.assertIs(self.s.db_segment_size,
                      self.s._db_segment_size)
        self.assertIs(self.s.db_top_record_number_in_segment,
                      self.s._db_top_record_number_in_segment)
        self.assertIs(self.s.db_upper_conversion_limit,
                      self.s._db_upper_conversion_limit)
        self.assertIs(self.s.db_lower_conversion_limit,
                      self.s._db_lower_conversion_limit)
        self.assertIs(self.s.empty_bitarray_bytes,
                      self.s._empty_bitarray_bytes)
        self.assertIs(self.s.empty_bitarray,
                      self.s._empty_bitarray)
        self.assertEqual(self.s.db_segment_size_bytes, 4096)
        self.assertEqual(self.s.segment_sort_scale, 30000)
        self.assertEqual(self.s.db_segment_size, 32768)
        self.assertEqual(self.s.db_top_record_number_in_segment, 32767)
        self.assertEqual(self.s.db_upper_conversion_limit, 2000)
        self.assertEqual(self.s.db_lower_conversion_limit, 1950)
        self.assertEqual(self.s.empty_bitarray_bytes, b'\x00'*4096)
        for i in range(4096):
            self.assertEqual(self.s.empty_bitarray[i], False)

    def test_SegmentSize_03(self):
        self.s.db_segment_size_bytes = None
        self.assertIs(self.s.db_segment_size_bytes,
                      self.s._db_segment_size_bytes)
        self.assertIs(self.s.segment_sort_scale,
                      self.s._segment_sort_scale)
        self.assertIs(self.s.db_segment_size,
                      self.s._db_segment_size)
        self.assertIs(self.s.db_top_record_number_in_segment,
                      self.s._db_top_record_number_in_segment)
        self.assertIs(self.s.db_upper_conversion_limit,
                      self.s._db_upper_conversion_limit)
        self.assertIs(self.s.db_lower_conversion_limit,
                      self.s._db_lower_conversion_limit)
        self.assertIs(self.s.empty_bitarray_bytes,
                      self.s._empty_bitarray_bytes)
        self.assertIs(self.s.empty_bitarray,
                      self.s._empty_bitarray)
        self.assertEqual(self.s.db_segment_size_bytes, 4096)
        self.assertEqual(self.s.segment_sort_scale, 30000)
        self.assertEqual(self.s.db_segment_size, 32768)
        self.assertEqual(self.s.db_top_record_number_in_segment, 32767)
        self.assertEqual(self.s.db_upper_conversion_limit, 2000)
        self.assertEqual(self.s.db_lower_conversion_limit, 1950)
        self.assertEqual(self.s.empty_bitarray_bytes, b'\x00'*4096)
        for i in range(4096):
            self.assertEqual(self.s.empty_bitarray[i], False)

    def test_SegmentSize_04(self):
        self.s.db_segment_size_bytes = 10000
        self.assertIs(self.s.db_segment_size_bytes,
                      self.s._db_segment_size_bytes)
        self.assertIs(self.s.segment_sort_scale,
                      self.s._segment_sort_scale)
        self.assertIs(self.s.db_segment_size,
                      self.s._db_segment_size)
        self.assertIs(self.s.db_top_record_number_in_segment,
                      self.s._db_top_record_number_in_segment)
        self.assertIs(self.s.db_upper_conversion_limit,
                      self.s._db_upper_conversion_limit)
        self.assertIs(self.s.db_lower_conversion_limit,
                      self.s._db_lower_conversion_limit)
        self.assertIs(self.s.empty_bitarray_bytes,
                      self.s._empty_bitarray_bytes)
        self.assertIs(self.s.empty_bitarray,
                      self.s._empty_bitarray)
        self.assertEqual(self.s.db_segment_size_bytes, 8192)
        self.assertEqual(self.s.segment_sort_scale, 30000)
        self.assertEqual(self.s.db_segment_size, 65536)
        self.assertEqual(self.s.db_top_record_number_in_segment, 65535)
        self.assertEqual(self.s.db_upper_conversion_limit, 4000)
        self.assertEqual(self.s.db_lower_conversion_limit, 3900)
        self.assertEqual(self.s.empty_bitarray_bytes, b'\x00'*8192)
        for i in range(8192):
            self.assertEqual(self.s.empty_bitarray[i], False)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(SegmentSize))
