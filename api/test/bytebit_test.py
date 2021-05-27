# bytebit_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""bytebit tests"""

import unittest

try:
    from bitarray import bitarray
except:
    bitarray = False

from .. import bytebit, _bytebit


class Bitarray(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_Bitarray(self):
        if bitarray:
            self.assertIs(bitarray, bytebit.Bitarray)
        else:
            self.assertIs(_bytebit.Bitarray, bytebit.Bitarray)

    def test_SINGLEBIT(self):
        if bitarray:
            self.assertEqual(bytebit.SINGLEBIT, bitarray('1'))
        else:
            self.assertIs(bytebit.SINGLEBIT, True)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Bitarray))
