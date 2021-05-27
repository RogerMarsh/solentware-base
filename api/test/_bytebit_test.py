# _bytebit_test.py
# Copyright 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_bytebit tests"""

import unittest

from .. import _bytebit


class Bitarray(unittest.TestCase):

    def setUp(self):
        self.baone = _bytebit.Bitarray(1)
        self.batwo = _bytebit.Bitarray(1)
        self.basix = _bytebit.Bitarray(1)
        self.baten = _bytebit.Bitarray(1)
        self.baall = _bytebit.Bitarray(1)
        self.baone.frombytes(b'\x03')
        self.batwo.frombytes(b'\x05')
        self.basix.frombytes(b'\x80\x00\x00\x00\x00\x01')
        self.baten.frombytes(b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.baall.frombytes(b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')

    def tearDown(self):
        pass

    def test___assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertEqual(self.baone.ba, b'\x03', msg)
        self.assertEqual(self.batwo.ba, b'\x05', msg)
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02', msg)
        self.assertIsInstance(self.baone.ba, bytearray, msg)
        self.assertIsInstance(self.batwo.ba, bytearray, msg)
        self.assertIsInstance(self.baten.ba, bytearray, msg)
        bits_set = tuple(
            tuple(j for j in range(8) if i & (128 >> j)) for i in range(256))
        self.assertEqual(_bytebit._bits_set, bits_set, msg)
        bits_count = bytearray.maketrans(
            bytearray(i for i in range(256)),
            bytearray(len(bs) for bs in bits_set))
        self.assertEqual(_bytebit._bits_count, bits_count, msg)
        reversed_bits = bytearray.maketrans(
            bytearray(i for i in range(256)),
            bytearray(sum(128 >> (8 - i - 1) for i in bs) for bs in bits_set))
        self.assertEqual(_bytebit._reversed_bits, reversed_bits, msg)
        inverted_bits = bytearray.maketrans(
            bytearray(i for i in range(256)),
            bytearray(255 - sum(128 >> i for i in bs) for bs in bits_set))
        self.assertEqual(_bytebit._inverted_bits, inverted_bits, msg)

    def test__and__(self):
        ba = self.baone & self.batwo
        self.assertEqual(ba.ba, b'\x01')
        self.assertEqual(self.baone.tobytes(), b'\x03')
        self.assertEqual(self.batwo.tobytes(), b'\x05')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)
        ba = self.baten & self.baall
        self.assertEqual(ba.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baall.ba, bytearray)
        self.assertIsInstance(self.baten.ba, bytearray)

    def test__or__(self):
        ba = self.baone | self.batwo
        self.assertEqual(ba.ba, b'\x07')
        self.assertEqual(self.baone.tobytes(), b'\x03')
        self.assertEqual(self.batwo.tobytes(), b'\x05')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)
        ba = self.baten | self.baall
        self.assertEqual(ba.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baall.ba, bytearray)
        self.assertIsInstance(self.baten.ba, bytearray)

    def test__xor__(self):
        ba = self.baone ^ self.batwo
        self.assertEqual(ba.ba, b'\x06')
        self.assertEqual(self.baone.tobytes(), b'\x03')
        self.assertEqual(self.batwo.tobytes(), b'\x05')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)
        ba = self.baten ^ self.baall
        self.assertEqual(ba.ba, b'\xf7\xff\x00\xff\xf5\xff\x7e\xff\x00\xfd')
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baall.ba, bytearray)
        self.assertIsInstance(self.baten.ba, bytearray)

    def test__iand__(self):
        self.baone &= self.batwo
        self.assertEqual(self.baone.ba, b'\x01')
        self.assertEqual(self.batwo.ba, b'\x05')
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)

    def test__ior__(self):
        self.baone |= self.batwo
        self.assertEqual(self.baone.ba, b'\x07')
        self.assertEqual(self.batwo.ba, b'\x05')
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)

    def test__ixor__(self):
        self.baone ^= self.batwo
        self.assertEqual(self.baone.ba, b'\x06')
        self.assertEqual(self.batwo.ba, b'\x05')
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertIsInstance(self.batwo.ba, bytearray)

    def test__invert__(self):
        ba = ~self.baone
        self.assertIsInstance(ba.ba, bytearray)
        self.assertIsInstance(self.baone.ba, bytearray)
        self.assertEqual(self.baone.ba, b'\x03')
        self.assertEqual(ba.ba, b'\xfc')
        ba = ~self.batwo
        self.assertEqual(self.batwo.ba, b'\x05')
        self.assertEqual(ba.ba, b'\xfa')

    def test__getitem__(self):
        self.assertRaises(KeyError, self.baone.__getitem__, *(-9,))
        self.assertRaises(KeyError, self.baone.__getitem__, *(8,))
        self.assertIsInstance(self.baone[-8], bool)
        self.assertIsInstance(self.baone[7], bool)
        self.assertIs(self.baone[-8], False)
        self.assertIs(self.baone[-7], False)
        self.assertIs(self.baone[-6], False)
        self.assertIs(self.baone[-5], False)
        self.assertIs(self.baone[-4], False)
        self.assertIs(self.baone[-3], False)
        self.assertIs(self.baone[-2], True)
        self.assertIs(self.baone[-1], True)
        self.assertIs(self.baone[0], False)
        self.assertIs(self.baone[1], False)
        self.assertIs(self.baone[2], False)
        self.assertIs(self.baone[3], False)
        self.assertIs(self.baone[4], False)
        self.assertIs(self.baone[5], False)
        self.assertIs(self.baone[6], True)
        self.assertIs(self.baone[7], True)
        self.assertRaises(KeyError, self.baten.__getitem__, *(-81,))
        self.assertRaises(KeyError, self.baten.__getitem__, *(80,))
        self.assertIsInstance(self.baten[-80], bool)
        self.assertIsInstance(self.baten[79], bool)

    def test__setitem__(self):
        self.assertRaises(KeyError, self.baone.__setitem__, *(-9, True))
        self.assertRaises(KeyError, self.baone.__setitem__, *(8, True))
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-8] = True
        self.assertEqual(self.baone.ba, b'\x83')
        self.baone[-8] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-7] = True
        self.assertEqual(self.baone.ba, b'\x43')
        self.baone[-7] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-6] = True
        self.assertEqual(self.baone.ba, b'\x23')
        self.baone[-6] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-5] = True
        self.assertEqual(self.baone.ba, b'\x13')
        self.baone[-5] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-4] = True
        self.assertEqual(self.baone.ba, b'\x0b')
        self.baone[-4] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-3] = True
        self.assertEqual(self.baone.ba, b'\x07')
        self.baone[-3] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-2] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-2] = False
        self.assertEqual(self.baone.ba, b'\x01')
        self.baone[-2] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-1] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[-1] = False
        self.assertEqual(self.baone.ba, b'\x02')
        self.baone[-1] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[0] = True
        self.assertEqual(self.baone.ba, b'\x83')
        self.baone[0] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[1] = True
        self.assertEqual(self.baone.ba, b'\x43')
        self.baone[1] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[2] = True
        self.assertEqual(self.baone.ba, b'\x23')
        self.baone[2] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[3] = True
        self.assertEqual(self.baone.ba, b'\x13')
        self.baone[3] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[4] = True
        self.assertEqual(self.baone.ba, b'\x0b')
        self.baone[4] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[5] = True
        self.assertEqual(self.baone.ba, b'\x07')
        self.baone[5] = False
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[6] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[6] = False
        self.assertEqual(self.baone.ba, b'\x01')
        self.baone[6] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[7] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.baone[7] = False
        self.assertEqual(self.baone.ba, b'\x02')
        self.baone[7] = True
        self.assertEqual(self.baone.ba, b'\x03')
        self.assertRaises(KeyError, self.baten.__setitem__, *(-81, True))
        self.assertRaises(KeyError, self.baten.__setitem__, *(80, True))
        self.baall[79] = False
        self.baall[0] = False
        self.assertIs(self.baall[-80], False)
        self.assertIs(self.baall[79], False)

    def test__contains__(self):
        self.assertRaises(IndexError, self.baone.__contains__, *(-9,))
        self.assertRaises(IndexError, self.baone.__contains__, *(8,))
        self.assertIsInstance(-8 in self.baone, bool)
        self.assertIsInstance(7 in self.baone, bool)
        self.assertIs(-8 in self.baone, False)
        self.assertIs(-7 in self.baone, False)
        self.assertIs(-6 in self.baone, False)
        self.assertIs(-5 in self.baone, False)
        self.assertIs(-4 in self.baone, False)
        self.assertIs(-3 in self.baone, False)
        self.assertIs(-2 in self.baone, True)
        self.assertIs(-1 in self.baone, True)
        self.assertIs(0 in self.baone, False)
        self.assertIs(1 in self.baone, False)
        self.assertIs(2 in self.baone, False)
        self.assertIs(3 in self.baone, False)
        self.assertIs(4 in self.baone, False)
        self.assertIs(5 in self.baone, False)
        self.assertIs(6 in self.baone, True)
        self.assertIs(7 in self.baone, True)
        self.assertRaises(IndexError, self.baten.__contains__, *(-81,))
        self.assertRaises(IndexError, self.baten.__contains__, *(80,))
        self.assertIsInstance(-80 in self.baten, bool)
        self.assertIsInstance(79 in self.baten, bool)

    def test_all(self):
        self.assertIs(self.baall.all(), True)
        self.assertIs(self.baten.all(), False)
        self.baall[4] = False
        self.assertIs(self.baall.all(), False)

    def test_any(self):
        self.assertIs(self.baall.any(), True)
        self.assertIs(self.baten.any(), True)
        self.baall[4] = False
        self.assertIs(self.baall.any(), True)

    def test_count(self):
        self.assertEqual(self.baall.count(), 80)
        self.assertEqual(self.baten.count(), 22)
        self.baall[4] = False
        self.assertEqual(self.baall.count(), 79)
        self.baten[4] = False
        self.assertEqual(self.baten.count(), 21)
        self.baten[5] = False
        self.assertEqual(self.baten.count(), 21)
        self.baten[28] = True
        self.assertEqual(self.baten.count(), 22)

    def test_frombytes(self):
        self.assertIsInstance(self.baall.ba, bytearray)
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.baall.frombytes(b'abcdef')
        self.assertIsInstance(self.baall.ba, bytearray)
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xffabcdef')

    def test_index(self):
        ba = self.baone.index(True)
        self.assertEqual(ba, 6)
        ba = self.baone.index(True, ba + 1)
        self.assertEqual(ba, 7)
        self.assertRaises(ValueError, self.baone.index, *(True, ba + 1))
        ba = self.batwo.index(True)
        self.assertEqual(ba, 5)
        ba = self.batwo.index(True, ba + 1)
        self.assertEqual(ba, 7)
        self.assertRaises(ValueError, self.batwo.index, *(True, ba + 1))
        ba = self.baten.index(True)
        self.assertEqual(ba, 4)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 16)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 17)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 18)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 19)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 20)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 21)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 22)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 23)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 36)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 38)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 48)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 55)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 64)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 65)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 66)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 67)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 68)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 69)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 70)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 71)
        ba = self.baten.index(True, ba + 1)
        self.assertEqual(ba, 78)
        self.assertRaises(ValueError, self.baten.index, *(True, ba + 1))
        ba = self.basix.index(True)
        self.assertEqual(ba, 0)
        ba = self.basix.index(True, ba + 1)
        self.assertEqual(ba, 47)

        # Added 16 March 2015, fault found when working on pydbitkit.bitmap.
        # Note find first False did not work at all.
        self.assertEqual(self.baten.index(True, 49, 55), 55)
        self.assertRaises(ValueError, self.baten.index, *(True, 49, 54))
        self.assertEqual(self.baten.index(False, 38, 39), 39)
        self.assertRaises(ValueError, self.baten.index, *(False, 38, 38))
        self.assertEqual(self.baten.index(False, 16, 24), 24)
        self.assertRaises(ValueError, self.baten.index, *(False, 16, 23))
        self.assertEqual(self.baten.index(False, 16, 50), 24)

    def test_invert(self):
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.baten.invert()
        self.assertEqual(
            self.baten.ba, b'\xf7\xff\x00\xff\xf5\xff\x7e\xff\x00\xfd')
        self.assertIsInstance(self.baten.ba, bytearray)

    def test_length(self):
        self.assertEqual(self.baten.length(), 80)
        self.assertEqual(self.batwo.length(), 8)
        self.baten.frombytes(b'abcdef')
        self.assertEqual(self.baten.length(), 128)

    def test_search(self):
        baone = self.baone.search(None)
        batwo = self.batwo.search(True)
        baten = self.baten.search(False)
        baall = self.baall.search({})
        self.assertEqual(baone, [6,7])
        self.assertEqual(batwo, [5,7])
        self.assertEqual(
            baten,
            [4,
             16,17,18,19,20,21,22,23,
             36,38,
             48,55,
             64,65,66,67,68,69,70,71,
             78,
             ])
        self.assertEqual(baall, [i for i in range(80)])
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')

    def test_setall(self):
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.baten.setall(True)
        self.assertEqual(
            self.baall.ba, b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff')
        self.baten.setall(False)
        self.assertEqual(
            self.baten.ba, b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')

    def test_tobytes(self):
        b = self.baten.tobytes()
        self.assertIsInstance(b, bytes)
        self.assertNotIsInstance(b, bytearray)
        self.assertIsInstance(self.baten.ba, bytearray)
        self.assertNotIsInstance(self.baten.ba, bytes)
        self.assertEqual(self.baten.ba, b)

    def test_copy(self):
        ba = self.baten.copy()
        self.assertIsNot(ba, self.baten)
        self.assertIsNot(ba.ba, self.baten.ba)
        self.assertIsInstance(self.baten.ba, bytearray)
        self.assertIsInstance(ba.ba, bytearray)
        self.assertEqual(self.baten.ba, ba.ba)
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')

    def test_reverse(self):
        self.assertEqual(
            self.baten.ba, b'\x08\x00\xff\x00\x0a\x00\x81\x00\xff\x02')
        self.baten.reverse()
        self.assertEqual(
            self.baten.ba, b'\x40\xff\x00\x81\x00\x50\x00\xff\x00\x10')
        self.assertIsInstance(self.baten.ba, bytearray)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Bitarray))
