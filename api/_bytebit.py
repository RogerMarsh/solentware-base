# _bytebit.py
# Copyright (c) 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A pure Python partial emulation of bitarray class for solentware_base.

The point of this module is being part of the solentware_base package, not a
product that has to be obtained built and intstalled separately.  Both bitarray,
an extension module written in C, and BitVector, a pure Python module, must be
installed separately: they are not in the Python distribution.

It is assumed that bitarray is faster than BitVector.  The bitarray class looks
simpler to emulate.

Bitarray mostly takes about 4 times longer to do something than bitarray.  The
exception is count set bits, where Bitarray takes at least 100 times longer to
do the count than bitarray if the count method is pure Python.  A C++ version
of count, accessed via a SWIG wrapper, is provided to get the 4 times longer
factor.

"""

from copy import copy

_bits_set = tuple(
    tuple(j for j in range(8) if i & (128 >> j)) for i in range(256))
_bits_count = bytearray.maketrans(
    bytearray(i for i in range(256)),
    bytearray(len(bs) for bs in _bits_set))
_reversed_bits = bytearray.maketrans(
    bytearray(i for i in range(256)),
    bytearray(sum(128 >> (8 - i - 1) for i in bs) for bs in _bits_set))
_inverted_bits = bytearray.maketrans(
    bytearray(i for i in range(256)),
    bytearray(255 - sum(128 >> i for i in bs) for bs in _bits_set))
_first_set_bit = {
    e: bs[0] if len(bs) else None
    for e, bs in enumerate(_bits_set)}
_last_set_bit = {
    e: bs[-1] if len(bs) else None
    for e, bs in enumerate(_bits_set)}


class Bitarray(object):
    """"""

    def __init__(self, bitlength=0):
        """"""
        super(Bitarray, self).__init__()
        self._ba = bytearray(bitlength // 8)

    @property
    def ba(self):
        """"""
        return self._ba

    # 'all' for compatibility with bitarray module - conventional is 'all_'
    def all(self):
        """"""
        return bool(int.from_bytes(b'\xff' * len(self._ba), 'big') ==
                    int.from_bytes(self._ba, 'big'))

    # 'any' for compatibility with bitarray module - conventional is 'any_'
    def any(self):
        """"""
        return bool(int.from_bytes(self._ba, 'big'))

    # bitarray module count() is about 100 times quicker than Bitarray
    # count().  The count.count() function is about 25 times quicker than
    # Bitarray count() so if the 'import count' succeeded use the count()
    # method provided there.
    # In SWIGged C++ the problem is that the C++ method is given a utf-8
    # encoded string.  So the worst case, with all bytes having leftmost
    # bit set, takes twice as long as best case.
    def count(self, value=True):
        """Return count of set bits in self.

        The value argument is present for compatibility with count() method
        in bitarray.bitarray class.

        """
        # Time taken proportional to number of non-zero bytes.
        return sum(self._ba.translate(_bits_count, b'\x00'))

    def frombytes(self, from_):
        """"""
        self._ba.extend(from_)

    def index(self, value, *args):
        """"""
        if len(args) == 0:
            start = 0
            stop = 8 * len(self._ba) - 1
            start_byte = 0
            stop_byte = stop // 8
        elif len(args) == 1:
            start = args[0]
            stop = 8 * len(self._ba) - 1
            start_byte = start // 8
            stop_byte = stop // 8
        elif len(args) == 2:
            start, stop = args
            start_byte = start // 8
            stop_byte = stop // 8
        else:
            raise TypeError(''.join(
                ('index() takes at most 3 arguments (',
                 str(len(args) + 1),
                 ' given)',
                 )))
        if bool(value):
            try:
                if self._ba[start_byte] != 0:
                    for bit in range(
                        start % 8,
                        8 if stop_byte > start_byte else 1 + stop % 8):
                        if self._ba[start_byte] & 128 >> bit:
                            return 8 * start_byte + bit
            except IndexError:
                raise ValueError('Set bit (True) not found')
            for byte in range(1 + start_byte, stop_byte):
                if self._ba[byte] != 0:
                    return 8 * byte + _first_set_bit[self._ba[byte]]
            if start_byte < stop_byte:
                if self._ba[stop_byte] != 0:
                    bit = _first_set_bit[self._ba[stop_byte]]
                    if bit <= stop % 8: 
                        return 8 * stop_byte + bit
            raise ValueError('Set bit (True) not found')
        else:
            try:
                if self._ba[start_byte] != 255:
                    for bit in range(
                        start % 8,
                        8 if stop_byte > start_byte else 1 + stop % 8):
                        if not self._ba[start_byte] & 128 >> bit:
                            return 8 * start_byte + bit
            except IndexError:
                raise ValueError('Unset bit (False) not found')
            for byte in range(1 + start_byte, stop_byte):
                if self._ba[byte] != 255:
                    for bit in range(0, 8):
                        if not self._ba[byte] & 128 >> bit:
                            return 8 * byte + bit
            if start_byte < stop_byte:
                for bit in range(0, 1 + stop % 8):
                    if not self._ba[stop_byte] & 128 >> bit:
                        return 8 * stop_byte + bit
            raise ValueError('Unset bit (False) not found')

    def invert(self):
        """"""
        self._ba = self._ba.translate(_inverted_bits)

    def length(self):
        """"""
        return len(self._ba) * 8

    # ba must be present for compatibility with bitarray module.
    # But this search() ignores the argument and looks for set bits.
    # Having tolist() do this would be natural, but bitarray tolist() does
    # something different and bitarray search() with the correct argument
    # does the job.
    # search() is slow. 10 Bitarray __and__ operations are done in the same
    # time as one Bitarray search() operation.  But the Bitarray to bitarray
    # ratio is about 4, like all the other methods, such as __and__, except
    # count().
    def search(self, ba, limit=None):
        """Return list of set bit positions in Bitarray. ba must be SINGLEBIT.

        The arguments are present for compatibility with search() method in
        bitarray.bitarray class from the bitarray-0.8.1 package (from PyPI).

        SINGLEBIT is defined in solentware_base.api.bytebit where
        bitarray.bitarray has been imported rather than
        solentware_base.tools.bytebit.Bitarray (this module) if possible.

        This method ignores the arguments, but the equivalent call to search()
        in bitarray-0.8.1 is search(bitarray('1')).

        """
        bitscan = []
        for e, b in enumerate(self._ba):
            if not b:
                continue
            ea = e * 8
            for bs in _bits_set[b]:
                bitscan.append(ea + bs)
        return bitscan

    def setall(self, value):
        """"""
        if bool(value):
            self._ba = bytearray(b'\xff' * len(self._ba))
        else:
            self._ba = bytearray(b'\x00' * len(self._ba))

    def tobytes(self):
        """"""
        return bytes(self._ba)

    def copy(self):
        """"""
        ba = Bitarray()
        ba._ba = copy(self._ba)
        return ba

    # bitarray module reverse() can be about 60 times slower than Bitarray
    # reverse().
    # Only used in UI scrolling operations from some position towards
    # beginning of list, so may be acceptable.
    def reverse(self):
        """"""
        self._ba.reverse()
        self._ba = self._ba.translate(_reversed_bits)

    def __and__(self, other):
        """"""
        ba = Bitarray()
        ba._ba.extend(
            (int.from_bytes(self._ba, 'big') &
             int.from_bytes(other._ba, 'big')).to_bytes(len(self._ba), 'big'))
        return ba

    def __or__(self, other):
        """"""
        ba = Bitarray()
        ba._ba.extend(
            (int.from_bytes(self._ba, 'big') |
             int.from_bytes(other._ba, 'big')).to_bytes(len(self._ba), 'big'))
        return ba

    def __xor__(self, other):
        """"""
        ba = Bitarray()
        ba._ba.extend(
            (int.from_bytes(self._ba, 'big') ^
             int.from_bytes(other._ba, 'big')).to_bytes(len(self._ba), 'big'))
        return ba

    def __iand__(self, other):
        """"""
        self._ba = bytearray(
            (int.from_bytes(self._ba, 'big') &
             int.from_bytes(other._ba, 'big')
             ).to_bytes(len(self._ba), 'big'))
        return self

    def __ior__(self, other):
        """"""
        self._ba = bytearray(
            (int.from_bytes(self._ba, 'big') |
             int.from_bytes(other._ba, 'big')
             ).to_bytes(len(self._ba), 'big'))
        return self

    def __ixor__(self, other):
        """"""
        self._ba = bytearray(
            (int.from_bytes(self._ba, 'big') ^
             int.from_bytes(other._ba, 'big')
             ).to_bytes(len(self._ba), 'big'))
        return self

    def __invert__(self):
        """"""
        ba = Bitarray()
        ba._ba = self._ba.translate(_inverted_bits)
        return ba
                  

    def __getitem__(self, key):
        """"""
        k, b = divmod(key, 8) 
        if k < len(self._ba) and len(self._ba) >= -k:
            return bool(self._ba[k] & 128 >> b)
        else:
            raise KeyError('Bit not in Bitarray')

    def __setitem__(self, key, value):
        """"""
        k, b = divmod(key, 8) 
        if k < len(self._ba) and len(self._ba) >= -k:
            if value:
                self._ba[k] |= 128 >> b
            else:
                self._ba[k] &= 255 ^ 128 >> b
        else:
            raise KeyError('Bit not in Bitarray')

    def __contains__(self, key):
        """"""
        k, b = divmod(key, 8) 
        if k < len(self._ba) and len(self._ba) >= -k:
            return bool(self._ba[k] & 128 >> b)
        else:
            raise IndexError('Bit not in Bitarray')
