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
    tuple(j for j in range(8) if i & (128 >> j)) for i in range(256)
)
_bits_count = bytes(len(bs) for bs in _bits_set)
_reversed_bits = bytes(sum(128 >> (8 - i - 1) for i in bs) for bs in _bits_set)
_inverted_bits = bytes(255 - sum(128 >> i for i in bs) for bs in _bits_set)
_first_set_bit = {
    e: bs[0] if len(bs) else None for e, bs in enumerate(_bits_set)
}
_last_set_bit = {
    e: bs[-1] if len(bs) else None for e, bs in enumerate(_bits_set)
}


class Bitarray:
    """Provide a bitarray."""

    def __init__(self, bitlength=0):
        """Initialize 'self.bitarray' to 'bitlength // 8' unset bits."""
        super().__init__()
        self.bitarray_bytes = bytearray(bitlength // 8)

    # 'all' for compatibility with bitarray module - conventional is 'all_'
    def all(self):
        """Return True if all bits in 'self.bitarray' are set."""
        return bool(
            int.from_bytes(b"\xff" * len(self.bitarray_bytes), "big")
            == int.from_bytes(self.bitarray_bytes, "big")
        )

    # 'any' for compatibility with bitarray module - conventional is 'any_'
    def any(self):
        """Return True if at least one bit in 'self.bitarray' is set."""
        return bool(int.from_bytes(self.bitarray_bytes, "big"))

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
        return sum(self.bitarray_bytes.translate(_bits_count, b"\x00"))

    def frombytes(self, from_):
        """Extend 'self.bitarray' with bitarray created from 'from_' bytes."""
        self.bitarray_bytes.extend(from_)

    def index(self, value, *args):
        """Return position of first bit with bool(value) in 'self.bitarray'.

        args is an optional range specifying limits for the search:
        [start[, stop]].

        """
        if len(args) == 0:
            start = 0
            stop = 8 * len(self.bitarray_bytes) - 1
            start_byte = 0
            stop_byte = stop // 8
        elif len(args) == 1:
            start = args[0]
            stop = 8 * len(self.bitarray_bytes) - 1
            start_byte = start // 8
            stop_byte = stop // 8
        elif len(args) == 2:
            start, stop = args
            start_byte = start // 8
            stop_byte = stop // 8
        else:
            raise TypeError(
                "".join(
                    (
                        "index() takes at most 3 arguments (",
                        str(len(args) + 1),
                        " given)",
                    )
                )
            )
        if bool(value):
            try:
                if self.bitarray_bytes[start_byte] != 0:
                    for bit in range(
                        start % 8,
                        8 if stop_byte > start_byte else 1 + stop % 8,
                    ):
                        if self.bitarray_bytes[start_byte] & 128 >> bit:
                            return 8 * start_byte + bit
            except IndexError:
                raise ValueError("Set bit (True) not found")
            for byte in range(1 + start_byte, stop_byte):
                if self.bitarray_bytes[byte] != 0:
                    return 8 * byte + _first_set_bit[self.bitarray_bytes[byte]]
            if start_byte < stop_byte:
                if self.bitarray_bytes[stop_byte] != 0:
                    bit = _first_set_bit[self.bitarray_bytes[stop_byte]]
                    if bit <= stop % 8:
                        return 8 * stop_byte + bit
            raise ValueError("Set bit (True) not found")
        try:
            if self.bitarray_bytes[start_byte] != 255:
                for bit in range(
                    start % 8,
                    8 if stop_byte > start_byte else 1 + stop % 8,
                ):
                    if not self.bitarray_bytes[start_byte] & 128 >> bit:
                        return 8 * start_byte + bit
        except IndexError:
            raise ValueError("Unset bit (False) not found")
        for byte in range(1 + start_byte, stop_byte):
            if self.bitarray_bytes[byte] != 255:
                for bit in range(0, 8):
                    if not self.bitarray_bytes[byte] & 128 >> bit:
                        return 8 * byte + bit
        if start_byte < stop_byte:
            for bit in range(0, 1 + stop % 8):
                if not self.bitarray_bytes[stop_byte] & 128 >> bit:
                    return 8 * stop_byte + bit
        raise ValueError("Unset bit (False) not found")

    def invert(self):
        """Invert all bits in 'self.bitarray'."""
        self.bitarray_bytes = self.bitarray_bytes.translate(_inverted_bits)

    def length(self):
        """Return number of bits in 'self.bitarray'."""
        return len(self.bitarray_bytes) * 8

    # bitarray_bytes must be present for compatibility with bitarray module.
    # But this search() ignores the argument and looks for set bits.
    # Having tolist() do this would be natural, but bitarray tolist() does
    # something different and bitarray search() with the correct argument
    # does the job.
    # search() is slow. 10 Bitarray __and__ operations are done in the same
    # time as one Bitarray search() operation.  But the Bitarray to bitarray
    # ratio is about 4, like all the other methods, such as __and__, except
    # count().
    def search(self, bitarray, limit=None):
        """Return list of set bit positions matching bitarray pattern.

        The arguments are present for compatibility with search() method in
        bitarray.bitarray class from the bitarray-0.8.1 package (from PyPI).

        The call should be search(SINGLEBIT).

        SINGLEBIT is defined in solentware_base.core.bytebit where
        bitarray.bitarray has been imported rather than
        solentware_base.tools.bytebit.Bitarray (this module) if possible.

        This method ignores the arguments, but the equivalent call to search()
        in bitarray-0.8.1 is search(bitarray('1')).

        """
        bitscan = []
        for j, byte in enumerate(self.bitarray_bytes):
            if not byte:
                continue
            base = j * 8
            for k in _bits_set[byte]:
                bitscan.append(base + k)
        return bitscan

    def setall(self, value):
        """Set all bits in 'self.bitarray' to bool(value)."""
        if bool(value):
            self.bitarray_bytes = bytearray(b"\xff" * len(self.bitarray_bytes))
        else:
            self.bitarray_bytes = bytearray(b"\x00" * len(self.bitarray_bytes))

    def tobytes(self):
        """Return 'self.bitarray' converted to bytes."""
        return bytes(self.bitarray_bytes)

    def copy(self):
        """Return a copy of self."""
        j = Bitarray()
        j.bitarray_bytes = copy(self.bitarray_bytes)
        return j

    # bitarray module reverse() can be about 60 times slower than Bitarray
    # reverse().
    # Only used in UI scrolling operations from some position towards
    # beginning of list, so may be acceptable.
    def reverse(self):
        """Reverse bit order of 'self.bitarray'."""
        self.bitarray_bytes.reverse()
        self.bitarray_bytes = self.bitarray_bytes.translate(_reversed_bits)

    def __and__(self, other):
        """Do 'new.bitarray = self.bitarray & other.bitarray': return new."""
        j = Bitarray()
        j.bitarray_bytes.extend(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                & int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return j

    def __or__(self, other):
        """Do 'new.bitarray = self.bitarray | other.bitarray': return new."""
        j = Bitarray()
        j.bitarray_bytes.extend(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                | int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return j

    def __xor__(self, other):
        """Do 'new.bitarray = self.bitarray ^ other.bitarray': return new."""
        j = Bitarray()
        j.bitarray_bytes.extend(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                ^ int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return j

    def __iand__(self, other):
        """Do 'self.bitarray = self.bitarray & other.bitarray': return self."""
        self.bitarray_bytes = bytearray(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                & int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return self

    def __ior__(self, other):
        """Do 'self.bitarray = self.bitarray | other.bitarray': return self."""
        self.bitarray_bytes = bytearray(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                | int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return self

    def __ixor__(self, other):
        """Do 'self.bitarray = self.bitarray ^ other.bitarray': return self."""
        self.bitarray_bytes = bytearray(
            (
                int.from_bytes(self.bitarray_bytes, "big")
                ^ int.from_bytes(other.bitarray_bytes, "big")
            ).to_bytes(len(self.bitarray_bytes), "big")
        )
        return self

    def __invert__(self):
        """Return a copy of bitarray with all bits inverted."""
        j = Bitarray()
        j.bitarray_bytes = self.bitarray_bytes.translate(_inverted_bits)
        return j

    def __getitem__(self, key):
        """Return True if bit for key is set in bitarray, or False if not."""
        k, bit = divmod(key, 8)
        if k < len(self.bitarray_bytes) and len(self.bitarray_bytes) >= -k:
            return bool(self.bitarray_bytes[k] & 128 >> bit)
        raise KeyError("Bit not in Bitarray")

    def __setitem__(self, key, value):
        """Set bit for key in bitarray if bool(value) is True, or unset bit."""
        k, bit = divmod(key, 8)
        if k < len(self.bitarray_bytes) and len(self.bitarray_bytes) >= -k:
            if value:
                self.bitarray_bytes[k] |= 128 >> bit
            else:
                self.bitarray_bytes[k] &= 255 ^ 128 >> bit
        else:
            raise KeyError("Bit not in Bitarray")

    def __contains__(self, key):
        """Return True if bit for key is set in bitarray, or False if not."""
        k, bit = divmod(key, 8)
        if k < len(self.bitarray_bytes) and len(self.bitarray_bytes) >= -k:
            return bool(self.bitarray_bytes[k] & 128 >> bit)
        raise IndexError("Bit not in Bitarray")
