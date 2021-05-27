# bytebit.py
# Copyright (c) 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide a bit array class (called Bitarray) for basesup package.

Use the bitarray package if it has been installed, otherwise use the bytebit
module in basesup.tools.

The tool.bytebit module implements the subset of the bitarray interface used
in basesup.

Bitarray mostly takes about 4 times longer to do something than bitarray.

"""
# The decision is made here, rather than in basesup.__init__, because the DPT
# specific modules in basesup do not need a bit array class as DPT provides
# it's own bit array handling.

try:
    # Use bitarray class from bitarray module if it is available.
    # The class is more general than needed so refer to it as Bitarray, the
    # more restricted interface defined if the import fails.
    from bitarray import bitarray as Bitarray

    from .constants import DB_SEGMENT_SIZE

    SINGLEBIT = Bitarray('1')
    EMPTY_BITARRAY = Bitarray('0') * DB_SEGMENT_SIZE

except ImportError:

    from .constants import DB_SEGMENT_SIZE
    from ..tools.bytebit import Bitarray

    SINGLEBIT = True
    EMPTY_BITARRAY = Bitarray(DB_SEGMENT_SIZE)
