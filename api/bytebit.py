# bytebit.py
# Copyright (c) 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide a bit array class (called Bitarray) for solentware_base package.

Use the bitarray package if it has been installed, otherwise use the _bytebit
module in solentware_base.api.

The api._bytebit module implements the subset of the bitarray interface used
in solentware_base.

Bitarray mostly takes about 4 times longer to do something than bitarray, but
takes about 100 times longer to count set bits.

"""
# The decision is made here, rather than in solentware_base.__init__, because
# the DPT specific modules in solentware_base do not need a bit array class as
# DPT provides it's own bit array handling.

try:
    # Use bitarray class from bitarray module if it is available.
    # The class is more general than needed so refer to it as Bitarray, the
    # more restricted interface defined if the import fails.
    from bitarray import bitarray as Bitarray

    SINGLEBIT = Bitarray('1')

except ImportError:

    from ._bytebit import Bitarray

    SINGLEBIT = True
