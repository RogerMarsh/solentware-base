# testshelf.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test inverted list bitmap manager in DPT style applied to a BsdDbShelf."""


if __name__=='__main__':

    from basesup.api.shelf import (
        _Segment, Shelf, _SegmentStringKeys, ShelfString,
        )

    g = _Segment()
    s = Shelf()
    gs = _SegmentStringKeys()
    ss = ShelfString()
