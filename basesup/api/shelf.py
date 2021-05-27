# shelf.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""An inverted list bitmap manager in DPT style applied to a BsdDbShelf.

Obsolescent.  Use the bitarray package from pypi.python.org/pypi/bitarray

List of classes:

_Segment
Shelf
_SegmentStringKeys
_ShelfNoCompress
ShelfString

"""

import os
import shelve
import cPickle
import heapq

# bsddb removed from Python 3.n
try:
    import bsddb
except:
    import bsddb3 as bsddb

INTEGERSIZE = 32  # 32 bit integers
BITMASK = [1 << x for x in range(INTEGERSIZE - 1)]
BITMASK.append(~sum(BITMASK))  # 1 << INTEGERSIZE gives +ve Long Integer

DEFAULT_SEGMENTSIZE = (8192 - 32) * (INTEGERSIZE / 4)  # match DPT segments
CONVERSION_LIMIT = min(1024, DEFAULT_SEGMENTSIZE)  # high as disk space allows


class _Segment(list):
    
    """A list to collect record numbers for inverted index.
    
    Plus methods to convert to bitmap where this takes less space.
    Used as the lowest level entry in Shelf.deferbuffer.

    Methods added:

    compress

    Methods overridden:

    None
    
    Methods extended:

    None
    
    """

    def compress(self):
        """Return bitmap or list representation of values using least space.
        
        If there are less than CONVERSION_LIMIT values do nothing.
        If the number of missing values is less than CONVERSION_LIMIT
        convert the set to those values not present.
        Otherwise convert to bitlist of values present.
        
        """
        if len(self) < CONVERSION_LIMIT:
            return self
        
        low = min(self)
        high = max(self)
        bitmapsize = 1 + (high - low) / INTEGERSIZE
        
        if len(self) <= bitmapsize:
            return self
        
        if high - low - len(self) < CONVERSION_LIMIT:
            values = set(self)
            values.symmetric_difference_update(xrange(low, high + 1))
            return (low, high, list(values))
        
        values = [0] * bitmapsize
        for r in self:
            element, bit = divmod(r - low, INTEGERSIZE)
            values[element] |= BITMASK[bit]
        return (low, values)


class Shelf(object):
    
    """Python shelve and buffer to support deferred update.

    Methods added:

    close_current_shelve
    close_shelf
    compress (@staticmethod)
    decompress (@staticmethod)
    defer_put
    delete_shelve
    get_current_shelve
    flush_index
    make_segment (@staticmethod)
    open_shelf
    set_defer_folder
    sort_index
    sort_and_flush_index

    Methods overridden:

    __init__
    
    Methods extended:

    None
    
    """

    def __init__(self):
        """Database engine indepenent deferred update."""
        self.deferbuffer = dict()
        self.dufolder = None

        # Placeholders for shelve objects which can be used to hold large
        # quantities of data for sorting during deferred update.
        self.shelves = dict()
        self.shelf = None

    def close_current_shelve(self):
        """Close a shelve object.

        Typically used to close a shelf when furter update is too slow.
        
        """
        self.shelves[self.shelf].close()
        self.shelves[self.shelf] = None
        self.shelf = None

    def close_shelf(self):
        """Close self.shelf"""
        if self.shelf is not None:
            self.close_current_shelve()

    @staticmethod
    def compress(deferbuffer):
        """Return dictionary of compressed data.
        
        Convert _Segments in defer to bitmap where less space used.
        Return the compressed data.

        """
        defer = dict()
        for db in deferbuffer:
            defer[db] = deferbuffer[db].compress()
        return defer

    @staticmethod
    def decompress(defer):
        """Return a _SegmentRecnumKeys from defer (created by compress).
        
        If defer is a _Segment it has the correct values so return it.
        If defer[1] is integer the associated _SegmentRecumKeys has the values
        in the specified range that are absent. So invert and return it.
        Otherwise it is a bitlist representing the range of values so convert
        to _Segment and return it.
        
        """
        if isinstance(defer, _Segment):
            return defer

        if isinstance(defer[1], int):
            return list(set(defer[-1]).symmetric_difference(
                xrange(defer[0], defer[1] + 1)))

        segment = Shelf.make_segment()
        low, values = defer
        for b in xrange(len(values) * INTEGERSIZE):
            element, bit = divmod(b, INTEGERSIZE)
            if values[element] & BITMASK[bit]:
                segment.append(low + b)
        return segment

    def defer_put(self, key, value):
        """Add key and value to deferred update buffer.
        
        key is the DPT field value and value is the DPT record number.
        The argument names follow Berkeley DB secondary db usage.

        """
        self.deferbuffer.setdefault(key, self.make_segment()).append(value)

    def delete_shelve(self):
        """Delete a shelve object."""
        for s in self.shelves:
            try:
                self.shelves[s].close()
            except:
                pass

            name = ''.join(('shelf', str(s)))
            try:
                os.remove(os.path.join(self.dufolder, name))
            except:
                pass

    def get_current_shelve(self):
        """Create a shelve object."""
        if self.shelf is None:
            self.shelf = self.open_shelf(len(self.shelves))

    def flush_index(self, defer_put):
        """Apply all (key, value)s for a field from Shelfs to database.
        
        See Shelf.defer_put for interpretation of (key, value).
        
        """
        self.close_shelf()
        shelves = self.shelves
        for i in shelves:
            self.open_shelf(i)

        heapify = heapq.heapify
        heappop = heapq.heappop
        heappush = heapq.heappush
        
        updates = []
        heapify(updates)
        for i in shelves:
            try:
                r = shelves[i].first()
                k, v = r
                heappush(updates, (k, i, v))
            except:
                shelves[i].close()
                shelves[i] = None
        more = len(updates)
        while more:
            k, i, v = heappop(updates)
            for value in sorted(self.decompress(v)):
                defer_put(k, value)
            try:
                r = shelves[i].next()
                nk, nv = r
                heappush(updates, (nk, i, nv))
            except:
                shelves[i].close()
                shelves[i] = None
                more = len(updates)

    @staticmethod
    def make_segment():
        """Return _Segment which can compress record number lists."""
        return _Segment()

    def open_shelf(self, index):
        """Return a new open shelf."""
        self.shelves[index] = shelve.BsdDbShelf(
            bsddb.btopen(
                os.path.join(
                    self.dufolder,
                    ''.join(('shelf', str(index))))),
            protocol = cPickle.HIGHEST_PROTOCOL)
        return index

    def set_defer_folder(self, dufolder):
        """Set defer update folder."""
        self.dufolder = dufolder

    def sort_index(self):
        """Write a batch of deferred updates to Shelf.

        Method name is historical (updates were done direct from deferbuffer).
        str(db) to convert unicode to str for Python2.6 on MS Windows

        """
        self.get_current_shelve()
        deferbuffer = self.deferbuffer
        shelf = self.shelves[self.shelf]
        for db in sorted(deferbuffer):
            shelf[str(db)] = deferbuffer[db].compress()
        self.close_current_shelve()
        self.deferbuffer.clear()

    def sort_and_flush_index(self, defer_put):
        """Process final batch of deferred update.
        
        Write final batch of deferred updates to Shelf then call defer_put
        for every (key, value) on Shelf.
        See Shelf.defer_put for interpretation of (key, value).
        Method name is historical (updates were done direct from deferbuffer).
        
        """
        self.sort_index()
        self.flush_index(defer_put)
        self.delete_shelve()


# The following classes are for use with string keys (non-RECNO Berkeley DB
# databases for example). It is safe to use them instead of _Segment and Shelf
# for record number databases. The compressions possible are not done which
# may be a problem if disk space for temporary files is short.


class _SegmentStringKeys(_Segment):
    
    """A list to collect record keys for inverted index.
    
    Plus methods to compress list where this takes less space.
    Used as the lowest level entry in Shelf.deferbuffer.

    Methods added:

    None

    Methods overridden:

    compress
    
    Methods extended:

    None
    
    """

    def compress(self):
        """Return self.

        Compression to bitmap not possible for string keys.

        """
        return self


class _ShelfNoCompress(object):
    
    """Compression methods for string keys.

    Methods added:

    compress (@staticmethod)
    decompress (@staticmethod)
    make_segment (@staticmethod)

    Methods overridden:

    None
    
    Methods extended:

    None
    
    """

    @staticmethod
    def compress(deferbuffer):
        """Compression to bitmap not possible for string keys."""
        return deferbuffer

    @staticmethod
    def decompress(defer):
        """Compression to bitmap not possible for string keys."""
        return defer

    @staticmethod
    def make_segment():
        """Return _Segment for keys which cannot be compressed."""
        return _SegmentStringKeys()


class ShelfString(_ShelfNoCompress, Shelf):
    
    """Python shelve and buffer to support deferred update.

    Methods added:

    None

    Methods overridden:

    None
    
    Methods extended:

    None
    
    """
    
    pass
    
