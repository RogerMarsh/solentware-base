# segmentsize.py
# Copyright (c) 2017 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""The SegmentSize class defines constants fixing the segment size for Berkeley
DB and Sqlite databases, and some constants controlling their use.

The default segment size is 32768 but 65536 is supported too.

The original 65536 size segment more easily leads to memory problems: in
particular Microsoft Windows with only 2Gb, and OpenBSD where a standard user
process has a memory quota of 512Mb.

The propeties implementing the constants are listed here.  (Last thing in
module is SegmentSize = SegmentSize() which causes pydoc to ignore the class
definition.)

db_segment_size_bytes
segment_sort_scale
db_segment_size
db_top_record_number_in_segment
db_upper_conversion_limit (2000 or 4000 depending on segment size)
db_lower_conversion_limit (1950 or 3900 depending on segment size)
empty_bitarray_bytes
empty_bitarray

Segment size bytes is 2 ** e (e = 0,1,2, ..) to fit database page size.

For both conversion limits 'conversion limit * byte size of record number'
is less than segment size bytes.

Normalization converts a list with more than 'upper' records to a bitmap.
Normalization converts a bitmap with less than 'lower' records to a list.

"""
from . import bytebit


class SegmentSize:
    """Segment size constants.

    Segment size bytes is 2 ** e (e = 0,1,2, ..) to fit database page size.

    For both conversion limits 'conversion limit * byte size of record number'
    is less than segment size bytes.
    
    Normalization converts a list with more than 'upper' records to a bitmap.
    Normalization converts a bitmap with less than 'lower' records to a list.
    
    """
    def __init__(self):
        """"""
        self.db_segment_size_bytes = 4096

        # 30000 is chosen because reading 1000 key-value pairs from a segment
        # buffer happened to work for up to 33 segments each containing 65536
        # records.
        self._segment_sort_scale = 30000

    @property
    def db_segment_size_bytes(self):
        """The byte size of a segment.

        Either 4096, the default, or 8192.
        """
        return self._db_segment_size_bytes

    @db_segment_size_bytes.setter
    def db_segment_size_bytes(self, value):
        """Set segment size constants.

        This property setter allows the constants to be adjusted after seeing
        the size of a bitmap on an existing database.

        """
        if isinstance(value, int):
            if value > 4096:
                self._db_segment_size_bytes = 8192
                self._db_upper_conversion_limit = 4000
                self._db_lower_conversion_limit = 3900
            else:
                self._db_segment_size_bytes = 4096
                self._db_upper_conversion_limit = 2000
                self._db_lower_conversion_limit = 1950
        else:
            self._db_segment_size_bytes = 4096
            self._db_upper_conversion_limit = 2000
            self._db_lower_conversion_limit = 1950
        self._db_segment_size = self._db_segment_size_bytes * 8
        self._db_top_record_number_in_segment = self._db_segment_size - 1
        self._empty_bitarray_bytes = b'\x00' * self._db_segment_size_bytes
        if bytebit.SINGLEBIT is True:
            self._empty_bitarray = bytebit.Bitarray(self._db_segment_size)
        else:
            self._empty_bitarray = bytebit.Bitarray('0') * self._db_segment_size

    @property
    def segment_sort_scale(self):
        """"""
        return self._segment_sort_scale

    @property
    def db_segment_size(self):
        """The record number size of a segment.

        Either 32768, the default, or 65536.

        """
        return self._db_segment_size

    @property
    def db_top_record_number_in_segment(self):
        """The high record number in each segment.

        Either 32767, the default, or 65535.

        Most database engines count records from 1, so record number 0 in
        segment 0 in their databases is never used.

        """
        return self._db_top_record_number_in_segment

    @property
    def db_upper_conversion_limit(self):
        """The number of records in a segment which causes conversion of lists
        to bitmaps when reached by addition of a record to the segment.

        Either 2000, the default, or 4000.

        """
        return self._db_upper_conversion_limit

    @property
    def db_lower_conversion_limit(self):
        """The number of records in a segment which causes conversion of
        bitmaps to lists when reached by deletion of a record from the
        segment.

        Either 1950, the default, or 3900.

        """
        return self._db_lower_conversion_limit

    @property
    def empty_bitarray_bytes(self):
        """A segment sized bytes object with all bytes set to 0."""
        return self._empty_bitarray_bytes

    @property
    def empty_bitarray(self):
        """A segment sized bitarray object with all bits set to 0."""
        return self._empty_bitarray


SegmentSize = SegmentSize()

# Hack: all databases are still 65536 record number segments.
#SegmentSize.db_segment_size_bytes = 8192
