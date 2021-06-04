# segmentsize.py
# Copyright (c) 2017 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define default values for segment size constants.

The default segment size is 32768 but sizes between 4000 and 65536 are
supported too.

The propeties implementing the constants are listed here.  (Last thing in
module is SegmentSize = SegmentSize() because only one instance is useful.)

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
        """Set defaults for segment_size_bytes and segment_sort_scale."""
        self.db_segment_size_bytes = 4096

        # 30000 is chosen because reading 1000 key-value pairs from a segment
        # buffer happened to work for up to 33 segments each containing 65536
        # records.
        self._segment_sort_scale = 30000

    @property
    def db_segment_size_bytes(self):
        """Return byte size of a segment.

        By default 4096, or a value set between 500 and 8192, or 16 intended
        for testing.  The sibling database engine modules use the value set in
        constants.DEFAULT_SEGMENT_SIZE_BYTES (4000) as the default and call the
        setter as part of their initialisation to make it so.

        """
        return self._db_segment_size_bytes

    @db_segment_size_bytes.setter
    def db_segment_size_bytes(self, value):
        """Set segment size constants from value, a number of bytes.

        This property setter allows the constants to be adjusted after seeing
        the size of a bitmap on an existing database.

        A value above 8192 is treated as 8192, and a value below 500 is treated
        as 500.

        A non-int value gives a segment size of 16, intended for testing.

        """
        if isinstance(value, int):
            if value > 4096:
                self._db_segment_size_bytes = min(
                    self.db_segment_size_bytes_maximum, value
                )
                self._db_upper_conversion_limit = (
                    self._db_segment_size_bytes // 2 - 96
                )
                self._db_lower_conversion_limit = (
                    self._db_upper_conversion_limit - 100
                )
            else:
                self._db_segment_size_bytes = max(
                    self.db_segment_size_bytes_minimum, value
                )
                self._db_upper_conversion_limit = (
                    self._db_segment_size_bytes // 2 - 48
                )
                self._db_lower_conversion_limit = (
                    self._db_upper_conversion_limit - 50
                )
        else:
            self._db_segment_size_bytes = 16
            self._db_upper_conversion_limit = 7
            self._db_lower_conversion_limit = 4
        self._db_segment_size = self._db_segment_size_bytes * 8
        self._db_top_record_number_in_segment = self._db_segment_size - 1
        self._empty_bitarray_bytes = b"\x00" * self._db_segment_size_bytes
        if bytebit.SINGLEBIT is True:
            self._empty_bitarray = bytebit.Bitarray(self._db_segment_size)
        else:
            self._empty_bitarray = (
                bytebit.Bitarray("0") * self._db_segment_size
            )

    @property
    def db_segment_size_bytes_maximum(self):
        """Maximum value allowed for segment size bytes."""
        return 8192

    @property
    def db_segment_size_bytes_minimum(self):
        """Minimum value allowed for segment size bytes."""
        return 500

    @property
    def segment_sort_scale(self):
        """Return scaling factor for deferred updates.

        After segment_sort_scale index entries have been added in
        deferred update mode, the deferred index updates for those
        records are done.

        Value is chosen by application to limit the size of a deferred
        update.  The number of index entries created per record and
        available memory (for sorting index values) are the important
        factors.

        Deferred updates are a lot slower if the segment_sort_scale
        limit for index entries is reached by less than segment_size
        records.

        """
        return self._segment_sort_scale

    @property
    def db_segment_size(self):
        """Return number of records represented in a segment.

        The record numbers are a range, the same in all segments.

        Default is 32768.

        """
        return self._db_segment_size

    @property
    def db_top_record_number_in_segment(self):
        """Return high record number in a segment.

        Default is 32767.

        Most database engines count records from 1, so record number 0 in
        segment 0 in their databases is never used.

        """
        return self._db_top_record_number_in_segment

    @property
    def db_upper_conversion_limit(self):
        """Return record count at which list is converted to bitarray.

        The number of records in a segment which causes conversion of lists
        to bitmaps when reached by addition of a record to the segment.

        Default is 2000.

        """
        return self._db_upper_conversion_limit

    @property
    def db_lower_conversion_limit(self):
        """Return record count at which bitarray is converted to list.

        The number of records in a segment which causes conversion of
        bitmaps to lists when reached by deletion of a record from the
        segment.

        Default is 1950.

        """
        return self._db_lower_conversion_limit

    @property
    def empty_bitarray_bytes(self):
        """Return segment sized bytes object with all bytes set to 0."""
        return self._empty_bitarray_bytes

    @property
    def empty_bitarray(self):
        """Return segment sized bitarray object with all bits set to 0."""
        return self._empty_bitarray


SegmentSize = SegmentSize()

# Hack: all databases are still 65536 record number segments.
# SegmentSize.db_segment_size_bytes = 8192
