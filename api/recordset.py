# recordset.py
# Copyright 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A Recordset class using bitarrays and lists to represent sets of records.

Follows the example of DPT's record sets (www.dptoolkit.com).

List of classes

Recordset - collection of segments representing record numbers
SegmentBitarray - Cursor and foundset actions for bit map segment
SegmentInt - Cursor and foundset actions for single record number segment
SegmentList - Cursor and foundset actions for record number list segment

"""

from collections import deque
from copy import deepcopy
from bisect import bisect_left

from .bytebit import Bitarray, SINGLEBIT, EMPTY_BITARRAY

from .constants import (
    DB_SEGMENT_SIZE,
    DB_CONVERSION_LIMIT,
    DB_SEGMENT_SIZE_BYTES,
    )

# Ultimate home to be decided
#EMPTY_BITARRAY = bitarray('0') * DB_SEGMENT_SIZE
EMPTY_BITARRAY_BYTES = b'\x00' * DB_SEGMENT_SIZE_BYTES


class SegmentInt(object):
    
    """Segment for record number interval with one record.

    Methods added:

    count_records
    current
    first
    get_position_of_record_number
    get_record_number_at_position
    last
    next
    normalize
    prev
    promote
    setat
    tobytes
    __and__
    __contains__
    __deepcopy__
    __or__
    __xor__
    _empty_segment
    
    Methods overridden:

    None
    
    Methods extended:

    __init__

    Properties:

    segment_number
    
    """
    # The refresh_recordset may be relevent in this class

    def __init__(self, segment_number, key, records=b''):
        """Create segment for key for records (one record) in segment number.

        records is segment_record_number.to_bytes(n, byteorder='big') where
        segment_number, segment_record_number = divmod(
            record_number_in_file, DB_SEGMENT_SIZE)

        """
        super(SegmentInt, self).__init__()
        self._record_number = int.from_bytes(records, byteorder='big')
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)"""
        return self._segment_number

    def count_records(self):
        """Return record count in segment"""
        return 1

    def current(self):
        """Return current record in segement"""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._record_number + (self._segment_number * DB_SEGMENT_SIZE))
        else:
            return None

    def first(self):
        """Return first record in segment"""
        if self._current_position_in_segment is None:
            self._current_position_in_segment = 0
        return (
            self._key,
            self._record_number + (self._segment_number * DB_SEGMENT_SIZE))

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist"""
        return 0 if recnum < self._record_number else 1

    def get_record_number_at_position(self, position, forward=True):
        """Return record number at position from start or end of segment"""
        return self._record_number + (self._segment_number * DB_SEGMENT_SIZE)

    def last(self):
        """Return last record in segment"""
        if self._current_position_in_segment is None:
            self._current_position_in_segment = 0
        return (
            self._key,
            self._record_number + (self._segment_number * DB_SEGMENT_SIZE))

    def next(self):
        """Return next record in segment"""
        if self._current_position_in_segment is None:
            return self.first()
        else:
            return None

    def prev(self):
        """Return previous record in segment"""
        if self._current_position_in_segment is None:
            return self.last()
        else:
            return None

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_in_segment = divmod(record, DB_SEGMENT_SIZE)
        if record == (self._record_number +
                      (self._segment_number * DB_SEGMENT_SIZE)):
            self._current_position_in_segment = 0
            return (self._key, record)
        else:
            return None

    def _empty_segment(self):
        """Create and return an empty instance of SegmentInt."""
        class E(SegmentInt):
            def __init__(self):
                pass
        e = E()
        e.__class__ = SegmentInt
        return e

    def __deepcopy__(self, memo):
        """Return a customized copy of self."""
        sc = self._empty_segment()
        # deepcopy the object representing the records in the segment
        sc._record_number = deepcopy(self._record_number, memo)
        # bind the immutable attributes
        sc._key = self._key
        sc._segment_number = self._segment_number
        # the copy forgets the current position in segment
        sc._current_position_in_segment = None
        return sc

    def __contains__(self, relative_record_number):
        """Return True if relative record number is in self, else False"""
        return bool(relative_record_number == self._record_number)

    def normalize(self):
        """Return version of self appropriate to record count of self."""
        return self

    def promote(self):
        """Return SegmentBitarray version of self."""
        sb = SegmentBitarray(
            self._segment_number, self._key, EMPTY_BITARRAY_BYTES)
        sb._bitarray[self._record_number] = True
        return sb

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'or' segments with different segment numbers")
        return self.promote() | other.promote()

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'and' segments with different segment numbers")
        return self.promote() & other.promote()

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'xor' segments with different segment numbers")
        return self.promote() ^ other.promote()

    def tobytes(self):
        """Return self._record_number as bytes."""
        return self._record_number.to_bytes(2, byteorder='big')


class SegmentBitarray(object):
    
    """Segment for record number interval with over DB_CONVERSION_LIMIT records.

    Methods added:

    count_records
    current
    first
    get_position_of_record_number
    get_record_number_at_position
    last
    next
    normalize
    prev
    promote
    setat
    tobytes
    __and__
    __contains__
    __deepcopy__
    __ior__
    __iand__
    __ixor__
    __or__
    __xor__
    _empty_segment
    
    Methods overridden:

    None
    
    Methods extended:

    __init__

    Properties:

    segment_number
    
    """
    # The refresh_recordset may be relevent in this class

    def __init__(self, segment_number, key, records=EMPTY_BITARRAY_BYTES):
        """Create bitarray segment for key for records in segment number.

        records is rnbitarray.tobytes() where rnbitarray is a bitarray of length
        DB_SEGMENT_SIZE bits and a set bit rnbitarray[segment_record_number]
        means segment_record_number is in the segment given
        segment_number, segment_record_number = divmod(
            record_number_in_file, DB_SEGMENT_SIZE)

        """
        super(SegmentBitarray, self).__init__()
        self._bitarray = Bitarray()
        self._bitarray.frombytes(records)
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None
        self._reversed = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)"""
        return self._segment_number

    def count_records(self):
        """Return record count in segment"""
        return self._bitarray.count()

    def current(self):
        """Return current record in segemnet"""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * DB_SEGMENT_SIZE))
        else:
            return None

    def first(self):
        """Return first record in segment"""
        try:
            self._current_position_in_segment = self._bitarray.index(True, 0)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * DB_SEGMENT_SIZE))
        except ValueError:
            return None

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist"""
        return self._bitarray[:recnum].count()

    def get_record_number_at_position(self, position, forward=True):
        """Return record number at position from start or end of segment"""
        if forward:
            try:
                record = self._bitarray.search(SINGLEBIT, position)[-1]
                return (record + (self._segment_number * DB_SEGMENT_SIZE))
            except ValueError:
                return None
        else:
            try:
                record = self._bitarray.search(SINGLEBIT)[position]
                return (record + (self._segment_number * DB_SEGMENT_SIZE))
            except ValueError:
                return None

    def last(self):
        """Return last record in segment"""
        if self._reversed is None:
            self._reversed = self._bitarray.copy()
            self._reversed.reverse()
        try:
            rcpis = self._reversed.index(True, 0)
            self._current_position_in_segment = DB_SEGMENT_SIZE - rcpis - 1
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * DB_SEGMENT_SIZE))
        except ValueError:
            return None

    def next(self):
        """Return next record in segment"""
        if self._current_position_in_segment is None:
            return self.first()
        try:
            self._current_position_in_segment = self._bitarray.index(
                True,
                self._current_position_in_segment + 1,
                DB_SEGMENT_SIZE - 1)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * DB_SEGMENT_SIZE))
        except ValueError:
            return None

    def prev(self):
        """Return previous record in segment"""
        if self._current_position_in_segment is None:
            return self.last()
        if self._reversed is None:
            self._reversed = self._bitarray.copy()
            self._reversed.reverse()
        try:
            rcpis = DB_SEGMENT_SIZE - self._current_position_in_segment
            rcpis = self._reversed.index(
                True,
                rcpis,
                DB_SEGMENT_SIZE - 1)
            self._current_position_in_segment = DB_SEGMENT_SIZE - rcpis - 1
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * DB_SEGMENT_SIZE))
        except ValueError:
            return None

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_in_segment = divmod(record, DB_SEGMENT_SIZE)
        if (self._bitarray[record_in_segment] and
            self._segment_number == segment):
            self._current_position_in_segment = record_in_segment
            return (self._key, record)
        else:
            return None

    def normalize(self):
        """Return version of self appropriate to record count of self."""
        c = self._bitarray.count()
        if c > DB_CONVERSION_LIMIT:
            return self
        elif c == 1:
            # May be better to use the SegmentList style in else clause.
            return SegmentInt(
                self._segment_number,
                self._key,
                records=self._bitarray.search(
                    SINGLEBIT)[0].to_bytes(2, byteorder='big'))
        else:
            sl = SegmentList(self._segment_number, self._key)
            sl._list.extend(self._bitarray.search(SINGLEBIT))
            return sl

    def promote(self):
        """Return SegmentBitarray version of self."""
        return self

    def _empty_segment(self):
        """Create and return an empty instance of SegmentBitarray."""
        class E(SegmentBitarray):
            def __init__(self):
                pass
        e = E()
        e.__class__ = SegmentBitarray
        return e

    def __deepcopy__(self, memo):
        """Return a customized copy of self."""
        sc = self._empty_segment()
        # deepcopy the object representing the records in the segment
        sc._bitarray = deepcopy(self._bitarray, memo)
        # bind the immutable attributes
        sc._key = self._key
        sc._segment_number = self._segment_number
        # the copy forgets the current position in segment
        sc._current_position_in_segment = None
        # the copy makes its own reverse when needed
        # the original may be wrong when copy used in boolean operations
        sc._reversed = None
        return sc

    def __contains__(self, relative_record_number):
        """Return True if relative record number is in self, else False"""
        return self._bitarray[relative_record_number]

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'or' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray |= other.promote()._bitarray
        return sb

    def __ior__(self, other):
        """Include records in other segment in self segment"""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'ior' segments with different segment numbers")
        self._bitarray |= other.promote()._bitarray
        return self

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'and' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray &= other.promote()._bitarray
        return sb

    def __iand__(self, other):
        """Remove records from self which are not in other."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'iand' segments with different segment numbers")
        self._bitarray &= other.promote()._bitarray
        return self

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'xor' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray ^= other.promote()._bitarray
        return sb

    def __ixor__(self, other):
        """Include records in other segment in self segment"""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'ixor' segments with different segment numbers")
        self._bitarray ^= other.promote()._bitarray
        return self

    def tobytes(self):
        """Return self._bitarray as bytes."""
        return self._bitarray.tobytes()


class SegmentList(object):
    
    """Segment for record number interval of up to DB_CONVERSION_LIMIT records.

    Methods added:

    count_records
    current
    first
    get_position_of_record_number
    get_record_number_at_position
    insort_left_nodup
    last
    next
    normalize
    prev
    promote
    setat
    tobytes
    __and__
    __contains__
    __deepcopy__
    __or__
    __xor__
    _empty_segment
    
    Methods overridden:

    None
    
    Methods extended:

    __init__

    Properties:

    segment_number
    
    """
    # The refresh_recordset may be relevent in this class

    def __init__(self, segment_number, key, records=b''):
        """Create list segment for key for records in segment number.

        records is ''.join([rn.to_bytes(n, byteorder='big') for rn in rnlist}
        where rnlist is a sorted list of segment_record_number and
        segment_number, segment_record_number = divmod(
            record_number_in_file, DB_SEGMENT_SIZE)

        """
        super(SegmentList, self).__init__()
        self._list = []
        for i in range(0, len(records), 2):
            self.insort_left_nodup(
                int.from_bytes(records[i:i+2], byteorder='big'))
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)"""
        return self._segment_number

    def count_records(self):
        """Return record count in segment"""
        return len(self._list)

    def current(self):
        """Return current record in segment"""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * DB_SEGMENT_SIZE))
        else:
            return None

    def first(self):
        """Return first record in segment"""
        try:
            self._current_position_in_segment = 0
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * DB_SEGMENT_SIZE))
        except TypeError:
            if self._segment_number is None:
                return None
            else:
                raise

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist"""
        try:
            return self._list.index(recnum) + 1
        except ValueError:
            return len([e for e in self._list if recnum >= e])

    def get_record_number_at_position(self, position, forward=True):
        """Return record number at position from start or end of segment"""
        if forward:
            return (
                self._list[position] +
                (self._segment_number * DB_SEGMENT_SIZE))
        else:
            return (
                self._list[len(self._list) - position - 1] +
                (self._segment_number * DB_SEGMENT_SIZE))

    def last(self):
        """Return last record in segment"""
        try:
            self._current_position_in_segment = len(self._list) - 1
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * DB_SEGMENT_SIZE))
        except TypeError:
            if self._segment_number is None:
                return None
            else:
                raise

    def next(self):
        """Return next record in segment"""
        if self._current_position_in_segment is None:
            return self.first()
        else:
            self._current_position_in_segment += 1
            if self._current_position_in_segment < len(self._list):
                return (
                    self._key,
                    self._list[self._current_position_in_segment] +
                    (self._segment_number * DB_SEGMENT_SIZE))
            self._current_position_in_segment = len(self._list) - 1
            return None

    def prev(self):
        """Return previous record in segment"""
        if self._current_position_in_segment is None:
            return self.last()
        else:
            self._current_position_in_segment -= 1
            if self._current_position_in_segment < 0:
                self._current_position_in_segment = 0
                return None
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * DB_SEGMENT_SIZE))

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_number = divmod(record, DB_SEGMENT_SIZE)
        if self._segment_number == segment:
            try:
                self._current_position_in_segment = self._list.index(
                    record_number)
                return (self._key, record)
            except ValueError:
                return None
        else:
            return None

    def insort_left_nodup(self, record_number):
        """Insert record_number in sorted order without duplicating entries."""
        i = bisect_left(self._list, record_number)
        if i != len(self._list) and self._list[i] == record_number:
            return
        self._list.insert(i, record_number)

    # Only if SegmentList items are guaranteed sorted ascending order.
    def __contains__(self, relative_record_number):
        """Return True if relative record number is in self, else False"""
        i = bisect_left(self._list, relative_record_number)
        return bool(i != len(self._list) and
                    self._list[i] == relative_record_number)

    def normalize(self):
        """Return version of self appropriate to record count of self."""
        c = self.count_records()
        if c > DB_CONVERSION_LIMIT:
            return self.promote()
        elif c == 1:
            # See comment in SegmentBitarray.normalize()
            return SegmentInt(
                self._segment_number,
                self._key,
                records=self._list[0].to_bytes(2, byteorder='big'))
        else:
            return self

    def promote(self):
        """Return SegmentBitarray version of self."""
        sb = SegmentBitarray(
            self._segment_number, self._key, EMPTY_BITARRAY_BYTES)
        for r in self._list:
            sb._bitarray[r] = True
        return sb

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'or' segments with different segment numbers")
        return self.promote() | other.promote()

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'and' segments with different segment numbers")
        return self.promote() & other.promote()

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RuntimeError(
                "Attempt to 'xor' segments with different segment numbers")
        return self.promote() ^ other.promote()

    def _empty_segment(self):
        """Create and return an empty instance of SegmentList."""
        class E(SegmentList):
            def __init__(self):
                pass
        e = E()
        e.__class__ = SegmentList
        return e

    def __deepcopy__(self, memo):
        """Return a customized copy of self."""
        sc = self._empty_segment()
        # deepcopy the object representing the records in the segment
        sc._list = deepcopy(self._list, memo)
        # bind the immutable attributes
        sc._key = self._key
        sc._segment_number = self._segment_number
        # the copy forgets the current position in segment
        sc._current_position_in_segment = None
        return sc

    def tobytes(self):
        """Return self._list as bytes."""
        return b''.join([n.to_bytes(2, byteorder='big')for n in self._list])


class Recordset(object):
    
    """Define a record set on a database with record access.

    Methods added:

    close
    clear_recordset
    count_records
    current
    first
    get_position_of_record_number
    get_record_number_at_position
    insort_left_nodup
    is_record_number_in_record_set
    last
    next
    normalize
    prev
    setat
    __contains__
    __deepcopy__
    __del__
    __delitem__
    __getitem__
    __len__
    __setitem__
    _empty_recordset

    Methods overridden:

    None

    Methods extended:

    __init__

    Properties:

    dbidentity
    rs_segments
    sorted_segnums
    
    Notes:

    May need nearest get_position_of_record and get_record_at_position as well.

    """

    def __init__(self, dbhome, dbset, cache_size=1):
        """Create recordset for database using deque of size cache_size.

        dbhome = instance of a subclass of Database.
        dbset = name of set of associated databases in dbhome to be accessed.
        cache_size = size of cache for recently accessed records
        
        Specifying cache_size less than 1, or None, gives deque(maxlen=1).

        A recordset is associated with dbset.  There is no dbname argument,
        like for DataSource, because it does not matter which dbname was used
        to create it when comparing or combining recordsets.

        """
        super(Recordset, self).__init__()
        if dbhome.exists(dbset, dbset):
            self._dbhome = dbhome
            self._dbset = dbset
            self._database = dbhome.get_database(dbset, dbset)
            dbhome.get_database_instance(dbset, dbset)._recordsets[self] = True
        else:
            self._dbhome = None
            self._dbset = None
            self._database = None
        self._rs_segments = dict()
        self.record_cache = dict()
        self.record_deque = deque(maxlen=max(1, cache_size))
        self._current_segment = None
        self._sorted_segnums = []
        self._clientcursors = dict()

    def __del__(self):
        """Delete record set"""
        self.close()

    def close(self):
        """Close record set making it unusable"""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        try:
            del self._dbhome.get_database_instance(
                self._dbset, self._dbset)._recordsets[self]
        except:
            pass
        self._dbhome = None
        self._dbset = None
        self._database = None
        self._rs_segments = None
        self.record_cache = None
        self.record_deque = None
        self._current_segment = None
        self._sorted_segnums = None

    def clear_recordset(self):
        """Remove all records from instance record set"""
        self._rs_segments.clear()
        self.record_cache.clear()
        self.record_deque.clear()
        self._current_segment = None
        self._sorted_segnums.clear()

    @property
    def dbset(self):
        """Return name of database from which record set created"""
        return self._dbset

    @property
    def dbidentity(self):
        """Return id(database) from which record set created"""
        return id(self._database)

    @property
    def rs_segments(self):
        """Return dictionary of populated segments {segment_number:segment}"""
        return self._rs_segments

    @property
    def sorted_segnums(self):
        """Return sorted list of segment numbers of populated segments"""
        return self._sorted_segnums

    def __len__(self):
        """Return number of segments in record set"""
        return len(self._rs_segments)

    def __getitem__(self, segment):
        """Return segment in record set"""
        return self._rs_segments[segment]

    def __setitem__(self, segment, record_numbers):
        """Add segment to record set"""
        self._rs_segments[segment] = record_numbers
        self.insort_left_nodup(segment)

    def __delitem__(self, segment):
        """Remove segment from record set"""
        del self._rs_segments[segment]
        i = bisect_left(self._sorted_segnums, segment)
        if i != len(self._sorted_segnums):
            if self._sorted_segnums[i] == segment:
                del self._sorted_segnums[i]
                if self._current_segment is not None:
                    if self._current_segment >= len(self._sorted_segnums):
                        self._current_segment = len(self._sorted_segnums) - 1

    def __contains__(self, segment):
        """Return True if segment is in self, else False"""
        return bool(segment in self._rs_segments)

    def count_records(self):
        """Return number of records in recordset."""
        return sum([s.count_records() for s in self._rs_segments.values()])

    def get_position_of_record_number(self, recnum):
        """Return recnum position in recordset counting records that exist"""
        segment, record_number = divmod(recnum, DB_SEGMENT_SIZE)
        try:
            position = self._rs_segments[segment].get_position_of_record_number(
                record_number)
        except KeyError:
            position = 0
        return (sum([self._rs_segments[s].count_records()
                     for s in self._rs_segments if s < segment]) +
                position)

    def get_record_number_at_position(self, position):
        """Return record number at position from start or end of recordset"""
        p = 0
        rp = abs(position) - 1 if position < 0 else position
        # Change this to use _sorted_segnums to reference segments?
        for s, rseg in sorted(self._rs_segments.items(), reverse=position<0):
            c = rseg.count_records()
            if p + c > rp:
                return rseg.get_record_number_at_position(
                    rp-p, forward=position>=0)
            else:
                p += c
        return None

    def insort_left_nodup(self, segment):
        """Insert item in sorted order without duplicating entries."""
        i = bisect_left(self._sorted_segnums, segment)
        if i != len(self._sorted_segnums):
            if self._sorted_segnums[i] == segment:
                return
        self._sorted_segnums.insert(i, segment)

    def first(self):
        """Return first record in recordset"""
        sn = self._sorted_segnums[0]
        try:
            self._current_segment = 0
            return self._rs_segments[sn].first()
        except ValueError:
            return None

    def last(self):
        """Return last record in recordset"""
        sn = self._sorted_segnums[-1]
        try:
            self._current_segment = len(self._rs_segments) - 1
            return self._rs_segments[sn].last()
        except ValueError:
            return None

    def next(self):
        """Return next record in recordset"""
        if self._current_segment is None:
            return self.first()
        r = self._rs_segments[
            self._sorted_segnums[self._current_segment]].next()
        if r is not None:
            return r
        if self._current_segment + 1 == len(self._sorted_segnums):
            return None
        self._current_segment += 1
        return self._rs_segments[
            self._sorted_segnums[self._current_segment]].first()

    def prev(self):
        """Return previous record in recordset"""
        if self._current_segment is None:
            return self.last()
        r = self._rs_segments[
            self._sorted_segnums[self._current_segment]].prev()
        if r is not None:
            return r
        if self._current_segment == 0:
            return None
        self._current_segment -= 1
        return self._rs_segments[
            self._sorted_segnums[self._current_segment]].last()

    def current(self):
        """Return current record in recordset"""
        if self._current_segment is None:
            return None
        return self._rs_segments[
            self._sorted_segnums[self._current_segment]].current()

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_number = divmod(record, DB_SEGMENT_SIZE)
        if segment not in self:
            return None
        r = self._rs_segments[segment].setat(record)
        if r is None:
            return None
        self._current_segment = self._sorted_segnums.index(segment)
        return r

    def __or__(self, other):
        """Return new record set of self records with other records included."""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'or' record sets for different databases")
        rs = Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                rs[segment] = v | other[segment]
            else:
                rs[segment] = deepcopy(v)
        for segment, v in other._rs_segments.items():
            if segment not in self:
                rs[segment] = deepcopy(v)
        return rs

    def __ior__(self, other):
        """Include records in other record set in self record set"""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'ior' record sets for different databases")
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                self[segment] = v | other[segment]
        for segment, v in other._rs_segments.items():
            if segment not in self:
                self[segment] = deepcopy(v)
        return self

    def __and__(self, other):
        """Return record set of records in both self and other record sets."""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'and' record sets for different databases")
        rs = Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                rs[segment] = v & other[segment]
                if rs[segment].count_records() == 0:
                    del rs[segment]
        return rs

    def __iand__(self, other):
        """Remove records from self which are not in other."""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'iand' record sets for different databases")
        drs = []
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                self[segment] = v & other[segment]
                if self[segment].count_records() == 0:
                    drs.append(segment)
            else:
                drs.append(segment)
        for segment in drs:
            del self[segment]
        return self

    def __xor__(self, other):
        """Return record set of self records with other records included."""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'xor' record sets for different databases")
        rs = Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                rs[segment] = v ^ other[segment]
                if rs[segment].count_records() == 0:
                    del rs[segment]
            else:
                rs[segment] = deepcopy(v)
        for segment, v in other._rs_segments.items():
            if segment not in self:
                rs[segment] = deepcopy(v)
        return rs

    def __ixor__(self, other):
        """Include records in other record set in self record sets"""
        if self._database != other._database:
            raise RuntimeError(
                "Attempt to 'ixor' record sets for different databases")
        drs = []
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being SegmentInt should be special case
                self[segment] = v ^ other[segment]
                if self[segment].count_records() == 0:
                    drs.append(segment)
        for segment, v in other._rs_segments.items():
            if segment not in self:
                self[segment] = deepcopy(v)
        for segment in drs:
            del self[segment]
        return self

    def normalize(self):
        """Convert record set segments to version for record count."""
        for segment in self._sorted_segnums:
            self._rs_segments[segment] = self._rs_segments[segment].normalize()

    def is_record_number_in_record_set(self, record_number):
        """Return True if record number is in self, a record set, else False"""
        segment, record_number = divmod(record_number, DB_SEGMENT_SIZE)
        return (False if segment not in self else
                record_number in self._rs_segments[segment])

    def _empty_recordset(self):
        """Create and return an empty instance of Recordset."""
        class E(Recordset):
            def __init__(self):
                pass
        e = E()
        e.__class__ = Recordset
        return e

    def __deepcopy__(self, memo):
        """Return a customized copy of self."""
        sc = self._empty_recordset()
        # deepcopy the objects representing the records in the segment
        sc._rs_segments = deepcopy(self._rs_segments, memo)
        sc._sorted_segnums = deepcopy(self._sorted_segnums, memo)
        # bind the immutable attributes
        sc._dbhome = self._dbhome
        sc._dbset = self._dbset
        sc._database = self._database
        # the copy forgets the current position in recordset
        sc._current_segment = None
        # the copy forgets the current recordset cursors
        sc._clientcursors = dict()
        # the copy forgets the current recordset cache
        sc.record_cache = dict()
        sc.record_deque = deque(maxlen=self.record_deque.maxlen)
        # register the copy with the database
        if sc._dbhome is not None:
            sc._dbhome.get_database_instance(
                sc._dbset, sc._dbset)._recordsets[sc] = True
        return sc
