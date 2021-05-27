# recordset.py
# Copyright 2013 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A Recordset class using bitarrays and lists to represent sets of
records.

Follows the example of DPT's record sets (www.dptoolkit.com).

"""

from collections import deque
from copy import deepcopy
from bisect import bisect_left

from .bytebit import Bitarray, SINGLEBIT
from .segmentsize import SegmentSize
from . import cursor


class RecordsetError(Exception):
    pass


class RecordsetSegmentInt:
    
    """Segment for record number interval with one record.
    """
    # The refresh_recordset may be relevent in this class

    # Should records argument be like the RecordsetSegmentBitarray version?
    def __init__(self, segment_number, key, records=b''):
        """Create segment for key for records (one record) in segment number.

        records is segment_record_number.to_bytes(n, byteorder='big') where
        segment_number, segment_record_number = divmod(
            record_number_in_file, SegmentSize.db_segment_size)

        """
        super().__init__()
        self._record_number = int.from_bytes(records, byteorder='big')
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)."""
        return self._segment_number

    def count_records(self):
        """Return record count in segment."""
        return 1

    def current(self):
        """Return current record in segment."""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._record_number + (self._segment_number *
                                       SegmentSize.db_segment_size))
        else:
            return None

    def first(self):
        """Return first record in segment."""
        if self._current_position_in_segment is None:
            self._current_position_in_segment = 0
        return (
            self._key,
            self._record_number + (self._segment_number *
                                   SegmentSize.db_segment_size))

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist."""
        return 0 if recnum < self._record_number else 1

    def get_record_number_at_position(self, position):
        """Return record number at position from start or end of segment."""
        if position in (0, -1):
            return self._record_number + (self._segment_number *
                                          SegmentSize.db_segment_size)

    def last(self):
        """Return last record in segment."""
        if self._current_position_in_segment is None:
            self._current_position_in_segment = 0
        return (
            self._key,
            self._record_number + (self._segment_number *
                                   SegmentSize.db_segment_size))

    def next(self):
        """Return next record in segment."""
        if self._current_position_in_segment is None:
            return self.first()
        else:
            return None

    def prev(self):
        """Return previous record in segment."""
        if self._current_position_in_segment is None:
            return self.last()
        else:
            return None

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        if record == (self._record_number +
                      (self._segment_number * SegmentSize.db_segment_size)):
            self._current_position_in_segment = 0
            return (self._key, record)
        else:
            return None

    def _empty_segment(self):
        """Create and return an empty instance of RecordsetSegmentInt."""
        class E(RecordsetSegmentInt):
            def __init__(self):
                pass
        e = E()
        e.__class__ = RecordsetSegmentInt
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
        """Return True if relative record number is in self, else False."""
        return bool(relative_record_number == self._record_number)

    def normalize(self, use_upper_limit=True):
        """Return version of self appropriate to record count of self.

        limit is relevant to lists and bitarrays of record numbers.

        """
        return self

    def promote(self):
        """Return RecordsetSegmentBitarray version of self."""
        sb = RecordsetSegmentBitarray(
            self._segment_number, self._key, SegmentSize.empty_bitarray_bytes)
        sb._bitarray[self._record_number] = True
        return sb

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'or' segments with different segment numbers")
        return self.promote() | other.promote()

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'and' segments with different segment numbers")
        return self.promote() & other.promote()

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'xor' segments with different segment numbers")
        return self.promote() ^ other.promote()

    def tobytes(self):
        """Return self._record_number as bytes."""
        return self._record_number.to_bytes(2, byteorder='big')


class RecordsetSegmentBitarray:
    
    """Segment for record number interval with over db_upper_conversion_limit
    records.  Note that a segment which is losing records remains a bitmap
    until db_lower_conversion_limit records are in segment.
    """
    # The refresh_recordset may be relevent in this class

    def __init__(self, segment_number, key, records=None):
        """Create bitarray segment for key for records in segment number.

        records is rnbitarray.tobytes() where rnbitarray is a bitarray of length
        SegmentSize.db_segment_size bits and a set bit
        rnbitarray[segment_record_number] means segment_record_number is in the
        segment given
        segment_number, segment_record_number = divmod(
            record_number_in_file, SegmentSize.db_segment_size)

        """
        super().__init__()
        if records is None:
            records = SegmentSize.empty_bitarray_bytes
        self._bitarray = Bitarray()
        self._bitarray.frombytes(records)
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None
        self._reversed = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)."""
        return self._segment_number

    def count_records(self):
        """Return record count in segment."""
        return self._bitarray.count()

    def current(self):
        """Return current record in segment."""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * SegmentSize.db_segment_size))
        else:
            return None

    def first(self):
        """Return first record in segment."""
        try:
            self._current_position_in_segment = self._bitarray.index(True, 0)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * SegmentSize.db_segment_size))
        except ValueError:
            return None

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist."""
        return bisect_left(self._bitarray.search(SINGLEBIT), recnum)

    def get_record_number_at_position(self, position):
        """Return record number at position in segment.

        segment[0] means return record number for first set bit in segment,
        segment[-1] means return record number for last set bit in segment,

        """
        try:
            record = self._bitarray.search(SINGLEBIT)[position]
            return (record + (self._segment_number *
                              SegmentSize.db_segment_size))
        except IndexError:
            return None

    def last(self):
        """Return last record in segment."""
        if self._reversed is None:
            self._reversed = self._bitarray.copy()
            self._reversed.reverse()
        try:
            rcpis = self._reversed.index(True, 0)
            self._current_position_in_segment = (
                SegmentSize.db_segment_size - rcpis - 1)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * SegmentSize.db_segment_size))
        except ValueError:
            return None

    def next(self):
        """Return next record in segment."""
        if self._current_position_in_segment is None:
            return self.first()
        try:
            self._current_position_in_segment = self._bitarray.index(
                True,
                self._current_position_in_segment + 1,
                SegmentSize.db_segment_size - 1)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * SegmentSize.db_segment_size))
        except ValueError:
            return None

    def prev(self):
        """Return previous record in segment."""
        if self._current_position_in_segment is None:
            return self.last()
        if self._reversed is None:
            self._reversed = self._bitarray.copy()
            self._reversed.reverse()
        try:
            rcpis = (
                SegmentSize.db_segment_size - self._current_position_in_segment)
            rcpis = self._reversed.index(
                True,
                rcpis,
                SegmentSize.db_segment_size - 1)
            self._current_position_in_segment = (
                SegmentSize.db_segment_size - rcpis - 1)
            return (
                self._key,
                self._current_position_in_segment +
                (self._segment_number * SegmentSize.db_segment_size))
        except ValueError:
            return None

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_in_segment = divmod(record, SegmentSize.db_segment_size)
        if (self._bitarray[record_in_segment] and
            self._segment_number == segment):
            self._current_position_in_segment = record_in_segment
            return (self._key, record)
        else:
            return None

    def normalize(self, use_upper_limit=True):
        """Return version of self appropriate to record count of self.

        limit allows a range of record numbers, below the upper conversion
        limit, to be defined where lists and bitarrays are not converted to
        the other form on deletion or insertion of records.  The ides is to
        avoid excessive conversion in delete-insert sequences around a single
        conversion point.

        """
        c = self._bitarray.count()
        if c > SegmentSize.db_upper_conversion_limit:
            return self
        if use_upper_limit:
            limit = SegmentSize.db_upper_conversion_limit
        else:
            limit = SegmentSize.db_lower_conversion_limit
        if c > limit:
            return self
        elif c == 1:
            return RecordsetSegmentInt(
                self._segment_number,
                self._key,
                records=self._bitarray.search(
                    SINGLEBIT)[0].to_bytes(2, byteorder='big'))
        else:
            # RecordsetSegmentInt style, above, may be better here.
            sl = RecordsetSegmentList(self._segment_number, self._key)
            sl._list.extend(self._bitarray.search(SINGLEBIT))
            return sl

    def promote(self):
        """Return RecordsetSegmentBitarray version of self."""
        return self

    def _empty_segment(self):
        """Create and return an empty instance of RecordsetSegmentBitarray."""
        class E(RecordsetSegmentBitarray):
            def __init__(self):
                pass
        e = E()
        e.__class__ = RecordsetSegmentBitarray
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
        """Return True if relative record number is in self, else False."""
        return self._bitarray[relative_record_number]

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'or' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray |= other.promote()._bitarray
        return sb

    def __ior__(self, other):
        """Include records in other segment in self segment."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'ior' segments with different segment numbers")
        self._bitarray |= other.promote()._bitarray
        return self

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'and' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray &= other.promote()._bitarray
        return sb

    def __iand__(self, other):
        """Remove records from self which are not in other."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'iand' segments with different segment numbers")
        self._bitarray &= other.promote()._bitarray
        return self

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'xor' segments with different segment numbers")
        sb = deepcopy(self)
        sb._bitarray ^= other.promote()._bitarray
        return sb

    def __ixor__(self, other):
        """Include records in other segment in self segment."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'ixor' segments with different segment numbers")
        self._bitarray ^= other.promote()._bitarray
        return self

    def tobytes(self):
        """Return self._bitarray as bytes."""
        return self._bitarray.tobytes()

    def __setitem__(self, key, value):
        """"""
        segment, offset = key
        if segment != self._segment_number:
            raise RecordsetError(
                ''.join((
                    "'", self.__class__.__name__,
                    "' segment is not the one for this 'key'")))
        self._bitarray[offset] = value


class RecordsetSegmentList:
    
    """Segment for record number interval of up to, but not including,
    db_upper_conversion_limit records.  Note that bitmaps for segments with
    less than db_upper_conversion_limit records may exist when a segment is
    losing records, until db_lower_conversion_limit is reached.
    """
    # The refresh_recordset may be relevent in this class

    # Should records argument be like the RecordsetSegmentBitarray version?
    def __init__(self, segment_number, key, records=b''):
        """Create list segment for key for records in segment number.

        records is ''.join([rn.to_bytes(n, byteorder='big') for rn in rnlist}
        where rnlist is a sorted list of segment_record_number and
        segment_number, segment_record_number = divmod(
            record_number_in_file, SegmentSize.db_segment_size)

        """
        super().__init__()
        self._list = []
        for i in range(0, len(records), 2):
            self.insort_left_nodup(
                int.from_bytes(records[i:i+2], byteorder='big'))
        self._key = key
        self._segment_number = segment_number
        self._current_position_in_segment = None

    @property
    def segment_number(self):
        """Return the segment number of the segment (zero-based)."""
        return self._segment_number

    def count_records(self):
        """Return record count in segment."""
        return len(self._list)

    def current(self):
        """Return current record in segment."""
        if self._current_position_in_segment is not None:
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * SegmentSize.db_segment_size))
        else:
            return None

    def first(self):
        """Return first record in segment."""
        try:
            self._current_position_in_segment = 0
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * SegmentSize.db_segment_size))
        except TypeError:
            if self._segment_number is None:
                return None
            else:
                raise

    def get_position_of_record_number(self, recnum):
        """Return position of recnum in segment counting records that exist."""
        try:
            return self._list.index(recnum)# + 1
        except ValueError:
            return len([e for e in self._list if recnum > e])#= e])

    def get_record_number_at_position(self, position):
        """Return record number at position from start or end of segment."""
        try:
            return (
                self._list[position] +
                (self._segment_number * SegmentSize.db_segment_size))
        except IndexError:
            return None

    def last(self):
        """Return last record in segment."""
        try:
            self._current_position_in_segment = len(self._list) - 1
            return (
                self._key,
                self._list[self._current_position_in_segment] +
                (self._segment_number * SegmentSize.db_segment_size))
        except TypeError:
            if self._segment_number is None:
                return None
            else:
                raise

    def next(self):
        """Return next record in segment."""
        if self._current_position_in_segment is None:
            return self.first()
        else:
            self._current_position_in_segment += 1
            if self._current_position_in_segment < len(self._list):
                return (
                    self._key,
                    self._list[self._current_position_in_segment] +
                    (self._segment_number * SegmentSize.db_segment_size))
            self._current_position_in_segment = len(self._list) - 1
            return None

    def prev(self):
        """Return previous record in segment."""
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
                (self._segment_number * SegmentSize.db_segment_size))

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_number = divmod(record, SegmentSize.db_segment_size)
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
        #Insert record_number in sorted order without duplicating entries.
        i = bisect_left(self._list, record_number)
        if i != len(self._list) and self._list[i] == record_number:
            return
        self._list.insert(i, record_number)

    # Only if RecordsetSegmentList items are guaranteed sorted ascending order.
    def __contains__(self, relative_record_number):
        """Return True if relative record number is in self, else False."""
        i = bisect_left(self._list, relative_record_number)
        return bool(i != len(self._list) and
                    self._list[i] == relative_record_number)

    def normalize(self, use_upper_limit=True):
        """Return version of self appropriate to record count of self.

        limit allows a range of record numbers, below the upper conversion
        limit, to be defined where lists and bitarrays are not converted to
        the other form on deletion or insertion of records.  The ides is to
        avoid excessive conversion in delete-insert sequences around a single
        conversion point.

        """
        c = self.count_records()
        if c > SegmentSize.db_upper_conversion_limit:
            return self.promote()
        if use_upper_limit:
            limit = SegmentSize.db_upper_conversion_limit
        else:
            limit = SegmentSize.db_lower_conversion_limit
        if c > limit:
            # It seems the option to promote when use_upper_limit is False is
            # not taken!
            return self
        elif c == 1:
            # See comment in RecordsetSegmentBitarray.normalize()
            return RecordsetSegmentInt(
                self._segment_number,
                self._key,
                records=self._list[0].to_bytes(2, byteorder='big'))
        else:
            return self

    def promote(self):
        """Return RecordsetSegmentBitarray version of self."""
        sb = RecordsetSegmentBitarray(
            self._segment_number, self._key, SegmentSize.empty_bitarray_bytes)
        for r in self._list:
            sb._bitarray[r] = True
        return sb

    def __or__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'or' segments with different segment numbers")
        return self.promote() | other.promote()

    def __and__(self, other):
        """Return new segment of records in both self and other segments."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'and' segments with different segment numbers")
        return self.promote() & other.promote()

    def __xor__(self, other):
        """Return new segment of self records with other records included."""
        if self._segment_number != other._segment_number:
            raise RecordsetError(
                "Attempt to 'xor' segments with different segment numbers")
        return self.promote() ^ other.promote()

    def _empty_segment(self):
        """Create and return an empty instance of RecordsetSegmentList."""
        class E(RecordsetSegmentList):
            def __init__(self):
                pass
        e = E()
        e.__class__ = RecordsetSegmentList
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


class _Recordset:
    
    """Define a record set on a database with record access.

    May need nearest get_position_of_record and get_record_at_position as well.

    _Recordset is roughly equivalent to dptapi.APIRecordList and RecordList is
    roughly equivalent to _dpt._DPTRecordList.

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
        super().__init__()
        self._rs_segments = dict()
        self.record_cache = dict()
        self.record_deque = deque(maxlen=max(1, cache_size))
        self._current_segment = None
        self._sorted_segnums = []
        #self._clientcursors = dict()
        if dbhome.exists(dbset, dbset):
            self._dbhome = dbhome
            self._dbset = dbset
            self._database = dbhome.get_table_connection(dbset)
            #dbhome.get_database_instance(dbset, dbset)._recordsets[self] = True
        else:
            self._dbhome = None
            self._dbset = None
            self._database = None

    # Commented because unittest does not like cache_size='a'. Not understood!
    # Look at this later, but maybe __del__ is not essential?
    #def __del__(self):
    #    """Delete record set."""
    #    self.close()

    def close(self):
        """Close record set making it unusable."""
        #for c in list(self._clientcursors.keys()):
        #    c.close()
        #self._clientcursors.clear()
        #try:
        #    del self._dbhome.get_database_instance(
        #        self._dbset, self._dbset)._recordsets[self]
        #except:
        #    pass
        self._dbhome = None
        self._dbset = None
        self._database = None
        self.clear_recordset()

    def clear_recordset(self):
        """Remove all records from instance record set."""
        self._rs_segments.clear()
        self.record_cache.clear()
        self.record_deque.clear()
        self._current_segment = None
        self._sorted_segnums.clear()

    @property
    def dbhome(self):
        """Return Database instance from which record set created."""
        return self._dbhome

    @property
    def dbset(self):
        """Return name of database from which record set created."""
        return self._dbset

    @property
    def dbidentity(self):
        """Return id(database) from which record set created."""
        return id(self._database)

    @property
    def rs_segments(self):
        """Return dictionary of populated segments {segment_number:segment}."""
        return self._rs_segments

    @property
    def sorted_segnums(self):
        """Return sorted list of segment numbers of populated segments."""
        return self._sorted_segnums

    def __len__(self):
        """Return number of segments in record set."""
        return len(self._rs_segments)

    def __getitem__(self, segment):
        """Return segment in record set."""
        return self._rs_segments[segment]

    def __setitem__(self, segment, record_numbers):
        """Add segment to record set."""
        self._rs_segments[segment] = record_numbers
        self.insort_left_nodup(segment)

    def __delitem__(self, segment):
        """Remove segment from record set."""
        del self._rs_segments[segment]
        i = bisect_left(self._sorted_segnums, segment)
        if i != len(self._sorted_segnums):
            if self._sorted_segnums[i] == segment:
                del self._sorted_segnums[i]
                if self._current_segment is not None:
                    if self._current_segment >= len(self._sorted_segnums):
                        self._current_segment = len(self._sorted_segnums) - 1
                        if self._current_segment < 0:
                            self._current_segment = None

    def __contains__(self, segment):
        """Return True if segment is in self, else False."""
        return bool(segment in self._rs_segments)

    def count_records(self):
        """Return number of records in recordset."""
        return sum([s.count_records() for s in self._rs_segments.values()])

    def get_position_of_record_number(self, recnum):
        """Return recnum position in recordset counting records that exist."""
        segment, record_number = divmod(recnum, SegmentSize.db_segment_size)
        try:
            position = self._rs_segments[segment].get_position_of_record_number(
                record_number)
        except KeyError:
            position = 0
        return (sum([self._rs_segments[s].count_records()
                     for s in self._rs_segments if s < segment]) +
                position)

    def get_record_number_at_position(self, position):
        """Return record number at position from start or end of recordset."""
        c = 0
        segments = self.rs_segments
        if position < 0:
            for s in reversed(self.sorted_segnums):
                segcount = segments[s].count_records()
                c -= segcount
                if c > position:
                    continue
                c += segcount
                return segments[s].get_record_number_at_position(position - c)
        else:
            for s in self.sorted_segnums:
                segcount = segments[s].count_records()
                c += segcount
                if c <= position:
                    continue
                c -= segcount
                return segments[s].get_record_number_at_position(position - c)

    def insort_left_nodup(self, segment):
        """Insert item in sorted order without duplicating entries."""
        i = bisect_left(self._sorted_segnums, segment)
        if i != len(self._sorted_segnums):
            if self._sorted_segnums[i] == segment:
                return
        self._sorted_segnums.insert(i, segment)

    def first(self):
        """Return first record in recordset."""
        try:
            sn = self._sorted_segnums[0]
        except IndexError:
            return None
        try:
            self._current_segment = 0
            return self._rs_segments[sn].first()
        except ValueError:
            return None

    def last(self):
        """Return last record in recordset."""
        try:
            sn = self._sorted_segnums[-1]
        except IndexError:
            return None
        try:
            self._current_segment = len(self._rs_segments) - 1
            return self._rs_segments[sn].last()
        except ValueError:
            return None

    def next(self):
        """Return next record in recordset."""
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
        """Return previous record in recordset."""
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
        """Return current record in recordset."""
        if self._current_segment is None:
            return None
        return self._rs_segments[
            self._sorted_segnums[self._current_segment]].current()

    def setat(self, record):
        """Return current record after positioning cursor at record."""
        segment, record_number = divmod(record, SegmentSize.db_segment_size)
        if segment not in self:
            return None
        r = self._rs_segments[segment].setat(record)
        if r is None:
            return None
        self._current_segment = self._sorted_segnums.index(segment)
        return r

    def __or__(self, other):
        """Return new record set of self records with other records included."""
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'or' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'or' record sets for different tables")
        rs = _Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
                rs[segment] = v | other[segment]
            else:
                rs[segment] = deepcopy(v)
        for segment, v in other._rs_segments.items():
            if segment not in self:
                rs[segment] = deepcopy(v)
        return rs

    def __ior__(self, other):
        """Include records in other record set in self record set."""
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'ior' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'ior' record sets for different tables")
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
                self[segment] = v | other[segment]
        for segment, v in other._rs_segments.items():
            if segment not in self:
                self[segment] = deepcopy(v)
        return self

    def __and__(self, other):
        """Return record set of records in both self and other record sets."""
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'and' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'and' record sets for different tables")
        rs = _Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
                rs[segment] = v & other[segment]
                if rs[segment].count_records() == 0:
                    del rs[segment]
        return rs

    def __iand__(self, other):
        """Remove records from self which are not in other."""
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'iand' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'iand' record sets for different tables")
        drs = []
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
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
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'xor' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'xor' record sets for different tables")
        rs = _Recordset(self._dbhome, self._dbset)
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
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
        """Include records in other record set in self record sets."""
        if self._database is not other._database:
            raise RecordsetError(
                "Attempt to 'ixor' record sets for different databases")
        if self._dbset != other._dbset:
            raise RecordsetError(
                "Attempt to 'ixor' record sets for different tables")
        drs = []
        for segment, v in self._rs_segments.items():
            if segment in other:
                # Maybe both being RecordsetSegmentInt should be special case
                self[segment] = v ^ other[segment]
                if self[segment].count_records() == 0:
                    drs.append(segment)
        for segment, v in other._rs_segments.items():
            if segment not in self:
                self[segment] = deepcopy(v)
        for segment in drs:
            del self[segment]
        return self

    def normalize(self, use_upper_limit=True):
        """Convert record set segments to version for record count.

        limit is relevant to lists and bitarrays of record numbers.

        """
        for s in self._sorted_segnums:
            self._rs_segments[s] = self._rs_segments[s].normalize(
                use_upper_limit=use_upper_limit)

    def is_record_number_in_record_set(self, record_number):
        """Return True if record number is in self, a record set, else False."""
        segment, record_number = divmod(record_number,
                                        SegmentSize.db_segment_size)
        return (False if segment not in self else
                record_number in self._rs_segments[segment])

    def __deepcopy__(self, memo):
        """Return a customized copy of self."""
        sc = _empty__recordset()
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
        #sc._clientcursors = dict()
        # the copy forgets the current recordset cache
        sc.record_cache = dict()
        sc.record_deque = deque(maxlen=self.record_deque.maxlen)
        # register the copy with the database
        #if sc._dbhome is not None:
        #    sc._dbhome.get_database_instance(
        #        sc._dbset, sc._dbset)._recordsets[sc] = True
        return sc

    def place_record_number(self, record_number):
        """Set the bit representing record_number."""
        segment, offset = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in self._rs_segments:
            self[segment] = RecordsetSegmentBitarray(segment, None)
        elif not isinstance(self[segment], RecordsetSegmentBitarray):
            self[segment] = self[segment].promote()
        self[segment][(segment, offset)] = True

    def remove_record_number(self, record_number):
        """Unset the bit representing record_number."""
        segment, offset = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in self._rs_segments:
            return
        elif not isinstance(self[segment], RecordsetSegmentBitarray):
            self[segment] = self[segment].promote()
        self[segment][(segment, offset)] = False
        #self[segment] = self[segment].normalize()

    def create_recordset_cursor(self):
        """Create and return a cursor for this recordset."""
        return self._dbhome.create_recordset_cursor(self)


class RecordsetCursor(cursor.Cursor):
    
    """Provide a bsddb3 style cursor for a recordset of arbitrary records.

    The cursor does not support partial keys because the records in the
    recordset do not have an implied order (apart from the accidential order
    of existence on the database).

    """

    @property
    def recordset(self):
        """"""
        return self._dbset

    def close(self):
        """Delete record set cursor."""
        #try:
        #    del self._dbset._clientcursors[self]
        #except:
        #    pass
        #self._dbset = None
        super().close()

    def count_records(self):
        """return record count or None."""
        try:
            return self._dbset.count_records()
        except TypeError:
            return None
        except AttributeError:
            return None

    def database_cursor_exists(self):
        """Return True if self.records is not None and False otherwise.

        Simulates existence test for a database cursor.

        """
        # The cursor methods are defined in this class and operate on
        # self.records if it is a list so do that test here as well.
        return self._dbset is not None

    def first(self):
        """Return first record."""
        if len(self._dbset):
            try:
                #return self._dbset.get_record(self._dbset.first()[1])
                return self._get_record(self._dbset.first()[1])
            except TypeError:
                return None
            except:
                raise

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)."""
        try:
            return self._dbset.get_position_of_record_number(record[0])
        except ValueError:
            return 0
        except TypeError:
            return 0

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None."""
        try:
            return self._get_record(
                self._dbset.get_record_number_at_position(position))
        except IndexError:
            return None
        except TypeError:
            if position is None:
                return None
            raise

    def last(self):
        """Return last record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.last()[1])
            except TypeError:
                return None
            except:
                raise
        
    def nearest(self, key):
        """Return nearest record. An absent record has no nearest record.

        Perhaps get_record_at_position() is the method to use.
        
        The recordset is created with arbitrary criteria.  The selected records
        are displayed in record number order for consistency.  Assumption is
        that all records on the recordset are equally near the requested record
        if it is not in the recordset itself, so whatever is already displayed
        is as near as any other records that might be chosen.

        """
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.setat(key)[1])
            except TypeError:
                return None
            except:
                raise

    def next(self):
        """Return next record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.next()[1])
            except TypeError:
                return None
            except:
                raise

    def prev(self):
        """Return previous record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.prev()[1])
            except TypeError:
                return None
            except:
                raise

    def setat(self, record):
        """Return record after positioning cursor at record."""
        if len(self._dbset):
            try:
                return self._get_record(
                    self._dbset.setat(record[0])[1])
            except TypeError:
                return None
            except:
                raise

    def _get_record(self, record_number, use_cache=False):
        """Raise exception.  Must be implemented in a subclass."""
        raise RecordsetError('_get_record must be implemented in a subclass')

    # Should this method be in solentware_misc datagrid module, or perhaps in
    # .record module?
    # Is referesh_recordset an appropriate name?
    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        if instance is None:
            return
        if self.recordset.is_record_number_in_record_set(instance.key.recno):
            if instance.newrecord is not None:
                raise RecordsetError('refresh_recordset not implemented')
            self.recordset.remove_record_number(instance.key.recno)


# __init__ may follow _DPTRecordSet example eventually.
class _RecordSetBase:
    
    """Wrapper for _Recordset compatible with _dpt._DPTRecordSet.

    _Recordset is roughly equivalent to dptapi.APIRecordList and RecordList is
    roughly equivalent to _dpt._DPTRecordList.

    This class can always just ask the wrapped _Recordset instance to do any
    action, but _DPTRecordList has to implement __and__, __xor__, __or__,
    __iand__, __ixor__, __ior__, and __del__, for itself.

    """
    # The RecordsetCursor methods may go directly to the _Recordset methods.

    def __init__(self, dbhome, dbset, cache_size=1):
        """Create a _Recordset instance."""
        self._recordset = _Recordset(dbhome, dbset, cache_size=cache_size)

    # Added for compatibility with _DPTRecordList class in _dpt module where
    # explicit destruction of underlying APIRecordList instance is mandatory.
    # At time of writing an explicit __del__ does not seem necessary.
    def __del__(self):
        """Destroy _Recordset instance if not done by explicit close()."""

        # hasattr test so calls like _RecordSetBase() raise argument exceptions
        # rather than 'attribute _recordset does not exist' exception.
        if hasattr(self, '_recordset') and self._recordset:
            self.close()

    def __setitem__(self, key, value):
        self._recordset[key] = value

    def __getitem__(self, key):
        return self._recordset[key]

    def __delitem__(self, segment):
        del self._recordset[segment]

    def __contains__(self, segment):
        return segment in self._recordset

    def __len__(self):
        return len(self._recordset)

    @property
    def dbhome(self):
        return self._recordset.dbhome
    
    @property
    def dbset(self):
        return self._recordset.dbset
    
    @property
    def dbidentity(self):
        return self._recordset.dbidentity
    
    @property
    def rs_segments(self):
        return self._recordset.rs_segments
    
    @property
    def sorted_segnums(self):
        return self._recordset.sorted_segnums

    def count_records(self):
        return self._recordset.count_records()

    # _recordset set to None for compatibility with _DPTRecordList class in
    # _dpt module.
    def close(self):
        self._recordset.close()
        self._recordset = None

    def get_position_of_record_number(self, recnum):
        return self._recordset.get_position_of_record_number(recnum)

    def get_record_number_at_position(self, position):
        return self._recordset.get_record_number_at_position(position)

    def insort_left_nodup(self, segment):
        self._recordset.insort_left_nodup(segment)

    def first(self):
        return self._recordset.first()

    def last(self):
        return self._recordset.last()

    def next(self):
        return self._recordset.next()

    def prev(self):
        return self._recordset.prev()

    def current(self):
        return self._recordset.current()

    def setat(self, record):
        return self._recordset.setat(record)
    
    def __or__(self, other):
        """Return new record set of self records with other records included."""
        rs = self._recordset | other._recordset
        r = _empty_recordlist()
        r._recordset = rs
        return r

    def __and__(self, other):
        """Return record set of records in both self and other record sets."""
        rs = self._recordset & other._recordset
        r = _empty_recordlist()
        r._recordset = rs
        return r

    def __xor__(self, other):
        """Return record set of self records with other records included."""
        rs = self._recordset ^ other._recordset
        r = _empty_recordlist()
        r._recordset = rs
        return r

    def normalize(self, use_upper_limit=True):
        self._recordset.normalize(use_upper_limit=use_upper_limit)

    def is_record_number_in_record_set(self, record_number):
        return self._recordset.is_record_number_in_record_set(record_number)

    def create_recordset_cursor(self):
        return self._recordset.create_recordset_cursor()


# To be renamed RecordList.
# __init__ may follow _DPTRecordList example eventually.
class RecordList(_RecordSetBase):
    
    """Wrapper for _Recordset compatible with _dpt._DPTRecordList.

    _Recordset is roughly equivalent to dptapi.APIRecordList and RecordList is
    roughly equivalent to _dpt._DPTRecordList.

    This class can always just ask the wrapped _Recordset instance to do any
    action, but _DPTRecordList has to implement __and__, __xor__, __or__,
    __iand__, __ixor__, __ior__, and __del__, for itself.

    """

    def __ior__(self, other):
        """Include records in other record set in self record set."""
        self._recordset |= other._recordset
        return self

    def __iand__(self, other):
        """Remove records from self which are not in other."""
        self._recordset &= other._recordset
        return self

    def __ixor__(self, other):
        """Include records in other record set in self record sets."""
        self._recordset ^= other._recordset
        return self

    def clear_recordset(self):
        self._recordset.clear_recordset()

    def place_record_number(self, record_number):
        """Place record record_number on self, a RecordList."""
        self._recordset.place_record_number(record_number)

    def remove_record_number(self, record_number):
        """Remove record record_number on self, a RecordList."""
        self._recordset.remove_record_number(record_number)

    def remove_recordset(self, recordset):
        """Remove other's records from recordset using '|=' and '^=' operators.

        Equivalent to the Romove() method of DPT recordsets.
        """
        self._recordset |= recordset._recordset
        self._recordset ^= recordset._recordset

    def replace_records(self, newrecords):
        """Replace records in recordset with newrecords.

        This method exists for compatibility with DPT where simply binding an
        attribute to newrecords may not be correct.

        """
        self.clear_recordset()
        self._recordset |= newrecords._recordset


# Following the class hierarchy in _dpt module.
# In particular not RecordList(FoundSet) because dptapi.APIFoundSet supports
# record locks but dptapi.RecordList does not.
# __init__ may follow _DPTFoundSet example eventually.
class FoundSet(_RecordSetBase):
    
    """Wrapper for _Recordset compatible with _dpt._DPTFoundSet.

    _Recordset is roughly equivalent to dptapi.APIRecordList and RecordList is
    roughly equivalent to _dpt._DPTRecordList.

    This class can always just ask the wrapped _Recordset instance to do any
    action, but _DPTRecordList has to implement __and__, __xor__, __or__,
    __iand__, __ixor__, __ior__, and __del__, for itself.

    """


# This is for actual recordset.
def _empty__recordset():
    """Create and return an empty instance of _Recordset."""
    class E(_Recordset):
        def __init__(self):
            pass
    e = E()
    e.__class__ = _Recordset
    return e


# This is for wrapper.
def _empty_recordlist():
    """Create and return an empty instance of RecordList."""
    class E(RecordList):
        def __init__(self):
            pass
    e = E()
    e.__class__ = RecordList
    return e
