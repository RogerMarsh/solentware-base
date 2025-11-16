# _databasedu.py
# Copyright 2008, 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the database interface for deferred updates.

The major component is the put_instance method.  The DPT database way of doing
deferred updates is so different there is no equivalent separate module for
DPT.

"""
from . import _database
from .segmentsize import SegmentSize
from .constants import SECONDARY
from .bytebit import Bitarray
from . import merge


class DatabaseduError(_database.DatabaseError):
    """Exception for Database class."""


class Database(_database.Database):
    """Provide deferred update versions of the record update methods."""

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.

        This method assumes all primary databases are integer primary key,
        and there is enough memory to do a segment at a time.

        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        if putkey is not None:
            # reuse record number is not allowed
            raise DatabaseduError(
                "Cannot reuse record number in deferred update."
            )
        key = self.put(dbset, putkey, instance.srvalue)

        # put was append to record number database and
        # returned the new primary key. Adjust record key
        # for secondary updates.
        instance.key.load(key)
        putkey = key

        instance.srkey = self.encode_record_number(putkey)
        srindex = instance.srindex
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        self._defer_add_record_to_ebm(dbset, segment, record_number)
        pcb = instance.putcallbacks
        for secondary in srindex:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for j in srindex[secondary]:
                self._defer_add_record_to_field_value(
                    dbset, secondary, j, segment, record_number
                )

        if record_number in self.deferred_update_points:
            self._write_existence_bit_map(dbset, segment)
            for secondary in self.specification[dbset][SECONDARY]:
                self.sort_and_write(dbset, secondary, segment)
            if record_number == max(self.deferred_update_points):
                self.first_chunk[dbset] = True
            elif record_number == min(self.deferred_update_points):
                self.first_chunk[dbset] = False
                self.high_segment[dbset] = segment

    def index_instance(self, dbset, instance):
        """Apply instance index values on database dbset.

        This method assumes all primary databases are integer primary key,
        and there is enough memory to do a segment at a time.

        """
        putkey = instance.key.pack()
        if putkey is None:
            # Apply index values without record number is not allowed.
            raise DatabaseduError(
                "Cannot apply index values without a record number"
            )
        instance.set_packed_value_and_indexes()
        instance.srkey = self.encode_record_number(putkey)
        srindex = instance.srindex
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        pcb = instance.putcallbacks
        for secondary in srindex:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for j in srindex[secondary]:
                self._defer_add_record_to_field_value(
                    dbset, secondary, j, segment, record_number
                )

        if record_number in self.deferred_update_points:
            for secondary in srindex:
                assert secondary in self.specification[dbset][SECONDARY]
                self.sort_and_write(dbset, secondary, segment)
            if record_number == max(self.deferred_update_points):
                self.first_chunk[dbset] = True
            elif record_number == min(self.deferred_update_points):
                self.first_chunk[dbset] = False
                self.high_segment[dbset] = segment

    def _defer_add_record_to_ebm(self, file, segment, record_number):
        """Add bit to existence bit map for new record and defer update."""
        assert file in self.specification
        try:
            # Assume cached segment existence bit map exists
            self.existence_bit_maps[file][segment][record_number] = True
        except KeyError:
            # Get the segment existence bit map from database
            ebmb = self.get_ebm_segment(self.ebm_control[file], segment)
            if ebmb is None:
                # It does not exist so create a new empty one
                ebm = SegmentSize.empty_bitarray.copy()
            else:
                # It does exist so convert database representation to bitarray
                ebm = Bitarray()
                ebm.frombytes(ebmb)
            # Set bit for record number and add segment to cache
            ebm[record_number] = True
            if file not in self.existence_bit_maps:
                self.existence_bit_maps[file] = {}
            self.existence_bit_maps[file][segment] = ebm

    def _defer_add_record_to_field_value(
        self, file, field, key, segment, record_number
    ):
        """Add record_number to cached segment for key."""
        del segment
        assert file in self.specification
        try:
            value_segments = self.value_segments[file][field]
        except KeyError:
            value_segments = self.value_segments.setdefault(
                file, {}
            ).setdefault(field, {})
        values = value_segments.get(key)
        if values is None:
            value_segments[key] = [record_number]
        elif isinstance(values, list):
            # A (value, record_number) can be given many times.
            # Ensure a record_number appears in the list once only.
            if values[-1] != record_number:
                values.append(record_number)
                if len(values) > SegmentSize.db_upper_conversion_limit:
                    vsk = value_segments[key] = (
                        SegmentSize.empty_bitarray.copy()
                    )
                    for j in values:
                        vsk[j] = True
                    vsk[record_number] = True

        else:
            values[record_number] = True

    def _prepare_segment_record_list(self, file, field):
        """Convert dict of record number lists to database record format.

        A single record in a segment for an index value is represented
        as a number within the segment.

        Multiple records are represented as lists of record numbers or
        bitmaps depending on how many are in the segment.

        """
        # Lookup table is much quicker, and noticeable, in bulk use.
        int_to_bytes = self._int_to_bytes

        segvalues = self.value_segments[file][field]
        for k in segvalues:
            value = segvalues[k]
            if isinstance(value, list):
                # A single record is presented as an integer: the
                # database engine will decide the transformation.
                if len(value) == 1:
                    segvalues[k] = [1, value[-1]]
                else:
                    segvalues[k] = [
                        len(value),
                        b"".join([int_to_bytes[n] for n in value]),
                    ]

            else:
                segvalues[k] = [
                    value.count(),
                    value.tobytes(),
                ]

    def set_segment_size(self):
        """Extend and set a deferred update point at end of segment."""
        super().set_segment_size()

        # Override in subclasses if more frequent deferred update is required.
        self.deferred_update_points = frozenset(
            [SegmentSize.db_segment_size - 1]
        )

    def deferred_update_housekeeping(self):
        """Do nothing.  Subclasses should override this method as required.

        Actions are specific to a database engine.

        """

    def encode_number_for_sequential_file_dump(self, number, bytes_):
        """Return big-endian encoding of number in bytes_ bytes."""
        return number.to_bytes(bytes_, byteorder="big")

    def encode_segment_for_sequential_file_dump(self, record_numbers):
        """Return encoding of record numbers appropriate to record count."""
        if len(record_numbers) > SegmentSize.db_upper_conversion_limit:
            seg = SegmentSize.empty_bitarray.copy()
            for bit in record_numbers:
                seg[bit] = True
            return seg.tobytes()
        int_to_bytes = self._int_to_bytes
        return b"".join([int_to_bytes[n] for n in record_numbers])

    def set_int_to_bytes_lookup(self, lookup=True):
        """Create lookup to convert integers to bytestring big-endian.

        Set lookup table to None if bool(lookup) is False.

        """
        if lookup:
            self._int_to_bytes = [
                n.to_bytes(2, byteorder="big")
                for n in range(SegmentSize.db_segment_size)
            ]
        else:
            self._int_to_bytes = None

    def merge_import(self, index_directory, file, field, commit_limit):
        """Yield count of sorted items written to an index at intervals.

        Sorted items from files in index_directory are written to index
        field in file, and the count of items added is yielded every
        commit_limit items.

        """
        writer = self.merge_writer(file, field)
        merger = merge.Merge(index_directory)
        commit_count = None
        for commit_count, item in enumerate(merger.sorter()):
            if not commit_count % commit_limit:
                if commit_count:
                    writer.close_cursor()
                    self.commit()
                    self.deferred_update_housekeeping()
                    yield commit_count
                    self.start_transaction()
                    writer.make_new_cursor()
            writer.write(item)
        writer.close_cursor()
        if commit_count is not None:
            self.commit()
            self.deferred_update_housekeeping()
            self.start_transaction()

    def next_sorted_item(self, index_directory):
        """Yield sorted items from files in index_directory."""
        merger = merge.Merge(index_directory)
        for item in merger.sorter():
            yield item

    def get_merge_import_sort_area(self):
        """Return database directory.

        The directory containing database is used for sorting by default,
        subclasses should override as required.

        There is little point in doing so unless the sort area is then on
        a different drive or mount point.

        """
        return self.home_directory
