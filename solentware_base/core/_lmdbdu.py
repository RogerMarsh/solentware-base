# lmdbdu.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Deferred update access to Symas Lightning Memory-Mapped Database (LMMD)."""
from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    SEGMENT_HEADER_LENGTH,
    EXISTING_SEGMENT_REFERENCE,
    NEW_SEGMENT_CONTENT,
)
from .segmentsize import SegmentSize
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
)
from . import _databasedu


class DatabaseError(_databasedu.DatabaseduError):
    """Exception for Database class."""


class Database(_databasedu.Database):
    """Customise _db.Database for deferred update.

    The class which chooses the interface to Berkeley DB must include this
    class earlier in the Method Resolution Order than _db.Database.

    Normally deferred updates are synchronised with adding the last record
    number to a segment.  Sometimes memory constraints will force deferred
    updates to be done more frequently, but this will likely increase the time
    taken to do the deferred updates for the second and later points in a
    segment.
    """

    def __init__(self, *a, **kw):
        """Extend and initialize deferred update data structures."""
        super().__init__(*a, **kw)
        self.deferred_update_points = None
        self.first_chunk = {}
        self.high_segment = {}
        self.initial_high_segment = {}
        self.existence_bit_maps = {}
        self.value_segments = {}  # was values in secondarydu.Secondary
        self._int_to_bytes = None

    def environment_flags(self, dbe):
        """Return environment flags for deferred update."""
        return super().environment_flags(dbe)

    def database_cursor(self, file, field, keyrange=None, recordset=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def deferred_update_housekeeping(self):
        """Override and continue to do nothing.

        In Symas LMDB the size of the memory map should be adjusted to cope
        with expected size of the update.  Symas LMDB suggests just setting
        this to maximum; but here applications would set this to a good
        estimate of the required size plus some space too spare.

        Applications should override and set their own estimate.

        """

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            with self.dbtxn.transaction.cursor(
                db=self.table[file].datastore
            ) as dbc:
                if not dbc.last():
                    continue
                segment, record_number = divmod(
                    int.from_bytes(dbc.item()[0], byteorder="big"),
                    SegmentSize.db_segment_size,
                )
                if record_number in self.deferred_update_points:
                    continue  # Assume put_instance did deferred updates
            self._write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self.set_int_to_bytes_lookup(lookup=True)
        self.start_transaction()
        for file in self.specification:
            high_record = None
            with self.dbtxn.transaction.cursor(
                db=self.table[file].datastore
            ) as dbc:
                if dbc.last():
                    high_record = dbc.item()
            if high_record is None:
                self.initial_high_segment[file] = None
                self.high_segment[file] = None
                self.first_chunk[file] = None
                continue
            segment, record = divmod(
                int.from_bytes(high_record[0], byteorder="big"),
                SegmentSize.db_segment_size,
            )
            self.initial_high_segment[file] = segment
            self.high_segment[file] = segment
            self.first_chunk[file] = record < min(self.deferred_update_points)

    def unset_defer_update(self):
        """Unset deferred update for db DBs. Default all."""
        self.set_int_to_bytes_lookup(lookup=False)
        for file in self.specification:
            self.high_segment[file] = None
            self.first_chunk[file] = None
        self.commit()

    def _write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment."""
        self.dbtxn.transaction.put(
            segment.to_bytes(4, byteorder="big"),
            self.existence_bit_maps[file][segment].tobytes(),
            db=self.ebm_control[file].ebm_table.datastore,
        )

    def _sort_and_write_high_or_chunk(
        self, file, field, segment, cursor_new, segvalues
    ):
        # Note cursor_high binds to database (table_connection_list[0]) only if
        # it is the only table.
        # if self.specification[file][FIELDS].get(ACCESS_METHOD) == HASH:
        #    segkeys = tuple(segvalues)
        # else:
        #    segkeys = sorted(segvalues)
        # Follow example set it merge().
        # To verify path coverage uncomment the '_path_marker' code.
        # self._path_marker = set()
        segkeys = sorted(segvalues)
        with self.dbtxn.transaction.cursor(
            db=self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor_high:
            for skey in segkeys:
                k = skey.encode()

                # Get high existing segment for value.
                if not cursor_high.set_key(k):
                    # No segments for this index value.
                    # self._path_marker.add('p1')
                    continue

                if not cursor_high.next_nodup():
                    cursor_high.last()
                    segref = cursor_high.item()[1]
                    # self._path_marker.add('p2a')
                else:
                    # self._path_marker.add('p2b')
                    cursor_high.prev()
                    segref = cursor_high.item()[1]
                if segment != int.from_bytes(segref[:4], byteorder="big"):
                    # No records exist in high segment for this index
                    # value.
                    # self._path_marker.add('p3')
                    continue

                current_segment = self.populate_segment(segref, file)
                seg = (
                    self.make_segment(k, segment, *segvalues[skey])
                    | current_segment
                ).normalize()

                # Avoid 'RecordsetSegment<*>.count_records()' methods becasue
                # the Bitarray version is too slow, and the counts are derived
                # from sources available here.
                # Safe to add the counts because the new segment will not use
                # record numbers already present on current segment.
                if isinstance(current_segment, RecordsetSegmentInt):
                    # self._path_marker.add('p4a')
                    current_count = 1
                else:
                    # self._path_marker.add('p4b')
                    current_count = int.from_bytes(
                        segref[4:SEGMENT_HEADER_LENGTH], "big"
                    )
                new_count = segvalues[skey][0] + current_count

                if isinstance(seg, RecordsetSegmentBitarray):
                    # self._path_marker.add('p5a')
                    if isinstance(current_segment, RecordsetSegmentList):
                        # self._path_marker.add('p5a-a')
                        self.dbtxn.transaction.put(
                            segref[-4:],
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        cursor_high.delete()
                        cursor_high.put(
                            k,
                            b"".join(
                                (
                                    segref[:4],
                                    new_count.to_bytes(2, byteorder="big"),
                                    segref[-4:],
                                )
                            ),
                        )
                    elif isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5a-b')
                        with self.dbtxn.transaction.cursor(
                            self.segment_table[file].datastore
                        ) as cursor:
                            if cursor.last():
                                srn = (
                                    int.from_bytes(
                                        cursor.key(),
                                        byteorder="big",
                                    )
                                    + 1
                                )
                            else:
                                srn = 0
                        self.dbtxn.transaction.put(
                            srn.to_bytes(4, byteorder="big"),
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        # Why not use cursor_high throughout this method?
                        cursor_high.delete()
                        cursor_new.put(
                            k,
                            b"".join(
                                (
                                    segref[:4],
                                    new_count.to_bytes(2, byteorder="big"),
                                    srn.to_bytes(4, byteorder="big"),
                                )
                            ),
                        )
                    else:
                        # self._path_marker.add('p5a-c')
                        self.dbtxn.transaction.put(
                            segref[-4:],
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        cursor_high.delete()
                        cursor_high.put(
                            k,
                            b"".join(
                                (
                                    segref[:4],
                                    new_count.to_bytes(2, byteorder="big"),
                                    segref[-4:],
                                )
                            ),
                        )
                elif isinstance(seg, RecordsetSegmentList):
                    # self._path_marker.add('p5b')
                    if isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5b-a')
                        with self.dbtxn.transaction.cursor(
                            self.segment_table[file].datastore
                        ) as cursor:
                            if cursor.last():
                                srn = (
                                    int.from_bytes(
                                        cursor.key(),
                                        byteorder="big",
                                    )
                                    + 1
                                )
                            else:
                                srn = 0
                        self.dbtxn.transaction.put(
                            srn.to_bytes(4, byteorder="big"),
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        # Why not use cursor_high throughout this method?
                        cursor_high.delete()
                        cursor_new.put(
                            k,
                            b"".join(
                                (
                                    segref[:4],
                                    new_count.to_bytes(2, byteorder="big"),
                                    srn.to_bytes(4, byteorder="big"),
                                )
                            ),
                        )
                    else:
                        self.dbtxn.transaction.put(
                            segref[-4:],
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        cursor_high.delete()
                        cursor_high.put(
                            k,
                            b"".join(
                                (
                                    segref[:4],
                                    new_count.to_bytes(2, byteorder="big"),
                                    segref[-4:],
                                )
                            ),
                        )
                else:
                    # self._path_marker.add('p5c')
                    raise DatabaseError("Unexpected segment type")

                # Delete segment so it is not processed again as a new
                # segment.
                del segvalues[skey]

        del cursor_high
        del segkeys

    def sort_and_write(self, file, field, segment):
        """Sort the segment deferred updates before writing to database."""
        # Anything to do?
        if field not in self.value_segments[file]:
            return

        # Prepare to wrap the record numbers in an appropriate Segment class.
        self._prepare_segment_record_list(file, field)
        segvalues = self.value_segments[file][field]

        # New records go into temporary databases, one for each segment, except
        # when filling the segment which was high when this update started.
        if (
            self.first_chunk[file]
            and self.initial_high_segment[file] != segment
        ):
            self.new_deferred_root(file, field)

        # The low segment in the import may have to be merged with an existing
        # high segment on the database, or the current segment in the import
        # may be done in chunks of less than a complete segment.  (The code
        # which handles this is in self._sort_and_write_high_or_chunk because
        # the indentation seems too far right for easy reading: there is an
        # extra 'try ... finally ...' compared with the _sqlitedu module which
        # makes the difference.)
        with self.dbtxn.transaction.cursor(
            db=self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor_new:
            if (
                self.high_segment[file] == segment
                or not self.first_chunk[file]
            ):
                self._sort_and_write_high_or_chunk(
                    file, field, segment, cursor_new, segvalues
                )

            # Add the new segments in segvalues
            segment_bytes = segment.to_bytes(4, byteorder="big")
            # Block comment retained from _dbdu module but Symas LMMD does
            # not have hash.
            # if self.specification[file][FIELDS].get(ACCESS_METHOD) == HASH:
            #    segkeys = tuple(segvalues)
            # else:
            #    segkeys = sorted(segvalues)
            segkeys = sorted(segvalues)
            for skey in segkeys:
                count, records = segvalues[skey]
                del segvalues[skey]
                k = skey.encode()
                if count > 1:
                    with self.dbtxn.transaction.cursor(
                        db=self.segment_table[file].datastore
                    ) as cursor:
                        if cursor.last():
                            srn = (
                                int.from_bytes(cursor.key(), byteorder="big")
                                + 1
                            )
                        else:
                            srn = 0
                        cursor.put(
                            srn.to_bytes(4, byteorder="big"),
                            records,
                            overwrite=False,
                        )
                    cursor_new.put(
                        k,
                        b"".join(
                            (
                                segment_bytes,
                                count.to_bytes(2, byteorder="big"),
                                srn.to_bytes(4, byteorder="big"),
                            )
                        ),
                    )
                else:
                    cursor_new.put(
                        k,
                        b"".join(
                            (
                                segment_bytes,
                                records.to_bytes(2, byteorder="big"),
                            )
                        ),
                    )

        # Flush buffers to avoid 'missing record' exception in populate_segment
        # calls in later multi-chunk updates on same segment.  Not known to be
        # needed generally yet.
        # self.segment_table[file].sync()

    def new_deferred_root(self, file, field):
        """Do nothing: at least at first.

        See merge() method docstring for environment issues.

        Deferred update always uses the '-1' database so the main database is
        accessed automatically since it is the '0' database.
        """
        # Assuming the staging area technique is needed for faster deferred
        # updates, the temporary sorted file technique seems forced (very old
        # versions of <>du modules have such).

    def merge(self, file, field):
        """Do nothing: at least at first.

        Temporary databases in the main environment seems wrong because:
            the number of temporary databases is not known at the start so
            the max_dbs argument cannot be provided,
            the transaction may be huge leaving a lot of space wasted or to
            be recovered before returning the database to normal use.

        Temporary environments may be possible but there will be lots of
        them, and an unlimited number would need to be open simultaneously
        when merging.
        """

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        # record keys are 0-based converted to bytes.
        # segment_numbers are 0-based.
        return self.dbtxn.transaction.get(
            key.to_bytes(4, byteorder="big"),
            db=ebm_control.ebm_table.datastore,
        )

    def find_value_segments(self, field, file):
        """Yield segment references for field in file."""
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            record = cursor.first()
            while record:
                value, segment = cursor.item()
                if len(segment) == 6:
                    count = b"\x00\x01"
                    reference = segment[4:]
                else:
                    count = segment[4:6]
                    reference = segment[6:]
                yield [
                    value,
                    segment[:4],
                    EXISTING_SEGMENT_REFERENCE,
                    count,
                    reference,
                ]
                record = cursor.next()

    def delete_index(self, file, field):
        """Remove all records from database for field in file."""
        self.dbtxn.transaction.drop(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore,
            delete=False,
        )

    def merge_writer(self, file, field):
        """Return a Writer instance for the field index on table file.

        Call the write() method with object returned by next_sorted_item
        method.

        """

        def make_segment_from_high(high, transaction):
            if high[2] == b"\x00\x01":
                return RecordsetSegmentInt(
                    int.from_bytes(high[1], byteorder="big"),
                    None,
                    records=high[3],
                )
            bytestring_records = transaction.get(high[3], db=datastore)
            if len(bytestring_records) == SegmentSize.db_segment_size_bytes:
                return RecordsetSegmentBitarray(
                    int.from_bytes(high[1], byteorder="big"),
                    None,
                    records=bytestring_records,
                )
            return RecordsetSegmentList(
                int.from_bytes(high[1], byteorder="big"),
                None,
                records=bytestring_records,
            )

        def make_segment_from_item(item):
            if item[2] == b"\x00\x01":
                return RecordsetSegmentInt(
                    int.from_bytes(item[1], byteorder="big"),
                    None,
                    records=item[3],
                )
            if len(item[3]) == SegmentSize.db_segment_size_bytes:
                return RecordsetSegmentBitarray(
                    int.from_bytes(item[1], byteorder="big"),
                    None,
                    records=item[3],
                )
            return RecordsetSegmentList(
                int.from_bytes(item[1], byteorder="big"),
                None,
                records=item[3],
            )

        def read_high_item_in_index(cursor):
            if cursor.last():
                record = cursor.item()
            else:
                record = None
            assert len(record) == 2
            value, segment = record
            if len(segment) == 6:
                count = b"\x00\x01"
                reference = segment[4:]
            else:
                count = segment[4:6]
                reference = segment[6:]
            return [
                value,
                segment[:4],
                count,
                reference,
            ]

        def write_segment_value(segment_value, segment_cursor):
            if segment_cursor.last():
                srn = int.from_bytes(segment_cursor.key(), byteorder="big") + 1
            else:
                srn = 0
            srn = srn.to_bytes(4, byteorder="big")
            segment_cursor.put(srn, segment_value, overwrite=False)
            return srn

        table = self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        datastore = self.segment_table[file].datastore

        class Writer:
            """Write index entries to database."""

            def __init__(self, database):
                self.prev_segment = None
                self.prev_key = None
                self.database = database
                self.cursor = self.database.dbtxn.transaction.cursor(table)
                self.segment_cursor = self.database.dbtxn.transaction.cursor(
                    db=datastore
                )

            def make_new_cursor(self):
                """Create cursors on the assumed new transaction."""
                self.cursor = self.database.dbtxn.transaction.cursor(table)
                self.segment_cursor = self.database.dbtxn.transaction.cursor(
                    db=datastore
                )

            def close_cursor(self):
                """Close the cursors open on the index."""
                self.cursor.close()
                self.segment_cursor.close()

            def write(self, item):
                """Write item to index on database."""
                assert len(item) == 5
                segment = item[1]
                if self.prev_segment != segment:
                    self.prev_segment = segment
                    self.prev_key = item[0]
                    item_type = item.pop(2)
                    if item_type == EXISTING_SEGMENT_REFERENCE:
                        assert len(item) == 4
                        if item[-2] == b"\x00\x01":
                            item.pop(-2)
                        self.cursor.put(item[0], b"".join(item[1:]))
                        length = len(b"".join(item[1:]))
                        assert length == 10 or length == 6
                        return
                    if int.from_bytes(item[-2], byteorder="big") > 1:
                        item[-1] = write_segment_value(
                            item[-1], self.segment_cursor
                        )
                        assert len(item) == 4
                        assert len(b"".join(item[1:])) == 10
                    else:
                        item.pop(2)
                        assert len(item) == 3
                        assert len(b"".join(item[1:])) == 6
                    self.cursor.put(item[0], b"".join(item[1:]))
                    assert item_type == NEW_SEGMENT_CONTENT
                    return
                if self.prev_key == item[0]:
                    assert item[2] == NEW_SEGMENT_CONTENT
                    del item[2]
                    high = read_high_item_in_index(self.cursor)
                    new_segment = make_segment_from_item(item)
                    new_segment |= make_segment_from_high(
                        high, self.database.dbtxn.transaction
                    )
                    new_segment.normalize()
                    item[-2] = (
                        self.database.encode_number_for_sequential_file_dump(
                            new_segment.count_records(), 2
                        )
                    )
                    if high[2] == b"\x00\x01":
                        item[-1] = write_segment_value(
                            new_segment.tobytes(), self.segment_cursor
                        )
                        self.cursor.delete()
                        assert len(b"".join(item[1:])) == 10
                        self.cursor.put(item[0], b"".join(item[1:]))
                    else:
                        self.database.dbtxn.transaction.put(
                            high[-1],
                            new_segment.tobytes(),
                            db=datastore,
                        )
                        self.cursor.delete()
                        item[-1] = high[-1]
                        assert len(b"".join(item[1:])) == 10
                        self.cursor.put(item[0], b"".join(item[1:]))
                    assert len(item) == 4
                    return
                item_type = item.pop(2)
                if item_type == EXISTING_SEGMENT_REFERENCE:
                    assert len(item) == 4
                    self.prev_key = item[0]
                    if item[-2] == b"\x00\x01":
                        item.pop(-2)
                    self.cursor.put(item[0], b"".join(item[1:]))
                    length = len(b"".join(item[1:]))
                    assert length == 10 or length == 6
                    return
                if int.from_bytes(item[-2], byteorder="big") > 1:
                    item[-1] = write_segment_value(
                        item[-1], self.segment_cursor
                    )
                    assert len(item) == 4
                    assert len(b"".join(item[1:])) == 10
                else:
                    item.pop(2)
                    assert len(item) == 3
                    assert len(b"".join(item[1:])) == 6
                self.cursor.put(item[0], b"".join(item[1:]))
                assert item_type == NEW_SEGMENT_CONTENT
                return

        return Writer(self)
