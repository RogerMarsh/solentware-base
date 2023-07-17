# _dbdu.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database with the berkeleydb or bsddb3 modules."""
from .constants import (
    SECONDARY,
    # ACCESS_METHOD,
    # HASH,
    SUBFILE_DELIMITER,
    SEGMENT_HEADER_LENGTH,
)
from .segmentsize import SegmentSize
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
)
from . import _databasedu


class DatabaseError(Exception):
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

    # Always checkpoint after commit in deferred update.
    _MINIMUM_CHECKPOINT_INTERVAL = 0

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

    def database_cursor(self, file, field, keyrange=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def deferred_update_housekeeping(self):
        """Override to commit transaction for segment and clear log files.

        Deferred update within transactions is not practical in Berkeley DB
        unless the log files are pruned frequently.

        Applications should extend this method as required: perhaps to
        record progress at commit time to assist restart.

        """
        self.commit()
        self.start_transaction()

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            dbc = self.table[file][0].cursor(txn=self.dbtxn)
            try:
                segment, record_number = divmod(
                    dbc.last()[0], SegmentSize.db_segment_size
                )
                if record_number in self.deferred_update_points:
                    continue  # Assume put_instance did deferred updates
            except TypeError:
                continue
            finally:
                dbc.close()
            self.write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.start_transaction()
        for file in self.specification:
            dbc = self.table[file][0].cursor(txn=self.dbtxn)
            try:
                high_record = dbc.last()
            finally:
                dbc.close()
            if high_record is None:
                self.initial_high_segment[file] = None
                self.high_segment[file] = None
                self.first_chunk[file] = None
                continue
            segment, record = divmod(
                high_record[0], SegmentSize.db_segment_size
            )
            self.initial_high_segment[file] = segment
            self.high_segment[file] = segment
            self.first_chunk[file] = record < min(self.deferred_update_points)

    def unset_defer_update(self):
        """Unset deferred update for db DBs. Default all."""
        self._int_to_bytes = None
        for file in self.specification:
            self.high_segment[file] = None
            self.first_chunk[file] = None
        self.commit()

    def write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment."""
        self.ebm_control[file].ebm_table.put(
            segment + 1,
            self.existence_bit_maps[file][segment].tobytes(),
            txn=self.dbtxn,
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
        cursor_high = self.table[SUBFILE_DELIMITER.join((file, field))][
            -1
        ].cursor(txn=self.dbtxn)
        try:
            for skey in segkeys:
                k = skey.encode()

                # Get high existing segment for value.
                if not cursor_high.set(k):

                    # No segments for this index value.
                    # self._path_marker.add('p1')
                    continue

                if not cursor_high.next_nodup():
                    segref = cursor_high.last()[1]
                    # self._path_marker.add('p2a')
                else:
                    # self._path_marker.add('p2b')
                    segref = cursor_high.prev()[1]
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
                        self.segment_table[file].put(
                            int.from_bytes(segref[-4:], "big"),
                            seg.tobytes(),
                            txn=self.dbtxn,
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
                            self._dbe.DB_KEYLAST,
                        )
                    elif isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5a-b')
                        srn = self.segment_table[file].append(
                            seg.tobytes(), txn=self.dbtxn
                        )
                        # Why not use cursor_high throughout this method?
                        # Then why not use DB_CURRENT and remove the delete()?
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
                            self._dbe.DB_KEYLAST,
                        )
                    else:
                        # self._path_marker.add('p5a-c')
                        self.segment_table[file].put(
                            int.from_bytes(segref[-4:], "big"),
                            seg.tobytes(),
                            txn=self.dbtxn,
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
                            self._dbe.DB_KEYLAST,
                        )
                elif isinstance(seg, RecordsetSegmentList):
                    # self._path_marker.add('p5b')
                    if isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5b-a')
                        srn = self.segment_table[file].append(
                            seg.tobytes(), txn=self.dbtxn
                        )
                        # Why not use cursor_high throughout this method?
                        # Then why not use DB_CURRENT and remove the delete()?
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
                            self._dbe.DB_KEYLAST,
                        )
                    else:
                        # self._path_marker.add('p5b-b')
                        self.segment_table[file].put(
                            int.from_bytes(segref[-4:], "big"),
                            seg.tobytes(),
                            txn=self.dbtxn,
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
                            self._dbe.DB_KEYLAST,
                        )
                else:
                    # self._path_marker.add('p5c')
                    raise DatabaseError("Unexpected segment type")

                # Delete segment so it is not processed again as a new
                # segment.
                del segvalues[skey]

        finally:
            # self._path_marker.add('p6')
            cursor_high.close()
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
        # Below now obsolete.
        # Note the substantive difference between this module and _sqlitedu:
        # the code for Berkeley DB updates the main index directly if an entry
        # already exists, but the code for SQLite always updates a temporary
        # table and merges into the main table later.
        cursor_new = self.table[SUBFILE_DELIMITER.join((file, field))][
            -1
        ].cursor(txn=self.dbtxn)
        try:
            if (
                self.high_segment[file] == segment
                or not self.first_chunk[file]
            ):
                self._sort_and_write_high_or_chunk(
                    file, field, segment, cursor_new, segvalues
                )

            # Add the new segments in segvalues
            segment_bytes = segment.to_bytes(4, byteorder="big")
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
                    srn = self.segment_table[file].append(
                        records, txn=self.dbtxn
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
                        self._dbe.DB_KEYLAST,
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
                        self._dbe.DB_KEYLAST,
                    )

        finally:
            cursor_new.close()
            # self.table_connection_list[-1].close() # multi-chunk segments

        # Flush buffers to avoid 'missing record' exception in populate_segment
        # calls in later multi-chunk updates on same segment.  Not known to be
        # needed generally yet.
        self.segment_table[file].sync()

    def new_deferred_root(self, file, field):
        """Do nothing.

        Populating main database is slower than using a sequence of small
        staging areas, but makes transaction commits in applications at
        convenient intervals awkward.

        Deferred update always uses the '-1' database so the main database is
        accessed automatically since it is the '0' database.
        """

    def merge(self, file, field):
        """Do nothing: there is nothing to do in _dbdu module."""

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        # record keys are 1-based but segment_numbers are 0-based.
        return ebm_control.ebm_table.get(key + 1, txn=self.dbtxn)
