# lmdbdu.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Deferred update access to Symas Lightning Memory-Mapped Database (LMMD)."""
from .constants import (
    SECONDARY,
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

    def environment_flags(self, dbe):
        """Return environment flags for deferred update."""
        return super().environment_flags(dbe)

    def deferred_update_housekeeping(self):
        """Override to commit transaction for segment.

        In Symas LMMD this is not essential, but is done for compatibility
        with Berkeley DB where it is necessary to prune log files frequently.

        Applications should extend this method as required: perhaps to
        record progress at commit time to assist restart.

        """
        self.commit()
        self.start_transaction()

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            with self.dbtxn.transaction.cursor(
                db=self.table[file][0].datastore
            ) as dbc:
                if not dbc.last():
                    continue
                segment, record_number = divmod(
                    int.from_bytes(dbc.item()[0], byteorder="big"),
                    SegmentSize.db_segment_size,
                )
                if record_number in self.deferred_update_points:
                    continue  # Assume put_instance did deferred updates
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
            high_record = None
            with self.dbtxn.transaction.cursor(
                db=self.table[file][0].datastore
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
        self._int_to_bytes = None
        for file in self.specification:
            self.high_segment[file] = None
            self.first_chunk[file] = None
        self.commit()

    def write_existence_bit_map(self, file, segment):
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
            db=self.table[SUBFILE_DELIMITER.join((file, field))][-1].datastore
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
            db=self.table[SUBFILE_DELIMITER.join((file, field))][-1].datastore
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
