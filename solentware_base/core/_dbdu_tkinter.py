# _dbdu_tkinter.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database with tcl via the tkinter module."""
from ..db_tcl import tcl_tk_call
from .constants import (
    SECONDARY,
    # ACCESS_METHOD,
    # HASH,
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

    def database_cursor(self, file, field, keyrange=None, recordset=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def deferred_update_housekeeping(self):
        """Override to remove unused log files.

        Deferred update within transactions is not practical in Berkeley DB
        unless the log files are pruned frequently.

        The Tcl interface to Berkeley DB does not support the set_flags()
        or log_set_config() methods of DBEnv; and the open() method of
        DBEnv does not support theDB_LOG_AUTOREMOVE flag.

        So the DB archive utility must be run with the '-d' option.

        """
        self._run_db_archive()

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            command = [self.table[file], "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            dbc = tcl_tk_call(tuple(command))
            try:
                rec = tcl_tk_call((dbc, "get", "-last")) or None
                if rec:
                    rec = rec[0]
                segment, record_number = divmod(
                    rec[0], SegmentSize.db_segment_size
                )
                if record_number in self.deferred_update_points:
                    continue  # Assume put_instance did deferred updates
            except TypeError:
                continue
            finally:
                tcl_tk_call((dbc, "close"))
            self._write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self.set_int_to_bytes_lookup(lookup=True)
        self.start_transaction()
        for file in self.specification:
            command = [self.table[file], "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            dbc = tcl_tk_call(tuple(command))
            try:
                high_record = tcl_tk_call((dbc, "get", "-last"))
            finally:
                tcl_tk_call((dbc, "close"))
            if not high_record:
                self.initial_high_segment[file] = None
                self.high_segment[file] = None
                self.first_chunk[file] = None
                continue
            high_record = high_record[0]
            segment, record = divmod(
                high_record[0], SegmentSize.db_segment_size
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
        command = [self.ebm_control[file].ebm_table, "put"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend(
            [segment + 1, self.existence_bit_maps[file][segment].tobytes()]
        )
        tcl_tk_call(tuple(command))

    def _sort_and_write_high_or_chunk(
        self, file, field, segment, cursor_new, segvalues
    ):
        # Commented statements kept without conversion.
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
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor_high = tcl_tk_call(tuple(command))
        try:
            for skey in segkeys:
                k = skey.encode()

                # Get high existing segment for value.
                if not tcl_tk_call((cursor_high, "get", "-set", k)):
                    # No segments for this index value.
                    # self._path_marker.add('p1')
                    continue

                if not tcl_tk_call((cursor_high, "get", "-nextnodup")):
                    segref = tcl_tk_call((cursor_high, "get", "-last"))[0][1]
                    # self._path_marker.add('p2a')
                else:
                    # self._path_marker.add('p2b')
                    segref = tcl_tk_call((cursor_high, "get", "-prev"))[0][1]
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
                        command = [self.segment_table[file], "put"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend(
                            [int.from_bytes(segref[-4:], "big"), seg.tobytes()]
                        )
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor_high, "del"))
                        tcl_tk_call(
                            (
                                cursor_high,
                                "put",
                                "-keylast",
                                k,
                                b"".join(
                                    (
                                        segref[:4],
                                        new_count.to_bytes(2, byteorder="big"),
                                        segref[-4:],
                                    )
                                ),
                            )
                        )
                    elif isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5a-b')
                        command = [self.segment_table[file], "put", "-append"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.append(seg.tobytes())
                        srn = tcl_tk_call(tuple(command))
                        # Why not use cursor_high throughout this method?
                        # Then why not use -current and remove the delete()?
                        tcl_tk_call((cursor_high, "del"))
                        tcl_tk_call(
                            (
                                cursor_new,
                                "put",
                                "-keylast",
                                k,
                                b"".join(
                                    (
                                        segref[:4],
                                        new_count.to_bytes(2, byteorder="big"),
                                        srn.to_bytes(4, byteorder="big"),
                                    )
                                ),
                            )
                        )
                    else:
                        # self._path_marker.add('p5a-c')
                        command = [self.segment_table[file], "put"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend(
                            [int.from_bytes(segref[-4:], "big"), seg.tobytes()]
                        )
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor_high, "del"))
                        tcl_tk_call(
                            (
                                cursor_high,
                                "put",
                                "-keylast",
                                k,
                                b"".join(
                                    (
                                        segref[:4],
                                        new_count.to_bytes(2, byteorder="big"),
                                        segref[-4:],
                                    )
                                ),
                            )
                        )
                elif isinstance(seg, RecordsetSegmentList):
                    # self._path_marker.add('p5b')
                    if isinstance(current_segment, RecordsetSegmentInt):
                        # self._path_marker.add('p5b-a')
                        command = [self.segment_table[file], "put", "-append"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.append(seg.tobytes())
                        srn = tcl_tk_call(tuple(command))
                        # Why not use cursor_high throughout this method?
                        # Then why not use -current and remove the delete()?
                        tcl_tk_call((cursor_high, "del"))
                        tcl_tk_call(
                            (
                                cursor_new,
                                "put",
                                "-keylast",
                                k,
                                b"".join(
                                    (
                                        segref[:4],
                                        new_count.to_bytes(2, byteorder="big"),
                                        srn.to_bytes(4, byteorder="big"),
                                    )
                                ),
                            )
                        )
                    else:
                        # self._path_marker.add('p5b-b')
                        command = [self.segment_table[file], "put"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend(
                            [
                                int.from_bytes(segref[-4:], "big"),
                                seg.tobytes(),
                            ]
                        )
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor_high, "del"))
                        tcl_tk_call(
                            (
                                cursor_high,
                                "put",
                                "-keylast",
                                k,
                                b"".join(
                                    (
                                        segref[:4],
                                        new_count.to_bytes(2, byteorder="big"),
                                        segref[-4:],
                                    )
                                ),
                            )
                        )
                else:
                    # self._path_marker.add('p5c')
                    raise DatabaseError("Unexpected segment type")

                # Delete segment so it is not processed again as a new
                # segment.
                del segvalues[skey]

        finally:
            # self._path_marker.add('p6')
            tcl_tk_call((cursor_high, "close"))
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
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor_new = tcl_tk_call(tuple(command))
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
            # Commented statements kept without conversion.
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
                    command = [self.segment_table[file], "put", "-append"]
                    if self.dbtxn:
                        command.extend(["-txn", self.dbtxn])
                    command.append(records)
                    srn = tcl_tk_call(tuple(command))
                    tcl_tk_call(
                        (
                            cursor_new,
                            "put",
                            "-keylast",
                            k,
                            b"".join(
                                (
                                    segment_bytes,
                                    count.to_bytes(2, byteorder="big"),
                                    srn.to_bytes(4, byteorder="big"),
                                )
                            ),
                        )
                    )
                else:
                    tcl_tk_call(
                        (
                            cursor_new,
                            "put",
                            "-keylast",
                            k,
                            b"".join(
                                (
                                    segment_bytes,
                                    records.to_bytes(2, byteorder="big"),
                                )
                            ),
                        )
                    )

        finally:
            tcl_tk_call((cursor_new, "close"))
            # Commented statement kept without conversion.
            # self.table_connection_list[-1].close() # multi-chunk segments

        # Flush buffers to avoid 'missing record' exception in populate_segment
        # calls in later multi-chunk updates on same segment.  Not known to be
        # needed generally yet.
        tcl_tk_call((self.segment_table[file], "sync"))

    def new_deferred_root(self, file, field):
        """Do nothing.

        Populating main database is slower than using a sequence of small
        staging areas, but makes transaction commits in applications at
        convenient intervals awkward.

        Deferred update always uses the '-1' database so the main database is
        accessed automatically since it is the '0' database.
        """

    def merge(self, file, field):
        """Do nothing: there is nothing to do in _dbdu_tkinter module."""

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        # record keys are 1-based but segment_numbers are 0-based.
        command = [ebm_control.ebm_table, "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(key + 1)
        seg = tcl_tk_call(tuple(command))
        if not seg:
            return None
        return seg[0][1]

    def find_value_segments(self, field, file):
        """Yield segment references for field in file."""
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-first"))
            while record:
                value, segment = record[0]
                if len(segment) == 6:
                    count = b"\x00\x01"
                    reference = segment[4:]
                else:
                    assert len(segment) == 10
                    count = segment[4:6]
                    reference = segment[6:]
                yield [
                    value,
                    segment[:4],
                    EXISTING_SEGMENT_REFERENCE,
                    count,
                    reference,
                ]
                record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))

    def delete_index(self, file, field):
        """Remove all records from database for field in file."""
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "truncate",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        tcl_tk_call(tuple(command))

    def merge_writer(self, file, field):
        """Return a Writer instance for the field index on table file.

        Call the write() method with object returned by next_sorted_item
        method.

        """

        def make_segment_from_high(high, get_segment_value):
            if high[2] == b"\x00\x01":
                return RecordsetSegmentInt(
                    int.from_bytes(high[1], byteorder="big"),
                    None,
                    records=high[3],
                )
            bytestring_records = tcl_tk_call(
                tuple(
                    get_segment_value
                    + [int.from_bytes(high[3], byteorder="big")]
                )
            )[0][1]
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

        def read_high_item_in_index(get_last_record_in_index):
            record = tcl_tk_call(tuple(get_last_record_in_index))
            assert len(record) == 1
            value, segment = record[0]
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

        table = self.table[SUBFILE_DELIMITER.join((file, field))]
        segment_table = self.segment_table[file]

        class Writer:
            """Write index entries to database."""

            def __init__(self, database):
                self.prev_segment = None
                self.prev_key = None
                self.database = database
                if self.database.dbtxn is None:
                    self.cursor = tcl_tk_call(tuple([table, "cursor"]))
                    self.write_item_to_index = [self.cursor, "put", "-keylast"]
                    self.delete_index_item = [self.cursor, "del"]
                    self.get_last_record_in_index = [
                        self.cursor,
                        "get",
                        "-last",
                    ]
                    self.write_segment_value = [
                        segment_table,
                        "put",
                        "-append",
                    ]
                    self.get_segment_value = [segment_table, "get"]
                    self.replace_segment_value = [segment_table, "put"]
                else:
                    self.make_new_cursor()

            def make_new_cursor(self):
                """Create a cursor on the assumed new transaction.

                The existing cursor is retained for no transaction.

                """
                if self.database.dbtxn is None:
                    return
                self.cursor = tcl_tk_call(
                    tuple([table, "cursor", "-txn", self.database.dbtxn])
                )
                self.write_item_to_index = [self.cursor, "put", "-keylast"]
                self.delete_index_item = [self.cursor, "del"]
                self.get_last_record_in_index = [self.cursor, "get", "-last"]
                self.write_segment_value = [
                    segment_table,
                    "put",
                    "-append",
                    "-txn",
                    self.database.dbtxn,
                ]
                self.get_segment_value = [
                    segment_table,
                    "get",
                    "-txn",
                    self.database.dbtxn,
                ]
                self.replace_segment_value = [
                    segment_table,
                    "put",
                    "-txn",
                    self.database.dbtxn,
                ]

            def close_cursor(self):
                """Close the cursor open on the index."""
                tcl_tk_call((self.cursor, "close"))

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
                        tcl_tk_call(
                            tuple(
                                self.write_item_to_index
                                + [item[0], b"".join(item[1:])]
                            )
                        )
                        length = len(b"".join(item[1:]))
                        assert length == 10 or length == 6
                        return
                    if int.from_bytes(item[-2], byteorder="big") > 1:
                        item[-1] = tcl_tk_call(
                            tuple(self.write_segment_value + [item[-1]])
                        ).to_bytes(4, byteorder="big")
                        assert len(item) == 4
                        assert len(b"".join(item[1:])) == 10
                    else:
                        item.pop(2)
                        assert len(item) == 3
                        assert len(b"".join(item[1:])) == 6
                    tcl_tk_call(
                        tuple(
                            self.write_item_to_index
                            + [item[0], b"".join(item[1:])]
                        )
                    )
                    assert item_type == NEW_SEGMENT_CONTENT
                    return
                if self.prev_key == item[0]:
                    assert item[2] == NEW_SEGMENT_CONTENT
                    del item[2]
                    high = read_high_item_in_index(
                        self.get_last_record_in_index
                    )
                    new_segment = make_segment_from_item(item)
                    new_segment |= make_segment_from_high(
                        high, self.get_segment_value
                    )
                    new_segment.normalize()
                    item[-2] = (
                        self.database.encode_number_for_sequential_file_dump(
                            new_segment.count_records(), 2
                        )
                    )
                    if high[2] == b"\x00\x01":
                        item[-1] = tcl_tk_call(
                            tuple(self.write_segment_value + [item[-1]])
                        ).to_bytes(4, byteorder="big")
                        tcl_tk_call(tuple(self.delete_index_item))
                        assert len(b"".join(item[1:])) == 10
                        tcl_tk_call(
                            tuple(
                                self.write_item_to_index
                                + [item[0], b"".join(item[1:])]
                            )
                        )
                    else:
                        tcl_tk_call(
                            tuple(
                                self.replace_segment_value
                                + [
                                    int.from_bytes(high[-1], "big"),
                                    new_segment.tobytes(),
                                ]
                            )
                        )
                        tcl_tk_call(tuple(self.delete_index_item))
                        item[-1] = high[-1]
                        assert len(b"".join(item[1:])) == 10
                        tcl_tk_call(
                            tuple(
                                self.write_item_to_index
                                + [item[0], b"".join(item[1:])]
                            )
                        )
                    assert len(item) == 4
                    return
                item_type = item.pop(2)
                if item_type == EXISTING_SEGMENT_REFERENCE:
                    assert len(item) == 4
                    self.prev_key = item[0]
                    if item[-2] == b"\x00\x01":
                        item.pop(-2)
                    tcl_tk_call(
                        tuple(
                            self.write_item_to_index
                            + [item[0], b"".join(item[1:])]
                        )
                    )
                    length = len(b"".join(item[1:]))
                    assert length == 10 or length == 6
                    return
                if int.from_bytes(item[-2], byteorder="big") > 1:
                    item[-1] = tcl_tk_call(
                        tuple(self.write_segment_value + [item[-1]])
                    ).to_bytes(4, byteorder="big")
                    assert len(item) == 4
                    assert len(b"".join(item[1:])) == 10
                else:
                    item.pop(2)
                    assert len(item) == 3
                    assert len(b"".join(item[1:])) == 6
                tcl_tk_call(
                    tuple(
                        self.write_item_to_index
                        + [item[0], b"".join(item[1:])]
                    )
                )
                assert item_type == NEW_SEGMENT_CONTENT
                return

        return Writer(self)
