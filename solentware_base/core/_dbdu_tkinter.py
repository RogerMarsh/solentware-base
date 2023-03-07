# _dbdu_tkinter.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database with the tkinter module."""
import heapq
import collections

from ..db_tcl import tcl_tk_call, TclError
from .bytebit import Bitarray
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

    # Deferred updates are non-transactional in Berkeley DB.
    def environment_flags(self, dbe):
        """Return environment flags for deferred update."""
        # Do not see how to say DB_INIT_MPOOL and DB_INIT_LOCK.
        # Maybe setting those is pointless in _dbdu module?
        return ["-create", "-private"]

    def checkpoint_before_close_dbenv(self):
        """Do nothing.  Deferred updates are non-transactional."""
        # Most calls of txn_checkpoint() are conditional on self.dbtxn, but the
        # call when closing the database does not check for a transaction.
        # Rely on environment_flags() call for transaction state.

    def start_transaction(self):
        """Do not start transaction in deferred update mode."""
        self.dbtxn = None

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            command = [self.table[file][0], "cursor"]
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
            self.write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                # In Tcl API the database handles opened in sort_and_write
                # must be closed explicitly to avoid a crash, after updates
                # have been completed, when closing the environment.
                tablename = SUBFILE_DELIMITER.join((file, secondary))
                for obj in self.table[tablename][1:]:
                    tcl_tk_call((obj, "close"))
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.start_transaction()
        for file in self.specification:
            command = [self.table[file][0], "cursor"]
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
        self._int_to_bytes = None
        for file in self.specification:
            self.high_segment[file] = None
            self.first_chunk[file] = None
        self.commit()

    def write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment."""
        tcl_tk_call(
            (
                self.ebm_control[file].ebm_table,
                "put",
                segment + 1,
                self.existence_bit_maps[file][segment].tobytes(),
            )
        )

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
            self.table[SUBFILE_DELIMITER.join((file, field))][-1],
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

        # Lookup table is much quicker, and noticeable, in bulk use.
        int_to_bytes = self._int_to_bytes

        segvalues = self.value_segments[file][field]

        # Prepare to wrap the record numbers in an appropriate Segment class.
        for k in segvalues:
            value = segvalues[k]
            if isinstance(value, list):
                segvalues[k] = [
                    len(value),
                    b"".join([int_to_bytes[n] for n in value]),
                ]
            elif isinstance(value, Bitarray):
                segvalues[k] = [
                    value.count(),
                    value.tobytes(),
                ]
            elif isinstance(value, int):
                segvalues[k] = [1, value]

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
        # Note the substantive difference between this module and _sqlitedu:
        # the code for Berkeley DB updates the main index directly if an entry
        # already exists, but the code for SQLite always updates a temporary
        # table and merges into the main table later.
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))][-1],
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
        """Make new DB in dbenv for deferred updates and close current one."""
        command = [
            "berkdb",
            "open",
            "-env",
            self.dbenv,
            "-dupsort",
            "-btree",
            "-create",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append("--")
        tablename = SUBFILE_DELIMITER.join((file, field))
        secondary = SUBFILE_DELIMITER.join(
            (str(len(self.table[tablename]) - 1), file, field)
        )
        if self.home_directory is not None:
            command.append(secondary)
        command.append(secondary)
        try:
            # Commented statement kept without conversion.
            # am = self.specification[file][FIELDS][field].get(ACCESS_METHOD)
            self.table[tablename].append(tcl_tk_call(tuple(command)))
        except:
            for obj in self.table[tablename][1:]:
                try:
                    tcl_tk_call((obj, "close"))
                except:
                    pass
            self.close()
            raise

    def merge(self, file, field):
        """Merge the segment deferred updates into database."""
        # Merge the segment deferred updates into database.

        # Some of the unit testing using commented '_path_marker' code can be
        # done with unittest.mock with suitable blocks of code delegated to
        # methods.  For example is the outer 'finally' block executed with or
        # without the block labelled 'p3'?  But how about the blocks labelled
        # 'p13' through 'p18'?
        # Dividing 'merge' into 'merge main' and some 'merge helpers' to avoid
        # commented testing code seems wrong since doing discrete portions of
        # merge is nonsense.
        # To verify path coverage uncomment the '_path_marker' code.
        # self._path_marker = set()

        tablename = SUBFILE_DELIMITER.join((file, field))

        # Any deferred updates?
        if len(self.table[tablename]) == 1:
            # self._path_marker.add('p1')
            return

        # self._path_marker.add('p2')
        # Rename existing index and create new empty one.
        # Open the old and new index, and all the deferred update indexes.
        # Commented statement kept without conversion.
        # am = self.specification[file][FIELDS][field].get(ACCESS_METHOD)
        dudbc = len(self.table[tablename]) - 1
        # This block exists in bsddb3 or berkeleydb version of this module.
        # Kept without conversion.
        # if self._file_per_database:
        #    f, d = self.table[tablename][0].get_dbname()
        #    self.table[tablename][0].close()
        #    newname = SUBFILE_DELIMITER.join(('0', d))
        #    if self.home_directory is not None:
        #        newname = os.path.join(self.home_directory, newname)
        #    self.dbenv.dbrename(f, None, newname=newname)
        #    self.dbenv.dbrename(
        #        newname, d, newname=SUBFILE_DELIMITER.join(('0', d)))
        #    self.table[tablename] = [self._dbe.DB(self.dbenv)]
        #    self.table[tablename][0].set_flags(self._dbe.DB_DUPSORT)
        #    self.table[tablename][0].open(
        #        f,
        #        dbname=d,
        #        dbtype=self._dbe.DB_BTREE,
        #        flags=self._dbe.DB_CREATE)
        del self.table[tablename][1:]
        # The Tcl API does not have an equivalent to get_dbname, so prepare
        # to note names for database command names.
        db_file_database = {}
        for i in range(dudbc):
            # self._path_marker.add('p3')
            secondary = SUBFILE_DELIMITER.join(
                (str(len(self.table[tablename]) - 1), file, field)
            )
            command = [
                "berkdb",
                "open",
                "-dupsort",
                "-btree",
                "-env",
                self.dbenv,
            ]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.append("--")
            if self.home_directory:
                command.append(secondary)
            command.append(secondary)
            self.table[tablename].append(tcl_tk_call(tuple(command)))
            db_file_database[self.table[tablename][-1]] = (
                secondary if self.home_directory else "",
                secondary,
            )

        # if self._file_per_database:
        #    self.table[tablename].insert(1, self._dbe.DB(self.dbenv))
        #    self.table[tablename][1].set_flags(self._dbe.DB_DUPSORT)
        #    self.table[tablename][1].open(
        #        newname if self.home_directory is not None else None,
        #        dbname=SUBFILE_DELIMITER.join(('0', d)),
        #        dbtype=self._dbe.DB_BTREE)

        # Write the entries from the old index and deferred update indexes to
        # the new index in sort order: otherwise might as well have written the
        # index entries direct to the old index rather than to the deferred
        # update indexes.
        # Assume at least 65536 records in each index. (segment_sort_scale)
        # But OS ought to make the buffering done here a waste of time.
        db_deferred = self.table[tablename][1:]
        db_buffers = []
        db_cursors = []
        for dbo in db_deferred:
            # self._path_marker.add('p4')
            db_buffers.append(collections.deque())
            command = [dbo, "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            db_cursors.append(tcl_tk_call(tuple(command)))
        try:
            length_limit = int(
                SegmentSize.segment_sort_scale // max(1, len(db_buffers))
            )
            for i, buffer in enumerate(db_buffers):
                # self._path_marker.add('p5')
                dbc = db_cursors[i]
                while len(buffer) < length_limit:
                    # self._path_marker.add('p6')
                    record = tcl_tk_call((dbc, "get", "-next")) or None
                    if record:
                        record = record[0]
                    buffer.append(record)
                try:
                    # self._path_marker.add('p7')
                    while buffer[-1] is None:
                        # self._path_marker.add('p8')
                        buffer.pop()
                        ## self._path_marker.add("p8")
                except IndexError:
                    # self._path_marker.add('p9')
                    tcl_tk_call((dbc, "close"))
                    db_cursors[i] = None
                    # The Tcl API does not have an equivalent to get_dbname.
                    # fname and dname remembered from creation of database.
                    # Closing now leads to exception closing database in
                    # close_database_contexts() unless the command is removed
                    # from the list of open databases, but not in the bsddb3
                    # and berkeleydb version of this module.
                    tcl_tk_call((db_deferred[i], "close"))
                    # print('*', f, d)
                    fname, dname = db_file_database[db_deferred[i]]
                    if fname:
                        command = ["berkdb", "dbremove"]
                        if self.dbenv:
                            command.extend(["-env", self.dbenv])
                        command.extend(["--", fname, dname])
                        tcl_tk_call(tuple(command))
                    del fname, dname
                del buffer
                del dbc
            updates = []
            heapq.heapify(updates)
            heappop = heapq.heappop
            heappush = heapq.heappush
            for i, buffer in enumerate(db_buffers):
                # self._path_marker.add('p10')
                if buffer:
                    # self._path_marker.add('p11')
                    heappush(updates, (buffer.popleft(), i))
            command = [self.table[tablename][0], "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            cursor = tcl_tk_call(tuple(command))
            try:
                while updates:
                    # self._path_marker.add('p12')
                    record, i = heappop(updates)
                    tcl_tk_call(
                        (cursor, "put", "-keylast", record[0], record[1])
                    )
                    buffer = db_buffers[i]
                    if not buffer:
                        # self._path_marker.add('p13')
                        dbc = db_cursors[i]
                        if dbc is None:
                            # self._path_marker.add('p14')
                            continue
                        while len(buffer) < length_limit:
                            # self._path_marker.add('p15')
                            record = tcl_tk_call((dbc, "get", "-next")) or None
                            if record:
                                record = record[0]
                            buffer.append(record)
                        try:
                            # self._path_marker.add('p16')
                            while buffer[-1] is None:
                                # self._path_marker.add('p17')
                                buffer.pop()
                        except IndexError:
                            # self._path_marker.add('p18')
                            tcl_tk_call((dbc, "close"))
                            db_cursors[i] = None
                            # The Tcl API does not have an equivalent to
                            # get_dbname.
                            # fname and dname remembered from creation of
                            # database.
                            # Closing now leads to exception closing database
                            # in close_database_contexts() unless the command
                            # is removed from the list of open databases, but
                            # not in the bsddb3 and berkeleydb version of
                            # this module.
                            tcl_tk_call((db_deferred[i], "close"))
                            # print('*', f, d)
                            fname, dname = db_file_database[db_deferred[i]]
                            if fname:
                                command = ["berkdb", "dbremove"]
                                if self.dbenv:
                                    command.extend(["-env", self.dbenv])
                                command.extend(["--", fname, dname])
                                tcl_tk_call(tuple(command))
                            del fname, dname
                            continue
                        del dbc
                    heappush(updates, (buffer.popleft(), i))
            finally:
                tcl_tk_call((cursor, "close"))
        finally:
            for dbc in db_cursors:
                # self._path_marker.add('p19')
                if dbc:
                    # self._path_marker.add('p20')
                    tcl_tk_call((dbc, "close"))
            # Clear out the command names of closed databases.
            # This is not necessary in the bsddb3 or berkeleydb version of
            # this module, but should be done there too for clarity.
            # Exceptions are raised in close_database_contexts() if these are
            # not deleted here.
            del self.table[tablename][1:]

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        # record keys are 1-based but segment_numbers are 0-based.
        command = [ebm_control.ebm_table, "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(key + 1)
        return tcl_tk_call(tuple(command)) or None
