# _sqlitedu.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite3 database with either the apsw or sqlite3 modules.

When using sqlite3 the Python version must be 3.6 or later.

"""
import heapq
import collections

from .bytebit import Bitarray
from .constants import (
    SECONDARY,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_VALUE_COLUMN,
    SUBFILE_DELIMITER,
    INDEXPREFIX,
    TABLEPREFIX,
)
from .segmentsize import SegmentSize
from . import _databasedu


class DatabaseError(Exception):
    """Exception for Database class."""


class Database(_databasedu.Database):
    """Customise _sqlite.Database for deferred update.

    The class which chooses the interface to SQLite3 must include this class
    earlier in the Method Resolution Order than _sqlite.Database.

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

    # This method is uncommented if deferred updates are done without a journal
    # and without synchronous updates.  See pragmas in set_defer_update and
    # unset_defer_update methods.
    # def commit(self):
    #    """Override superclass method to do nothing."""

    def database_cursor(self, file, field, keyrange=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            statement = " ".join(
                (
                    "select",
                    file,
                    "from",
                    self.table[file][0],
                    "order by",
                    file,
                    "desc",
                    "limit 1",
                )
            )
            values = ()
            cursor = self.dbenv.cursor()
            try:
                segment, record_number = divmod(
                    cursor.execute(statement, values).fetchone()[0],
                    SegmentSize.db_segment_size,
                )
                if record_number in self.deferred_update_points:
                    continue  # Assume put_instance did deferred updates
            except TypeError:
                continue
            finally:
                cursor.close()
            self.write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        # Dropping the indexes before the update starts and recreating them
        # after it finishes can be a lot quicker.  The disadvantage is the
        # amount of free space needed in /var/tmp on BSD, including Mac, and
        # Linux systems.  If all disc space is mounted as / it is just a free
        # space requirement; but if the traditional recommended mount points
        # are used /var may well be too small.  Cannot do this when adding to
        # an existing database unless unless the index records are sorted
        # before updating the database: something like the bsddb3 version.
        # Timings when adding to an empty database suggest the sqlite3 version
        # would be a little slower than the bsddb3 version.

        self._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]

        # Comment these if the 'do-nothing' override of commit() is commented.
        # self.dbenv.cursor().execute('pragma journal_mode = off')
        # self.dbenv.cursor().execute('pragma synchronous = off')
        self.start_transaction()

        for file in self.specification:
            cursor = self.dbenv.cursor()
            try:
                high_record = cursor.execute(
                    " ".join(
                        (
                            "select max(rowid) from",
                            file,
                        )
                    )
                ).fetchone()[0]
            finally:
                cursor.close()
            if high_record is None:
                self.initial_high_segment[file] = None
                self.high_segment[file] = None
                self.first_chunk[file] = None
                continue
            segment, record = divmod(high_record, SegmentSize.db_segment_size)
            self.initial_high_segment[file] = segment
            self.high_segment[file] = segment
            self.first_chunk[file] = record < min(self.deferred_update_points)

    def unset_defer_update(self):
        """Tidy-up at end of deferred update run."""
        self._int_to_bytes = None
        for file in self.specification:
            self.high_segment[file] = None
            self.first_chunk[file] = None

        # See comment in set_defer_update method.

        self.commit()

        # Comment these if the 'do-nothing' override of commit() is commented.
        # self.dbenv.cursor().execute('pragma journal_mode = delete')
        # self.dbenv.cursor().execute('pragma synchronous = full')

    def write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment in file."""
        assert file in self.specification
        statement = " ".join(
            (
                "insert or replace into",
                self.ebm_control[file].ebm_table,
                "(",
                self.ebm_control[file].ebm_table,
                ",",
                SQLITE_VALUE_COLUMN,
                ")",
                "values ( ? , ? )",
            )
        )
        values = (
            segment + 1,
            self.existence_bit_maps[file][segment].tobytes(),
        )
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def sort_and_write(self, file, field, segment):
        """Sort the segment deferred updates before writing to database.

        Index updates are serialized as much as practical: meaning the lists
        or bitmaps of record numbers are put in a subsidiary table and the
        tables are written one after the other.

        """
        # Anything to do?
        if field not in self.value_segments[file]:
            return

        # Lookup table is much quicker, and noticeable, in bulk use.
        int_to_bytes = self._int_to_bytes

        segvalues = self.value_segments[file][field]

        # Prepare to wrap the record numbers in an appropriate Segment class.
        for k in segvalues:
            svk = segvalues[k]
            if isinstance(svk, list):
                segvalues[k] = [
                    len(svk),
                    b"".join([int_to_bytes[n] for n in svk]),
                ]
            elif isinstance(svk, Bitarray):
                segvalues[k] = [
                    svk.count(),
                    svk.tobytes(),
                ]
            elif isinstance(svk, int):
                segvalues[k] = [1, svk]

        # New records go into temporary databases, one for each segment, except
        # when filling the segment which was high when this update started.
        if (
            self.first_chunk[file]
            and self.initial_high_segment[file] != segment
        ):
            self.new_deferred_root(file, field)

        # The low segment in the import may have to be merged with an existing
        # high segment on the database, or the current segment in the import
        # may be done in chunks of less than a complete segment.
        # Note the difference between this code, and the similar code in module
        # apswduapi.py, and the code in module dbduapi.py: the Berkeley DB
        # code updates the main index directly if an entry already exists, but
        # the Sqlite code always updates a temporary table and merges into the
        # main table later.
        tablename = self.table[SUBFILE_DELIMITER.join((file, field))][-1]
        if self.high_segment[file] == segment or not self.first_chunk[file]:

            # select (index value, segment number, record count, key reference)
            # statement for (index value, segment number).  Execution returns
            # None if no splicing needed.
            select_existing_segment = " ".join(
                (
                    "select",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    "from",
                    tablename,
                    "where",
                    field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "== ?",
                )
            )

            # Update (record count) statement for (index value, segment number)
            # used when splicing needed.
            update_record_count = " ".join(
                (
                    "update",
                    tablename,
                    "set",
                    SQLITE_COUNT_COLUMN,
                    "= ?",
                    "where",
                    field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "== ?",
                )
            )

            # Update (record count, key reference) statement
            # for (index value, segment number) used when record count increased
            # from 1.
            update_count_and_reference = " ".join(
                (
                    "update",
                    tablename,
                    "set",
                    SQLITE_COUNT_COLUMN,
                    "= ? ,",
                    file,
                    "= ?",
                    "where",
                    field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "== ?",
                )
            )

            cursor = self.dbenv.cursor()
            try:
                for k in sorted(segvalues):
                    values = (k, segment)
                    segref = cursor.execute(
                        select_existing_segment, values
                    ).fetchone()
                    if segref is None:
                        continue
                    current_segment = self.populate_segment(segref, file)
                    values = (segvalues[k][0] + segref[2], k, segment)
                    cursor.execute(update_record_count, values)

                    # If the existing segment record for a segment in segvalues
                    # had a record count > 1 before being updated, a subsidiary
                    # table record already exists.  Otherwise it must be
                    # created.
                    # Key reference is a record number if record count is 1.
                    seg = (
                        self.make_segment(k, segment, *segvalues[k])
                        | current_segment
                    ).normalize()
                    if segref[2] > 1:
                        self.set_segment_records(
                            (seg.tobytes(), segref[3]), file
                        )
                    else:
                        nvr = self.insert_segment_records(
                            (seg.tobytes(),), file
                        )
                        cursor.execute(
                            update_count_and_reference,
                            (segref[2] + segvalues[k][0], nvr, k, segref[1]),
                        )
                    del segvalues[k]
            finally:
                cursor.close()

        # Process segments which do not need to be spliced.
        # This includes any not dealt with by low segment processing.

        # Insert new record lists in subsidiary table and note rowids.
        # Modify the index record values to refer to the rowid if necessary.
        for k in segvalues:
            svk = segvalues[k]
            if svk[0] > 1:
                svk[1] = self.insert_segment_records((svk[1],), file)

        # insert (index value, segment number, record count, key reference)
        # statement.
        insert_new_segment = " ".join(
            (
                "insert into",
                tablename,
                "(",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )

        # Insert new index records.
        self.dbenv.cursor().executemany(
            insert_new_segment, self._rows(segvalues, segment)
        )
        segvalues.clear()

    def _rows(self, segvalues, segment):
        """Yield arguments for ~.executemany() call."""
        for k in sorted(segvalues):
            svk = segvalues[k]
            yield (k, segment, svk[0], svk[1])

    def new_deferred_root(self, file, field):
        """Make new temporary table for deferred updates and close current."""
        # The temporary tables go in /tmp, at least in OpenBSD where the default
        # mount points allocate far too little space to /tmp for this program.
        # The FreeBSD default layout is now a single '/' area, so the space is
        # available but it is not clear which '/<any>' gets used.
        tablename = SUBFILE_DELIMITER.join((file, field))
        self.table[tablename].append(
            SUBFILE_DELIMITER.join(
                (TABLEPREFIX, str(len(self.table[tablename]) - 1), tablename)
            )
        )
        self.index[tablename].append(
            "".join((INDEXPREFIX, self.table[tablename][-1]))
        )
        try:
            statement = " ".join(
                (
                    "create temp table",
                    self.table[tablename][-1],
                    "(",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    ")",
                )
            )
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(statement)
            finally:
                cursor.close()

            # The index is not needed if deferred_update_points has exactly one
            # element because each segment is done in one chunk.
            statement = " ".join(
                (
                    "create unique index",
                    self.index[tablename][-1],
                    "on",
                    self.table[tablename][-1],
                    "(",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ")",
                )
            )
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(statement)
            finally:
                cursor.close()

        except:
            self.close()
            raise

    def merge(self, file, field):
        """Merge deferred updates for field in file into database."""
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

        # Any deferred updates?
        if len(self.table[SUBFILE_DELIMITER.join((file, field))]) == 1:
            # self._path_marker.add('p1')
            return

        # Cannot imitate the renaming of databases done in bsddb3.  The sqlite3
        # table names can be changed using 'alter', but the indicies keep their
        # names. A bsddb3 database is equivalent to an sqlite3 table and it's
        # index here.

        # Write the entries from the deferred update indices to the existing
        # index in sort order: otherwise might as well have written the index
        # entries direct to the existing index rather than to the deferred
        # update indices.
        # Each deferred update index will have SegmentSize.segment_sort_scale
        # records at most.

        # self._path_marker.add('p2')
        sq_deferred = self.table[SUBFILE_DELIMITER.join((file, field))][1:]
        sq_buffers = []
        sq_cursors = []
        try:
            for i, sqo in enumerate(sq_deferred):
                # self._path_marker.add('p3')

                # The 'order by' clause is not needed if deferred_update_points
                # has exactly one element because each segment is done in one
                # chunk.
                select_segments = " ".join(
                    (
                        "select",
                        field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        file,
                        "from",
                        sqo,
                        "order by",
                        field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                    )
                )

                sq_cursors.append(self.dbenv.cursor().execute(select_segments))

            # The arraysize property is assumed available only in sqlite3.
            # In particular, apsw does not have it.
            arraysize = int(
                SegmentSize.segment_sort_scale // max(1, len(sq_cursors))
            )

            # self._path_marker.add('p4')
            for i, sqc in enumerate(sq_cursors):
                # self._path_marker.add('p5')
                buffer = collections.deque()
                sq_buffers.append(buffer)
                for j in range(arraysize):
                    # self._path_marker.add('p6')
                    row = sqc.fetchone()
                    if row is None:
                        # self._path_marker.add('p7')
                        break
                    buffer.append(row)
                if len(buffer) < arraysize:
                    # self._path_marker.add('p8')
                    sq_cursors[i].close()
                    sq_cursors[i] = None
                del buffer
            updates = []
            heapq.heapify(updates)
            heappop = heapq.heappop
            heappush = heapq.heappush
            for i, buffer in enumerate(sq_buffers):
                # self._path_marker.add('p9')
                if buffer:
                    # self._path_marker.add('p10')
                    heappush(updates, (buffer.popleft(), i))
            insert_new_segment = " ".join(
                (
                    "insert into",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "(",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    ")",
                    "values ( ? , ? , ? , ? )",
                )
            )
            cursor = self.dbenv.cursor()
            try:
                # self._path_marker.add('p11')
                while updates:
                    # self._path_marker.add('p12')
                    record, i = heappop(updates)
                    cursor.execute(insert_new_segment, record)
                    buffer = sq_buffers[i]
                    if not buffer:
                        # self._path_marker.add('p13')
                        sqc = sq_cursors[i]
                        if sqc is None:
                            # self._path_marker.add('p14')
                            continue
                        for j in range(arraysize):
                            # self._path_marker.add('p15')
                            row = sqc.fetchone()
                            if row is None:
                                # self._path_marker.add('p16')
                                break
                            buffer.append(row)
                        if len(buffer) < arraysize:
                            # self._path_marker.add('p17')
                            sqc.close()
                            sq_cursors[i] = None
                        del sqc
                        if not buffer:
                            # self._path_marker.add('p18')
                            continue
                    heappush(updates, (buffer.popleft(), i))
            finally:
                cursor.close()
        finally:
            for sqc in sq_cursors:
                # self._path_marker.add('p19')
                if sqc:
                    # self._path_marker.add('p20')
                    sqc.close()
        # self._path_marker.add('p21')

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        return ebm_control.get_ebm_segment(key, self.dbenv)
