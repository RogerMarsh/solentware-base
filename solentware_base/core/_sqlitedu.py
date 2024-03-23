# _sqlitedu.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite3 database with either the apsw or sqlite3 modules.

When using sqlite3 the Python version must be 3.6 or later.

"""
from .constants import (
    SECONDARY,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_VALUE_COLUMN,
    SUBFILE_DELIMITER,
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

    def database_cursor(self, file, field, keyrange=None, recordset=None):
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
            self._write_existence_bit_map(file, segment)
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

    def _write_existence_bit_map(self, file, segment):
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
        # may be done in chunks of less than a complete segment.
        # Below now obsolete.
        # Note the substantive difference between this module and _dbdu:
        # the code for Berkeley DB updates the main index directly if an entry
        # already exists, but the code for SQLite always updates a temporary
        # table and merges into the main table later.
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
            # for (index value, segment number) used when record count
            # increased from 1.
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
                "insert or replace into",
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
        """Do nothing.

        Populating main database is slower than using a sequence of small
        staging areas, but makes transaction commits in applications at
        convenient intervals awkward.

        Deferred update always uses the '-1' database so the main database is
        accessed automatically since it is the '0' database.
        """

    def merge(self, file, field):
        """Do nothing: there is nothing to do in _sqlitedu module."""

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        return ebm_control.get_ebm_segment(key, self.dbenv)
