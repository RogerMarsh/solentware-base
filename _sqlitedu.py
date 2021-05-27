# _sqlite3du.py
# Copyright (c) 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API for bulk insertion of records, implemented using an interface
to SQLite3, where indicies are represented as lists or bitmaps of record
numbers.

The database is accessed as if made from Berkeley DB primary and secondary
databases.

The classes in this module implement deferred updates to a SQLite3 database,
providing replacements for methods in the _sqlite module.  Their subclasses
choose the interface to SQLite3.
"""

import heapq
import collections

from .api.bytebit import Bitarray
from . import _sqlite
from .api.constants import (
    SQLITE_SEGMENT_COLUMN,
    PRIMARY,
    SECONDARY,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_VALUE_COLUMN,
    SUBFILE_DELIMITER,
    INDEXPREFIX,
    TABLEPREFIX,
    )
from .api.segmentsize import SegmentSize
from .api import primarydu
from .api import secondarydu
from .api import databasedu


class Sqlite3duapiError(_sqlite.Sqlite3apiError):
    pass


class Sqlite3duapi(databasedu.Database):
    
    """
    Provide replacements of methods in _sqlite.Sqlite3api suitable for deferred
    update.

    The class which chooses the interface to SQLite3 must include this class
    earlier in the Method Resolution Order than _sqlite3.Sqlite3api.

    Normally deferred updates are synchronised with adding the last record
    number to a segment.  Sometimes memory constraints will force deferred
    updates to be done more frequently, but this will likely increase the time
    taken to do the deferred updates for the second and later points in a
    segment.
    """

    # Override in subclasses if more frequent deferred update is required.
    deferred_update_points = frozenset([SegmentSize.db_segment_size - 1])
            
    # This method is uncommented if deferred updates are done without a journal
    # and without synchronous updates.  See pragmas in set_defer_update and
    # unset_defer_update methods.
    #def commit(self):
    #    """Override superclass method to do nothing."""

    def make_cursor(self, dbname):
        raise Sqlite3duapiError('make_cursor not implemented')

    def do_segment_deferred_updates(self, primarydb, segment, offset, db=None):
        """Do deferred updates for segment filled during run."""
        primarydb.write_existence_bit_map(segment)
        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        main = self.database_definition
        for d in defer:
            for s in main[d].secondary.values():
                if not s.is_primary():
                    s.sort_and_write(segment)
            if offset == max(self.deferred_update_points):
                main[d].primary.first_chunk = True
            elif offset == min(self.deferred_update_points):
                main[d].primary.first_chunk = False
                main[d].primary.high_segment = segment

    def do_final_segment_deferred_updates(self, db=None):
        """Do deferred updates for partially filled final segment."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return

        # Write the final deferred segment database for each index
        main = self.database_definition
        for d in defer:
            primary = main[d].primary
            statement = ' '.join((
                'select',
                primary.dataname,
                'from',
                primary.table_link,
                'order by',
                primary.dataname, 'desc',
                'limit 1',
                ))
            values = ()
            c = primary.table_dbservices.cursor()
            try:
                segment, record_number = divmod(
                    c.execute(statement, values).fetchone()[0],
                    SegmentSize.db_segment_size)
                if record_number in self.deferred_update_points:
                    continue # Assume put_instance did deferred updates
            except TypeError:
                # Assume fetchone() reurned None (empty file)
                continue
            finally:
                c.close()
            primary.write_existence_bit_map(segment)
            for s in main[d].secondary.values():
                if not s.is_primary():
                    s.sort_and_write(segment)
                    s.merge()

    def set_defer_update(self, db=None, duallowed=False):

        defer = self._get_deferable_update_files(db)
        if not defer:
            return

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

        # Comment these if the 'do-nothing' override of commit() is commented.
        #self.dbservices.cursor().execute('pragma journal_mode = off')
        #self.dbservices.cursor().execute('pragma synchronous = off')
        self.start_transaction()

        main = self.database_definition
        for d in defer:
            t = main[d].primary
            c = self.dbservices.cursor()
            try:
                high_record = c.execute(
                    ' '.join((
                        'select max(rowid) from',
                        t.dataname,
                        ))).fetchone()[0]
            finally:
                c.close()
            if high_record is not None:
                segment, record = divmod(high_record,
                                         SegmentSize.db_segment_size)
                t.initial_high_segment = segment
                t.high_segment = segment
                t.first_chunk = record < min(self.deferred_update_points)
                continue
            t.initial_high_segment = None
            t.high_segment = None
            t.first_chunk = None
        return duallowed

    def unset_defer_update(self, db=None):
        """Tidy-up at end of deferred update run."""

        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        main = self.database_definition
        for d in defer:
            main[d].primary.high_segment = None
            main[d].primary.first_chunk = None

        # See comment in set_defer_update method.

        self.commit()
        
        # Comment these if the 'do-nothing' override of commit() is commented.
        #self.dbservices.cursor().execute('pragma journal_mode = delete')
        #self.dbservices.cursor().execute('pragma synchronous = full')


class Primary(primarydu.Primary, _sqlite.Primary):

    """Add methods for deferred update processing to _sqlite.Primary.

    Primary database updates are done directly, but the existence bitmap
    updates are deferred (along with secondary database updates) until the
    next deferred update point usually when the last record has been added
    to a segment.
    """

    def __init__(self, *args, **kargs):
        """Delegate arguments to superclass and prepare to cache existence
        bitmap segments for deferred update."""
        super().__init__(*args, **kargs)

        # The safe setting unless processing knows better.
        self.initial_high_segment = None

    def write_existence_bit_map(self, segment):
        """Write the existence bit map for segment."""
        statement = ' '.join((
            'insert or replace into',
            self.get_existence_bits()._segment_link,
            '(',
            self.get_existence_bits()._segment_link, ',',
            SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? , ? )',
            ))
        values = (segment + 1, self.existence_bit_maps[segment].tobytes())
        c = self.get_existence_bits()._segment_dbservices.cursor()
        try:
            c.execute(statement, values)
        finally:
            c.close()

    # Hack for primarydu.Primary.defer_put
    def _get_existence_bits(self, segment):
        return self.get_existence_bits().get(segment + 1)


class Secondary(secondarydu.Secondary, _sqlite.Secondary):

    """Add methods for deferred update processing to _sqlite.Secondary.

    Secondary database updates are deferred by doing them to a sequence of
    temporary secondary databases, one per deferred update point, followed by
    merging the temporary databases into the secondary databases after all
    updates to the associated primary database are done.
    """

    def make_cursor(self, dbname):
        raise Sqlite3duapiError('make_cursor not implemented')

    def new_deferred_root(self):
        """Make new temporary table for deferred updates and close current."""
        # The temporary tables go in /tmp, at least in OpenBSD where the default
        # mount points allocate far too little space to /tmp for this program.
        # The FreeBSD default layout is now a single '/' area, so the space is
        # available but it is not clear which '/<any>' gets used.
        self.table_connection_list.append(
            SUBFILE_DELIMITER.join((TABLEPREFIX,
                                    str(len(self.table_connection_list) - 1),
                                    self.dataname)))
        self.indexname_list.append(''.join((INDEXPREFIX,
                                            self.table_connection_list[-1])))
        try:
            statement = ' '.join((
                'create temp table',
                self.table_connection_list[-1],
                '(',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                ')',
                ))
            c = self.table_dbservices.cursor()
            try:
                c.execute(statement)
            finally:
                c.close()

            # The index is not needed if deferred_update_points has exactly one
            # element because each segment is done in one chunk.
            statement = ' '.join((
                'create unique index',
                self.indexname_list[-1],
                'on', self.table_connection_list[-1],
                '(',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN,
                ')',
                ))
            c = self.table_dbservices.cursor()
            try:
                c.execute(statement)
            finally:
                c.close()

        except:
            self.close()
            raise

    def sort_and_write(self, segment):
        """Sort the segment deferred updates before writing to database.

        Index updates are serialized as much as practical: meaning the lists
        or bitmaps of record numbers are put in a subsidiary table and the
        tables are written one after the other.

        """

        # Lookup table is much quicker, and noticeable, in bulk use.
        # Big enough to discard when done.
        int_to_bytes = [n.to_bytes(2, byteorder='big')
                        for n in range(SegmentSize.db_segment_size)]
        #bytes_to_int = {b:e for e, b in enumerate(int_to_bytes)}

        segvalues = self.values

        # Prepare to wrap the record numbers in an appropriate Segment class.
        for k in segvalues:
            v = segvalues[k]
            if isinstance(v, list):
                segvalues[k] = [
                    len(v),
                    b''.join([int_to_bytes[n] for n in v]),
                    ]
            elif isinstance(v, Bitarray):
                segvalues[k] = [
                    v.count(),
                    v.tobytes(),
                    ]
            elif isinstance(v, int):
                segvalues[k] = [1, v]

        # Discard lookup tables.
        del int_to_bytes
        #del bytes_to_int

        gpd = self.get_primary_database()

        # New records go into temporary databases, one for each segment, except
        # when filling the segment which was high when this update started. 
        if gpd.first_chunk and gpd.initial_high_segment != segment:
            self.new_deferred_root()

        # The low segment in the import may have to be merged with an existing
        # high segment on the database, or the current segment in the import
        # may be done in chunks of less than a complete segment.
        # Note the difference between this code, and the similar code in module
        # apswduapi.py, and the code in module dbduapi.py: the Berkeley DB
        # code updates the main index directly if an entry already exists, but
        # the Sqlite code always updates a temporary table and merges into the
        # main table later.
        # Here tablename always binds to table_connection_list[-1].
        tablename = self.table_connection_list[-1]
        if gpd.high_segment == segment or not gpd.first_chunk:

            # select (index value, segment number, record count, key reference)
            # statement for (index value, segment number).  Execution returns
            # None if no splicing needed.
            select_existing_segment = ' '.join((
                'select',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                'from',
                tablename,
                'where',
                self.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))

            # Update (record count) statement for (index value, segment number)
            # used when splicing needed.
            update_record_count = ' '.join((
                'update',
                tablename,
                'set',
                SQLITE_COUNT_COLUMN, '= ?',
                'where',
                self.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))

            # Update (record count, key reference) statement
            # for (index value, segment number) used when record count increased
            # from 1.
            update_count_and_reference = ' '.join((
                'update',
                tablename,
                'set',
                SQLITE_COUNT_COLUMN, '= ? ,',
                self.rowids_in_primary, '= ?',
                'where',
                self.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))

            c = self.table_dbservices.cursor()
            try:
                for k in sorted(segvalues):
                    values = (k, segment)
                    s = c.execute(select_existing_segment, values).fetchone()
                    if s is None:
                        continue
                    current_segment = self.populate_segment(s)
                    values = (segvalues[k][0] + s[2], k, segment)
                    c.execute(update_record_count, values)

                    # If the existing segment record for a segment in segvalues
                    # had a record count > 1 before being updated, a subsidiary
                    # table record already exists.  Otherwise it must be
                    # created.
                    # Key reference is a record number if record count is 1.
                    seg = (self.make_segment(k, segment, *segvalues[k]
                                             ) | current_segment).normalize()
                    if s[2] > 1:
                        gpd.set_segment_records((seg.tobytes(), s[3]))
                    else:
                        nv = gpd.insert_segment_records((seg.tobytes(),))
                        c.execute(
                            update_count_and_reference,
                            (s[2]+segvalues[k][0], nv, k, s[1]))
                    del segvalues[k]
            finally:
                c.close()

        # Process segments which do not need to be spliced.
        # This includes any not dealt with by low segment processing.

        # Insert new record lists in subsidiary table and note rowids.
        # Modify the index record values to refer to the rowid if necessary.
        for k in segvalues:
            v = segvalues[k]
            if v[0] > 1:
                v[1] = gpd.insert_segment_records((v[1],))

        # insert (index value, segment number, record count, key reference)
        # statement.
        insert_new_segment = ' '.join((
            'insert into',
            tablename,
            '(',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            ')',
            'values ( ? , ? , ? , ? )',
            ))

        # Insert new index records.
        self.table_dbservices.cursor().executemany(
            insert_new_segment, self._rows(sorted(segvalues), segment))
        segvalues.clear()

    def _rows(self, ssv, s):
        """Helper method to avoid len(ssv) ~.execute() calls."""
        segvalues = self.values
        for k in ssv:
            v = segvalues[k]
            yield (k, s, v[0], v[1])

    def merge(self):
        """Merge the segment deferred updates into database."""

        # Any deferred updates?
        if len(self.table_connection_list) == 1:
            return

        # Cannot imitate the renaming of databases done in bsddb3.  The sqlite3
        # table names can be changed using 'alter', but the indicies keep their
        # names. A bsddb3 database is equivalent to an sqlite3 table and it's
        # index here.

        # Write the entries from the deferred update indices to the existing
        # index in sort order: otherwise might as well have written the index
        # entries direct to the existing index rather than to the deferred
        # update indices.
        # Assume at least SegmentSize.segment_sort_scale records in each index.
        # But OS ought to make the buffering done here a waste of time.
        sq_deferred = self.table_connection_list[1:]
        sq_buffers = []
        sq_cursors = []
        try:
            for e, sqo in enumerate(sq_deferred):

                # select
                # (index value, segment number, record count, key reference)
                # statement for (index value, segment number).
                # The 'order by' clause is not needed if deferred_update_points
                # has exactly one element because each segment is done in one
                # chunk.
                select_segments = ' '.join((
                    'select',
                    self.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self.rowids_in_primary,
                    'from',
                    sqo,
                    'order by',
                    self.dataname, ',',
                    SQLITE_SEGMENT_COLUMN,
                    ))

                sq_cursors.append(
                    self.table_dbservices.cursor().execute(select_segments))

            # The arraysize property and the fetchmany method are assumed to
            # be available only in sqlite3.
            # In particular, apsw does not have them.
            arraysize = int(
                SegmentSize.segment_sort_scale // max(1, len(sq_cursors)))
            #for sqc in sq_cursors:
            #    sqc.arraysize = arraysize
            for e, sqc in enumerate(sq_cursors):
                buffer = collections.deque()
                sq_buffers.append(buffer)
                #buffer.extend(sqc.fetchmany())
                for i in range(arraysize):
                    r = sqc.fetchone()
                    if r is None:
                        break
                    buffer.append(r)
                #if len(buffer) < sqc.arraysize:
                if len(buffer) < arraysize:
                    sq_cursors[e].close()
                    sq_cursors[e] = None
                del buffer
            updates = []
            heapq.heapify(updates)
            heappop = heapq.heappop
            heappush = heapq.heappush
            for e, buffer in enumerate(sq_buffers):
                if buffer:
                    heappush(updates, (buffer.popleft(), e))

            # insert (index value, segment number, record count, key reference)
            # statement.
            insert_new_segment = ' '.join((
                'insert into',
                self.table_link,
                '(',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                ')',
                'values ( ? , ? , ? , ? )',
                ))

            cursor = self.table_dbservices.cursor()
            try:
                while len(updates):
                    record, e = heappop(updates)
                    cursor.execute(insert_new_segment, record)
                    buffer = sq_buffers[e]
                    if not buffer:
                        c = sq_cursors[e]
                        if c is None:
                            continue
                        #buffer.extend(c.fetchmany())
                        for i in range(arraysize):
                            r = c.fetchone()
                            if r is None:
                                break
                            buffer.append(r)
                        #if len(buffer) < c.arraysize:
                        if len(buffer) < arraysize:
                            c.close()
                            sq_cursors[e] = None
                        del c
                        if not buffer:
                            continue
                    heappush(updates, (buffer.popleft(), e))
            finally:
                cursor.close()
        finally:
            for c in sq_cursors:
                if c:
                    c.close()
