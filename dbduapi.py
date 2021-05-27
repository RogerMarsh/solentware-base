# dbduapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API for bulk insertion of records, implemented using bsddb3,
where indicies are represented as lists or bitmaps of record numbers.
    
bsddb3 is an interface to Berkeley DB.

The Primary and Secondary classes modify the behaviour of dbapi.Primary and
dbapi.Secondary to defer index updates and prevent record deletion and
amendment.

The DBduapi class configures it's superclass, dbapi.Database, to use the
Primary and Secondary classes.

Transactions are not supported.

"""

import heapq
import collections

from .api.bytebit import Bitarray, SINGLEBIT

# bsddb removed from Python 3.n
try:
    from bsddb3.db import (
        DB, DB_CREATE, DB_KEYLAST, DB_CURRENT, DB_DUPSORT, DB_BTREE, DB_HASH,
        DBKeyExistError)
except ImportError:
    from bsddb.db import (
        DB, DB_CREATE, DB_KEYLAST, DB_CURRENT, DB_DUPSORT, DB_BTREE, DB_HASH,
        DBKeyExistError)

from . import dbapi

from .api.constants import (
    DB_DEFER_FOLDER,
    SECONDARY,
    PRIMARY,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    ACCESS_METHOD,
    HASH,
    SUBFILE_DELIMITER,
    )

from .api.segmentsize import SegmentSize

from .api.database import DatabaseError
from .api.recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    )
from .api import primarydu
from .api import secondarydu
from .api import databasedu


class DBduapiError(dbapi.DBapiError):
    pass


class DBduapi(databasedu.Database, dbapi.Database):
    
    """Access database with bsddb3.  See superclass for *args and **kargs.
    
    bsddb3 is an interface to Berkeley DB.
    
    Primary instances are used to do bulk data insertions, and
    Secondary instances are used to update indicies for the bulk data
    insertions.

    There will be one Primary instance for each Berkeley DB primary
    database, used approximately like a SQLite table.

    There will be one Secondary instance for each Berkeley DB secondary
    database, used approximately like a SQLite3 index.

    Primary and secondary terminology comes from Berkeley DB documentation but
    the association technique is not used.
    
    """
    # Override in subclasses if more frequent deferred update is required.
    deferred_update_points = frozenset([SegmentSize.db_segment_size - 1])

    def __init__(self, database_specification, *args, deferfolder=None, **kargs):
        """Use Primary and Secondary classes."""
        super().__init__(
            Primary, Secondary, database_specification, *args, **kargs)
        self._control = dbapi.ControlFile(database_instance=self)
        m = self.database_definition
        for n in database_specification:
            m[n].primary.set_control_database(self._control)
            # Segment database updates are done in do_segment_deferred_updates
            # and do_final_segment_deferred_updates, not in the temporary
            # secondary databases that collect the index values.  No need to
            # link these for access to segment databases like in non-deferred
            # updates.
            for k, v in database_specification[n][SECONDARY].items():
                m[n].associate(k).set_primary_database(m[n].primary)

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
                    s.sort_and_write(self.dbservices, segment)
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
            c = primary.table_link.cursor()
            try:
                segment, record_number = divmod(
                    c.last()[0],
                    SegmentSize.db_segment_size)
            except TypeError:

                # Assume deferred update to an empty file with nothing.
                continue

            finally:
                c.close()
            if record_number in self.deferred_update_points:
                continue # Assume put_instance did deferred updates
            primary.write_existence_bit_map(segment)
            for s in main[d].secondary.values():
                if not s.is_primary():
                    s.sort_and_write(self.dbservices, segment)
                    s.merge(self.dbservices)

    def reset_defer_limit(self):
        """Do nothing - DPT compatibility."""

    def set_defer_limit(self, limit):
        """Do nothing - DPT compatibility."""

    def set_defer_update(self, db=None, duallowed=False):
        """Set deferred update for db DBs and return duallowed. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        main = self.database_definition
        for d in defer:
            t = main[d].primary
            c = t.table_link.cursor()
            try:
                high_record = c.last()
            finally:
                c.close()
            if high_record is not None:
                segment, record = divmod(high_record[0],
                                         SegmentSize.db_segment_size)
                t.high_segment = segment
                t.first_chunk = record < min(self.deferred_update_points)
                continue
            t.high_segment = None
            t.first_chunk = None
        return duallowed
        
    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        main = self.database_definition
        for d in defer:
            main[d].primary.high_segment = None
            main[d].primary.first_chunk = None

    def start_transaction(self):
        """Do nothing.  Deferred updates are not done within transactions.

        self._dbtxn is assumed to be None.  The txn argument in all bsddb3
        calls in this module is allowed to default to None because these
        updates must be done outside any transactions.
        """


class Primary(primarydu.Primary, dbapi.Primary):

    """Add methods for deferred update processing to dbapi.Primary.

    Primary database updates are done directly, but the existence bitmap
    updates are deferred (along with secondary database updates) until the
    next deferred update point usually when the last record has been added
    to a segment.
    """

    def write_existence_bit_map(self, segment):
        """Write the existence bit map for segment."""
        self.get_existence_bits_database().put(
            segment + 1, self.existence_bit_maps[segment].tobytes())

    def make_cursor(self, dbobject, keyrange):
        raise DBduapiError('make_cursor not implemented')

    # Hack for primarydu.Primary.defer_put
    def _get_existence_bits(self, segment):
        return self.get_existence_bits_database().get(segment + 1)


class Secondary(secondarydu.Secondary, dbapi.Secondary):

    """Add methods for deferred update processing to dbapi.Secondary.

    Secondary database updates are deferred by doing them to a sequence of
    temporary secondary databases, one per deferred update point, followed by
    merging the temporary databases into the secondary databases after all
    updates to the associated primary database are done.
    """

    def defer_put(self, key, *args):
        """Encode key and delegate to superclass."""
        super().defer_put(key.encode(), *args)

    def new_deferred_root(self, dbenv):
        """Make new DB in dbenv for deferred updates and close current one."""
        self.table_connection_list.append(DB(dbenv))
        if len(self.table_connection_list) > 2:
            try:
                self.table_connection_list[-2].close()
            except:
                pass
        try:
            self.table_connection_list[-1].set_flags(DB_DUPSORT)
            self.table_connection_list[-1].open(
                SUBFILE_DELIMITER.join(
                    (str(len(self.table_connection_list) - 1), self._dataname)),
                self._keyvalueset_name,
                DB_HASH if self._fieldatts[ACCESS_METHOD] == HASH else DB_BTREE,
                DB_CREATE)
        except:
            for o in self.table_connection_list[1:]:
                try:
                    o.close()
                except:
                    pass
            self.close()
            raise

    def sort_and_write(self, dbenv, segment):
        """Sort the segment deferred updates before writing to database."""
        gpsb = self.get_primary_segment_bits()
        gpsl = self.get_primary_segment_list()
        gpd = self.get_primary_database()
        note_freed_bits_page = gpd.get_control_secondary().note_freed_bits_page
        note_freed_list_page = gpd.get_control_secondary().note_freed_list_page
        get_freed_bits_page = gpd.get_control_secondary().get_freed_bits_page
        get_freed_list_page = gpd.get_control_secondary().get_freed_list_page

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

        # New records go into temporary databases, one for each segment.
        if gpd.first_chunk:
            self.new_deferred_root(dbenv)

        # The low segment in the import may have to be merged with an existing
        # high segment on the database, or the current segment in the import
        # may be done in chunks of less than a complete segment.
        # Note the difference between this code, and the similar code in modules
        # sqlite3duapi.py and apswduapi.py: the Berkeley DB code updates the
        # main index directly if an entry already exists, but the Sqlite code
        # always updates a temporary table and merges into the main table later.
        # Here cursor_high binds to database (table_connection_list[0]) only if
        # it is the only table.
        cursor_new = self.table_connection_list[-1].cursor()
        try:
            if gpd.high_segment == segment or not gpd.first_chunk:
                if self._fieldatts[ACCESS_METHOD] == HASH:
                    segkeys = tuple(segvalues)
                else:
                    segkeys = sorted(segvalues)
                cursor_high = self.table_connection_list[-1].cursor()
                try:
                    for k in segkeys:

                        # Get high existing segment for value.
                        if not cursor_high.set(k):

                            # No segments for this index value.
                            continue

                        if not cursor_high.next_nodup():
                            v = cursor_high.last()[1]
                        else:
                            v = cursor_high.prev()[1]
                        if segment != int.from_bytes(v[:4], byteorder='big'):

                            # No records exist in high segment for this index
                            # value.
                            continue

                        # len(v)==6 : record (segment, record number) (4b, 2b)
                        # len(v)==10:
                        #     list (segment, count, reference) (4b, 2b, 4b)
                        # len(v)==11:
                        #     bitarray (segment, count, reference (4b, 3b, 4b)
                        # Combined with the value from segvalues the result
                        # will be a list or bitarray.
                        current_segment = self.populate_segment((v[:4], v[4:]))
                        seg = (
                            self.make_segment(k, segment, *segvalues[k]
                                              ) | current_segment).normalize()
                        if len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                            current_count = int.from_bytes(v[4:7], 'big')
                        elif len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                            current_count = int.from_bytes(v[4:6], 'big')
                        else:
                            current_count = 1
                        new_count = segvalues[k][0] + current_count

                        if isinstance(seg, RecordsetSegmentBitarray):
                            if isinstance(current_segment,
                                          RecordsetSegmentList):
                                note_freed_list_page(
                                    int.from_bytes(v[-4:], 'big'))
                                gpsl.delete(int.from_bytes(v[-4:], 'big'))
                                srn_bits = get_freed_bits_page()
                                if srn_bits == 0:
                                    srn_bits = gpsb.append(seg.tobytes())
                                else:
                                    gpsb.put(srn_bits, seg.tobytes())
                                cursor_high.delete()
                                cursor_high.put(
                                    k,
                                    b''.join(
                                        (v[:4],
                                         new_count.to_bytes(
                                             3, byteorder='big'),
                                         srn_bits.to_bytes(
                                             4, byteorder='big'))),
                                    DB_KEYLAST)
                            elif isinstance(current_segment,
                                            RecordsetSegmentInt):
                                srn_bits = get_freed_bits_page()
                                if srn_bits == 0:
                                    srn_bits = gpsb.append(seg.tobytes())
                                else:
                                    gpsb.put(srn_bits, seg.tobytes())
                                cursor_new.put(
                                    k,
                                    b''.join(
                                        (v[:4],
                                         new_count.to_bytes(
                                             3, byteorder='big'),
                                         srn_bits.to_bytes(
                                             4, byteorder='big'))),
                                    DB_KEYLAST)
                            else:
                                gpsb.put(int.from_bytes(v[-4:], 'big'),
                                         seg.tobytes())
                                cursor_high.delete()
                                cursor_high.put(
                                    k,
                                    b''.join(
                                        (v[:4],
                                         new_count.to_bytes(
                                             3, byteorder='big'),
                                         v[-4:])),
                                    DB_KEYLAST)
                        elif isinstance(seg, RecordsetSegmentList):
                            if isinstance(current_segment, RecordsetSegmentInt):
                                srn_list = get_freed_list_page()
                                if srn_list == 0:
                                    srn_list = gpsl.append(seg.tobytes())
                                else:
                                    gpsl.put(srn_list, seg.tobytes())
                                cursor_new.put(
                                    k,
                                    b''.join(
                                        (v[:4],
                                         new_count.to_bytes(
                                             2, byteorder='big'),
                                         srn_list.to_bytes(
                                             4, byteorder='big'))),
                                    DB_KEYLAST)
                            else:
                                gpsl.put(int.from_bytes(v[-4:], 'big'),
                                         seg.tobytes())
                                cursor_high.delete()
                                cursor_high.put(
                                    k,
                                    b''.join(
                                        (v[:4],
                                         new_count.to_bytes(
                                             2, byteorder='big'),
                                         v[-4:])),
                                    DB_KEYLAST)
                        else:
                            raise DBduapiError('Unexpected segment type')

                        # Delete segment so it is not processed again as a new
                        # segment.
                        del segvalues[k]

                finally:
                    cursor_high.close()
                del cursor_high
                del segkeys

            # Add the new segments in segvalues
            segment_bytes = segment.to_bytes(4, byteorder='big')
            if self._fieldatts[ACCESS_METHOD] == HASH:
                segkeys = tuple(segvalues)
            else:
                segkeys = sorted(segvalues)
            ducl = SegmentSize.db_upper_conversion_limit
            for k in segkeys:
                count, records = segvalues[k]
                del segvalues[k]
                if count > ducl:
                    srn_bits = get_freed_bits_page()
                    if srn_bits == 0:
                        srn_bits = gpsb.append(records)
                    else:
                        gpsb.put(srn_bits, records)
                    cursor_new.put(
                        k,
                        b''.join(
                            (segment_bytes,
                             count.to_bytes(3, byteorder='big'),
                             srn_bits.to_bytes(4, byteorder='big'))),
                        DB_KEYLAST)
                elif count > 1:
                    srn_list = get_freed_list_page()
                    if srn_list == 0:
                        srn_list = gpsl.append(records)
                    else:
                        gpsl.put(srn_list, records)
                    cursor_new.put(
                        k,
                        b''.join(
                            (segment_bytes,
                             count.to_bytes(2, byteorder='big'),
                             srn_list.to_bytes(4, byteorder='big'))),
                        DB_KEYLAST)
                else:
                    cursor_new.put(
                        k,
                        b''.join(
                            (segment_bytes,
                             records.to_bytes(2, byteorder='big'))),
                        DB_KEYLAST)

        finally:
            cursor_new.close()
            #self.table_connection_list[-1].close() # multi-chunk segments

        # Flush buffers to avoid 'missing record' exception in populate_segment
        # calls in later multi-chunk updates on same segment.  Not known to be
        # needed generally yet.
        self.get_primary_segment_list().sync()
        self.get_primary_segment_bits().sync()

    def merge(self, dbenv):
        """Merge the segment deferred updates into database."""

        # Any deferred updates?
        if len(self.table_connection_list) == 1:
            return

        # Rename existing index and create new empty one.
        # Open the old and new index, and all the deferred update indexes.
        # Use open_root() to create the new index, but the others must exist.
        f, d = self.table_link.get_dbname()
        self.table_link.close()
        dbenv.dbrename(f, None, newname=SUBFILE_DELIMITER.join(('0', f)))
        dudbc = len(self.table_connection_list) - 1
        self.open_root(dbenv)
        for i in range(dudbc):
            self.table_connection_list.append(DB(dbenv))
            self.table_connection_list[-1].set_flags(DB_DUPSORT)
            self.table_connection_list[-1].open(
                SUBFILE_DELIMITER.join(
                    (str(len(self.table_connection_list) - 1), f)),
                d,
                DB_HASH if self._fieldatts[ACCESS_METHOD] == HASH else DB_BTREE)
        self.table_connection_list.insert(1, DB(dbenv))
        self.table_connection_list[1].set_flags(DB_DUPSORT)
        self.table_connection_list[1].open(
            SUBFILE_DELIMITER.join(('0', f)),
            d,
            DB_HASH if self._fieldatts[ACCESS_METHOD] == HASH else DB_BTREE)

        # Write the entries from the old index and deferred update indexes to
        # the new index in sort order: otherwise might as well have written the
        # index entries direct to the old index rather than to the deferred
        # update indexes.
        # Assume at least 65536 records in each index. (segment_sort_scale)
        # But OS ought to make the buffering done here a waste of time.
        db_deferred = self.table_connection_list[1:]
        db_buffers = []
        db_cursors = []
        for dbo in db_deferred:
            db_buffers.append(collections.deque())
            db_cursors.append(dbo.cursor())
        try:
            length_limit = int(
                SegmentSize.segment_sort_scale // max(1, len(db_buffers)))
            for e, buffer in enumerate(db_buffers):
                c = db_cursors[e]
                while len(buffer) < length_limit:
                    r = c.next()
                    buffer.append(r)
                try:
                    while buffer[-1] is None:
                        buffer.pop()
                except IndexError:
                    c.close()
                    db_cursors[e] = None
                    f, d = db_deferred[e].get_dbname()
                    db_deferred[e].close()
                    dbenv.dbremove(f)
                    del f, d
                del buffer
                del c
                del r
            updates = []
            heapq.heapify(updates)
            heappop = heapq.heappop
            heappush = heapq.heappush
            for e, buffer in enumerate(db_buffers):
                if buffer:
                    heappush(updates, (buffer.popleft(), e))
            cursor = self.table_link.cursor()
            try:
                while len(updates):
                    record, e = heappop(updates)
                    cursor.put(record[0], record[1], DB_KEYLAST)
                    buffer = db_buffers[e]
                    if not buffer:
                        c = db_cursors[e]
                        if c is None:
                            continue
                        while len(buffer) < length_limit:
                            r = c.next()
                            buffer.append(r)
                        try:
                            while buffer[-1] is None:
                                buffer.pop()
                        except IndexError:
                            c.close()
                            db_cursors[e] = None
                            f, d = db_deferred[e].get_dbname()
                            db_deferred[e].close()
                            dbenv.dbremove(f)
                            del f, d
                            continue
                        del c
                        del r
                    heappush(updates, (buffer.popleft(), e))
            finally:
                cursor.close()
        finally:
            for c in db_cursors:
                if c:
                    c.close()

    def make_cursor(self, dbobject, keyrange):
        raise DBduapiError('make_cursor not implemented')
