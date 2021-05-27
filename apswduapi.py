# apswduapi.py
# Copyright (c) 2015 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide sqlite3 database access methods compatible with DPT single-step.

The compatibility provided is doing large updates in a separate process.

The sqlite3 equivalent to any form of DPT deferred update is drop indexes
before the update and create the indexes after the update.

See www.dptoolkit.com for details of DPT

List of classes

Sqlite3duapiError - Exceptions
Sqlite3bitduapi - database definition, bit-mapped record numbers
Sqlite3bitduPrimary - record level access, bit-mapped record numbers
Sqlite3bitduSecondary - record level access, bit-mapped record numbers

"""

import apsw
from collections import deque

from .api.bytebit import Bitarray
from .apswapi import (
    _Sqlite3api,
    Sqlite3apiError,
    Sqlite3bitPrimary,
    Sqlite3bitSecondary,
    Sqlite3bitSecondaryFile,
    EMPTY_BITARRAY,
    Sqlite3bitControlFile,
    )
from .api.constants import (
    USE_BYTES,
    SQLITE_SEGMENT_COLUMN,
    DB_SEGMENT_SIZE,
    DB_TOP_RECORD_NUMBER_IN_SEGMENT,
    PRIMARY,
    SECONDARY,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_VALUE_COLUMN,
    DB_CONVERSION_LIMIT,
    )


class Sqlite3duapiError(Sqlite3apiError):
    pass


class Sqlite3bitduapi(_Sqlite3api):
    
    """Support sqlite3 equivalent for DPT single-step deferred updates.

    Extend and override _Sqlite3api methods for update.

    Methods added:

    do_deferred_updates
    do_segment_deferred_updates
    _get_deferable_update_files
    set_defer_update
    unset_defer_update

    Methods overridden:

    make_cursor - raise exception
    put_instance
    use_deferred_update_process - raise exception

    Methods extended:

    __init__
    close_context
    open_context
    
    """

    def __init__(self, sqlite3tables, *args, **kargs):
        """Define database structure.  See superclass for *args and **kargs."""
        super(Sqlite3bitduapi, self).__init__(
            Sqlite3bitduPrimary,
            Sqlite3bitduSecondary,
            sqlite3tables,
            *args,
            **kargs)
        self._control = Sqlite3bitControlFile()

        # Hackish, but added because the indexes cannot be dropped until the
        # high segment at start of update run has been filled.
        # This is a solution to the problem mentioned in set_defer_update()
        # comments.  The cost of dropping and re-creating indexes, when more
        # expensive than not doing so, will not be much for small databases.
        # Hopefully the same situation will not arise for very large databases:
        # it is not clear what is 'very large' compared to ~100000, which is
        # about where dropping and re-creating kicks in at present.
        self._du_file_defs = None
        self._du_drop_indexes = None
        
        # Refer to primary from secondary for access to segment databases
        # Link each primary to control file for segment management
        m = self._sqtables
        for n in sqlite3tables:
            m[sqlite3tables[n][PRIMARY]].set_control_database(self._control)
            for k, v in sqlite3tables[n][SECONDARY].items():
                m[sqlite3tables.field_name(k)
                  if v is None else v].set_primary_database(
                    m[sqlite3tables[n][PRIMARY]])

    def close_context(self):
        """Close main and deferred update databases and environment."""
        self._control.close()
        super(Sqlite3bitduapi, self).close_context()

    def open_context(self):
        """Open all DBs."""
        super(Sqlite3bitduapi, self).open_context()
        self._control.open_root(self._sqconn)
        return True
            
    # This method is uncommented if deferred updates are done without a journal
    # and without synchronous updates.  See pragmas in set_defer_update and
    # unset_defer_update methods.
    #def commit(self):
    #    """Override superclass method to do nothing."""

    def make_cursor(self, dbname):
        raise Sqlite3duapiError('make_cursor not implemented')

    def use_deferred_update_process(self):
        raise Sqlite3duapiError('Query use of du when in deferred update mode')

    def set_defer_update(self, db=None, duallowed=False):

        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        self._du_file_defs = defer

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
        #self._sqconn,cursor().execute('pragma journal_mode = off')
        #self._sqconn.cursor().execute('pragma synchronous = off')

        self.start_transaction()
        for d in defer:
            t = self._sqtables[self._associate[d][d]]
            t.low_record_number_in_segment = None
            t.low_segment = None
            t.high_rowid_at_du_start = self._sqconn.cursor().execute(
                ' '.join((
                    'select max(rowid) from',
                    t._fd_name,
                    ))).fetchone()[0]
            if t.high_rowid_at_du_start is not None:
                if t.high_rowid_at_du_start % DB_SEGMENT_SIZE:
                    continue
            for s in self._associate[d]:
                if s != d:
                    self._sqconn.cursor().execute(
                        ' '.join((
                            'drop index if exists',
                            t._indexname,
                            )))
                

    def unset_defer_update(self, db=None):
        """Tidy-up at end of deferred update run."""

        defer = self._get_deferable_update_files(db)
        if not defer:
            return

        # Create indexes if they were dropped in do_deferred_updates(),
        # do_segment_deferred_updates(), or set_defer_update().
        for d in defer:
            for s in self._associate[d]:
                if s != d:
                    t = self._sqtables[self._associate[d][s]]
                    self._sqconn.cursor().execute(
                        ' '.join((
                            'create unique index if not exists', t._indexname,
                            'on', t._fd_name,
                            '(',
                            t._fd_name, ',',
                            SQLITE_SEGMENT_COLUMN,
                            ')',
                            )))

        # See comment in set_defer_update method.

        # Now this class' commit() is called neither comment nor uncomment.
        # Uncomment this if the 'do-nothing' override of commit() is commented.
        #self._sqconn.commit()
        self.commit()
        
        # Comment these if the 'do-nothing' override of commit() is commented.
        #self._sqconn.cursor().execute('pragma journal_mode = delete')
        #self._sqconn.cursor().execute('pragma synchronous = full')

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are DB_RECNO and enough
        memory is available to do a segemnt at a time.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables
        primarydb = main[db[dbset]]

        if putkey != 0:
            # reuse record number is not allowed
            raise Sqlite3duapiError(
                'Cannot reuse record number in deferred update.')
        key = primarydb.put(putkey, instance.srvalue)
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)

        srindex = instance.srindex
        segment, record_number = divmod(putkey, DB_SEGMENT_SIZE)
        try:
            if record_number < primarydb.low_record_number_in_segment:
                primarydb.low_record_number_in_segment = record_number
                if primarydb.low_segment is None:
                    primarydb.low_segment = segment
        except TypeError:
            primarydb.low_record_number_in_segment = record_number
            primarydb.low_segment = segment
        primarydb.defer_put(segment, record_number)
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].defer_put(v, segment, record_number)
        if record_number == DB_TOP_RECORD_NUMBER_IN_SEGMENT:
            self.do_segment_deferred_updates(main, main[db[dbset]], segment)

    def do_segment_deferred_updates(self, main, dbassoc, segment):
        """Do deferred updates for segment filled during run."""
        defer = self._get_deferable_update_files(self._du_file_defs)
        if not defer:
            return
        dbassoc.write_existence_bit_map(segment)

        # Drop indexes before first update after the high segment at start of
        # run has been filled, but after it is known there will be a further
        # update, of at least one full segment.
        if self._du_drop_indexes is True:
            for d in defer:
                t = self._sqtables[self._associate[d][d]]
                for s in self._associate[d]:
                    if s != d:
                        self._sqconn.cursor().execute(
                            ' '.join((
                                'drop index if exists',
                                t._indexname,
                                )))
            self._du_drop_indexes = False

        secondaries = {m for m in main
                       if isinstance(main[m], Sqlite3bitSecondaryFile)}
        for d in defer:
            for s in self._associate[d]:
                secondary = self._associate[d][s]
                if secondary not in secondaries:
                    continue
                main[secondary].sort_and_write(segment)

        # The next call of do_segment_deferred_updates(), if any, will
        # drop the indexes.
        # Indexes will not be dropped and re-created unless at least 65537
        # records are added, and is certain to happen only if more than
        # 131072 records are added.
        if self._du_drop_indexes is None:
            self._du_drop_indexes = True

    def do_deferred_updates(self, db=None):
        """Do deferred updates for partially filled final segment."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return

        # Write the final deferred segment database for each index
        main = self._sqtables
        for d in defer:
            assoc = self._associate[d]
            primary = main[assoc[d]]
            statement = ' '.join((
                'select',
                primary._fd_name,
                'from',
                primary._fd_name,
                'order by',
                primary._fd_name, 'desc',
                'limit 1',
                ))
            values = ()
            try:
                segment, record_number = divmod(
                    primary._connection.cursor().execute(
                        statement, values).fetchone()[0],
                    DB_SEGMENT_SIZE)
                if record_number == DB_TOP_RECORD_NUMBER_IN_SEGMENT:
                    continue # Assume put_instance did deferred updates
            except TypeError:
                # Assume fetchone() reurned None (empty file)
                continue
            primary.write_existence_bit_map(segment)
            for s in assoc:
                f = main[assoc[s]]
                if not f._primary:
                    f.sort_and_write(segment)

    def _get_deferable_update_files(self, db):
        """Return dictionary of databases in db whose updates are deferable."""
        deferable = False
        for d in self._sqtables:
            if not self._sqtables[d]._primary:
                deferable = True
                break
        if not deferable:
            return False
        
        if isinstance(db, str):
            db = [db]
        elif not isinstance(db, (list, tuple, dict)):
            db = list(self._associate.keys())
        dbadd = dict()
        for d in db:
            if d in self._associate:
                dbadd[d] = []
                for s in self._associate[d]:
                    if s != d:
                        dbadd[d].append(self._associate[d][s])
        return dbadd


class Sqlite3bitduPrimary(Sqlite3bitPrimary):

    """Provide sqlite equivqlent for DPT single-step deferred update.

    This class disables methods not appropriate to deferred update.

    Methods added:

    defer_put
    write_existence_bit_map

    Methods overridden:

    make_cursor - not supported by this class.

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Define a Sqlite3 file in deferred update mode"""
        super(Sqlite3bitduPrimary, self).__init__(*args)
        self.existence_bit_maps = dict()
        self.low_record_number_in_segment = None
        self.low_segment = None
        self.high_rowid_at_du_start = None

    def defer_put(self, segment, record_number):
        """Add bit to existence bit map for new record and defer update."""
        try:
            # Assume cached segment existence bit map exists
            self.existence_bit_maps[segment][record_number] = True
        except KeyError:
            # Get the segment existence bit map from database
            ebmb = self.get_existence_bits().get(segment + 1)
            if ebmb is None:
                # It does not exist so create a new empty one
                ebm = EMPTY_BITARRAY.copy()
            else:
                # It does exist so convert database representation to bitarray
                ebm = Bitarray()
                ebm.frombytes(ebmb)
            # Set bit for record number and add segment to cache
            ebm[record_number] = True
            self.existence_bit_maps[segment] = ebm

    def write_existence_bit_map(self, segment):
        """Write the existence bit map for segment."""
        statement = ' '.join((
            'insert or replace into',
            self.get_existence_bits()._seg_dbfile,
            '(',
            self.get_existence_bits()._seg_dbfile, ',',
            SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? , ? )',
            ))
        values = (segment + 1, self.existence_bit_maps[segment].tobytes())
        self.get_existence_bits(
            )._seg_object.cursor().execute(statement, values)

    def make_cursor(self, dbname):
        raise Sqlite3duapiError('make_cursor not implemented')


class Sqlite3bitduSecondary(Sqlite3bitSecondary):

    """Provide sqlite equivqlent for DPT single-step deferred update.

    This class disables methods not appropriate to deferred update.

    Methods added:

    defer_put
    sort_and_write

    Methods overridden:

    make_cursor - not supported by this class.

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Define a Sqlite3 secondary table in deferred update mode"""
        super(Sqlite3bitduSecondary, self).__init__(*args)
        self.values = dict()

    def make_cursor(self, dbname):
        raise Sqlite3duapiError('make_cursor not implemented')
    
    def defer_put(self, key, segment, record_number):
        """Add record_number to cached segment for key."""
        values = self.values.get(key)
        if values is None:
            self.values[key] = record_number
        elif isinstance(values, int):
            self.values[key] = [values]
            self.values[key].append(record_number)
        elif isinstance(values, list):
            values.append(record_number)
            if len(values) > DB_CONVERSION_LIMIT:
                v = self.values[key] = EMPTY_BITARRAY.copy()
                for rn in values:
                    v[rn] = True
                v[record_number] = True
        else:
            values[record_number] = True

    def sort_and_write(self, segment):
        """Sort the segment deferred updates before writing to database.

        Index updates are serialized as much as practical: meaning the lists
        or bitmaps of record numbers are put in a subsidiary table and the
        tables are written one after the other.

        """
        gpd = self.get_primary_database()
        lowvalues = deque()
        newvalues = deque()

        # The additional records may have to be spliced to an existing segment
        # if the first record is not the first of a segment and the segment is
        # the low segment being processed.
        # The false positive which occurs on an empty database because sqlite
        # counts rows from 1, not 0, is allowed to cause some wasted work.
        low = gpd.low_record_number_in_segment and gpd.low_segment == segment

        # select (index value, segment number, record count, key reference)
        # statement for (index value, segment number).  Execution returns None
        # if no splicing needed.
        select_existing_segment = ' '.join((
            'select',
            self._fd_name, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            'from',
            self._fd_name,
            'where',
            self._fd_name, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Update (record count) statement for (index value, segment number) used
        # when splicing needed.
        update_record_count = ' '.join((
            'update',
            self._fd_name,
            'set',
            SQLITE_COUNT_COLUMN, '= ?',
            'where',
            self._fd_name, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Update (record count, key reference) statement
        # for (index value, segment number) used when record count increased
        # from 1.
        update_count_and_reference = ' '.join((
            'update',
            self._fd_name,
            'set',
            SQLITE_COUNT_COLUMN, '= ? ,',
            self._primaryname, '= ?',
            'where',
            self._fd_name, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # insert (index value, segment number, record count, key reference)
        # statement.
        insert_new_segment = ' '.join((
            'insert into',
            self._fd_name,
            '(',
            self._fd_name, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            ')',
            'values ( ? , ? , ? , ? )',
            ))

        segvalues = self.values

        # Wrap the record number lists in an appropriate Segment class.
        for k in segvalues:
            v = segvalues[k]
            if isinstance(v, list):
                segvalues[k] = [
                    segment,
                    len(v),
                    b''.join([n.to_bytes(2, byteorder='big') for n in v]),
                    ]
            elif isinstance(v, Bitarray):
                segvalues[k] = [
                    segment,
                    v.count(),
                    v.tobytes(),
                    ]
            elif isinstance(v, int):
                segvalues[k] = [segment, 1, v]

        # The low segment in the import may have to be merged with an
        # existing high segment on the database.
        # These segments are cached in lowvalues and the record counts are
        # updated on the existing segment records.
        if low:
            for k in sorted(segvalues):
                values = (k, segment)
                s = self._connection.cursor().execute(
                    select_existing_segment, values).fetchone()
                if s is not None:
                    lowvalues.append((k, (self.populate_segment(s), s)))
                    values = (segvalues[k][1] + s[2], k, segment)
                    self._connection.cursor(
                        ).execute(update_record_count, values)

            # If the existing segment record for a segment in lowvalues had a
            # record count > 1 before being updated, a subsidiary table record
            # already exists.  Otherwise it must be created.
            # Key reference is a record number if the record count is 1.

            # Deal with the segments which already exist, and need splicing.
            while len(lowvalues):
                k, v = lowvalues.popleft()
                if v[1][2] > 1:
                    seg = self.make_segment(k, *segvalues[k]) | v[0]
                    seg = seg.normalize()
                    gpd.set_segment_records((seg.tobytes(), v[1][3]))
                    del segvalues[k]
                else:
                    newvalues.append((k, v))

            # Deal with segments which now need a subsidiary table row because
            # record count has increased from 1.
            while len(newvalues):
                k, v = newvalues.popleft()
                seg = self.make_segment(k, *segvalues[k]) | v[0]
                seg = seg.normalize()
                nv = gpd.insert_segment_records((seg.tobytes(),))
                self._connection.cursor().execute(
                    update_count_and_reference,
                    (v[1][2]+segvalues[k][1], nv, k, v[1][1]))
                del segvalues[k]

        # Process segments which do not need to be spliced.
        # This includes any not dealt with by low segment processing.

        ssv = sorted(segvalues)

        # Insert new record lists in subsidiary table and note rowids.
        # Modify the index record values to refer to the rowid if necessary.
        for k in ssv:
            v = segvalues[k]
            if v[1] > 1:
                v[2] = gpd.insert_segment_records((v[2],))

        # Insert new index records.
        self._connection.cursor(
            ).executemany(insert_new_segment, self._rows(ssv))
        segvalues.clear()

    def _rows(self, ssv):
        """Helper method to avoid len(ssv) ~.execute() calls."""
        segvalues = self.values
        for k in ssv:
            v = segvalues[k]
            yield (k, v[0], v[1], v[2])
