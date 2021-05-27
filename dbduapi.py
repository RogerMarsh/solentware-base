# dbduapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide DB file access in custom deferred update mode.

List of classes

DBduapiError - Exceptions
DBduapi - Deferred update API without file segment support
DBbitduapi - Deferred update API with file segment support
DBbitduPrimary
DBbitduSecondary
_DBduapi - Methods common to DBduapi and DBbitduapi including _DBapi overrides
_DBduSecondary
DBduSecondary - Deferred update record level access to each secondary file

"""

import os
import heapq

from .api.bytebit import Bitarray

# bsddb removed from Python 3.n
try:
    from bsddb3.db import DB, DB_KEYLAST, DB_RECNO, DBKeyExistError
except ImportError:
    from bsddb.db import DB, DB_KEYLAST, DB_RECNO, DBKeyExistError

from .dbapi import (
    _DBapi,
    DBPrimary,
    DBbitPrimary,
    DBSecondary,
    DBapiError,
    EMPTY_BITARRAY,
    DBbitControlFile,
    )

from .api.shelf import Shelf, ShelfString, DEFAULT_SEGMENTSIZE
from .api.constants import (
    USE_BYTES,
    DB_DEFER_FOLDER,
    DB_SEGMENT_SIZE,
    DB_CONVERSION_LIMIT,
    DB_TOP_RECORD_NUMBER_IN_SEGMENT,
    SECONDARY,
    PRIMARY,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    )


class DBduapiError(DBapiError):
    pass


class _DBduapi(object):
    
    """Methods common to DBduapi and DBbitduapi.

    Methods added:

    get_deferred_update_folder
    reset_defer_limit
    set_defer_limit
    _get_deferable_update_files

    Methods overridden:

    delete_instance - raise exception
    do_deferred_updates
    edit_instance - raise exception
    make_cursor - raise exception
    put_instance
    use_deferred_update_process - raise exception
    set_defer_update
    unset_defer_update

    Methods extended:

    None
    
    """
    
    # Number of records that can be collected for deferred update before
    # applying to file. Depends on memory available.
    # Call set_defer_limit to set an appropriate value for each file.
    _defer_record_limit = DEFAULT_SEGMENTSIZE

    def do_deferred_updates(self):
        """Do deferred updates for DBapi."""

        secondaries = [m for m in self._main if not self._main[m].is_primary()]

        for m in secondaries:
            if self._main[m].deferclass is not None:
                if len(self._main[m].deferclass.deferbuffer):
                    self._main[m].sort_and_write()

        for m in secondaries:
            if self._main[m].deferclass is not None:
                #self._main[m].dump_secondary()
                pass

        for m in secondaries:
            if self._main[m].deferclass is not None:
                #self._main[m].new_secondary(
                #    self._dbenv, self.get_database_folder())
                pass

        for m in secondaries:
            if self._main[m].deferclass is not None:
                self._main[m].merge_update()

        for m in secondaries:
            if self._main[m].deferclass is not None:
                self._main[m].tidy_up_after_merge_update(
                    self.get_database_folder(), self._deferfolder)

        for m in secondaries:
            if self._main[m].deferclass is not None:
                self._main[m].close()

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        Puts may be direct or deferred while callbacks handle subsidiary
        databases and non-standard inverted indexes.  Deferred updates are
        controlled by counting the calls to put_instance and comparing with
        self._defer_record_limit.
        
        """
        putkey = instance.key.pack()
        dodurecno = self._defer_record_count >= _DBduapi._defer_record_limit
        if dodurecno:
            self._defer_record_count = 0
        self._defer_record_count += 1
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main

        key = main[db[dbset]].put(putkey, instance.srvalue.encode())
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)
        convertedkey = putkey

        srindex = instance.srindex
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            if main[db[secondary]].deferclass is not None:
                if dodurecno:
                    main[db[secondary]].sort_and_write()
                for v in srindex[secondary]:
                    main[db[secondary]].defer_put(v.encode(), convertedkey)
            else:
                for v in srindex[secondary]:
                    main[db[secondary]].put(v.encode(), convertedkey)

    def reset_defer_limit(self):
        """Set defer record limit to default class limit"""
        self.set_defer_limit(DBapi._defer_record_limit)

    def set_defer_limit(self, limit):
        """Set defer record limit."""
        self._defer_record_limit = limit

    def set_defer_update(self, db=None, duallowed=False):
        """Set deferred update for db DBs and return duallowed. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return duallowed

        try:
            os.mkdir(os.path.join(
                self.get_database_folder(), self._deferfolder))
        except:
            msg = ' '.join((
                'Create defer update folder',
                ' '.join((self.get_database_folder(), self._deferfolder)),
                'fails'))
            # raise hangs the process (leave dbdefer directory in existence)
            raise DBapiError(msg)

        for d in defer:
            if self._main[self._associate[d][d]].is_recno():
                defaultshelf = Shelf
            else:
                defaultshelf = ShelfString
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    if f.deferclass is None:
                        f.deferclass = defaultshelf()
                        deferfolder = os.path.join(
                            self.get_database_folder(),
                            self._deferfolder,
                            f.get_database_file())
                        try:
                            os.mkdir(deferfolder)
                        except:
                            msg = ' '.join((
                                'Create defer update folder',
                                deferfolder,
                                'fails'))
                            raise DBapiError(msg)
                        f.deferclass.set_defer_folder(deferfolder)
        return duallowed
            
    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        for d in self._associate:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    if f.deferclass is not None:
                        try:
                            os.rmdir(os.path.join(
                                self.get_database_folder(),
                                self._deferfolder,
                                f.get_database_file()))
                        except:
                            pass
                        f.deferclass = None
        try:
            os.rmdir(os.path.join(
                self.get_database_folder(),
                self._deferfolder))
        except:
            pass

    def _get_deferable_update_files(self, db):
        """Return dictionary of databases in db whose updates are deferable."""
        deferable = False
        for d in self._main:
            if not self._main[d].is_primary():
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
            
    def delete_instance(self, dbset, instance):
        raise DBduapiError('delete_instance not implemented')

    def edit_instance(self, dbset, instance):
        raise DBduapiError('edit_instance not implemented')

    def make_cursor(self, dbset, dbname, keyrange=None):
        raise DBduapiError('make_cursor not implemented')

    def use_deferred_update_process(self, **kargs):
        raise DBduapiError('Query use of du when in deferred update mode')

    def get_deferred_update_folder(self):
        """return deferred database update folder name"""
        return self._deferfolder


class DBduapi(_DBduapi, _DBapi):
    
    """Support custom deferred updates on DB database.

    Extend and override DBapi methods for custom deferred update.
    __init__ is extended to provide control structures for deferred update.
    DB does not support edit and delete operations in deferred update so
    edit_instance and delete_instance methods raise exceptions.
    This class is not intended for general processing so the make_cursor
    method raises an exception.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args, deferfolder=None, **kargs):
        """Define database structure.  See superclass for *args and **kargs.

        deferfolder = folder for deferred updates (default DB_DEFER_FOLDER)
        
        """
        super(DBduapi, self).__init__(DBPrimary, DBduSecondary, *args, **kargs)
        
        if deferfolder is None:
            deferfolder = DB_DEFER_FOLDER
        
        # Name of deferred update DB folder
        self._deferfolder = deferfolder
        
        # Count of records with deferred updates pending.
        self._defer_record_count = 0


class _DBduSecondary(DBSecondary):

    """Provide custom deferred update sort processing for DB file.

    This class creates attributes added for deferred update.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Define a DB file in deferred update mode"""
        super(_DBduSecondary, self).__init__(*args)

        self._defercount = 0
        self.deferclass = None


class DBduSecondary(_DBduSecondary):

    """Provide custom deferred update sort processing for DB file.

    This class disables methods not appropriate to deferred update.

    Methods added:

    defer_put
    dump_secondary
    merge_update
    new_secondary
    put_deferred
    sort_and_write
    tidy_up_after_merge_update

    Methods overridden:

    delete - not implemented in DB for deferred updates.
    replace - not implemented in DB for deferred updates.
    make_cursor - not supported by this class.

    Methods extended:

    None
    
    """
    
    def defer_put(self, key, value):
        """Write key and value to sequential file for database."""
        self.deferclass.defer_put(key, value)

    def dump_secondary(self):
        """Copy existing secondary db to sequential file."""
        c = self._object.cursor()
        r = c.first()
        try:
            current_segment = (
                self.decode_record_number(r[1]) // DEFAULT_SEGMENTSIZE)
        except:
            current_segment = None
        while r:
            k, v = r
            vi = self.decode_record_number(v)
            s = vi // DEFAULT_SEGMENTSIZE
            if s != current_segment:
                self.deferclass.sort_index()
                current_segment = s
            self.deferclass.defer_put(k, vi)
            r = c.next()
        if current_segment is not None:
            self.deferclass.sort_index()
        c.close()

    def merge_update(self):
        """Do merge updates to database file from sources.
        
        The sources are assumed to be sorted.  Function insort from
        module bisect may be a better alternative to the functions
        from module heapq.
        
        """
        self.deferclass.flush_index(self.put_deferred)

    def new_secondary(self, dbenv, home):
        """Delete secondary DB and open a new one in same environment."""
        self.close()
        db = DB()
        db.remove(os.path.join(
            home,
            self.get_database_file()))
        del db
        self.open_root(dbenv)

    def put_deferred(self, key, value):
        """Put (key, value) on database.
        
        The cursor put method is used because updating secondary DB.
        value is still the integer version of primary key for recno dbs
        
        """
        if self.is_value_recno():
            try:
                self._object.cursor().put(
                    key, self.encode_record_number(value), DB_KEYLAST)
            except DBKeyExistError:
                # Application may legitimately do duplicate updates (-30996)
                # to a sorted secondary database for DPT compatibility.
                pass
            except:
                raise
        else:
            self._object.cursor().put(key, value, DB_KEYLAST)

    def sort_and_write(self):
        """Sort the deferred updates before writing to sequential file."""
        self.deferclass.sort_index()

    def tidy_up_after_merge_update(self, home, deferfolder):
        """Delete defer update files and control records."""
        self.deferclass.delete_shelve()
        folder = os.path.join(
            home,
            deferfolder,
            self.get_database_file())
        paths = os.listdir(folder)
        for p in paths:
            try:
                os.remove(os.path.join(folder, p))
            except:
                pass

    def delete(self, key, value):
        raise DBduapiError('delete not implemented')

    def replace(self, key, oldvalue, newvalue):
        raise DBduapiError('replace not implemented')

    def make_cursor(self, dbobject, keyrange):
        raise DBduapiError('make_cursor not implemented')


class DBbitduapi(_DBduapi, _DBapi):
    
    """Support custom deferred updates on DB database.

    Extend and override DBapi methods for custom deferred update.
    __init__ is extended to provide control structures for deferred update.
    DB does not support edit and delete operations in deferred update so
    edit_instance and delete_instance methods raise exceptions.
    This class is not intended for general processing so the make_cursor
    method raises an exception.

    Methods added:

    do_segment_deferred_updates

    Methods overridden:

    delete_instance - raise exception
    do_deferred_updates
    edit_instance - raise exception
    make_cursor - raise exception
    put_instance
    use_deferred_update_process - raise exception
    set_defer_update
    unset_defer_update

    Methods extended:

    __init__
    close_context
    open_context
    
    """

    def __init__(self, DBnames, *args, deferfolder=None, **kargs):
        """Define database structure.  See superclass for *args and **kargs.

        deferfolder = folder for deferred updates (default DB_DEFER_FOLDER)
        
        """
        super(DBbitduapi, self).__init__(
            DBbitduPrimary, DBbitduSecondary, DBnames, *args, **kargs)
        self._control = DBbitControlFile()
        for n in DBnames:
            self._main[DBnames[n][PRIMARY]].set_control_database(self._control)
            # Segment database updates are done in do_segment_deferred_updates
            # and do_deferred_updates, not in the temporary secondary databases
            # that collect the index values.  No need to link these for access
            # to segment databases like in non-deferred updates.
            for s in DBnames[n][SECONDARY].values():
                self._main[s].set_primary_database(
                    self._main[DBnames[n][PRIMARY]])
        
        if deferfolder is None:
            deferfolder = DB_DEFER_FOLDER
        
        # Name of deferred update DB folder
        self._deferfolder = deferfolder
        
        # Database definition to create temporary deferred update secondaries
        self._dbnames = DBnames

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are DB_RECNO and enough
        memory is available to do a segemnt at a time.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main

        if putkey != 0:
            # reuse record number is not allowed
            raise DBduapiError('Cannot reuse record number in deferred update.')
        key = main[db[dbset]].put(putkey, instance.srvalue.encode())
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)

        srindex = instance.srindex
        segment, record_number = divmod(putkey, DB_SEGMENT_SIZE)
        main[db[dbset]].defer_put(segment, record_number)
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].defer_put(
                    v.encode(), segment, record_number)
        if record_number == DB_TOP_RECORD_NUMBER_IN_SEGMENT:
            self.do_segment_deferred_updates(main, main[db[dbset]], segment)

    def do_segment_deferred_updates(self, main, dbassoc, segment):
        """Do deferred updates for segment filled during run."""
        dbassoc.write_existence_bit_map(segment)
        defer = self._get_deferable_update_files(None)
        if not defer:
            return
        secondaries = {m for m in main if not main[m].is_primary()}
        for d in defer:
            for s in self._associate[d]:
                secondary = self._associate[d][s]
                if secondary not in secondaries:
                    continue
                df = DBbitduSecondary(
                    os.path.join(
                        self._deferfolder,
                        main[secondary].get_database_file(),
                        str(segment)),
                    self._dbnames[d],
                    self._dbnames[d][SECONDARY][s])
                df.open_root(self._dbenv)
                df.sort_and_write(
                    segment,
                    dbassoc.get_segment_list_database(),
                    dbassoc.get_segment_bits_database(),
                    main[secondary].values,
                    dbassoc.get_control_secondary(),
                    )
                df.close()

    def set_defer_update(self, db=None, duallowed=False):
        """Set deferred update for db DBs and return duallowed. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return duallowed

        try:
            os.mkdir(os.path.join(
                self.get_database_folder(), self._deferfolder))
        except:
            msg = ' '.join((
                'Create defer update folder',
                ' '.join((self.get_database_folder(), self._deferfolder)),
                'fails'))
            # raise hangs the process (leave dbdefer directory in existence)
            raise DBapiError(msg)

        for d in defer:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    deferfolder = os.path.join(
                        self.get_database_folder(),
                        self._deferfolder,
                        f.get_database_file())
                    try:
                        os.mkdir(deferfolder)
                    except:
                        msg = ' '.join((
                            'Create defer update folder',
                            deferfolder,
                            'fails'))
                        raise DBapiError(msg)
        return duallowed

    def do_deferred_updates(self, db=None):
        """Do deferred updates for partially filled final segment."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return

        # Write the final deferred segment database for each index
        for d in defer:
            assoc = self._associate[d]
            segment, record_number = divmod(
                self._main[assoc[d]]._object.cursor().last()[0],
                DB_SEGMENT_SIZE)
            if record_number == DB_TOP_RECORD_NUMBER_IN_SEGMENT:
                continue # Assume the call in put_instance did deferred updates
            self._main[assoc[d]].write_existence_bit_map(segment)
            for s in assoc:
                f = self._main[assoc[s]]
                if not f.is_primary():
                    df = DBbitduSecondary(
                        os.path.join(
                            self._deferfolder,
                            f.get_database_file(),
                            str(segment)),
                        self._dbnames[d],
                        self._dbnames[d][SECONDARY][s])
                    df.open_root(self._dbenv)
                    df.sort_and_write(
                        segment,
                        self._main[assoc[d]].get_segment_list_database(),
                        self._main[assoc[d]].get_segment_bits_database(),
                        f.values,
                        self._main[assoc[d]].get_control_secondary(),
                        )
                    df.close()
        
        # Move index databases to deferred update folder and create empty ones
        for d in defer:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    f.close()
                    old_name = os.path.join(
                        self.get_database_folder(),
                        f.get_database_file())
                    new_name = os.path.join(
                        self.get_database_folder(),
                        self._deferfolder,
                        f.get_database_file(),
                        f.get_database_file())
                    os.rename(old_name, new_name)
        
        # Update the new empty index databases from the deferred update folder
        def do_merge():
            """Merge dv nv segments into new and write then return new dv."""
            # In general multiple segments for a key should be collected and
            # merged once. This implementation assumes sufficient memory is
            # available to build all indexes for a segment without using swap
            # space.
            # same index value and segment number so merge
            # fittest is to combine dv and nv, bind to dv, and
            # defer put to next pass round loop
            # for now ignore earlier entry
            #f._object.cursor().put(dk, dv, DB_KEYLAST)
            if len(dv) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                # both segments could be bitarray
                newseg = get_bits_segment(dv)
                oldcount = newseg.count()
                if len(nv) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    newseg |= get_bits_segment(nv)
                elif len(nv) == LENGTH_SEGMENT_LIST_REFERENCE:
                    for r in get_list_segment(nv):
                        newseg[r] = True
                else:
                    newseg[get_recnum_segment(nv)] = True
                count = newseg.count()
            elif len(nv) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                # the other segment is not bitarray
                newseg = get_bits_segment(nv)
                oldcount = newseg.count()
                if len(dv) == LENGTH_SEGMENT_LIST_REFERENCE:
                    for r in get_list_segment(nv):
                        newseg[r] = True
                else:
                    newseg[get_recnum_segment(nv)] = True
                count = newseg.count()
            elif len(dv) == LENGTH_SEGMENT_LIST_REFERENCE:
                # both segments could be list but not bitarray
                newseg = get_list_segment(dv)
                oldcount = len(newseg)
                if len(nv) == LENGTH_SEGMENT_LIST_REFERENCE:
                    newseg.extend(get_list_segment(nv))
                else:
                    newseg.extend(get_recnum_segment(nv))
                if len(newseg) > DB_CONVERSION_LIMIT:
                    seg = EMPTY_BITARRAY.copy()
                    for rn in newseg:
                        seg[rn] = True
                    newseg = seg
                    count = newseg.count()
                else:
                    count = len(newseg)
            elif len(nv) == LENGTH_SEGMENT_LIST_REFERENCE:
                # the other segment is record number
                newseg = get_list_segment(nv)
                oldcount = len(newseg)
                newseg.extend(get_recnum_segment(dv))
                if len(newseg) > DB_CONVERSION_LIMIT:
                    seg = EMPTY_BITARRAY.copy()
                    for rn in newseg:
                        seg[rn] = True
                    newseg = seg
                    count = newseg.count()
                else:
                    count = len(newseg)
            else:
                # both segments are record number
                # assume DB_CONVERSION_LIMIT > 1
                newseg = get_recnum_segment(dv)
                oldcount = len(newseg)
                newseg.extend(get_recnum_segment(dv))
                count = len(newseg)
            if count > DB_CONVERSION_LIMIT:
                # see line 1942 in dbapi.py (define method in dbapi?)
                srn = f.get_primary_database(
                    ).get_control_secondary().get_freed_bits_page()
                if srn == 0:
                    srn = f.get_primary_segment_bits(
                        ).append(newseg.tobytes())
                else:
                    f.get_primary_segment_bits().put(srn, newseg.tobytes())
                return b''.join(
                    (dv[:4],
                     count.to_bytes(3, byteorder='big'),
                     srn.to_bytes(4, byteorder='big')))
            else:
                # see line 1852 in dbapi.py (define method in dbapi?)
                srn = f.get_primary_database(
                    ).get_control_secondary().get_freed_list_page()
                if srn == 0:
                    srn = f.get_primary_segment_list().append(
                        b''.join(
                            [rn.to_bytes(2, byteorder='big')
                             for rn in sorted(newseg)]))
                else:
                    f.get_primary_segment_list().put(
                        srn,
                        b''.join(
                            [rn.to_bytes(2, byteorder='big')
                             for rn in sorted(newseg)]))
                return b''.join(
                    (dv[:4],
                     count.to_bytes(2, byteorder='big'),
                     srn.to_bytes(4, byteorder='big')))
        
        def get_bits_segment(sv):
            """Return bitarray for segment number in sv and free segment."""
            # see line 1972 in dbapi.py (define method in dbapi?)
            srn_bits = int.from_bytes(sv[7:], byteorder='big')
            bs = f.get_primary_segment_bits().get(srn_bits)
            if bs is None:
                raise DatabaseError('Segment record missing')
            # stub call to put srn_bits on reuse stack
            f.get_primary_database().get_control_secondary(
                ).note_freed_bits_page(srn_bits)
            # ok if reuse bitmap but not if reuse stack
            f.get_primary_segment_bits().delete(srn_bits)
            recnums = Bitarray()
            recnums.frombytes(bs)
            return recnums
        
        def get_list_segment(sv):
            """Return list for segment number in sv and free segment."""
            # see line 1925 in dbapi.py (define method in dbapi?)
            srn_list = int.from_bytes(sv[6:], byteorder='big')
            bs = f.get_primary_segment_list().get(srn_list)
            if bs is None:
                raise DatabaseError('Segment record missing')
            # stub call to put srn_list on reuse stack
            f.get_primary_database().get_control_secondary(
                ).note_freed_list_page(srn_list)
            # ok if reuse bitmap but not if reuse stack
            f.get_primary_segment_list().delete(srn_list)
            recnums = [int.from_bytes(bs[i:i+2], byteorder='big')
                       for i in range(0, len(bs), 2)]
            return recnums
        
        def get_recnum_segment(sv):
            """Return list for segment number in sv."""
            # see line 1989 in dbapi.py (define method in dbapi?)
            return [int.from_bytes(sv[4:], byteorder='big')]
        
        heapify = heapq.heapify
        heappop = heapq.heappop
        heappush = heapq.heappush
        updates = []
        heapify(updates)
        for d in defer:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    f.open_root(self._dbenv)
                    deferred = dict()
                    cursors = dict()
                    for fn in os.listdir(
                        os.path.join(
                            self.get_database_folder(),
                            self._deferfolder,
                            f.get_database_file())):
                        deferred[fn] = DB(self._dbenv)
                        deferred[fn].open(
                            os.path.join(
                                self._deferfolder,
                                f.get_database_file(),
                                fn),
                            f._dbname)
                        cursors[fn] = deferred[fn].cursor()
                    for c in cursors.values():
                        try:
                            k, v = c.first()
                            heappush(updates, (k, v, c))
                        except:
                            c.close()
                            deferred[fn].close()
                    udlen = len(updates)
                    if len(updates):
                        dk, dv, c = heappop(updates)
                        try:
                            k, v = c.next()
                            heappush(updates, (k, v, c))
                        except:
                            c.close()
                            deferred[fn].close()
                    cursor = f._object.cursor()
                    while len(updates):
                        nk, nv, c = heappop(updates)
                        try:
                            k, v = c.next()
                            heappush(updates, (k, v, c))
                        except:
                            c.close()
                            deferred[fn].close()
                        if dk != nk:
                            # different index values
                            cursor.put(dk, dv, DB_KEYLAST)
                            dk, dv = nk, nv
                        elif dv[:4] != nv[:4]:
                            # different segments in order for same index value
                            cursor.put(dk, dv, DB_KEYLAST)
                            dk, dv = nk, nv
                        else:
                            # same index value and segment number
                            dv = do_merge()
                    if udlen:
                        cursor.put(dk, dv, DB_KEYLAST)
                    cursor.close()
            
        # Delete deferred segment databases, and moved old one, for each index
        for d in defer:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    for fn in os.listdir(
                        os.path.join(
                            self.get_database_folder(),
                            self._deferfolder,
                            f.get_database_file())):
                        try:
                            os.remove(
                                os.path.join(
                                    self.get_database_folder(),
                                    self._deferfolder,
                                    f.get_database_file(),
                                    fn))
                        except:
                            pass

    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        defer = self._get_deferable_update_files(db)
        if not defer:
            return
        for d in defer:
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f.is_primary():
                    try:
                        os.rmdir(os.path.join(
                            self.get_database_folder(),
                            self._deferfolder,
                            f.get_database_file()))
                    except:
                        pass
        try:
            os.rmdir(os.path.join(
                self.get_database_folder(),
                self._deferfolder))
        except:
            pass

    def close_context(self):
        """Close main and deferred update databases and environment."""
        self._control.close()
        super(DBbitduapi, self).close_context()

    def open_context(self):
        """Open all DBs."""
        super(DBbitduapi, self).open_context()
        self._control.open_root(self._dbenv)
        return True


class DBbitduPrimary(DBbitPrimary):

    """Provide custom deferred update sort processing for DB file.

    This class disables methods not appropriate to deferred update.

    Methods added:

    defer_put
    write_existence_bit_map

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Define a DB file in deferred update mode"""
        super(DBbitduPrimary, self).__init__(*args)
        self.existence_bit_maps = dict()

    def defer_put(self, segment, record_number):
        """Add bit to existence bit map for new record and defer update."""
        try:
            # Assume cached segment existence bit map exists
            self.existence_bit_maps[segment][record_number] = True
        except KeyError:
            # Get the segment existence bit map from database
            ebmb = self.get_existence_bits_database().get(segment + 1)
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
        self.get_existence_bits_database().put(
            segment + 1, self.existence_bit_maps[segment].tobytes())


class DBbitduSecondary(DBSecondary):

    """Provide custom deferred update sort processing for DB file.

    This class disables methods not appropriate to deferred update.

    Methods added:

    defer_put
    dump_secondary
    get_primary_database
    get_primary_segment_bits
    get_primary_segment_list
    merge_update
    new_secondary
    put_deferred
    set_primary_database
    sort_and_write
    tidy_up_after_merge_update

    Methods overridden:

    delete
    make_cursor
    replace

    Methods extended:

    __init__
    
    """

    # __init__ added for build and test.  Remove when done.
    # Lots copied late from DBbitSecondaryFile to avoid changing superclass.
    # See notes in sort_and_write method.
    def __init__(self, *args):
        """Define a DB file in deferred update mode"""
        super(DBbitduSecondary, self).__init__(*args)
        self.values = dict()
        self._primary_database = None
    
    def defer_put(self, key, segment, record_number):
        """Add record_number to cached segment for key."""
        values = self.values.get(key)
        if values is None:
            self.values[key] = record_number.to_bytes(
                length=2, byteorder='big')
        elif isinstance(values, bytes):
            self.values[key] = [values]
            self.values[key].append(
                record_number.to_bytes(length=2, byteorder='big'))
        elif isinstance(values, list):
            values.append(record_number.to_bytes(length=2, byteorder='big'))
            if len(values) > DB_CONVERSION_LIMIT:
                v = self.values[key] = EMPTY_BITARRAY.copy()
                for rn in values:
                    v[int.from_bytes(rn, byteorder='big')] = True
                v[record_number] = True
        else:
            values[record_number] = True

    def dump_secondary(self):
        """Do nothing - compatibility with DBduSecondary."""
        pass

    def merge_update(self):
        """Do nothing - compatibility with DBduSecondary."""
        pass

    def new_secondary(self, dbenv, home):
        """Do nothing - compatibility with DBduSecondary."""
        pass

    def put_deferred(self, key, value):
        """Put (key, value) on database.
        
        The cursor put method is used because updating secondary DB.
        value is still the integer version of primary key for recno dbs
        
        """
        self._object.cursor().put(key, value, DB_KEYLAST)

    def sort_and_write(self, segment, listdb, bitsdb, segvalues, filecontrol):
        """Sort the segment deferred updates before writing to database."""
        # Should any joining of segments be done here.  Deferred updates will
        # almost certainly start in the middle of a segment when the database
        # already contains records.
        # Probably.
        # But that means passing in a cursor to the secondary database, and
        # right now a set_range(...) next_nodup() prev() sequence is needed to
        # find the segment reference.  If a version of this database engine is
        # using just one index record per value, rather than one index record
        # per segment per value, it may well get done that way.  An extra level
        # of indirection is involved, but may make counting records faster.
        # For now it is simpler to do this in do_deferred_updates method with
        # maybe more wasted space.
        seg_bytes = segment.to_bytes(length=4, byteorder='big')
        for k in sorted(segvalues):
            v = segvalues[k]
            if isinstance(v, list):
                length_bytes = len(v).to_bytes(length=2, byteorder='big')
                segpage = filecontrol.get_freed_list_page()
                if segpage == 0:
                    segpage = listdb.append(b''.join(v))
                else:
                    listdb.put(segpage, b''.join(v))
                segvalues[k] = b''.join(
                    (seg_bytes,
                     length_bytes,
                     segpage.to_bytes(length=4, byteorder='big'),
                     ))
            elif isinstance(v, Bitarray):
                length_bytes = v.count().to_bytes(length=3, byteorder='big')
                segpage = filecontrol.get_freed_bits_page()
                if segpage == 0:
                    segpage = bitsdb.append(v.tobytes())
                else:
                    bitsdb.put(segpage, v.tobytes())
                segvalues[k] = b''.join(
                    (seg_bytes,
                     length_bytes,
                     segpage.to_bytes(length=4, byteorder='big'),
                     ))
            elif isinstance(v, bytes):
                segvalues[k] = b''.join((seg_bytes, v))
        for k in sorted(segvalues):
            self.put_deferred(k, segvalues[k])
        segvalues.clear()

    def tidy_up_after_merge_update(self, home, deferfolder):
        """Do nothing - compatibility with DBduSecondary."""
        pass

    def delete(self, key, value):
        raise DBduapiError('delete not implemented')

    def replace(self, key, oldvalue, newvalue):
        raise DBduapiError('replace not implemented')

    def make_cursor(self, dbobject, keyrange):
        raise DBduapiError('make_cursor not implemented')

    # Start copied from DBbitSecondaryFile
    def set_primary_database(self, database):
        """Set reference to primary database to access segment databases."""
        self._primary_database = database

    def get_primary_database(self):
        """Set reference to primary database to access segment databases."""
        return self._primary_database

    def get_primary_segment_bits(self):
        """Return the segment bitmap database of primary database."""
        return self._primary_database.get_segment_bits_database()

    def get_primary_segment_list(self):
        """Return the segment list database of primary database."""
        return self._primary_database.get_segment_list_database()
    # End copied from DBbitSecondaryFile
