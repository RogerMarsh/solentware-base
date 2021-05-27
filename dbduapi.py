# dbduapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide DB file access in custom deferred update mode.

List of classes

DBduapiError - Exceptions
DBduapi - DB database definition and custom deferred update API
DBduapiRecord - DB record level access in custom deferred update mode

"""

from api.database import DatabaseError

import os

# bsddb removed from Python 3.n
try:
    from bsddb3.db import DB, DB_KEYLAST, DB_RECNO, DBKeyExistError
except ImportError:
    from bsddb.db import DB, DB_KEYLAST, DB_RECNO, DBKeyExistError

from dbapi import DBapi, DBapiRecord, DBapiError

from api.database import decode_record_number, encode_record_number
from api.shelf import Shelf, ShelfString, DEFAULT_SEGMENTSIZE


class DBduapiError(DBapiError):
    pass


class DBduapi(DBapi):
    
    """Support custom deferred updates on DB database.

    Extend and override DBapi methods for custom deferred update.
    __init__ is extended to provide control structures for deferred update.
    DB does not support edit and delete operations in deferred update so
    edit_instance and delete_instance methods raise exceptions.
    This class is not intended for general processing so the make_cursor
    method raises an exception.

    Methods added:

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
    make_root - use DBduapiRecord to open file
    set_defer_update
    unset_defer_update

    Methods extended:

    __init__
    
    """
    
    # Number of records that can be collected for deferred update before
    # applying to file. Depends on memory available.
    # Call set_defer_limit to set an appropriate value for each file.
    _defer_record_limit = DEFAULT_SEGMENTSIZE

    def __init__(self,
                 DBhome,
                 DBnames,
                 DBtypes,
                 DBenvironment,
                 **kargs):
        """Extend DB database definition with deferred update.

        DPTfiles = {name:{ddname:name,
                          folder:name,
                          file:name,
                          filedesc:{property:value, ...},
                          fields:{name:{property:value, ...}, ...},
                          }, ...}
        DPTfolder = folder for files unless overridden in DPTfiles
        **kargs = DPT database system parameters

        """
        super(DBduapi, self).__init__(
            DBhome,
            DBnames,
            DBtypes,
            DBenvironment,
            **kargs)

    def do_deferred_updates(self):
        """Do deferred updates for DBapi."""

        self.make_internal_cursors()

        for m in self._main:
            if self._main[m].deferclass is not None:
                if len(self._main[m].deferclass.deferbuffer):
                    self._main[m].sort_and_write()

        for m in self._main:
            if self._main[m].deferclass is not None:
                #self._main[m].dump_secondary()
                pass

        for m in self._main:
            if self._main[m].deferclass is not None:
                #self._main[m].new_secondary(self._dbenv, self._home)
                pass

        for m in self._main:
            if self._main[m].deferclass is not None:
                self._main[m].merge_update()

        for m in self._main:
            if self._main[m].deferclass is not None:
                self._main[m].tidy_up_after_merge_update(
                    self._home, self._defercontrol)

        for m in self._main:
            if self._main[m].deferclass is not None:
                self._main[m].close()

        self.close_internal_cursors()

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        Puts may be direct or deferred while callbacks handle subsidiary
        databases and non-standard inverted indexes.  Deferred updates are
        controlled by counting the calls to put_instance and comparing with
        self._defer_record_limit.
        
        """
        if self.is_primary_recno(dbset):
            putkey = instance.key.pack()
            dodurecno = self._defer_record_count >= self._defer_record_limit
            if dodurecno:
                self._defer_record_count = 0
            self._defer_record_count += 1
        else:
            putkey = instance.packed_key()
            dodurecno = False
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main

        key = main[db[dbset]].put(putkey, instance.srvalue)
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        if self.is_primary_recno(dbset):
            instance.srkey = encode_record_number(putkey)
        else:
            instance.srkey = putkey

        srindex = instance.srindex
        srkey = instance.srkey
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            if main[db[secondary]].deferclass is not None:
                if dodurecno:
                    main[db[secondary]].deferclass.sort_index()
                for v in srindex[secondary]:
                    main[db[secondary]].defer_put(v, putkey)
            else:
                for v in srindex[secondary]:
                    main[db[secondary]].put(v, putkey)

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
                self._home, self._defercontrol))
        except:
            msg = ' '.join((
                'Create defer update folder',
                ' '.join((self._home, self._defercontrol)),
                'fails'))
            raise DBapiError, msg

        for d in defer:
            if self._main[self._associate[d][d]]._dbtype == DB_RECNO:
                defaultshelf = Shelf
            else:
                defaultshelf = ShelfString
            for s in self._associate[d]:
                f = self._main[self._associate[d][s]]
                if not f._dbprimary:
                    if f.deferclass is None:
                        f.deferclass = defaultshelf()
                        deferfolder = os.path.join(
                            self._home,
                            self._defercontrol,
                            self._associate[d][s])
                        try:
                            os.mkdir(deferfolder)
                        except:
                            msg = ' '.join((
                                'Create defer update folder',
                                deferfolder,
                                'fails'))
                            raise DBapiError, msg
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
                if not f._dbprimary:
                    if f.deferclass is not None:
                        try:
                            os.rmdir(os.path.join(
                                self._home,
                                self._defercontrol,
                                self._associate[d][s]))
                        except:
                            pass
                        f.deferclass = None
        try:
            os.rmdir(os.path.join(
                self._home,
                self._defercontrol))
        except:
            pass

    def _get_deferable_update_files(self, db):
        """Return dictionary of databases in db whose updates are deferable."""
        deferable = False
        for d in self._main:
            if not self._main[d]._dbprimary:
                deferable = True
                break
        if not deferable:
            return False
        
        if isinstance(db, str):
            db = [db]
        elif not isinstance(db, (list, tuple, dict)):
            db = self._associate.keys()
        dbadd = dict()
        for d in db:
            if d in self._associate:
                dbadd[d] = []
                for s in self._associate[d]:
                    if s != d:
                        dbadd[d].append(self._associate[d][s])
        return dbadd
            
    def delete_instance(self, dbset, instance):
        raise DBduapiError, 'delete_instance not implemented'

    def edit_instance(self, dbset, instance):
        raise DBduapiError, 'edit_instance not implemented'

    def make_cursor(self, dbset, dbname, keyrange=None):
        raise DBduapiError, 'make_cursor not implemented'

    def use_deferred_update_process(self, **kargs):
        raise DBduapiError, 'Query use of du when in deferred update mode'

    def make_root(
        self, dbfile, dbname, dbtype, primary, dupsort, value_is_recno):

        return DBduapiRecord(
            dbfile, dbname, dbtype, primary, dupsort, value_is_recno)


class DBduapiRecord(DBapiRecord):

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

    __init__
    
    """

    def __init__(
        self, dbfile, dbname, dbtype, primary, dupsort, value_is_recno):
        """Define a DB file in deferred update mode

        value_is_recno=boolean stating if values are encoded record numbers
        See superclass for other arguments
        
        """
        super(DBduapiRecord, self).__init__(
            dbfile, dbname, dbtype, primary, dupsort, value_is_recno)

        self._defercount = 0
        self.deferclass = None
    
    def defer_put(self, key, value):
        """Write key and value to sequential file for database."""
        self.deferclass.defer_put(key, value)

    def dump_secondary(self):
        """Copy existing secondary db to sequential file."""
        c = self._db_engine_cursor
        r = c.first()
        try:
            current_segment = decode_record_number(r[1]) / DEFAULT_SEGMENTSIZE
        except:
            current_segment = None
        while r:
            k, v = r
            vi = decode_record_number(v)
            s = vi / DEFAULT_SEGMENTSIZE
            if s != current_segment:
                self.deferclass.sort_index()
                current_segment = s
            self.deferclass.defer_put(k, vi)
            r = c.next()
        if current_segment is not None:
            self.deferclass.sort_index()

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
            self._dbfolder,
            self._dbfile))
        del db
        self.open_root(dbenv)

    def put_deferred(self, key, value):
        """Put (key, value) on database.
        
        The cursor put method is used because updating secondary DB.
        value is still the integer version of primary key for recno dbs
        
        """
        if self._value_is_recno:
            try:
                self._db_engine_cursor.put(
                    key, encode_record_number(value), DB_KEYLAST)
            except DBKeyExistError:
                # Application may legitimately do duplicate updates (-30996)
                # to a sorted secondary database for DPT compatibility.
                pass
            except:
                raise
        else:
            self._db_engine_cursor.put(key, value, DB_KEYLAST)

    def sort_and_write(self):
        """Sort the deferred updates before writing to sequential file."""
        self.deferclass.sort_index()

    def tidy_up_after_merge_update(self, home, defercontrol):
        """Delete defer update files and control records."""
        self.deferclass.delete_shelve()
        folder = os.path.join(
            home,
            defercontrol,
            self._dbfile)
        paths = os.listdir(folder)
        for p in paths:
            try:
                os.remove(os.path.join(folder, p))
            except:
                pass

    def delete(self, key, value):
        raise DBduapiError, 'delete not implemented'

    def replace(self, key, oldvalue, newvalue):
        raise DBduapiError, 'replace not implemented'

    def make_cursor(self, dbobject, keyrange):
        raise DBduapiError, 'make_cursor not implemented'
