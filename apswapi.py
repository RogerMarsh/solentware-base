# apswapi.py
# Copyright 2015 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Object database using apsw, an alternative python sqlite3 wrapper.

List of classes

CursorSqlite3 - Define cursor on file and access methods
CursorSqlite3bit
CursorSqlite3bitPrimary
CursorSqlite3bitSecondary
CursorSqlite3Primary
CursorSqlite3Secondary
_DatabaseEncoders
FileControl
FileControlPrimary
Sqlite3api
Sqlite3apiError - Exceptions
_Sqlite3api - Define database and file and record level access methods
Sqlite3ExistenceBitMap
Sqlite3bitapi - _Sqlite3api with file segment support
Sqlite3bitControlFile
Sqlite3bitFile - File level access to each file in database (Open, Close)
Sqlite3bitPrimaryFile - File level access to each primary file in database
Sqlite3bitSecondaryFile - File level access to each secondary file in database
Sqlite3bitPrimary - Record level access to primary files in database
Sqlite3bitSecondary - Record level access to secondary files in database
Sqlite3File
Sqlite3Primary
Sqlite3Secondary
Sqlite3Segment

Segmented databases take account of the local density of values per key on
secondary databases.  A primary database, always with a single column integer
primary key, is seen as a sequence of fixed size intervals called segments.
Secondary databases have zero or one keys per segment for each key.  The value
associated with each segment key is a number, a list of numbers, or a bit map,
representing the record numbers in the segment referenced by the key.  The
absence of a segment key means the key has no values in that segment.  Bit maps
are fixed length and record lists are variable length.  The maximum byte size
of a list is less than or equal to the byte size of a bit map.

Idea taken from DPT, an emulation of Model 204 which runs on Microsoft Windows.

The primary database and secondary database terms are from Berkeley DB.

The representation of multiple rows of a table by one row makes it at least
difficult to use SQL statements directly to evaluate queries.  But the reason
for the sqlite3 interface is it's status as the only cross-platform database
engine included in the Python 3 distribution.  Here SQL statements are used to
build emulations of some of the Berkeley DB interface available at Python 2.

"""

import os
import subprocess
import apsw
from ast import literal_eval

import sys
_platform_win32 = sys.platform == 'win32'
_python_version = '.'.join(
    (str(sys.version_info[0]),
     str(sys.version_info[1])))
del sys

from .api.bytebit import Bitarray, SINGLEBIT

from .api.database import (
    DatabaseError,
    Database,
    Cursor,
    SegmentBitarray,
    SegmentInt,
    SegmentList,
    Recordset,
    EMPTY_BITARRAY,
    )
from .api.constants import (
    FLT, SPT, KEY_VALUE,
    FILEDESC, FILEORG, EO,
    PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, SQLITE3_FIELDATTS,
    FIELDS,
    PRIMARY, SECONDARY, INDEXPREFIX, SEGMENTPREFIX,
    SQLITE_VALUE_COLUMN,
    USE_BYTES,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_RECORDS_COLUMN,
    DB_SEGMENT_SIZE_BYTES,
    DB_SEGMENT_SIZE,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    DB_CONVERSION_LIMIT,
    SUBFILE_DELIMITER,
    )


class Sqlite3apiError(DatabaseError):
    pass


class _DatabaseEncoders(object):
    
    """Define default record key encoder and decoder.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    decode_record_number
    encode_record_number
    
    """

    def encode_record_number(self, key):
        """Return base64 string for integer with left-end most significant.

        Typically used to convert Berkeley DB primary key to secondary index
        format.
        
        """
        return repr(key)

    def decode_record_number(self, skey):
        """Return integer from base64 string with left-end most significant.

        Typically used to convert Berkeley DB primary key held on secondary
        index.

        """
        return literal_eval(skey)

    def encode_record_selector(self, key):
        """Return base64 string for integer with left-end most significant.

        Typically used to convert Berkeley DB primary key to secondary index
        format.
        
        """
        return key


class _Sqlite3api(Database, _DatabaseEncoders):
    
    """Define a Berkeley DB-like database structure using sqlite3.
    
    Primary databases are created as 'integer primary key'.
    Secondary databases are:
    'index, segment, reference, count, primary key (index, segment)'.

    Primary and secondary terminology comes from Berkeley DB documentation.

    segment moves from the value part of a secondary key:value, (segment,
    reference, count), to the primary key definition in sqlite3 where segment
    follows DPT terminology.  Reference can be a record number relative to
    segment start, a reference to a list of record numbers, or a reference to a
    bitmap representing such record numbers.  Count is the number of records
    referenced by this value.

    Secondary databases are supported by an 'integer primary key' table, for
    lists of record numbers or bitmap representations of record numbers.  The
    reference is the key into the relevant table.

    Methods added:

    allocate_and_open_contexts
    cede_contexts_to_process
    close_contexts
    do_database_task
    files_exist
    file_records_under
    get_database_instance
    increase_database_size
    initial_database_size
    make_recordset_all
    make_recordset_key
    make_recordset_key_range
    make_recordset_key_startswith
    open_contexts
    recordset_for_segment
    repair_cursor

    Methods overridden:

    backout
    close_context
    close_database
    commit
    database_cursor
    db_compatibility_hack
    decode_as_primary_key
    encode_primary_key
    exists
    get_database_folder
    get_database
    get_first_primary_key_for_index_key
    get_packed_key
    get_primary_record
    is_primary
    is_primary_recno
    is_recno
    open_context
    start_transaction
    use_deferred_update_process

    Methods extended:

    __init__
    
    """

    # Database engine uses bytes.
    # If a str is passed it is encoded using iso-8859-1.
    engine_uses_bytes_or_str = USE_BYTES

    def __init__(
        self,
        primary_class,
        secondary_class,
        sqlite3tables,
        sqlite3databasefolder,
        *args,
        **kwargs):
        """Define database structure.
        
        sqlite3tables = {
            name:{
                primary:name,
                fields:{
                    name:{property:value, ...},
                    ),
                }, ...
            }
        sqlite3databasefolder = folder containing database file
        record_class = class implementing access to databases (sqlite3 tables)

        """
        super(_Sqlite3api, self).__init__()
        # The database definition from sqlite3tables after validation
        self._sqtables = None

        # The file from sqlite3databasefolder after validation
        self._sqfile = None

        # sqlite3 database connection object
        self._sqconn = None

        # Associate primary and secondary tables by name.
        # {secondary name:primary name, ...,
        #  primary name:[secondary name, ...], ...}
        # A secondary name may be a primary name if a loop is not made.
        self._associate = dict()
        
        try:
            sqfolder = os.path.abspath(sqlite3databasefolder)
        except:
            msg = ' '.join(['Database file name', str(sqlite3databasefolder),
                            'is not valid'])
            raise Sqlite3apiError(msg)

        # sqlite3tables processing

        sqtables = dict()

        if not isinstance(sqlite3tables, dict):
            raise Sqlite3apiError('Table definitions must be a dictionary')

        for t in sqlite3tables:
            if not isinstance(sqlite3tables[t], dict):
                msg = ' '.join(
                    ['Table definition for', repr(t),
                     'must be a dictionary'])
                raise Sqlite3apiError(msg)

            sqlite3desc = sqlite3tables[t]

            primary = sqlite3desc[PRIMARY]
            if primary not in sqlite3desc[FIELDS]:
                msg = ' '.join(['Primary column name', str(primary),
                                'for', t,
                                'does not have a column description'])
                raise Sqlite3apiError(msg)
            if primary in sqtables:
                msg = ' '.join(['Primary table name', str(primary),
                                'for', t,
                                'already used'])
                raise Sqlite3apiError(msg)
                    
            sqtables[primary] = primary_class(
                primary,
                sqlite3desc,
                primary)
            self._associate[t] = {t:primary}

            if SECONDARY in sqlite3desc:
                for name, secondary in sqlite3desc[SECONDARY].items():
                    if not isinstance(name, str):
                        msg = ' '.join(['Secondary table name', str(name),
                                        'for', repr(t),
                                        'must be a string'])
                        raise Sqlite3apiError(msg)

                    if secondary is None:
                        secondary = sqlite3tables.field_name(name)
                    if secondary in sqtables:
                        msg = ' '.join(['Secondary table name', str(secondary),
                                        'for', t,
                                        'already used'])
                        raise Sqlite3apiError(msg)
                    
                    if secondary == primary:
                        msg = ' '.join(['Secondary table name', str(secondary),
                                        'for', t,
                                        'cannot be same as primary'])
                        raise Sqlite3apiError(msg)

                    if secondary not in sqlite3desc[FIELDS]:
                        msg = ' '.join(['Secondary table name',
                                        str(secondary),
                                        'for', t, 'does not have',
                                        'a column description'])
                        raise Sqlite3apiError(msg)

                    sqtables[secondary] = secondary_class(
                        secondary,
                        sqlite3desc,
                        primary)
                    self._associate[t][name] = secondary

        # For the do_database_task method.
        self._sqlite3tables = sqlite3tables
        
        self._sqtables = sqtables
        self._sqfile = os.path.join(sqfolder, os.path.split(sqfolder)[-1])

    def backout(self):
        """Backout tranaction."""
        if self._sqconn:
            self._sqconn.cursor().execute('rollback')

    def close_context(self):
        """Close all sqlite3 cursors."""
        if self._sqconn is None:
            return
        for table in self._sqtables.values():
            table.close()

    def close_contexts(self, close_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    def close_database(self):
        """Close connection to database."""
        if self._sqconn is None:
            return
        self.close_context()
        self._sqconn.close()
        self._sqconn = None
            
    def commit(self):
        """Commit tranaction."""
        if self._sqconn:
            self._sqconn.cursor().execute('commit')

    def db_compatibility_hack(self, record, srkey):
        """Convert to (key, value) format returned by Berkeley DB access.

        sqlite3 is compatible with the conventions for Berkeley DB RECNO
        databases except for a Berkeley DB index where the primary key is not
        held as the value on an index record (maybe the primary key is embedded
        in the secondary key). Here the Berkeley DB index record is (key, None)
        rather than (key, primary key). The correponding sqlite3 structure is
        always (index field value, record number).
        DataClient works to Berkeley DB conventions.
        The user code side of DataClient adopts the appropriate Berkeley DB
        format because it defines the format used. The incompatibility that
        comes from mapping a (key, None) to sqlite3 while using the same user
        code is dealt with in this method.

        """
        key, value = record
        if value is None:
            return (key, self.decode_record_number(srkey))
        else:
            return record

    def exists(self, dbset, dbname):
        """Return True if dbname is a primary or secondary DB in dbset."""
        if dbset in self._associate:
            return dbname in self._associate[dbset]
        else:
            return False

    def files_exist(self):
        """Return True if all defined files exist in self._home folder."""
        return os.path.exists(self._sqfile)

    def database_cursor(self, dbset, dbname, keyrange=None):
        """Create and return a cursor on DB dbname in dbset.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        return self._sqtables[self._associate[dbset][dbname]].make_cursor(
            self._sqtables[self._associate[dbset][dbname]],
            keyrange)

    def repair_cursor(self, cursor, *a):
        """Return cursor for compatibility with DPT which returns a new one."""
        return cursor

    def get_database_folder(self):
        """return database folder name"""
        return os.path.dirname(self._sqfile)
    
    def get_database(self, dbset, dbname):
        """Return DB for dbname in dbset."""
        return self._sqtables[self._associate[dbset][dbname]]._connection

    def get_database_instance(self, dbset, dbname):
        """Return DB instance for dbname in dbset."""
        return self._sqtables[self._associate[dbset][dbname]]

    def get_first_primary_key_for_index_key(self, dbset, dbname, key):
        """Return first primary key for secondary key in dbname for dbname.

        Consider restricting use of this method to secondary DBs whose keys
        each have a unique value.
        
        """
        return self._sqtables[self._associate[dbset][dbname]
                              ].get_first_primary_key_for_index_key(key)

    def get_primary_record(self, dbset, key):
        """Return primary record (key, value) given primary key on dbset."""
        return self._sqtables[self._associate[dbset][dbset]
                              ].get_primary_record(key)

    def is_primary(self, dbset, dbname):
        """Return True if dbname is primary table in dbset."""
        return self._sqtables[self._associate[dbset][dbname]]._primary

    def is_primary_recno(self, dbset):
        """Return True for compatibility with Berkeley DB.

        sqlite3 tables defined to be equivalent to Berkeley DB RECNO database.

        """
        return True

    def is_recno(self, dbset, dbname):
        """Return True if dbname is primary table in dbset."""
        return self._sqtables[self._associate[dbset][dbname]]._primary

    def open_context(self):
        """Open all tables on database."""
        f, b = os.path.split(self._sqfile)
        if not os.path.exists(f):
            os.makedirs(f, mode=0o700)
        if self._sqconn is None:
            self._sqconn = apsw.Connection(self._sqfile)
            # Remove the following statement to convert to unicode strings
            #self._sqconn.text_factory = str
        for table in self._sqtables.values():
            table.open_root(self._sqconn)
        return True

    def open_contexts(self, closed_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    def allocate_and_open_contexts(self, closed_contexts):
        """Open closed_contexts which had been closed.

        This method is intended for use only when re-opening a table after
        closing it temporarily so another thread can make and commit changes
        to a table.

        The sqlite3api version of this method does nothing.

        The method name comes from the dptbase module where it describes
        extactly what is done.  For apswapi it is a callback hook where the
        name is already chosen.

        """
        #self.open_context()
        #sqtables = self._sqtables
        #for c in closed_contexts:
        #    sqtables[c].open_root(self._sqconn)
        self.open_context()

    def get_packed_key(self, dbset, instance):
        """Convert instance.key for use as database value.

        For sqlite3 just return instance.key.pack().
        dbname is relevant to Berkeley DB and retained for compatibility.

        """
        return instance.key.pack()

    def decode_as_primary_key(self, dbset, pkey):
        """Convert pkey for use as database key.

        For sqlite3 just return integer form of pkey.

        """
        #KEYCHANGE
        # Avoid isinstance test?
        if isinstance(pkey, int):
            return pkey
        else:
            return self.decode_record_number(pkey)

    def encode_primary_key(self, dbname, instance):
        """Convert instance.key for use as database value.

        For sqlite3 just return self.get_packed_key() converted to string.

        """
        # Should this be like Berkeley DB version of method?
        return self.encode_record_number(self.get_packed_key(dbname, instance))

    def use_deferred_update_process(self, **kargs):
        """Return module name or None

        **kargs - soak up any arguments other database engines need.

        """
        raise Sqlite3apiError('use_deferred_update_process not implemented')

    def initial_database_size(self):
        """Do nothing and return True as method exists for DPT compatibility"""
        return True

    def increase_database_size(self, **ka):
        """Do nothing because method exists for DPT compatibility"""

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs={},
        use_specification_items=None,
        ):
        """Open new connection to database, run method, then close database.

        This method is intended for use in a separate thread from the one
        dealing with the user interface.  If the normal user interface thread
        also uses a separate thread for it's normal, quick, database actions
        there is probably no need to use this method at all.

        """
        db = self.__class__(
            self.get_database_folder(),
            use_specification_items=use_specification_items)
        db.open_context()
        try:
            taskmethod(db, logwidget, **taskmethodargs)
        finally:
            db.close_database()

    def make_recordset_key(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._sqtables[self._associate[dbset][dbname]
                       ].populate_recordset_key(rs, key)
        return rs

    def make_recordset_key_startswith(
        self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._sqtables[self._associate[dbset][dbname]
                       ].populate_recordset_key_startswith(rs, key)
        return rs

    def make_recordset_key_range(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._sqtables[self._associate[dbset][dbname]
                       ].populate_recordset_key_range(rs, key)
        return rs

    def make_recordset_all(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._sqtables[self._associate[dbset][dbname]
                       ].populate_recordset_all(rs)
        return rs

    def recordset_for_segment(self, recordset, dbname, segment):
        """Return recordset populated with records for segment."""
        self._sqtables[self._associate[recordset.dbset][dbname]
                       ].populate_recordset_from_segment(recordset, segment)
        return recordset
    
    def file_records_under(self, dbset, dbname, recordset, key):
        """File recordset under key in dbname if created from dbset in self."""
        if recordset.dbidentity != id(self.get_database(dbset, dbset)):
            raise DatabaseError(
                'Record set was not created from this database instance')
        if recordset.dbset != dbset:
            raise DatabaseError(
                'Record set was not created from dbset database')
        self._sqtables[self._associate[recordset.dbset][dbname]
                       ].file_records_under(recordset, key)

    def is_engine_uses_bytes(self):
        """Return True if database engine interface is bytes"""
        return self.engine_uses_bytes_or_str is USE_BYTES

    def is_engine_uses_str(self):
        """Return True if database engine interface is str (C not unicode)"""
        return self.engine_uses_bytes_or_str is USE_STR

    def do_deferred_updates(self, pyscript, filepath):
        """Invoke a deferred update process and wait for it to finish.

        pyscript is the script to do the deferred update.
        filepath is a file or a sequence of files containing updates.

        """
        if _platform_win32:
            args = ['pythonw']
        else:
            args = [''.join(('python', _python_version))]
        
        if not os.path.isfile(pyscript):
            msg = ' '.join([repr(pyscript),
                            'is not an existing file'])
            raise Sqlite3apiError(msg)

        args.append(pyscript)
        
        if isinstance(filepath, str):
            filepath = (filepath,)
        for fp in filepath:
            if not os.path.isfile(fp):
                msg = ' '.join([repr(fp),
                                'is not an existing file'])
                raise Sqlite3apiError(msg)

        args.append(os.path.abspath(os.path.dirname(self._sqfile)))
        args.extend(filepath)

        return subprocess.Popen(args)

    def set_defer_update(self, db=None, duallowed=False):
        """Close files before doing deferred updates.

        Replace the original Berkeley DB version with a DPT look-alike.
        It is the same code but implementation of close_context ie different
        because the database engines are different.  Most of the code in the
        earlier set_defer_update will move to the subprocess.
        
        """
        self.close_context()
        return duallowed

    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        # Original method moved to dbduapi.py
        return self.open_context()

    def start_transaction(self):
        """Start a transaction."""
        if self._sqconn:
            self._sqconn.cursor().execute('begin')

    def cede_contexts_to_process(self, close_contexts):
        """Close all contexts so another process, or thread, can commit.

        close_contexts is ignored by this module's version of the method.

        The sqlite3api version of this method does nothing.

        """
        # Closing just the tables in close_contexts seems to be insufficient
        # and the complete shutdown in close_database() seems unnecessary.
        #self.close_database()
        #for c in close_contexts:
        #    self._sqtables[c].close()
        self.close_context()


class Sqlite3api(_Sqlite3api):
    
    """Define a Berkeley DB-like database structure using sqlite3.
    
    Primary databases are created as 'integer primary key'.
    Secondary databases are:
    'index, segment, reference, count, primary key (index, segment)'.

    Primary and secondary terminology comes from Berkeley DB documentation.

    segment moves from the value part of a secondary key:value, (segment,
    reference, count), to the primary key definition in sqlite3 where segment
    follows DPT terminology.  Reference can be a record number relative to
    segment start, a reference to a list of record numbers, or a reference to a
    bitmap representing such record numbers.  Count is the number of records
    referenced by this value.

    Secondary databases are supported by two 'integer primary key' tables, one
    for lists of record numbers and one for bitmap representations of record
    numbers. The reference is the key into the relevant table.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args, **kargs):
        """Define database structure.  See superclass for *args and **kargs."""
        super(Sqlite3api, self).__init__(
            Sqlite3Primary, Sqlite3Secondary, *args, **kargs)

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.
        
        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.
        
        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables
        primarydb = main[db[dbset]]

        primarydb.delete(deletekey, instance.srvalue)
        instance.srkey = self.encode_record_number(deletekey)

        srindex = instance.srindex
        dcb = instance._deletecallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v, deletekey)

    def edit_instance(self, dbset, instance):
        """Edit an existing instance on databases in dbset.
        
        Edits are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.

        """
        oldkey = instance.key.pack()
        newkey = instance.newrecord.key.pack()
        instance.set_packed_value_and_indexes()
        instance.newrecord.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables

        srindex = instance.srindex
        nsrindex = instance.newrecord.srindex
        dcb = instance._deletecallbacks
        ndcb = instance.newrecord._deletecallbacks
        pcb = instance._putcallbacks
        npcb = instance.newrecord._putcallbacks
        
        ionly = []
        nionly = []
        iandni = []
        for f in srindex:
            if f in nsrindex:
                iandni.append(f)
            else:
                ionly.append(f)
        for f in nsrindex:
            if f not in srindex:
                nionly.append(f)

        if oldkey != newkey:
            main[db[dbset]].delete(oldkey, instance.srvalue)
            key = main[db[dbset]].put(newkey, instance.newrecord.srvalue)
            if key != None:
                # put was append to record number database and
                # returned the new primary key. Adjust record key
                # for secondary updates.
                instance.newrecord.key.load(key)
                newkey = key
        elif instance.srvalue != instance.newrecord.srvalue:
            main[db[dbset]].replace(
                oldkey,
                instance.srvalue,
                instance.newrecord.srvalue)

        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)

        for secondary in ionly:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v, oldkey)

        for secondary in nionly:
            if secondary not in db:
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in nsrindex[secondary]:
                main[db[secondary]].put(v, newkey)

        for secondary in iandni:
            if srindex[secondary] == nsrindex[secondary]:
                if oldkey == newkey:
                    continue
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v, oldkey)
            for v in nsrindex[secondary]:
                main[db[secondary]].put(v, newkey)

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        Puts may be direct or deferred while callbacks handle subsidiary
        databases and non-standard inverted indexes.  Deferred updates are
        controlled by counting the calls to put_instance and comparing with
        self._defer_record_limit.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables
        primarydb = main[db[dbset]]

        key = primarydb.put(putkey, instance.srvalue)
        if key != None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)

        srindex = instance.srindex
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].put(v, putkey)

            
class Sqlite3File(object):
    
    """Define a sqlite3 table open_root and close methods.

    Methods added:

    close
    get_database_file
    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.
        
        name = table description name
        sqlite3desc = description of related tables
        primaryname = primary table description name

        Interpret primary in the Berleley DB sense of primary and secondary
        databases for the relationship between primaryname and name.
        
        """
        super(Sqlite3File, self).__init__()

        self._fd_name = name
        self._primaryname = primaryname
        self._indexname = ''.join((INDEXPREFIX, name))
        self._primary = name == primaryname
        self._fieldatts = dict()
        self._connection = None # a sqlite3 cursor on a table
        self._class = None # the adapter class if needed
        
        if self._primary:
            fieldatts = PRIMARY_FIELDATTS
            # Interpret EO (from DPT) as 'integer primary key autoincrement'
            if sqlite3desc[FILEDESC][FILEORG] == EO:
                self._autoincrementprimary = True
            else:
                self._autoincrementprimary = False
        else:
            fieldatts = SECONDARY_FIELDATTS
            self._autoincrementprimary = None
        for attr in SQLITE3_FIELDATTS:
            if attr in fieldatts:
                self._fieldatts[attr] = fieldatts[attr]
        description = sqlite3desc[FIELDS][name]
        if description == None:
            description = dict()
        if not isinstance(description, dict):
            msg = ' '.join(['Attributes for column', fieldname,
                            'in table', repr(self._fd_name),
                            'must be a dictionary or "None"'])
            raise Sqlite3apiError(msg)
        
        for attr in description:
            if attr not in fieldatts:
                msg = ' '.join(['Attribute', repr(attr),
                                'for column', fieldname,
                                'in table', self._fd_name,
                                'is not allowed'])
                raise Sqlite3apiError(msg)
            
            if not isinstance(description[attr], type(fieldatts[attr])):
                msg = ' '.join([attr, 'for column', fieldname,
                                'in table', self._fd_name, 'is wrong type'])
                raise Sqlite3apiError(msg)
            
            if attr == SPT:
                if (description[attr] < 0 or
                    description[attr] > 100):
                    msg = ' '.join(['Split percentage for field',
                                    fieldname, 'in file', self._fd_name,
                                    'is invalid'])
                    raise Sqlite3apiError(msg)

            if attr in SQLITE3_FIELDATTS:
                self._fieldatts[attr] = description[attr]

        #if self._fieldatts.get(SQLITE_ADAPTER):
        #    self._class, adapter, converter = self._fieldatts[SQLITE_ADAPTER]
        #    sqlite3.register_adapter(self._class, adapter)
        #    sqlite3.register_converter(
        #        self._class.__name__, converter)
        #else:
        #    sqlite3.register_converter('TEXT', lambda bs: bs)
        # When using either pickle.dumps() and pickle.loads(). or repr() and
        # ast.literal_eval(), the stored bytestring is returned for consistency
        # with the other database engines.
        #sqlite3.register_converter('TEXT', lambda bs: bs)

    def close(self):
        """Close DB and cursor."""
        # The _connection fills the role of DB
        self._connection = None

    def open_root(self, sqconn):
        """Open sqlite3 database cursor and create table unless it exists."""
        self._connection = sqconn
        if self._primary:
            if self._autoincrementprimary:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, 'integer primary key autoincrement ,',
                    SQLITE_VALUE_COLUMN, 'text',
                    ')',
                    ))
                self._connection.cursor().execute(statement)
            else:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, 'integer primary key ,',
                    SQLITE_VALUE_COLUMN, 'text',
                    ')',
                    ))
                self._connection.cursor().execute(statement)
        elif self._class is not None:
            statement = ' '.join((
                'create table if not exists', self._fd_name,
                '(',
                self._fd_name, self._class.__name__, ',',
                self._primaryname, 'integer',
                ')',
                ))
            self._connection.cursor().execute(statement)
            statement = ' '.join((
                'create index if not exists', self._indexname,
                'on', self._fd_name, '(', self._fd_name, ')',
                ))
            self._connection.cursor().execute(statement)
        else:
            statement = ' '.join((
                'create table if not exists', self._fd_name,
                '(',
                self._fd_name, 'text ,',
                self._primaryname, 'integer',
                ')',
                ))
            self._connection.cursor().execute(statement)
            statement = ' '.join((
                'create index if not exists', self._indexname,
                'on', self._fd_name, '(', self._fd_name, ')',
                ))
            self._connection.cursor().execute(statement)
        # Commit must be at higher level: in open_context() at least.
        #sqconn.cursor().execute('commit')

    def get_database_file(self):
        """Return database file name"""
        return self._fd_name


class Sqlite3Primary(Sqlite3File, _DatabaseEncoders):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
    get_first_primary_key_for_index_key
    get_primary_record
    make_cursor
    populate_recordset_all
    populate_recordset_from_segment
    populate_recordset_key
    populate_recordset_key_range
    populate_recordset_key_startswith
    put
    replace

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.

        See superclass for argument descriptionss
        
        """
        super(Sqlite3Primary, self).__init__(name, sqlite3desc, primaryname)
        self._clientcursors = dict()
        self._recordsets = dict()
    
    def close(self):
        """Close DB and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(Sqlite3Primary, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            self._connection.cursor().execute(
                ' '.join((
                    'delete from',
                    self._fd_name,
                    'where',
                    self._fd_name, '== ?')),
                (key,))
        except:
            pass

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        raise Sqlite3apiError((
            'get_first_primary_key_for_index_key for primary table'))
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        if key is None:
            return None
        statement = ' '.join((
            'select * from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            'order by',
            self._fd_name,
            'limit 1',
            ))
        values = (key,)
        return self._connection.cursor().execute(statement, values).fetchone()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSqlite3Primary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new rows."""
        if not key: #key == 0:  # Change test to "key is None" when sure
            self._connection.cursor().execute(
                ' '.join((
                    'insert into',
                    self._fd_name,
                    '(', SQLITE_VALUE_COLUMN, ')',
                    'values ( ? )',
                    )),
                (value,)
                )
            return self._connection.cursor().execute(
                ' '.join((
                    'select last_insert_rowid() from',
                    self._fd_name))).fetchone()[0]
        else:
            self._connection.cursor().execute(
                ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_VALUE_COLUMN, '= ?',
                    'where',
                    self._primaryname, '== ?',
                    )),
                (value, key))
            return None

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on table.
        
        (key, newvalue) is put on table only if (key, oldvalue) is on table.
        
        """
        try:
            self._connection.cursor().execute(
                ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_VALUE_COLUMN, '= ?',
                    'where',
                    self._primaryname, '== ?'
                    )),
                (newvalue, key))
        except:
            pass

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        self._connection.cursor().execute(
            ' '.join((
                'select', self._fd_name, 'from',
                self._fd_name,
                'where',
                self._fd_name, '== ?',
                'order by', self._fd_name,
                'limit 1')),
            (key,))
        if len(self._connection.cursor().fetchone()):
            s, rn = divmod(key, DB_SEGMENT_SIZE)
            recordset[s] = SegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))

    def populate_recordset_key_startswith(self, recordset, key):
        """Raise exception - populate_recordset_key_startswith primary db."""
        raise Sqlite3apiError(
            ''.join(
                ('populate_recordset_key_startswith not available ',
                 'on primary database')))

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None:
            if keyend is None:
                where = ''
                values = ()
            else:
                where = ' '.join(('where', self._fd_name, '<= ?'))
                values = (keyend,)
        elif keyend is None:
            where = ' '.join(('where', self._fd_name, '>= ?'))
            values = (keystart,)
        else:
            where = ' '.join(
                ('where',
                 self._fd_name, '>= ? and',
                 self._fd_name, '<= ?'))
            values = (keystart. keyend)
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._fd_name, 'from',
                self._fd_name,
                where)),
            values):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._fd_name, 'from',
                self._fd_name))):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        raise DatabaseError(
            'populate_recordset_from_segment not implemented for DBPrimary')
    
    def file_records_under(self, recordset, key):
        """Raise exception as DBPrimary.file_records_under() is nonsense."""
        raise DatabaseError(
            'file_records_under not implemented for DBPrimary')


class Sqlite3Secondary(Sqlite3File, _DatabaseEncoders):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
    get_first_primary_key_for_index_key
    get_primary_record
    make_cursor
    populate_recordset_all
    populate_recordset_from_segment
    populate_recordset_key
    populate_recordset_key_range
    populate_recordset_key_startswith
    put
    replace

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.

        See superclass for argument descriptionss
        
        """
        super(Sqlite3Secondary, self).__init__(
            name, sqlite3desc, primaryname)
        self._clientcursors = dict()
        self._recordsets = dict()
    
    def close(self):
        """Close DB and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(Sqlite3Secondary, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            self._connection.cursor().execute(
                ' '.join((
                    'delete from',
                    self._fd_name,
                    'where',
                    self._fd_name, '== ? and',
                    self._primaryname, '== ?')),
                (key, value))
        except:
            pass

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        statement = ' '.join((
            'select',
            self._primaryname,
            'from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            'order by',
            self._primaryname,
            'limit 1',
            ))
        values = (key,)
        try:
            return self._connection.cursor().execute(
                statement, values).fetchone()[0]
        except TypeError:
            return None
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        raise Sqlite3apiError((
            'get_primary_record for secondary table'))

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSqlite3Secondary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new rows."""
        try:
            self._connection.cursor().execute(
                ' '.join((
                    'insert into', self._fd_name,
                    '(', self._fd_name, ',', self._primaryname, ')',
                    'values ( ? , ? )',
                    )),
                (key, value)
                )
        #except DBKeyExistError:
            # Application may legitimately do duplicate updates (-30996)
            # to a sorted secondary database for DPT compatibility.
            #pass
        except:
            raise

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on table.
        
        (key, newvalue) is put on table only if (key, oldvalue) is on table.
        
        """
        try:
            self._connection.cursor().execute(
                ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    self._fd_name, '= ?',
                    'where',
                    self._primaryname, '== ?',
                    self._fd_name, '== ?',
                    )),
                (newvalue, key, oldvalue))
        except:
            pass

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        if isinstance(key, str):
            key = key.encode('utf8')
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._primaryname, 'from',
                self._fd_name,
                'where',
                self._fd_name, '== ?',
                )),
            (key,)):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True

    def populate_recordset_key_startswith(self, recordset, key):
        """Raise exception - populate_recordset_key_startswith primary db."""
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._primaryname, 'from',
                self._fd_name,
                'where',
                self._fd_name, 'glob ?',
                )),
            (b''.join(
                (key.encode('utf8') if isinstance(key, str) else key, b'*'))
             ,)):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None:
            if keyend is None:
                where = ''
                values = ()
            else:
                where = ' '.join(('where', self._fd_name, '<= ?'))
                values = (keyend,)
        elif keyend is None:
            where = ' '.join(('where', self._fd_name, '>= ?'))
            values = (keystart,)
        else:
            where = ' '.join(
                ('where',
                 self._fd_name, '>= ? and',
                 self._fd_name, '<= ?'))
            values = (keystart. keyend)
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._primaryname, 'from',
                self._fd_name,
                where)),
            values):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        for r in self._connection.cursor().execute(
            ' '.join((
                'select', self._primaryname, 'from',
                self._fd_name))):
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        raise DatabaseError(
            'populate_recordset_from_segment not implemented for DBSecondary')
    
    def file_records_under(self, recordset, key):
        """Raise exception as DBPrimary.file_records_under() is nonsense."""
        print('DBSecondary', 'file_records_under')


class Sqlite3bitapi(_Sqlite3api):
    
    """Define a Berkeley DB-like database structure using sqlite3.
    
    Primary databases are created as 'integer primary key'.
    Secondary databases are:
    'index, segment, reference, count, primary key (index, segment)'.

    Primary and secondary terminology comes from Berkeley DB documentation.

    segment moves from the value part of a secondary key:value, (segment,
    reference, count), to the primary key definition in sqlite3 where segment
    follows DPT terminology.  Reference can be a record number relative to
    segment start, a reference to a list of record numbers, or a reference to a
    bitmap representing such record numbers.  Count is the number of records
    referenced by this value.

    Secondary databases are supported by an 'integer primary key' table, for
    lists of record numbers or bitmap representations of record numbers.  The
    reference is the key into the relevant table.

    Methods added:

    do_deferred_updates
    set_defer_update
    unset_defer_update

    Methods overridden:

    delete_instance
    edit_instance
    put_instance

    Methods extended:

    __init__
    close_context
    open_context
    
    """

    def __init__(self, sqlite3tables, *args, **kargs):
        """Define database structure.  See superclass for *args and **kargs."""
        super(Sqlite3bitapi, self).__init__(
            Sqlite3bitPrimary,
            Sqlite3bitSecondary,
            sqlite3tables,
            *args,
            **kargs)
        self._control = Sqlite3bitControlFile()
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
        super(Sqlite3bitapi, self).close_context()

    def open_context(self):
        """Open all DBs."""
        super(Sqlite3bitapi, self).open_context()
        self._control.open_root(self._sqconn)
        return True

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.
        
        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.
        
        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables
        primarydb = main[db[dbset]]

        statement = ' '.join((
            'select',
            primarydb._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            primarydb._fd_name,
            'order by',
            primarydb._fd_name, 'desc',
            'limit 1',
            ))
        values = ()
        high_record = primarydb._connection.cursor().execute(
            statement, values).fetchone()
        primarydb.delete(deletekey, instance.srvalue)
        instance.srkey = self.encode_record_number(deletekey)

        srindex = instance.srindex
        segment, record_number = divmod(deletekey, DB_SEGMENT_SIZE)
        primarydb.segment_delete(segment, record_number)
        dcb = instance._deletecallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].segment_delete(v, segment, record_number)
        try:
            high_segment = divmod(high_record[0], DB_SEGMENT_SIZE)[0]
        except TypeError:
            # Implies attempt to delete record from empty database.
            # The delete method will have raised an exception if appropriate.
            return
        if segment < high_segment:
            primarydb.get_control_primary().note_freed_record_number_segment(
                segment, record_number)

    def edit_instance(self, dbset, instance):
        """Edit an existing instance on databases in dbset.
        
        Edits are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.

        """
        oldkey = instance.key.pack()
        newkey = instance.newrecord.key.pack()
        instance.set_packed_value_and_indexes()
        instance.newrecord.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables

        srindex = instance.srindex
        nsrindex = instance.newrecord.srindex
        dcb = instance._deletecallbacks
        ndcb = instance.newrecord._deletecallbacks
        pcb = instance._putcallbacks
        npcb = instance.newrecord._putcallbacks

        # Changing oldkey to newkey should not be allowed
        old_segment, old_record_number = divmod(oldkey, DB_SEGMENT_SIZE)
        # Not changed by default.  See oldkey != newkey below.
        new_segment, new_record_number = old_segment, old_record_number
        
        ionly = []
        nionly = []
        iandni = []
        for f in srindex:
            if f in nsrindex:
                iandni.append(f)
            else:
                ionly.append(f)
        for f in nsrindex:
            if f not in srindex:
                nionly.append(f)

        if oldkey != newkey:
            main[db[dbset]].delete(oldkey, instance.srvalue)
            key = main[db[dbset]].put(newkey, instance.newrecord.srvalue)
            if key is not None:
                # put was append to record number database and
                # returned the new primary key. Adjust record key
                # for secondary updates.
                instance.newrecord.key.load(key)
                newkey = key
                new_segment, new_record_number = divmod(newkey, DB_SEGMENT_SIZE)
            main[db[dbset]].segment_delete(old_segment, old_record_number)
            main[db[dbset]].segment_put(new_segment, new_record_number)
        elif instance.srvalue != instance.newrecord.srvalue:
            main[db[dbset]].replace(
                oldkey,
                instance.srvalue,
                instance.newrecord.srvalue)
        
        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)

        for secondary in ionly:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].segment_delete(
                    v, old_segment, old_record_number)

        for secondary in nionly:
            if secondary not in db:
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in nsrindex[secondary]:
                main[db[secondary]].segment_put(
                    v, new_segment, new_record_number)

        for secondary in iandni:
            if srindex[secondary] == nsrindex[secondary]:
                if oldkey == newkey:
                    continue
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].segment_delete(
                    v, old_segment, old_record_number)
            for v in nsrindex[secondary]:
                main[db[secondary]].segment_put(
                    v, new_segment, new_record_number)

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are integer primary key.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._sqtables
        primarydb = main[db[dbset]]

        if putkey == 0:
            # reuse record number if possible
            putkey = primarydb.get_control_primary(
                ).get_lowest_freed_record_number()
            if putkey != 0:
                instance.key.load(putkey)
        key = primarydb.put(putkey, instance.srvalue)
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            # Perhaps _control_primary should hold this key to avoid the cursor
            # operation to find the high segment in every delete_instance call.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)

        srindex = instance.srindex
        segment, record_number = divmod(putkey, DB_SEGMENT_SIZE)
        primarydb.segment_put(segment, record_number)
        pcb = instance._putcallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].segment_put(v, segment, record_number)

    def do_deferred_updates(self, pyscript, filepath):
        """Invoke a deferred update process and wait for it to finish.

        pyscript is the script to do the deferred update.
        filepath is a file or a sequence of files containing updates.

        """
        if _platform_win32:
            args = ['pythonw']
        else:
            args = [''.join(('python', _python_version))]
        
        if not os.path.isfile(pyscript):
            msg = ' '.join([repr(pyscript),
                            'is not an existing file'])
            raise Sqlite3apiError(msg)

        args.append(pyscript)
        
        if isinstance(filepath, str):
            filepath = (filepath,)
        for fp in filepath:
            if not os.path.isfile(fp):
                msg = ' '.join([repr(fp),
                                'is not an existing file'])
                raise Sqlite3apiError(msg)

        args.append(os.path.abspath(os.path.dirname(self._sqfile)))
        args.extend(filepath)

        return subprocess.Popen(args)

    def set_defer_update(self, db=None, duallowed=False):
        """Close files before doing deferred updates.

        Replace the original Berkeley DB version with a DPT look-alike.
        It is the same code but implementation of close_context ie different
        because the database engines are different.  Most of the code in the
        earlier set_defer_update will move to the subprocess.
        
        """
        self.close_context()
        return duallowed

    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        # Original method moved to dbduapi.py
        return self.open_context()

            
class Sqlite3bitFile(object):
    
    """Define a sqlite3 table open_root and close methods.

    Methods added:

    close
    get_database_file
    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.
        
        name = table description name
        sqlite3desc = description of related tables
        primaryname = primary table description name

        Interpret primary in the Berleley DB sense of primary and secondary
        databases for the relationship between primaryname and name.
        
        """
        super(Sqlite3bitFile, self).__init__()

        self._fd_name = name
        self._primaryname = primaryname
        self._indexname = ''.join((INDEXPREFIX, name))
        self._segmentname = ''.join((SEGMENTPREFIX, name))
        self._primary = name == primaryname
        self._fieldatts = dict()
        self._connection = None # a sqlite3 cursor on a table
        self._class = None # the adapter class if needed
        
        if self._primary:
            fieldatts = PRIMARY_FIELDATTS
            # Interpret EO (from DPT) as 'integer primary key autoincrement'
            if sqlite3desc[FILEDESC][FILEORG] == EO:
                self._autoincrementprimary = True
            else:
                self._autoincrementprimary = False
        else:
            fieldatts = SECONDARY_FIELDATTS
            self._autoincrementprimary = None
        for attr in SQLITE3_FIELDATTS:
            if attr in fieldatts:
                self._fieldatts[attr] = fieldatts[attr]
        description = sqlite3desc[FIELDS][name]
        if description == None:
            description = dict()
        if not isinstance(description, dict):
            msg = ' '.join(['Attributes for column', repr(name),
                            'in table', repr(self._fd_name),
                            "must be a 'dict' or 'None'"])
            raise Sqlite3apiError(msg)
        
        for attr in description:
            if attr not in fieldatts:
                msg = ' '.join(['Attribute', repr(attr),
                                'for column', fieldname,
                                'in table', self._fd_name,
                                'is not allowed'])
                raise Sqlite3apiError(msg)
            
            if not isinstance(description[attr], type(fieldatts[attr])):
                msg = ' '.join([attr, 'for column', fieldname,
                                'in table', self._fd_name, 'is wrong type'])
                raise Sqlite3apiError(msg)
            
            if attr == SPT:
                if (description[attr] < 0 or
                    description[attr] > 100):
                    msg = ' '.join(['Split percentage for field',
                                    fieldname, 'in file', self._fd_name,
                                    'is invalid'])
                    raise Sqlite3apiError(msg)

            if attr in SQLITE3_FIELDATTS:
                self._fieldatts[attr] = description[attr]

        # When using either pickle.dumps() and pickle.loads(). or repr() and
        # ast.literal_eval(), the stored bytestring is returned for consistency
        # with the other database engines.
        #sqlite3.register_converter('TEXT', lambda bs: bs)

    def close(self):
        """Close DB and cursor."""
        # The _connection fills the role of DB
        self._connection = None

    def open_root(self, sqconn):
        """Open sqlite3 database cursor and create table unless it exists."""
        self._connection = sqconn
        if self._primary:
            if self._autoincrementprimary:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, 'integer primary key autoincrement ,',
                    SQLITE_VALUE_COLUMN,
                    ')',
                    ))
                self._connection.cursor().execute(statement)
            else:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, 'integer primary key ,',
                    SQLITE_VALUE_COLUMN,
                    ')',
                    ))
                self._connection.cursor().execute(statement)
            statement = ' '.join((
                'create table if not exists', self._segmentname,
                '(',
                SQLITE_RECORDS_COLUMN,
                ')',
                ))
            self._connection.cursor().execute(statement)
        else:
            if self._class is not None:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, self._class.__name__, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._primaryname,
                    ')',
                    ))
                pass
            else:
                statement = ' '.join((
                    'create table if not exists', self._fd_name,
                    '(',
                    self._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._primaryname,
                    ')',
                    ))
            self._connection.cursor().execute(statement)
            statement = ' '.join((
                'create unique index if not exists', self._indexname,
                'on', self._fd_name,
                '(',
                self._fd_name, ',',
                SQLITE_SEGMENT_COLUMN,
                ')',
                ))
            self._connection.cursor().execute(statement)
        # Commit must be at higher level: in open_context() at least.
        #sqconn.cursor().execute('commit')

    def get_database_file(self):
        """Return database file name"""
        return self._fd_name

            
class Sqlite3bitPrimaryFile(Sqlite3bitFile):
    
    """Define a sqlite3 table open_root and close methods.

    Methods added:

    get_control_database
    get_existence_bits
    get_existence_bits_database
    set_control_database
    get_control_primary

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    open_root
    
    """

    def __init__(self, *args):
        """Bitmapped primary database file for name in description."""
        super(Sqlite3bitPrimaryFile, self).__init__(*args)

        # Description to be provided
        self._control_database = None

        # Existence bit map control structure (reuse record numbers)
        self._control_primary = FileControlPrimary(self)

        # Record number existence bit map for this primary database
        self._existence_bits = Sqlite3ExistenceBitMap(self.get_database_file())

    def open_root(self, *args):
        """Open primary database and inverted index databases."""
        super(Sqlite3bitPrimaryFile, self).open_root(*args)
        self._existence_bits.open_root(*args)

    def close(self):
        """Close inverted index databases then primary database."""
        super(Sqlite3bitPrimaryFile, self).close()

    def get_control_database(self):
        """Return the database containing segment control data."""
        return self._control_database.get_control_database()

    def get_existence_bits(self):
        """Return the existence bit map control data."""
        return self._existence_bits

    def get_existence_bits_database(self):
        """Return the database containing existence bit map."""
        # Use instance.get_existence_bits().put(...) and so forth in sqlite3
        return self._existence_bits._seg_object

    def set_control_database(self, database):
        """Set reference to segment control databases."""
        self._control_database = database

    def get_control_primary(self):
        """Return the re-use record number control data."""
        return self._control_primary

    def get_segment_records(self, rownumber):
        """Return the record list or bitmap in self._segmentname rownumber."""
        statement = ' '.join((
            'select',
            SQLITE_RECORDS_COLUMN,
            'from',
            self._segmentname,
            'where rowid == ?',
            ))
        values = (rownumber,)
        return self._connection.cursor(
            ).execute(statement, values).fetchone()[0]

    def set_segment_records(self, values):
        """Update self._segmentname row using values"""
        statement = ' '.join((
            'update',
            self._segmentname,
            'set',
            SQLITE_RECORDS_COLUMN, '= ?',
            'where rowid == ?',
            ))
        self._connection.cursor().execute(statement, values)

    def delete_segment_records(self, values):
        """Delete self._segmentname row using values"""
        statement = ' '.join((
            'delete from',
            self._segmentname,
            'where rowid == ?',
            ))
        self._connection.cursor().execute(statement, values)

    def insert_segment_records(self, values):
        """Insert self._segmentname row using values"""
        statement = ' '.join((
            'insert into',
            self._segmentname,
            '(',
            SQLITE_RECORDS_COLUMN,
            ')',
            'values ( ? )',
            ))
        self._connection.cursor().execute(statement, values)
        return self._connection.cursor().execute(
            ' '.join((
                'select last_insert_rowid() from',
                self._segmentname))).fetchone()[0]

            
class Sqlite3bitSecondaryFile(Sqlite3bitFile):
    
    """Define a sqlite3 table open_root and close methods.

    Methods added:

    get_primary_database
    set_primary_database

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Bitmapped secondary database.  See superclass for *args."""
        super(Sqlite3bitSecondaryFile, self).__init__(*args)
        self._primary_database = None

    def set_primary_database(self, database):
        """Set reference to primary database to access segment databases."""
        self._primary_database = database

    def get_primary_database(self):
        """Set reference to primary database to access segment databases."""
        return self._primary_database


class Sqlite3bitPrimary(Sqlite3bitPrimaryFile):
    
    """Define a sqlite3 table with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
    get_first_primary_key_for_index_key
    get_primary_record
    make_cursor
    populate_recordset_all
    populate_recordset_from_segment
    populate_recordset_key
    populate_recordset_key_range
    populate_recordset_key_startswith
    put
    replace
    segment_delete
    segment_put

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.

        See superclass for argument descriptionss
        
        """
        super(Sqlite3bitPrimary, self).__init__(
            name, sqlite3desc, primaryname)
        self._clientcursors = dict()
        self._recordsets = dict()
    
    def close(self):
        """Close DB and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(Sqlite3bitPrimary, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            statement = ' '.join((
                'delete from',
                self._fd_name,
                'where',
                self._fd_name, '== ?',
                ))
            values = (key,)
            self._connection.cursor().execute(statement, values)
        except:
            pass

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        raise Sqlite3apiError((
            'get_first_primary_key_for_index_key for primary table'))
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        if key is None:
            return None
        statement = ' '.join((
            'select * from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            'order by',
            self._fd_name,
            'limit 1',
            ))
        values = (key,)
        return self._connection.cursor().execute(statement, values).fetchone()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSqlite3bitPrimary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new rows."""
        if not key: #key == 0:  # Change test to "key is None" when sure
            statement = ' '.join((
                'insert into',
                self._fd_name,
                '(', SQLITE_VALUE_COLUMN, ')',
                'values ( ? )',
                ))
            values = (value,)
            self._connection.cursor().execute(statement, values)
            return self._connection.cursor().execute(
                ' '.join((
                    'select last_insert_rowid() from',
                    self._fd_name))).fetchone()[0]
        else:
            statement = ' '.join((
                'update',
                self._fd_name,
                'set',
                SQLITE_VALUE_COLUMN, '= ?',
                'where',
                self._primaryname, '== ?',
                ))
            values = (value, key)
            self._connection.cursor().execute(statement, values)
            return None

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on table.
        
        (key, newvalue) is put on table only if (key, oldvalue) is on table.
        
        """
        try:
            statement = ' '.join((
                'update',
                self._fd_name,
                'set',
                SQLITE_VALUE_COLUMN, '= ?',
                'where',
                self._primaryname, '== ?',
                ))
            values = (newvalue, key)
            self._connection.cursor().execute(statement, values)
        except:
            pass

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        statement = ' '.join((
            'select',
            self._fd_name,
            'from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            'order by',
            self._fd_name,
            'limit 1',
            ))
        values = (key,)
        if len(self._connection.cursor().execute(statement, values).fetchone()):
            s, rn = divmod(key, DB_SEGMENT_SIZE)
            recordset[s] = SegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))

    def populate_recordset_key_startswith(self, recordset, key):
        """Raise exception - populate_recordset_key_startswith primary db."""
        raise Sqlite3apiError(
            ''.join(
                ('populate_recordset_key_startswith not available ',
                 'on primary database')))

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None and keyend is None:
            self.populate_recordset_all(recordset)
            return
        
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(keystart, DB_SEGMENT_SIZE)
        if keyend is not None:
            segment_end, record_number_end = divmod(keyend, DB_SEGMENT_SIZE)

        if keyend is None:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._seg_dbfile,
                'where',
                self.get_existence_bits()._seg_dbfile, '>= ?',
                ))
            values = (segment_start,)
        elif keystart is None:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._seg_dbfile,
                'where',
                self.get_existence_bits()._seg_dbfile, '<= ?',
                ))
            values = (segment_end,)
        else:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._seg_dbfile,
                'where',
                self.get_existence_bits()._seg_dbfile, '>= ? and',
                self.get_existence_bits()._seg_dbfile, '<= ?',
                ))
            values = (segment_start, segment_end)
        for r in self.get_existence_bits_database(
            ).cursor().execute(statement, values):
            recordset[r[0] - 1] = SegmentBitarray(r[0] - 1, None, records=r[1])
        try:
            recordset[segment_start][:recnum_start] = False
        except KeyError:
            pass
        try:
            recordset[segment_end][recnum_end + 1:] = False
        except KeyError:
            pass
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        statement = ' '.join((
            'select',
            self.get_existence_bits()._seg_dbfile, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self.get_existence_bits()._seg_dbfile,
            ))
        values = ()
        for r in self.get_existence_bits_database(
            ).cursor().execute(statement, values):
            recordset[r[0] - 1] = SegmentBitarray(r[0] - 1, None, records=r[1])

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        recordset.clear_recordset()
        if segment[2] > DB_CONVERSION_LIMIT:
            bs = self.get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[segment[1]] = SegmentBitarray(
                segment[1], None, records=bs)
        elif segment[2] > 1:
            bs = self.get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[segment[1]] = SegmentList(
                segment[1], None, records=bs)
        else:
            recordset[segment[1]] = SegmentInt(
                segment[1],
                None,
                records=segment[3].to_bytes(2, byteorder='big'))
    
    def file_records_under(self, recordset, key):
        """Raise exception as DBPrimary.file_records_under() is nonsense."""
        raise DatabaseError(
            'file_records_under not implemented for Sqlite3bitPrimary')

    def segment_delete(self, segment, record_number):
        """Remove record_number from existence bit map for segment."""
        # See dbduapi.py DBbitduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately (and delete!!).
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits().get(segment + 1)
        if ebmb is None:
            # It does not exist so raise an exception
            raise Sqlite3apiError('Existence bit map for segment does not exist')
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            # Set bit for record number and write segment back to database
            ebm[record_number] = False
            self.get_existence_bits().put(segment + 1, ebm.tobytes())

    def segment_put(self, segment, record_number):
        """Add record_number to existence bit map for segment."""
        # See dbduapi.py DBbitduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately.
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits().get(segment + 1)
        if ebmb is None:
            # It does not exist so create a new empty one
            ebm = EMPTY_BITARRAY.copy()
            # Set bit for record number and write segment to database
            ebm[record_number] = True
            self.get_existence_bits().append(ebm.tobytes())
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            # Set bit for record number and write segment back to database
            ebm[record_number] = True
            self.get_existence_bits().put(segment + 1, ebm.tobytes())


class Sqlite3bitSecondary(Sqlite3bitSecondaryFile):
    
    """Define a sqlite3 table with record access and deferred update methods.

    Methods added:

    file_records_under
    make_cursor
    populate_recordset_all
    populate_recordset_from_segment
    populate_recordset_key
    populate_recordset_key_range
    populate_recordset_key_startswith
    segment_delete
    segment_put

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(self, name, sqlite3desc, primaryname):
        """Define a sqlite3 table.

        See superclass for argument descriptionss
        
        """
        super(Sqlite3bitSecondary, self).__init__(
            name, sqlite3desc, primaryname)
        self._clientcursors = dict()
        self._recordsets = dict()
    
    def close(self):
        """Close DB and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(Sqlite3bitSecondary, self).close()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSqlite3bitSecondary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        statement = ' '.join((
            'select',
            self._fd_name, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            'from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            ))
        values = (key,)
        for record in self._connection.cursor().execute(statement, values):
            if record[2] > DB_CONVERSION_LIMIT:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                recordset[record[1]] = SegmentBitarray(
                    record[1], None, records=bs)
            elif record[2] > 1:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                recordset[record[1]] = SegmentList(record[1], None, records=bs)
            else:
                recordset[record[1]] = SegmentInt(
                    record[1],
                    None,
                    records=record[3].to_bytes(2, byteorder='big'))

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting."""
        statement = ' '.join((
            'select',
            self._fd_name, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            'from',
            self._fd_name,
            'where',
            self._fd_name, 'glob ?',
            ))
        values = (
            b''.join(
                (key.encode('utf8') if isinstance(key, str) else key,
                 b'*',
                 )),)
        for record in self._connection.cursor().execute(statement, values):
            if record[2] > DB_CONVERSION_LIMIT:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentBitarray(record[1], None, records=bs)
            elif record[2] > 1:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentList(record[1], None, records=bs)
            else:
                segment = SegmentInt(
                    record[1],
                    None,
                    records=record[3].to_bytes(2, byteorder='big'))
            if record[1] not in recordset:
                recordset[record[1]] = segment.promote()
            else:
                recordset[record[1]] |= segment

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None and keyend is None:
            self.populate_recordset_all(recordset)
            return

        if keyend is None:
            statement = ' '.join((
                'select',
                self._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._primaryname,
                'from',
                self._fd_name,
                'where',
                self._fd_name, '>= ?',
                ))
            values = (keystart,)
        elif keystart is None:
            statement = ' '.join((
                'select',
                self._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._primaryname,
                'from',
                self._fd_name,
                'where',
                self._fd_name, '<= ?',
                ))
            values = (keyend,)
        else:
            statement = ' '.join((
                'select',
                self._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._primaryname,
                'from',
                self._fd_name,
                'where',
                self._fd_name, '>= ? and',
                self._fd_name, '<= ?',
                ))
            values = (keystart, keyend)
        for record in self._connection.cursor().execute(statement, values):
            if record[2] > DB_CONVERSION_LIMIT:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentBitarray(record[1], None, records=bs)
            elif record[2] > 1:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentList(record[1], None, records=bs)
            else:
                segment = SegmentInt(
                    record[1],
                    None,
                    records=record[3].to_bytes(2, byteorder='big'))
            if record[1] not in recordset:
                recordset[record[1]] = segment.promote()
            else:
                recordset[record[1]] |= segment
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        statement = ' '.join((
            'select',
            self._fd_name, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            'from',
            self._fd_name,
            ))
        values = ()
        for record in self._connection.cursor().execute(statement, values):
            if record[2] > DB_CONVERSION_LIMIT:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentBitarray(record[1], None, records=bs)
            elif record[2] > 1:
                bs = self.get_primary_database().get_segment_records(record[3])
                if bs is None:
                    raise DatabaseError('Segment record missing')
                segment = SegmentList(record[1], None, records=bs)
            else:
                segment = SegmentInt(
                    record[1],
                    None,
                    records=record[3].to_bytes(2, byteorder='big'))
            if record[1] not in recordset:
                recordset[record[1]] = segment.promote()
            else:
                recordset[record[1]] |= segment

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        recordset.clear_recordset()
        if segment[2] > DB_CONVERSION_LIMIT:
            bs = self.get_primary_database().get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[segment[1]] = SegmentBitarray(
                segment[1], None, records=bs)
        elif segment[2] > 1:
            bs = self.get_primary_database().get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[segment[1]] = SegmentList(
                segment[1], None, records=bs)
        else:
            recordset[segment[1]] = SegmentInt(
                segment[1],
                None,
                records=segment[3].to_bytes(2, byteorder='big'))

    def populate_segment(self, segment):
        """Return a Segment subclass instance with records in segment."""
        if segment[2] > DB_CONVERSION_LIMIT:
            bs = self.get_primary_database().get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            return SegmentBitarray(segment[1], None, records=bs)
        elif segment[2] > 1:
            bs = self.get_primary_database().get_segment_records(segment[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            return SegmentList(segment[1], None, records=bs)
        else:
            return SegmentInt(
                segment[1],
                None,
                records=segment[3].to_bytes(2, byteorder='big'))

    def make_segment(self, key, segment_number, record_count, records):
        """Return a Segment subclass instance created from arguments."""
        if record_count > DB_CONVERSION_LIMIT:
            return SegmentBitarray(segment_number, None, records=records)
        elif record_count > 1:
            return SegmentList(segment_number, None, records=records)
        else:
            return SegmentInt(
                segment_number,
                None,
                records=records.to_bytes(2, byteorder='big'))
    
    def file_records_under(self, recordset, key):
        """File records in recordset under key."""
        statement = ' '.join((
            'select',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self._primaryname,
            'from',
            self._fd_name,
            'indexed by',
            self._indexname,
            'where',
            self._fd_name, '== ?',
            ))
        values = (key,)
        segments = []
        lists = []
        rows = self._connection.cursor().execute(statement, values).fetchall()
        rows.sort()
        for r in rows:
            if r[1] == 1:
                pass
            elif r[1] > DB_CONVERSION_LIMIT:
                segments.append(r)
            else:
                lists.append(r)
        rows = {r[0] for r in rows}
        gpd = self.get_primary_database()
        for sn in recordset.sorted_segnums:
            if isinstance(recordset.rs_segments[sn], SegmentBitarray):
                if len(segments):
                    sk = segments.pop(0)[2]
                    gpd.set_segment_records(
                        (recordset.rs_segments[sn]._bitarray.tobytes(), sk))
                elif len(lists):
                    sk = lists.pop(0)[2]
                    gpd.set_segment_records(
                        (recordset.rs_segments[sn]._bitarray.tobytes(), sk))
                else:
                    sk = gpd.insert_segment_records(
                        (recordset.rs_segments[sn]._bitarray.tobytes(),))
                if sn in rows:
                    rows.remove(sn)
                    statement = ' '.join((
                        'update',
                        self._fd_name,
                        'set',
                        SQLITE_COUNT_COLUMN, '= ? ,',
                        self._primaryname,
                        'where',
                        self._fd_name, '== ? and',
                        SQLITE_SEGMENT_COLUMN, '== ?',
                        ))
                    values = (
                        recordset.rs_segments[sn].count_records(),
                        sk,
                        key,
                        sn)
                else:
                    statement = ' '.join((
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
                    values = (
                        key,
                        sn,
                        recordset.rs_segments[sn].count_records(),
                        sk)
                self._connection.cursor().execute(statement, values)
            elif isinstance(recordset.rs_segments[sn], SegmentList):
                rnlist = b''.join(
                    [rn.to_bytes(2, byteorder='big')
                     for rn in recordset.rs_segments[sn]._list])
                if len(lists):
                    sk = lists.pop(0)[2]
                    gpd.set_segment_records((rnlist, sk))
                elif len(segments):
                    sk = segments.pop(0)[2]
                    gpd.set_segment_records((rnlist, sk))
                else:
                    sk = gpd.insert_segment_records((rnlist,))
                if sn in rows:
                    rows.remove(sn)
                    statement = ' '.join((
                        'update',
                        self._fd_name,
                        'set',
                        SQLITE_COUNT_COLUMN, '= ? ,',
                        self._primaryname,
                        'where',
                        self._fd_name, '== ? and',
                        SQLITE_SEGMENT_COLUMN, '== ?',
                        ))
                    values = (
                        recordset.rs_segments[sn].count_records(),
                        sk,
                        key,
                        sn)
                else:
                    statement = ' '.join((
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
                    values = (
                        key,
                        sn,
                        recordset.rs_segments[sn].count_records(),
                        sk)
                self._connection.cursor().execute(statement, values)
            elif isinstance(recordset.rs_segments[sn], SegmentInt):
                # divmod to avoid defining a relative record number getter
                # for here only
                sk = divmod(
                    recordset.rs_segments[sn].current()[-1],
                    DB_SEGMENT_SIZE)[-1]
                if sn in rows:
                    rows.remove(sn)
                    statement = ' '.join((
                        'update',
                        self._fd_name,
                        'set',
                        SQLITE_COUNT_COLUMN, '= ? ,',
                        self._primaryname,
                        'where',
                        self._fd_name, '== ? and',
                        SQLITE_SEGMENT_COLUMN, '== ?',
                        ))
                    values = (
                        recordset.rs_segments[sn].count_records(),
                        sk,
                        key,
                        sn)
                else:
                    statement = ' '.join((
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
                    values = (
                        key,
                        sn,
                        recordset.rs_segments[sn].count_records(),
                        sk)
                self._connection.cursor().execute(statement, values)
            pass
        # Delete any references not reused by file_records_under.
        for r in rows:
            statement = ' '.join((
                'delete from',
                self._fd_name,
                'where',
                self._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))
            values = (key, r)
            self._connection.cursor().execute(statement, values)
        for r in segments, lists:
            for sk in r:
                gpd.delete_segment_records((sk[2]))

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        statement = ' '.join((
            'select',
            self._primaryname,
            'from',
            self._fd_name,
            'where',
            self._fd_name, '== ?',
            'order by',
            self._primaryname,
            'limit 1',
            ))
        values = (key,)
        try:
            return self._connection.cursor(
                ).execute(statement, values).fetchone()[0]
        except TypeError:
            return None
    
    def segment_delete(self, key, segment, record_number):
        """Remove record_number from segment for key and write to database"""
        # See DBbitSecondary.segment_put (in this class definition) for model.
        statement = ' '.join((
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
        values = (key, segment)
        record = self._connection.cursor().execute(statement, values).fetchone()
        if record is None:
            # Assume that multiple requests to delete an index value have been
            # made for a record.  The segment_put method uses sets to avoid
            # adding multiple entries.  Consider using set rather than list
            # in the pack method of the ...value... subclass of Value if this
            # will happen a lot.
            return
        if record[2] > DB_CONVERSION_LIMIT:
            bs = self.get_primary_database().get_segment_records(record[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recnums = Bitarray()
            recnums.frombytes(bs)
            # maybe ignore possibility record_number already deleted
            if not recnums[record_number]:
                return
            recnums[record_number] = False
            count = recnums.count()
            if count < DB_CONVERSION_LIMIT:
                recnums = {rn for rn in recums.search(SINGLEBIT)}
                rnlist = b''.join(
                    [rn.to_bytes(2, byteorder='big') for rn in sorted(recnums)])
                statement = ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_COUNT_COLUMN, '= ?',
                    'where',
                    self._fd_name, '== ? and',
                    SQLITE_SEGMENT_COLUMN, '== ?',
                    ))
                values = (count, key, segment)
                self._connection.cursor().execute(statement, values)
                values = (rn_list, record[3])
                self.get_primary_database().set_segment_records(values)
            else:
                statement = ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_COUNT_COLUMN, '= ?',
                    'where',
                    self._fd_name, '== ? and',
                    SQLITE_SEGMENT_COLUMN, '== ?',
                    ))
                values = (count, key, segment)
                self._connection.cursor().execute(statement, values)
                values = (recnums.tobytes(), record[3])
                self.get_primary_database().set_segment_records(values)
        elif record[2] > 1:
            bs = self.get_primary_database().get_segment_records(record[3])
            recnums = {int.from_bytes(bs[i:i+2], byteorder='big')
                       for i in range(0, len(bs), 2)}
            # maybe ignore possibility record_number already deleted
            if record_number not in recnums:
                return
            recnums.discard(record_number)
            count = len(recnums)
            if count < 2:
                if count:
                    statement = ' '.join((
                        'update',
                        self._fd_name,
                        'set',
                        SQLITE_COUNT_COLUMN, '= ? ,',
                        self._primaryname, '= ?',
                        'where',
                        self._fd_name, '== ? and',
                        SQLITE_SEGMENT_COLUMN, '== ?',
                        ))
                    values = (count, recnums.pop(), key, segment)
                    self._connection.cursor().execute(statement, values)
                    values = (record[3],)
                    self.get_primary_database().delete_segment_records(values)
                else:
                    statement = ' '.join((
                        'delete from',
                        self._fd_name,
                        'where',
                        self._fd_name, '== ? and',
                        SQLITE_SEGMENT_COLUMN, '== ?',
                        ))
                    values = (key, segment)
                    self._connection.cursor().execute(statement, values)
            else:
                seg = b''.join(tuple(
                    rn.to_bytes(length=2, byteorder='big')
                    for rn in sorted(recnums)))
                statement = ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_COUNT_COLUMN, '= ?',
                    'where',
                    self._fd_name, '== ? and',
                    SQLITE_SEGMENT_COLUMN, '== ?',
                    ))
                values = (count, key, segment)
                self._connection.cursor().execute(statement, values)
                values = (seg, record[3])
                self.get_primary_database().set_segment_records(values)
        else:
            if record[3] != record_number:
                return
            statement = ' '.join((
                'delete from',
                self._fd_name,
                'where',
                self._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))
            values = (key, segment)
            self._connection.cursor().execute(statement, values)
    
    def segment_put(self, key, segment, record_number):
        """Add record_number to segment for key and write to database"""
        # See dbduapi.py DBbitduSecondary.defer_put for model.
        # The dance to find the segment record is a reason to convert these
        # secondary databases from DUP to NODUP.  Also a NODUP database allows
        # implementation equivalent to DPT 'FOR EACH VALUE' directly and easy
        # counting of values for manipulation of scrollbar sliders.
        # Assumption is that new records usually go in last segment for value.
        statement = ' '.join((
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
            'limit 1',
            ))
        values = (key, segment)
        record = self._connection.cursor().execute(statement, values).fetchone()
        if record is None:
            statement = ' '.join((
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
            values = (key, segment, 1, record_number)
            self._connection.cursor().execute(statement, values)
            return
        if record[2] > DB_CONVERSION_LIMIT:
            bs = self.get_primary_database().get_segment_records(record[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recnums = Bitarray()
            recnums.frombytes(bs)
            # maybe ignore possibility record_number already present
            if recnums[record_number]:
                return
            recnums[record_number] = True
            statement = ' '.join((
                'update',
                self._fd_name,
                'set',
                SQLITE_COUNT_COLUMN, '= ?',
                'where',
                self._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))
            values = (recnums.count(), key, record[1])
            self._connection.cursor().execute(statement, values)
            values = (recnums.tobytes(), record[3])
            self.get_primary_database().set_segment_records(values)
        elif record[2] > 1:
            bs = self.get_primary_database().get_segment_records(record[3])
            if bs is None:
                raise DatabaseError('Segment record missing')
            recnums = {int.from_bytes(bs[i:i+2], byteorder='big')
                       for i in range(0, len(bs), 2)}
            # maybe ignore possibility record_number already present
            if record_number in recnums:
                return
            recnums.add(record_number)
            count = len(recnums)
            if count > DB_CONVERSION_LIMIT:
                seg = EMPTY_BITARRAY.copy()
                for rn in recnums:
                    seg[rn] = True
                statement = ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_COUNT_COLUMN, '= ?',
                    'where',
                    self._fd_name, '== ? and',
                    SQLITE_SEGMENT_COLUMN, '== ?',
                    ))
                values = (len(recnums), key, record[1])
                self._connection.cursor().execute(statement, values)
                values = (seg.tobytes(), record[3])
                self.get_primary_database().set_segment_records(values)
            else:
                seg = b''.join(tuple(
                    rn.to_bytes(length=2, byteorder='big')
                    for rn in sorted(recnums)))
                statement = ' '.join((
                    'update',
                    self._fd_name,
                    'set',
                    SQLITE_COUNT_COLUMN, '= ?',
                    'where',
                    self._fd_name, '== ? and',
                    SQLITE_SEGMENT_COLUMN, '== ?',
                    ))
                values = (len(recnums), key, record[1])
                self._connection.cursor().execute(statement, values)
                values = (seg, record[3])
                self.get_primary_database().set_segment_records(values)
        else:
            if record[3] > record_number:
                rnlist = b''.join(
                    (record_number.to_bytes(length=2, byteorder='big'),
                     record[3].to_bytes(length=2, byteorder='big'),
                     ))
            elif record[3] < record_number:
                rnlist = b''.join((
                    record[3].to_bytes(length=2, byteorder='big'),
                    record_number.to_bytes(length=2, byteorder='big'),
                    ))
            else:
                return
            values = (rnlist,)
            row = self.get_primary_database().insert_segment_records(values)
            statement = ' '.join((
                'update',
                self._fd_name,
                'set',
                SQLITE_COUNT_COLUMN, '= ?',
                'where',
                self._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                ))
            values = (2, key, row)
            self._connection.cursor().execute(statement, values)


class CursorSqlite3(Cursor):
    
    """Define bsddb3 style cursor methods on a sqlite table.

    Primary and secondary database, and others, should be read as the Berkeley
    DB usage.  This class emulates interaction with a Berkeley DB database via
    the Python bsddb3 module.
    
    Methods added:

    None
    
    Methods overridden:

    close
    database_cursor_exists
    get_converted_partial
    get_converted_partial_with_wildcard
    get_partial
    get_partial_with_wildcard
    refresh_recordset
    
    Methods extended:

    __init__
    
    Notes:

    self._cursor is am sqlite3 cursor.  The database.Cursor class, which is the
    direct superclass of this class, is intended to provide bsddb3 style cursor
    methods so the cursor is defined to persist for the lifetime of the Cursor
    instance.

    """

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the sqlite3 engine."""
        super(CursorSqlite3, self).__init__(dbset)
        self._most_recent_row_read = False
        if dbset._connection is not None:
            self._cursor = dbset._connection.cursor()

    def close(self):
        """Delete database cursor"""
        try:
            del self._dbset._clientcursors[self]
        except:
            pass
        self._cursor = None
        self._dbset = None
        self.set_partial_key(None)
        self._most_recent_row_read = False

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise"""
        return bool(self._cursor)

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass

    def get_partial(self):
        """return self._partial"""
        return self._partial

    def get_converted_partial(self):
        """return self._partial as it would be held on database"""
        # See comment at get_converted_partial_with_wildcard().
        return self._partial#.encode()

    def get_partial_with_wildcard(self):
        """return self._partial with wildcard suffix appended"""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """return converted self._partial with wildcard suffix appended"""
        # Code replaced:
        #return self._dbset.get_converter(USE_BYTES)(
        #    ''.join((self._partial, '*')))
        # implies self._partial is str always: so calling the converter, which
        # tests if argument is str. is pointless.
        # Sqlite will encode str internally so the encode() is commented.
        return ''.join((self._partial, '*'))#.encode()


class CursorSqlite3Primary(CursorSqlite3):
    
    """Define bsddb3 cursor methods for primary database on a sqlite table.
    
    Methods added:

    None
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    setat
    set_partial_key
    
    Methods extended:

    None
    
    """

    def count_records(self):
        """return record count or None if cursor is not usable"""
        statement = ' '.join((
            'select count(*) from',
            self._dbset._fd_name,
            ))
        values = ()
        return self._cursor.execute(statement, values).fetchone()[0]

    def first(self):
        """Return first record taking partial key into account"""
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        statement = ' '.join((
            'select count(*) from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '<= ?',
            ))
        values = (record[0],)
        position = self._cursor.execute(statement, values).fetchone()[0]
        return position

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, 'desc',
                'limit 1',
                'offset ?',
                ))
            values = (str(-1 - position),)
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name,
                'limit 1',
                'offset ?',
                ))
            values = (str(position - 1),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account"""
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'order by',
            self._dbset._fd_name, 'desc',
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def set_partial_key(self, partial):
        """Set partial key to None for primary cursor"""
        #self._partial = partial
        #self._most_recent_row_read = False
        self._partial = None

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '>= ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (key,)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '> ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def prev(self):
        """Return previous record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.last()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '< ?',
            'order by',
            self._dbset._fd_name, 'desc',
            'limit 1',
            ))
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            if not record[0].startswith(self.get_converted_partial()):
                return None
        statement = ' '.join((
            'select * from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '== ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (record[0],)
        row = self._cursor.execute(statement, values).fetchone()
        if row:
            self._most_recent_row_read = row
        return row


class CursorSqlite3Secondary(CursorSqlite3):
    
    """Define bsddb3 cursor methods for secondary database on a sqlite table.
    
    Methods added:

    None
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    setat
    set_partial_key
    
    Methods extended:

    None
    
    """

    def count_records(self):
        """return record count or None if cursor is not usable"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                ))
            values = ()
        else:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        return self._cursor.execute(statement, values).fetchone()[0]

    def first(self):
        """Return first record taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                'order by ',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        if self.get_partial() is None:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '< ?',
                ))
            values = (record[0],)
        elif self.get_partial() is False:
            return 0
        else:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '< ?',
                ))
            values = (self.get_converted_partial_with_wildcard(), record[0])
        position = self._cursor.execute(statement, values).fetchone()[0]
        if self.get_partial() is None:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '<= ?',
                ))
            values = record[:]
        else:
            statement = ' '.join((
                'select count(*) from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '<= ?',
                ))
            values = [self.get_converted_partial_with_wildcard()]
            values.extend(record)
        return position + self._cursor.execute(statement, values).fetchone()[0]

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            if self.get_partial() is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    self._dbset._primaryname, 'desc',
                    'limit 1',
                    'offset ?',
                    ))
                values = (str(-1 - position),)
            elif self.get_partial() is False:
                return None
            else:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    self._dbset._primaryname, 'desc',
                    'limit 1',
                    'offset ?',
                    ))
                values = (
                    self.get_converted_partial_with_wildcard(),
                    str(-1 - position),
                    )
        else:
            if self.get_partial() is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'order by',
                    self._dbset._fd_name, ',', self._dbset._primaryname,
                    'limit 1',
                    'offset ?',
                    ))
                values = (str(position - 1),)
            elif self.get_partial() is False:
                return None
            else:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ?',
                    'order by',
                    self._dbset._fd_name, ',', self._dbset._primaryname,
                    'limit 1',
                    'offset ?',
                    ))
                values = (
                    self.get_converted_partial_with_wildcard(),
                    str(position - 1),
                    )
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, 'desc', ',',
                self._dbset._primaryname, 'desc',
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                self._dbset._primaryname, 'desc',
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial
        self._most_recent_row_read = False

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '>= ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = (key,)
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '>= ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(), key)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        if self.get_partial() is None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '> ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = self._most_recent_row_read[:]
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, '> ?',
                    'order by',
                    self._dbset._fd_name, ',', self._dbset._primaryname,
                    'limit 1',
                    ))
                values = (values[0],)
                row = self._cursor.execute(statement, values).fetchone()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '> ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1',
                ))
            values = [self.get_converted_partial_with_wildcard()]
            values.extend(self._most_recent_row_read)
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ? and',
                    self._dbset._fd_name, '> ?',
                    'order by',
                    self._dbset._fd_name, ',', self._dbset._primaryname,
                    'limit 1',
                    ))
                values = (values[0],)
                row = self._cursor.execute(statement, values).fetchone()
        if row is not None:
            self._most_recent_row_read = row
        return row

    def prev(self):
        """Return previous record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.last()
        elif self._most_recent_row_read is None:
            return None
        if self.get_partial() is None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '< ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                self._dbset._primaryname, 'desc',
                'limit 1',
                ))
            values = self._most_recent_row_read[:]
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, '< ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    self._dbset._primaryname, 'desc',
                    'limit 1',
                    ))
                values = (values[0],)
                row = self._cursor.execute(statement, values).fetchone()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '< ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                self._dbset._primaryname, 'desc',
                'limit 1',
                ))
            values = [self.get_converted_partial_with_wildcard()]
            values.extend(self._most_recent_row_read)
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = ' '.join((
                    'select * from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ? and',
                    self._dbset._fd_name, '< ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    self._dbset._primaryname, 'desc',
                    'limit 1',
                    ))
                values = (values[0],)
                row = self._cursor.execute(statement, values).fetchone()
        if row is not None:
            self._most_recent_row_read = row
        return row

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            if not record[0].startswith(self.get_converted_partial()):
                return None
        if self.get_partial() is not None:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '== ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1'))
            values = [self.get_converted_partial_with_wildcard()]
            values.extend(record)
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                self._dbset._primaryname, '== ?',
                'order by',
                self._dbset._fd_name, ',', self._dbset._primaryname,
                'limit 1'))
            values = record
        row = self._cursor.execute(statement, values).fetchone()
        if row:
            self._most_recent_row_read = row
        return row


class CursorSqlite3bit(CursorSqlite3):
    
    """Define bsddb3 style cursor methods on a segmented sqlite table.

    Primary and secondary database, and others, should be read as the Berkeley
    DB usage.  This class emulates interaction with a Berkeley DB database via
    the Python bsddb3 module.

    Segmented should be read as the DPT database engine usage.

    The value part of (key, value) on primary or secondary databases is either:

        primary key (segment and record number)
        reference to a list of primary keys for a segment
        reference to a bit map of primary keys for a segment

    References are to records on RECNO databases, one each for lists and bit
    maps, containing the primary keys.

    Each rowid is mapped to a bit in the bitmap associated with a segment.
    
    Methods added:

    None
    
    Methods overridden:

    nearest
    next
    prev
    
    Methods extended:

    __init__

    Notes:

    CursorSqlite3bitPrimary and CursorSqlite3bitSecondary bypass
    CursorSqlite3Primary and CursorSqlite3Secondary as superclasses so override
    nearest next and prev as the CursorSqlite3 versions are not appropriate in
    the ...bit... versions of the class.
    
    """
    # The refresh_recordset may be relevent in this class

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the Berkeley DB engine."""
        super(CursorSqlite3bit, self).__init__(dbset, keyrange=keyrange)
        self._current_segment = None
        self._current_segment_number = None
        self._current_record_number_in_segment = None

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        raise DatabaseError('nearest should be implemented in subclass')

    def next(self):
        """Return next record taking partial key into account"""
        raise DatabaseError('next should be implemented in subclass')

    def prev(self):
        """Return previous record taking partial key into account"""
        raise DatabaseError('prev should be implemented in subclass')


# Maybe CursorSqlite3bitPrimary should be subclass of CursorSqlite3Primary as
# the table definition is same but 'existence segment' table is available.
class CursorSqlite3bitPrimary(CursorSqlite3bit):
    
    """Define bsddb3 cursor methods for primary database on a segmented table.
    
    Methods added:

    None
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    setat
    set_partial_key
    
    Methods extended:

    None
    
    """
    # The refresh_recordset may be relevent in this class

    def count_records(self):
        """return record count or None if cursor is not usable"""
        statement = ' '.join((
            'select count(*) from',
            self._dbset._fd_name,
            ))
        values = ()
        return self._cursor.execute(statement, values).fetchone()[0]

    def first(self):
        """Return first record taking partial key into account"""
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        statement = ' '.join((
            'select count(*) from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '<= ?',
            ))
        values = (record[0],)
        position = self._cursor.execute(statement, values).fetchone()[0]
        return position

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, 'desc',
                'limit 1',
                'offset ?',
                ))
            values = (str(-1 - position),)
        else:
            statement = ' '.join((
                'select * from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name,
                'limit 1',
                'offset ?',
                ))
            values = (str(position - 1),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account"""
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'order by',
            self._dbset._fd_name, 'desc',
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '>= ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (key,)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '> ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def prev(self):
        """Return previous record taking partial key into account"""
        if self._most_recent_row_read is False:
            return self.last()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '< ?',
            'order by',
            self._dbset._fd_name, 'desc',
            'limit 1',
            ))
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            if not record[0].startswith(self.get_partial()):
                return None
        statement = ' '.join((
            'select',
            self._dbset._fd_name, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset._fd_name,
            'where',
            self._dbset._fd_name, '== ?',
            'order by',
            self._dbset._fd_name,
            'limit 1',
            ))
        values = (record[0],)
        row = self._cursor.execute(statement, values).fetchone()
        if row:
            self._most_recent_row_read = row
        return row

    def set_partial_key(self, partial):
        """Set partial key to None for primary cursor"""
        #self._partial = partial
        #self._most_recent_row_read = False
        self._partial = None


class CursorSqlite3bitSecondary(CursorSqlite3bit):
    
    """Define bsddb3 cursor methods for secondary database on a segmented table.
    
    Methods added:

    get_segment
    set_current_segment
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    setat
    set_partial_key
    
    Methods extended:

    None
    
    """
    # The refresh_recordset may be relevent in this class

    def count_records(self):
        """Return record count."""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                SQLITE_COUNT_COLUMN, # to avoid sum() overflow
                'from',
                self._dbset._fd_name,
                ))
            values = ()
        else:
            statement = ' '.join((
                'select',
                SQLITE_COUNT_COLUMN, # to avoid sum() overflow
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        count = 0
        for r in self._cursor.execute(statement, values):
            count += r[0]
        return count

    def first(self):
        """Return first record taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).first()

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(value, DB_SEGMENT_SIZE)
        # Get position of record relative to start point
        if not self.get_partial():
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                ))
            values = ()
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        gpd = self._dbset.get_primary_database()
        position = 0
        for r in self._cursor.execute(statement, values):
            if r[0] < key:
                position += r[2]
            elif r[1] < segment_number:
                position += r[2]
            elif r[1] > segment_number:
                break
            else:
                if r[2] > DB_CONVERSION_LIMIT:
                    segment = SegmentBitarray(
                        segment_number,
                        None,
                        records=gpd.get_segment_records(r[3]))
                elif r[2] > 1:
                    segment = SegmentList(
                        segment_number,
                        None,
                        records=gpd.get_segment_records(r[3]))
                else:
                    segment = SegmentInt(
                        segment_number,
                        None,
                        records=r[3].to_bytes(2, byteorder='big'))
                position += segment.get_position_of_record_number(record_number)
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None"""
        if position is None:
            return None
        # Start at first or last record whichever is likely closer to position
        if position < 0:
            is_step_forward = False
            position = -1 - position
            if not self.get_partial():
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    ))
                values = ()
            else:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    ))
                values = (self.get_converted_partial_with_wildcard(),)
        else:
            is_step_forward = True
            if not self.get_partial():
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'order by',
                    self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                    ))
                values = ()
            else:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ?',
                    'order by',
                    self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                    ))
                values = (self.get_converted_partial_with_wildcard(),)
        # Get record at position relative to start point
        gpd = self._dbset.get_primary_database()
        count = 0
        for r in self._cursor.execute(statement, values):
            count += r[2]
            if count < position:
                continue
            count -= position
            if r[2] > DB_CONVERSION_LIMIT:
                segment = SegmentBitarray(
                    r[1],
                    None,
                    records=gpd.get_segment_records(r[3]))
            elif r[2] > 1:
                segment = SegmentList(
                    r[1],
                    None,
                    records=gpd.get_segment_records(r[3]))
            else:
                segment = SegmentInt(
                    r[1],
                    None,
                    records=r[3].to_bytes(2, byteorder='big'))
            record_number = segment.get_record_number_at_position(
                count, is_step_forward)
            if record_number is not None:
                return r[0], record_number
            break
        return None

    def last(self):
        """Return last record taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'order by',
                self._dbset._fd_name, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).last()

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '>= ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (key,)
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '>= ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(), key)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).last()

    def next(self):
        """Return next record taking partial key into account"""
        record = self._current_segment.next()
        if record is not None:
            return record
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '> ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self._current_segment._key, self._current_segment_number)
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, '> ?',
                    'order by',
                    self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                    'limit 1',
                    ))
                values = (self._current_segment._key,)
                self._most_recent_row_read = self._cursor.execute(
                    statement, values).fetchone()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '> ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (
                self.get_converted_partial_with_wildcard(),
                self._current_segment._key,
                self._current_segment_number,
                )
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ? and',
                    self._dbset._fd_name, '> ?',
                    'order by',
                    self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                    'limit 1',
                    ))
                values = (
                    self.get_converted_partial_with_wildcard(),
                    self._current_segment._key,
                    )
                self._most_recent_row_read = self._cursor.execute(
                    statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).first()

    def prev(self):
        """Return previous record taking partial key into account"""
        record = self._current_segment.prev()
        if record is not None:
            return record
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '< ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = (self._current_segment._key, self._current_segment_number)
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, '< ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    'limit 1',
                    ))
                values = (self._current_segment._key,)
                self._most_recent_row_read = self._cursor.execute(
                    statement, values).fetchone()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '< ?',
                'order by',
                self._dbset._fd_name, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = (
                self.get_converted_partial_with_wildcard(),
                self._current_segment._key,
                self._current_segment_number,
                )
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset._fd_name, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset._primaryname,
                    'from',
                    self._dbset._fd_name,
                    'where',
                    self._dbset._fd_name, 'glob ? and',
                    self._dbset._fd_name, '< ?',
                    'order by',
                    self._dbset._fd_name, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    'limit 1',
                    ))
                values = (
                    self.get_converted_partial_with_wildcard(),
                    self._current_segment._key,
                    )
                self._most_recent_row_read = self._cursor.execute(
                    statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).last()

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            if not record[0].startswith(self.get_partial()):
                return None
        segment_number, record_number = divmod(record[1], DB_SEGMENT_SIZE)
        if self.get_partial() is not None:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, 'glob ? and',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (
                self.get_converted_partial_with_wildcard(),
                record[0],
                segment_number,
                )
        else:
            statement = ' '.join((
                'select',
                self._dbset._fd_name, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset._primaryname,
                'from',
                self._dbset._fd_name,
                'where',
                self._dbset._fd_name, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                'order by',
                self._dbset._fd_name, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (record[0], segment_number)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        segment = self.get_segment(*row)
        if record_number not in segment:
            return None
        self._current_segment = segment
        self._current_segment_number = row[1]
        self._most_recent_row_read = row
        return segment.setat(record[1])

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial

    def get_segment(self, key, segment_number, count, record_number):
        """Return a SegmentBitarray, SegmentInt, or SegmentList instance.

        Arguments are the 4-tuple segment reference returned by fetchone().

        """
        if count > DB_CONVERSION_LIMIT:
            if self._current_segment_number == segment_number:
                if key == self._current_segment._key:
                    return self._current_segment
            return SegmentBitarray(
                segment_number,
                key,
                records=self._dbset.get_primary_database(
                    ).get_segment_records(record_number))
        elif count > 1:
            if self._current_segment_number == segment_number:
                if key == self._current_segment._key:
                    return self._current_segment
            return SegmentList(
                segment_number,
                key,
                records=self._dbset.get_primary_database(
                    ).get_segment_records(record_number))
        else:
            return SegmentInt(
                segment_number,
                key,
                records=record_number.to_bytes(2, byteorder='big'))

    def set_current_segment(self, segment_reference):
        """Return a SegmentBitarray, SegmentInt, or SegmentList instance.

        Argument is the 4-tuple segment reference returned by fetchone().

        """
        self._current_segment = self.get_segment(*segment_reference)
        self._current_segment_number = segment_reference[1]
        return self._current_segment

            
class Sqlite3Segment(object):
    
    """Define a sqlite3 table to store inverted record number lists or bitmaps.

    Methods added:

    append
    close
    delete
    get
    open_root
    put

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, dbfile, segment_type):
        """Define segment file for segment type of name in file description.
        
        dbfile=file name relative to environment home directory
        segment_type=representation style for inverted list of record numbers

        For large numbers of records fixed sized bit maps representing record
        numbers relative to the base for the segment.  Otherwise a list of
        record numbers relative to the base.
        
        """
        super(Sqlite3Segment, self).__init__()
        self._seg_dbfile = (SUBFILE_DELIMITER * 2).join((dbfile, segment_type))
        self._seg_object = None

    def close(self):
        """Close inverted index DB."""
        self._seg_object = None

    def open_root(self, sqconn):
        """Create inverted index DB in dbenv."""
        try:
            self._seg_object = sqconn
        except:
            raise
        try:
            statement = ' '.join((
                'create table if not exists', self._seg_dbfile,
                '(',
                self._seg_dbfile,
                'integer primary key', ',',
                SQLITE_VALUE_COLUMN,
                ')',
                ))
            self._seg_object.cursor().execute(statement)
        except:
            self._seg_object = None
            raise

    def get(self, key):
        """Get a segment record from the database."""
        statement = ' '.join((
            'select',
            SQLITE_VALUE_COLUMN,
            'from',
            self._seg_dbfile,
            'where',
            self._seg_dbfile, '== ?',
            'limit 1',
            ))
        values = (key,)
        try:
            return self._seg_object.cursor(
                ).execute(statement, values).fetchone()[0]
        except TypeError:
            return None

    def delete(self, key):
        """Delete a segment record from the database."""
        statement = ' '.join((
            'delete from',
            self._seg_dbfile,
            'where',
            self._seg_dbfile, '== ?',
            ))
        values = (key,)
        self._seg_object.cursor().execute(statement, values)

    def put(self, key, value):
        """Put a segment record on the database using key"""
        statement = ' '.join((
            'update',
            self._seg_dbfile,
            'set',
            SQLITE_VALUE_COLUMN, '= ?',
            'where',
            self._seg_dbfile, '== ?',
            ))
        values = (value, key)
        self._seg_object.cursor().execute(statement, values)

    def append(self, value):
        """Append a segment record on the database using a new key"""
        statement = ' '.join((
            'insert into',
            self._seg_dbfile,
            '(',
            SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? )',
            ))
        values = (value,)
        return self._seg_object.cursor().execute(statement, values).execute(
                ' '.join((
                    'select last_insert_rowid() from',
                    self._seg_dbfile))).fetchone()[0]

            
class Sqlite3ExistenceBitMap(Sqlite3Segment):
    
    """Sqlite3 table to store record existence bit map keyed by segment number.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    open_root

    Properties:
    
    segment_count
    
    """

    def __init__(self, dbfile):
        """Define dbfile in environment for segment record bit maps.
        
        dbfile=file name relative to environment home directory
        
        """
        super(Sqlite3ExistenceBitMap, self).__init__(dbfile, 'exist')
        self._segment_count = None

    @property
    def segment_count(self):
        """Return number of records in segment."""
        return self._segment_count

    @segment_count.setter
    def segment_count(self, segment_number):
        """Set segment count from 0-based segment_number if greater"""
        if segment_number > self._segment_count:
            self._segment_count = segment_number + 1
    
    def open_root(self, sqconn):
        """Create inverted index DB in dbenv."""
        super(Sqlite3ExistenceBitMap, self).open_root(sqconn)
        statement = ' '.join(('select count(*) from', self._seg_dbfile))
        self._segment_count = self._seg_object.cursor(
            ).execute(statement).fetchone()[0]

            
class Sqlite3bitControlFile(object):
    
    """Define sqlite3 table for control information about the sqlite3 tables.

    Methods added:

    close
    get_control_database
    open_root

    Methods overridden:

    None

    Methods extended:

    __init__

    Properties:

    control_file

    Notes

    The method names used in Sqlite3bitapiPrimaryFile and superclasses are used
    where possible.  But this file is primary DB_BTREE NODUP.  It is used by
    all FileControl instances.
    
    """

    def __init__(self, control_file='control'):
        """File control database for all DB files."""
        super(Sqlite3bitControlFile, self).__init__()
        self._control_file = ''.join((SUBFILE_DELIMITER * 3, control_file))
        self._control_object = None

    def open_root(self, sqconn):
        """Create file control database in environment"""
        self._control_object = sqconn
        try:
            statement = ' '.join((
                'create table if not exists', self._control_file,
                '(',
                self._control_file, ',',
                SQLITE_VALUE_COLUMN, ',',
                'primary key',
                '(',
                self._control_file, ',',
                SQLITE_VALUE_COLUMN,
                ') )',
                ))
            self._control_object.cursor().execute(statement)
        except:
            self._control_object = None
            raise

    def close(self):
        """Close file control database."""
        self._control_object = None

    def get_control_database(self):
        """Return the database containing file control records."""
        return self._control_object

    @property
    def control_file(self):
        """Return the name which is both primary column and table name."""
        return self._control_file


class FileControl(object):
    
    """Freed resource data for a segmented file.

    Methods added:

    None
    
    Methods overridden:

    None
    
    Methods extended:

    __init__
    
    """

    def __init__(self, dbfile):
        """Define the file control data."""
        super(FileControl, self).__init__()

        # Primary or Secondary file instance whose segment reuse is handled
        self._dbfile = dbfile


class FileControlPrimary(FileControl):
    
    """Freed resource data for a segmented file.

    Methods added:

    get_lowest_freed_record_number
    note_freed_record_number
    note_freed_record_number_segment
    _read_exists_segment
    
    Methods overridden:

    None
    
    Methods extended:

    __init__
    
    Properties:

    freed_record_number_pages
    
    Notes

    Introduced to keep track of pages on the existence bit map that contain
    freed record numbers that can be reused.

    The list of existence bit map page numbers containing freed record numbers
    is cached.

    """

    def __init__(self, *args):
        """Define the file control data for primary files."""
        super(FileControlPrimary, self).__init__(*args)
        self._freed_record_number_pages = None

    @property
    def freed_record_number_pages(self):
        """Return existence bit map record numbers available for re-use"""
        if self._freed_record_number_pages is None:
            return None
        return bool(self._freed_record_number_pages)

    def note_freed_record_number(self, record_number):
        """Adjust segment of high and low freed record numbers"""
        self.note_freed_record_number_segment(
            *divmod(record_number, DB_SEGMENT_SIZE))

    def note_freed_record_number_segment(
        self, segment, record_number_in_segment):
        """Adjust segment of high and low freed record numbers"""
        if self._freed_record_number_pages is None:
            self._freed_record_number_pages = []
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self._dbfile._control_database.control_file,
                'where',
                self._dbfile._control_database.control_file, '== ?',
                'order by',
                SQLITE_VALUE_COLUMN,
                )),
            values = (b'B',)
            for record in self._dbfile.get_control_database(
                ).cursor().execute(statement, values):
                self._freed_record_number_pages.append(
                    int.from_bytes(record[0], byteorder='big'))
        insert = bisect.bisect_left(self._freed_record_number_pages, segment)
        if self._freed_record_number_pages[insert] == segment:
            return
        self._freed_record_number_pages.insert(insert, segment)
        statement = ' '.join((
            'insert into',
            self._dbfile._control_database.control_file,
            '(',
            self._dbfile._control_database.control_file, ',', SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? , ? )',
            ))
        values = (b'B', page)
        self._dbfile.get_control_database().cursor().execute(statement, values)

    def get_lowest_freed_record_number(self):
        """Return low record number in segments with freed record numbers"""
        if self._freed_record_number_pages is None:
            self._freed_record_number_pages = []
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self._dbfile._control_database.control_file,
                'where',
                self._dbfile._control_database.control_file, '== ?',
                'order by',
                SQLITE_VALUE_COLUMN,
                ))
            values = (b'E',)
            for record in self._dbfile.get_control_database(
                ).cursor().execute(statement, values):
                self._freed_record_number_pages.append(record[0])
        while len(self._freed_record_number_pages):
            s = self._freed_record_number_pages[0]
            lfrns = self._read_exists_segment(s)
            if lfrns is None:
                # Do not reuse record number on segment of high record number
                return 0
            try:
                first_zero_bit = lfrns.index(False)
            except ValueError:
                statement = ' '.join((
                    'delete from',
                    self._dbfile._control_database.control_file,
                    'where',
                    self._dbfile._control_database.control_file, '== ? and',
                    SQLITE_VALUE_COLUMN, '== ?',
                    ))
                values = (b'E', s)
                self._dbfile.get_control_database(
                    ).cursor().execute(statement, values)
                del self._freed_record_number_pages[0]
                continue
            return s * DB_SEGMENT_SIZE + first_zero_bit + 1
        else:
            return 0 # record number when inserting into RECNO database

    def _read_exists_segment(self, segment_number):
        """Return existence bit map for segment_number if not high segment."""
        # record keys are 1-based but segment_numbers are 0-based
        page = segment_number + 1
        if page < self._dbfile.get_existence_bits().segment_count():
            ebm = Bitarray()
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self._dbfile._control_database.control_file,
                'where',
                self._dbfile._control_database.control_file, '== ?',
                'limit 1',
                ))
            values = (page,)
            return ebm.frombytes(
                self._dbfile.get_existence_bits_database(
                    ).execute(statement, values).fetchone()[0])
        return None

    def _read_exists_segment(self, segment_number):
        """Return existence bit map for segment_number if not high segment."""
        # record keys are 1-based but segment_numbers are 0-based
        page = segment_number + 1
        if page < self._dbfile.get_existence_bits().segment_count():
            ebm = Bitarray()
            return ebm.frombytes(self._dbfile.get_existence_bits().get(page)[0])
        return None
