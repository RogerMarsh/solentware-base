# dbapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Object database using Berkeley DB.

List of classes

CursorDB - Define cursor on file and access methods
CursorDBbit - Extend CursorDB for segmented databases
CursorDBbitPrimary - Extend CursorDBbit for primary segmented databases
CursorDBbitSecondary - Extend CursorDBbit for secondary segmented databases
CursorDBPrimary - Extend CursorDB for primary databases
CursorDBSecondary - Extend CursorDB for secondary databases
_DatabaseEncoders
DBapiError - Exceptions
_DBapi - Define database and file and record level access methods
DBapi - _DBapi without file segment support
DBFile - File level access to each file in database (Open, Close)
DBPrimary - Record level access to each primary file in database
DBPrimaryFile
DBSecondary - Record level access to each secondary file in database
DBSecondaryFile
DBbitapi - Add file segment support to _DBapi using bit mapped record numbers
DBbitControlFile - File and record level access to file control data
DBbitPrimaryFile
DBbitSecondaryFile
DBbitPrimary
DBbitSecondary
DBExistenceBitMap
DBSegment - File level access to inverted record numbers
DBSegmentBits - Represent sets of record numbers with bit maps
DBSegmentList - Represent sets of record numbers with lists
FileControl - Freed resource data (segment and record numbers) for a file
FileControlPrimary - Freed resource data (record numbers) for a file
FileControlSecondary - Freed resource data (list or bit map segments) for a file

Segmented databases take account of the local density of values per key on
secondary databases.  A primary database, always RECNO, is seen as a sequence
of fixed size intervals called segments.  Secondary databases have zero or one
keys per segment for each key.  The value associated with each segment key is a
number, a list of numbers, or a bit map, representing the record numbers in the
segment referenced by the key.  The absence of a segment key means the key has
no values in that segment.  Bit maps are fixed length and record lists are
variable length.  The maximum byte size of a list is less than or equal to the
byte size of a bit map.

Idea taken from DPT, an emulation of Model 204 which runs on Microsoft Windows.

"""

import os
import subprocess
from ast import literal_eval

import sys
_platform_win32 = sys.platform == 'win32'
_python_version = '.'.join(
    (str(sys.version_info[0]), 
     str(sys.version_info[1])))
del sys

from .api.bytebit import Bitarray, SINGLEBIT

# bsddb removed from Python 3.n
try:
    from bsddb3.db import (
        DB_KEYLAST, DB_CURRENT, DB_DUP, DB_DUPSORT, DB_NODUPDATA,
        DB_BTREE, DB_HASH, DB_RECNO, DB_UNKNOWN,
        DBEnv, DB, DB_CREATE, DB_FAST_STAT,
        DBKeyExistError, DBNotFoundError,
        )
except ImportError:
    from bsddb.db import (
        DB_KEYLAST, DB_CURRENT, DB_DUP, DB_DUPSORT, DB_NODUPDATA,
        DB_BTREE, DB_HASH, DB_RECNO, DB_UNKNOWN,
        DBEnv, DB, DB_CREATE, DB_FAST_STAT,
        DBKeyExistError, DBNotFoundError,
        )

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
    DB_DEFER_FOLDER, SECONDARY_FOLDER,
    PRIMARY, SECONDARY, FILE, HASH_DUPSORT, BTREE_DUPSORT,
    PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, DB_FIELDATTS,
    DUP, BTREE, HASH, RECNO, DUPSORT,
    FIELDS, SPT, KEY_VALUE,
    USE_BYTES,
    DB_SEGMENT_SIZE_BYTES,
    DB_SEGMENT_SIZE,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    DB_CONVERSION_LIMIT,
    SUBFILE_DELIMITER,
    )

_DB_CONST_MAP = {
    DUP: DB_DUP, BTREE: DB_BTREE, HASH: DB_HASH,
    RECNO: DB_RECNO, DUPSORT: DB_DUPSORT,
    HASH_DUPSORT: (DB_HASH, DB_DUPSORT),
    BTREE_DUPSORT: (DB_BTREE, DB_DUPSORT),
    }


class DBapiError(DatabaseError):
    pass

        
class _DatabaseEncoders(object):
    
    """Define default record key encoder and decoder.

    Methods added:

    decode_record_number
    encode_record_number
    is_engine_uses_bytes
    is_engine_uses_str

    Methods overridden:

    None

    Methods extended:

    None
    
    """

    def encode_record_number(self, key):
        """Return base 256 string for integer with left-end most significant.

        Typically used to convert Berkeley DB primary key to secondary index
        format.
        
        """
        return repr(key).encode()

    def decode_record_number(self, skey):
        """Return integer from base 256 string with left-end most significant.

        Typically used to convert Berkeley DB primary key held on secondary
        index.

        """
        return literal_eval(skey.decode())

    def encode_record_selector(self, key):
        """Return base 256 string for integer with left-end most significant.

        Typically used to convert Berkeley DB primary key to secondary index
        format.
        
        """
        return key.encode()


class _DBapi(Database, _DatabaseEncoders):
    
    """Define a Berkeley DB database structure.
    
    Primary databases are created as DB_RECNO.
    Secondary databases are DB_BTREE with DB_DUPSORT set.

    Primary and secondary terminology comes from Berkeley DB documentation but
    the association technique is not used.

    The value part of a secondary key:value is a (segment, reference, count)
    tuple where segment follows DPT terminology.  Reference can be a record
    number relative to segment start, a reference to a list of record numbers,
    or a reference to a bitmap representing such record numbers.  Count is the
    number of records referenced by this value.

    Secondary databases are supported by two DB_RECNO databases, one for lists
    of record numbers and one for bitmap representations of record numbers. The
    reference is the key into the relevant DB_RECNO database.

    Methods added:

    allocate_and_open_contexts
    cede_contexts_to_process
    close_contexts
    do_database_task
    do_deferred_updates
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
    set_defer_update
    unset_defer_update

    Methods overridden:

    backout
    close_context
    close_database
    commit
    db_compatibility_hack
    decode_as_primary_key
    delete_instance
    edit_instance
    encode_primary_key
    exists
    get_database
    get_database_folder
    get_first_primary_key_for_index_key
    get_packed_key
    get_primary_record
    is_primary
    is_primary_recno
    is_recno
    database_cursor
    open_context
    put_instance
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
        DBnames,
        DBhome,
        DBenvironment,
        *args,
        **kwargs):
        """Define database structure.
        
        DBhome = full path for database directory
        DBnames = {name:{primary:name,
                         secondary:{name:name...},
                         }, ...}
        DBenvironment = {<DB property>:<value>, ...}
        primary_class = class implementing access to primary databases
        secondary_class = class implementing access to secondary databases

        """
        super(_DBapi, self).__init__(*args, **kwargs)
        # The DBenv object
        self._dbenv = None
        
        # Parameters for setting up the DBenv object
        self._DBenvironment = DBenvironment
            
        basefile = os.path.basename(DBhome)

        # Map db names to file names relative to DBhome (+ suffix)
        DBfiles = dict()

        for n in DBnames:
            f = DBnames[n].setdefault(PRIMARY, n)
            if f is None:
                DBnames[n][PRIMARY] = n
                f = n
            if f in DBfiles:
                msg = ' '.join(['DB name', f, 'requested for primary name',
                                n, 'is already specified'])
                raise DBapiError(msg)
            if SUBFILE_DELIMITER in f:
                raise DBapiError(''.join(
                    ('Primary file name ',
                     f,
                     ' contains "',
                     SUBFILE_DELIMITER,
                     '", which is not allowed.',
                     )))
            DBfiles[f] = f

        for n in DBnames:
            sec = DBnames[n].setdefault(SECONDARY, dict())
            for s in sec:
                if sec[s] is None:
                    sec[s] = DBnames.field_name(s)
                f = sec[s]
                if f in DBfiles:
                    msg = ' '.join(['DB name', f, 'requested for secondary',
                                    'name', s, 'in primary name', n,
                                    'is already specified'])
                    raise DBapiError(msg)
                DBfiles[f] = SUBFILE_DELIMITER.join((n, f))

        # Associate primary and secondary DBs by name.
        # {secondary name:primary name, ...,
        #  primary name:[secondary name, ...], ...}
        # A secondary name may be a primary name if a loop is not made.
        self._associate = dict()
        
        # DBbitapiRecord objects, containing the DB object, for all DB names
        # {name:DBbitapiRecord instance, ...}
        self._main = dict()
        
        # Home directory for the DBenv
        self._home = DBhome
        
        # Set up primary databases in DBnames.
        for n in DBnames:
            f = DBnames[n][PRIMARY]
            self._main[f] = primary_class(
                DBfiles[f],
                DBnames[n],
                f)
            self._associate[n] = {n:f}

        # Set up secondary databases in DBnames.
        for n in DBnames:
            fn = DBnames[n][PRIMARY]
            for s in DBnames[n][SECONDARY]:
                f = DBnames[n][SECONDARY][s]
                self._main[f] = secondary_class(
                    DBfiles[f],
                    DBnames[n],
                    f)
                self._associate[n][s] = f

    def backout(self):
        """Do nothing.  Added for compatibility with DPT.

        The transaction control available in Berkeley DB is not used.

        """
        return

    def close_context(self):
        """Close main and deferred update databases and environment."""
        for n in self._main:
            self._main[n].close()
        if self._dbenv is not None:
            self._dbenv.close()
            self._dbenv = None

    def close_contexts(self, close_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    def close_database(self):
        """Close main and deferred update databases and environment.

        Introduced for compatibility with DPT.  There is a case for closing
        self._dbenv in this method rather than doing it all in close_context.
        
        """
        self.close_context()
            
    def commit(self):
        """Do nothing.  Added for compatibility with DPT.

        The transaction control available in Berkeley DB is not used.

        """
        return

    def db_compatibility_hack(self, record, srkey):
        """Convert record and return in (key, value) format.
        
        Do nothing as record is in (key, value) format on Berkeley DB.
        Added for compatibility with DPT.
        
        """
        return record

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.
        
        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.
        
        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main

        main[db[dbset]].delete(deletekey, instance.srvalue.encode())
        instance.srkey = self.encode_record_number(deletekey)
        convertedkey = deletekey

        srindex = instance.srindex
        dcb = instance._deletecallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v.encode(), convertedkey)

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
            raise DBapiError(msg)

        args.append(pyscript)
        
        try:
            if os.path.exists(filepath):
                paths = (filepath,)
            else:
                msg = ' '.join([repr(filepath),
                                'is not an existing file'])
                raise DBapiError(msg)
        except:
            paths = tuple(filepath)
            for fp in paths:
                if not os.path.isfile(fp):
                    msg = ' '.join([repr(fp),
                                    'is not an existing file'])
                    raise DBapiError(msg)

        args.append(os.path.abspath(self._home))
        args.extend(paths)

        return subprocess.Popen(args)

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
        main = self._main

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
            main[db[dbset]].delete(oldkey, instance.srvalue.encode())
            key = main[db[dbset]].put(
                newkey, instance.newrecord.srvalue.encode())
            if key is not None:
                # put was append to record number database and
                # returned the new primary key. Adjust record key
                # for secondary updates.
                instance.newrecord.key.load(key)
                newkey = key
        elif instance.srvalue != instance.newrecord.srvalue:
            main[db[dbset]].replace(
                oldkey,
                instance.srvalue.encode(),
                instance.newrecord.srvalue.encode())

        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)
        convertedoldkey = oldkey
        convertednewkey = newkey
        
        for secondary in ionly:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v.encode(), convertedoldkey)

        for secondary in nionly:
            if secondary not in db:
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in nsrindex[secondary]:
                main[db[secondary]].put(v.encode(), convertednewkey)

        for secondary in iandni:
            if srindex[secondary] == nsrindex[secondary]:
                if convertedoldkey == convertednewkey:
                    continue
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v.encode(), convertedoldkey)
            for v in nsrindex[secondary]:
                main[db[secondary]].put(v.encode(), convertednewkey)

    def exists(self, dbset, dbname):
        """Return True if dbname is a primary or secondary DB in dbset."""
        if dbset in self._associate:
            return dbname in self._associate[dbset]
        else:
            return False

    def files_exist(self):
        """Return True if all defined files exist in self._home folder."""
        fileset = set()
        for a in self._associate:
            fileset.add(self._associate[a][a])
        filecount = len(fileset)
        for f in os.listdir(self._home):
            if f in fileset:
                fileset.remove(f)
        if len(fileset) == filecount:
            return None
        return len(fileset) == 0

    def database_cursor(self, dbset, dbname, keyrange=None):
        """Create and return a cursor on DB dbname in dbset.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        return self._main[self._associate[dbset][dbname]].make_cursor(
            self._main[self._associate[dbset][dbname]],
            keyrange)

    def repair_cursor(self, cursor, *a):
        """Return cursor for compatibility with DPT which returns a new one."""
        return cursor

    def get_database_folder(self):
        """Return database folder name"""
        return self._home
    
    def get_database(self, dbset, dbname):
        """Return DB for dbname in dbset."""
        return self._main[self._associate[dbset][dbname]]._object

    def get_database_instance(self, dbset, dbname):
        """Return DB instance for dbname in dbset."""
        return self._main[self._associate[dbset][dbname]]

    def get_first_primary_key_for_index_key(self, dbset, dbname, key):
        """Return first primary key for secondary key in dbname for dbname.

        Consider restricting use of this method to secondary DBs whose keys
        each have a unique value.
        
        """
        if dbset == dbname:
            raise DBapiError((
                'get_first_primary_key_for_index_key for primary index'))

        if isinstance(key, str):
            key = key.encode('utf8')
        try:
            return self.decode_record_number(
                self._main[self._associate[dbset][dbname]
                           ]._object.cursor().set(key)[1])
        except:
            if not isinstance(key, bytes):
                raise
            return None

    def get_primary_record(self, dbset, key):
        """Return primary record (key, value) given primary key on dbset."""
        try:
            return self._decode_record(
                self._main[self._associate[dbset][dbset]
                           ]._object.cursor().set(key))
        except:
            return None

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            k, v = record
            return k, v.decode()
        except:
            if record is None:
                return record
            raise

    def is_primary(self, dbset, dbname):
        """Return True if dbname is primary database in dbset."""
        return self._main[self._associate[dbset][dbname]].is_primary()

    def is_primary_recno(self, dbset):
        """Return True if primary DB in dbset is RECNO.

        Primary DB is assumed to be RECNO, so return True.

        It is possible to override the open_root() method in the class passed
        to DBapi() as primary_class and not use RECNO, but things should soon
        fall apart if so.  The sibling DPTbase and _Sqlite3api classes cannot
        be other than the equivalent of RECNO, and their is_primary_recno()
        methods already return True always.

        """
        #return self._main[self._associate[dbset][dbset]].is_primary_recno()
        return True

    def is_recno(self, dbset, dbname):
        """Return True if DB dbname in dbset is RECNO."""
        return self._main[self._associate[dbset][dbname]].is_recno()

    def open_context(self):
        """Open all DBs."""
        try:
            os.mkdir(self._home)
        except FileExistsError:
            if not os.path.isdir(self._home):
                raise
        
        gbytes = self._DBenvironment.get('gbytes', 0)
        bytes_ = self._DBenvironment.get('bytes', 0)
        flags = self._DBenvironment.get('flags', 0)
        self._dbenv = DBEnv()
        if gbytes or bytes_:
            self._dbenv.set_cachesize(gbytes, bytes_)
        self._dbenv.open(self._home, flags)
        for p in self._main:
            self._main[p].open_root(self._dbenv)
        return True

    def open_contexts(self, closed_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    def allocate_and_open_contexts(self, closed_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    def get_packed_key(self, dbset, instance):
        """Return instance.key converted to string for dbset.

        encode_record_number provides this for RECNO databases.
        packed_key method of instance does conversion otherwise.

        """
        return self.encode_record_number(instance.key.pack())

    def decode_as_primary_key(self, dbset, pkey):
        """Return primary key after converting from secondary database format.

        No conversion is required if the primary DB is not RECNO.
        
        """
        return self.decode_record_number(pkey)

    def encode_primary_key(self, dbname, instance):
        """Convert instance.key for use as database value.
        
        For Berkeley DB just return self.get_packed_key().
        
        """
        return self.get_packed_key(dbname, instance)

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
            for v in srindex[secondary]:
                main[db[secondary]].put(v.encode(), convertedkey)

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

    def use_deferred_update_process(self, **kargs):
        """Return module name or None

        **kargs - soak up any arguments other database engines need.

        """
        raise DBapiError('use_deferred_update_process not implemented')

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
        """Run taskmethod to perform database task.

        This method is structured to be compatible with the requirements of
        the sqlite3 version which is intended for use in a separate thread and
        must open a separate connection to the database.  Such action seems to
        be unnecessary in Berkeley DB so far.

        """
        # See sqlite3api.py for code which justifies existence of this method.
        taskmethod(self, logwidget, **taskmethodargs)

    def make_recordset_key(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._main[self._associate[dbset][dbname]
                   ].populate_recordset_key(rs, key)
        return rs

    def make_recordset_key_startswith(
        self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._main[self._associate[dbset][dbname]
                   ].populate_recordset_key_startswith(rs, key)
        return rs

    def make_recordset_key_range(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._main[self._associate[dbset][dbname]
                   ].populate_recordset_key_range(rs, key)
        return rs

    def make_recordset_all(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self._main[self._associate[dbset][dbname]
                   ].populate_recordset_all(rs)#, key)
        return rs

    def recordset_for_segment(self, recordset, dbname, segment):
        """Return recordset populated with records for segment."""
        self._main[self._associate[recordset.dbset][dbname]
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
        self._main[self._associate[recordset.dbset][dbname]
                   ].file_records_under(recordset, key)

    def is_engine_uses_bytes(self):
        """Return True if database engine interface is bytes"""
        return self.engine_uses_bytes_or_str is USE_BYTES

    def is_engine_uses_str(self):
        """Return True if database engine interface is str (C not unicode)"""
        return self.engine_uses_bytes_or_str is USE_STR

    def start_transaction(self):
        """Do nothing. Added for compatibility with apsw Sqlite3 interface."""

    def cede_contexts_to_process(self, close_contexts):
        """Do nothing. Added for compatibility with apsw Sqlite3 interface."""
        pass


class DBapi(_DBapi):
    
    """Define a Berkeley DB database structure.
    
    Primary databases are created as DB_RECNO.
    Secondary databases are DB_BTREE with DB_DUPSORT set.

    Primary and secondary terminology comes from Berkeley DB documentation but
    the association technique is not used.

    The value part of a secondary key:value is a (segment, reference, count)
    tuple where segment follows DPT terminology.  Reference can be a record
    number relative to segment start, a reference to a list of record numbers,
    or a reference to a bitmap representing such record numbers.  Count is the
    number of records referenced by this value.

    Secondary databases are supported by two DB_RECNO databases, one for lists
    of record numbers and one for bitmap representations of record numbers. The
    reference is the key into the relevant DB_RECNO database.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args, **kargs):
        """Define database structure.  See superclass for *args and **kargs."""
        super(DBapi, self).__init__(DBPrimary, DBSecondary, *args, **kargs)

            
class DBFile(object):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    close
    get_database_file
    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(
        self,
        dbfile,
        dbdesc,
        fieldatts,
        description,
        dbname,
        ):
        """Define a DB file.
        
        dbfile=file name relative to environment home directory
        dbname=db name within file
        primary=True|False. DB is primary or secondary
        
        """
        super(DBFile, self).__init__()

        self._object = None
        self._dbfile = dbfile
        self._dbname = dbname
        self._fieldatts = dict()
        
        for attr in DB_FIELDATTS:
            self._fieldatts[attr] = fieldatts[attr]
        if description == None:
            description = dict()
        if not isinstance(description, dict):
            msg = ' '.join(['Attributes for index', dbname,
                            'in file', repr(dbdesc[PRIMARY]),
                            'must be a dictionary or "None"'])
            raise DBapiError(msg)
        
        for attr in description:
            if attr not in fieldatts:
                msg = ' '.join(['Attribute', repr(attr),
                                'for index', dbname,
                                'in file', repr(dbdesc[PRIMARY]),
                                'is not allowed'])
                raise DBapiError(msg)
            
            if not isinstance(description[attr], type(fieldatts[attr])):
                msg = ' '.join([attr, 'for field', dbname,
                                'in file', repr(dbdesc[PRIMARY]),
                                'is wrong type'])
                raise DBapiError(msg)
            
            if attr == SPT:
                if (description[attr] < 0 or
                    description[attr] > 100):
                    msg = ' '.join(['Split percentage for field',
                                    dbname, 'in file', repr(dbdesc[PRIMARY]),
                                    'is invalid'])
                    raise DBapiError(msg)

            if attr in DB_FIELDATTS:
                self._fieldatts[attr] = description[attr]

    def close(self):
        """Close DB and cursor."""
        if self._object is not None:
            self._object.close()
            self._object = None

    def open_root(self, dbenv):
        """Create DB in dbenv."""
        try:
            self._object = DB(dbenv)
        except:
            raise

    def get_database_file(self):
        """Return database file name"""
        return self._dbfile

            
class DBPrimaryFile(DBFile):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    is_primary
    is_primary_recno
    is_recno
    is_value_recno

    Methods overridden:

    None

    Methods extended:

    __init__
    open_root
    
    """

    def __init__(self, dbfile, dbdesc, *args):
        """Primary database file for name in description.
        
        dbfile=file name relative to environment home directory
        dbdesc=file description containing secondary database dbname
        
        """
        super(DBPrimaryFile, self).__init__(
            dbfile,
            dbdesc,
            PRIMARY_FIELDATTS,
            dbdesc[FIELDS][dbdesc[PRIMARY]],
            *args)

    def open_root(self, *args):
        """Open primary database.  See superclass for *args"""
        super(DBPrimaryFile, self).open_root(*args)
        try:
            self._object.open(
                self._dbfile,
                self._dbname,
                DB_RECNO,
                DB_CREATE)
        except:
            self._object = None
            raise

    def is_primary(self):
        """Return True."""
        # Maybe ask self._object
        return True

    def is_primary_recno(self):
        """Return True."""
        # Maybe ask self._object
        return True

    def is_recno(self):
        """Return True."""
        # Maybe ask self._object
        return True

    def is_value_recno(self):
        """Return False."""
        # Maybe ask self._object
        return False

            
class DBSecondaryFile(DBFile):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    is_primary
    is_primary_recno
    is_recno
    is_value_recno

    Methods overridden:

    None

    Methods extended:

    __init__
    open_root
    
    """

    def __init__(self, dbfile, dbdesc, dbname, *args):
        """Secondary database file for name in description.
        
        dbfile=file name relative to environment home directory
        dbdesc=file description containing secondary database dbname
        dbname=entry in file description for this file
        
        """
        super(DBSecondaryFile, self).__init__(
            dbfile,
            dbdesc,
            SECONDARY_FIELDATTS,
            dbdesc[FIELDS][dbname],
            dbname,
            *args)

    def open_root(self, *args):
        """Open secondary database.  See superclass for *args"""
        super(DBSecondaryFile, self).open_root(*args)
        try:
            self._object.set_flags(DB_DUPSORT)
            self._object.open(
                self._dbfile,
                self._dbname,
                DB_BTREE,
                DB_CREATE)
        except:
            self._object = None
            raise

    def is_primary(self):
        """Return False."""
        # Maybe ask self._object
        return False

    def is_primary_recno(self):
        """Return True."""
        # Maybe ask self._object
        return True

    def is_recno(self):
        """Return False."""
        # Maybe ask self._object
        return False

    def is_value_recno(self):
        """Return True."""
        # Maybe ask self._object
        return True


class DBPrimary(DBPrimaryFile, _DatabaseEncoders):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
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

    def __init__(self, *args):
        """Primary database.  See superclass for *args."""
        super(DBPrimary, self).__init__(*args)
        self._clientcursors = dict()
        self._recordsets = dict()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self._object.cursor()
            if cursor.set(key):
                cursor.delete()
        except:
            pass

    # This may be common between DBPrimary and DBbitPrimary
    def put(self, key, value):
        """Put (key, value) on database and return key for new RECNO records.

        The DB put method, or append for new RECNO records,is
        used for primary DBs with associated secondary DBs. The
        cursor put method is used otherwise.
        
        """
        # Primary assumed to be not DUPSORT nor DUP
        if not key: #key == 0:  # Change test to "key is None" when sure
            return self._object.append(value)
        else:
            self._object.put(key, value)
            return None

    # This may be common between DBPrimary and DBbitPrimary
    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on DB.
        
        (key, newvalue) is put on DB only if (key, oldvalue) is on DB.
        
        """
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self._object.cursor()
            if cursor.set(key):
                cursor.put(key, newvalue, DB_CURRENT)
        except:
            pass

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorDBPrimary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c
    
    def close(self):
        """Close DB and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(DBPrimary, self).close()

    # Copied to DBbitPrimary because it seems simpler for one key
    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        r = self._object.get(key)
        if r:
            s, rn = divmod(key, DB_SEGMENT_SIZE)
            recordset[s] = SegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))

    def populate_recordset_key_startswith(self, recordset, key):
        """Raise exception - populate_recordset_key_startswith primary db."""
        raise DBapiError(
            ''.join(
                ('populate_recordset_key_startswith not available ',
                 'on primary database')))

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None:
            segment_start = 0
        else:
            segment_start = divmod(keystart, DB_SEGMENT_SIZE)[0]
        if keyend is not None:
            segment_end = divmod(keyend, DB_SEGMENT_SIZE)[0]
        cursor = self.make_cursor(self._object, keystart)
        c = cursor._cursor
        r = c.set_range(keystart)
        while r:
            if keyend is not None:
                if r[0] > keyend:
                    break
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        cursor = self.make_cursor(self._object)
        c = cursor._cursor
        r = c.first()
        while r:
            s, rn = divmod(r[0], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        raise DatabaseError(
            'populate_recordset_from_segment not implemented for DBPrimary')
    
    def file_records_under(self, recordset, key):
        """Raise exception as DBPrimary.file_records_under() is nonsense."""
        raise DatabaseError(
            'file_records_under not implemented for DBPrimary')


class DBSecondary(DBSecondaryFile, _DatabaseEncoders):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
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

    def __init__(self, *args):
        """Secondary database.  See superclass for *args."""
        super(DBSecondary, self).__init__(*args)
        self._clientcursors = dict()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            # Primary database for this secondary is assumed to be RECNO
            cursor = self._object.cursor()
            if cursor.set_both(key, self.encode_record_number(value)):
                cursor.delete()
        except:
            pass

    def put(self, key, value):
        """Put (key, value) on database and return key for new RECNO records.

        The DB put method, or append for new RECNO records,is
        used for primary DBs with associated secondary DBs. The
        cursor put method is used otherwise.
        
        """
        try:
            # Primary database for this secondary is assumed to be RECNO
            self._object.cursor().put(
                key, self.encode_record_number(value), DB_KEYLAST)
        except DBKeyExistError:
            # Application may legitimately do duplicate updates (-30996)
            # to a sorted secondary database for DPT compatibility.
            pass
        except:
            raise

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on DB.
        
        (key, newvalue) is put on DB only if (key, oldvalue) is on DB.
        
        """
        try:
            # Primary database for this secondary is assumed to be RECNO
            cursor = self._object.cursor()
            if cursor.set_both(
                key, self.encode_record_number(oldvalue)):
                cursor.put(
                    key, self.encode_record_number(newvalue), DB_CURRENT)
        except:
            pass

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorDBSecondary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c
    
    def close(self):
        """Close DB and any cursors."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        super(DBSecondary, self).close()
    
    def populate_recordset_key(self, recordset, key):
        """Return recordset of segments containing records for key."""
        cursor = self.make_cursor(self, key)
        c = cursor._cursor
        r = c.set_range(key)
        while r:
            if r[0] != key:
                break
            s, rn = divmod(r[1], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting."""
        cursor = self.make_cursor(self, key)
        c = cursor._cursor
        r = c.set_range(key)
        while r:
            if not r[0].startswith(key):
                break
            s, rn = divmod(r[1], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        cursor = self.make_cursor(self, keystart)
        c = cursor._cursor
        r = c.set_range(keystart)
        while r:
            if keyend is not None:
                if r[0] > keyend:
                    break
            s, rn = divmod(r[1], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        cursor = self.make_cursor(self)
        c = cursor._cursor
        r = c.first()
        while r:
            s, rn = divmod(r[1], DB_SEGMENT_SIZE)
            if s not in recordset:
                recordset[s] = SegmentBitarray(s, None)
            recordset[s][rn] = True
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        raise DatabaseError(
            'populate_recordset_from_segment not implemented for DBSecondary')
    
    def file_records_under(self, recordset, key):
        """Replace records for index dbname[key] with recordset records."""
        print('DBSecondary', 'file_records_under')


class DBbitapi(_DBapi):
    
    """Define a Berkeley DB database structure.
    
    Primary databases are created as DB_RECNO.
    Secondary databases are DB_BTREE with DB_DUPSORT set.

    Primary and secondary terminology comes from Berkeley DB documentation but
    the association technique is not used.

    The value part of a secondary key:value is a (segment, reference, count)
    tuple where segment follows DPT terminology.  Reference can be a record
    number relative to segment start, a reference to a list of record numbers,
    or a reference to a bitmap representing such record numbers.  Count is the
    number of records referenced by this value.

    Secondary databases are supported by two DB_RECNO databases, one for lists
    of record numbers and one for bitmap representations of record numbers. The
    reference is the key into the relevant DB_RECNO database.

    Methods added:

    None

    Methods overridden:

    delete_instance
    edit_instance
    put_instance

    Methods extended:

    __init__
    close_context
    open_context
    
    """

    def __init__(self, DBnames, *args, **kargs):
        """Define database structure.  See superclass for *args and **kargs."""
        super(DBbitapi, self).__init__(
            DBbitPrimary, DBbitSecondary, DBnames, *args, **kargs)
        self._control = DBbitControlFile()
        # Refer to primary from secondary for access to segment databases
        # Link each primary to control file for segment management
        m = self._main
        for n in DBnames:
            m[DBnames[n][PRIMARY]].set_control_database(self._control)
            for s in DBnames[n][SECONDARY].values():
                m[s].set_primary_database(m[DBnames[n][PRIMARY]])

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.
        
        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.
        
        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main
        primarydb = main[db[dbset]]

        high_record = primarydb._object.cursor().last()
        primarydb.delete(deletekey, instance.srvalue.encode())
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
                main[db[secondary]].segment_delete(
                    v.encode(), segment, record_number)
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
        main = self._main

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
            main[db[dbset]].delete(oldkey, instance.srvalue.encode())
            key = main[db[dbset]].put(
                newkey, instance.newrecord.srvalue.encode())
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
                instance.srvalue.encode(),
                instance.newrecord.srvalue.encode())

        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)

        for secondary in ionly:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].segment_delete(
                    v.encode(), old_segment, old_record_number)

        for secondary in nionly:
            if secondary not in db:
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in nsrindex[secondary]:
                main[db[secondary]].segment_put(
                    v.encode(), new_segment, new_record_number)

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
                    v.encode(), old_segment, old_record_number)
            for v in nsrindex[secondary]:
                main[db[secondary]].segment_put(
                    v.encode(), new_segment, new_record_number)

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are DB_RECNO.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main
        primarydb = main[db[dbset]]

        if putkey == 0:
            # reuse record number if possible
            putkey = primarydb.get_control_primary(
                ).get_lowest_freed_record_number()
            if putkey != 0:
                instance.key.load(putkey)
        key = primarydb.put(putkey, instance.srvalue.encode())
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
                main[db[secondary]].segment_put(
                    v.encode(), segment, record_number)

    def close_context(self):
        """Close main and deferred update databases and environment."""
        self._control.close()
        super(DBbitapi, self).close_context()

    def open_context(self):
        """Open all DBs."""
        super(DBbitapi, self).open_context()
        self._control.open_root(self._dbenv)
        return True

            
class DBbitPrimaryFile(DBPrimaryFile):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    get_control_database
    get_existence_bits
    get_existence_bits_database
    get_segment_bits_database
    get_segment_list_database
    set_control_database
    get_control_primary
    get_control_secondary

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    open_root
    
    """

    def __init__(self, *args):
        """Bitmapped primary database file for name in description."""
        super(DBbitPrimaryFile, self).__init__(*args)

        # Description to be provided
        self._control_database = None

        # Existence bit map control structure (reuse record numbers)
        self._control_primary = FileControlPrimary(self)

        # Freed record list and bit map segment control structure
        self._control_secondary = FileControlSecondary(self)

        # Record number existence bit map for this primary database
        self._existence_bits = DBExistenceBitMap(self.get_database_file())

        # Inverted index record number lists for this primary database
        self._segment_list = DBSegmentList(self.get_database_file())

        # Inverted index record number bit maps for this primary database
        self._segment_bits = DBSegmentBitMap(self.get_database_file())

    def open_root(self, *args):
        """Open primary database and inverted index databases."""
        super(DBbitPrimaryFile, self).open_root(*args)
        self._segment_list.open_root(*args)
        self._segment_bits.open_root(*args)
        self._existence_bits.open_root(*args)

    def close(self):
        """Close inverted index databases then primary database."""
        self._segment_list.close()
        self._segment_bits.close()
        super(DBbitPrimaryFile, self).close()

    def get_control_database(self):
        """Return the database containing segment control data."""
        return self._control_database.get_control_database()

    # Added for sqlite3 compatibility.
    # Berkeley DB uses get_segment_list_database.
    def get_segment_list(self):
        """Return the segment list control data."""
        return self._segment_list

    def get_segment_list_database(self):
        """Return the database containing segment record number lists."""
        # Maybe use instance.get_segment_list().get_seg_object() instead of
        # instance.get_segment_list_database()
        return self._segment_list._seg_object

    # Added for sqlite3 compatibility.
    # Berkeley DB uses get_segment_bits_database.
    def get_segment_bits(self):
        """Return the segment bits control data."""
        return self._segment_bits

    def get_segment_bits_database(self):
        """Return the database containing segment record number bit maps."""
        # Maybe use instance.get_segment_bits().get_seg_object() instead of
        # instance.get_segment_bits_database()
        return self._segment_bits._seg_object

    def get_existence_bits(self):
        """Return the existence bit map control data."""
        return self._existence_bits

    def get_existence_bits_database(self):
        """Return the database containing existence bit map."""
        # Maybe use instance.get_existence_bits().get_seg_object() instead of
        # instance.get_existence_bits_database()
        return self._existence_bits._seg_object

    def set_control_database(self, database):
        """Set reference to segment control databases."""
        self._control_database = database

    def get_control_primary(self):
        """Return the re-use record number control data."""
        return self._control_primary

    def get_control_secondary(self):
        """Return the segment control data."""
        return self._control_secondary

            
class DBbitSecondaryFile(DBSecondaryFile):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    get_primary_database
    get_primary_segment_bits
    get_primary_segment_list
    set_primary_database

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, *args):
        """Bitmapped secondary database.  See superclass for *args."""
        super(DBbitSecondaryFile, self).__init__(*args)
        self._primary_database = None

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


class DBbitPrimary(DBbitPrimaryFile):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    file_records_under
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

    def __init__(self, *args):
        """Bitmapped primary database.  See superclass for *args."""
        super(DBbitPrimary, self).__init__(*args)
        self._clientcursors = dict()
        self._recordsets = dict()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorDBbitPrimary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c
    
    def close(self):
        """Close DB and any cursors."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super(DBbitPrimary, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self._object.cursor()
            if cursor.set(key):
                cursor.delete()
                self.get_control_primary().note_freed_record_number(key)
        except:
            pass

    # This may be common between DBPrimary and DBbitPrimary
    def put(self, key, value):
        """Put (key, value) on database and return key for new RECNO records.

        The DB put method, or append for new RECNO records,is
        used for primary DBs with associated secondary DBs. The
        cursor put method is used otherwise.
        
        """
        # Primary assumed to be not DUPSORT nor DUP
        if not key: #key == 0:  # Change test to "key is None" when sure
            return self._object.append(value)
        else:
            self._object.put(key, value)
            return None

    # This may be common between DBPrimary and DBbitPrimary
    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on DB.
        
        (key, newvalue) is put on DB only if (key, oldvalue) is on DB.
        
        """
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self._object.cursor()
            if cursor.set(key):
                cursor.put(key, newvalue, DB_CURRENT)
        except:
            pass

    def segment_delete(self, segment, record_number):
        """Remove record_number from existence bit map for segment."""
        # See dbduapi.py DBbitduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately (and delete!!).
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits_database().get(segment + 1)
        if ebmb is None:
            # It does not exist so raise an exception
            raise DBapiError('Existence bit map for segment does not exist')
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            # Set bit for record number and write segment back to database
            ebm[record_number] = False
            self.get_existence_bits_database().put(segment + 1, ebm.tobytes())

    def segment_put(self, segment, record_number):
        """Add record_number to existence bit map for segment."""
        # See dbduapi.py DBbitduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately.
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits_database().get(segment + 1)
        if ebmb is None:
            # It does not exist so create a new empty one
            ebm = EMPTY_BITARRAY.copy()
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
        # Set bit for record number and write segment back to database
        ebm[record_number] = True
        self.get_existence_bits_database().put(segment + 1, ebm.tobytes())

    # Same as DBPrimary for one key
    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        r = self._object.get(key)
        if r:
            s, rn = divmod(key, DB_SEGMENT_SIZE)
            recordset[s] = SegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))

    # Same algorithm as DBPrimary but look at segment record
    def populate_recordset_key_startswith(self, key, cache_size=1):
        """Raise DBapiError - populate_recordset_key_startswith primary db."""
        raise DBapiError(
            ''.join(
                ('populate_recordset_key_startswith not available ',
                 'on primary database')))

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(keystart, DB_SEGMENT_SIZE)
        if keyend is not None:
            segment_end, record_number_end = divmod(keyend, DB_SEGMENT_SIZE)
        c = self.get_existence_bits_database()._cursor
        r = c.set(segment_start + 1)
        while r:
            if keyend is not None:
                if r[0] - 1 > segment_end:
                    break
            recordset[r[0] - 1] = SegmentBitarray(r[0] - 1, None, records=r[1])
            r = c.next()
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
        c = self.get_existence_bits_database().cursor()
        r = c.first()
        while r:
            recordset[r[0] - 1] = SegmentBitarray(r[0] - 1, None, records=r[1])
            r = c.next()

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        recordset.clear_recordset()
        k, v = segment
        s = int.from_bytes(k, byteorder='big')
        if len(v) + len(k) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(v[2:], byteorder='big')
            bs = self.get_segment_list().get(srn)
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[s] = SegmentList(s, None, records=bs)
        elif len(v) + len(k) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(v[3:], byteorder='big')
            bs = self.get_segment_bits().get(srn)
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[s] = SegmentBitarray(s, None, records=bs)
        else:
            recordset[s] = SegmentList(s, None, records=v)
    
    def file_records_under(self, recordset, key):
        """Raise exception as DBbitPrimary.file_records_under() is nonsense."""
        raise DatabaseError(
            'file_records_under not implemented for DBbitPrimary')


class DBbitSecondary(DBbitSecondaryFile):
    
    """Define a DB file with record access and deferred update methods.

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

    def __init__(self, *args):
        """Bitmapped secondary database.  See superclass for *args."""
        super(DBbitSecondary, self).__init__(*args)
        self._clientcursors = dict()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorDBbitSecondary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c
    
    def close(self):
        """Close DB and any cursors."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        super(DBbitSecondary, self).close()
    
    def segment_delete(self, key, segment, record_number):
        """Remove record_number from segment for key and write to database"""
        # See DBbitSecondary.segment_put (in this class definition) for model.
        cursor = self._object.cursor()
        r = cursor.set_range(key)
        while r:
            k, v = r
            if k != key:
                # Assume that multiple requests to delete an index value have
                # been made for a record.  The segment_put method uses sets to
                # avoid adding multiple entries.  Consider using set rather
                # than list in the pack method of the ...value... subclass of
                # Value if this will happen a lot.
                return
            sr = int.from_bytes(v[:4], byteorder='big')
            if sr == segment:
                if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn_list = int.from_bytes(v[6:], byteorder='big')
                    bs = self.get_primary_segment_list().get(srn_list)
                    recnums = {int.from_bytes(bs[i:i+2], byteorder='big')
                               for i in range(0, len(bs), 2)}
                    # ignore possibility record_number already absent
                    recnums.discard(record_number)
                    count = len(recnums)
                    if count < 2:
                        for rn in recnums:
                            ref = b''.join(
                                (segment.to_bytes(4, byteorder='big'),
                                 rn.to_bytes(2, byteorder='big')))
                        # stub call to put srn_list on reuse stack
                        self.get_primary_database().get_control_secondary(
                            ).note_freed_list_page(srn_list)
                        # ok if reuse bitmap but not if reuse stack
                        self.get_primary_segment_list().delete(srn_list)
                        cursor.delete()
                        if count:
                            cursor.put(k, ref, DB_KEYLAST)
                    else:
                        seg = b''.join(tuple(
                            rn.to_bytes(length=2, byteorder='big')
                            for rn in sorted(recnums)))
                        self.get_primary_segment_list().put(srn_list, seg)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(2, byteorder='big'),
                                 v[6:])),
                            DB_KEYLAST)
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn_bits = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn_bits)
                    if bs is None:
                        raise DatabaseError('Segment record missing')
                    recnums = Bitarray()
                    recnums.frombytes(bs)
                    # ignore possibility record_number already absent
                    recnums[record_number] = False
                    count = recnums.count()
                    if count < DB_CONVERSION_LIMIT:
                        recnums = {rn for rn in recnums.search(SINGLEBIT)}
                        # stub call to get srn_list from reuse stack
                        srn_list = self.get_primary_database(
                            ).get_control_secondary().get_freed_list_page()
                        if srn_list == 0:
                            srn_list = self.get_primary_segment_list().append(
                                b''.join(
                                    [rn.to_bytes(2, byteorder='big')
                                     for rn in sorted(recnums)]))
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    [rn.to_bytes(2, byteorder='big')
                                     for rn in sorted(recnums)]))
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 len(recnums).to_bytes(2, byteorder='big'),
                                 srn_list.to_bytes(4, byteorder='big'))),
                            DB_KEYLAST)
                        # stub call to put srn_bits on reuse stack
                        self.get_primary_database().get_control_secondary(
                            ).note_freed_bits_page(srn_bits)
                        # ok if reuse bitmap but not if reuse stack
                        self.get_primary_segment_bits().delete(srn_bits)
                    else:
                        self.get_primary_segment_bits().put(
                            srn_bits, recnums.tobytes())
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 recnums.count().to_bytes(3, byteorder='big'),
                                 v[7:])),
                            DB_KEYLAST)
                elif record_number == int.from_bytes(v[4:], byteorder='big'):
                    cursor.delete()
                return
            elif sr > segment:
                return
            else:
                r = cursor._nodup()
    
    def segment_put(self, key, segment, record_number):
        """Add record_number to segment for key and write to database"""
        # See dbduapi.py DBbitduSecondary.defer_put for model.
        # The dance to find the segment record is a reason to convert these
        # secondary databases from DUP to NODUP.  Also a NODUP database allows
        # implementation equivalent to DPT 'FOR EACH VALUE' directly and easy
        # counting of values for manipulation of scrollbar sliders.
        # Assumption is that new records usually go in last segment for value.
        cursor = self._object.cursor()
        r = cursor.set_range(key)
        while r:
            k, v = r
            if k != key:
                # No index entry for key yet
                cursor.put(
                    key,
                    b''.join(
                        (segment.to_bytes(4, byteorder='big'),
                         record_number.to_bytes(2, byteorder='big'))),
                    DB_KEYLAST)
                return
            sr = int.from_bytes(v[:4], byteorder='big')
            if sr == segment:
                if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn_list = int.from_bytes(v[6:], byteorder='big')
                    bs = self.get_primary_segment_list().get(srn_list)
                    recnums = {int.from_bytes(bs[i:i+2], byteorder='big')
                               for i in range(0, len(bs), 2)}
                    # ignore possibility record_number already present
                    recnums.add(record_number)
                    count = len(recnums)
                    if count > DB_CONVERSION_LIMIT:
                        seg = EMPTY_BITARRAY.copy()
                        for rn in recnums:
                            seg[rn] = True
                        # stub call to put srn_list on reuse stack
                        self.get_primary_database().get_control_secondary(
                            ).note_freed_list_page(srn_list)
                        # ok if reuse bitmap but not if reuse stack
                        self.get_primary_segment_list().delete(srn_list)
                        # stub call to get srn_bits from reuse stack
                        srn_bits = self.get_primary_database(
                            ).get_control_secondary().get_freed_bits_page()
                        if srn_bits == 0:
                            srn_bits = self.get_primary_segment_bits(
                                ).append(seg.tobytes())
                        else:
                            self.get_primary_segment_bits(
                                ).put(srn_bits, seg.tobytes())
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(3, byteorder='big'),
                                 srn_bits.to_bytes(4, byteorder='big'))),
                            DB_KEYLAST)
                    else:
                        seg = b''.join(tuple(
                            rn.to_bytes(length=2, byteorder='big')
                            for rn in sorted(recnums)))
                        self.get_primary_segment_list().put(srn_list, seg)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(2, byteorder='big'),
                                 v[6:])),
                            DB_KEYLAST)
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn)
                    if bs is None:
                        raise DatabaseError('Segment record missing')
                    recnums = Bitarray()
                    recnums.frombytes(bs)
                    recnums[record_number] = True
                    self.get_primary_segment_bits().put(srn, recnums.tobytes())
                    cursor.delete()
                    cursor.put(
                        k,
                        b''.join(
                            (v[:4],
                             recnums.count().to_bytes(3, byteorder='big'),
                             v[7:])),
                        DB_KEYLAST)
                else:
                    rn = int.from_bytes(v[4:], byteorder='big')
                    if rn > record_number:
                        # stub call to get srn_list from reuse stack
                        srn_list = self.get_primary_database(
                            ).get_control_secondary().get_freed_list_page()
                        if srn_list == 0:
                            srn_list = self.get_primary_segment_list().append(
                                b''.join(
                                    (record_number.to_bytes(
                                        length=2, byteorder='big'),
                                     rn.to_bytes(length=2, byteorder='big'))))
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    (record_number.to_bytes(
                                        length=2, byteorder='big'),
                                     rn.to_bytes(length=2, byteorder='big'))))
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 b'\x00\x02',
                                 srn_list.to_bytes(4, byteorder='big'))),
                            DB_KEYLAST)
                    elif rn < record_number:
                        # stub call to get srn_list from reuse stack
                        srn_list = self.get_primary_database(
                            ).get_control_secondary().get_freed_list_page()
                        if srn_list == 0:
                            srn_list = self.get_primary_segment_list().append(
                                b''.join(
                                    (rn.to_bytes(length=2, byteorder='big'),
                                     record_number.to_bytes(
                                        length=2, byteorder='big'))))
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    (rn.to_bytes(length=2, byteorder='big'),
                                     record_number.to_bytes(
                                        length=2, byteorder='big'))))
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 b'\x00\x02',
                                 srn_list.to_bytes(4, byteorder='big'))),
                            DB_KEYLAST)
                return
            elif sr > segment:
                cursor.put(
                    k,
                    b''.join(
                        (segment.to_bytes(4, byteorder='big'),
                         record_number.to_bytes(2, byteorder='big'))),
                    DB_KEYLAST)
                return
            else:
                r = cursor._nodup()
        else:
            # No index entry for key yet because database empty
            cursor.put(
                key,
                b''.join(
                    (segment.to_bytes(4, byteorder='big'),
                     record_number.to_bytes(2, byteorder='big'))),
                DB_KEYLAST)
    
    def populate_recordset_key(self, recordset, key):
        """Return recordset of segments containing records for key."""
        cursor = self.make_cursor(self, key)
        c = cursor._cursor
        r = c.set_range(key)
        while r:
            k, v = r
            if k != key:
                break
            s = int.from_bytes(v[:4], byteorder='big')
            if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                srn = int.from_bytes(v[6:], byteorder='big')
                bs = self.get_primary_segment_list().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                recordset[s] = SegmentList(s, None, records=bs)
            elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                srn = int.from_bytes(v[7:], byteorder='big')
                bs = self.get_primary_segment_bits().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                recordset[s] = SegmentBitarray(s, None, records=bs)
            else:
                recordset[s] = SegmentList(s, None, records=v[4:])
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting."""
        cursor = self.make_cursor(self, key)
        c = cursor._cursor
        r = c.set_range(key)
        while r:
            if not r[0].startswith(key):
                break
            v = r[1]
            s = int.from_bytes(v[:4], byteorder='big')
            if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                srn = int.from_bytes(v[6:], byteorder='big')
                bs = self.get_primary_segment_list().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = recordset[s]._bitarray# needs tidying
                for i in range(0, len(bs), 2):
                    sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
            elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                srn = int.from_bytes(v[7:], byteorder='big')
                bs = self.get_primary_segment_bits().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = SegmentBitarray(s, None, records=bs)
                recordset[s] |= sba
            else:
                rn = int.from_bytes(v[4:], byteorder='big')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                recordset[s]._bitarray[rn] = True# needs tidying
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        cursor = self.make_cursor(self, keystart)
        c = cursor._cursor
        if keystart is None:
            r = c.first()
        else:
            r = c.set_range(keystart)
        while r:
            if keyend is not None:
                if r[0] > keyend:
                    break
            v = r[1]
            s = int.from_bytes(v[:4], byteorder='big')
            if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                srn = int.from_bytes(v[6:], byteorder='big')
                bs = self.get_primary_segment_list().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = recordset[s]._bitarray# needs tidying
                for i in range(0, len(bs), 2):
                    sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
            elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                srn = int.from_bytes(v[7:], byteorder='big')
                bs = self.get_primary_segment_bits().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = SegmentBitarray(s, None, records=bs)
                recordset[s] |= sba
            else:
                rn = int.from_bytes(v[4:], byteorder='big')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                recordset[s]._bitarray[rn] = True# needs tidying
            r = c.next()
        del c
        cursor.close()
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        cursor = self.make_cursor(self)
        c = cursor._cursor
        r = c.first()
        while r:
            v = r[1]
            s = int.from_bytes(v[:4], byteorder='big')
            if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                srn = int.from_bytes(v[6:], byteorder='big')
                bs = self.get_primary_segment_list().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = recordset[s]._bitarray# needs tidying
                for i in range(0, len(bs), 2):
                    sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
            elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                srn = int.from_bytes(v[7:], byteorder='big')
                bs = self.get_primary_segment_bits().get(srn)
                if bs is None:
                    raise DatabaseError('Segment record missing')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                sba = SegmentBitarray(s, None, records=bs)
                recordset[s] |= sba
            else:
                rn = int.from_bytes(v[4:], byteorder='big')
                if s not in recordset:
                    recordset[s] = SegmentBitarray(s, None)
                recordset[s]._bitarray[rn] = True# needs tidying
            r = c.next()
        del c
        cursor.close()

    def populate_recordset_from_segment(self, recordset, segment):
        """Populate recordset with records in segment."""
        recordset.clear_recordset()
        k, v = segment
        s = int.from_bytes(k, byteorder='big')
        if len(v) + len(k) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(v[2:], byteorder='big')
            bs = self.get_primary_segment_list().get(srn)
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[s] = SegmentList(s, None, records=bs)
        elif len(v) + len(k) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(v[3:], byteorder='big')
            bs = self.get_primary_segment_bits().get(srn)
            if bs is None:
                raise DatabaseError('Segment record missing')
            recordset[s] = SegmentBitarray(s, None, records=bs)
        else:
            recordset[s] = SegmentList(s, None, records=v)
    
    def file_records_under(self, recordset, key):
        """Replace records for index dbname[key] with recordset records."""
        # Delete existing segments for key
        cursor = self._object.cursor()
        r = cursor.set_range(key)
        while r:
            k, v = r
            if k != key:
                break
            sr = int.from_bytes(v[:4], byteorder='big')
            if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                srn_list = int.from_bytes(v[6:], byteorder='big')
                # stub call to put srn_list on reuse stack
                self.get_primary_database().get_control_secondary(
                    ).note_freed_list_page(srn_list)
                # ok if reuse bitmap but not if reuse stack
                self.get_primary_segment_list().delete(srn_list)
                #cursor.delete()
            elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                srn_bits = int.from_bytes(v[7:], byteorder='big')
                # stub call to put srn_bits on reuse stack
                self.get_primary_database().get_control_secondary(
                    ).note_freed_bits_page(srn_bits)
                # ok if reuse bitmap but not if reuse stack
                self.get_primary_segment_bits().delete(srn_bits)
                #cursor.delete()
            #elif record_number == int.from_bytes(v[4:], byteorder='big'):
                #cursor.delete()
            r = cursor.next()
        try:
            self._object.delete(key)
        except DBNotFoundError:
            pass
        # Put new segments for key
        for sn in recordset.sorted_segnums:
            if isinstance(recordset.rs_segments[sn], SegmentBitarray):
                count = recordset.rs_segments[sn].count_records()
                # stub call to get srn_bits from reuse stack
                srn_bits = self.get_primary_database(
                    ).get_control_secondary().get_freed_bits_page()
                if srn_bits == 0:
                    srn_bits = self.get_primary_segment_bits(
                        ).append(recordset.rs_segments[sn].tobytes())
                else:
                    self.get_primary_segment_bits(
                        ).put(srn_bits, recordset.rs_segments[sn].tobytes())
                cursor.put(
                    key,
                    b''.join(
                        (sn.to_bytes(4, byteorder='big'),
                         count.to_bytes(3, byteorder='big'),
                         srn_bits.to_bytes(4, byteorder='big'))),
                    DB_KEYLAST)
            elif isinstance(recordset.rs_segments[sn], SegmentList):
                count = recordset.rs_segments[sn].count_records()
                # stub call to get srn_list from reuse stack
                srn_list = self.get_primary_database(
                    ).get_control_secondary().get_freed_list_page()
                if srn_list == 0:
                    srn_list = self.get_primary_segment_list().append(
                        recordset.rs_segments[sn].tobytes())
                else:
                    self.get_primary_segment_list().put(
                        srn_list,
                        recordset.rs_segments[sn].tobytes())
                cursor.put(
                    key,
                    b''.join(
                        (sn.to_bytes(4, byteorder='big'),
                         count.to_bytes(2, byteorder='big'),
                         srn_list.to_bytes(4, byteorder='big'))),
                    DB_KEYLAST)
            elif isinstance(recordset.rs_segments[sn], SegmentInt):
                cursor.put(
                    key,
                    b''.join(
                        (sn.to_bytes(4, byteorder='big'),
                         recordset.rs_segments[sn].tobytes())),
                    DB_KEYLAST)

            
# Maybe this and DBFile should have same superclass for open_root and close.
class DBSegment(object):
    
    """Define a DB file to store inverted record number list representations.

    Methods added:

    append
    close
    delete
    get
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
        super(DBSegment, self).__init__()
        self._seg_dbfile = (SUBFILE_DELIMITER * 2).join((dbfile, segment_type))
        self._seg_object = None

    def close(self):
        """Close inverted index DB."""
        if self._seg_object is not None:
            self._seg_object.close()
            self._seg_object = None

    def get(self, key):
        """Get a segment record from the database."""
        # Exists to match the sqlite3 interface - may get used as wrapper for
        # DB get method eventually.  Sqlite3Segment version hides boilerplate
        # SQL statements.
        raise DBapiError('DBSegment.get not implemented')

    def delete(self, key):
        """Delete a segment record from the database."""
        # Exists to match the sqlite3 interface - may get used as wrapper for
        # DB delete method eventually.  Sqlite3Segment version hides boilerplate
        # SQL statements.
        raise DBapiError('DBSegment.delete not implemented')

    def put(self, key, value):
        """Put a segment record on the database, either replace or insert"""
        # Exists to match the sqlite3 interface - may get used as wrapper for
        # DB put method eventually.  Sqlite3Segment version hides boilerplate
        # SQL statements.
        raise DBapiError('DBSegment.put not implemented')

    def append(self, value):
        """Append a segment record on the database using a new key"""
        # Exists to match the sqlite3 interface - may get used as wrapper for
        # DB append method eventually.  Sqlite3Segment version hides boilerplate
        # SQL statements.
        raise DBapiError('DBSegment.append not implemented')

            
class DBExistenceBitMap(DBSegment):
    
    """DB file to store record number existence bit map keyed by segment number.

    Methods added:

    open_root
    segment_count

    Methods overridden:

    None

    Methods extended:

    __init__

    Properties:
    
    segment_count
    
    """

    def __init__(self, dbfile):
        """Define dbfile in environment for segment record bit maps.
        
        dbfile=file name relative to environment home directory
        
        """
        super(DBExistenceBitMap, self).__init__(dbfile, 'exist')
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
    
    def open_root(self, dbenv):
        """Create inverted index DB in dbenv."""
        try:
            self._seg_object = DB(dbenv)
        except:
            raise
        try:
            self._seg_object.set_re_pad(0)
            self._seg_object.set_re_len(DB_SEGMENT_SIZE_BYTES)
            self._seg_object.open(
                self._seg_dbfile,
                self._seg_dbfile,
                DB_RECNO,
                DB_CREATE)
            self._segment_count = self._seg_object.stat(
                flags=DB_FAST_STAT)['ndata']
        except:
            self._seg_object = None
            raise

            
class DBSegmentList(DBSegment):
    
    """DB file to store inverted record number list with arbitrary key.

    The arbitrary key is the number element of a (segment, count, number) tuple
    encoded as a value in a secondary database.

    Methods added:

    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, dbfile):
        """Define dbfile in environment for segment record number lists.
        
        dbfile=file name relative to environment home directory
        
        """
        super(DBSegmentList, self).__init__(dbfile, 'list')

    def open_root(self, dbenv):
        """Create inverted index DB in dbenv."""
        try:
            self._seg_object = DB(dbenv)
        except:
            raise
        try:
            self._seg_object.open(
                self._seg_dbfile,
                self._seg_dbfile,
                DB_RECNO,
                DB_CREATE)
        except:
            self._seg_object = None
            raise

            
class DBSegmentBitMap(DBSegment):
    
    """DB file to store inverted record number bit map with arbitrary key.

    The arbitrary key is the number element of a (segment, count, number) tuple
    encoded as a value in a secondary database.

    Methods added:

    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, dbfile):
        """Define dbfile in environment for segment record bit maps.
        
        dbfile=file name relative to environment home directory
        
        """
        super(DBSegmentBitMap, self).__init__(dbfile, 'bits')

    def open_root(self, dbenv):
        """Create inverted index DB in dbenv."""
        try:
            self._seg_object = DB(dbenv)
        except:
            raise
        try:
            self._seg_object.set_re_pad(0)
            self._seg_object.set_re_len(DB_SEGMENT_SIZE_BYTES)
            self._seg_object.open(
                self._seg_dbfile,
                self._seg_dbfile,
                DB_RECNO,
                DB_CREATE)
        except:
            self._seg_object = None
            raise


class CursorDB(Cursor):
    
    """Define bsddb3 style cursor methods on a Berkeley DB database.
    
    Mostly thin wrappers around the identically named bsddb3 cursor methods.

    Keys and values are converted to integer and str from Berkeley DB internal
    representation.

    Sibling classes in other ~api modules provide this interface for other
    database engines.

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

    The Berkeley DB Cursor put methods are not wrapped.

    The intended use is, using put_record as the example:

        cursor = <Database subclass instance>.database_cursor(...)
        record = cursor.<get method>(...)
        <Record subclass instance>.value.<attributes> = <Function(record)>
        <Record subclass instance>.put_record(...)

    where put_record is boilerplate, creating cursors if needed, and all the
    work to make the correct thing happen is in <Function>.
    
    """

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the Berkeley DB engine."""
        super(CursorDB, self).__init__(dbset)
        if dbset._object is not None:
            self._cursor = dbset._object.cursor()

    def close(self):
        """Delete database cursor"""
        try:
            del self._dbset._clientcursors[self]
        except:
            pass
        try:
            self._cursor.close()
        except:
            pass
        self._cursor = None
        self._dbset = None
        self.set_partial_key(None)

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise"""
        return bool(self._cursor)

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update.

        Do nothing in Berkeley DB.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass

    def get_partial(self):
        """Return self._partial"""
        return self._partial

    def get_converted_partial(self):
        """Return self._partial as it would be held on database"""
        return self._partial.encode()

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended"""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended"""
        # Berkeley DB uses a 'startswith(...)' technique
        return self._partial.encode()


class CursorDBPrimary(CursorDB):
    
    """Define bsddb3 cursor methods for a Berkeley DB primary database.

    The database must be a RECNO database.
    
    Methods added:

    _get_record
    
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
        """Return record count."""
        return self._dbset._object.stat(flags=DB_FAST_STAT)['ndata']

    def first(self):
        """Return first record taking partial key into account"""
        return self._decode_record(self._cursor.first())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        start = self._cursor.first
        step_nodup = self._cursor.next_nodup
        step = self._cursor.next
        stepback = self._cursor.prev_nodup
        keycount = self._cursor.count
        position = 0
        k = record[0]
        r = start()
        while r:
            if r[0] >= k:
                break
            position += 1
            r = step()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            start = self._cursor.last
            step_nodup = self._cursor.prev_nodup
            step = self._cursor.prev
            stepback = self._cursor.next_nodup
            position = -1 - position
        else:
            start = self._cursor.first
            step_nodup = self._cursor.next_nodup
            step = self._cursor.next
            stepback = self._cursor.prev_nodup
        keycount = self._cursor.count
        count = 0
        r = start()
        while r:
            count += 1
            if count > position:
                break
            r = step()
        if r is not None:
            return self._decode_record(r)

    def last(self):
        """Return last record taking partial key into account"""
        return self._decode_record(self._cursor.last())

    def set_partial_key(self, partial):
        """Set partial key to None for primary cursor"""
        self._partial = None

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        return self._decode_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account"""
        return self._decode_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account"""
        return self._decode_record(self._cursor.prev())

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DBapiError('_get_record not implemented')

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        return self._decode_record(self._cursor.set(record[0]))

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            k, v = record
            return k, v.decode()
        except:
            if record is None:
                return record
            raise


class CursorDBSecondary(CursorDB):
    
    """Define bsddb3 cursor methods for a Berkeley DB secondary database.

    The database must be a BTREE database.
    
    Methods added:

    _get_record
    
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
        """Return record count."""
        if self.get_partial() is None:
            count = 0
            r = self._cursor.first()
            while r:
                count += self._cursor.count()
                r = self._cursor.next_nodup()
            return count
        else:
            count = 0
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
            while r:
                if not r[0].startswith(self.get_converted_partial()):
                    break
                count += self._cursor.count()
                r = self._cursor.next_nodup()
            return count

    def first(self):
        """Return first record taking partial key into account"""
        if self.get_partial() is None:
            return self._get_record(self._cursor.first())
        elif self.get_partial() is False:
            return None
        else:
            return self.nearest(self.get_converted_partial())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        start = self._cursor.first
        step_nodup = self._cursor.next_nodup
        step = self._cursor.next
        stepback = self._cursor.prev_nodup
        keycount = self._cursor.count
        position = 0
        k = record[0]
        if not self.get_partial():
            r = start()
            while r:
                if r[0] > k:
                    break
                elif r[0] == k:
                    v = self._dbset.encode_record_number(record[1])
                    while r:
                        if r[1] > v:
                            break
                        position += 1
                        r = step()
                    break
                position += keycount()
                r = step_nodup()
            return position
        else:
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
            while r:
                if not r[0].startswith(self.get_converted_partial()):
                    break
                if r[0] > k:
                    break
                elif r[0] == k:
                    v = self._dbset.encode_record_number(record[1])
                    while r:
                        if not r[0].startswith(self.get_converted_partial()):
                            break
                        if r[1] > v:
                            break
                        position += 1
                        r = step()
                    break
                position += keycount()
                r = step_nodup()
            return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            start = self._cursor.last
            step_nodup = self._cursor.prev_nodup
            step = self._cursor.prev
            stepback = self._cursor.next_nodup
            position = -1 - position
        else:
            start = self._cursor.first
            step_nodup = self._cursor.next_nodup
            step = self._cursor.next
            stepback = self._cursor.prev_nodup
        keycount = self._cursor.count
        if not self.get_partial():
            count = 0
            r = start()
            while r:
                count += keycount()
                if count > position:
                    r = stepback()
                    count -= keycount()
                    if r is None:
                        r = start()
                    while r:
                        count += 1
                        if count > position:
                            break
                        r = step()
                    break
                r = step_nodup()
            if r is not None:
                return (r[0], self._dbset.decode_record_number(r[1]))
        else:
            count = 0
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
            while r:
                if not r[0].startswith(self.get_converted_partial()):
                    break
                count += keycount()
                if count > position:
                    r = stepback()
                    count -= keycount()
                    if r is None:
                        r = start()
                    while r:
                        if not r[0].startswith(self.get_converted_partial()):
                            break
                        count += 1
                        if count > position:
                            break
                        r = step()
                    break
                r = step_nodup()
            if r is not None:
                return (r[0], self._dbset.decode_record_number(r[1]))

    def last(self):
        """Return last record taking partial key into account"""
        if self.get_partial() is None:
            return self._get_record(self._cursor.last())
        elif self.get_partial() is False:
            return None
        else:
            k = list(self.get_partial())
            while True:
                try:
                    k[-1] = chr(ord(k[-1]) + 1)
                except ValueError:
                    k.pop()
                    if not len(k):
                        return self._get_record(self._cursor.last())
                    continue
                self._cursor.set_range(''.join(k).encode())
                return self.prev()

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        return self._get_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account"""
        return self._get_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account"""
        return self._get_record(self._cursor.prev())

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        if self.get_partial() is None:
            try:
                return (
                    record[0],
                    self._dbset.decode_record_number(record[1]))
            except:
                return None
        elif self.get_partial() is False:
            return None
        elif record[0].startswith(self.get_converted_partial()):
            try:
                return (
                    record[0],
                    self._dbset.decode_record_number(record[1]))
            except:
                return None
        else:
            return None

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
        key, value = record
        if self.get_partial() is not None:
            if not key.startswith(self.get_converted_partial()):
                return None
        if self._dbset.is_value_recno():
            return self._get_record(
                self._cursor.set_both(
                    key, self._dbset.encode_record_number(value)))
        else:
            return self._get_record(self._cursor.set_both(key, value))


class CursorDBbit(CursorDB):
    
    """Define bsddb3 style cursor methods on a segmented Berkeley DB database.

    Segmented should be read as the DPT database engine usage.

    The value part of (key, value) on secondary databases is either:

        primary key (segment and record number)
        reference to a list of primary keys for a segment
        reference to a bit map of primary keys for a segment

    References are to records on RECNO databases, one each for lists and bit
    maps, containing the primary keys.
    
    Mostly thin wrappers around the identically named bsddb3 cursor methods.

    Keys and values are converted to integer and str from Berkeley DB internal
    representation.

    Sibling classes in other ~api modules provide this interface for other
    database engines.

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

    The Berkeley DB Cursor put methods are not wrapped.

    The intended use is, using put_record as the example:

        cursor = <Database subclass instance>.database_cursor(...)
        record = cursor.<get method>(...)
        <Record subclass instance>.value.<attributes> = <Function(record)>
        <Record subclass instance>.put_record(...)

    where put_record is boilerplate, creating cursors if needed, and all the
    work to make the correct thing happen is in <Function>.

    CursorDBbitPrimary and CursorDBbitSecondary bypass CursorDBPrimary and
    CursorDBSecondary as superclasses so override nearest next and prev as
    the CursorDB versions are not appropriate in the ...bit... versions of
    the class.
    
    """

    # The refresh_recordset may be relevent in this class

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the Berkeley DB engine."""
        super(CursorDBbit, self).__init__(dbset, keyrange=keyrange)
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


class CursorDBbitPrimary(CursorDBbit):
    
    """Define bsddb3 cursor methods for primary segmented Berkeley DB database.

    The database must be a RECNO database.
    
    Methods added:

    None
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    _get_record
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
        return self._dbset._object.stat(flags=DB_FAST_STAT)['ndata']

    def first(self):
        """Return first record taking partial key into account"""
        return self._decode_record(self._cursor.first())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)"""
        ebd = self._dbset.get_existence_bits_database()
        try:
            segment, record_number = divmod(record[0], DB_SEGMENT_SIZE)
            position = 0
            for i in range(segment):
                sebm = Bitarray()
                sebm.frombytes(ebd.get(i + 1))
                position += sebm.count()
            sebm = Bitarray()
            sebm.frombytes(ebd.get(segment + 1))
            position += sebm[:record_number + 1].count()
            return position
        except:
            if record is None:
                return 0

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None"""
        if position is None:
            return None
        ebd = self._dbset.get_existence_bits_database()
        count = 0
        abspos = abs(position)
        ebdc = ebd.cursor()
        if position < 0:
            r = ebdc.last()
            while r:
                sebm = Bitarray()
                sebm.frombytes(r[1])
                sc = sebm.count()
                if count + sc < abspos:
                    count += sc
                    r = ebdc.prev()
                    continue
                recno = sebm.search(SINGLEBIT)[position + count] + (
                    (r[0] - 1) * DB_SEGMENT_SIZE)
                ebdc.close()
                return self._decode_record(self._cursor.set(recno))
        else:
            r = ebdc.first()
            while r:
                sebm = Bitarray()
                sebm.frombytes(r[1])
                sc = sebm.count()
                if count + sc < abspos:
                    count += sc
                    r = ebdc.next()
                    continue
                recno = sebm.search(SINGLEBIT)[position - count] + (
                    (r[0] - 1) * DB_SEGMENT_SIZE)
                ebdc.close()
                return self._decode_record(self._cursor.set(recno))
        ebdc.close()
        return None

    def last(self):
        """Return last record taking partial key into account"""
        return self._decode_record(self._cursor.last())

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DBapiError('_get_record not implemented')

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        return self._decode_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account"""
        return self._decode_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account"""
        return self._decode_record(self._cursor.prev())

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        return self._decode_record(self._cursor.set(record[0]))

    def set_partial_key(self, partial):
        """Set partial key to None for primary cursor"""
        self._partial = None

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            k, v = record
            return k, v.decode()
        except:
            if record is None:
                return record
            raise


class CursorDBbitSecondary(CursorDBbit):
    
    """Define bsddb3 cursor methods on secondary segmented Berkeley DB database.

    The database must be a BTREE database.
    
    Methods added:

    _first
    set_current_segment
    _last
    _next
    _prev
    _set_both
    _set_range
    _first_partial
    _last_partial
    
    Methods overridden:

    count_records
    first
    get_position_of_record
    _get_record
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

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the Berkeley DB engine."""
        super(CursorDBbitSecondary, self).__init__(dbset, keyrange=keyrange)
        self._segment_bits = self._dbset.get_primary_segment_bits()
        self._segment_list = self._dbset.get_primary_segment_list()

    def count_records(self):
        """Return record count."""
        if self.get_partial() is None:
            count = 0
            r = self._cursor.first()
            while r:
                if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                    count += int.from_bytes(r[1][4:6], byteorder='big')
                elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    count += int.from_bytes(r[1][4:7], byteorder='big')
                else:
                    count += 1
                r = self._cursor.next()
            return count
        else:
            count = 0
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
            while r:
                if not r[0].startswith(self.get_converted_partial()):
                    break
                if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                    count += int.from_bytes(r[1][4:6], byteorder='big')
                elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    count += int.from_bytes(r[1][4:7], byteorder='big')
                else:
                    count += 1
                r = self._cursor.next()
            return count

    def first(self):
        """Return first record taking partial key into account"""
        if self.get_partial() is None:
            try:
                k, v = self._first()
            except TypeError:
                return None
            return k.decode(), v
        elif self.get_partial() is False:
            return None
        else:
            return self.nearest(self.get_converted_partial())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)"""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(value, DB_SEGMENT_SIZE)
        # Define lambdas to handle presence or absence of partial key
        low = lambda rk, recordkey: rk < recordkey
        if not self.get_partial():
            high = lambda rk, recordkey: rk > recordkey
        else:
            high = lambda rk, partial: not rk.startswith(partial)
        # Get position of record relative to start point
        position = 0
        if not self.get_partial():
            r = self._cursor.first()
        else:
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
        while r:
            if low(r[0].decode(), key):
                if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                    position += int.from_bytes(r[1][4:6], byteorder='big')
                elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    position += int.from_bytes(r[1][4:7], byteorder='big')
                else:
                    position += 1
            elif high(r[0].decode(), key):
                break
            else:
                sr = int.from_bytes(r[1][:4], byteorder='big')
                if sr < segment_number:
                    if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                        position += int.from_bytes(
                            r[1][4:6], byteorder='big')
                    elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                        position += int.from_bytes(
                            r[1][4:7], byteorder='big')
                    else:
                        position += 1
                elif sr > segment_number:
                    break
                else:
                    if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                        srn = int.from_bytes(r[1][6:], byteorder='big')
                        segment = SegmentList(
                            segment_number,
                            None,
                            records=self._segment_list.get(srn))
                    elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                        srn = int.from_bytes(r[1][7:], byteorder='big')
                        segment = SegmentBitarray(
                            segment_number,
                            None,
                            records=self._segment_bits.get(srn))
                    else:
                        segment = SegmentInt(
                            segment_number,
                            None,
                            records=r[1][4:])
                    position += segment.get_position_of_record_number(
                        record_number)
                    break
            r = self._cursor.next()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None"""
        if position is None:
            return None
        # Start at first or last record whichever is likely closer to position
        # and define lambdas to handle presence or absence of partial key.
        if position < 0:
            step = self._cursor.prev
            get_partial = self.get_partial
            position = -1 - position
            if not self.get_partial():
                start = lambda partial: self._cursor.last()
            else:
                start = lambda partial: self._last_partial(partial)
        else:
            step = self._cursor.next
            get_partial = self.get_converted_partial
            if not self.get_partial():
                start = lambda partial: self._cursor.first()
            else:
                start = lambda partial: self._first_partial(partial)
        # Get record at position relative to start point
        count = 0
        r = start(get_partial())
        while r:
            if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                sc = int.from_bytes(r[1][4:6], byteorder='big')
            elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                sc = int.from_bytes(r[1][4:7], byteorder='big')
            else:
                sc = 1
            count += sc
            if count < position:
                r = step()
            else:
                count -= position
                if len(r[1]) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn = int.from_bytes(r[1][6:], byteorder='big')
                    segment = SegmentList(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=self._segment_list.get(srn))
                elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(r[1][7:], byteorder='big')
                    segment = SegmentBitarray(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=self._segment_bits.get(srn))
                else:
                    segment = SegmentInt(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=r[1][4:])
                record_number = segment.get_record_number_at_position(
                    count, step is self._cursor.next)
                if record_number is not None:
                    return r[0].decode(), record_number
                break
        return None

    def last(self):
        """Return last record taking partial key into account"""
        if self.get_partial() is None:
            try:
                k, v = self._last()
            except TypeError:
                return None
            return k.decode(), v
        elif self.get_partial() is False:
            return None
        else:
            c = list(self.get_partial())
            while True:
                try:
                    c[-1] = chr(ord(c[-1]) + 1)
                except ValueError:
                    c.pop()
                    if not len(c):
                        try:
                            k, v = self._cursor.last()
                        except TypeError:
                            return None
                        return k.decode(), v
                    continue
                self._set_range(''.join(c).encode())
                return self.prev()

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DBapiError('_get_record not implemented')

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        try:
            k, v = self._set_range(key)
        except TypeError:
            return None
        return k.decode(), v

    def next(self):
        """Return next record taking partial key into account"""
        try:
            k, v = self._next()
        except TypeError:
            return None
        return k.decode(), v

    def prev(self):
        """Return previous record taking partial key into account"""
        try:
            k, v = self._prev()
        except TypeError:
            return None
        return k.decode(), v

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
        key, value = record
        if self.get_partial() is not None:
            if not key.startswith(self.get_partial()):
                return None
        try:
            k, v = self._set_both(key.encode(), value)
        except TypeError:
            return None
        return k.decode(), v

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial

    def _first(self):
        """Return first record taking partial key into account"""
        record = self._cursor.first()
        if record is None:
            return None
        return self.set_current_segment(*record).first()

    def set_current_segment(self, key, reference):
        """Return a SegmentBitarray, SegmentInt, or SegmentList instance."""
        segment_number = int.from_bytes(reference[:4], byteorder='big')
        if len(reference) == LENGTH_SEGMENT_LIST_REFERENCE:
            if self._current_segment_number == segment_number:
                #return self._current_segment
                if key == self._current_segment._key:
                    return self._current_segment
            record_number = int.from_bytes(reference[6:], byteorder='big')
            segment = SegmentList(
                segment_number,
                key,
                records=self._segment_list.get(record_number))
        elif len(reference) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            if self._current_segment_number == segment_number:
                #return self._current_segment
                if key == self._current_segment._key:
                    return self._current_segment
            record_number = int.from_bytes(reference[7:], byteorder='big')
            segment = SegmentBitarray(
                segment_number,
                key,
                records=self._segment_bits.get(record_number))
        else:
            segment = SegmentInt(segment_number, key, records=reference[4:])
        self._current_segment = segment
        self._current_segment_number = segment_number
        return segment

    def _last(self):
        """Return last record taking partial key into account"""
        record = self._cursor.last()
        if record is None:
            return None
        return self.set_current_segment(*record).last()

    def _next(self):
        """Return next record taking partial key into account"""
        if self._current_segment is None:
            return self._first()
        record = self._current_segment.next()
        if record is None:
            record = self._cursor.next()
            if record is None:
                return None
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).first()
        else:
            return record

    def _prev(self):
        """Return previous record taking partial key into account"""
        if self._current_segment is None:
            return self._last()
        record = self._current_segment.prev()
        if record is None:
            record = self._cursor.prev()
            if record is None:
                return None
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).last()
        else:
            return record

    def _set_both(self, key, value):
        """Return current record after positioning cursor at (key, value)."""
        segment, record_number = divmod(value, DB_SEGMENT_SIZE)
        # Find the segment reference in secondary database
        cursor = self._dbset._object.cursor()
        record = cursor.set_range(key)
        while record:
            if record[0] != key:
                cursor.close()
                return None
            segment_number = int.from_bytes(record[1][:4], byteorder='big')
            if segment_number > segment:
                cursor.close()
                return None
            if segment_number == segment:
                cursor.close()
                break
            record = cursor.next()
        else:
            cursor.close()
            return None
        # Check if record number is in segment
        ref = record[1]
        if len(ref) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(ref[6:], byteorder='big')
            segment = SegmentList(
                segment_number, key, records=self._segment_list.get(srn))
        elif len(ref) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(ref[7:], byteorder='big')
            segment = SegmentBitarray(
                segment_number, key, records=self._segment_bits.get(srn))
        else:
            segment = SegmentInt(segment_number, key, records=ref[4:])
        if segment.setat(value) is None:
            return None
        # Move self._cursor to new segment reference
        record = self._cursor.set_both(key, ref)
        if record is None:
            return None
        self._current_segment = segment
        self._current_segment_number = segment_number
        # Return the "secondary database record".
        return key, value

    def _set_range(self, key):
        """Return current record after positioning cursor at nearest to key."""
        # Move self._cursor to nearest segment reference
        record = self._cursor.set_range(key)
        if record is None:
            self._current_segment = None
            self._current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        segment_number = int.from_bytes(record[1][:4], byteorder='big')
        # Set up the segment instance and position at first record
        ref = record[1]
        if len(ref) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(ref[6:], byteorder='big')
            segment = SegmentList(
                segment_number, record[0], records=self._segment_list.get(srn))
        elif len(ref) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(ref[7:], byteorder='big')
            segment = SegmentBitarray(
                segment_number, record[0], records=self._segment_bits.get(srn))
        else:
            segment = SegmentInt(segment_number, record[0], records=ref[4:])
        self._current_segment = segment
        self._current_segment_number = segment_number
        # Return the "secondary database record".
        return segment.first()

    def _first_partial(self, partial):
        """Place cursor at first record with partial key and return record."""
        r = self._cursor.set_range(partial)
        if r is None:
            return None
        if not r[0].startswith(partial):
            return None
        return r

    def _last_partial(self, partial):
        """Place cursor at last record with partial key and return record."""
        # This code is wrong but the only place using it does not work yet.
        # Should it be doing _cursot.last() _cursor.set_range() _cursor.prev()?
        k = list(partial)
        while True:
            try:
                k[-1] = chr(ord(k[-1]) + 1)
            except ValueError:
                k.pop()
                if not len(k):
                    return self._last()
                continue
            self._set_range(''.join(k).encode())
            return self._prev()

            
class DBbitControlFile(object):
    
    """Define a DB file for control information about the database DB files.

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

    The method names used in DBbitPrimaryFile and superclasses are used
    where possible.  But this file is primary DB_BTREE NODUP.  It is used by
    all FileControl instances.
    
    """

    def __init__(self, control_file='control'):
        """File control database for all DB files."""
        super(DBbitControlFile, self).__init__()
        self._control_file = ''.join((SUBFILE_DELIMITER * 3, control_file))
        self._control_object = None

    def open_root(self, dbenv):
        """Create file control database in environment"""
        try:
            self._control_object = DB(dbenv)
        except:
            raise
        try:
            self._control_object.set_flags(DB_DUPSORT)
            self._control_object.open(
                self._control_file,
                self._control_file,
                DB_BTREE,
                DB_CREATE)
        except:
            self._control_object = None
            raise

    def close(self):
        """Close file control database."""
        if self._control_object is not None:
            self._control_object.close()
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
            cursor = self._dbfile.get_control_database().cursor()
            record = cursor.set(b'E')
            while record:
                self._freed_record_number_pages.append(
                    int.from_bytes(record[1], byteorder='big'))
                record = cursor.next_dup()
        insert = bisect.bisect_left(self._freed_record_number_pages, segment)
        if self._freed_record_number_pages[insert] == segment:
            return
        self._freed_record_number_pages.insert(insert, segment)
        self._dbfile.get_control_database().put(
            b'E',
            segment.to_bytes(1 + page.bit_length() // 8, byteorder='big'),
            flags=DB_NODUPDATA)

    def get_lowest_freed_record_number(self):
        """Return low record number in segments with freed record numbers"""
        if self._freed_record_number_pages is None:
            self._freed_record_number_pages = []
            cursor = self._dbfile.get_control_database().cursor()
            record = cursor.set(b'E')
            while record:
                self._freed_record_number_pages.append(
                    int.from_bytes(record[1], byteorder='big'))
                record = cursor.next_dup()
            del cursor
        while len(self._freed_record_number_pages):
            s = self._freed_record_number_pages[0]
            lfrns = self._read_exists_segment(s)
            if lfrns is None:
                # Do not reuse record number on segment of high record number
                return 0
            try:
                first_zero_bit = lfrns.index(False)
            except ValueError:
                cursor = self._dbfile.get_control_database().cursor()
                if cursor.set_both(
                    b'E',
                    s.to_bytes(1 + page.bit_length() // 8, byteorder='big')):
                    cursor.delete()
                else:
                    raise
                del cursor
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
            return ebm.frombytes(
                self._dbfile.get_existence_bits_database().get(page)[1])
        return None


class FileControlSecondary(FileControl):
    
    """Freed resource data for a segmented file.

    Methods added:

    note_freed_bits_page
    note_freed_list_page
    _get_bits_page_number
    get_freed_bits_page
    get_freed_list_page
    _get_list_page_number
    _put_bits_page_number
    _put_list_page_number
    
    Methods overridden:

    None
    
    Methods extended:

    __init__
    
    Properties:

    freed_list_pages
    freed_bits_pages

    Notes

    Introduced to keep track of freed pages on the record number bit map and
    record number list files for each segmented file.

    These pages can be freed when records are deleted or when an inverted list
    for a key is moved between bit map and list representations.

    (The term page is used because record number n on --list and --bits files
    is not in general referring to segment number n on the segmented file.)

    """

    def __init__(self, *args):
        """Define the file control data for secondary files."""
        super(FileControlSecondary, self).__init__(*args)
        self._freed_list_pages = None
        self._freed_bits_pages = None

    @property
    def freed_list_pages(self):
        """List pages available for re-use"""
        return self._freed_list_pages

    @property
    def freed_bits_pages(self):
        """Bit Map pages available for re-use"""
        return self._freed_bits_pages

    def note_freed_bits_page(self, page_number):
        """Add page_number to freed bits pages"""
        self._put_bits_page_number(page_number)

    def note_freed_list_page(self, page_number):
        """Add page_number to freed list pages"""
        self._put_list_page_number(page_number)

    def get_freed_bits_page(self):
        """Return low page from freed bits pages"""
        if self._freed_bits_pages is False:
            return 0 # record number when inserting into RECNO database
        return self._get_bits_page_number()

    def get_freed_list_page(self):
        """Return low page from freed list pages"""
        if self._freed_list_pages is False:
            return 0 # record number when inserting into RECNO database
        return self._get_list_page_number()

    def _put_bits_page_number(self, page):
        """Put page on freed bits page record"""
        self._dbfile.get_control_database().put(
            b'B',
            page.to_bytes(1 + page.bit_length() // 8, byteorder='big'),
            flags=DB_NODUPDATA)
        self._freed_bits_pages = True

    def _put_list_page_number(self, page):
        """Put page on freed list page record"""
        self._dbfile.get_control_database().put(
            b'L',
            page.to_bytes(1 + page.bit_length() // 8, byteorder='big'),
            flags=DB_NODUPDATA)
        self._freed_list_pages = True

    def _get_bits_page_number(self):
        """Pop low page from freed bits page record"""
        cursor = self._dbfile.get_control_database().cursor()
        record = cursor.set(b'B')
        if record:
            cursor.delete()
            self._freed_bits_pages = False
            return int.from_bytes(record[1], byteorder='big')
        self._freed_bits_pages = False
        return 0 # record number when inserting into RECNO database

    def _get_list_page_number(self):
        """Pop low page from freed list page record"""
        cursor = self._dbfile.get_control_database().cursor()
        record = cursor.set(b'L')
        if record:
            cursor.delete()
            self._freed_list_pages = False
            return int.from_bytes(record[1], byteorder='big')
        self._freed_list_pages = False
        return 0 # record number when inserting into RECNO database
