# dbapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Object database using Berkeley DB.

List of classes

DBapiError - Exceptions
DBapi - Define database and file and record level access methods
DBapiFile - File level access to each file in database (Open, Close)
DBapiRecord - Record level access to each file in database
CursorDB - Define cursor on file and access methods

"""

import os
import subprocess

import sys
_platform_win32 = sys.platform == 'win32'
del sys

# bsddb removed from Python 3.n
try:
    from bsddb3.db import (
        DB_KEYLAST, DB_CURRENT, DB_DUP, DB_DUPSORT,
        DB_BTREE, DB_HASH, DB_RECNO, DB_UNKNOWN,
        DBEnv, DB, DB_CREATE, DB_FAST_STAT, DBKeyExistError,
        )
except ImportError:
    from bsddb.db import (
        DB_KEYLAST, DB_CURRENT, DB_DUP, DB_DUPSORT,
        DB_BTREE, DB_HASH, DB_RECNO, DB_UNKNOWN,
        DBEnv, DB, DB_CREATE, DB_FAST_STAT, DBKeyExistError,
        )

from api.database import (
    DatabaseError, Database, Cursor,
    decode_record_number, encode_record_number,
    )
from api.constants import (
    DB_DEFER_FOLDER, SECONDARY_FOLDER,
    PRIMARY, SECONDARY, FILE, HASH_DUPSORT, BTREE_DUPSORT,
    DUP, BTREE, HASH, RECNO, DUPSORT,
    )

_DB_CONST_MAP = {
    DUP:DB_DUP, BTREE:DB_BTREE, HASH:DB_HASH,
    RECNO:DB_RECNO, DUPSORT:DB_DUPSORT,
    HASH_DUPSORT:(DB_HASH, DB_DUPSORT),
    BTREE_DUPSORT:(DB_BTREE, DB_DUPSORT),
    }


class DBapiError(DatabaseError):
    pass


class DBapi(Database):
    
    """Define a Berkeley DB database structure.
    
    By default primary databases are created as DB_RECNO files.
    DB_BTREE and DB_HASH databases without secondary databases can be
    created with DB_DUPSORT set so that DB_NODUPDATA updates can be used.
    DBtypes specifies modifications to primary database properties.
    Secondary databases are DB_BTREE files with DB_DUPSORT set.
    Deferred update is allowed for each primary DB to allow speedier import
    of large amounts of data.  A set of sorted sequential files are created
    from the imported data then these are merged into main DB in key order.
    Existing indexes are written to sequential files, deleted, and included
    in the merge.

    Primary and secondary terminology comes from Berkeley DB documentation
    but the cursor joins possible in that implementation are not supported.

    Methods added:

    do_deferred_updates
    files_exist
    increase_database_size
    initial_database_size
    set_defer_update
    unset_defer_update

    Methods overridden:

    __init__
    backout
    close_context
    close_database
    commit
    close_internal_cursors
    db_compatibility_hack
    delete_instance
    edit_instance
    exists
    make_cursor
    get_database_folder
    get_database
    get_database_instance
    get_first_primary_key_for_index_key
    get_primary_record
    make_internal_cursors
    is_primary
    is_primary_recno
    is_recno
    open_context
    get_packed_key
    decode_as_primary_key
    encode_primary_key
    put_instance
    use_deferred_update_process

    Methods extended:

    None
    
    """

    def __init__(self,
                 DBhome,
                 DBnames,
                 DBtypes,
                 DBenvironment,
                 secondarydir=None,
                 defercontrol=None):
        """Define database structure.
        
        DBhome = full path for database directory
        DBnames = {name:{primary:name,
                         secondary:{name:name...},
                         }, ...}
        DBtypes = {name:((organisation, duplication), name),
                   name:(organisation, duplication), ...}
        DBenvironment = {<DB property>:<value>, ...}
        secondarydir = folder for secondary files (default SECONDARY_FOLDER)
        defercontrol = control db for deferred updates (default DB_DEFER_FOLDER)

        """
        # Comment no longer best placed at this point. Still relevant.
        # Functions to convert numeric keys to string representation.
        # By default base 256 with the least significant digit at the right.
        # least_significant_digit = string_value[-1] (lsd = sv[-1])
        # most_significant_digit = string_value[0]
        # This conversion makes string sort equivalent to numeric sort.
        # These functions introduced to allow dbapi.py and dptapi.py to be
        # interchangeable for user classes.
        # DPT (www.dptoolkit.com) does not allow CR or LF characters in data in
        # deferred update mode. decode_record_number and encode_record_number
        # allow a base 128 conversion to be used with the top bit set thus
        # providing one way of avoiding the problem characters. There are ways
        # to avoid the restriction allowing CR and LF in data.
        # DPT uses a big endian conversion (lsd = sv[0]).

        fileperdb = False
        for n in DBnames:
            if FILE in DBnames[n]:
                fileperdb = True
                break

        if defercontrol is None:
            defercontrol = DB_DEFER_FOLDER
            
        if fileperdb:
            if secondarydir is None:
                secondarydir = SECONDARY_FOLDER

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
                raise DBapiError, msg
            if fileperdb:
                DBfiles[f] = f
            else:
                DBfiles[f] = basefile

        for n in DBnames:
            sec = DBnames[n].setdefault(SECONDARY, dict())
            for s in sec:
                if sec[s] is None:
                    sec[s] = s
                f = sec[s]
                if f in DBfiles:
                    msg = ' '.join(['DB name', f, 'requested for secondary',
                                    'name', s, 'in primary name', n,
                                    'is already specified'])
                    raise DBapiError, msg
                if fileperdb:
                    DBfiles[f] = os.path.join(secondarydir, f)
                else:
                    DBfiles[f] = basefile

        for t in DBtypes:
            if len(DBtypes[t]) == 2:
                f = DBtypes[t][-1]
            else:
                f = t
            if f not in DBfiles:
                if fileperdb:
                    DBfiles[f] = f
                else:
                    DBfiles[f] = basefile

        # Associate primary and secondary DBs by name.
        # {secondary name:primary name, ...,
        #  primary name:[secondary name, ...], ...}
        # A secondary name may be a primary name if a loop is not made.
        self._associate = dict()
        
        # DBapiRecord objects, containing the DB object, for all DB names
        # {name:DBapiRecord instance, ...}
        self._main = dict()
        
        # Home directory for the DBenv
        self._home = DBhome
        
        # Name of deferred update control DB
        self._defercontrol = defercontrol
        
        # Set up primary databases in DBnames.
        for n in DBnames:
            if n not in DBtypes:
                dbtype = DB_RECNO
                dbdupsort = 0
            elif DBtypes[n][0][0] in (RECNO, BTREE, HASH):
                dbtype = _DB_CONST_MAP[DBtypes[n][0][0]]
                dbdupsort = 0
            elif DBtypes[n][0][0] in (BTREE_DUPSORT, HASH_DUPSORT):
                dbtype, dbdupsort = _DB_CONST_MAP[DBtypes[n][0][0]]
            else:
                dbtype = DB_RECNO
                dbdupsort = 0
            f = DBnames[n][PRIMARY]
            self._main[f] = self.make_root(
                DBfiles[f],
                f,
                dbtype,
                True,
                dbdupsort,
                False)
            self._associate[n] = {n:f}

        # Set up secondary databases in DBnames.
        for n in DBnames:
            fn = DBnames[n][PRIMARY]
            if self._main[fn]._dbdupsort == 0:
                for s in DBnames[n][SECONDARY]:
                    f = DBnames[n][SECONDARY][s]
                    self._main[f] = self.make_root(
                        DBfiles[f],
                        f,
                        DB_BTREE,
                        False,
                        DB_DUPSORT,
                        self._main[fn]._dbtype == DB_RECNO)
                    self._associate[n][s] = f

        # Set up primary databases implied by DBtypes not in DBnames.
        for n in DBtypes:
            if len(DBtypes[n]) == 2:
                f = DBtypes[n][1]
            else:
                f = n
            if f not in self._main:
                if DBtypes[n][0] in (RECNO, BTREE, HASH):
                    dbtype = DBtypes[n][0]
                    dbtype = _DB_CONST_MAP[DBtypes[n][0]]
                    dbdupsort = False
                elif DBtypes[n][0] in (BTREE_DUPSORT, HASH_DUPSORT):
                    dbtype, dbdupsort = _DB_CONST_MAP[DBtypes[n][0]]
                else:
                    dbtype = DB_RECNO
                    dbdupsort = 0
                self._main[f] = self.make_root(
                    DBfiles[f],
                    f,
                    dbtype,
                    True,
                    dbdupsort,
                    self.is_primary_recno,
                    False)
                self._associate[n] = {n:f}

        # The DBenv object
        self._dbenv = None
        
        # Parameters for setting up the DBenv object
        self._DBenvironment = DBenvironment
        
        # Name of directory containing secondary DB files
        self._secondarydir = secondarydir
        
        # Count of records with deferred updates pending.
        self._defer_record_count = 0

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

    def close_database(self):
        """Close main and deferred update databases and environment.

        Introduced for compatibility with DPT.  There is a case for closing
        self._dbenv in this method rather than doing it all in close_context.
        
        """
        self.close_context()

    def close_internal_cursors(self, dbsets=None):
        """Close cursors on database files named in dbsets and return True.

        Default all.

        """
        if dbsets is None:
            dbsets = self._associate
        elif isinstance(dbsets, str):
            dbsets = [dbsets]

        if not isinstance(dbsets, (list, tuple, dict)):
            return False

        for d in dbsets:
            db = self._associate[d]
            for n in db:
                self._main[db[n]].close_root_cursor()

        return True
            
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
        if self.is_primary_recno(dbset):
            deletekey = instance.key.pack()
        else:
            deletekey = instance.packed_key()
        instance.set_packed_value_and_indexes()
        
        db = self._associate[dbset]
        main = self._main

        main[db[dbset]].delete(deletekey, instance.srvalue)
        if self.is_primary_recno(dbset):
            instance.srkey = encode_record_number(deletekey)
        else:
            instance.srkey = deletekey

        srindex = instance.srindex
        srkey = instance.srkey
        dcb = instance._deletecallbacks
        for secondary in srindex:
            if secondary not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                main[db[secondary]].delete(v, deletekey)

    def do_deferred_updates(self, pyscript, filepath):
        """Invoke a deferred update process and wait for it to finish.

        pyscript is the script to do the deferred update.
        filepath is a file or a sequence of files containing updates.

        """
        if _platform_win32:
            args = ['pythonw']
        else:
            args = ['python']
        
        if not os.path.isfile(pyscript):
            msg = ' '.join([repr(pyscript),
                            'is not an existing file'])
            raise DBapiError, msg

        args.append(pyscript)
        
        try:
            if os.path.exists(filepath):
                paths = (filepath,)
            else:
                msg = ' '.join([repr(filepath),
                                'is not an existing file'])
                raise DBapiError, msg
        except:
            paths = tuple(filepath)
            for fp in paths:
                if not os.path.isfile(fp):
                    msg = ' '.join([repr(fp),
                                    'is not an existing file'])
                    raise DBapiError, msg

        args.append(os.path.abspath(self._home))
        args.extend(paths)

        return subprocess.Popen(args)

    def edit_instance(self, dbset, instance):
        """Edit an existing instance on databases in dbset.
        
        Edits are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.

        """
        if self.is_primary_recno(dbset):
            oldkey = instance.key.pack()
            newkey = instance.newrecord.key.pack()
        else:
            oldkey = instance.packed_key()
            newkey = instance.newrecord.packed_key()
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
            main[db[dbset]].delete(oldkey, instance.srvalue)
            key = main[db[dbset]].put(newkey, instance.newrecord.srvalue)
            if key is not None:
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

        if self.is_primary_recno(dbset):
            instance.srkey = encode_record_number(oldkey)
            instance.newrecord.srkey = encode_record_number(newkey)
        else:
            instance.srkey = oldkey
            instance.newrecord.srkey = newkey

        srkey = instance.srkey
        nsrkey = instance.newrecord.srkey
        
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

    def make_cursor(self, dbset, dbname, keyrange=None):
        """Create and return a cursor on DB dbname in dbset.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        return self._main[self._associate[dbset][dbname]].make_cursor(
            self._main[self._associate[dbset][dbname]],
            keyrange)

    def get_database_folder(self):
        """return database folder name"""
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
            raise DBapiError, (
                'get_first_primary_key_for_index_key for primary index')

        try:
            dbobj = self._main[self._associate[dbset][dbname]]
            db_engine_cursor = dbobj._db_engine_cursor
            if self.is_primary_recno(dbset):
                if db_engine_cursor is not None:
                    return decode_record_number(db_engine_cursor.set(key)[1])
                else:
                    return decode_record_number(dbobj._object.get(key))
            else:
                if db_engine_cursor is not None:
                    return db_engine_cursor.set(key)[1]
                else:
                    return dbobj._object.get(key)
        except:
            return None

    def get_primary_record(self, dbset, key):
        """Return primary record (key, value) given primary key on dbset."""
        try:
            dbobj = self._main[self._associate[dbset][dbset]]
            if dbobj._db_engine_cursor is not None:
                return dbobj._db_engine_cursor.set(key)
            else:
                return (key, dbobj._object.get(key))
        except:
            return None

    def make_internal_cursors(self, dbsets=None):
        """Create a cursor on each DB in dbsets and return True.  Default all.
        
        If the DBapiRecord already has a cursor use that.  These cursors are
        used by delete_instance edit_instance and put_instance and other DBapi
        methods.  Consider using make_cursor instead to avoid interference.
        
        """
        if dbsets is None:
            dbsets = self._associate
        elif isinstance(dbsets, str):
            dbsets = [dbsets]

        if not isinstance(dbsets, (list, tuple, dict)):
            return False

        for d in dbsets:
            db = self._associate[d]
            for n in db:
                self._main[db[n]].make_root_cursor()

        return True

    def is_primary(self, dbset, dbname):
        """Return True if dbname is primary database in dbset."""
        return self._main[self._associate[dbset][dbname]]._dbprimary

    def is_primary_recno(self, dbset):
        """Return True if primary DB in dbset is RECNO."""
        return self._main[self._associate[dbset][dbset]]._dbtype == DB_RECNO

    def is_recno(self, dbset, dbname):
        """Return True if DB dbname in dbset is RECNO."""
        return self._main[self._associate[dbset][dbname]]._dbtype == DB_RECNO

    def open_context(self):
        """Open all DBs."""
        try:
            os.mkdir(self._home)
        except:
            pass

        if self._secondarydir:
            try:
                os.mkdir(os.path.join(self._home, self._secondarydir))
            except:
                pass
        
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

    def get_packed_key(self, dbset, instance):
        """Return instance.key converted to string for dbset.

        encode_record_number provides this for RECNO databases.
        packed_key method of instance does conversion otherwise.

        """
        if self.is_primary_recno(dbset):
            return encode_record_number(instance.key.pack())
        else:
            return instance.packed_key()

    def decode_as_primary_key(self, dbset, pkey):
        """Return primary key after converting from secondary database format.

        No conversion is required if the primary DB is not RECNO.
        
        """
        if self.is_primary_recno(dbset):
            return decode_record_number(pkey)
        else:
            return pkey

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
        if self.is_primary_recno(dbset):
            putkey = instance.key.pack()
        else:
            putkey = instance.packed_key()
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
            for v in srindex[secondary]:
                main[db[secondary]].put(v, putkey)

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
        raise DBapiError, 'use_deferred_update_process not implemented'

    def make_root(
        self, dbfile, dbname, dbtype, primary, dupsort, value_is_recno):

        return DBapiRecord(
            dbfile, dbname, dbtype, primary, dupsort, value_is_recno)

    def initial_database_size(self):
        """Do nothing and return True as method exists for DPT compatibility"""
        return True

    def increase_database_size(self, **ka):
        """Do nothing because method exists for DPT compatibility"""

            
class DBapiFile(object):
    
    """Define a DB file with a cursor and open_root and close methods.

    Methods added:

    close
    make_root_cursor
    close_root_cursor
    open_root

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(
        self, dbfile, dbname, dbtype, primary, dupsort, value_is_recno):
        """Define a DB file.
        
        dbfile=file name relative to environment home directory
        dbname=db name within file
        dbtype=DB_BTREE|DB_HASH|DB_RECNO
        primary=True|False. DB is primary or secondary
        dupsort=True|False. DB has records with duplicate keys
        limit=maximum bytes for each defer update sequential file
        
        """
        super(DBapiFile, self).__init__()

        self._object = None
        self._db_engine_cursor = None
        self._dbfolder, self._dbfile = os.path.split(dbfile)
        self._dbname = dbname
        self._dbtype = dbtype
        self._dbdupsort = dupsort
        self._dbprimary = primary
        self._value_is_recno = value_is_recno
        
    def close(self):
        """Close DB and cursor."""
        self.close_root_cursor()
        if self._object is not None:
            self._object.close()
            self._object = None

    def make_root_cursor(self):
        """
        Create cursor on DB.
        
        This is the cursor used by delete put and replace so a
        reference is kept in self._db_engine_cursor. It is better to use
        DBapi.make_cursor to get a cursor for other purposes.
        
        """
        if self._db_engine_cursor is None:
            self._db_engine_cursor = self._object.cursor()

    def close_root_cursor(self):
        """Close cursor associated with DB."""
        if self._db_engine_cursor is not None:
            self._db_engine_cursor.close()
            self._db_engine_cursor = None

    def open_root(self, dbenv):
        """Open DB in dbenv."""
        try:
            self._object = DB(dbenv)
            if self._dbdupsort != 0:
                self._object.set_flags(self._dbdupsort)
            self._object.open(
                os.path.join(self._dbfolder, self._dbfile),
                self._dbname,
                self._dbtype,
                DB_CREATE)
            self.make_root_cursor()
        except:
            self._object = None
            raise


class DBapiRecord(DBapiFile):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    make_cursor
    put
    replace

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(
        self, dbfile, dbname, dbtype, primary, dupsort, value_is_recno):
        """Define a DB file.

        value_is_recno=boolean stating if values are encoded record numbers
        See superclass for other arguments
        
        """
        super(DBapiRecord, self).__init__(
            dbfile, dbname, dbtype, primary, dupsort, value_is_recno)

        self._dbputDB = self._dbprimary and not self._dbdupsort
        self._clientcursors = dict()
    
    def close(self):
        """Close DB and any cursors."""
        for c in self._clientcursors.keys():
            c.close()
        self._clientcursors.clear()
        super(DBapiRecord, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            if self._dbputDB:
                if self._db_engine_cursor.set(key):
                    self._db_engine_cursor.delete()
            elif self._value_is_recno:
                if self._db_engine_cursor.set_both(
                    key, encode_record_number(value)):
                    self._db_engine_cursor.delete()
            elif self._db_engine_cursor.set_both(key, value):
                self._db_engine_cursor.delete()
        except:
            pass

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorDB(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new RECNO records.

        The DB put method, or append for new RECNO records,is
        used for primary DBs with associated secondary DBs. The
        cursor put method is used otherwise.
        
        """
        if self._dbputDB:
            if not key: #key == 0:  # Change test to "key is None" when sure
                return self._object.append(value)
            else:
                self._object.put(key, value)
                return None

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

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on DB.
        
        (key, newvalue) is put on DB only if (key, oldvalue) is on DB.
        
        """
        try:
            if self._dbputDB:
                if self._db_engine_cursor.set(key):
                    self._db_engine_cursor.put(key, newvalue, DB_CURRENT)
            elif self._value_is_recno:
                if self._db_engine_cursor.set_both(
                    key, encode_record_number(oldvalue)):
                    self._db_engine_cursor.put(
                        key, encode_record_number(newvalue), DB_CURRENT)
            elif self._db_engine_cursor.set_both(key, oldvalue):
                self._db_engine_cursor.put(key, newvalue, DB_CURRENT)
        except:
            pass


class CursorDB(Cursor):
    
    """Define cursor implemented using the Berkeley DB cursor methods.
    
    Methods added:

    _get_record
    
    Methods overridden:

    __init__
    close
    count_records
    database_cursor_exists
    first
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    refresh_recordset
    setat
    set_partial_key
    
    Methods extended:

    None
    
    """

    def __init__(self, dbset, keyrange=None):
        """Define a cursor using the Berkeley DB engine."""
        self._cursor = None
        self._dbset = dbset
        self._partial = None

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
        self._partial = None

    def count_records(self):
        """return record count or None if cursor is not usable"""
        if self._dbset._dbtype == DB_RECNO:
            return self._dbset._object.stat(flags=DB_FAST_STAT)['ndata']
        elif self._partial is None:
            count = 0
            r = self._cursor.first()
            while r:
                count += self._cursor.count()
                r = self._cursor.next_nodup()
            return count
        else:
            count = 0
            r = self._cursor.set_range(self._partial)
            while r:
                if not r[0].startswith(self._partial):
                    break
                count += self._cursor.count()
                r = self._cursor.next_nodup()
            return count

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise"""
        return bool(self._cursor)

    def first(self):
        """Return first record taking partial key into account"""
        if self._partial is None:
            return self._get_record(self._cursor.first())
        elif self._partial == False:
            return None
        else:
            return self.nearest(self._partial)

    def get_position_of_record(self, key=None):
        """return position of record in file or 0 (zero)"""
        if key is None:
            return 0
        start = self._cursor.first
        step_nodup = self._cursor.next_nodup
        step = self._cursor.next
        stepback = self._cursor.prev_nodup
        keycount = self._cursor.count
        position = 0
        k = key[0]
        if self._dbset._dbtype == DB_RECNO:
            r = start()
            while r:
                if r[0] >= k:
                    break
                position += 1
                r = step()
            return position
        elif not self._partial:
            r = start()
            while r:
                if r[0] > k:
                    break
                elif r[0] == k:
                    while r:
                        if r > key:
                            break
                        position += 1
                        r = step()
                    break
                position += keycount()
                r = step_nodup()
            return position
        else:
            r = self._cursor.set_range(self._partial)
            while r:
                if not r[0].startswith(self._partial):
                    break
                if r[0] > k:
                    break
                elif r[0] == k:
                    while r:
                        if not r[0].startswith(self._partial):
                            break
                        if r > key:
                            break
                        position += 1
                        r = step()
                    break
                position += keycount()
                r = step_nodup()
            return position

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
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
        if self._dbset._dbtype == DB_RECNO:
            count = 0
            r = start()
            while r:
                count += 1
                if count > position:
                    break
                r = step()
            if r is not None:
                return r
        elif not self._partial:
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
                return (r[0], decode_record_number(r[1]))
        else:
            count = 0
            r = self._cursor.set_range(self._partial)
            while r:
                if not r[0].startswith(self._partial):
                    break
                count += keycount()
                if count > position:
                    r = stepback()
                    count -= keycount()
                    if r is None:
                        r = start()
                    while r:
                        if not r[0].startswith(self._partial):
                            break
                        count += 1
                        if count > position:
                            break
                        r = step()
                    break
                r = step_nodup()
            if r is not None:
                return (r[0], decode_record_number(r[1]))

    def last(self):
        """Return last record taking partial key into account"""
        if self._partial is None:
            return self._get_record(self._cursor.last())
        elif self._partial == False:
            return None
        else:
            k = list(self._partial)
            while ord(k[-1]) == 255:
                k.pop()
            if not len(k):
                return self._get_record(self._cursor.last())
            k[-1] = chr(ord(k[-1]) + 1)
            self._cursor.set_range(''.join(k))
            return self.prev()

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        if self._partial is None:
            #return record
            if self._dbset._value_is_recno:
                try:
                    return (record[0], decode_record_number(record[1]))
                except:
                    return None
            else:
                return record
        elif self._partial == False:
            return None
        elif record[0].startswith(self._partial):
            #return record
            if self._dbset._value_is_recno:
                try:
                    return (record[0], decode_record_number(record[1]))
                except:
                    return None
            else:
                return record
        else:
            return None

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        return self._get_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account"""
        return self._get_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account"""
        return self._get_record(self._cursor.prev())

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update.

        Do nothing in Berkeley DB.  The cursor (for the datagrid) accesses
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
        if self._partial == False:
            return None
        key, value = record
        if self._partial is not None:
            if not key.startswith(self._partial):
                return None
        if self._dbset._dbtype == DB_RECNO:
            return self._get_record(self._cursor.set(key))
        elif self._dbset._value_is_recno:
            return self._get_record(
                self._cursor.set_both(key, encode_record_number(value)))
        else:
            return self._get_record(self._cursor.set_both(key, value))

