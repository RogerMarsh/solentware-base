# sqlite3api.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Object database using sqlite3.

List of classes

Sqlite3apiError - Exceptions
Sqlite3api - Define database and file and record level access methods
Sqlite3apiFile - File level access to each file in database (Open, Close)
Sqlite3apiRecord - Record level access to each file in database
CursorSqlite3 - Define cursor on file and access methods

"""

import os
import subprocess
import sqlite3

import sys
_platform_win32 = sys.platform == 'win32'
del sys

from api.database import (
    DatabaseError,
    Database,
    Cursor,
    decode_record_number,
    encode_record_number,
    )
from api.constants import (
    FLT, SPT, SQLITE_ADAPTER,
    FILEDESC, FILEORG, EO,
    PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, SQLITE3_FIELDATTS,
    FIELDS,
    PRIMARY, SECONDARY, INDEXPREFIX,
    )


class Sqlite3apiError(DatabaseError):
    pass


class Sqlite3api(Database):
    
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

    def __init__(self, Sqlite3Tables, Sqlite3DatabaseFolder, **kargs):
        """Define database structure.
        
        Sqlite3Tables = {
            name:{
                primary:name,
                fields:{
                    name:{property:value, ...},
                    ),
                }, ...
            }
        Sqlite3DatabaseFolder = folder containing database file

        """
        # The database definition from Sqlite3Tables after validation
        self._sqtables = None

        # The file from Sqlite3DatabaseFolder after validation
        self._sqfile = None

        # sqlite3 database connection object
        self._sqconn = None

        # sqlite3 database connection cursor object
        self._sqconn_cursor = None

        # Associate primary and secondary tables by name.
        # {secondary name:primary name, ...,
        #  primary name:[secondary name, ...], ...}
        # A secondary name may be a primary name if a loop is not made.
        self._associate = dict()
        
        try:
            sqfolder = os.path.abspath(Sqlite3DatabaseFolder)
        except:
            msg = ' '.join(['Database file name', str(Sqlite3DatabaseFolder),
                            'is not valid'])
            raise Sqlite3apiError, msg

        # Sqlite3Tables processing

        sqtables = dict()

        if not isinstance(Sqlite3Tables, dict):
            raise Sqlite3apiError, 'Table definitions must be a dictionary'

        for t in Sqlite3Tables:
            if not isinstance(Sqlite3Tables[t], dict):
                msg = ' '.join(
                    ['Table definition for', repr(t),
                     'must be a dictionary'])
                raise Sqlite3apiError, msg

            sqlite3desc = Sqlite3Tables[t]

            primary = sqlite3desc[PRIMARY]
            if primary not in sqlite3desc[FIELDS]:
                msg = ' '.join(['Primary column name', str(primary),
                                'for', name,
                                'does not have a column description'])
                raise Sqlite3apiError, msg
            if primary in sqtables:
                msg = ' '.join(['Primary table name', str(primary),
                                'for', t,
                                'already used'])
                raise Sqlite3apiError, msg
                    
            sqtables[primary] = self.make_root(
                primary,
                sqlite3desc,
                primary)
            self._associate[t] = {t:primary}

            if SECONDARY in sqlite3desc:
                for name, secondary in sqlite3desc[SECONDARY].iteritems():
                    if not isinstance(name, str):
                        msg = ' '.join(['Secondary table name', str(name),
                                        'for', t,
                                        'must be a string'])
                        raise Sqlite3apiError, msg

                    if secondary is None:
                        secondary = name[0].upper() + name[1:]
                    if secondary in sqtables:
                        msg = ' '.join(['Secondary table name', str(secondary),
                                        'for', t,
                                        'already used'])
                        raise Sqlite3apiError, msg
                    
                    if secondary == primary:
                        msg = ' '.join(['Secondary table name', str(s),
                                        'for', t,
                                        'cannot be same as primary'])
                        raise Sqlite3apiError, msg

                    if secondary not in sqlite3desc[FIELDS]:
                        msg = ' '.join(['Secondary table name',
                                        str(secondary),
                                        'for', t, 'does not have',
                                        'a column description'])
                        raise Sqlite3apiError, msg

                    sqtables[secondary] = self.make_root(
                        secondary,
                        sqlite3desc,
                        primary)
                    self._associate[t][name] = secondary

        self._sqtables = sqtables
        self._sqfile = os.path.join(sqfolder, os.path.split(sqfolder)[-1])

    def backout(self):
        """Backout tranaction."""
        if self._sqconn:
            self._sqconn.rollback()

    def close_context(self):
        """Close all sqlite3 cursors."""
        if self._sqconn is None:
            return
        for table in self._sqtables.itervalues():
            table.close()

    def close_database(self):
        """Close connection to database."""
        if self._sqconn is None:
            return
        self.close_context()
        self._sqconn_cursor.close()
        self._sqconn_cursor = None
        self._sqconn.close()
        self._sqconn = None

    def close_internal_cursors(self, dbsets=None):
        """Return True for compatibility with Berkeley DB subclass."""
        return True
            
    def commit(self):
        """Commit tranaction."""
        if self._sqconn:
            self._sqconn.commit()

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
            return (key, decode_record_number(srkey))
        else:
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
        main = self._sqtables

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
            raise Sqlite3apiError, msg

        args.append(pyscript)
        
        try:
            if os.path.exists(filepath):
                paths = (filepath,)
            else:
                msg = ' '.join([repr(filepath),
                                'is not an existing file'])
                raise Sqlite3apiError, msg
        except:
            paths = tuple(filepath)
            for fp in paths:
                if not os.path.isfile(fp):
                    msg = ' '.join([repr(fp),
                                    'is not an existing file'])
                    raise Sqlite3apiError, msg

        args.append(os.path.abspath(os.path.dirname(self._sqfile)))
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
        return os.path.exists(self._sqfile)

    def make_cursor(self, dbset, dbname, keyrange=None):
        """Create and return a cursor on DB dbname in dbset.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        return self._sqtables[self._associate[dbset][dbname]].make_cursor(
            self._sqtables[self._associate[dbset][dbname]],
            keyrange)

    def get_database_folder(self):
        """return database folder name"""
        return os.path.dirname(self._sqfile)
    
    def get_database(self, dbset, dbname):
        """Return DB for dbname in dbset."""
        return self._sqtables[self._associate[dbset][dbname]]._connectioncursor

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

    def make_internal_cursors(self, dbsets=None):
        """Return True for compatibility with Berkeley DB subclass."""
        return True

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
            os.mkdir(f)
        if self._sqconn is None:
            self._sqconn = sqlite3.connect(
                self._sqfile,
                detect_types=sqlite3.PARSE_DECLTYPES)
            self._sqconn_cursor = self._sqconn.cursor()
            # Remove the following statement to convert to unicode strings
            self._sqconn.text_factory = str
        for table in self._sqtables.itervalues():
            table.open_root(self._sqconn)
        return True

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
            return decode_record_number(pkey)

    def encode_primary_key(self, dbname, instance):
        """Convert instance.key for use as database value.

        For sqlite3 just return self.get_packed_key() converted to string.

        """
        # Should this be like Berkeley DB version of method?
        return encode_record_number(self.get_packed_key(dbname, instance))

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
        main = self._sqtables

        key = main[db[dbset]].put(putkey, instance.srvalue)
        if key != None:
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
        raise Sqlite3apiError, 'use_deferred_update_process not implemented'

    def make_root(self, name, table, primaryname):
        """"""
        return Sqlite3apiRecord(name, table, primaryname)

    def initial_database_size(self):
        """Do nothing and return True as method exists for DPT compatibility"""
        return True

    def increase_database_size(self, **ka):
        """Do nothing because method exists for DPT compatibility"""

            
class Sqlite3apiFile(object):
    
    """Define a sqlite3 table open_root and close methods.

    Methods added:

    close
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
        super(Sqlite3apiFile, self).__init__()

        self._db_engine_cursor = None
        self._name = name
        self._primaryname = primaryname
        self._indexname = ''.join((INDEXPREFIX, name))
        self._primary = name == primaryname
        self._fieldatts = dict()
        self._connectioncursor = None # a sqlite3 cursor on a table
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
        self._fieldatts = fieldatts.copy()
        description = sqlite3desc[FIELDS][name]
        if description == None:
            description = dict()
        if not isinstance(description, dict):
            msg = ' '.join(['Attributes for column', fieldname,
                            'in table', repr(self._name),
                            'must be a dictionary or "None"'])
            raise Sqlite3apiError, msg
        
        for attr in description:
            if attr not in fieldatts:
                msg = ' '.join(['Attribute', repr(attr),
                                'for column', fieldname,
                                'in table', self._name,
                                'is not allowed'])
                raise Sqlite3apiError, msg
            
            if type(description[attr]) != type(fieldatts[attr]):
                msg = ' '.join([attr, 'for column', fieldname,
                                'in table', self._name, 'is wrong type'])
                raise Sqlite3apiError, msg
            
            if attr == SPT:
                if (description[attr] < 0 or
                    description[attr] > 100):
                    msg = ' '.join(['Split percentage for field',
                                    fieldname, 'in file', self._name,
                                    'is invalid'])
                    raise Sqlite3apiError, msg

            if attr in SQLITE3_FIELDATTS:
                self._fieldatts[attr] = description[attr]

        if self._fieldatts.get(SQLITE_ADAPTER):
            self._class, adapter, converter = self._fieldatts[SQLITE_ADAPTER]
            sqlite3.register_adapter(self._class, adapter)
            sqlite3.register_converter(self._name, converter)

    def close(self):
        """Close DB and cursor."""
        # The _connectioncursor fills the role of DB
        self.close_root_cursor()
        if self._connectioncursor is not None:
            self._connectioncursor.close()
            self._connectioncursor = None

    def close_root_cursor(self):
        """Close cursor associated with DB."""
        if self._db_engine_cursor is not None:
            self._db_engine_cursor.close()
            self._db_engine_cursor = None

    def make_root_cursor(self):
        """
        Create cursor on DB.
        
        This is the cursor used by delete put and replace so a
        reference is kept in self._db_engine_cursor. It is better to use
        DBapi.make_cursor to get a cursor for other purposes.
        
        """
        if self._db_engine_cursor is None:
            self._db_engine_cursor = self._connectioncursor.connection.cursor()

    def open_root(self, sqconn):
        """Open sqlite3 database cursor and create table unless it exists."""
        self._connectioncursor = sqconn.cursor()
        if self._primary:
            if self._autoincrementprimary:
                self._connectioncursor.execute(
                    ''.join((
                        'create table if not exists ', self._name,
                        ' (',
                        self._name, ' integer primary key autoincrement, ',
                        'value', ' text',
                        ')')),
                    )
            else:
                self._connectioncursor.execute(
                    ''.join((
                        'create table if not exists ', self._name,
                        ' (',
                        self._name, ' integer primary key, ',
                        'value', ' text',
                        ')')),
                    )
            '''self._connectioncursor.execute(
                ''.join((
                    'create index if not exists ', self._indexname,
                    ' on ', self._name, ' (', self._name, ')')),
                )'''
        elif self._class is not None:
            self._connectioncursor.execute(
                ''.join((
                    'create table if not exists ', self._name,
                    ' (',
                    self._name, ' ', self._name, ', ',
                    self._primaryname, ' integer',
                    ')')),
                )
            self._connectioncursor.execute(
                ''.join((
                    'create index if not exists ', self._indexname,
                    ' on ', self._name, ' (', self._name, ')')),
                )
        else:
            self._connectioncursor.execute(
                ''.join((
                    'create table if not exists ', self._name,
                    ' (',
                    self._name, ' text, ',
                    self._primaryname, ' integer',
                    ')')),
                )
            self._connectioncursor.execute(
                ''.join((
                    'create index if not exists ', self._indexname,
                    ' on ', self._name, ' (', self._name, ')')),
                )
        sqconn.commit()
        self.make_root_cursor()


class Sqlite3apiRecord(Sqlite3apiFile):
    
    """Define a DB file with record access and deferred update methods.

    Methods added:

    delete
    get_first_primary_key_for_index_key
    get_primary_record
    make_cursor
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
        super(Sqlite3apiRecord, self).__init__(name, sqlite3desc, primaryname)
        self._clientcursors = dict()
    
    def close(self):
        """Close DB and any cursors."""
        for c in self._clientcursors.keys():
            c.close()
        self._clientcursors.clear()
        super(Sqlite3apiRecord, self).close()

    def delete(self, key, value):
        """Delete (key, value) from database."""
        try:
            if self._primary:
                self._db_engine_cursor.execute(
                    ''.join((
                        'delete from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' == ?')),
                    (key,))
            else:
                if self._class:
                    key = self._class(key)
                self._db_engine_cursor.execute(
                    ''.join((
                        'delete from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' == ? and ',
                        self._dbset._primaryname, ' == ?')),
                    (key, value))
        except:
            pass

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        if self._primary:
            raise Sqlite3apiError, (
                'get_first_primary_key_for_index_key for primary table')
        self._connectioncursor.execute(
            ''.join((
                'select ', self._primaryname, ' from ',
                self._name,
                ' where ',
                self._name, ' == ? ',
                'order by ', self._primaryname,
                ' limit 1')),
            (key,))
        try:
            return self.fetch_one_record(self._connectioncursor)[0]
        except TypeError:
            return None
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        if key is None:
            return None
        self._connectioncursor.execute(
            ''.join((
                'select * from ',
                self._name,
                ' where ',
                self._name, ' == ? ',
                'order by ', self._name,
                ' limit 1')),
            (key,))
        return self.fetch_one_record(self._connectioncursor)

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSqlite3(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new rows."""
        if self._primary:
            if not key: #key == 0:  # Change test to "key is None" when sure
                v = (value,)
                self._db_engine_cursor.execute(
                    ''.join((
                        'insert into ', self._name,
                        ' ( value )',
                        ' values ( ? )',
                        )),
                    v
                    )
                return self._db_engine_cursor.lastrowid
            else:
                self._db_engine_cursor.put(key, value)
                return None
        else:
            try:
                if self._class:
                    key = self._class(key)
                v = (key, value)
                self._db_engine_cursor.execute(
                    ''.join((
                        'insert into ', self._name,
                        ' ( ', self._name, ', ',self._primaryname, ' )',
                        ' values ( ? , ? )',
                        )),
                    v
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
            if self._primary:
                self._db_engine_cursor.execute(
                    ''.join((
                        'update ',
                        self._dbset._name,
                        ' set ',
                        'value = ? ',
                        'where ',
                        self._dbset._name, ' == ? ',
                        'value = ? ',
                        'limit = 1')), # should not need limit?
                    (newvalue, key, oldvalue))
            else:
                if self._class:
                    key = self._class(key)
                self._db_engine_cursor.execute(
                    ''.join((
                        'update ',
                        self._dbset._name,
                        ' set ',
                        self._dbset._primaryname, ' = ? ',
                        'where ',
                        self._dbset._name, ' == ? ',
                        self._dbset._primaryname, ' = ? ',
                        'limit = 1')), # should not need limit?
                    (newvalue, key, oldvalue))
        except:
            pass

    def fetch_one_record(self, cursor):
        """Return one record from the execute already evaluated by cursor.

        Originally convert the buffer returned for a BLOB to a str.
        Now placeholder for python-sqlite3 adaptors and converters

        """
        return cursor.fetchone()


class CursorSqlite3(Cursor):
    
    """Define cursor implemented using the sqlite3 cursor methods.
    
    Methods added:

    None
    
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
        """Define a cursor using the sqlite3 engine."""
        self._cursor = None
        self._dbset = dbset
        self._partial = None
        self._most_recent_row_read = None
        
        if dbset._connectioncursor is not None:
            self._cursor = dbset._connectioncursor.connection.cursor()

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
        self._most_recent_row_read = None

    def count_records(self):
        """return record count or None if cursor is not usable"""
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name))
                )
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name))
                )
        else:
            s = (''.join((self._partial, '*')),)
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ', self._dbset._name,
                    ' glob ?')),
                s)
        return self._cursor.fetchone()[0]

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise"""
        return bool(self._cursor)

    def first(self):
        """Return first record taking partial key into account"""
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' order by ',
                    self._dbset._name,
                    ' limit 1'))
                )
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1'))
                )
        elif self._partial is False:
            return None
        else:
            s = (''.join((self._partial, '*')),)
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                s)
        self._most_recent_row_read = self._dbset.fetch_one_record(self._cursor)
        return self._most_recent_row_read

    def get_position_of_record(self, key=None):
        """return position of record in file or 0 (zero)"""
        if key is None:
            return 0
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' <= ?')),
                (key[0],))
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' < ?')),
                (key[0],))
        elif self._partial is False:
            return 0
        else:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' < ?')),
                (''.join((self._partial, '*')), key[0]))
        position = self._cursor.fetchone()[0]
        if self._dbset._primary:
            return position
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' <= ?')),
                key)
        else:
            s = [''.join((self._partial, '*'))]
            s.extend(key)
            self._cursor.execute(
                ''.join((
                    'select count(*) from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' <= ?')),
                s)
        return position + self._cursor.fetchone()[0]

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        if position is None:
            return None
        if position < 0:
            if self._dbset._primary:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' order by ',
                        self._dbset._name, ' desc ',
                        'limit 1 ',
                        'offset ?')),
                    (str(-1 - position),))
            elif self._partial is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' order by ',
                        self._dbset._name, ' desc , ',
                        self._dbset._primaryname, ' desc ',
                        'limit 1 ',
                        'offset ?')),
                    (str(-1 - position),))
            elif self._partial is False:
                return None
            else:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' glob ? ',
                        'order by ',
                        self._dbset._name, ' desc , ',
                        self._dbset._primaryname, ' desc ',
                        'limit 1 ',
                        'offset ?')),
                    (''.join((self._partial, '*')), str(-1 - position)))
        else:
            if self._dbset._primary:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' order by ',
                        self._dbset._name,
                        ' limit 1 ',
                        'offset ?')),
                    (str(position - 1),))
            elif self._partial is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' order by ',
                        self._dbset._name, ' , ',
                        self._dbset._primaryname,
                        ' limit 1 ',
                        'offset ?')),
                    (str(position - 1),))
            elif self._partial is False:
                return None
            else:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' glob ? ',
                        'order by ',
                        self._dbset._name, ' , ',
                        self._dbset._primaryname,
                        ' limit 1 ',
                        'offset ?')),
                    (''.join((self._partial, '*')), str(position - 1)))
        self._most_recent_row_read = self._dbset.fetch_one_record(self._cursor)
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account"""
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' order by ',
                    self._dbset._name, ' desc ',
                    'limit 1'))
                )
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' order by ',
                    self._dbset._name, ' desc , ',
                    self._dbset._primaryname, ' desc ',
                    'limit 1'))
                )
        elif self._partial is False:
            return None
        else:
            s = (''.join((self._partial, '*')),)
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? ',
                    'order by ',
                    self._dbset._name, ' desc , ',
                    self._dbset._primaryname, ' desc ',
                    'limit 1')),
                s)
        self._most_recent_row_read = self._dbset.fetch_one_record(self._cursor)
        return self._most_recent_row_read

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial
        self._most_recent_row_read = None

    def nearest(self, key):
        """Return nearest record to key taking partial key into account"""
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' >= ? ',
                    'order by ',
                    self._dbset._name,
                    ' limit 1')),
                (key,))
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' >= ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                (key,))
        elif self._partial is False:
            return None
        else:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' >= ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                (''.join((self._partial, '*')), key))
        self._most_recent_row_read = self._dbset.fetch_one_record(self._cursor)
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account"""
        if self._most_recent_row_read is None:
            return self.first()
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' > ? ',
                    'order by ',
                    self._dbset._name,
                    ' limit 1')),
                (self._most_recent_row_read[0],))
            row = self._dbset.fetch_one_record(self._cursor)
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' > ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                self._most_recent_row_read)
            row = self._dbset.fetch_one_record(self._cursor)
            if row is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' > ? ',
                        'order by ',
                        self._dbset._name, ' , ',
                        self._dbset._primaryname,
                        ' limit 1')),
                    (self._most_recent_row_read[0],))
                row = self._dbset.fetch_one_record(self._cursor)
        elif self._partial is False:
            return None
        else:
            s = [''.join((self._partial, '*'))]
            s.extend(self._most_recent_row_read)
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' > ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                s)
            row = self._dbset.fetch_one_record(self._cursor)
            if row is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' glob ? and ',
                        self._dbset._name, ' > ? ',
                        'order by ',
                        self._dbset._name, ' , ',
                        self._dbset._primaryname,
                        ' limit 1')),
                    s[:-1])
                row = self._dbset.fetch_one_record(self._cursor)
        self._most_recent_row_read = row
        return self._most_recent_row_read

    def prev(self):
        """Return previous record taking partial key into account"""
        if self._most_recent_row_read is None:
            return self.last()
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' < ? ',
                    'order by ',
                    self._dbset._name, ' desc ',
                    'limit 1')),
                (self._most_recent_row_read[0],))
            row = self._dbset.fetch_one_record(self._cursor)
        elif self._partial is None:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' < ? ',
                    'order by ',
                    self._dbset._name, ' desc , ',
                    self._dbset._primaryname, ' desc ',
                    'limit 1')),
                self._most_recent_row_read)
            row = self._dbset.fetch_one_record(self._cursor)
            if row is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' < ? ',
                        'order by ',
                        self._dbset._name, ' desc , ',
                        self._dbset._primaryname, ' desc ',
                        'limit 1')),
                    (self._most_recent_row_read[0],))
                row = self._dbset.fetch_one_record(self._cursor)
        elif self._partial is False:
            return None
        else:
            s = [''.join((self._partial, '*'))]
            s.extend(self._most_recent_row_read)
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' < ? ',
                    'order by ',
                    self._dbset._name, ' desc , ',
                    self._dbset._primaryname, ' desc ',
                    'limit 1')),
                s)
            row = self._dbset.fetch_one_record(self._cursor)
            if row is None:
                self._cursor.execute(
                    ''.join((
                        'select * from ',
                        self._dbset._name,
                        ' where ',
                        self._dbset._name, ' glob ? and ',
                        self._dbset._name, ' < ? ',
                        'order by ',
                        self._dbset._name, ' desc , ',
                        self._dbset._primaryname, ' desc ',
                        'limit 1')),
                    s[:-1])
                row = self._dbset.fetch_one_record(self._cursor)
        self._most_recent_row_read = row
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
        if self._partial is False:
            return None
        if self._partial is not None:
            if not record[0].startswith(self._partial):
                return None
        if self._dbset._primary:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' == ? ',
                    'order by ',
                    self._dbset._name,
                    ' limit 1')),
                (record[0],))
        elif self._partial is not None:
            s = [''.join((self._partial, '*'))]
            s.extend(record)
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' glob ? and ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' == ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                s)
        else:
            self._cursor.execute(
                ''.join((
                    'select * from ',
                    self._dbset._name,
                    ' where ',
                    self._dbset._name, ' == ? and ',
                    self._dbset._primaryname, ' == ? ',
                    'order by ',
                    self._dbset._name, ' , ',
                    self._dbset._primaryname,
                    ' limit 1')),
                record)
        row = self._dbset.fetch_one_record(self._cursor)
        if row:
            self._most_recent_row_read = row
        return row
