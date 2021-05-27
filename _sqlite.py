# _sqlite.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API, implemented using an interface to SQLite3, where indicies are
represented as lists or bitmaps of record numbers.

The database is accessed in the style of Berkeley DB primary and secondary
databases.

The classes in this module emulate the classes in dbapi.  SQL statements
emulate the bsddb3 database and cursor methods, and (key, value) tuples are
returned to the application rather than lists of rows.

Primary and secondary databases are implemented as tables in a SQLite3 database.

The rowid of a row in a table corresponding to a primary database is used as
it's key, but the only function of the tables corresponding to secondary
databases is to exist so they can be indexed.

The representation of multiple rows of a table by one row, a list or bitmap of
record numbers, makes it at least difficult to use SQL statements directly to
evaluate queries.  The reason for using an SQLite3 database is sqlite3 being
the only cross-platform database engine included in the Python 3 distribution.

"""

import os
import subprocess
from ast import literal_eval
import bisect
import re

import sys
_platform_win32 = sys.platform == 'win32'
_python_version = '.'.join(
    (str(sys.version_info[0]),
     str(sys.version_info[1])))
del sys

from .api.bytebit import Bitarray
from .api import database
from .api import cursor
from .api.recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordsetCursor,
    )
from .api.constants import (
    FLT, SPT,
    FILEDESC, FILEORG, EO,
    PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, SQLITE3_FIELDATTS,
    FIELDS,
    PRIMARY, SECONDARY, INDEXPREFIX, SEGMENTPREFIX,
    SQLITE_VALUE_COLUMN,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_RECORDS_COLUMN,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    SUBFILE_DELIMITER,
    )
from .api.segmentsize import SegmentSize
from .api import definition
from .api import segment
from .api import controlfile
from .api import file
from .api import primaryfile
from .api import secondaryfile
from .api import primary
from .api import secondary


class Sqlite3apiError(database.DatabaseError):
    pass


class _DatabaseEncoders:
    
    """Define default record key encoder and decoder.
    """

    def encode_record_number(self, key):
        """Return repr(key) because this is sqlite3 version of method.

        Typically used to convert primary key to secondary index format,
        using Berkeley DB terminology.
        
        """
        return repr(key)

    def decode_record_number(self, skey):
        """Return literal_eval(skey) because this is sqlite3 version of method.

        Typically used to convert DB primary key held on secondary index,
        using Berkeley DB terminology, to integer.

        """
        return literal_eval(skey)

    def encode_record_selector(self, key):
        """Return key because this is sqlite3 version of method.

        Typically used to convert a secondary index key value to database
        engine format, using Berkeley DB terminology.
        
        """
        return key

    def encode_record_key(self, key):
        """Return key because this is sqlite3 version of method.
        
        Specifically when comparing the key of a record read from database with
        the key used to find it.
        
        """
        return key


class Database(database.Database, _DatabaseEncoders):
    
    """Emulate dbapi.Database access to database with interface to SQLite.
    
    SQLite tables are used to emulate the primary and secondary databases in
    an equivalent dbapi.Database setup.

    Primary and secondary terminology comes from Berkeley DB documentation.

    The key part of a secondary key:value is a (key, segment) tuple and the
    value part is a (reference, count) tuple where segment follows DPT
    terminology.  Reference can be a record number relative to segment start,
    a reference to a list of record numbers, or a reference to a bitmap
    representing such record numbers.  Count is the number of records
    referenced by this value.

    Secondary databases are supported by an 'integer primary key' table, for
    lists of record numbers or bitmap representations of record numbers.  The
    reference is the key into the relevant table.
    
    """

    def __init__(
        self,
        primary_class,
        secondary_class,
        database_specification,
        databasefolder,
        *args,
        **kwargs):
        """Define database using interface to SQLite.

        primary_class - a subclass of Primary.
        secondary_class - a subclass of Secondary.
        database_specification - a FileSpec instance.
        databasefolder - folder containing the database.
        *args - absorb positional arguments for other database engines.
        **kwargs - arguments for self.make_root() call.
        """
        super().__init__(database_specification, databasefolder, **kwargs)

        # database_specification processing
        definitions = dict()
        for name, specification in self.database_specification.items():
            definitions[name] = self.make_root(
                dbset=name,
                specification=specification,
                field_name_converter=self.database_specification.field_name,
                primary_class=primary_class,
                secondary_class=secondary_class,
                **kwargs)

        # The database definition from database_specification after validation
        self._dbdef = definitions
        
        # The segment control object
        self._control = ControlFile()
        for definition in self.database_definition.values():
            definition.primary.set_control_database(self._control)

    def make_root(self, **kw):
        """Return Definition instance created from **kw arguments.
        """
        return Definition(**kw)

    def close_context(self):
        """Close all tables."""
        if self._dbservices is None:
            return
        for table in self.database_definition.values():
            table.primary.close()
            for s in table.secondary.values():
                s.close()

    def close_database(self):
        """Close all tables and connection to database."""
        if self._dbservices is None:
            return
        self.close_context()
        self._dbservices.close()
        self._dbservices = None

    def start_transaction(self):
        """Start a transaction."""
        if self._dbservices:
            cursor = self._dbservices.cursor()
            try:
                cursor.execute('begin')
            finally:
                cursor.close()

    def backout(self):
        """Backout tranaction."""
        if self._dbservices:
            cursor = self._dbservices.cursor()
            try:
                cursor.execute('rollback')
            finally:
                cursor.close()
            
    def commit(self):
        """Commit tranaction."""
        if self._dbservices:
            cursor = self._dbservices.cursor()
            try:
                cursor.execute('commit')
            finally:
                cursor.close()

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

    def files_exist(self):
        """Return True if all defined files exist in self._home_directory.

        Sqlite3 databases are held in the file self._home, which is in
        self._home_directory.

        """
        return os.path.exists(self._home)

    def get_database_filenames(self):
        """Return list of database file names."""
        if self._home:
            return [os.path.basename(self._home)]
        else:
            return [] 
    
    def get_database(self, dbset, dbname):
        """Return database connection for dbname in dbset."""
        return self.database_definition[dbset].primary.table_dbservices

    def is_recno(self, dbset, dbname):
        """Return True if dbname is primary table in dbset."""
        return self.database_definition[dbset].associate(dbname).is_primary()

    def open_context(self):
        """Open all tables on database."""
        f, b = os.path.split(self._home)
        if not os.path.exists(f):
            os.makedirs(f, mode=0o700)
        self.make_connection()
        for table in self.database_definition.values():
            table.primary.open_root(self.dbservices)
            for s in table.secondary.values():
                s.open_root(self.dbservices)
        return True

    def allocate_and_open_contexts(self, closed_contexts):
        """Open closed_contexts which had been closed.

        This method is intended for use only when re-opening a table after
        closing it temporarily so another thread can make and commit changes
        to a table.

        The method name comes from the dptbase module where it describes
        extactly what is done.  For sqlite3api it is a callback hook where the
        name is already chosen.

        """
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

    def encode_primary_key(self, dbset, instance):
        """Convert instance.key for use as database value.

        For sqlite3 just return self.get_packed_key() converted to string.

        """
        # Should this be like Berkeley DB version of method?
        return self.encode_record_number(self.get_packed_key(dbset, instance))

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs={},
        use_specification_items=None,
        ):
        """Open new connection to database, run method, then close connection.

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

    def cede_contexts_to_process(self, close_contexts):
        """Close all contexts so another process, or thread, can commit.

        close_contexts is ignored by this module's version of the method.

        """
        # Closing just the tables in close_contexts seems to be insufficient
        # and the complete shutdown in close_database() seems unnecessary.
        self.close_context()

    def create_recordset_cursor(self, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursor(recordset)


class Sqlite3api:
    
    """Access database with an interface to SQLite.

    Usage is "class C(_sqlite.Sqlite3api, D)" where _sqlite.Database is a
    superclass of D.

    Method do_deferred_updates is in this class, rather than _sqlite.Database,
    to hide it from the classes which process the deferred updates because
    these are subclasses of _sqlite.Database but not _sqlite.Sqlite3api.

    """

    def close_context(self):
        """Close control table and delegate to superclass close_context method.
        """
        self._control.close()
        super().close_context()

    def open_context(self):
        """Delegate to superclass open_context method and open control table."""
        super().open_context()
        self._control.open_root(self.dbservices)
        return True

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

        args.append(os.path.abspath(os.path.dirname(self._home)))
        args.extend(filepath)

        return subprocess.Popen(args)

            
class File(file.File):
    
    """Emulate dbapi.File using a SQLite table.

    This class defines elements common to primary and secondary databases.

    Interpret primary in the Berleley DB sense of primary and secondary
    databases.
    """

    def __init__(self, name, sqlite3desc, primaryname, **kargs):
        """Define a SQLite table.

        name - primary or secondary database name.
        sqlite3desc - database description for primary and associated
                      secondaries.
        primaryname - primary database name.

        This class defines elements common to primary and secondary databases.

        """
        super().__init__(name,
                         sqlite3desc[FIELDS][name],
                         primaryname,
                         SQLITE3_FIELDATTS,
                         repr(name),
                         repr(primaryname),
                         **kargs)
        self._table_dbservices = None

        # When using either pickle.dumps() and pickle.loads(). or repr() and
        # ast.literal_eval(), the stored bytestring is returned for consistency
        # with the other database engines.
        #sqlite3.register_converter('TEXT', lambda bs: bs)

    def close(self):
        """Forget connection, not close because it is not 'owned'."""
        # The _table_link fills the role of DB
        self._table_dbservices = None
        self._table_link = None

    def open_root(self, connection):
        """Note database connection and table name."""
        self._table_dbservices = connection
        if self.is_primary():
            self._table_link = [self.dataname]
        else:
            self._table_link = [
                SUBFILE_DELIMITER.join((self._keyvalueset_name, self.dataname))]

    @property
    def dataname(self):
        """Return name of column holding key.

        This is usually the same as table name if case is ignored.

        In primary tables the value associated with the key is the data
        provided when the row was created or last amended.

        In secondary tables the value associated with the key is a set of
        rowids in the primary table from which the key in the secondary table
        is derived.  The value is a rowid, a reference to a list of rowids, or
        a reference to a bitmap of rowids, depending on the number of matching
        rows in the primary table.

        """
        return self._dataname

    # _keyvalueset_name is bound to the name of the primary table.
    # A constant column name, 'RowidsInPrimary' for example, would do but the
    # primary table name may not be obvious from the index table name.
    # The variable column name forced the choice of an attribute name roughly
    # neutral across database engines: _keyvalueset_name seemed good.
    @property
    def rowids_in_primary(self):
        """Return name of column in index table referencing primary rowids.

        If the record count for the row is 1, rowids_in_primary is the rowid in
        the primary table.

        If the record count for the row is greater than 1, rowids_in_primary
        refers to a row in the associated segment table which contains either
        a list of rowids in the primary table or a bitmap representing the
        rowids in the primary table.

        Upper and lower record count limits exists which control when rowids
        are switched from list to bitmap representation and back respectively.

        """
        return self._keyvalueset_name

    @property
    def table_dbservices(self):
        """Return sqlite3 database connection."""
        return self._table_dbservices

            
class PrimaryFile(File, primaryfile.PrimaryFile):
    
    """Add segment support to SQLite3File for primary database.

    Interpret primary in the Berleley DB sense of primary and secondary
    databases.

    This class provides the methods to manage the lists and bitmaps of record
    numbers for segments of the table supporting the primary database.
    """

    def __init__(self, name, sqlite3desc, primaryname, **kargs):
        """Add segment support to SQLite3File.

        All arguments are delegated to superclass.
        Pick out sqlite3desc[FILEDESC][FILEORG] value which is relevant to
        this class and it's subclasses, but not it's superclasses.
        """
        super().__init__(name,
                         sqlite3desc,
                         primaryname,
                         filecontrolprimary_class=FileControlPrimary,
                         existencebitmap_class=ExistenceBitMap,
                         file_reference=name,
                         **kargs)
        self._segmentname = ''.join((SEGMENTPREFIX, self.dataname))

        # Interpret EO (from DPT) as 'integer primary key autoincrement'
        if sqlite3desc[FILEDESC][FILEORG] == EO:
            self._autoincrementprimary = True
        else:
            self._autoincrementprimary = False

    def open_root(self, *args):
        """Open sqlite3 database cursor and create table unless it exists."""
        super().open_root(*args)
        cursor = self.table_dbservices.cursor()
        try:
            if self._autoincrementprimary:
                statement = ' '.join((
                    'create table if not exists', self.table_link,
                    '(',
                    self.dataname, 'integer primary key autoincrement ,',
                    SQLITE_VALUE_COLUMN,
                    ')',
                    ))
                cursor.execute(statement)
            else:
                statement = ' '.join((
                    'create table if not exists', self.table_link,
                    '(',
                    self.dataname, 'integer primary key ,',
                    SQLITE_VALUE_COLUMN,
                    ')',
                    ))
                cursor.execute(statement)
            statement = ' '.join((
                'create table if not exists', self._segmentname,
                '(',
                SQLITE_RECORDS_COLUMN,
                ')',
                ))
            cursor.execute(statement)
        finally:
            cursor.close()
        self._existence_bits.open_root(self.table_dbservices)
        # Commit must be at higher level: in open_context() at least.
        #connection.commit()

    def close(self):
        """Delegate to superclass close() method."""
        super().close()

    def get_existence_bits_database(self):
        """Return the database containing existence bit map."""
        # Use instance.get_existence_bits().put(...) and so forth in sqlite3
        return self._existence_bits._segment_dbservices

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
        cursor = self.table_dbservices.cursor()
        try:
            return cursor.execute(statement, values).fetchone()[0]
        finally:
            cursor.close()

    def set_segment_records(self, values):
        """Update self._segmentname row using values."""
        statement = ' '.join((
            'update',
            self._segmentname,
            'set',
            SQLITE_RECORDS_COLUMN, '= ?',
            'where rowid == ?',
            ))
        cursor = self.table_dbservices.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def delete_segment_records(self, values):
        """Delete self._segmentname row using values."""
        statement = ' '.join((
            'delete from',
            self._segmentname,
            'where rowid == ?',
            ))
        cursor = self.table_dbservices.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def insert_segment_records(self, values):
        """Insert self._segmentname row using values."""
        statement = ' '.join((
            'insert into',
            self._segmentname,
            '(',
            SQLITE_RECORDS_COLUMN,
            ')',
            'values ( ? )',
            ))
        #self.table_dbservices.execute(statement, values)
        #return self.table_dbservices.lastrowid
        #return self.table_dbservices.execute(statement, values).lastrowid
        cursor = self.table_dbservices.cursor()
        try:
            cursor.execute(statement, values)
            return cursor.execute(
                ' '.join((
                    'select last_insert_rowid() from',
                    self._segmentname))).fetchone()[0]
        finally:
            cursor.close()

            
class SecondaryFile(File, secondaryfile.SecondaryFile):
    
    """Add segment support to SQLite3File for secondary database.

    Interpret secondary in the Berleley DB sense of primary and secondary
    databases.

    This class uses the methods of the PrimaryFile class to manage the
    lists and bitmaps of record numbers for segments of the table supporting
    the secondary database.  The link is set after both tables have been
    opened.
    """

    def __init__(self, *args):
        """Add segment support to SQLite3File for secondary database."""
        super().__init__(*args)
        self._indexname = [
            ''.join((INDEXPREFIX,
                     SUBFILE_DELIMITER.join((self._keyvalueset_name,
                                             self.dataname))))]

    @property
    def indexname(self):
        """Return index name."""
        return self._indexname[0]

    @property
    def indexname_list(self):
        """Return index name list."""
        return self._indexname

    def open_root(self, *args):
        """Delegate to superclass open_root() method then create table and
        index if they do not exist.
        """
        super().open_root(*args)
        cursor = self.table_dbservices.cursor()
        try:
            statement = ' '.join((
                'create table if not exists', self.table_link,
                '(',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                ')',
                ))
            cursor.execute(statement)
            statement = ' '.join((
                'create unique index if not exists', self.indexname,
                'on', self.table_link,
                '(',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN,
                ')',
                ))
            cursor.execute(statement)
            # Commit must be at higher level: in open_context() at least.
            #self.table_dbservices.commit()
        finally:
            cursor.close()


class Primary(PrimaryFile, primary.Primary, _DatabaseEncoders):
    
    """Add update, cursor, and recordset processing, to SQLite3PrimaryFile.

    This class provides methods to add, delete, and modify, records on the
    primary database.

    This class provides methods to process recordsets created from records in
    the primary database.  A dictionary of created recordsets is maintained
    and any that exist when the primary database is closed are destroyed too.

    The make_cursor method creates a CursorPrimary instance, a Berkeley
    DB style cursor, which is used to traverse the primary database.  A
    dictionary of these cursors is maintained and any that exist when the
    primary database is closed are destroyed too.
    """

    def delete(self, key, value):
        """Delete (key, value) from database.

        Actually update to null because the rowid must be preserved to fit
        the segment structure when record numbers are re-used.

        """
        cursor = self.table_dbservices.cursor()
        try:
            statement = ' '.join((
                'update',
                self.table_link,
                'set',
                SQLITE_VALUE_COLUMN, '= null',
                'where',
                self.rowids_in_primary, '== ?',
                ))
            values = (key,)
            cursor.execute(statement, values)

            # See comment in Sqlite3api.delete_instance()
            #self.get_control_primary().note_freed_record_number(key)
            
        except:
            pass
        finally:
            cursor.close()
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        if key is None:
            return None
        statement = ' '.join((
            'select * from',
            self.table_link,
            'where',
            self.dataname, '== ?',
            'order by',
            self.dataname,
            'limit 1',
            ))
        values = (key,)
        cursor = self.table_dbservices.cursor()
        try:
            return cursor.execute(statement, values).fetchone()
        finally:
            cursor.close()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorPrimary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new rows."""
        cursor = self.table_dbservices.cursor()
        try:
            if not key: #key == 0:  # Change test to "key is None" when sure
                statement = ' '.join((
                    'insert into',
                    self.table_link,
                    '(', SQLITE_VALUE_COLUMN, ')',
                    'values ( ? )',
                    ))
                values = (value,)
                #return self.table_dbservices.execute(statement,
                #                                     values).lastrowid
                cursor.execute(statement, values)
                return cursor.execute(
                    ' '.join((
                        'select last_insert_rowid() from',
                        self.dataname))).fetchone()[0]
            else:
                statement = ' '.join((
                    'update',
                    self.table_link,
                    'set',
                    SQLITE_VALUE_COLUMN, '= ?',
                    'where',
                    self.rowids_in_primary, '== ?',
                    ))
                values = (value, key)
                cursor.execute(statement, values)
                return None
        finally:
            cursor.close()

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on table.
        
        (key, newvalue) is put on table only if (key, oldvalue) is on table.
        
        """
        cursor = self.table_dbservices.cursor()
        try:
            statement = ' '.join((
                'update',
                self.table_link,
                'set',
                SQLITE_VALUE_COLUMN, '= ?',
                'where',
                self.rowids_in_primary, '== ?',
                ))
            values = (newvalue, key)
            cursor.execute(statement, values)
        except:
            pass
        finally:
            cursor.close()

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        if key is None:
            return
        statement = ' '.join((
            'select',
            self.dataname,
            'from',
            self.table_link,
            'where',
            self.dataname, '== ?',
            'order by',
            self.dataname,
            'limit 1',
            ))
        values = (key,)
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement, values):
                s, rn = divmod(key, SegmentSize.db_segment_size)
                recordset[s] = RecordsetSegmentList(
                    s, None, records=rn.to_bytes(2, byteorder='big'))
        finally:
            cursor.close()

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None and keyend is None:
            self.populate_recordset_all(recordset)
            return
        
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(keystart,
                                                 SegmentSize.db_segment_size)
        if keyend is not None:
            segment_end, recnum_end = divmod(keyend,
                                                    SegmentSize.db_segment_size)

        if keyend is None:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._segment_link,
                'where',
                self.get_existence_bits()._segment_link, '>= ?',
                ))
            values = (segment_start,)
        elif keystart is None:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._segment_link,
                'where',
                self.get_existence_bits()._segment_link, '<= ?',
                ))
            values = (segment_end,)
        else:
            statement = ' '.join((
                'select',
                SQLITE_VALUE_COLUMN,
                'from',
                self.get_existence_bits()._segment_link,
                'where',
                self.get_existence_bits()._segment_link, '>= ? and',
                self.get_existence_bits()._segment_link, '<= ?',
                ))
            values = (segment_start, segment_end)
        cursor = self.get_existence_bits_database().cursor()
        try:
            for r in cursor.execute(statement, values):
                recordset[r[0] - 1] = RecordsetSegmentBitarray(
                    r[0] - 1, None, records=r[1])
        finally:
            cursor.close()
        try:
            recordset[segment_start][:recnum_start] = False
        except KeyError:
            pass
        if keyend is not None:
            try:
                recordset[segment_end][recnum_end + 1:] = False
            except KeyError:
                pass
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        statement = ' '.join((
            'select',
            self.get_existence_bits()._segment_link, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self.get_existence_bits()._segment_link,
            ))
        values = ()
        cursor = self.get_existence_bits_database().cursor()
        try:
            for r in cursor.execute(statement, values):
                recordset[r[0] - 1] = RecordsetSegmentBitarray(
                    r[0] - 1, None, records=r[1])
        finally:
            cursor.close()

    def segment_delete(self, segment, record_number):
        """Remove record_number from existence bit map for segment."""
        # See dbduapi.py DBduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately (and delete!!).
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits().get(segment + 1)
        if ebmb is None:
            # It does not exist so raise an exception
            raise Sqlite3apiError(
                'Existence bit map for segment does not exist')
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            # Set bit for record number and write segment back to database
            ebm[record_number] = False
            self.get_existence_bits().put(segment + 1, ebm.tobytes())

    def segment_put(self, segment, record_number):
        """Add record_number to existence bit map for segment."""
        # See dbduapi.py DBduPrimary.defer_put for model.  Main difference
        # is the write back to database is done immediately.
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits().get(segment + 1)
        if ebmb is None:
            # It does not exist so create a new empty one
            ebm = SegmentSize.empty_bitarray.copy()
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

    def get_high_record(self):
        """Return record with highest record number."""
        statement = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self.table_link,
            'order by',
            self.dataname, 'desc',
            'limit 1',
            ))
        values = ()
        cursor = self.table_dbservices.cursor()
        try:
            return cursor.execute(statement, values).fetchone()
        finally:
            cursor.close()


class Secondary(SecondaryFile, secondary.Secondary):
    
    """Add update, cursor, and recordset processing, to SQLite3SecondaryFile.

    This class provides methods to add, delete, and modify, records on the
    secondary database.

    This class provides methods to process recordsets created from records in
    a secondary database.  A dictionary of created recordsets is maintained
    and any that exist when the secondary database is closed are destroyed too.

    The make_cursor method creates a CursorSecondary instance, a Berkeley
    DB style cursor, which is used to traverse the secondary database.  A
    dictionary of these cursors is maintained and any that exist when the
    secondary database is closed are destroyed too.
    """

    def __init__(self, *args):
        """Add update, cursor, and recordset methods, to SecondaryFile.

        name - a secondary database name.
        sqlite3desc - primary and secondary database descriptions.
        primaryname - primary database name.

        Interpret primary in the Berleley DB sense of primary and secondary
        databases for the relationship between primaryname and name.
        """
        super().__init__(*args)
        self._recordsets = dict()
    
    def close(self):
        """Close any recordsets then delegate to superclass close() method."""
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super().close()

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSecondary(dbobject, keyrange)
        if c:
            self._clientcursors[c] = True
        return c

    def populate_recordset_key_like(self, recordset, key):
        """Return recordset containing database records with keys like key."""
        statement = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            ))
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        matcher = re.compile('.*?' + key, flags=re.IGNORECASE|re.DOTALL)
        get_segment_records = self.get_primary_database().get_segment_records
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement):
                if not matcher.match(record[0]):
                    continue
                column = record[1]
                if record[2] == 1:
                    if column in recordset:
                        recordset[column] |= RecordsetSegmentInt(
                            column,
                            None,
                            records=record[3].to_bytes(2, byteorder='big'))
                    else:
                        recordset[column] = RecordsetSegmentInt(
                            column,
                            None,
                            records=record[3].to_bytes(2, byteorder='big'))
                else:
                    bs = get_segment_records(record[3])
                    if bs is None:
                        raise Sqlite3apiError('Segment record missing')
                    if len(bs) == db_segment_size_bytes:
                        if column in recordset:
                            recordset[column] |= RecordsetSegmentBitarray(
                            column, None, records=bs)
                        else:
                            recordset[column] = RecordsetSegmentBitarray(
                            column, None, records=bs)
                    else:
                        if column in recordset:
                            recordset[column] |= RecordsetSegmentList(
                                column, None, records=bs)
                        else:
                            recordset[column] = RecordsetSegmentList(
                                column, None, records=bs)
        finally:
            cursor.close()

    def populate_recordset_key(self, recordset, key):
        """Return recordset on database containing records for key."""
        statement = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'where',
            self.dataname, '== ?',
            ))
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        values = (key,)
        get_segment_records = self.get_primary_database().get_segment_records
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    recordset[record[1]] = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder='big'))
                else:
                    bs = get_segment_records(record[3])
                    if bs is None:
                        raise Sqlite3apiError('Segment record missing')
                    if len(bs) == db_segment_size_bytes:
                        recordset[record[1]] = RecordsetSegmentBitarray(
                            record[1], None, records=bs)
                    else:
                        recordset[record[1]] = RecordsetSegmentList(
                            record[1], None, records=bs)
        finally:
            cursor.close()

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting."""
        if key is None:
            return
        statement = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'where',
            self.dataname, 'glob ?',
            ))
        values = (
            b''.join(
                (key.encode() if isinstance(key, str) else key,
                 b'*',
                 )),)
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_primary_database().get_segment_records
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder='big'))
                else:
                    bs = get_segment_records(record[3])
                    if bs is None:
                        raise Sqlite3apiError('Segment record missing')
                    if len(bs) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=bs)
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=bs)
                if record[1] not in recordset:
                    recordset[record[1]] = segment.promote()
                else:
                    recordset[record[1]] |= segment
        finally:
            cursor.close()

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None and keyend is None:
            self.populate_recordset_all(recordset)
            return

        if keyend is None:
            statement = ' '.join((
                'select',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                'from',
                self.table_link,
                'where',
                self.dataname, '>= ?',
                ))
            values = (keystart,)
        elif keystart is None:
            statement = ' '.join((
                'select',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                'from',
                self.table_link,
                'where',
                self.dataname, '<= ?',
                ))
            values = (keyend,)
        else:
            statement = ' '.join((
                'select',
                self.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self.rowids_in_primary,
                'from',
                self.table_link,
                'where',
                self.dataname, '>= ? and',
                self.dataname, '<= ?',
                ))
            values = (keystart, keyend)
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_primary_database().get_segment_records
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder='big'))
                else:
                    bs = get_segment_records(record[3])
                    if bs is None:
                        raise Sqlite3apiError('Segment record missing')
                    if len(bs) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=bs)
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=bs)
                if record[1] not in recordset:
                    recordset[record[1]] = segment.promote()
                else:
                    recordset[record[1]] |= segment
        finally:
            cursor.close()
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        statement = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            ))
        values = ()
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_primary_database().get_segment_records
        cursor = self.table_dbservices.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder='big'))
                else:
                    bs = get_segment_records(record[3])
                    if bs is None:
                        raise Sqlite3apiError('Segment record missing')
                    if len(bs) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=bs)
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=bs)
                if record[1] not in recordset:
                    recordset[record[1]] = segment.promote()
                else:
                    recordset[record[1]] |= segment
        finally:
            cursor.close()

    def populate_segment(self, segment):
        """Helper for segment_delete and segment_put methods."""
        if segment[2] == 1:
            return RecordsetSegmentInt(
                segment[1],
                None,
                records=segment[3].to_bytes(2, byteorder='big'))
        else:
            bs = self.get_primary_database().get_segment_records(segment[3])
            if bs is None:
                raise Sqlite3apiError('Segment record missing')
            if len(bs) == SegmentSize.db_segment_size_bytes:
                return RecordsetSegmentBitarray(segment[1], None, records=bs)
            else:
                return RecordsetSegmentList(segment[1], None, records=bs)

    def make_segment(self, key, segment_number, record_count, records):
        """Return a Segment subclass instance created from arguments."""
        if record_count == 1:
            return RecordsetSegmentInt(
                segment_number,
                None,
                records=records.to_bytes(2, byteorder='big'))
        else:
            if len(records) == SegmentSize.db_segment_size_bytes:
                return RecordsetSegmentBitarray(
                    segment_number, None, records=records)
            else:
                return RecordsetSegmentList(
                    segment_number, None, records=records)
    
    def file_records_under(self, recordset, key):
        """File records in recordset under key.

        The existing references are replaced, not added to as in segment_put
        nor removed from as in segment_delete.

        """
        gpd = self.get_primary_database()

        # select (segment number, record count, key reference) statement for
        # (index value, key) which finds existing segments for index value.
        select_existing_segments = ' '.join((
            'select',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'indexed by',
            self.indexname,
            'where',
            self.dataname, '== ?',
            ))

        # Update (record count, key reference) statement
        # for (index value, segment number) used when exactly one record is
        # replaced by more than one record or more than one record is replaced
        # by exactly one record.  The former is associated with creation of a
        # segment reference record and the latter with a deletion.
        update_count_and_reference = ' '.join((
            'update',
            self.table_link,
            'set',
            SQLITE_COUNT_COLUMN, '= ? ,',
            self.rowids_in_primary, '= ?',
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # insert or replace (index value, segment number, record count,
        # key reference) statement used when a new segment is created or an
        # existing one replaced.  Key reference is the segment reference if
        # more than one record is in the segment for the index value, or the
        # record key when one record is referenced.
        insert_new_segment = ' '.join((
            'insert or replace into',
            self.table_link,
            '(',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            ')',
            'values ( ? , ? , ? , ? )',
            ))

        # delete (index value, segment number) statement used when a segment
        # which references one or more records is replaced by an empty segment.
        delete_existing_segment = ' '.join((
            'delete from',
            self.table_link,
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Find the segments and sort by record counts.
        # The segment references are reused, not necessarily for the same
        # segment, or deleted.
        # Originally DB_CONVERSION_LIMIT marked the boundary, in record count,
        # between lists and bitarrays and was used to stay close to the bsddb
        # version.  But sorting by record count is just as good here.
        cursor = self.table_dbservices.cursor()
        try:
            rows = cursor.execute(select_existing_segments, (key,)).fetchall()
        finally:
            cursor.close()
        old_rows = [s[1] for s in sorted((r[1], r) for r in rows if r[1] > 1)]
        rows = {r[0] for r in rows}
        recordset.normalize()

        # Process the segments in segment number order.
        cursor = self.table_dbservices.cursor()
        try:
            for sn in recordset.sorted_segnums:
                rs_segment = recordset.rs_segments[sn]

                if isinstance(rs_segment, RecordsetSegmentBitarray):

                    # Use an existing bitarray in preference to a list.
                    if len(old_rows):
                        sk = old_rows.pop()[2]
                        gpd.set_segment_records(
                            (rs_segment._bitarray.tobytes(), sk))
                        rows.remove(sn)
                        cursor.execute(
                            update_count_and_reference,
                            (rs_segment.count_records(),
                             sk,
                             key,
                             sn))
                    else:
                        sk = gpd.insert_segment_records(
                            (rs_segment._bitarray.tobytes(),))
                        cursor.execute(
                            insert_new_segment,
                            (key,
                             sn,
                             rs_segment.count_records(),
                             sk))
                        
                elif isinstance(rs_segment, RecordsetSegmentList):
                    rnlist = b''.join(
                        [rn.to_bytes(2, byteorder='big')
                         for rn in rs_segment._list])

                    # Use an existing list in preference to a bitarray.
                    if len(old_rows):
                        sk = old_rows.pop(0)[2]
                        gpd.set_segment_records((rnlist, sk))
                        rows.remove(sn)
                        cursor.execute(
                            update_count_and_reference,
                            (rs_segment.count_records(),
                             sk,
                             key,
                             sn))
                    else:
                        sk = gpd.insert_segment_records((rnlist,))
                        cursor.execute(
                            insert_new_segment,
                            (key,
                             sn,
                             rs_segment.count_records(),
                             sk))
                        
                elif isinstance(rs_segment, RecordsetSegmentInt):

                    # Use direct reference to record number.
                    if sn in rows:
                        rows.remove(sn)
                        cursor.execute(
                            update_count_and_reference,
                            (rs_segment.count_records(),
                             int.from_bytes(
                                 rs_segment.tobytes(), 'big'),
                             key,
                             sn))
                    else:
                        cursor.execute(
                            insert_new_segment,
                            (key,
                             sn,
                             rs_segment.count_records(),
                             int.from_bytes(
                                 rs_segment.tobytes(), 'big')
                             ))
        finally:
            cursor.close()
                    
        # Delete any references not reused by file_records_under.
        cursor = self.table_dbservices.cursor()
        try:
            for reuse in rows:
                cursor.execute(delete_existing_segment, (key, reuse))
        finally:
            cursor.close()
        for sk in old_rows:
            gpd.delete_segment_records((sk[2],))
    
    def unfile_records_under(self, key):
        """Delete the reference to records in file under key.

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        gpd = self.get_primary_database()

        # select (segment number, record count, key reference) statement for
        # (index value, key) which finds existing segments for index value.
        select_existing_segments = ' '.join((
            'select',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'indexed by',
            self.indexname,
            'where',
            self.dataname, '== ?',
            ))

        # delete (index value, segment number) statement used when a segment
        # which references one or more records is replaced by an empty segment.
        delete_existing_segment = ' '.join((
            'delete from',
            self.table_link,
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Find the segments.
        # The segment references are deleted.
        # Originally DB_CONVERSION_LIMIT marked the boundary, in record count,
        # between lists and bitarrays and was used to stay close to the bsddb
        # version.
        cursor = self.table_dbservices.cursor()
        try:
            rows = cursor.execute(select_existing_segments, (key,)).fetchall()
        finally:
            cursor.close()
        old_rows = [s[1] for s in sorted((r[1], r) for r in rows if r[1] > 1)]
        rows = {r[0] for r in rows}
                    
        # Delete all references.
        cursor = self.table_dbservices.cursor()
        try:
            for reuse in rows:
                cursor.execute(delete_existing_segment, (key, reuse))
        finally:
            cursor.close()
        for sk in old_rows:
            gpd.delete_segment_records((sk[2],))

    def get_first_primary_key_for_index_key(self, key):
        """Return the record number on primary table given key on index.

        This method should be used only on indexed columns whose keys each
        reference a single row. The intended use is where a key for a
        column in table has been derived from a row in some other table.

        """
        statement = ' '.join((
            'select',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'where',
            self.dataname, '== ?',
            'order by',
            self.rowids_in_primary,
            'limit 1',
            ))
        values = (key,)
        cursor = self.table_dbservices.cursor()
        try:
            s, c, n = cursor.execute(statement, values).fetchone()
        except TypeError:
            if not isinstance(key, (str, bytes)):
                raise
            return None
        finally:
            cursor.close()
        if c != 1:
            raise Sqlite3apiError('Index must refer to unique record')
        return s * SegmentSize.db_segment_size + n
    
    def segment_delete(self, key, segment, record_number):
        """Remove record_number from segment for key and write to database.

        """
        gpd = self.get_primary_database()

        # select (index value, segment number, record count, key reference)
        # statement for (index value, segment number).  Execution returns None
        # if no record to delete.
        select_existing_segment = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Update (record count) statement for (index value, segment number) used
        # when at least one record remains after deletion.
        update_record_count = ' '.join((
            'update',
            self.table_link,
            'set',
            SQLITE_COUNT_COLUMN, '= ?',
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Update (record count, key reference) statement
        # for (index value, segment number) used when exactly one record
        # remains after deletion.
        update_count_and_reference = ' '.join((
            'update',
            self.table_link,
            'set',
            SQLITE_COUNT_COLUMN, '= ? ,',
            self.rowids_in_primary, '= ?',
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # delete (index value, segment number) statement used when only record
        # in segment is deleted.
        delete_existing_segment = ' '.join((
            'delete from',
            self.table_link,
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Get the existing segment if there is one.
        cursor = self.table_dbservices.cursor()
        try:
            s = cursor.execute(select_existing_segment, (key, segment)
                               ).fetchone()
        finally:
            cursor.close()

        if s is None:

            # Assume record already deleted.
            return
        
        # Remove record_number from existing segment.
        seg = RecordsetSegmentInt(
            segment,
            None,
            records=record_number.to_bytes(2, byteorder='big')
            )
        existing_segment = self.populate_segment(s)
        seg = (seg & existing_segment) ^ existing_segment

        count = seg.count_records()
        if count == existing_segment.count_records():

            # Assume record already deleted.
            return

        if not isinstance(existing_segment, RecordsetSegmentBitarray):
            seg = seg.normalize()
        else:
            seg = seg.normalize(use_upper_limit=False)
        if count > 1:

            # Update segment and record count.
            gpd.set_segment_records((seg.tobytes(), s[3]))
            cursor = self.table_dbservices.cursor()
            try:
                cursor.execute(update_record_count, (count, key, segment))
            finally:
                cursor.close()
            return

        if count == 1:

            # Assume count was >1 before delete and delete segment leaving a
            # direct reference to the remaining record number.
            gpd.delete_segment_records((s[3],))
            rn = seg.get_record_number_at_position(0)
            cursor = self.table_dbservices.cursor()
            try:
                cursor.execute(
                    update_count_and_reference,
                    (count,
                     rn % (segment * SegmentSize.db_segment_size
                           ) if segment else rn,
                     key,
                     segment))
            finally:
                cursor.close()
            return

        # Assume count was 1 before delete and delete segment.
        cursor = self.table_dbservices.cursor()
        try:
            cursor.execute(delete_existing_segment, (key, segment))
        finally:
            cursor.close()
        return
    
    def segment_put(self, key, segment, record_number):
        """Add record_number to segment for key and write to database.

        """
        gpd = self.get_primary_database()

        # All puts of new records except the first in a segment are splicing
        # into an existing segment, so follow the example of sort_and_write()
        # in sqlite3duapi.py.

        # select (index value, segment number, record count, key reference)
        # statement for (index value, segment number).  Execution returns None
        # if no splicing needed.
        select_existing_segment = ' '.join((
            'select',
            self.dataname, ',',
            SQLITE_SEGMENT_COLUMN, ',',
            SQLITE_COUNT_COLUMN, ',',
            self.rowids_in_primary,
            'from',
            self.table_link,
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

        # Update (record count) statement for (index value, segment number) used
        # when splicing needed.
        update_record_count = ' '.join((
            'update',
            self.table_link,
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
            self.table_link,
            'set',
            SQLITE_COUNT_COLUMN, '= ? ,',
            self.rowids_in_primary, '= ?',
            'where',
            self.dataname, '== ? and',
            SQLITE_SEGMENT_COLUMN, '== ?',
            ))

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

        # Get the existing segment if there is one.
        cursor = self.table_dbservices.cursor()
        try:
            s = cursor.execute(select_existing_segment, (key, segment)
                               ).fetchone()
        finally:
            cursor.close()

        if s is None:

            # Create a new segment referring to one record.
            cursor = self.table_dbservices.cursor()
            try:
                cursor.execute(insert_new_segment,
                               (key, segment, 1, record_number))
            finally:
                cursor.close()
            return

        # Splice record_number into existing segment.
        # If the existing segment record for a segment had a record count > 1
        # before being updated, a subsidiary table record already exists.
        # Otherwise it must be created.
        # Key reference is a record number if the record count is 1.
        existing_segment = self.populate_segment(s)
        seg = RecordsetSegmentInt(
            segment,
            None,
            records=record_number.to_bytes(2, byteorder='big')
            ) | existing_segment
        if not isinstance(existing_segment, RecordsetSegmentBitarray):
            seg = seg.normalize()
        if s[2] > 1:
            gpd.set_segment_records((seg.tobytes(), s[3]))
            cursor = self.table_dbservices.cursor()
            try:
                cursor.execute(update_record_count, (s[2] + 1, key, segment))
            finally:
                cursor.close()
        else:
            nv = gpd.insert_segment_records((seg.tobytes(),))
            cursor = self.table_dbservices.cursor()
            try:
                cursor.execute(update_count_and_reference,
                               (s[2] + 1, nv, key, s[1]))
            finally:
                cursor.close()

    def find_values(self, valuespec):
        """Yield values meeting valuespec specification."""
        if valuespec.above_value and valuespec.below_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '> ? and',
                self.dataname, '< ?',
                ))
            values = valuespec.above_value, valuespec.below_value
        elif valuespec.above_value and valuespec.to_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '> ? and',
                self.dataname, '<= ?',
                ))
            values = valuespec.above_value, valuespec.to_value
        elif valuespec.from_value and valuespec.to_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '>= ? and',
                self.dataname, '<= ?',
                ))
            values = valuespec.from_value, valuespec.to_value
        elif valuespec.from_value and valuespec.below_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '>= ? and',
                self.dataname, '< ?',
                ))
            values = valuespec.from_value, valuespec.below_value
        elif valuespec.above_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '> ?',
                ))
            values = valuespec.above_value,
        elif valuespec.from_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '>= ?',
                ))
            values = valuespec.from_value,
        elif valuespec.to_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '<= ?',
                ))
            values = valuespec.to_value,
        elif valuespec.below_value:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                'where',
                self.dataname, '< ?',
                ))
            values = valuespec.below_value,
        else:
            statement = ' '.join((
                'select distinct',
                self.dataname,
                'from',
                self.table_link,
                ))
            values = ()
        apply_to_value = valuespec.apply_pattern_and_set_filters_to_value
        cursor = self.table_dbservices.cursor()
        try:
            for r in cursor.execute(statement, values):
                if apply_to_value(r[0]):
                    yield r[0]
        finally:
            cursor.close()


class Definition(definition.Definition):

    """Define method to create secondary database classes for apsw and sqlite3
    interfaces.

    Actions are delegated to Definition superclass.

    """
    def make_secondary_class(self,
                             secondary_class,
                             dbset,
                             primary,
                             secondary,
                             specification,
                             **kw):
        return secondary_class(secondary, specification, primary)


class Cursor(cursor.Cursor):
    
    """Emulate dbapi.Cursor using _sqlite3.File emulation of dbapi.File.

    A SQLite3 cursor is created which exists until this Cursor is
    deleted.

    The CursorPrimary and CursorSecondary subclasses define the
    bsddb style cursor methods peculiar to primary and secondary databases.

    Primary and secondary database, and others, should be read as the Berkeley
    DB usage.  This class emulates interaction with a Berkeley DB database via
    the Python bsddb3 module.

    Segmented should be read as the DPT database engine usage.

    The value part of (key, value) on primary or secondary databases is either:

        primary key (segment and record number)
        reference to a list of primary keys for a segment
        reference to a bit map of primary keys for a segment

    References are to rowids on the primary database's segment table.

    Each primary database rowid is mapped to a bit in the bitmap associated
    with the segment for the primary database rowid.

    """

    def __init__(self, dbset, keyrange=None, **kargs):
        """Emulate dbapi.Cursor using _sqlite3.File emulation of dbapi.File.

        dbset - a _sqlite3.File instance.
        keyrange - not used.
        kargs - absorb argunents relevant to other database engines.
        """
        super().__init__(dbset)
        self._most_recent_row_read = False
        if dbset.table_connection_list is not None:
            self._cursor = dbset.table_dbservices.cursor()
        self._current_segment = None
        self._current_segment_number = None
        self._current_record_number_in_segment = None

    def close(self):
        """Delete database cursor then delegate to superclass close() method."""
        try:
            del self._dbset._clientcursors[self]
        except:
            pass
        self._most_recent_row_read = False
        super().close()

    def get_converted_partial(self):
        """return self._partial as it would be held on database."""
        # See comment at get_converted_partial_with_wildcard().
        return self._partial#.encode()

    def get_partial_with_wildcard(self):
        """return self._partial with wildcard suffix appended."""
        raise Sqlite3apiError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """return converted self._partial with wildcard suffix appended."""
        # Code replaced:
        #return self._dbset.get_converter(USE_BYTES)(
        #    ''.join((self._partial, '*')))
        # implies self._partial is str always: so calling the converter, which
        # tests if argument is str. is pointless.
        # Sqlite will encode str internally so the encode() is commented.
        return ''.join((self._partial, '*'))#.encode()

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass


class CursorPrimary(Cursor):
    
    """Emulate dbapi version of CursorPrimary using a _sqlite3.File."""

    def count_records(self):
        """return record count or None if cursor is not usable."""

        # Quicker than executing 'select count ( * ) ...' for many records.
        statement = ' '.join((
            'select',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.get_existence_bits()._segment_link,
            ))
        return sum(RecordsetSegmentBitarray(0,
                                            None,
                                            records=r[0]).count_records()
                   for r in self._cursor.execute(statement))

    def first(self):
        """Return first record taking partial key into account."""
        statement = ' '.join((
            'select',
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'order by',
            self._dbset.dataname,
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)."""
        if record is None:
            return 0

        # Quicker than executing 'select count ( * ) ...' for many records.
        statement = ' '.join((
            'select',
            'rowid', ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.get_existence_bits()._segment_link,
            'order by rowid',
            ))
        dss = SegmentSize.db_segment_size
        position = 0
        rowid = record[0]
        for r in self._cursor.execute(statement):
            segment = RecordsetSegmentBitarray(r[0]-1, None, records=r[1])
            if r[0] * dss <= rowid:
                position += segment.count_records()
                continue
            position += segment.get_position_of_record_number(
                rowid % dss)
            break
        return position

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None."""
        if position is None:
            return None
        if position < 0:
            statement = ' '.join((
                'select * from',
                self._dbset.table_link,
                'order by',
                self._dbset.dataname, 'desc',
                'limit 1',
                'offset ?',
                ))
            values = (str(-1 - position),)
        else:
            statement = ' '.join((
                'select * from',
                self._dbset.table_link,
                'order by',
                self._dbset.dataname,
                'limit 1',
                'offset ?',
                ))
            values = (str(position - 1),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account."""
        statement = ' '.join((
            'select',
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'order by',
            self._dbset.dataname, 'desc',
            'limit 1',
            ))
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        statement = ' '.join((
            'select',
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'where',
            self._dbset.dataname, '>= ?',
            'order by',
            self._dbset.dataname,
            'limit 1',
            ))
        values = (key,)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select',
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'where',
            self._dbset.dataname, '> ?',
            'order by',
            self._dbset.dataname,
            'limit 1',
            ))
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        return self._most_recent_row_read

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.last()
        elif self._most_recent_row_read is None:
            return None
        statement = ' '.join((
            'select',
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'where',
            self._dbset.dataname, '< ?',
            'order by',
            self._dbset.dataname, 'desc',
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
            self._dbset.dataname, ',',
            SQLITE_VALUE_COLUMN,
            'from',
            self._dbset.table_link,
            'where',
            self._dbset.dataname, '== ?',
            'order by',
            self._dbset.dataname,
            'limit 1',
            ))
        values = (record[0],)
        row = self._cursor.execute(statement, values).fetchone()
        if row:
            self._most_recent_row_read = row
        return row

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        #raise Sqlite3apiError('refresh_recordset not implemented')


class CursorSecondary(Cursor):
    
    """Emulate dbapi version of CursorSecondary using a _sqlite3.File."""

    def count_records(self):
        """Return record count."""
        #if self.get_partial() is None:
        if self.get_partial() in (None, False):
            statement = ' '.join((
                'select',
                SQLITE_COUNT_COLUMN, # to avoid sum() overflow
                'from',
                self._dbset.table_link,
                ))
            values = ()
        else:
            statement = ' '.join((
                'select',
                SQLITE_COUNT_COLUMN, # to avoid sum() overflow
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ?',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        count = 0
        for r in self._cursor.execute(statement, values):
            count += r[0]
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).first()

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(value,
                                               SegmentSize.db_segment_size)
        # Get position of record relative to start point
        if not self.get_partial():
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                ))
            values = ()
        else:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                ))
            values = (self.get_converted_partial_with_wildcard(),)
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        gpd = self._dbset.get_primary_database()
        position = 0
        for r in self._cursor.execute(statement, values):
            if r[0] < key:
                position += r[2]
            elif r[0] > key:
                break
            elif r[1] < segment_number:
                position += r[2]
            elif r[1] > segment_number:
                break
            else:
                if r[2] == 1:
                    segment = RecordsetSegmentInt(
                        segment_number,
                        None,
                        records=r[3].to_bytes(2, byteorder='big'))
                else:
                    bs = gpd.get_segment_records(r[3])
                    if len(bs) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            segment_number, None, records=bs)
                    else:
                        segment = RecordsetSegmentList(
                            segment_number, None, records=bs)
                position += segment.get_position_of_record_number(record_number)
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        # Start at first or last record whichever is likely closer to position
        if position < 0:
            is_step_forward = False
            position = -1 - position
            if not self.get_partial():
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'order by',
                    self._dbset.dataname, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    ))
                values = ()
            else:
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, 'glob ?',
                    'order by',
                    self._dbset.dataname, 'desc', ',',
                    SQLITE_SEGMENT_COLUMN, 'desc',
                    ))
                values = (self.get_converted_partial_with_wildcard(),)
        else:
            is_step_forward = True
            if not self.get_partial():
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'order by',
                    self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                    ))
                values = ()
            else:
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, 'glob ?',
                    'order by',
                    self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                    ))
                values = (self.get_converted_partial_with_wildcard(),)
        # Get record at position relative to start point
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        gpd = self._dbset.get_primary_database()
        count = 0
        for r in self._cursor.execute(statement, values):
            count += r[2]
            if count < position:
                continue
            count -= position
            if r[2] == 1:
                segment = RecordsetSegmentInt(
                    r[1],
                    None,
                    records=r[3].to_bytes(2, byteorder='big'))
            else:
                bs = gpd.get_segment_records(r[3])
                if len(bs) == db_segment_size_bytes:
                    segment = RecordsetSegmentBitarray(r[1], None, records=bs)
                else:
                    segment = RecordsetSegmentList(r[1], None, records=bs)
            record_number = segment.get_record_number_at_position(
                count, is_step_forward)
            if record_number is not None:
                return r[0], record_number
            break
        return None

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'order by',
                self._dbset.dataname, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ?',
                'order by',
                self._dbset.dataname, 'desc', ',',
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
        """Return nearest record to key taking partial key into account."""
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, '>= ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (key,)
        elif self.get_partial() is False:
            return None
        else:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ? and',
                self._dbset.dataname, '>= ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self.get_converted_partial_with_wildcard(), key)
        self._most_recent_row_read = self._cursor.execute(
            statement, values).fetchone()
        if self._most_recent_row_read is None:
            return None
        return self.set_current_segment(self._most_recent_row_read).first()

    def next(self):
        """Return next record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        record = self._current_segment.next()
        if record is not None:
            return record
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '> ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (self._current_segment._key, self._current_segment_number)
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, '> ?',
                    'order by',
                    self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
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
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ? and',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '> ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
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
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, 'glob ? and',
                    self._dbset.dataname, '> ?',
                    'order by',
                    self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
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
        """Return previous record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.first()
        elif self._most_recent_row_read is None:
            return None
        record = self._current_segment.prev()
        if record is not None:
            return record
        if self.get_partial() is None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '< ?',
                'order by',
                self._dbset.dataname, 'desc', ',',
                SQLITE_SEGMENT_COLUMN, 'desc',
                'limit 1',
                ))
            values = (self._current_segment._key, self._current_segment_number)
            self._most_recent_row_read = self._cursor.execute(
                statement, values).fetchone()
            if self._most_recent_row_read is None:
                statement = ' '.join((
                    'select',
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, '< ?',
                    'order by',
                    self._dbset.dataname, 'desc', ',',
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
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ? and',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '< ?',
                'order by',
                self._dbset.dataname, 'desc', ',',
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
                    self._dbset.dataname, ',',
                    SQLITE_SEGMENT_COLUMN, ',',
                    SQLITE_COUNT_COLUMN, ',',
                    self._dbset.rowids_in_primary,
                    'from',
                    self._dbset.table_link,
                    'where',
                    self._dbset.dataname, 'glob ? and',
                    self._dbset.dataname, '< ?',
                    'order by',
                    self._dbset.dataname, 'desc', ',',
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
        segment_number, record_number = divmod(record[1],
                                               SegmentSize.db_segment_size)
        if self.get_partial() is not None:
            statement = ' '.join((
                'select',
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, 'glob ? and',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
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
                self._dbset.dataname, ',',
                SQLITE_SEGMENT_COLUMN, ',',
                SQLITE_COUNT_COLUMN, ',',
                self._dbset.rowids_in_primary,
                'from',
                self._dbset.table_link,
                'where',
                self._dbset.dataname, '== ? and',
                SQLITE_SEGMENT_COLUMN, '== ?',
                'order by',
                self._dbset.dataname, ',', SQLITE_SEGMENT_COLUMN,
                'limit 1',
                ))
            values = (record[0], segment_number)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        segment = self._get_segment(*row)
        if record_number not in segment:
            return None
        self._current_segment = segment
        self._current_segment_number = row[1]
        self._most_recent_row_read = row
        return segment.setat(record[1])

    def set_partial_key(self, partial):
        """Set partial key."""
        self._partial = partial

    def _get_segment(self, key, segment_number, count, record_number):
        if count == 1:
            return RecordsetSegmentInt(
                segment_number,
                key,
                records=record_number.to_bytes(2, byteorder='big'))
        if self._current_segment_number == segment_number:
            if key == self._current_segment._key:
                return self._current_segment
        records=self._dbset.get_primary_database().get_segment_records(
            record_number)
        if len(records) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                segment_number, key, records=records)
        else:
            return RecordsetSegmentList(segment_number, key, records=records)

    def set_current_segment(self, segment_reference):
        """Return a RecordsetSegmentBitarray, RecordsetSegmentInt, or
        RecordsetSegmentList instance, depending on the current representation
        of the segment on the database.

        Argument is the 4-tuple segment reference returned by fetchone().

        """
        self._current_segment = self._get_segment(*segment_reference)
        self._current_segment_number = segment_reference[1]
        return self._current_segment

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.
        
        #raise Sqlite3apiError('refresh_recordset not implemented')


class RecordsetCursor(RecordsetCursor):
    
    """Add _get_record method to RecordsetCursor."""

    def __init__(self, recordset, **kargs):
        """Delegate recordset to superclass.

        kargs absorbs arguments relevant to other database engines.

        """
        super().__init__(recordset)

    # Hack to get round self._dbset._database being a Sqlite.Cursor which means
    # the Recordset.get_record method does not work here because it does:
    # record = self._database.get(record_number)
    # All self._dbset.get_record(..) calls replaced by self._get_record(..) in
    # this module (hope no external use for now).
    # Maybe Recordset should not have a get_record method.
    def _get_record(self, record_number, use_cache=False):
        """Return (record_number, record) using cache if requested."""
        dbset = self._dbset
        #primary = dbset.dbhome.get_database_instance(dbset.dbset, dbset.dbset
        #                                             ).dataname
        if use_cache:
            record = dbset.record_cache.get(record_number)
            if record is not None:
                return record # maybe (record_number, record)
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return # maybe raise
        primary = dbset.dbhome.get_database_instance(dbset.dbset, dbset.dbset)
        statement = ' '.join((
            'select',
            SQLITE_VALUE_COLUMN,
            'from',
            primary.table_link,
            'where',
            primary.dataname, '== ?',
            'limit 1',
            ))
        values = (record_number,)
        database_cursor = dbset._database.cursor()
        try:
            record = database_cursor.execute(statement, values).fetchone()[0]
        finally:
            database_cursor.close()
        # maybe raise if record is None (if not, None should go on cache)
        if use_cache:
            dbset.record_cache[record_number] = record
            dbset.record.deque.append(record_number)
        return (record_number, record)

            
class Segment(segment.Segment):
    
    """Define a primary database to store inverted record number lists or
    bitmaps.

    There are three types of segment: existence bitmap, recordset bitmap, and
    recordset record number list.

    Existence bitmaps are slightly different from the other two segment types
    because a count of records in the bitmap is maintained, so it gets a
    subclass of it's own.

    The SQL statements for the three types of segment are identical.

    """

    def __init__(self, file_reference=None, segment_type=None, **kargs):
        """Define SQLite3 table concatenating dbfile and segment_file as the
        table's name.
        
        segment_type is a suffix to the database name indicating the database
        is for existence bitmaps, recordset bitmaps, or recordset record number
        lists.

        file_reference is the name of the primary database containing the
        records the segment referes to.
        
        """
        super().__init__()
        self._segment_link = (SUBFILE_DELIMITER * 2).join((file_reference,
                                                           segment_type))
        self._segment_dbservices = None

    def close(self):
        """Delegate to superclass close() method then forger reference to
        database connection.
        """
        super().close()
        self._segment_dbservices = None

    def open_root(self, connection):
        """Remember connection and create table for segment references if it
        does not exist.
        """
        try:
            self._segment_dbservices = connection
        except:
            raise
        cursor = self._segment_dbservices.cursor()
        try:
            statement = ' '.join((
                'create table if not exists', self._segment_link,
                '(',
                self._segment_link,
                'integer primary key', ',',
                SQLITE_VALUE_COLUMN,
                ')',
                ))
            cursor.execute(statement)
        except:
            self._segment_dbservices = None
            raise
        finally:
            cursor.close()

    def get(self, key):
        """Get a segment record from the database."""
        statement = ' '.join((
            'select',
            SQLITE_VALUE_COLUMN,
            'from',
            self._segment_link,
            'where',
            self._segment_link, '== ?',
            'limit 1',
            ))
        values = (key,)
        cursor = self._segment_dbservices.cursor()
        try:
            return cursor.execute(statement, values).fetchone()[0]
        except TypeError:
            return None
        finally:
            cursor.close()

    def delete(self, key):
        """Delete a segment record from the database."""
        statement = ' '.join((
            'delete from',
            self._segment_link,
            'where',
            self._segment_link, '== ?',
            ))
        values = (key,)
        cursor = self._segment_dbservices.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def put(self, key, value):
        """Put a segment record on the database using key."""
        statement = ' '.join((
            'update',
            self._segment_link,
            'set',
            SQLITE_VALUE_COLUMN, '= ?',
            'where',
            self._segment_link, '== ?',
            ))
        values = (value, key)
        cursor = self._segment_dbservices.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def append(self, value):
        """Append a segment record on the database using a new key."""
        statement = ' '.join((
            'insert into',
            self._segment_link,
            '(',
            SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? )',
            ))
        values = (value,)
        #return self._segment_dbservices.execute(statement, values).lastrowid
        cursor = self._segment_dbservices.cursor()
        try:
            return cursor.execute(statement, values).execute(
                    ' '.join((
                        'select last_insert_rowid() from',
                        self._segment_link))).fetchone()[0]
        finally:
            cursor.close()

            
class ExistenceBitMap(Segment):
    
    """Define a primary database for existence bitmap segments.

    A count of segments is maintained.  (The thing of interest is the high
    segment number, which is assumed to be <number of segments> - 1. Perhaps
    segment_count is a very misleading name for the property.)
    """

    def __init__(self, **kargs):
        """Define SQLite3 table concatenating dbfile and 'exist' as the table's
        name, and create placeholder for number of segments.

        """
        super().__init__(segment_type='exist', **kargs)
        self._segment_count = None

    @property
    def segment_count(self):
        """Return number of segments."""
        return self._segment_count

    @segment_count.setter
    def segment_count(self, segment_number):
        """Set segment count from 0-based segment_number if greater."""
        if segment_number > self._segment_count:
            self._segment_count = segment_number + 1
    
    def open_root(self, connection):
        """Delegate to superclass open_root() method then remember count of
        segments."""
        super().open_root(connection)
        statement = ' '.join(('select count(*) from', self._segment_link))
        cursor = self._segment_dbservices.cursor()
        try:
            self._segment_count = cursor.execute(statement).fetchone()[0]
        finally:
            cursor.close()

            
class ControlFile(controlfile.ControlFile):
    
    """Define a SQLite3 table to hold the keys of existence bitmap segments
    which contain unset bits corresponding to deleted records in a primary
    database.

    The keys in this database are the names of the primary databases specified
    in the FileSpec instance for the application.

    This database is not in a 'primary database, secondary database'
    relationship.

    This database is used by FileControl instances to find record numbers which
    can be re-used rather than add a new record number at the end of a database
    without need.
    """

    def __init__(self, control_file='control', **kargs):
        """Define database named control_file.
        
        control_file, prefixed with '___', is used as the database name.

        """
        super().__init__()
        self._control_link = ''.join((SUBFILE_DELIMITER * 3, control_file))
        self._control_dbservices = None

    def open_root(self, connection):
        """Remember connection and create control table if it does not exist."""
        self._control_dbservices = connection
        cursor = self._control_dbservices.cursor()
        try:
            statement = ' '.join((
                'create table if not exists', self._control_link,
                '(',
                self._control_link, ',',
                SQLITE_VALUE_COLUMN, ',',
                'primary key',
                '(',
                self._control_link, ',',
                SQLITE_VALUE_COLUMN,
                ') )',
                ))
            cursor.execute(statement)
        except:
            self._control_dbservices = None
            raise
        finally:
            cursor.close()

    def close(self):
        """Forget database connection."""
        self._control_dbservices = None

    def get_control_database(self):
        """Return the database containing file control records."""
        return self._control_dbservices

    @property
    def control_file(self):
        """Return the name which is both primary column and table name."""
        return self._control_link


class FileControl:
    
    """Base class for managing _sqlite.Segment, or subclass, databases.

    Note the primary or secondary database name to be managed.

    Subclasses implement the management.
    """

    def __init__(self, dbfile):
        """Note dbfile as primary or secondary database name to be managed.
        """
        super().__init__()

        # Primary or Secondary file instance whose segment reuse is handled
        self._dbfile = dbfile


class FileControlPrimary(FileControl):
    
    """Keep track of segments in an existence bit map that contain freed record
    numbers that can be reused.

    The list of existence bit map segments containing freed record numbers
    is cached.
    """

    def __init__(self, *args):
        """Delegate arguments to superclass and create placeholder for list of
        segments with freed record numbers.
        """
        super().__init__(*args)
        self._freed_record_number_pages = None
        self._ebmkey = self._dbfile.encode_record_selector(
            'E' + self._dbfile._keyvalueset_name)

    @property
    def freed_record_number_pages(self):
        """Return existence bit map record numbers available for re-use."""
        if self._freed_record_number_pages is None:
            return None
        return bool(self._freed_record_number_pages)

    def note_freed_record_number(self, record_number):
        """Adjust segment of high and low freed record numbers."""
        self.note_freed_record_number_segment(
            *divmod(record_number, SegmentSize.db_segment_size))

    def note_freed_record_number_segment(
        self, segment, record_number_in_segment):
        """Adjust segment of high and low freed record numbers."""
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
            values = (self._ebmkey,)
            cursor = self._dbfile.get_control_database().cursor()
            try:
                for record in cursor.execute(statement, values):
                    self._freed_record_number_pages.append(
                        int.from_bytes(record[0], byteorder='big'))
            finally:
                cursor.close()
        insert = bisect.bisect_left(self._freed_record_number_pages, segment)
        if self._freed_record_number_pages:
            if insert < len(self._freed_record_number_pages):
                if self._freed_record_number_pages[insert] == segment:
                    return
        self._freed_record_number_pages.insert(insert, segment)
        statement = ' '.join((
            'insert into',
            self._dbfile._control_database.control_file,
            '(',
            self._dbfile._control_database.control_file,
            ',',
            SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? , ? )',
            ))
        values = (self._ebmkey, segment)
        cursor = self._dbfile.get_control_database().cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def get_lowest_freed_record_number(self):
        """Return low record number in segments with freed record numbers."""
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
            values = (self._ebmkey,)
            cursor = self._dbfile.get_control_database().cursor()
            try:
                for record in cursor.execute(statement, values):
                    self._freed_record_number_pages.append(record[0])
            finally:
                cursor.close()
        db_segment_size = SegmentSize.db_segment_size
        while len(self._freed_record_number_pages):
            s = self._freed_record_number_pages[0]
            lfrns = self._read_exists_segment(s)
            if lfrns is None:
                # Do not reuse record number on segment of high record number
                return 0
            try:
                first_zero_bit = lfrns.index(False, 0 if s else 1)
            except ValueError:
                statement = ' '.join((
                    'delete from',
                    self._dbfile._control_database.control_file,
                    'where',
                    self._dbfile._control_database.control_file, '== ? and',
                    SQLITE_VALUE_COLUMN, '== ?',
                    ))
                values = (self._ebmkey, s)
                cursor = self._dbfile.get_control_database().cursor()
                try:
                    cursor.execute(statement, values)
                finally:
                    cursor.close()
                del self._freed_record_number_pages[0]
                continue
            return s * db_segment_size + first_zero_bit
        else:
            return 0 # record number when inserting into RECNO database

    def _read_exists_segment(self, segment_number):
        """Return existence bit map for segment_number if not high segment."""
        # record keys are 1-based but segment_numbers are 0-based
        if segment_number < self._dbfile.get_existence_bits().segment_count - 1:
            ebm = Bitarray()
            ebm.frombytes(
                self._dbfile.get_existence_bits().get(segment_number + 1))
            return ebm
        return None

