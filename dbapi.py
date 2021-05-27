# dbapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
A database API, implemented using bsddb3, where indicies are represented as
lists or bitmaps of record numbers.

bsddb3 is an interface to Berkeley DB.

The database contains Berkeley DB primary databases, each with a set of
Berkeley DB secondary databases, where the associations are implied by the
behaviour of the Primary and Secondary classes.  The 'DB.associate' method
of Berkeley DB is not used.

Primary databases use the Recno access method, and secondary databases use the
Btree or Hash access methods.  (Switching an application which uses the Hash
access method to any of apswapi, sqlite3api, and dptapi, is fine but the access
method will be like Btree.)

The Primary and Secondary classes emulate the bitmapped record numbers used
by DPT's database engine.  The values in a secondary database are one of:

(<segment number>, <record number in segment>, <record count>)
(<segment number>, <list of record numbers in segment>, <record count>)
(<segment number>, <bitmap of record numbers in segment>, <record count>)

depending on how many record numbers need to be noted in the segment.

Bitmaps are fixed size but lists vary in size to fit the number of records.

The DBapi class configures it's superclass, Database, to use the Primary and
Secondary classes.

Transactions are supported but cannot be nested.

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

from .api.bytebit import Bitarray, SINGLEBIT
from .api import database
from .api import cursor
from .api.recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordsetCursor,
    )
from .api.constants import (
    DB_DEFER_FOLDER, SECONDARY_FOLDER,
    PRIMARY, SECONDARY, FILE,
    DB_FIELDATTS,
    ACCESS_METHOD, HASH,
    FIELDS,
    LENGTH_SEGMENT_BITARRAY_REFERENCE,
    LENGTH_SEGMENT_LIST_REFERENCE,
    LENGTH_SEGMENT_RECORD_REFERENCE,
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


class DBapiError(database.DatabaseError):
    pass

        
class _DatabaseEncoders:
    
    """Define default record key encoder and decoder.
    """

    def encode_record_number(self, key):
        """Return repr(key).encode() because this is bsddb3 version of method.

        Typically used to convert primary key to secondary index format,
        using Berkeley DB terminology.
        
        """
        return repr(key).encode()

    def decode_record_number(self, skey):
        """Return literal_eval(skey.decode()) because this is bsddb3 version.

        Typically used to convert DB primary key held on secondary index,
        using Berkeley DB terminology, to integer.

        """
        return literal_eval(skey.decode())

    def encode_record_selector(self, key):
        """Return key.encode() because this is bsddb3 version of method.

        Typically used to convert a secondary index key value to database
        engine format, using Berkeley DB terminology.
        
        """
        return key.encode()

    def encode_record_key(self, key):
        """Return key.encode() because this is bsddb3 version of method.
        
        Specifically when comparing the key of a record read from database with
        the key used to find it.
        
        """
        return key.encode()


class Database(database.Database, _DatabaseEncoders):
    
    """Access database with bsddb3.
    
    Primary databases are created as DB_RECNO.
    Secondary databases are DB_BTREE or DB_HASH both with DB_DUPSORT set.

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
    
    """

    def __init__(
        self,
        primary_class,
        secondary_class,
        database_specification,
        databasefolder,
        DBenvironment,
        *args,
        **kwargs):
        """Define database using bsddb3.

        primary_class - a subclass of Primary.
        secondary_class - a subclass of Secondary.
        database_specification - a FileSpec instance.
        databasefolder - folder containing the database.
        DBenvironment - flags for DBEnv.
        *args - absorb positional arguments for other database engines.
        **kwargs - arguments for self.make_root() call.
        """
        super().__init__(database_specification, databasefolder, **kwargs)
        
        # The active transaction object
        self._dbtxn = None
        
        # Parameters for setting up the DBenv object
        self._DBenvironment = DBenvironment

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
        self._control = None
        self._control_file = ''.join((SUBFILE_DELIMITER * 3, 'control'))

    def make_root(self, **kw):
        """Return Definition instance created from **kw arguments.
        """
        return Definition(database_instance=self, **kw)

    def backout(self):
        """Abort the active transaction and remove binding to txn object."""
        if self._dbtxn is not None:
            self._dbtxn.abort()
            self._dbtxn = None
            self._dbservices.txn_checkpoint(5)

    def close_context(self):
        """Close databases (DB instances) and environment."""
        if self._control:
            self._control.close()
        for table in self.database_definition.values():
            table.primary.close()
            for s in table.secondary.values():
                s.close()
        if self._dbservices is not None:
            self._dbservices.txn_checkpoint()
            self._dbservices.close()
            self._dbservices = None
        m = self.database_definition
        database_specification = self._dbspec
        for n in database_specification:
            m[n].primary.set_control_database(None)
            for k, v in database_specification[n][SECONDARY].items():
                m[n].associate(k).set_primary_database(None)

    def close_database(self):
        """Close databases (DB instances) and environment.

        Introduced for compatibility with DPT.  There is a case for closing
        self._dbservices in this method rather than doing it all in
        close_context.
        
        """
        self.close_context()
            
    def commit(self):
        """Commit the active transaction and remove binding to txn object."""
        if self._dbtxn is not None:
            self._dbtxn.commit()
            self._dbtxn = None
            self._dbservices.txn_checkpoint(5)

    def db_compatibility_hack(self, record, srkey):
        """Convert record and return in (key, value) format.
        
        Do nothing as record is in (key, value) format on Berkeley DB.
        Added for compatibility with DPT.
        
        """
        return record

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

        args.append(os.path.abspath(self._home_directory))
        args.extend(paths)

        return subprocess.Popen(args)

    def files_exist(self):
        """Return True if all defined files exist in self._home_directory.

        Berkeley DB databases are held, one per file, in self._home_directory.

        """
        if not os.path.isdir(self._home_directory):
            return False
        fileset = set()
        for d in self.database_definition.values():
            fileset.add(d.primary._dataname)
            fileset.add(self._control_file)
            fileset.add(d.primary._existence_bits._segment_file)
            fileset.add(d.primary._segment_list._segment_file)
            fileset.add(d.primary._segment_bits._segment_file)
            for s in d.secondary.values():
                fileset.add(s._dataname)
        filecount = len(fileset)
        for f in os.listdir(self._home_directory):
            if f in fileset:
                fileset.remove(f)
        if len(fileset) == filecount:
            return None
        return len(fileset) == 0

    def get_database_filenames(self):
        """Return list of database file names."""
        names = ['___control']
        for f in os.listdir(self._home_directory):
            b, e = os.path.splitext(f)
            if b == 'log' and e[1:].isdigit():
                names.append(f)
        for mv in self.database_definition.values():
            names.extend(mv.primary.get_filenames())
            for s in mv.secondary.values():
                names.extend(s.get_filenames())
        return names
    
    def get_database(self, dbset, dbname):
        """Return DB for dbname in dbset."""
        return self.database_definition[dbset].primary.table_link
    
    def get_transaction(self):
        """Return object created by an earlier DBEnv.txn_begin() call or None
        if a DBEnv.abort() or DBEnv.commit() call is more recent."""
        return self._dbtxn

    def is_recno(self, dbset, dbname):
        """Return True if DB dbname in dbset is RECNO."""
        return self.database_definition[dbset].associate(dbname).is_recno()

    def open_context(self):
        """Open all DBs."""
        try:
            os.mkdir(self._home_directory)
        except FileExistsError:
            if not os.path.isdir(self._home_directory):
                raise

        # Set up control database for segments with unused record numbers.
        self._control = ControlFile(database_instance=self)
        
        gbytes = self._DBenvironment.get('gbytes', 0)
        bytes_ = self._DBenvironment.get('bytes', 0)
        flags = self._DBenvironment.get('flags', 0)
        self._dbservices = DBEnv()
        if gbytes or bytes_:
            self._dbservices.set_cachesize(gbytes, bytes_)
        self._dbservices.open(self._home_directory, flags)
        self.start_transaction()
        for table in self.database_definition.values():
            table.primary.open_root(self._dbservices)
            for s in table.secondary.values():
                s.open_root(self._dbservices)
        self._control.open_root(self._dbservices)
        self.commit()

        # Refer to primary from secondary for access to segment databases
        # Link each primary to control file for segment management
        m = self.database_definition
        database_specification = self._dbspec
        for n in database_specification:
            m[n].primary.set_control_database(self._control)
            for k, v in database_specification[n][SECONDARY].items():
                m[n].associate(k).set_primary_database(m[n].primary)
        return True

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

    def encode_primary_key(self, dbset, instance):
        """Convert instance.key for use as database value.
        
        For Berkeley DB just return self.get_packed_key().
        
        """
        return self.get_packed_key(dbset, instance)

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

    def start_transaction(self):
        """Start transaction if none and bind txn object to self._dbtxn."""
        if self._dbtxn is None:
            self._dbtxn = self._dbservices.txn_begin()

    def cede_contexts_to_process(self, close_contexts):
        """Do nothing. Added for compatibility with Sqlite3 interfaces."""
        # According to earlier version of docstring this was for compatibility
        # with apsw interface specifically.
        # Perhaps the difference between Pythons 3.5 and 3.6 in transactions in
        # the sqlite3 interface is related.
        pass

    def create_recordset_cursor(self, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursor(recordset, self)


class DBapi(Database):
    
    """Access database with bsddb3.  See superclass for *args and **kargs.
    
    bsddb3 is an interface to Berkeley DB.

    Primary instances are used to access data, and Secondary instances are
    used to access indicies on the data.

    There will be one Primary instance for each Berkeley DB primary database,
    used approximately like a SQLite table.

    There will be one Secondary instance for each Berkeley DB secondary
    database, used approximately like a SQLite index.

    Primary and secondary terminology comes from Berkeley DB documentation but
    the association technique is not used.
    
    """

    def __init__(self, *args, **kargs):
        """Use Primary and Secondary classes."""
        super().__init__(Primary, Secondary, *args, **kargs)

            
class File(file.File):
    
    """Wrap a Berkeley DB database.

    This class defines elements common to primary and secondary databases.
    """

    def __init__(
        self,
        dbfile,
        dbdesc,
        dbname,
        database_instance=None,
        **kargs):
        """Define a Berkeley DB database.

        dbfile - primary database name, or secondary database name with a
                 primary name prefix.
        dbdesc - database description for primary and associated secondaries.
        dbname - primary or secondary database name.
        database_instance - DBapi instance.

        This class defines elements common to primary and secondary databases.

        """

        # This attribute is referenced in the super().__init__() chain after
        # moving the ExistenceBitMap() call to primaryfile.PrimaryFile.
        # The binding was after the super().__init__() previously.
        self._transaction = database_instance.get_transaction

        super().__init__(dbfile,
                         dbdesc[FIELDS][dbname],
                         dbname,
                         DB_FIELDATTS,
                         repr(dbname),
                         repr(dbdesc[PRIMARY]),
                         **kargs)

    @property
    def transaction(self):
        """Return the active transaction or None."""
        return self._transaction()

    def close(self):
        """Close DB and cursor."""
        if self._table_link is not None:
            self.table_link.close()
            self._table_link = None

    def open_root(self, dbenv):
        """Create DB in dbenv."""
        try:

            # Use a list for compatibility with dbduapi.py deferred updates.
            self._table_link = [DB(dbenv)]
            
        except:
            raise

    def get_database_file(self):
        """Return name of file containing database of key-value pairs.

        The name is a concatenation of the primary and secondary database
        names.  This assumes each key-value database is in a separate file.

        If all databases of key-value pairs are in a single file this attribute
        is a good candidate to replace _keyvalueset_name as the database name.

        """
        return self._dataname

    def get_filenames(self):
        """Return tuple of database file names."""
        return self._dataname,

            
class PrimaryFile(File, primaryfile.PrimaryFile):
    
    """Add segment support to File for primary database.

    This class provides the methods to manage the lists and bitmaps of record
    numbers for segments of the primary database.
    """

    def __init__(self, *args, **kargs):
        """Add segment support to File for primary database."""
        super().__init__(*args,
                         filecontrolprimary_class=FileControlPrimary,
                         existencebitmap_class=ExistenceBitMap,
                         file_reference=self,
                         **kargs)

        # Freed record list and bit map segment control structure
        self._control_secondary = FileControlSecondary(self)

        # Inverted index record number lists for this primary database
        self._segment_list = SegmentList(file_reference=self)

        # Inverted index record number bit maps for this primary database
        self._segment_bits = SegmentBitarray(file_reference=self)

    def is_primary_recno(self):
        """Return True."""
        # Maybe ask self._table_link
        return True

    def is_recno(self):
        """Return True."""
        # Maybe ask self._table_link
        return True

    def is_value_recno(self):
        """Return False."""
        # Maybe ask self._table_link
        return False

    def open_root(self, *args):
        """Open primary database.  See superclass for *args."""
        super().open_root(*args)
        try:
            self.table_link.open(
                self._dataname,
                self._keyvalueset_name,
                DB_RECNO,
                DB_CREATE,
                txn=self.transaction)
        except:
            self._table_link = None
            raise
        self._segment_list.open_root(*args)
        self._segment_bits.open_root(*args)
        self._existence_bits.open_root(*args)

    def close(self):
        """Close inverted index databases then delegate to superclass close()
        method."""
        self._segment_list.close()
        self._segment_bits.close()
        super().close()

    # Added for sqlite3 compatibility.
    # Berkeley DB uses get_segment_list_database.
    def get_segment_list(self):
        """Return the segment list control data."""
        return self._segment_list

    def get_segment_list_database(self):
        """Return the database containing segment record number lists."""
        # Maybe use instance.get_segment_list().get_seg_object() instead of
        # instance.get_segment_list_database()
        return self._segment_list._segment_link

    # Added for sqlite3 compatibility.
    # Berkeley DB uses get_segment_bits_database.
    def get_segment_bits(self):
        """Return the segment bits control data."""
        return self._segment_bits

    def get_segment_bits_database(self):
        """Return the database containing segment record number bit maps."""
        # Maybe use instance.get_segment_bits().get_seg_object() instead of
        # instance.get_segment_bits_database()
        return self._segment_bits._segment_link

    def get_existence_bits_database(self):
        """Return the database containing existence bit map."""
        # Maybe use instance.get_existence_bits().get_seg_object() instead of
        # instance.get_existence_bits_database()
        return self._existence_bits._segment_link

    def get_control_secondary(self):
        """Return the segment control data."""
        return self._control_secondary

    def get_filenames(self):
        """Return tuple of database file names."""
        return (self.get_database_file(),
                self._existence_bits._segment_file,
                self._segment_list._segment_file,
                self._segment_bits._segment_file)

            
class SecondaryFile(File, secondaryfile.SecondaryFile):
    
    """Add segment support to File for secondary database.

    This class uses the methods of the PrimaryFile class to manage the
    lists and bitmaps of record numbers for segments of the table supporting
    the secondary database.  The link is set after both tables have been
    opened.
    """

    def is_primary_recno(self):
        """Return True."""
        # Maybe ask self._table_link
        return True

    def is_recno(self):
        """Return False."""
        # Maybe ask self._table_link
        return False

    def is_value_recno(self):
        """Return True."""
        # Maybe ask self._table_link
        return True

    def open_root(self, *args):
        """Delegate to superclass open_root() method then create database if
        it does not exist.
        """
        super().open_root(*args)
        try:
            self.table_link.set_flags(DB_DUPSORT)
            self.table_link.open(
                self._dataname,
                self._keyvalueset_name,
                DB_HASH if self._fieldatts[ACCESS_METHOD] == HASH else DB_BTREE,
                DB_CREATE,
                txn=self.transaction)
        except:
            self._table_link = None
            raise

    def get_primary_segment_bits(self):
        """Return the segment bitmap database of primary database."""
        return self._primary_database.get_segment_bits_database()

    def get_primary_segment_list(self):
        """Return the segment list database of primary database."""
        return self._primary_database.get_segment_list_database()


class Primary(PrimaryFile, primary.Primary, _DatabaseEncoders):
    
    """Add record update and recordset processing to PrimaryFile.

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
        """Delete (key, value) from database."""
        # Transferred encode() from delete_instance, edit_instance, and
        # put_instance, calls (the only ones) so those methods are identical
        # to sqlite3 versions: well would have if value were mentioned.
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self.table_link.cursor(txn=self.transaction)
            try:
                if cursor.set(key):
                    cursor.delete()

                    # See comment in DBapi.delete_instance()
                    #self.get_control_primary().note_freed_record_number(key)

            finally:
                cursor.close()
        except:
            pass

    def get_primary_record(self, key):
        """Return primary record (key, value) given primary key on dbset."""
        if key is None:
            return None
        c = self.table_link.cursor(txn=self.transaction)
        try:
            record = c.set(key)
        finally:
            c.close()
        try:
            return key, record[1].decode()
        except:
            if record is None:
                return record
            raise

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorPrimary(dbobject,
                          keyrange=keyrange,
                          transaction=self.transaction)
        if c:
            self._clientcursors[c] = True
        return c

    def put(self, key, value):
        """Put (key, value) on database and return key for new RECNO records.

        The DB put method, or append for new RECNO records,is
        used for primary DBs with associated secondary DBs. The
        cursor put method is used otherwise.
        
        """
        # Transferred encode() from delete_instance, edit_instance, and
        # put_instance, calls (the only ones) so those methods are identical
        # to sqlite3 versions.
        # Primary assumed to be not DUPSORT nor DUP
        if not key: #key == 0:  # Change test to "key is None" when sure
            return self.table_link.append(
                value.encode(), txn=self.transaction)
        else:
            self.table_link.put(key, value.encode(), txn=self.transaction)
            return None

    def replace(self, key, oldvalue, newvalue):
        """Replace (key, oldvalue) with (key, newvalue) on DB.
        
        (key, newvalue) is put on DB only if (key, oldvalue) is on DB.
        
        """
        # Transferred encode() from delete_instance, edit_instance, and
        # put_instance, calls (the only ones) so those methods are identical
        # to sqlite3 versions.
        try:
            # Primary assumed to be not DUPSORT nor DUP
            cursor = self.table_link.cursor(txn=self.transaction)
            try:
                if cursor.set(key):
                    cursor.put(key, newvalue.encode(), DB_CURRENT)
            finally:
                cursor.close()
        except:
            pass

    def populate_recordset_key(self, recordset, key=None):
        """Return recordset on database containing records for key."""
        if key is None:
            return
        r = self.table_link.get(key, txn=self.transaction)
        if r:
            s, rn = divmod(key, SegmentSize.db_segment_size)
            recordset[s] = RecordsetSegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(keystart,
                                                 SegmentSize.db_segment_size)
        if keyend is not None:
            segment_end, recnum_end = divmod(keyend,
                                             SegmentSize.db_segment_size)
        c = self.get_existence_bits_database().cursor(txn=self.transaction)
        try:
            r = c.set(segment_start + 1)
            while r:
                if keyend is not None:
                    if r[0] - 1 > segment_end:
                        break
                recordset[r[0] - 1] = RecordsetSegmentBitarray(
                    r[0] - 1, None, records=r[1])
                r = c.next()
        finally:
            c.close()
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
        c = self.get_existence_bits_database().cursor(txn=self.transaction)
        try:
            r = c.first()
            while r:
                recordset[r[0] - 1] = RecordsetSegmentBitarray(
                    r[0] - 1, None, records=r[1])
                r = c.next()
        finally:
            c.close()

    def segment_delete(self, segment, record_number):
        """Remove record_number from existence bit map for segment."""
        # See dbduapi.Primary.defer_put for model.  Main difference
        # is the write back to database is done immediately (and delete!!).
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits_database().get(segment + 1,
                                                      txn=self.transaction)
        if ebmb is None:
            # It does not exist so raise an exception
            raise DBapiError('Existence bit map for segment does not exist')
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            # Set bit for record number and write segment back to database
            ebm[record_number] = False
            self.get_existence_bits_database().put(segment + 1,
                                                   ebm.tobytes(),
                                                   txn=self.transaction)

    def segment_put(self, segment, record_number):
        """Add record_number to existence bit map for segment."""
        # See dbduapi.Primary.defer_put for model.  Main difference
        # is the write back to database is done immediately.
        # Get the segment existence bit map from database
        ebmb = self.get_existence_bits_database().get(segment + 1,
                                                      txn=self.transaction)
        if ebmb is None:
            # It does not exist so create a new empty one
            ebm = SegmentSize.empty_bitarray.copy()
        else:
            # It does exist so convert database representation to bitarray
            ebm = Bitarray()
            ebm.frombytes(ebmb)
        # Set bit for record number and write segment back to database
        ebm[record_number] = True
        self.get_existence_bits_database().put(segment + 1,
                                               ebm.tobytes(),
                                               txn=self.transaction)

    def get_high_record(self):
        """Return record with highest record number."""
        c = self.table_link.cursor(txn=self.transaction)
        try:
            return c.last()
        finally:
            c.close()


class Secondary(SecondaryFile, secondary.Secondary, _DatabaseEncoders):
    
    """Add update, cursor, and recordset processing, to SecondaryFile.

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

    def make_cursor(self, dbobject, keyrange):
        """Create a cursor on the dbobject positiioned at start of keyrange."""
        c = CursorSecondary(dbobject,
                            keyrange=keyrange,
                            transaction=self.transaction)
        if c:
            self._clientcursors[c] = True
        return c
    
    def populate_recordset_key_like(self, recordset, key):
        """Return recordset containing database records with keys like key."""
        pattern = b'.*?' + key
        cursor = self.make_cursor(self, key)
        try:
            c = cursor._cursor
            r = c.first()
            while r:
                k, v = r
                if re.match(pattern, k, flags=re.IGNORECASE|re.DOTALL):
                    s = int.from_bytes(v[:4], byteorder='big')
                    if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                        srn = int.from_bytes(v[6:], byteorder='big')
                        bs = self.get_primary_segment_list().get(srn)
                        if bs is None:
                            raise DBapiError('Segment record missing')
                        if s in recordset:
                            recordset[s] |= RecordsetSegmentList(
                                s, None, records=bs)
                        else:
                            recordset[s] = RecordsetSegmentList(
                                s, None, records=bs)
                    elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                        srn = int.from_bytes(v[7:], byteorder='big')
                        bs = self.get_primary_segment_bits().get(srn)
                        if bs is None:
                            raise DBapiError('Segment record missing')
                        if s in recordset:
                            recordset[s] |= RecordsetSegmentBitarray(
                                s, None, records=bs)
                        else:
                            recordset[s] = RecordsetSegmentBitarray(
                                s, None, records=bs)
                    else:
                        if s in recordset:
                            recordset[s] |= RecordsetSegmentList(
                                s, None, records=v[4:])
                        else:
                            recordset[s] = RecordsetSegmentList(
                                s, None, records=v[4:])
                r = c.next()
        finally:
            cursor.close()
    
    def populate_recordset_key(self, recordset, key):
        """Return recordset of segments containing records for key."""
        cursor = self.make_cursor(self, key)
        try:
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
                        raise DBapiError('Segment record missing')
                    recordset[s] = RecordsetSegmentList(s, None, records=bs)
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    recordset[s] = RecordsetSegmentBitarray(s, None, records=bs)
                else:
                    recordset[s] = RecordsetSegmentList(s, None, records=v[4:])
                r = c.next()
        finally:
            cursor.close()

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting."""
        cursor = self.make_cursor(self, key)
        try:
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
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = recordset[s]._bitarray# needs tidying
                    for i in range(0, len(bs), 2):
                        sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = RecordsetSegmentBitarray(s, None, records=bs)
                    recordset[s] |= sba
                else:
                    rn = int.from_bytes(v[4:], byteorder='big')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    recordset[s]._bitarray[rn] = True# needs tidying
                r = c.next()
        finally:
            cursor.close()

    def populate_recordset_key_range(
        self, recordset, keystart=None, keyend=None):
        """Return recordset on database containing records for key range."""
        cursor = self.make_cursor(self, keystart)
        try:
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
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = recordset[s]._bitarray# needs tidying
                    for i in range(0, len(bs), 2):
                        sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = RecordsetSegmentBitarray(s, None, records=bs)
                    recordset[s] |= sba
                else:
                    rn = int.from_bytes(v[4:], byteorder='big')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    recordset[s]._bitarray[rn] = True# needs tidying
                r = c.next()
        finally:
            cursor.close()
    
    def populate_recordset_all(self, recordset):
        """Return recordset containing all referenced records."""
        cursor = self.make_cursor(self, None)
        try:
            c = cursor._cursor
            r = c.first()
            while r:
                v = r[1]
                s = int.from_bytes(v[:4], byteorder='big')
                if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn = int.from_bytes(v[6:], byteorder='big')
                    bs = self.get_primary_segment_list().get(srn)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = recordset[s]._bitarray# needs tidying
                    for i in range(0, len(bs), 2):
                        sba[int.from_bytes(bs[i:i+2], byteorder='big')] = True
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(v[7:], byteorder='big')
                    bs = self.get_primary_segment_bits().get(srn)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    sba = RecordsetSegmentBitarray(s, None, records=bs)
                    recordset[s] |= sba
                else:
                    rn = int.from_bytes(v[4:], byteorder='big')
                    if s not in recordset:
                        recordset[s] = RecordsetSegmentBitarray(s, None)
                    recordset[s]._bitarray[rn] = True# needs tidying
                r = c.next()
        finally:
            cursor.close()

    def populate_segment(self, segment):
        """Helper for segment_delete and segment_put methods."""
        k, v = segment
        s = int.from_bytes(k, byteorder='big')
        if len(v) + len(k) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(v[2:], byteorder='big')
            bs = self.get_primary_segment_list().get(srn)
            if bs is None:
                raise DBapiError('Segment record missing')
            return RecordsetSegmentList(s, None, records=bs)
        elif len(v) + len(k) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(v[3:], byteorder='big')
            bs = self.get_primary_segment_bits().get(srn)
            if bs is None:
                raise DBapiError('Segment record missing')
            return RecordsetSegmentBitarray(s, None, records=bs)
        else:
            return RecordsetSegmentList(s, None, records=v)

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
        """Replace records for index dbname[key] with recordset records."""
        # Delete existing segments for key
        self.unfile_records_under(key)
        # Put new segments for key
        cursor = self.table_link.cursor(txn=self.transaction)
        try:
            recordset.normalize()
            for sn in recordset.sorted_segnums:
                if isinstance(recordset.rs_segments[sn], RecordsetSegmentBitarray):
                    count = recordset.rs_segments[sn].count_records()
                    # stub call to get srn_bits from reuse stack
                    srn_bits = self.get_primary_database(
                        ).get_control_secondary().get_freed_bits_page()
                    if srn_bits == 0:
                        srn_bits = self.get_primary_segment_bits(
                            ).append(recordset.rs_segments[sn].tobytes(),
                                     txn=self.transaction)
                    else:
                        self.get_primary_segment_bits(
                            ).put(srn_bits,
                                  recordset.rs_segments[sn].tobytes(),
                                  txn=self.transaction)
                    cursor.put(
                        key,
                        b''.join(
                            (sn.to_bytes(4, byteorder='big'),
                             count.to_bytes(3, byteorder='big'),
                             srn_bits.to_bytes(4, byteorder='big'))),
                        DB_KEYLAST)
                elif isinstance(recordset.rs_segments[sn], RecordsetSegmentList):
                    count = recordset.rs_segments[sn].count_records()
                    # stub call to get srn_list from reuse stack
                    srn_list = self.get_primary_database(
                        ).get_control_secondary().get_freed_list_page()
                    if srn_list == 0:
                        srn_list = self.get_primary_segment_list().append(
                            recordset.rs_segments[sn].tobytes(),
                            txn=self.transaction)
                    else:
                        self.get_primary_segment_list().put(
                            srn_list,
                            recordset.rs_segments[sn].tobytes(),
                            txn=self.transaction)
                    cursor.put(
                        key,
                        b''.join(
                            (sn.to_bytes(4, byteorder='big'),
                             count.to_bytes(2, byteorder='big'),
                             srn_list.to_bytes(4, byteorder='big'))),
                        DB_KEYLAST)
                elif isinstance(recordset.rs_segments[sn], RecordsetSegmentInt):
                    cursor.put(
                        key,
                        b''.join(
                            (sn.to_bytes(4, byteorder='big'),
                             recordset.rs_segments[sn].tobytes())),
                        DB_KEYLAST)
        finally:
            cursor.close()
    
    def unfile_records_under(self, key):
        """Delete the reference to records in file under key.

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        # Delete existing segments for key
        cursor = self.table_link.cursor(txn=self.transaction)
        try:
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
                    self.get_primary_segment_list(
                        ).delete(srn_list, txn=self.transaction)
                    #cursor.delete()
                elif len(v) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn_bits = int.from_bytes(v[7:], byteorder='big')
                    # stub call to put srn_bits on reuse stack
                    self.get_primary_database().get_control_secondary(
                        ).note_freed_bits_page(srn_bits)
                    # ok if reuse bitmap but not if reuse stack
                    self.get_primary_segment_bits(
                        ).delete(srn_bits, txn=self.transaction)
                    #cursor.delete()
                #elif record_number == int.from_bytes(v[4:], byteorder='big'):
                    #cursor.delete()
                r = cursor.next()
            try:
                self.table_link.delete(key, txn=self.transaction)
            except DBNotFoundError:
                pass
        finally:
            cursor.close()

    def get_first_primary_key_for_index_key(self, key):
        """Return first primary key for secondary key in dbname for dbname.

        This method should be ued only on secondary DBs whose keys each have a
        unique value.
        
        """
        if isinstance(key, str):
            key = key.encode()
        try:
            c = self.table_link.cursor(txn=self.transaction)
            try:
                v = c.set(key)[1]
            finally:
                c.close()
        except:
            if not isinstance(key, bytes):
                raise
            return None
        if len(v) != LENGTH_SEGMENT_RECORD_REFERENCE:
            raise DBapiError('Index must refer to unique record')
        return (
            int.from_bytes(v[:4],
                           byteorder='big') * SegmentSize.db_segment_size +
            int.from_bytes(v[4:], byteorder='big'))

    def find_values(self, valuespec):
        """Yield values meeting valuespec specification."""
        cursor = self.table_link.cursor(txn=self.transaction)
        try:
            if valuespec.above_value and valuespec.below_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.above_value and valuespec.to_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value and valuespec.to_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value and valuespec.below_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.above_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.to_value:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.below_value:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            else:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
        finally:
            cursor.close()
    
    def segment_delete(self, key, segment, record_number):
        """Remove record_number from segment for key and write to database."""
        # Transferred from delete_instance, edit_instance, and put_instance,
        # calls (the only ones) so those methods are identical to sqlite3
        # versions.
        key = key.encode()
        # See dbapi.Secondary.segment_put (in this class definition) for model.
        cursor = self.table_link.cursor(txn=self.transaction)
        try:
            r = cursor.set_range(key)
            while r:
                k, v = r
                if k != key:
                    # Assume that multiple requests to delete an index value
                    # have been made for a record.  The segment_put method uses
                    # sets to avoid adding multiple entries.  Consider using
                    # set rather than list in the pack method of the subclass
                    # of Value if this will happen a lot.
                    return
                sr = int.from_bytes(v[:4], byteorder='big')
                if sr < segment:
                    r = cursor.next_dup()
                    continue
                elif sr > segment:
                    return
                if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn_list = int.from_bytes(v[6:], byteorder='big')
                    bs = self.get_primary_segment_list(
                        ).get(srn_list, txn=self.transaction)
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
                        self.get_primary_segment_list(
                            ).delete(srn_list, txn=self.transaction)
                        cursor.delete()
                        if count:
                            cursor.put(k, ref, DB_KEYLAST)
                    else:
                        seg = b''.join(tuple(
                            rn.to_bytes(length=2, byteorder='big')
                            for rn in sorted(recnums)))
                        self.get_primary_segment_list(
                            ).put(srn_list, seg, txn=self.transaction)
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
                    bs = self.get_primary_segment_bits(
                        ).get(srn_bits, txn=self.transaction)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    recnums = Bitarray()
                    recnums.frombytes(bs)
                    # ignore possibility record_number already absent
                    recnums[record_number] = False
                    count = recnums.count()
                    if count > SegmentSize.db_lower_conversion_limit:
                        self.get_primary_segment_bits().put(
                            srn_bits, recnums.tobytes(), txn=self.transaction)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 recnums.count().to_bytes(3, byteorder='big'),
                                 v[7:])),
                            DB_KEYLAST)
                    else:
                        recnums = {rn for rn in recnums.search(SINGLEBIT)}
                        # stub call to get srn_list from reuse stack
                        srn_list = self.get_primary_database(
                            ).get_control_secondary().get_freed_list_page()
                        if srn_list == 0:
                            srn_list = self.get_primary_segment_list().append(
                                b''.join(
                                    [rn.to_bytes(2, byteorder='big')
                                     for rn in sorted(recnums)]),
                                txn=self.transaction)
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    [rn.to_bytes(2, byteorder='big')
                                     for rn in sorted(recnums)]),
                                txn=self.transaction)
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
                        self.get_primary_segment_bits(
                            ).delete(srn_bits, txn=self.transaction)
                elif record_number == int.from_bytes(v[4:], byteorder='big'):
                    cursor.delete()
                return
        finally:
            cursor.close()
    
    def segment_put(self, key, segment, record_number):
        """Add record_number to segment for key and write to database."""
        # Transferred from delete_instance, edit_instance, and put_instance,
        # calls (the only ones) so those methods are identical to sqlite3
        # versions.
        key = key.encode()
        # See dbduapi.Secondary.defer_put for model.
        # The dance to find the segment record is a reason to convert these
        # secondary databases from DUP to NODUP.  Also a NODUP database allows
        # implementation equivalent to DPT 'FOR EACH VALUE' directly and easy
        # counting of values for manipulation of scrollbar sliders.
        # Assumption is that new records usually go in last segment for value.
        cursor = self.table_link.cursor(txn=self.transaction)
        try:
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
                if sr < segment:
                    r = cursor.next_dup()
                    continue
                elif sr > segment:
                    cursor.put(
                        k,
                        b''.join(
                            (segment.to_bytes(4, byteorder='big'),
                             record_number.to_bytes(2, byteorder='big'))),
                        DB_KEYLAST)
                    return
                if len(v) == LENGTH_SEGMENT_LIST_REFERENCE:
                    srn_list = int.from_bytes(v[6:], byteorder='big')
                    bs = self.get_primary_segment_list(
                        ).get(srn_list, txn=self.transaction)
                    recnums = {int.from_bytes(bs[i:i+2], byteorder='big')
                               for i in range(0, len(bs), 2)}
                    # ignore possibility record_number already present
                    recnums.add(record_number)
                    count = len(recnums)
                    if count > SegmentSize.db_upper_conversion_limit:
                        seg = SegmentSize.empty_bitarray.copy()
                        for rn in recnums:
                            seg[rn] = True
                        # stub call to put srn_list on reuse stack
                        self.get_primary_database().get_control_secondary(
                            ).note_freed_list_page(srn_list)
                        # ok if reuse bitmap but not if reuse stack
                        self.get_primary_segment_list(
                            ).delete(srn_list, txn=self.transaction)
                        # stub call to get srn_bits from reuse stack
                        srn_bits = self.get_primary_database(
                            ).get_control_secondary().get_freed_bits_page()
                        if srn_bits == 0:
                            srn_bits = self.get_primary_segment_bits(
                                ).append(seg.tobytes(), txn=self.transaction)
                        else:
                            self.get_primary_segment_bits(
                                ).put(srn_bits,
                                      seg.tobytes(),
                                      txn=self.transaction)
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
                        self.get_primary_segment_list(
                            ).put(srn_list, seg, txn=self.transaction)
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
                    bs = self.get_primary_segment_bits(
                        ).get(srn, txn=self.transaction)
                    if bs is None:
                        raise DBapiError('Segment record missing')
                    recnums = Bitarray()
                    recnums.frombytes(bs)
                    recnums[record_number] = True
                    self.get_primary_segment_bits(
                        ).put(srn, recnums.tobytes(), txn=self.transaction)
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
                                     rn.to_bytes(length=2, byteorder='big'))),
                                txn=self.transaction)
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    (record_number.to_bytes(
                                        length=2, byteorder='big'),
                                     rn.to_bytes(length=2, byteorder='big'))),
                                txn=self.transaction)
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
                                        length=2, byteorder='big'))),
                                txn=self.transaction)
                        else:
                            self.get_primary_segment_list().put(
                                srn_list,
                                b''.join(
                                    (rn.to_bytes(length=2, byteorder='big'),
                                     record_number.to_bytes(
                                        length=2, byteorder='big'))),
                                txn=self.transaction)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 b'\x00\x02',
                                 srn_list.to_bytes(4, byteorder='big'))),
                            DB_KEYLAST)
                return
            else:
                # No index entry for key yet because database empty
                cursor.put(
                    key,
                    b''.join(
                        (segment.to_bytes(4, byteorder='big'),
                         record_number.to_bytes(2, byteorder='big'))),
                    DB_KEYLAST)
        finally:
            cursor.close()


class Definition(definition.Definition):
    
    """Define method to create secondary database classes for bsddb3 interface.

    Actions are delegated to Definition superclass.

    """
    def make_secondary_class(self,
                             secondary_class,
                             dbset,
                             primary,
                             secondary,
                             specification,
                             database_instance=None,
                             **kw):
        return secondary_class(SUBFILE_DELIMITER.join((dbset, secondary)),
                               specification,
                               secondary,
                               database_instance,
                               **kw)


class Cursor(cursor.Cursor):
    
    """Wrap bsddb3 cursor with record encoding and decoding.

    The wrapped cursor is created on the Berkeley DB database in a File
    instance.

    The transaction argument in the Cursor() call should be a function
    which returns current tranasction active on the Berkeley DB environment, or
    None if there isn't one.  If supplied it's return value is used in all calls
    to methods of the wrapped cursor which have the 'txn' parameter.  By default
    the calls are not within a transaction.

    The CursorPrimary and CursorSecondary subclasses define the bsddb
    style cursor methods peculiar to primary and secondary databases.
    """

    def __init__(self, dbset, keyrange=None, transaction=None, **kargs):
        """Cursor wraps bsddb3 cursor with record encoding and decoding.

        dbset - A File instance.
        keyrange - Not used.
        transaction - A function which returns the current transaction.
        kargs - absorb argunents relevant to other database engines.

        The function provided by transaction will be a method of the Database
        class, which returns the current tranasction active on the Berkeley DB
        environment, or None if there isn't one.

        """
        super().__init__(dbset)
        self._transaction = transaction
        if dbset.table_connection_list is not None:
            self._cursor = dbset.table_link.cursor(txn=transaction)
        self._current_segment = None
        self._current_segment_number = None
        self._current_record_number_in_segment = None

    def close(self):
        """Delete database cursor then delegate to superclass close() method."""
        try:
            del self._dbset._clientcursors[self]
        except:
            pass
        super().close()
        #self._transaction = None

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial.encode()

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DBapiError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        # Berkeley DB uses a 'startswith(...)' technique
        return self._partial.encode()

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in Berkeley DB.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass


class CursorPrimary(Cursor):
    
    """Wrap bsddb3 primary database cursor with record encoding and decoding.

    The primary database must use the RECNO access method.

    The value is assumed to be a repr(<python object>).encode('utf8').
    
    """

    def count_records(self):
        """Return record count."""
        return self._dbset.table_link.stat(flags=DB_FAST_STAT,
                                           txn=self._transaction)['ndata']

    def first(self):
        """Return first record taking partial key into account."""
        return self._decode_record(self._cursor.first())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        ebd = self._dbset.get_existence_bits_database()
        try:
            segment, record_number = divmod(record[0],
                                            SegmentSize.db_segment_size)
            position = 0
            for i in range(segment):
                sebm = Bitarray()
                sebm.frombytes(ebd.get(i + 1, txn=self._transaction))
                position += sebm.count()
            sebm = Bitarray()
            sebm.frombytes(ebd.get(segment + 1, txn=self._transaction))
            try:
                position += sebm.search(SINGLEBIT).index(record_number)
            except ValueError:
                position += bisect.bisect_left(
                    record_number, sebm.search(SINGLEBIT))
            return position
        except:
            if record is None:
                return 0
            raise

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        ebd = self._dbset.get_existence_bits_database()
        count = 0
        abspos = abs(position)
        ebdc = ebd.cursor(txn=self._transaction)
        try:
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
                        (r[0] - 1) * SegmentSize.db_segment_size)
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
                        (r[0] - 1) * SegmentSize.db_segment_size)
                    ebdc.close()
                    return self._decode_record(self._cursor.set(recno))
        finally:
            ebdc.close()
        return None

    def last(self):
        """Return last record taking partial key into account."""
        return self._decode_record(self._cursor.last())

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        return self._decode_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account."""
        return self._decode_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account."""
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

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            k, v = record
            return k, v.decode()
        except:
            if record is None:
                return record
            raise

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DBapiError('_get_record not implemented')

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        #raise DBapiError('refresh_recordset not implemented')


class CursorSecondary(Cursor):
    
    """Wrap bsddb3 secondary database cursor with record encoding and decoding,
    and segment navigation.

    The secondary database must use the BTREE access method.

    The value part of (key, value) on secondary databases is either:

        primary key (segment and record number)
        reference to a list of primary keys for a segment
        reference to a bit map of primary keys for a segment

    The value is assumed to be a repr(<python object>).encode('utf8').

    Segment should be read as the DPT database engine usage.

    References are to records on RECNO databases, one each for lists and bit
    maps, containing the primary keys.

    The key part of (key, value) on secondary databases is calculated from the
    value of any of the referenced records in the associated primary database.
    
    """

    def __init__(self, *a, **k):#dbset, keyrange=None):
    
        """Delegate arguments to superclass and note segment databases."""

        super().__init__(*a, **k)#dbset, keyrange=keyrange)
        self._segment_bits = self._dbset.get_primary_segment_bits()
        self._segment_list = self._dbset.get_primary_segment_list()

    def count_records(self):
        """Return record count."""
        #if self.get_partial() is None:
        if self.get_partial() in (None, False):
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
        """Return first record taking partial key into account."""
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
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(value,
                                               SegmentSize.db_segment_size)
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
                        segment = RecordsetSegmentList(
                            segment_number,
                            None,
                            records=self._segment_list.get(
                                srn, txn=self._transaction))
                    elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                        srn = int.from_bytes(r[1][7:], byteorder='big')
                        segment = RecordsetSegmentBitarray(
                            segment_number,
                            None,
                            records=self._segment_bits.get(
                                srn, txn=self._transaction))
                    else:
                        segment = RecordsetSegmentInt(
                            segment_number,
                            None,
                            records=r[1][4:])
                    position += segment.get_position_of_record_number(
                        record_number)
                    break
            r = self._cursor.next()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        # Start at first or last record whichever is likely closer to position
        # and define lambdas to handle presence or absence of partial key.
        if not self.get_partial():
            get_partial = self.get_partial
        else:
            get_partial = self.get_converted_partial
        if position < 0:
            is_step_forward = False
            step = self._cursor.prev
            position = -1 - position
            if not self.get_partial():
                start = lambda partial: self._cursor.last()
            else:
                start = lambda partial: self._last_partial(partial)
        else:
            is_step_forward = True
            step = self._cursor.next
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
                    segment = RecordsetSegmentList(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=self._segment_list.get(srn,
                                                       txn=self._transaction))
                elif len(r[1]) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
                    srn = int.from_bytes(r[1][7:], byteorder='big')
                    segment = RecordsetSegmentBitarray(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=self._segment_bits.get(srn,
                                                       txn=self._transaction))
                else:
                    segment = RecordsetSegmentInt(
                        int.from_bytes(r[1][:4], byteorder='big'),
                        None,
                        records=r[1][4:])
                record_number = segment.get_record_number_at_position(
                    count, is_step_forward)
                if record_number is not None:
                    return r[0].decode(), record_number
                break
        return None

    def last(self):
        """Return last record taking partial key into account."""
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

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        try:
            k, v = self._set_range(key)
        except TypeError:
            return None
        return k.decode(), v

    def next(self):
        """Return next record taking partial key into account."""
        try:
            k, v = self._next()
        except TypeError:
            return None
        return k.decode(), v

    def prev(self):
        """Return previous record taking partial key into account."""
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

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DBapiError('_get_record not implemented')

    def set_current_segment(self, key, reference):
        """Return a RecordsetSegmentBitarray, RecordsetSegmentInt, or
        RecordsetSegmentList instance, depending on the current representation
        of the segment on the database."""
        segment_number = int.from_bytes(reference[:4], byteorder='big')
        if len(reference) == LENGTH_SEGMENT_LIST_REFERENCE:
            if self._current_segment_number == segment_number:
                #return self._current_segment
                if key == self._current_segment._key:
                    return self._current_segment
            record_number = int.from_bytes(reference[6:], byteorder='big')
            segment = RecordsetSegmentList(
                segment_number,
                key,
                records=self._segment_list.get(record_number,
                                               txn=self._transaction))
        elif len(reference) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            if self._current_segment_number == segment_number:
                #return self._current_segment
                if key == self._current_segment._key:
                    return self._current_segment
            record_number = int.from_bytes(reference[7:], byteorder='big')
            segment = RecordsetSegmentBitarray(
                segment_number,
                key,
                records=self._segment_bits.get(record_number,
                                               txn=self._transaction))
        else:
            segment = RecordsetSegmentInt(segment_number,
                                          key,
                                          records=reference[4:])
        self._current_segment = segment
        self._current_segment_number = segment_number
        return segment

    def _first(self):
        """Return first record taking partial key into account."""
        record = self._cursor.first()
        if record is None:
            return None
        return self.set_current_segment(*record).first()

    def _last(self):
        """Return last record taking partial key into account."""
        record = self._cursor.last()
        if record is None:
            return None
        return self.set_current_segment(*record).last()

    def _next(self):
        """Return next record taking partial key into account."""
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
        """Return previous record taking partial key into account."""
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
        segment, record_number = divmod(value, SegmentSize.db_segment_size)
        # Find the segment reference in secondary database
        cursor = self._dbset.table_link.cursor(txn=self._transaction)
        try:
            record = cursor.set_range(key)
            while record:
                if record[0] != key:
                    return None
                segment_number = int.from_bytes(record[1][:4], byteorder='big')
                if segment_number > segment:
                    return None
                if segment_number == segment:
                    break
                record = cursor.next()
            else:
                return None
        finally:
            cursor.close()
        # Check if record number is in segment
        ref = record[1]
        if len(ref) == LENGTH_SEGMENT_LIST_REFERENCE:
            srn = int.from_bytes(ref[6:], byteorder='big')
            segment = RecordsetSegmentList(
                segment_number,
                key,
                records=self._segment_list.get(srn, txn=self._transaction))
        elif len(ref) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(ref[7:], byteorder='big')
            segment = RecordsetSegmentBitarray(
                segment_number,
                key,
                records=self._segment_bits.get(srn, txn=self._transaction))
        else:
            segment = RecordsetSegmentInt(segment_number, key, records=ref[4:])
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
            segment = RecordsetSegmentList(
                segment_number,
                record[0],
                records=self._segment_list.get(srn, txn=self._transaction))
        elif len(ref) == LENGTH_SEGMENT_BITARRAY_REFERENCE:
            srn = int.from_bytes(ref[7:], byteorder='big')
            segment = RecordsetSegmentBitarray(
                segment_number,
                record[0],
                records=self._segment_bits.get(srn, txn=self._transaction))
        else:
            segment = RecordsetSegmentInt(segment_number,
                                          record[0],
                                          records=ref[4:])
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

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.
        # It seems not needed by this class.
        
        #raise DBapiError('refresh_recordset not implemented')


class RecordsetCursor(RecordsetCursor):
    
    """Add _get_record method and tranasction support to RecordsetCursor."""

    def __init__(self, recordset, database_instance=None, **kargs):
        """Note method which returns transaction identity and delegate
        recordset to superclass.

        kargs absorbs arguments relevant to other database engines.

        """
        super().__init__(recordset)
        self._transaction = database_instance.get_transaction

    @property
    def transaction(self):
        """Return the active transaction or None."""
        return self._transaction()

    # The _get_record hack in sqlite3bitdatasource.py becomes the correct way
    # to do this because the record has bsddb-specific decoding needs.
    def _get_record(self, record_number, use_cache=False):
        """Return (record_number, record) using cache if requested."""
        dbset = self._dbset
        if use_cache:
            record = dbset.record_cache.get(record_number)
            if record is not None:
                return record # maybe (record_number, record)
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return # maybe raise
        try:
            record = dbset._database.get(record_number,
                                         txn=self.transaction).decode()
        except AttributeError:
            # Assume get() returned None.
            record = None
        # maybe raise if record is None (if not, None should go on cache)
        if use_cache:
            dbset.record_cache[record_number] = record
            dbset.record.deque.append(record_number)
        return (record_number, record)

            
class Segment(segment.Segment):
    
    """Define a primary database with transaction support for lists or bitmaps
    of record numbers.

    There are three types of segment: existence bitmap, recordset bitmap, and
    recordset record number list.  Each is opened in a slightly different way
    so the relevant open_root method is defined in the subclasses.

    Record access methods are not defined at present, leaving the caller to
    do the bsddb3 calls needed.

    """

    def __init__(self, segment_type=None, file_reference=None, **kargs):
        """Define primary database for segment type for file_reference.
        
        segment_type is a suffix to the database name indicating the database
        is for existence bitmaps, recordset bitmaps, or recordset record number
        lists.

        file_reference is the File instance which provides the name of the
        primary database containing the records the segment referes to, and the
        method which returns the current transaction identity.
        
        """
        super().__init__()
        self._segment_file = (SUBFILE_DELIMITER * 2).join(
            (file_reference.get_database_file(), segment_type))
        self._transaction = file_reference._transaction

    @property
    def transaction(self):
        """Return the active transaction or None."""
        return self._transaction()

    def close(self):
        """Close inverted index DB."""
        try:
            self._segment_link.close()
        except:
            pass
        super().close()

    def _set_pre_open_parameters(self):
        pass

    def open_root(self, dbenv):
        """Create inverted index DB in dbenv."""
        try:
            self._segment_link = DB(dbenv)
        except:
            raise
        try:
            self._set_pre_open_parameters()
            self._segment_link.open(
                self._segment_file,
                self._segment_file,
                DB_RECNO,
                DB_CREATE,
                txn=self.transaction)
        except:
            self._segment_link = None
            raise

            
class ExistenceBitMap(Segment):
    
    """Define a primary database for existence bitmap segments.

    A count of segments is maintained.  (The thing of interest is the high
    segment number, which is assumed to be <number of segments> - 1. Perhaps
    segment_count is a very misleading name for the property.)

    The primary database (Recno access method) is configured to use fixed
    length values.
    """

    def __init__(self, **k):
        """Delegate arguments to superclass and say this is an 'exist', and
        create placeholder for count of segments.
        """
        super().__init__(segment_type='exist', **k)
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

    def _set_pre_open_parameters(self):
        self._segment_link.set_re_pad(0)
        self._segment_link.set_re_len(SegmentSize.db_segment_size_bytes)
    
    def open_root(self, dbenv):
        """Create inverted index DB in dbenv."""
        super().open_root(dbenv)
        self._segment_count = self._segment_link.stat(
            flags=DB_FAST_STAT, txn=self.transaction)['ndata']

            
class SegmentList(Segment):
    
    """Define a primary database for record number list segments.

    The values in this database will be variable length.
    """

    def __init__(self, **k):
        """Delegate arguments to superclass and say this is a 'list'.
        """
        super().__init__(segment_type='list', **k)

            
class SegmentBitarray(Segment):
    
    """Define a primary database for bitarray segments.

    The primary database (Recno access method) is configured to use fixed
    length values.
    """

    def __init__(self, **k):
        """Delegate arguments to superclass and say this is a 'bits'.
        """
        super().__init__(segment_type='bits', **k)

    def _set_pre_open_parameters(self):
        self._segment_link.set_re_pad(0)
        self._segment_link.set_re_len(SegmentSize.db_segment_size_bytes)

            
class ControlFile(controlfile.ControlFile):
    
    """Define a Berkeley DB database to hold the keys of existence bitmap
    segments which contain unset bits corresponding to deleted records in a
    primary database.

    The keys in this database are the names of the primary databases specified
    in the FileSpec instance for the application.

    The access method of this database, which is not in a 'primary database,
    secondary database' relationship, is DB_BTREE NODUP.

    This database is used by FileControl instances to find record numbers which
    can be re-used rather than add a new record number at the end of a database
    without need.
    """

    def __init__(self, database_instance=None, **kargs):
        """Define database named database_instance._control_file with
        transactions managed by database_instance.

        database_instance also provides the method that returns the current
        transaction identity.
        
        """
        super().__init__()
        self._control_file = database_instance._control_file
        self._control_link = None
        self._transaction = database_instance.get_transaction

    @property
    def transaction(self):
        """Return the active transaction or None."""
        return self._transaction()

    def open_root(self, dbenv):
        """Create file control database in environment."""
        try:
            self._control_link = DB(dbenv)
        except:
            raise
        try:
            self._control_link.set_flags(DB_DUPSORT)
            self._control_link.open(
                self._control_file,
                self._control_file,
                DB_BTREE,
                DB_CREATE,
                txn=self.transaction)
        except:
            self._control_link = None
            raise

    def close(self):
        """Close file control database."""
        if self._control_link is not None:
            self._control_link.close()
            self._control_link = None

    def get_control_database(self):
        """Return the database containing file control records."""
        return self._control_link

    @property
    def control_file(self):
        """Return the name which is both primary column and table name."""
        return self._control_file


class FileControl:
    
    """Base class for managing dbapi.Segment subclass databases.

    Note the primary or secondary database instance to be managed.

    Subclasses implement the management.
    """

    def __init__(self, dbfile):
        """Note dbfile as primary or secondary database instance to be managed.
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
            cursor = self._dbfile.get_control_database(
                ).cursor(txn=self._dbfile.transaction)
            try:
                record = cursor.set(self._ebmkey)
                while record:
                    self._freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder='big'))
                    record = cursor.next_dup()
            finally:
                cursor.close()
        insert = bisect.bisect_left(self._freed_record_number_pages, segment)
        # Should be:
        # if insert <= len(self._freed_record_number_pages):
        # Leave as it is until dbapi tests give same results as sqlite3 tests,
        # which have the same problem.
        if self._freed_record_number_pages:
            if insert < len(self._freed_record_number_pages):
                if self._freed_record_number_pages[insert] == segment:
                    return
        self._freed_record_number_pages.insert(insert, segment)
        self._dbfile.get_control_database().put(
            self._ebmkey,
            segment.to_bytes(1 + segment.bit_length() // 8, byteorder='big'),
            flags=DB_NODUPDATA,
            txn=self._dbfile.transaction)

    def get_lowest_freed_record_number(self):
        """Return low record number in segments with freed record numbers."""
        if self._freed_record_number_pages is None:
            self._freed_record_number_pages = []
            cursor = self._dbfile.get_control_database(
                ).cursor(txn=self._dbfile.transaction)
            try:
                record = cursor.set(self._ebmkey)
                while record:
                    self._freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder='big'))
                    record = cursor.next_dup()
            finally:
                cursor.close()
        while len(self._freed_record_number_pages):
            s = self._freed_record_number_pages[0]
            lfrns = self._read_exists_segment(s)
            if lfrns is None:
                # Do not reuse record number on segment of high record number
                return 0
            try:
                first_zero_bit = lfrns.index(False, 0 if s else 1)
            except ValueError:
                cursor = self._dbfile.get_control_database(
                    ).cursor(txn=self._dbfile.transaction)
                try:
                    if cursor.set_both(
                        self._ebmkey,
                        s.to_bytes(1 + s.bit_length() // 8, byteorder='big')):
                        cursor.delete()
                    else:
                        raise
                finally:
                    cursor.close()
                del self._freed_record_number_pages[0]
                continue
            return s * SegmentSize.db_segment_size + first_zero_bit
        else:
            return 0 # record number when inserting into RECNO database

    def _read_exists_segment(self, segment_number):
        """Return existence bit map for segment_number if not high segment."""
        # record keys are 1-based but segment_numbers are 0-based
        if segment_number < self._dbfile.get_existence_bits().segment_count - 1:
            ebm = Bitarray()
            ebm.frombytes(
                self._dbfile.get_existence_bits_database(
                    ).get(segment_number + 1,
                          txn=self._dbfile.transaction))
            return ebm
        return None


class FileControlSecondary(FileControl):
    
    """Keep track of freed record numbers on the 'record number bitmap' and
    'record number list' databases for each primary database.

    These record numbers can be freed when records are deleted from the primary
    database or when an inverted list for a key is moved between bitmap and
    list representations.

    The goal is to reuse records in the 'list' and 'bits' databases (see
    SegmentList and SegmentBitarray classes) if possible rather than add
    new records at the end.
    """

    def __init__(self, *args):
        """Delegate arguments to superclass and create placeholders for lists
        of freed 'list' and 'bits' records.
        """
        super().__init__(*args)
        self._freed_list_pages = None
        self._freed_bits_pages = None
        self._listkey = self._dbfile.encode_record_selector(
            'L' + self._dbfile._keyvalueset_name)
        self._bitskey = self._dbfile.encode_record_selector(
            'B' + self._dbfile._keyvalueset_name)

    @property
    def freed_list_pages(self):
        """List pages available for re-use."""
        return self._freed_list_pages

    @property
    def freed_bits_pages(self):
        """Bit Map pages available for re-use."""
        return self._freed_bits_pages

    def note_freed_bits_page(self, page_number):
        """Add page_number to freed bits pages."""
        self._put_bits_page_number(page_number)

    def note_freed_list_page(self, page_number):
        """Add page_number to freed list pages."""
        self._put_list_page_number(page_number)

    def get_freed_bits_page(self):
        """Return low page from freed bits pages."""
        if self._freed_bits_pages is False:
            return 0 # record number when inserting into RECNO database
        return self._get_bits_page_number()

    def get_freed_list_page(self):
        """Return low page from freed list pages."""
        if self._freed_list_pages is False:
            return 0 # record number when inserting into RECNO database
        return self._get_list_page_number()

    def _put_bits_page_number(self, page):
        """Put page on freed bits page record."""
        try:
            self._dbfile.get_control_database().put(
                self._bitskey,
                page.to_bytes(1 + page.bit_length() // 8, byteorder='big'),
                flags=DB_NODUPDATA,
                txn=self._dbfile.transaction)
        except DBKeyExistError:
            # Assume callers do not check if page is already marked free.
            pass
        self._freed_bits_pages = True

    def _put_list_page_number(self, page):
        """Put page on freed list page record."""
        try:
            self._dbfile.get_control_database().put(
                self._listkey,
                page.to_bytes(1 + page.bit_length() // 8, byteorder='big'),
                flags=DB_NODUPDATA,
                txn=self._dbfile.transaction)
        except DBKeyExistError:
            # Assume callers do not check if page is already marked free.
            pass
        self._freed_list_pages = True

    def _get_bits_page_number(self):
        """Pop low page from freed bits page record."""
        cursor = self._dbfile.get_control_database(
            ).cursor(txn=self._dbfile.transaction)
        try:
            record = cursor.set(self._bitskey)
            if record:
                cursor.delete()
                self._freed_bits_pages = False
                return int.from_bytes(record[1], byteorder='big')
        finally:
            cursor.close()
        self._freed_bits_pages = False
        return 0 # record number when inserting into RECNO database

    def _get_list_page_number(self):
        """Pop low page from freed list page record."""
        cursor = self._dbfile.get_control_database(
            ).cursor(txn=self._dbfile.transaction)
        try:
            record = cursor.set(self._listkey)
            if record:
                cursor.delete()
                self._freed_list_pages = False
                return int.from_bytes(record[1], byteorder='big')
        finally:
            cursor.close()
        self._freed_list_pages = False
        return 0 # record number when inserting into RECNO database
