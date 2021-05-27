# database.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the database interface.

Subclasses will provided appropriate implementations for the record definition
classes to use.

"""
import os

from .constants import (
    FIELDS, PRIMARY, SECONDARY, SUBFILE_DELIMITER,
    DDNAME, FOLDER, FILE,
    )
from .segmentsize import SegmentSize
from .recordset import Recordset
from .find import Find
from .where import Where
from .findvalues import FindValues
from .wherevalues import WhereValues


class DatabaseError(Exception):
    pass


class Database:
    
    """Define file and record access methods which subclasses may override if
    necessary.
    """

    def __init__(self,
                 database_specification,
                 databasefolder,
                 **kwargs):
        super().__init__()
        try:
            self._home_directory = os.path.abspath(databasefolder)
        except:
            msg = ' '.join(['Database folder name', str(databasefolder),
                            'is not valid'])
            raise DatabaseError(msg)

        if not isinstance(database_specification, dict):
            raise DatabaseError('Database specification must be a dictionary')
        
        definitions = set()
        pathnames = dict()
        for name, specification in database_specification.items():
            if not isinstance(specification, dict):
                msg = ' '.join(
                    ['Specification for', repr(name),
                     'must be a dictionary'])
                raise DatabaseError(msg)
            if PRIMARY not in specification:
                msg = ' '.join(['Specification for', repr(name),
                                'must contain a primary name'])
                raise DatabaseError(msg)
            primary = specification[PRIMARY]
            if SUBFILE_DELIMITER in primary:
                raise DatabaseError(''.join(
                    ('Primary name ',
                     primary,
                     " contains '",
                     SUBFILE_DELIMITER,
                     "', which is not allowed",
                     )))
            if primary in definitions:
                msg = ' '.join(['Primary name', primary,
                                'for', name,
                                'already used'])
                raise DatabaseError(msg)
            if SECONDARY in specification:
                for k, v in specification[SECONDARY].items():
                    if v is None:
                        if k.lower() != primary.lower():
                            continue
                    elif v.lower() != primary.lower():
                        continue
                    msg = ' '.join(['Primary name', primary,
                                    'for', name,
                                    'must not be in secondary definition',
                                    '(ignoring case)'])
                    raise DatabaseError(msg)
            if FIELDS not in specification:
                msg = ' '.join(['Field definitions must be present in',
                                'specification for primary fields'])
                raise DatabaseError(msg)
            if primary not in specification[FIELDS]:
                msg = ' '.join(['Primary name', primary,
                                'for', name,
                                'must be in fields definition'])
                raise DatabaseError(msg)
            try:
                ddname = specification[DDNAME]
            except KeyError:
                msg = ' '.join(['Specification for', name,
                                'must have a DD name'])
                raise DatabaseError(msg)
            if len(ddname) == 0:
                msg = ' '.join(['DD name', repr(ddname),
                                'for', name,
                                'is zero length'])
                raise DatabaseError(msg)
            elif len(ddname) > 8:
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'is over 8 characters'])
                raise DatabaseError(msg)
            elif not ddname.isalnum():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise DatabaseError(msg)
            elif not ddname.isupper():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise DatabaseError(msg)
            elif not ddname[0].isupper():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise DatabaseError(msg)
            else:
                folder = specification.get(FOLDER, None)
                filename = specification.get(FILE, None)
                if folder is None:
                    folder = self._home_directory
                try:
                    folder = os.path.abspath(folder)

                    # At Python26+ need to convert unicode to str for DPT.
                    fname = str(os.path.join(folder, filename))

                except:
                    msg = ' '.join(
                        ['Full path name of DPT file for', name,
                         'is invalid'])
                    raise DatabaseError(msg)
                if fname in pathnames:
                    msg = ' '.join(['File name', os.path.basename(fname),
                                    'linked to', pathnames[fname],
                                    'cannot link to', name])
                    raise DatabaseError(msg)
                pathnames[fname] = name

            definitions.add(primary)
        
        self._dbspec = database_specification
        
        # The database connection
        # apsw (SQLite3):    Connection instance (apsw)
        # Berkeley DB:       DBEnv instance
        # DPT:               APIDatabaseServices instance
        # sqlite3 (SQLite3): Connection instance (sqlite3)
        self._dbservices = None
        
        # The operating system file containing the database if all things are
        # put in one operating system file.
        # Things means Tables in SQLite, Databases in Berkeley DB, and Files in
        # DPT; but each supports accessing values by key. 
        self._home = os.path.join(self._home_directory,
                                  os.path.split(self._home_directory)[-1])

    @property
    def dbservices(self):
        """Return the database connection.

        The type of object returned depends on the database engine:
            apsw (SQLite3)     Connection instance (apsw)
            Berkeley DB        DBEnv instance (bsddb3)
            DPT                APIDatabaseServices instance (dptapi)
            sqlite3 (SQLite3)  Connection instance (sqlite3)
        
        """
        return self._dbservices
    
    def backout(self):
        """return None if implemented."""
        raise DatabaseError('backout not implemented')

    def close_context(self):
        """return None if implemented."""
        raise DatabaseError('close_context not implemented')

    def close_database(self):
        """return None if implemented."""
        raise DatabaseError('close_database not implemented')

    def commit(self):
        """return None if implemented."""
        raise DatabaseError('commit not implemented')

    def db_compatibility_hack(self, record, srkey):
        """return None if implemented."""
        raise DatabaseError('db_compatibility_hack not implemented')

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.
        
        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.
        
        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        main = self.database_definition
        primarydb = main[dbset].primary
        db = main[dbset].secondary
        lookup = main[dbset].dbname_to_secondary_key

        high_record = primarydb.get_high_record()
        primarydb.delete(deletekey, instance.srvalue)
        instance.srkey = self.encode_record_number(deletekey)

        srindex = instance.srindex
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        primarydb.segment_delete(segment, record_number)
        dcb = instance._deletecallbacks
        for secondary in srindex:
            lusec = lookup[secondary]
            if lusec not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                db[lusec].segment_delete(v, segment, record_number)

        # Call note_freed_record_number() in primarydb.delete() instead?
        try:
            high_segment = divmod(high_record[0],
                                  SegmentSize.db_segment_size)[0]
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
        
        main = self.database_definition
        primarydb = main[dbset].primary
        db = main[dbset].secondary
        lookup = main[dbset].dbname_to_secondary_key

        srindex = instance.srindex
        nsrindex = instance.newrecord.srindex
        dcb = instance._deletecallbacks
        ndcb = instance.newrecord._deletecallbacks
        pcb = instance._putcallbacks
        npcb = instance.newrecord._putcallbacks

        # Changing oldkey to newkey should not be allowed
        old_segment, old_record_number = divmod(oldkey,
                                                SegmentSize.db_segment_size)
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
            primarydb.delete(oldkey, instance.srvalue)
            key = primarydb.put(newkey, instance.newrecord.srvalue)
            if key is not None:
                # put was append to record number database and
                # returned the new primary key. Adjust record key
                # for secondary updates.
                instance.newrecord.key.load(key)
                newkey = key
                new_segment, new_record_number = divmod(
                    newkey, SegmentSize.db_segment_size)
            primarydb.segment_delete(old_segment, old_record_number)
            primarydb.segment_put(new_segment, new_record_number)
        elif instance.srvalue != instance.newrecord.srvalue:
            primarydb.replace(
                oldkey,
                instance.srvalue,
                instance.newrecord.srvalue)
        
        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)

        for secondary in ionly:
            lusec = lookup[secondary]
            if lusec not in db:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                db[lusec].segment_delete(
                    v, old_segment, old_record_number)

        for secondary in nionly:
            lusec = lookup[secondary]
            if lusec not in db:
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            for v in nsrindex[secondary]:
                db[lusec].segment_put(
                    v, new_segment, new_record_number)

        for secondary in iandni:
            lusec = lookup[secondary]
            if lusec not in db:
                if srindex[secondary] == nsrindex[secondary]:
                    if oldkey == newkey:
                        continue
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                if secondary in npcb:
                    npcb[secondary](
                        instance.newrecord, nsrindex[secondary])
                continue
            srset = set(srindex[secondary])
            nsrset = set(nsrindex[secondary])
            if oldkey == newkey:
                for v in sorted(srset - nsrset):
                    db[lusec].segment_delete(
                        v, old_segment, old_record_number)
                for v in sorted(nsrset - srset):
                    db[lusec].segment_put(
                        v, new_segment, new_record_number)
            else:
                for v in srset:
                    db[lusec].segment_delete(
                        v, old_segment, old_record_number)
                for v in nsrset:
                    db[lusec].segment_put(
                        v, new_segment, new_record_number)

    def get_database_folder(self):
        """Return database folder name."""
        return self._home_directory

    def get_database_home(self):
        """Return file name of database if implemented in a single file.

        SQLite3 puts all tables in a database in one file.
        Berkeley DB can put each DB of a database in a separate file or put
        all of them in one file.
        DPT puts all DPT files in separate files.

        """
        return self._home

    def get_database(self, dbset, dbname):
        """return database object if implemented."""
        raise DatabaseError('get_database not implemented')

    def is_recno(self, dbset, dbname):
        """return True or False if implemented."""
        raise DatabaseError('is_recno not implemented')

    def open_context(self):
        """return True if database is opened if implemented."""
        raise DatabaseError('open_context not implemented')

    def get_packed_key(self, dbset, instance):
        """return key if implemented.
        
        self may derive key from instance.key or call
        instance.key.pack() to derive key.
        
        """
        raise DatabaseError('get_packed_key not implemented')

    def decode_as_primary_key(self, dbset, srkey):
        """return key if implemented.

        self derives the primary key from srkey.

        """
        raise DatabaseError('decode_as_primary_key not implemented')

    def encode_primary_key(self, dbset, instance):
        """return string representation of key if implemented.
        
        self derives string representation of instance.key
        probably using instance.key.pack().
        
        """
        raise DatabaseError('encode_primary_key not implemented')

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are integer primary key.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        main = self.database_definition
        primarydb = main[dbset].primary
        db = main[dbset].secondary
        lookup = main[dbset].dbname_to_secondary_key

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
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        primarydb.segment_put(segment, record_number)
        pcb = instance._putcallbacks
        for secondary in srindex:
            lusec = lookup[secondary]
            if lusec not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                db[lusec].segment_put(v, segment, record_number)

    def start_transaction(self):
        """return None if implemented.

        Named start_transaction rather than begin_transaction to avoid implying
        adherence to the DB API 2.0 specification (PEP 249).

        The transaction methods were introduced to support the DPT interface,
        which happens to be similar to DB API 2.0 in some respects.

        Transactions are started automatically, but there is no way of starting
        a transaction by decree (Sqlite 'begin' command).  The DPT interface is
        forced into compliance with DB API 2.0 here.

        Exceptions are backed out (sqlite rollback) in DPT, but the default
        way of ending a transaction is commit rather than backout (sqlite
        rollback).  This is exactly opposite to DB API 2.0.

        This method is provided to support the apsw interface to Sqlite3, which
        is intentionally not compliant with DB API 2.0.  Instead apsw aims to
        be the thinnest possible wrapper of the Sqlite3 API as an alternative
        to the API provided by Python's sqlite3 module.

        """

        # Introduced to use the apsw interface to Sqlite3 and remain compatible
        # with the sqlite3, bsddb3, and dptdb interfaces.
        # The commit() method was added to support dptdb which always starts
        # transactions implicitely, like the DB2 API used by the sqlite3 module
        # distributed with Python.  Transactions are not used in the bsddb3
        # interface.  Thus an explicit 'start transaction' method was never
        # defined.  Explicit transactions must be used in the apsw interface to
        # avoid the automatic 'one statement' transactions that would otherwise
        # occur, because solentware_base assumes transactions persist until
        # explicitly committed or backed out (DPT term for rollback).
        raise DatabaseError('start_transaction not implemented')

    def make_root(self, *a, **kw):
        """Return None if implemented."""
        raise DatabaseError('make_root not implemented')

    def deferred_update_housekeeping(self):
        """Do nothing.  Subclasses should override this method as required.

        Actions are specific to a database engine.
        
        """

    @property
    def database_specification(self):
        """Return validated database specification with defaults added."""
        return self._dbspec
        
    @property
    def database_definition(self):
        """Return database definition built from database specification.

        The objects used to open and access a database.

        The underlying _dbdef attribute is bound in a subclass of Database
        because the details are specific to each database engine.

        """
        return self._dbdef

    # Override in _sqlite.py and dbapi.py
    def get_database_filenames(self):
        """Raise not implemented."""
        raise DatabaseError('get_database_filenames not implemented')

    # Override in dptbase.py
    def close_contexts(self, close_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    # Override in dptbase.py
    def exists(self, dbset, dbname):
        """Return True if dbname is a primary or secondary DB in dbset."""
        s = self._dbspec[dbset]
        if dbname == self._dbdef[dbset]._dbset:
            return True
        return dbname in s[SECONDARY]

    # Override in dptbase.py
    def database_cursor(self, dbset, dbname, keyrange=None):
        """Create and return a cursor on DB dbname in dbset.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        s = self.database_definition[dbset].associate(dbname)
        return s.make_cursor(s, keyrange)

    # Override in dptbase.py
    def repair_cursor(self, cursor, *a):
        """Return cursor for compatibility with DPT which returns a new one."""
        return cursor

    # Override in dptbase.py
    def get_database_instance(self, dbset, dbname):
        """Return DB instance for dbname in dbset."""
        return self.database_definition[dbset].associate(dbname)

    # Override in dptbase.py
    def get_first_primary_key_for_index_key(self, dbset, dbname, key):
        """Return first primary key for secondary key in dbname for dbname.

        Consider restricting use of this method to secondary DBs whose keys
        each have a unique value.
        
        """
        return self.database_definition[dbset].associate(
            dbname).get_first_primary_key_for_index_key(key)

    # Override in dptbase.py
    def get_primary_record(self, dbset, key):
        """Return primary record (key, value) given primary key on dbset."""
        return self.database_definition[dbset].associate(
            dbset).get_primary_record(key)

    # Override in dptbase.py
    def get_table_index(self, dbset, dbname):
        """Return table index for dbname in dbset."""
        s = self._dbspec[dbset]
        if dbname == dbset:
            n = s[PRIMARY]
        else:
            n = s[SECONDARY].get(dbname, dbname)
        return n

    # Override in dptbase.py
    def get_associated_indicies(self, dbset):
        """Return frozenset of all table indicies for dbset."""
        s = self.database_definition[dbset]
        return frozenset(set((s._dbset,)).union(s.secondary))

    # Override in dptbase.py
    def get_table_indicies(self):
        """Return frozenset of all table indicies."""
        return frozenset(self._dbdef)

    # Override in dptbase.py
    def is_primary(self, dbset, dbname):
        """Return True if dbname is primary database in dbset."""
        return self.database_definition[dbset].associate(dbname).is_primary()

    def is_primary_recno(self, dbset):
        """Return True if primary DB in dbset is RECNO.

        Primary DB is assumed to be RECNO, so return True.

        It is possible to override the open_root() method in the class passed
        to DBapi() as primary_class and not use RECNO, but things should soon
        fall apart if so.

        """
        #return self.database_definition[dbset
        #                                ].associate(dbset).is_primary_recno()
        return True

    # Override in dptbase.py
    def open_contexts(self, closed_contexts):
        """Do nothing, present for DPT compatibility."""
        pass

    # Override in dptbase.py
    def set_defer_update(self, db=None, duallowed=False):
        """Close files before doing deferred updates.

        Replace the original Berkeley DB version with a DPT look-alike.
        It is the same code but implementation of close_context ie different
        because the database engines are different.  Most of the code in the
        earlier set_defer_update will move to the subprocess.
        
        """
        self.close_context()
        return duallowed

    # Override in dptbase.py
    def unset_defer_update(self, db=None):
        """Unset deferred update for db DBs. Default all."""
        # Original method moved to dbduapi.py
        return self.open_context()

    # Override if needed.
    def use_deferred_update_process(self, **kargs):
        """Return module name or None if implemented.

        **kargs - soak up any arguments other database engines need.

        """
        raise DatabaseError('use_deferred_update_process not implemented')

    # Override in dptbase.py
    def initial_database_size(self):
        """Do nothing and return True as method exists for DPT compatibility."""
        return True

    # Override in dptbase.py
    def increase_database_size(self, **ka):
        """Do nothing because method exists for DPT compatibility."""

    # Override in dptbase.py
    def make_recordset_key_like(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset containing database records with keys like key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self.database_definition[dbset].associate(
            dbname).populate_recordset_key_like(rs, key)
        return rs

    # Override in dptbase.py
    def make_recordset_key(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self.database_definition[dbset].associate(
            dbname).populate_recordset_key(rs, key)
        return rs

    # Override in dptbase.py
    def make_recordset_key_startswith(
        self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self.database_definition[dbset].associate(
            dbname).populate_recordset_key_startswith(rs, key)
        return rs

    # Override in dptbase.py
    def make_recordset_key_range(
        self, dbset, dbname, key=None, keyend=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self.database_definition[dbset].associate(
            dbname).populate_recordset_key_range(rs, key, keyend)
        return rs

    # Override in dptbase.py
    def make_recordset_all(self, dbset, dbname, key=None, cache_size=1):
        """Return recordset on database containing records for key."""
        rs = Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
        self.database_definition[dbset].associate(
            dbname).populate_recordset_all(rs)
        return rs

    # Override in dptbase.py
    def make_recordset(self, dbset, cache_size=1):
        """Return empty recordset on database."""
        return Recordset(dbhome=self, dbset=dbset, cache_size=cache_size)
    
    # Override in dptbase.py
    def file_records_under(self, dbset, dbname, recordset, key):
        """File recordset under key in dbname if created from dbset in self."""
        if recordset.dbidentity != id(self.get_database(dbset, dbset)):
            raise DatabaseError(
                'Record set was not created from this database instance')
        if recordset.dbset != dbset:
            raise DatabaseError(
                'Record set was not created from dbset database')
        self.database_definition[dbset].associate(
            dbname).file_records_under(recordset, key)
    
    # Override in dptbase.py
    def unfile_records_under(self, dbset, dbname, key):
        """Forget recordset filed under key in dbname."""
        self.database_definition[dbset].associate(
            dbname).unfile_records_under(key)

    # Override in dptbase.py
    def record_finder(self, dbset, recordclass=None):
        """Return an instance of solentware_base.api.find.Find class."""
        return Find(self, dbset, recordclass=recordclass)

    # Override in dptbase.py
    def record_selector(self, statement):
        """Return an instance of solentware_base.api.where.Where class."""
        return Where(statement)

    # Override in dptbase.py
    def values_finder(self, dbset):
        """Return an instance of solentware_base.api.findvalues.FindValues
        class."""
        return FindValues(self, dbset)

    # Override in dptbase.py
    def values_selector(self, statement):
        """Return an instance of solentware_base.api.wherevalues.WhereValues
        class."""
        return WhereValues(statement)

    def make_connection(self):
        """Connect to an SQLite3 database with sqlite3 module."""
        raise DatabaseError(
            'make_connection not implemented: specific to Sqlite3 interfaces')
    
    def get_transaction(self):
        """Return object created by an earlier DBEnv.txn_begin() call or None
        if a DBEnv.abort() or DBEnv.commit() call is more recent."""
        raise DatabaseError(
            ''.join(('get_transaction not implemented: specific to Berkeley ',
                     'DB interfaces')))

    # These, raising DatabaseError, added to satisfy 'same_interface_test'.
    
    # Override in dptbase.py
    def create_recordsetlist_cursor(self, dbset, dbname, keyrange, recordset):
        """Raise not implemented."""
        raise DatabaseError('create_recordsetlist_cursor not implemented')
        
    # Override in dptbase.py
    def get_parms(self):
        """Raise not implemented."""
        raise DatabaseError('get_parms not implemented')

    # Override in dptbase.py
    def get_database_increase(self, files=None):
        """Raise not implemented."""
        raise DatabaseError('get_database_increase not implemented')

    # Override in dptbase.py
    def get_database_parameters(self, files=None):
        """Raise not implemented."""
        raise DatabaseError('get_database_parameters not implemented')

    # Override in dptbase.py
    def open_context_allocated(self, files=()):
        """Raise not implemented."""
        raise DatabaseError('open_context_allocated not implemented')

    # Override in dptbase.py
    def open_context_normal(self, files=()):
        """Raise not implemented."""
        raise DatabaseError('open_context_normal not implemented')

    # Override in dptbase.py
    def get_sfserv(self):
        """Raise not implemented."""
        raise DatabaseError('get_sfserv not implemented')
        
    # Override in dptbase.py
    def get_dptsysfolder(self):
        """Raise not implemented."""
        raise DatabaseError('get_dptsysfolder not implemented')

    # Override in dptbase.py
    def create_default_parms(self):
        """Raise not implemented."""
        raise DatabaseError('create_default_parms not implemented')
