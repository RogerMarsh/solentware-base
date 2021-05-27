# database.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the database interface.

Subclasses will provided appropriate implementations for the record definition
classes to use.

List of classes

DatabaseError - Exceptions
Database - File and record access methods that subclasses must provide
Cursor - Cursor methods that subclasses must provide

"""

from .constants import USE_GIVEN, USE_BYTES, USE_STR, DB_SEGMENT_SIZE
from .recordset import (
    Recordset,
    SegmentBitarray,
    SegmentInt,
    SegmentList,
    EMPTY_BITARRAY,
    )


class DatabaseError(Exception):
    pass


class Database(object):
    
    """Define file and record access methods that subclasses must provide.

    Methods added:

    backout
    close_context
    close_database
    commit
    database_cursor
    db_compatibility_hack
    decode_as_primary_key
    deferred_update_housekeeping
    delete_instance
    edit_instance
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
    put_instance
    start_transaction
    use_deferred_update_process

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    # Database engine copes with both, so use value as given by application.
    engine_uses_bytes_or_str = USE_GIVEN

    def __init__(self):
        super(Database, self).__init__()
    
    def backout(self):
        """return None"""
        raise DatabaseError('backout not implemented')

    def close_context(self):
        """return None"""
        raise DatabaseError('close_context not implemented')

    def close_database(self):
        """return None"""
        raise DatabaseError('close_database not implemented')

    def commit(self):
        """return None"""
        raise DatabaseError('commit not implemented')

    def db_compatibility_hack(self, record, srkey):
        """return None"""
        raise DatabaseError('db_compatibility_hack not implemented')

    def delete_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError('delete_instance not implemented')

    def edit_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError('edit_instance not implemented')

    def exists(self, dbset, dbname):
        """return True or False"""
        raise DatabaseError('exists not implemented')

    def database_cursor(self, dbname):
        """return a cursor or None"""
        raise DatabaseError('database_cursor not implemented')

    def get_database_folder(self):
        """return database folder name"""
        raise DatabaseError('get_database_folder not implemented')

    def get_database(self, dbname):
        """return database object"""
        raise DatabaseError('get_database not implemented')

    def get_first_primary_key_for_index_key(self, dbset, dbname, key):
        """return primary key as held on dbname or None"""
        raise DatabaseError((
            'get_first_primary_key_for_index_key not implemented'))

    def get_primary_record(self, dbname, key):
        """return (primary key, value) or None
        
        key is a number or a string. value is a string.
        dbname must be the primary DB (Berkeley DB term).
        
        """
        raise DatabaseError('get_primary_record not implemented')

    def is_primary(self, dbname):
        """return True or False"""
        raise DatabaseError('is_primary not implemented')

    def is_primary_recno(self, dbname):
        """return True or False"""
        raise DatabaseError('is_primary_recno not implemented')

    def is_recno(self, dbname):
        """return True or False"""
        raise DatabaseError('is_recno not implemented')

    def open_context(self):
        """return True if database is opened"""
        raise DatabaseError('open_context not implemented')

    def get_packed_key(self, dbname, instance):
        """return key
        
        self may derive key from instance.key or call
        instance.key.pack() to derive key.
        
        """
        raise DatabaseError('get_packed_key not implemented')

    def decode_as_primary_key(self, dbname, srkey):
        """return key

        self derives the primary key from srkey.

        """
        raise DatabaseError('decode_as_primary_key not implemented')

    def encode_primary_key(self, dbname, instance):
        """return string representation of key
        
        self derives string representation of instance.key
        probably using instance.key.pack().
        
        """
        raise DatabaseError('encode_primary_key not implemented')

    def put_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError('put_instance not implemented')

    def use_deferred_update_process(self, **kargs):
        """Return module name or None"""
        raise DatabaseError('use_deferred_update_process not implemented')

    def start_transaction(self):
        """return None

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
        # occur, because basesup assumes transactions persist until explicitly
        # committed or backed out (DPT term for rollback).
        raise DatabaseError('start_transaction not implemented')

    def deferred_update_housekeeping(self):
        """Do nothing.  Subclasses should override this method as required.

        Actions are specific to a database engine.
        
        """


class Cursor(object):
    """Define cursor access methods that subclasses must provide.

    These methods will be implemented using the cursor methods of the
    underlying database engine.  Subclasses may also provide methods
    with names matching those of the bsddb interface (typically first
    corresponding to first and so on).
    
    Methods added:

    close
    count_records
    database_cursor_exists
    __del__
    first
    get_converted_partial
    get_converted_partial_with_wildcard
    get_partial
    get_partial_with_wildcard
    get_position_of_record
    get_record_at_position
    last
    nearest
    next
    prev
    refresh_recordset
    setat
    set_partial_key
    
    Methods overridden:

    None
    
    Methods extended:

    __init__
    
    bsddb methods that may be provided by subclasses:

    close
    count
    current
    first
    last
    next
    prev
    set
    set_both
    set_range

    """

    def __init__(self, dbset):
        """Define a cursor on the underlying database engine"""
        super(Cursor, self).__init__()
        self._cursor = None
        self._dbset = dbset
        self._partial = None

    def close(self):
        """return None"""
        raise DatabaseError('close not implemented')

    def __del__(self):
        """Call the instance close() method."""
        self.close()

    def count_records(self):
        """return record count or None"""
        raise DatabaseError('count_records not implemented')

    def database_cursor_exists(self):
        """return True if cursor exists or False"""
        raise DatabaseError('database_cursor_exists not implemented')

    def first(self):
        """return (key, value) or None"""
        raise DatabaseError('first not implemented')

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)"""
        raise DatabaseError('get_position_of_record not implemented')

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        raise DatabaseError('get_record_at_position not implemented')

    def last(self):
        """return (key, value) or None"""
        raise DatabaseError('last not implemented')

    def nearest(self, key):
        """return (key, value) or None"""
        raise DatabaseError('nearest not implemented')

    def next(self):
        """return (key, value) or None"""
        raise DatabaseError('next not implemented')

    def prev(self):
        """return (key, value) or None"""
        raise DatabaseError('prev not implemented')

    def refresh_recordset(self):
        """Amend cursor data structures on database update and return None

        It may be correct to do nothing.

        """
        raise DatabaseError('refresh_recordset not implemented')

    def setat(self, record):
        """return (key, value) or None"""
        raise DatabaseError('setat not implemented')

    def set_partial_key(self, partial):
        """return (key, value) or None"""
        raise DatabaseError('set_partial_key not implemented')

    def get_partial(self):
        """return self._partial"""
        raise DatabaseError('get_partial not implemented')

    def get_converted_partial(self):
        """return self._partial as it would be held on database"""
        raise DatabaseError('get_converted_partial not implemented')

    def get_partial_with_wildcard(self):
        """return self._partial with wildcard suffix appended"""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """return converted self._partial with wildcard suffix appended"""
        raise DatabaseError(
            'get_converted_partial_with_wildcard not implemented')
