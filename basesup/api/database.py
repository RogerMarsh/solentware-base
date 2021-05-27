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

List of functions

encode_record_number
decode_record_number

"""
# Do decode_record_number and encode_record_number belong in dbapi.py?
# There is a need for functions to convert unique identifiers within database
# but at present record number is used as the unique identifier within file.
# This allows database engine to adjust record numbers in reorganisations
# without breaking record references.

# shifts and masks for base 256 record number encoding
MASK_SHIFT = (
    (255 << 24, 24),
    (255 << 16, 16),
    (255 << 8, 8),
    (255, 0))


class DatabaseError(StandardError):
    pass


class Database(object):
    
    """Define file and record access methods that subclasses must provide.

    Methods added:

    close_context
    close_database
    commit
    close_internal_cursors
    dpt_db_compatibility_hack
    delete_instance
    edit_instance
    exists
    make_cursor
    get_database_folder
    get_database
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

    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self):
        super(Database, self).__init__()
    
    def close_context(self):
        """return None"""
        raise DatabaseError, 'close_context not implemented'

    def close_database(self):
        """return None"""
        raise DatabaseError, 'close_database not implemented'

    def commit(self):
        """return None"""
        raise DatabaseError, 'commit not implemented'

    def close_internal_cursors(self, dbnames=None):
        """return True or False"""
        raise DatabaseError, 'close_internal_cursors not implemented'

    def dpt_db_compatibility_hack(self, record, srkey):
        """return None"""
        raise DatabaseError, 'dpt_db_compatibility_hack not implemented'

    def delete_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError, 'delete_instance not implemented'

    def edit_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError, 'edit_instance not implemented'

    def exists(self, dbset, dbname):
        """return True or False"""
        raise DatabaseError, 'exists not implemented'

    def make_cursor(self, dbname):
        """return a cursor or None"""
        raise DatabaseError, 'make_cursor not implemented'

    def get_database_folder(self):
        """return database folder name"""
        raise DatabaseError, 'get_database_folder not implemented'

    def get_database(self, dbname):
        """return database object"""
        raise DatabaseError, 'get_database not implemented'

    def get_first_primary_key_for_index_key(self, dbset, dbname, key):
        """return primary key as held on dbname or None"""
        raise DatabaseError, (
            'get_first_primary_key_for_index_key not implemented')

    def get_primary_record(self, dbname, key):
        """return (primary key, value) or None
        
        key is a number or a string. value is a string.
        dbname must be the primary DB (Berkeley DB term).
        
        """
        raise DatabaseError, 'get_primary_record not implemented'

    def make_internal_cursors(self, dbnames=None):
        """return True or False"""
        raise DatabaseError, 'make_internal_cursors not implemented'

    def is_primary(self, dbname):
        """return True or False"""
        raise DatabaseError, 'is_primary not implemented'

    def is_primary_recno(self, dbname):
        """return True or False"""
        raise DatabaseError, 'is_primary_recno not implemented'

    def is_recno(self, dbname):
        """return True or False"""
        raise DatabaseError, 'is_recno not implemented'

    def open_context(self):
        """return True if database is opened"""
        raise DatabaseError, 'open_context not implemented'

    def get_packed_key(self, dbname, instance):
        """return key
        
        self may derive key from instance.key or call
        instance.key.pack() to derive key.
        
        """
        raise DatabaseError, 'get_packed_key not implemented'

    def decode_as_primary_key(self, dbname, srkey):
        """return key

        self derives the primary key from srkey.

        """
        raise DatabaseError, 'decode_as_primary_key not implemented'

    def encode_primary_key(self, dbname, instance):
        """return string representation of key
        
        self derives string representation of instance.key
        probably using instance.key.pack().
        
        """
        raise DatabaseError, 'encode_primary_key not implemented'

    def put_instance(self, dbname, instance):
        """return None"""
        raise DatabaseError, 'put_instance not implemented'

    def use_deferred_update_process(self, **kargs):
        """return True False or None"""
        raise DatabaseError, 'use_deferred_update_process not implemented'


class Cursor(object):
    """Define cursor access methods that subclasses must provide.

    These methods will be implemented using the cursor methods of the
    underlying database engine.  Subclasses may also provide methods
    with names matching those of the bsddb interface (typically first
    corresponding to first and so on).
    
    Methods added:

    close
    database_cursor_exists
    first
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
    current
    first
    last
    next
    prev
    set
    set_both
    set_range

    """

    def __init__(self):
        """Define a cursor on the underlying database engine"""
        super(Cursor, self).__init__()

    def close(self):
        """return None"""
        raise DatabaseError, 'close not implemented'

    def database_cursor_exists(self):
        """return True if cursor exists or False"""
        raise DatabaseError, 'database_cursor_exists not implemented'

    def first(self):
        """return (key, value) or None"""
        raise DatabaseError, 'first not implemented'

    def last(self):
        """return (key, value) or None"""
        raise DatabaseError, 'last not implemented'

    def nearest(self, key):
        """return (key, value) or None"""
        raise DatabaseError, 'nearest not implemented'

    def next(self):
        """return (key, value) or None"""
        raise DatabaseError, 'next not implemented'

    def prev(self):
        """return (key, value) or None"""
        raise DatabaseError, 'prev not implemented'

    def refresh_recordset(self):
        """Amend cursor data structures on database update and return None

        It may be correct to do nothing.

        """
        raise DatabaseError, 'refresh_recordset not implemented'

    def setat(self, record):
        """return (key, value) or None"""
        raise DatabaseError, 'setat not implemented'

    def set_partial_key(self, partial):
        """return (key, value) or None"""
        raise DatabaseError, 'set_partial_key not implemented'

        
def encode_record_number(key):
    """Return base 256 string for integer with left-end most significant.

    Typically used to convert Berkeley DB primary key to secondary index
    format.
    
    """
    si = []
    for m, s in MASK_SHIFT:
        si.append(chr((key & m) >> s))
    return ''.join(si)


def decode_record_number(skey):
    """Return integer from base 256 string with left-end most significant.

    Typically used to convert Berkeley DB primary key held on secondary
    index.

    """
    i = 0
    for b in skey:
        i = (i << 8) + ord(b)
    return i
    
