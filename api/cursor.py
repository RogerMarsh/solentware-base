# cursor.py
# Copyright 2017 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the cursor interface using method names taken from Berkeley DB to
indicate preference for that style of cursor.

Subclasses will provided appropriate implementations for the record definition
classes to use.

"""


class CursorError(Exception):
    pass


class Cursor:
    """Define a cursor on the underlying database engine to traverse dbset.

    These methods will be implemented using the cursor methods of the
    underlying database engine.  Subclasses may also provide methods
    with names matching those of the bsddb interface (typically first
    corresponding to first and so on).

    """

    def __init__(self, dbset):
        """Define a cursor on the underlying database engine."""
        super().__init__()
        self._cursor = None
        self._dbset = dbset
        self._partial = None

    def close(self):
        """Close database cursor."""
        try:
            self._cursor.close()
        except:
            pass
        self._cursor = None
        self._dbset = None
        self._partial = None

    def __del__(self):
        """Call the instance close() method."""
        self.close()

    def count_records(self):
        """return record count or None if implemented."""
        raise CursorError('count_records not implemented')

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise."""
        return bool(self._cursor)

    def first(self):
        """return (key, value) or None if implemented."""
        raise CursorError('first not implemented')

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero) if implemented."""
        raise CursorError('get_position_of_record not implemented')

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None if implemented.
        """
        raise CursorError('get_record_at_position not implemented')

    def last(self):
        """return (key, value) or None if implemented."""
        raise CursorError('last not implemented')

    def nearest(self, key):
        """return (key, value) or None if implemented."""
        raise CursorError('nearest not implemented')

    def next(self):
        """return (key, value) or None if implemented."""
        raise CursorError('next not implemented')

    def prev(self):
        """return (key, value) or None if implemented."""
        raise CursorError('prev not implemented')

    def refresh_recordset(self, instance=None):
        """Amend data structures after database update and return None if
        implemented.

        It may be correct to do nothing.

        """
        raise CursorError('refresh_recordset not implemented')

    def setat(self, record):
        """return (key, value) or None if implemented."""
        raise CursorError('setat not implemented')

    def set_partial_key(self, partial):
        """Set partial key to None.  Override to use partial argument.

        Subclasses of Cursor for secondary databases, named CursorSecondary
        usually, should override this method to bind self._partial to partial.

        Subclasses of Cursor for primary databases, named CursorPrimary usually,
        should use this method because partial keys make no sense for arbitrary
        numeric keys.

        Subclasses of Cursor for recordsets built from primary or secondary
        databases should use this method because the selection criteria for
        the recordset will have picked just the records needed.

        """
        self._partial = None

    def get_partial(self):
        """Return self._partial."""
        return self._partial

    def get_converted_partial(self):
        """return self._partial as it would be held on database if implemented.
        """
        raise CursorError('get_converted_partial not implemented')

    def get_partial_with_wildcard(self):
        """return self._partial with wildcard suffix appended if implemented."""
        raise CursorError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """return converted self._partial with wildcard suffix appended if
        implemented."""
        raise CursorError(
            'get_converted_partial_with_wildcard not implemented')
