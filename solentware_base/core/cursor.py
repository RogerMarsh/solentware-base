# cursor.py
# Copyright 2017 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the cursor interface.

Method names are taken from Berkeley DB to indicate preference for that
style of cursor.

Subclasses will provided appropriate implementations for the record
definition classes to use.

"""


class CursorError(Exception):
    """Exception for Cursor class."""


class Cursor:
    """Define a cursor on the underlying database engine for dbset.

    The methods defined in this class will be implemented using the cursor
    methods of the underlying database engine in a subclass of this class
    specific to the database engine.  Subclasses may also provide methods with
    names matching those of the bsddb interface (typically first and so forth).

    For lmdb (Symas LMDB) dbset will be a core._lmdb.DataStore() object.

    For berkeleydb and bsddb3 (Berkeley DB) dbset will be a DB() object,

    For apsw and sqlite3 (SQLite) dbset will be a Connection() object.

    For dptdb (DPT) dbset will be a core._dpt.DPTFile() object which creates
    a RecordSetCursor() or DirectValueCursor(), for records and indicies, on
    an OpenContext() object.

    (A version of the DPT interface is planned where dbset will be an
    OpenContext() object.)

    For tkinter (Tcl interface to Berkeley DB via tkinter) dbset will be
    a str() object (the generated name of the Tcl command to access the
    Berkeley DB database).

    For unqlite (UnQlite) dbset will be an unqlite.UnQlite() object.

    For vedis (Vedis) dbset will be a vedis.Vedis() object.

    For dbm.gnu (GNU DBM) dbset will be a gnu_module.Gnu() object.

    For dbm.ndbm (NDBM) dbset will be a ndbm_module.Ndbm() object.

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
        except Exception:
            pass
        self._cursor = None
        self._dbset = None
        self._partial = None

    def __del__(self):
        """Call the instance close() method."""
        self.close()

    def count_records(self):
        """Return record count or None."""
        raise CursorError("count_records not implemented")

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise."""
        return bool(self._cursor)

    def first(self):
        """Return (key, value) or None."""
        raise CursorError("first not implemented")

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        raise CursorError("get_position_of_record not implemented")

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        raise CursorError("get_record_at_position not implemented")

    def last(self):
        """Return (key, value) or None."""
        raise CursorError("last not implemented")

    def nearest(self, key):
        """Return (key, value) or None."""
        raise CursorError("nearest not implemented")

    def next(self):
        """Return (key, value) or None."""
        raise CursorError("next not implemented")

    def prev(self):
        """Return (key, value) or None."""
        raise CursorError("prev not implemented")

    def refresh_recordset(self, instance=None):
        """Amend data structures after database update and return None.

        It may be correct to do nothing.

        """
        raise CursorError("refresh_recordset not implemented")

    def setat(self, record):
        """Return (key, value) or None if implemented."""
        raise CursorError("setat not implemented")

    def set_partial_key(self, partial):
        """Set partial key to None.  Override to use partial argument.

        Subclasses of Cursor for secondary databases, named CursorSecondary
        usually, should override this method to bind self._partial to partial.

        Subclasses of Cursor for primary databases, named CursorPrimary
        usually, should use this method because partial keys make no sense
        for arbitrary numeric keys.

        Subclasses of Cursor for recordsets built from primary or secondary
        databases should use this method because the selection criteria for
        the recordset will have picked just the records needed.

        """
        del partial
        self._partial = None

    def get_partial(self):
        """Return self._partial."""
        return self._partial

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        raise CursorError("get_converted_partial not implemented")

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise CursorError("get_partial_with_wildcard not implemented")

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        raise CursorError(
            "get_converted_partial_with_wildcard not implemented"
        )
