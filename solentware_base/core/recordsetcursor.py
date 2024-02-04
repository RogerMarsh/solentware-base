# recordsetcursor.py
# Copyright 2024 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the cursor interface to an arbitrary recordset.

Method names are taken from Berkeley DB to indicate preference for that
style of cursor.

"""
from . import cursor


class RecordsetCursorError(Exception):
    """Exception for classes in recordsetcursor module."""


class RecordsetCursor(cursor.Cursor):
    """Provide a bsddb3 style cursor for a recordset of arbitrary records.

    The cursor does not support partial keys because the records in the
    recordset do not have an implied order (apart from the accidential order
    of existence on the database).

    """

    @property
    def recordset(self):
        """Return recordset."""
        return self._dbset

    def close(self):
        """Delete record set cursor."""
        # try:
        #    del self._dbset._clientcursors[self]
        # except:
        #    pass
        # self._dbset = None
        super().close()

    def count_records(self):
        """Return record count or None."""
        try:
            return self._dbset.count_records()
        except TypeError:
            return None
        except AttributeError:
            return None

    def database_cursor_exists(self):
        """Return True if self.records is not None and False otherwise.

        Simulates existence test for a database cursor.

        """
        # The cursor methods are defined in this class and operate on
        # self.records if it is a list so do that test here as well.
        return self._dbset is not None

    def first(self):
        """Return first record."""
        if len(self._dbset):
            try:
                # return self._dbset.get_record(self._dbset.first()[1])
                return self._get_record(self._dbset.first()[1])
            except TypeError:
                return None
        return None

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        try:
            return self._dbset.get_position_of_record_number(record[0])
        except ValueError:
            return 0
        except TypeError:
            return 0

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        try:
            return self._get_record(
                self._dbset.get_record_number_at_position(position)
            )
        except IndexError:
            return None
        except TypeError:
            if position is None:
                return None
            raise

    def last(self):
        """Return last record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.last()[1])
            except TypeError:
                return None
        return None

    def nearest(self, key):
        """Return nearest record. An absent record has no nearest record.

        Perhaps get_record_at_position() is the method to use.

        The recordset is created with arbitrary criteria.  The selected records
        are displayed in record number order for consistency.  Assumption is
        that all records on the recordset are equally near the requested record
        if it is not in the recordset itself, so whatever is already displayed
        is as near as any other records that might be chosen.

        """
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.setat(key)[1])
            except TypeError:
                return None
        return None

    def next(self):
        """Return next record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.next()[1])
            except TypeError:
                return None
        return None

    def prev(self):
        """Return previous record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.prev()[1])
            except TypeError:
                return None
        return None

    def setat(self, record):
        """Return record after positioning cursor at record."""
        if len(self._dbset):
            try:
                return self._get_record(self._dbset.setat(record[0])[1])
            except TypeError:
                return None
        return None

    def _get_record(self, record_number, use_cache=False):
        """Raise exception.  Must be implemented in a subclass."""
        raise RecordsetCursorError(
            "_get_record must be implemented in a subclass"
        )

    # Should this method be in solentware_misc datagrid module, or perhaps in
    # .record module?
    # Is referesh_recordset an appropriate name?
    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        if instance is None:
            return
        if self.recordset.is_record_number_in_record_set(instance.key.recno):
            if instance.newrecord is not None:
                raise RecordsetCursorError("refresh_recordset not implemented")
            self.recordset.remove_record_number(instance.key.recno)
