# recordsetbasecursor.py
# Copyright 2024 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define cursor interface for repeatable access to an arbitrary recordset.

Method names are taken from Berkeley DB to indicate preference for that
style of cursor.

"""


class RecordSetBaseCursor:
    """Cursor for wrapper for _Recordset compatible with _dpt._DPTRecordSet.

    recordset: the _Recordset instance on which this is the cursor.
    location: the Location instance which holds this cursor's position.
        None means use the internal location of the _Recordset instance.
    """

    def __init__(self, recordset, location=None):
        """Create a _Recordset instance."""
        self.recordset = recordset
        self.location = location

    # recordset set to None for compatibility with _DPTRecordList class in
    # _dpt module.
    def close(self):
        """Close recordset."""
        self.recordset = None

    def get_position_of_record_number(self, recnum):
        """Return position of record number in recordset."""
        return self.recordset.get_position_of_record_number(recnum)

    def get_record_number_at_position(self, position):
        """Return record number of record at position in recordset."""
        return self.recordset.get_record_number_at_position(position)

    # Before solentware_base 5.2 wrongly returned self.recordset.first()
    def first(self):
        """Position at first record in recordset and return record."""
        return self._get_record(self.recordset.first(location=self.location))

    # Before solentware_base 5.2 wrongly returned self.recordset.last()
    def last(self):
        """Position at last record in recordset and return record."""
        return self._get_record(self.recordset.last(location=self.location))

    # Before solentware_base 5.2 wrongly returned self.recordset.next()
    def next(self):
        """Position at next record in recordset and return record."""
        return self._get_record(self.recordset.next(location=self.location))

    # Before solentware_base 5.2 wrongly returned self.recordset.prev()
    def prev(self):
        """Position at previous record in recordset and return record."""
        return self._get_record(self.recordset.prev(location=self.location))

    # Before solentware_base 5.2 wrongly returned self.recordset.current()
    def current(self):
        """Return current record."""
        return self._get_record(self.recordset.current(location=self.location))

    # Before solentware_base 5.2 wrongly returned self.recordset.setat(record)
    def setat(self, record):
        """Position at record and return record."""
        return self._get_record(
            self.recordset.setat(record, location=self.location)
        )

    def _get_record(self, reference):
        """Return record for reference from RecordList instance."""
        if reference is None:
            return None
        return self.recordset.dbhome.get_primary_record(
            self.recordset.dbset, reference[1]
        )

    def first_record_number(self):
        """Position at first record in recordset and return record number."""
        return self._get_record_number(
            self.recordset.first(location=self.location)
        )

    def last_record_number(self):
        """Position at last record in recordset and return record number."""
        return self._get_record_number(
            self.recordset.last(location=self.location)
        )

    def next_record_number(self):
        """Position at next record in recordset and return record number."""
        return self._get_record_number(
            self.recordset.next(location=self.location)
        )

    def prev_record_number(self):
        """Position at prior record in recordset and return record number."""
        return self._get_record_number(
            self.recordset.prev(location=self.location)
        )

    def current_record_number(self):
        """Return current record number."""
        return self._get_record_number(
            self.recordset.current(location=self.location)
        )

    def setat_record_number(self, record):
        """Position at record and return record number."""
        return self._get_record_number(
            self.recordset.setat(record, location=self.location)
        )

    def _get_record_number(self, reference):
        """Return record number for reference from RecordList instance."""
        if reference is None:
            return None
        return reference[1]
