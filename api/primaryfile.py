# primaryfile.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add segments to File class for primary database.

The term 'primary database' is from Berkeley DB.

The term 'segment' is from DPT.

Combining the two requires an instance using Berkeley DB to be RECNO, and an
instance using SQLite3 to have rowid as the unique key.

Each segment contains a fixed number, the segmentsize, of record numbers where
segment N contains record numbers N to ( N * segmentsize ) - 1 and N is 0, 1,
2, ....

Keys in Berkeley DB RECNO databases and rowids in SQLite3 tables start at 1, so
record number 0 exists but the record does not for these two database engines.

Record numbers in DPT files start at 0.

Each data page in a DPT file (Table B) is allocated the fixed number of record
numbers defined for data pages in the file.  When record sizes vary enough from
their average size it is possible, even likely, a data page will not have
enough room for all it's allocated record numbers.  The special case for record
number 0 in Berkeley DB and SQLite3 may affect any record number in DPT.

"""

# The coding for creating the ExistenceBitMap instance is clumsy, but
# emphasises the similarity in structure of dbapi.PrimaryFile and
# _sqlite.PrimaryFile for ExistenceBitMap.  Also the stark and
# unnecessary differences between the two PrimaryFile subclasses are
# made more obvious: the FileControlSecondary, SegmentList, and
# SegmentBitarray, classes.

            
class PrimaryFile:
    
    """Add segment support to File for primary database.

    This class provides the methods to manage the lists and bitmaps of record
    numbers for segments of the primary database.
    """

    def __init__(self,
                 *args,
                 filecontrolprimary_class=None,
                 existencebitmap_class=None,
                 file_reference=None,
                 **kargs):
        """Add segment support to File for primary database."""
        super().__init__()

        # Description to be provided
        self._control_database = None

        # Existence bit map control structure (reuse record numbers)
        self._control_primary = filecontrolprimary_class(self)

        # Record number existence bit map for this primary database
        self._existence_bits = existencebitmap_class(
            file_reference=file_reference)

    def get_control_database(self):
        """Return the database containing segment control data."""
        return self._control_database.get_control_database()

    def get_existence_bits(self):
        """Return the existence bit map control data."""
        return self._existence_bits

    def set_control_database(self, database):
        """Set reference to segment control databases."""
        self._control_database = database

    def get_control_primary(self):
        """Return the re-use record number control data."""
        return self._control_primary
