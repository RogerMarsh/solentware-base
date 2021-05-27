# secondary.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add segments to File class for secondary database.

The term 'secondary database' is from Berkeley DB.

The term 'segment' is from DPT.

Combining the two requires an instance using Berkeley DB to be BTREE, an
instance using SQLite3 to have something other than rowid as the unique key,
and an instance using DPT is an invisible ordered character field.

Here Berkeley DB's secondary database and DPT's invisible ordered character
field are equivalent.  In SQLite3 an extra table has to be defined, against
which the equivalent index is defined.

The value associated with the key is either a record number or a reference to
a set of record numbers in the primary database.  The set of record numbers is
divided into segments as described in primary module docstring.

"""

            
class SecondaryFile:
    
    """Add segment support to File for secondary database.

    This class uses the methods of the PrimaryFile class to manage the
    lists and bitmaps of record numbers for segments of the table supporting
    the secondary database.  The link is set after both tables have been
    opened.
    """

    def __init__(self, *args):
        """Add segment support to File for secondary database."""
        super().__init__(*args)
        self._primary_database = None

    def get_primary_database(self):
        """Set reference to primary database to access segment databases."""
        return self._primary_database

    def set_primary_database(self, database):
        """Set reference to primary database to access segment databases."""
        self._primary_database = database
