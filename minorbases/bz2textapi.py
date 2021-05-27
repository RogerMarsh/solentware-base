# bz2textapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide read access to text files compressed by bzip2 using the database
interface defined in the api.database.Database and api.cursor.Cursor classes
.
"""

import bz2

from . import textapi


class BZ2Textapi(textapi.Textapi):
    
    """Define a textdb database structure for a bz2 compressed file.
    
    """

    def make_root(self, filename):
        """Return a BZ2TextapiRoot instance for filename."""
        return BZ2TextapiRoot(filename)


class BZ2TextapiRoot(textapi.TextapiRoot):
    
    """Define a bz2 compressed text file.
    
    See superclass for description.
    
    """
    
    def open_root(self):
        """Open a bz2 compressed text file and read all lines."""
        try:
            self._table_link = bz2.BZ2File(self.filename, 'rb')
            self.textlines = self._table_link.read().splitlines()
            self.record_count = len(self.textlines)
            self.record_number = None
            self.record_select = None
        except:
            self._table_link = None

