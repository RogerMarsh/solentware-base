# ziptextapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide read access to text files compressed by zip using the database
interface defined in the api.database.Database and api.cursor.Cursor classes.

"""

import zipfile

from . import textapi


class ZipTextapi(textapi.Textapi):
    
    """Define a textdb database structure for a zip compressed file.
    
    See superclass for description.
    
    """

    def make_root(self, filename):
        """Return a ZipTextapiRoot instance for filename."""
        return ZipTextapiRoot(filename)


class ZipTextapiRoot(textapi.TextapiRoot):
    
    """Define a zip compressed text file.
    
    See superclass for description.
    
    """
    
    def open_root(self):
        """Open a zip compressed text file and read all lines."""
        try:
            self._table_link = zipfile.ZipFile(self.filename, 'r')
            #open method added at Python 2.6
            '''self.textlines = [
                t for t in self._table_link.open(
                    self._table_link.infolist()[0])]'''
            #should use csv.reader to cope with '\n' as data in csv files
            self.textlines = self._table_link.read(
                self._table_link.namelist()[0]).split('\n')
            self.record_count = len(self.textlines)
            self.record_number = None
            self.record_select = None
        except:
            self._table_link = None

