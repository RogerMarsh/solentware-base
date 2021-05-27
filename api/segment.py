# segment.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
"""


class SegmentError(Exception):
    pass


class Segment:
    
    """Define a primary database with transaction support for lists or bitmaps
    of record numbers.

    There are three types of segment: existence bitmap, recordset bitmap, and
    recordset record number list.  Each is opened in a slightly different way
    so the relevant open_root method is defined in the subclasses.

    """

    def __init__(self):
        """Primary database for inverted record number tables.

        The main database engines have one attribute in common, and use that
        in different ways.

        """
        super().__init__()
        self._segment_link = None

    def __del__(self):
        self.close()

    def close(self):
        """Close inverted index DB."""
        self._segment_link = None

    def get(self, key):
        """Get a segment record from the database."""
        raise SegmentError('Segment.get not implemented')

    def delete(self, key):
        """Delete a segment record from the database."""
        raise SegmentError('Segment.delete not implemented')

    def put(self, key, value):
        """Put a segment record on the database, either replace or insert."""
        raise SegmentError('Segment.put not implemented')

    def append(self, value):
        """Append a segment record on the database using a new key."""
        raise SegmentError('Segment.append not implemented')
