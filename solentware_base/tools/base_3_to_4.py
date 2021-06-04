# base_3_to_4.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Upgrade a solentware_base version 3 database to version 4."""
import os

from ..core.filespec import FileSpec
from ..core import constants

# Not defined in .core.constants at version 3 and defined here with V3_ prefix.
# _exist is _ebm.
V3_EXISTENCE_BITMAP_SUFFIX = "_exist"


class Base_3_to_4Error(Exception):
    """Exception raised in Base_3_to_4 class."""


class Base_3_to_4:
    """Convert database with filespec using segment size v4_segment_size.

    database: path name to directory containing database to be converted.
    filespec: an instance of FileSpec, or a subclass of FileSpec
    v4_segment_size: segment size, in bytes, to use at version 4.  The default
                    is same as used in all version 3 databases (8192 bytes).

    Methods are provided to:

    Calculate version 3 table names from a FileSpec instance.
    Calculate version 4 table names from a FileSpec instance.
    Generate version 4 segments from version 3 segments: the default segment
    size at version 4 is the segment size always used at version 3.

    """

    def __init__(self, filespec, database, engine):
        """Set solentware_base 3 and 4 table and index names for database."""
        if not isinstance(filespec, FileSpec):
            raise Base_3_to_4Error(
                "filespec is not a FileSpec, or a subclass, instance"
            )
        if not os.path.isdir(database):
            raise Base_3_to_4Error(
                "".join((database, " is not a directory or does not exist"))
            )

        # If db_segment_size_bytes is not called, db_segment_size_bytes retains
        # it's default value of 8192, the same as version 3 segment size.
        # Note that a non-int v4_segment_size gives a db_segment_size_bytes of
        # 16 bytes, intended for testing, if db_segment_size_bytes is set here.
        # if v4_segment_size is not None:
        #    SegmentSize.db_segment_size_bytes = v4_segment_size

        # The filemap and files dict()s are populated only for version 3 using
        # Berkeley DB: the other three cases put tables in database/database.
        # The indexmap and indicies dicts() are populated only for SQLite 3: an
        # index must be defined on a table.  The equivalent Berkeley DB 'table'
        # is the index.  However the indicies are populated as a consequence of
        # loading the table, so these dict()s are documentary only.
        self.filespec = filespec
        self.database = database
        self.engine = engine
        self.segment_size = None
        self.v3filemap = {}
        self.v3files = set()
        self.v3tablemap = {}
        self.v3tables = set()
        self.v3indexmap = {}
        self.v3indicies = set()
        self.v3existmap = {}
        self.v3exists = set()
        self.v4filemap = {}
        self.v4files = set()
        self.v4tablemap = {}
        self.v4tables = set()
        self.v4indexmap = {}
        self.v4indicies = set()
        self.v4existmap = {}
        self.v4exists = set()
        self.v3segmentmap = {}
        self.v3segments = set()
        self.v4segmentmap = {}
        self.v4segments = set()

        self._generate_v3_names()
        self._generate_v4_names()

    def _generate_v3_names(self):
        tablemap = self.v3tablemap
        tables = self.v3tables
        existmap = self.v3existmap
        exists = self.v3exists
        tables.add(constants.CONTROL_FILE)
        for k, value in self.filespec.items():
            primary = value[constants.PRIMARY]
            tablemap[
                k,
            ] = primary
            tables.add(primary)
            name = constants.SUBFILE_DELIMITER.join(
                (primary, V3_EXISTENCE_BITMAP_SUFFIX)
            )
            existmap[
                k,
            ] = name
            exists.add(name)

    def _generate_v4_names(self):
        tablemap = self.v4tablemap
        tables = self.v4tables
        existmap = self.v4existmap
        exists = self.v4exists
        tables.add(constants.CONTROL_FILE)
        for k, value in self.filespec.items():
            secondary = value[constants.SECONDARY]
            for key in secondary:
                name = constants.SUBFILE_DELIMITER.join((k, key))
                tablemap[k, key] = name
                tables.add(name)
            tablemap[
                k,
            ] = k
            tables.add(k)
            name = constants.SUBFILE_DELIMITER.join(
                (k, constants.EXISTENCE_BITMAP_SUFFIX)
            )
            existmap[
                k,
            ] = name
            exists.add(name)
