# filespec.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide behaviour common to all file specifications.

List of classes

FileSpec

"""

from .constants import (
    BSIZE,
    BRECPPG,
    DSIZE,
    BTOD_FACTOR,
    RRN,
    DEFAULT_RECORDS,
    FILEDESC,
    FILEORG,
    DEFAULT_INITIAL_NUMBER_OF_RECORDS,
    )


class FileSpec(dict):

    """Create database specification from **kargs.

    BSIZE and DSIZE are calculated from requested number of records for a
    file.  DEFAULT_RECORDS for a file is used if available. The default_records
    argument is used otherwise.

    BSIZE is size of data area, and DSIZE the index area, of a DPT file.

    DPT files do not increase, or decrease, in size automatically to meet the
    demand for space.  The file size must be specified somehow; hence the use
    of the default_records argument.

    Methods added:

    None
    
    Methods overridden:

    None
    
    Methods extended:

    __init__

    """

    @staticmethod
    def dpt_dsn(file_def):
        """Return a standard filename (DSN name) for DPT from file_def"""
        return ''.join((file_def.lower(), '.dpt'))
    
    @staticmethod
    def field_name(field_def):
        """Return standard fieldname to be the implementation resource name"""
        return ''.join((field_def[0].upper(), field_def[1:]))

    def __init__(self, use_specification_items=None, dpt_records=None, **kargs):
        """Set BSIZE and DSIZE for files defined in subclass.

         use_specification_items=<items in kargs to be used as specification>
             Use all items if use_specification_items is None
         dpt_records=
            <dictionary of number of records for DPT file size calculation>
            Overrides defaults in kargs and the default from constants module.
        **kargs=<file specifications>

        Berkeley DB makes databases of key:value pairs distributed across one
        or more files depending on the environment specification.

        Sqlite3 makes tables and indexes in a single file.

        DPT makes one file per item in kargs containing non-ordered and ordered
        fields.

        """
        super(FileSpec, self).__init__(**kargs)

        if use_specification_items is not None:
            for usi in [k for k in self.keys()
                        if k not in use_specification_items]:
                del self[usi]

        if dpt_records is None:
            dpt_records = {}
        if not isinstance(dpt_records, dict):
            raise RuntimeError('dpt_default_records must be a dict')
        for k, v in self.items():
            dpt_filesize = dpt_records.setdefault(
                k, DEFAULT_INITIAL_NUMBER_OF_RECORDS)
            if not isinstance(dpt_filesize, int):
                raise RuntimeError(''.join(
                    ('number of records must be a positive integer for item ',
                     k,
                     ' in filespec.',
                     )))
            if dpt_filesize < 1:
                raise RuntimeError(''.join(
                    ('number of records must be a positive integer for item ',
                     k,
                     ' in filespec.',
                     )))
            records = v.setdefault(DEFAULT_RECORDS, dpt_filesize)
            filedesc = v.setdefault(FILEDESC, {})
            brecppg = filedesc.setdefault(BRECPPG, 10)
            filedesc.setdefault(FILEORG, RRN)
            btod_factor = v.setdefault(BTOD_FACTOR, 8)
            bsize = records // brecppg
            if bsize * brecppg < records:
                bsize += 1
            v[FILEDESC][BSIZE] = bsize
            v[FILEDESC][DSIZE] = int(round(bsize * btod_factor))

