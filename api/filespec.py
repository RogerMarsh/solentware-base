# filespec.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide FileSpec creation behaviour common to all file specifications.

Example database specifications are in the samples directory.

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
    FILE,
    DDNAME,
    PRIMARY,
    SECONDARY,
    FIELDS,
    )


class FileSpecError(Exception):
    pass


class FileSpec(dict):

    """Create FileSpec from database specification in **kargs.

    The simplest database specification is a dictionary where the keys are
    names of Berkeley DB databases used as primary databases and the values are
    iterables of names of Berkeley DB databases used as secondary databases.
    The generated FileSpec can be used to create a Berkeley DB database, an
    SQLite3 emulation of the Berkeley DB database, or a DPT emulation of the
    Berkeley DB database.

    Dictionary values in the database specification are copied to the FileSpec
    and used unchanged.  The main reason for dictionary values is control of
    file, table, and index, names in Berkeley DB and SQLite3.  Defaults are put
    in the FileSpec to allow creation DPT databases, but these will almost
    certainly be wrong for sizing reasons and appropriate parameters will have
    to be given in dictionary values.

    """

    @staticmethod
    def dpt_dsn(file_def):
        """Return a standard filename (DSN name) for DPT from file_def."""
        return ''.join((file_def.lower(), '.dpt'))
    
    @staticmethod
    def field_name(field_def):
        """Return standard fieldname to be the implementation resource name."""
        return ''.join((field_def[0].upper(), field_def[1:]))

    def __init__(self, use_specification_items=None, dpt_records=None, **kargs):
        """Provide default values for essential parameters for the DPT database
        engine.

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
        super().__init__(**kargs)

        if use_specification_items is not None:
            for usi in [k for k in self.keys()
                        if k not in use_specification_items]:
                del self[usi]

        if dpt_records is None:
            dpt_records = {}
        if not isinstance(dpt_records, dict):
            raise FileSpecError('dpt_default_records must be a dict')
        ddi = 0
        for k, v in self.items():
            dpt_filesize = dpt_records.setdefault(
                k, DEFAULT_INITIAL_NUMBER_OF_RECORDS)
            if not isinstance(dpt_filesize, int):
                raise FileSpecError(''.join(
                    ('number of records must be a positive integer for item ',
                     k,
                     ' in filespec.',
                     )))
            if dpt_filesize < 1:
                raise FileSpecError(''.join(
                    ('number of records must be a positive integer for item ',
                     k,
                     ' in filespec.',
                     )))
            if not isinstance(v, dict):
                names = v
                ddi += 1
                v = {PRIMARY: k,
                     DDNAME: DDNAME.upper() + str(ddi),
                     FILE: FileSpec.dpt_dsn(k),
                     SECONDARY: {},
                     FIELDS: {k: None},
                     }
                for n in names:
                    if n.lower() == k.lower():
                        raise FileSpecError(''.join(
                            ("Secondary name '",
                             n,
                             "' cannot be same as ",
                             "primary name '",
                             k,
                             "' in filespec.",
                             )))
                    v[SECONDARY][n] = None
                    v[FIELDS][FileSpec.field_name(n)] = None
                self[k] = v
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

