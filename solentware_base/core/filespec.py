# filespec.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide FileSpec creation behaviour common to all file specifications.

Example database specifications are in the samples directory.

"""
import os

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
    SUBFILE_DELIMITER,
    BTOD_CONSTANT,
    FOLDER,
    DPT_PRIMARY_FIELD_LENGTH,
    SAFE_DPT_FIELD_LENGTH,
    MANDATORY_FILEATTS,
    FILEATTS,
    SUPPORTED_FILEORGS,
    PRIMARY_FIELDATTS,
    SECONDARY_FIELDATTS,
    SPT,
    ACCESS_METHOD,
    HASH,
    BTREE,
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
            if SUBFILE_DELIMITER in k: # Just startswith?
                raise FileSpecError(''.join(
                    ("Primary name '",
                     k,
                     "' contains '",
                     SUBFILE_DELIMITER,
                     "'.",
                     )))
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

            # A set of secondary names, however presented, is converted to a
            # minimal dictionary representing a valid, but probably useless,
            # specification for a set of DPT files.
            if not isinstance(v, dict):
                if len(v) != len(set(v)):
                    duplicated = [f for f in set(v) if v.count(f) > 1]
                    raise FileSpecError(''.join(
                        ("Secondary names '",
                         "', '".join(sorted(duplicated)),
                         "' are duplicated",
                         )))
                names = v
                ddi += 1
                v = {PRIMARY: k,
                     DDNAME: DDNAME.upper() + str(ddi),
                     FILE: FileSpec.dpt_dsn(k),
                     SECONDARY: {},
                     FIELDS: {k: None},
                     }
                for n in names:
                    if n.startswith(SUBFILE_DELIMITER):
                        raise FileSpecError(''.join(
                            ("Secondary name '",
                             n,
                             "' starts with '",
                             SUBFILE_DELIMITER,
                             "'.",
                             )))
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
                    v[FIELDS][FileSpec.field_name(n)] = {}
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
            v.setdefault(BTOD_CONSTANT, 0)
        
        # Validate the specification, which may have been expanded in the
        # preceding section.
        # The name of the specification is usually, but does not have to be,
        # same as the primary name in specification[PRIMARY].
        # The keys of specification[SECONDARY] and specification[FIELDS] are
        # usually the same.  If not, specification[SECONDARY][i] maps to the
        # key of specification[FIELDS].
        definitions = set()
        pathnames = dict()
        for name, specification in self.items():
            if not isinstance(specification, dict):
                msg = ' '.join(
                    ['Specification for', repr(name),
                     'must be a dictionary'])
                raise FileSpecError(msg)
            if PRIMARY not in specification:
                msg = ' '.join(['Specification for', repr(name),
                                'must contain a primary name'])
                raise FileSpecError(msg)
            primary = specification[PRIMARY]
            if SUBFILE_DELIMITER in primary:
                raise FileSpecError(''.join(
                    ('Primary name ',
                     primary,
                     " contains '",
                     SUBFILE_DELIMITER,
                     )))
            if primary in definitions:
                msg = ' '.join(['Primary name', primary,
                                'for', name,
                                'already used'])
                raise FileSpecError(msg)
            if SECONDARY in specification:
                for k, v in specification[SECONDARY].items():
                    if v is None:
                        if k.lower() == primary.lower():
                            msg = ' '.join(
                                ['Primary name', primary,
                                 'for', name,
                                 'must not be in secondary definition',
                                 '(ignoring case)'])
                            raise FileSpecError(msg)
                    elif primary.lower() in (k.lower(), v.lower()):
                        msg = ' '.join(
                            ['Primary name', primary,
                             'for', name,
                             'must not be in secondary definition',
                             '(ignoring case)'])
                        raise FileSpecError(msg)
                    if (v if v else FileSpec.field_name(k)
                        ) not in specification[FIELDS]:
                        msg = ' '.join(['Secondary field name',
                                        str(k),
                                        'for', name, 'does not have',
                                        'a field description'])
                        raise FileSpecError(msg)
            if FIELDS not in specification:
                msg = ' '.join(['Field definitions must be present in',
                                'specification for primary fields'])
                raise FileSpecError(msg)
            if primary not in specification[FIELDS]:
                msg = ' '.join(['Primary name', primary,
                                'for', name,
                                'must be in fields definition'])
                raise FileSpecError(msg)
            if specification.get(DPT_PRIMARY_FIELD_LENGTH,
                                 SAFE_DPT_FIELD_LENGTH) > 255:
                msg = ' '.join(
                    ['Safe unicode length for utf-8 encoding is greater than',
                     '255 for primary field in DPT file',
                     dbset])
                raise FileSpecError(msg)
            try:
                os.path.join(specification.get(FILE))
            except TypeError:
                msg = ' '.join(['File name for', name,
                                'must be a valid path name'])
                raise FileSpecError(msg)
            try:
                os.path.join(specification.get(FOLDER, ''))
            except TypeError:
                msg = ' '.join(['Folder name for', name,
                                'must be a valid path name'])
                raise FileSpecError(msg)
            filedesc = specification.get(FILEDESC)
            if not isinstance(filedesc, dict):
                msg = ' '.join(['Description of file', name,
                                'must be a dictionary'])
                raise FileSpecError(msg)
            for attr in MANDATORY_FILEATTS:
                if attr not in filedesc:
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', name,
                                    'must be present'])
                    raise FileSpecError(msg)
            for attr in filedesc:
                if attr not in FILEATTS:
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', name,
                                    'is not allowed'])
                    raise FileSpecError(msg)

                if attr not in MANDATORY_FILEATTS:
                    if not isinstance(filedesc[attr], int):
                        msg = ' '.join(['Attribute', repr(attr),
                                        'for file', name,
                                        'must be a number'])
                        raise FileSpecError(msg)
                elif not isinstance(filedesc[attr],
                                    MANDATORY_FILEATTS[attr]):
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', name,
                                    'is not correct type'])
                    raise FileSpecError(msg)
            if filedesc.get(FILEORG, None) not in SUPPORTED_FILEORGS:
                msg = ' '.join(
                    ['File', name,
                     'must be "Entry Order" or',
                     '"Unordered and Reuse Record Number"'])
                raise FileSpecError(msg)
            fields = specification[FIELDS]
            if not isinstance(fields, dict):
                msg = ' '.join(['Field description of file', repr(name),
                                'must be a dictionary'])
                raise FileSpecError(msg)

            # Mostly for DPT, but one or two field attributes are relevant to
            # the other engines.  Field attribute validation specific to an
            # engine is not done here.
            for fieldname in fields:
                description = fields[fieldname]
                if description is None:
                    description = dict()
                if not isinstance(description, dict):
                    msg = ' '.join(['Attributes for field', fieldname,
                                    'in file', repr(name),
                                    'must be a dictionary or "None"'])
                    raise FileSpecError(msg)
                if fieldname == primary:
                    fieldatts = PRIMARY_FIELDATTS
                else:
                    fieldatts = SECONDARY_FIELDATTS
                for attr in description:
                    if attr not in fieldatts:
                        msg = ' '.join(['Attribute', repr(attr),
                                        'for field', fieldname,
                                        'in file', name,
                                        'is not allowed'])
                        raise FileSpecError(msg)
                    if not isinstance(description[attr], type(fieldatts[attr])):
                        msg = ' '.join([attr, 'for field', fieldname,
                                        'in file', name, 'is wrong type'])
                        raise FileSpecError(msg)
                    if attr == SPT:
                        if (description[attr] < 0 or
                            description[attr] > 100):
                            msg = ' '.join(['Split percentage for field',
                                            fieldname, 'in file', name,
                                            'is invalid'])
                            raise FileSpecError(msg)
            try:
                ddname = specification[DDNAME]
            except KeyError:
                msg = ' '.join(['Specification for', name,
                                'must have a DD name'])
                raise FileSpecError(msg)
            if len(ddname) == 0:
                msg = ' '.join(['DD name', repr(ddname),
                                'for', name,
                                'is zero length'])
                raise FileSpecError(msg)
            elif len(ddname) > 8:
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'is over 8 characters'])
                raise FileSpecError(msg)
            elif not ddname.isalnum():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise FileSpecError(msg)
            elif not ddname.isupper():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise FileSpecError(msg)
            elif not ddname[0].isupper():
                msg = ' '.join(['DD name', ddname,
                                'for', name,
                                'must be upper case alphanum',
                                'starting with alpha'])
                raise FileSpecError(msg)
            else:
                try:

                    # At Python26+ need to convert unicode to str for DPT.
                    fname = str(os.path.join(
                        specification.get(FOLDER, ''),
                        specification.get(FILE, None)))

                except:
                    msg = ' '.join(
                        ['Relative path name of DPT file for', name,
                         'is invalid'])
                    raise FileSpecError(msg)
                if fname in pathnames:
                    msg = ' '.join(['File name', os.path.basename(fname),
                                    'linked to', pathnames[fname],
                                    'cannot link to', name])
                    raise FileSpecError(msg)
                pathnames[fname] = name

            definitions.add(primary)

    def is_consistent_with(self, specification):
        """Raise FileSpecError if specification is not consistent with self.

        The specification is expected to be one stored with a database.

        In particular the access method for fields in the database version is
        allowed to be different from the version in self, by being BTREE rather
        than HASH.
        """
        # Compare specification with reference version in self to allow field
        # access methods to differ.  Specification can say, or imply by
        # default, BTREE while reference version can say HASH instead.
        # (Matters for _db and _nosql modules.)
        if self == specification:
            return
        sdbspec = sorted([s for s in specification])
        ssspec = sorted([s for s in self])
        if sdbspec != ssspec:
            raise FileSpecError(
                ''.join(
                    ('Specification does not have same files as defined in ',
                     'this FileSpec')))
        msgdh = ''.join(
            ('Specification does not have same detail headings for each file ',
             'as defined in this FileSpec'))
        msgd = ''.join(
            ('Specification does not have same detail for each file as ',
             'defined in this FileSpec'))
        msgfield = ''.join(
            ('Specification does not have same fields for each file as ',
             'defined in this FileSpec'))
        msgam = ''.join(
            ('Specification does not have same descriptions for each field ',
             'in each file as defined in this FileSpec'))
        for dbs, ss in zip(sdbspec, ssspec):
            sdbs = sorted(s for s in specification[dbs])
            sss = sorted(s for s in self[ss])
            if sdbs != sss:
                raise FileSpecError(msgdh)
            for dbsd in sdbs:
                if dbsd != FIELDS:
                    if (specification[dbs][dbsd] != self[ss][dbsd]):
                        raise FileSpecError(msgd)
                    continue
                sdbsf = specification[dbs][dbsd]
                sssf = self[dbs][dbsd]
                if sorted(sdbsf) != sorted(sssf):
                    raise FileSpecError(msgfield)
                for fn in sdbsf:
                    if sdbsf[fn] == sssf[fn]:
                        continue
                    dbfp = sdbsf[fn].copy()
                    sfp = sssf[fn].copy()
                    if ACCESS_METHOD in dbfp:
                        del dbfp[ACCESS_METHOD]
                    if ACCESS_METHOD in sfp:
                        del sfp[ACCESS_METHOD]
                    if dbfp != sfp:
                        raise FileSpecError(msgam)
                    dbfpam = sdbsf[fn].get(ACCESS_METHOD, BTREE)
                    sfpam = sssf[fn].get(ACCESS_METHOD, BTREE)
                    if dbfpam == sfpam:
                        continue
                    if dbfpam == BTREE and sfpam == HASH:
                        continue
                    raise FileSpecError(msgam)
