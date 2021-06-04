# base_3_to_4_db.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Upgrade a solentware_base version 3 database on Berkeley DB to version 4.

The Berkeley DB databases are moved from their 'one database per file'
organization to an 'all databases on one file' organization.

"""
import os

from ..core.segmentsize import SegmentSize
from ..core import constants
from . import base_3_to_4

# Not defined in .core.constants at version 3 and defined here with V3_ prefix.
# '_bits' and '_list' are combined in '_segment' at version 4.
V3_BITMAP_SEGMENT_SUFFIX = "_bits"
V3_LIST_SEGMENT_SUFFIX = "_list"


class Base_3_to_4_db(base_3_to_4.Base_3_to_4):
    """Convert solentware version 3 database to version 4 for Berkeley DB."""

    # Version 3 has a set of files each containing a table.
    @property
    def database_path_v3(self):
        """Return path to solentware_base 3 database."""
        return os.path.join(self.database)

    # Version 4 has a file containing all the tables. (Like SQLite 3).
    @property
    def database_path_v4(self):
        """Return path to solentware_base 4 database."""
        return os.path.join(self.database, os.path.basename(self.database))

    def get_missing_v3_tables(self):
        """Return specified solentware_base 3 table names that do not exist."""
        dbe = self.engine
        missing_tables = []
        for k, value in self.v3filemap.items():
            for filename in value[:-1]:
                pathname = os.path.join(self.database_path_v3, filename)
                if not os.path.exists(pathname):
                    missing_tables.append(filename)
                    continue
                dbedb = dbe.DB()
                try:
                    dbedb.open(pathname, filename, flags=dbe.DB_RDONLY)
                    dbedb.close()
                except dbe.DBNoSuchFileError:
                    missing_tables.append(filename)
                del dbedb
            for filename in value[-1].values():
                pathname = os.path.join(self.database_path_v3, filename)
                if not os.path.exists(pathname):
                    missing_tables.append(filename)
                    continue
                dbedb = dbe.DB()
                try:
                    dbedb.open(
                        pathname,
                        filename.partition(k[0] + constants.SUBFILE_DELIMITER)[
                            -1
                        ],
                        flags=dbe.DB_RDONLY,
                    )
                    dbedb.close()
                except dbe.DBNoSuchFileError:
                    missing_tables.append(filename)
                del dbedb
        return missing_tables

    # This does not matter at present because the existence of the file, where
    # the tables would be put, is sufficient to prevent the upgrade.
    def get_existing_v4_tables(self):
        """Return table names already in solentware_base 4 format."""
        dbe = self.engine
        existing_tables = []
        pathname = self.database_path_v4
        control = {constants.CONTROL_FILE: constants.CONTROL_FILE}
        for v4t in (
            self.v4tablemap,
            self.v4existmap,
            self.v4segmentmap,
            control,
        ):
            for filename in v4t.values():
                dbedb = dbe.DB()
                try:
                    dbedb.open(pathname, filename, flags=dbe.DB_RDONLY)
                    dbedb.close()
                    existing_tables.append(filename)
                except dbe.DBNoSuchFileError:
                    pass
                del dbedb
        return existing_tables

    # Should be able to use the db_dump and db_load utilities for the first two
    # loops.  Cannot do this for the other two loops as the 'list' and 'bits'
    # databases are being merged into one 'segment' database for each table.
    # Records in the last loop's databases refer to 'segment' database records.
    def convert_v3_to_v4(self):
        """Convert database to solentwareware_base 4 format.

        Store the database specification and segment size in control table.

        """
        dbe = self.engine
        v4tablemap = self.v4tablemap
        v4existmap = self.v4existmap
        v4segmentmap = self.v4segmentmap
        v4p = self.database_path_v4
        v3p = self.database_path_v3
        for k3fm, v3fm in self.v3filemap.items():

            # Data tables.
            dbedb3 = dbe.DB()
            dbedb3.open(
                os.path.join(v3p, v3fm[0]), v3fm[0], flags=dbe.DB_RDONLY
            )
            dbedb4 = dbe.DB()
            dbedb4.open(
                v4p, v4tablemap[k3fm], dbtype=dbe.DB_RECNO, flags=dbe.DB_CREATE
            )
            cursor3 = dbedb3.cursor()
            while True:
                record3 = cursor3.next()
                if record3 is None:
                    break
                dbedb4.put(*record3)
            cursor3.close()
            del cursor3
            dbedb4.close()
            del dbedb4
            dbedb3.close()
            del dbedb3

            # Existence bitmap segments.
            dbedb3 = dbe.DB()
            dbedb3.open(
                os.path.join(v3p, v3fm[1]), v3fm[1], flags=dbe.DB_RDONLY
            )
            dbedb4 = dbe.DB()
            dbedb4.set_re_pad(0)
            dbedb4.set_re_len(SegmentSize.db_segment_size_bytes)
            dbedb4.open(
                v4p, v4existmap[k3fm], dbtype=dbe.DB_RECNO, flags=dbe.DB_CREATE
            )
            cursor3 = dbedb3.cursor()
            while True:
                record3 = cursor3.next()
                if record3 is None:
                    break
                dbedb4.put(*record3)
            cursor3.close()
            dbedb4.close()
            del dbedb4
            dbedb3.close()
            del dbedb3

            # List segments.
            # These are in same database as bitmap segments at version 4.
            # List segment records retain their version 3 record numbers.
            dbedb3 = dbe.DB()
            dbedb3.open(
                os.path.join(v3p, v3fm[3]), v3fm[3], flags=dbe.DB_RDONLY
            )
            dbedb4 = dbe.DB()
            dbedb4.open(
                v4p,
                v4segmentmap[k3fm],
                dbtype=dbe.DB_RECNO,
                flags=dbe.DB_CREATE,
            )
            cursor3 = dbedb3.cursor()
            while True:
                record3 = cursor3.next()
                if record3 is None:
                    break
                dbedb4.put(*record3)
            cursor3.close()
            dbedb4.close()
            del dbedb4
            dbedb3.close()
            del dbedb3

            # Bitmap segments.
            # These are in same database as list segments at version 4.
            # Bitmap segment records get a new record number at version 4 so
            # the (version 3 key: version 4 key) mapping is noted for the
            # index stage where the version 4 key replaces the version key 3.
            dbedb3 = dbe.DB()
            dbedb3.open(
                os.path.join(v3p, v3fm[2]), v3fm[2], flags=dbe.DB_RDONLY
            )
            segment_recnum_map = {}
            dbedb4 = dbe.DB()
            dbedb4.open(v4p, v4segmentmap[k3fm])
            cursor3 = dbedb3.cursor()
            while True:
                record3 = cursor3.next()
                if record3 is None:
                    break
                segment_recnum_map[record3[0]] = dbedb4.append(record3[1])
            cursor3.close()
            dbedb4.close()
            del dbedb4
            dbedb3.close()
            del dbedb3

            # Index tables.
            # References to bitmap segment records are
            # constants.LENGTH_SEGMENT_BITARRAY_REFERENCE bytes (11) long.
            for name, table in v3fm[-1].items():
                tn3 = table.partition(k3fm[0] + constants.SUBFILE_DELIMITER)[
                    -1
                ]
                dbedb3 = dbe.DB()
                dbedb3.open(os.path.join(v3p, table), tn3, flags=dbe.DB_RDONLY)
                dbedb4 = dbe.DB()
                dbedb4.set_flags(dbe.DB_DUPSORT)
                dbedb4.open(
                    v4p,
                    "_".join((k3fm[0], name)),
                    dbtype=dbe.DB_BTREE,
                    flags=dbe.DB_CREATE,
                )
                cursor3 = dbedb3.cursor()
                while True:
                    record3 = cursor3.next()
                    if record3 is None:
                        break
                    segref = record3[1]
                    if (
                        len(segref)
                        == constants.LENGTH_SEGMENT_BITARRAY_REFERENCE
                    ):
                        ref3 = int.from_bytes(segref[-4:], byteorder="big")
                        ref4 = segment_recnum_map[ref3]
                        del segment_recnum_map[ref3]
                        segref = segref[:-4] + ref4.to_bytes(
                            4, byteorder="big"
                        )
                    dbedb4.put(record3[0], segref)
                cursor3.close()
                del cursor3
                dbedb4.close()
                del dbedb4
                dbedb3.close()
                del dbedb3

        # Control table.
        dbedb3 = dbe.DB()
        dbedb3.open(
            os.path.join(v3p, constants.CONTROL_FILE),
            constants.CONTROL_FILE,
            flags=dbe.DB_RDONLY,
        )
        dbedb4 = dbe.DB()
        dbedb4.set_flags(dbe.DB_DUPSORT)
        dbedb4.open(
            v4p,
            constants.CONTROL_FILE,
            dbtype=dbe.DB_BTREE,
            flags=dbe.DB_CREATE,
        )
        cursor3 = dbedb3.cursor()
        while True:
            record3 = cursor3.next()
            if record3 is None:
                break
            dbedb4.put(*record3)
        cursor3.close()
        del cursor3
        dbedb4.put(constants.SPECIFICATION_KEY, repr(self.filespec))
        dbedb4.put(
            constants.SEGMENT_SIZE_BYTES_KEY,
            repr(SegmentSize.db_segment_size_bytes),
        )
        dbedb4.close()
        dbedb3.close()
        del dbedb3

        # Delete files containing version 3 tables.
        # Use DB.remove() to be sure the file is a Berkeley DB database.
        for v3fm in self.v3filemap.values():

            # Data tables.
            dbe.DB().remove(os.path.join(v3p, v3fm[0]))

            # Existence bitmap segments.
            dbe.DB().remove(os.path.join(v3p, v3fm[1]))

            # List segments.
            # These are in same database as bitmap segments at version 4.
            # List segment records retain their version 3 record numbers.
            dbe.DB().remove(os.path.join(v3p, v3fm[3]))

            # Bitmap segments.
            # These are in same database as list segments at version 4.
            # Bitmap segment records get a new record number at version 4 so
            # the (version 3 key: version 4 key) mapping is noted for the
            # index stage where the version 4 key replaces the version key 3.
            dbe.DB().remove(os.path.join(v3p, v3fm[2]))

            # Index tables.
            # References to bitmap segment records are
            # constants.LENGTH_SEGMENT_BITARRAY_REFERENCE bytes (11) long.
            for name, table in v3fm[-1].items():
                dbe.DB().remove(os.path.join(v3p, table))

        # Control table.
        dbe.DB().remove(os.path.join(v3p, constants.CONTROL_FILE))

    def get_v3_segment_size(self):
        """Set database segment size recorded on database.

        self.segment_size is set to the segment size provided only one
        segment size is recorded on database.

        """
        sizes = set()
        dbe = self.engine
        for emv in self.v3existmap.values():
            dbedb3 = dbe.DB()
            dbedb3.open(
                os.path.join(self.database_path_v3, emv),
                emv,
                flags=dbe.DB_RDONLY,
            )
            cursor3 = dbedb3.cursor()
            while True:
                record3 = cursor3.next()
                if record3 is None:
                    break
                sizes.add(len(record3[1]))
            cursor3.close()
            dbedb3.close()
            del dbedb3
        if len(sizes) == 1:
            self.segment_size = sizes.pop()

    def _generate_v3_names(self):
        super()._generate_v3_names()
        tablemap = self.v3tablemap
        tables = self.v3tables
        v3filemap = self.v3filemap
        filemap = {}
        files = self.v3files
        segmentmap = self.v3segmentmap
        segments = self.v3segments
        files.add(constants.CONTROL_FILE)
        for k, value in self.filespec.items():
            primary = value[constants.PRIMARY]
            secondary = value[constants.SECONDARY]
            fields = set(value[constants.FIELDS].keys())
            fields.remove(primary)
            low = {}
            for ksec, vsec in secondary.items():
                if vsec:
                    name = constants.SUBFILE_DELIMITER.join((k, vsec))
                    tablemap[k, ksec] = vsec
                    tables.add(vsec)
                    filemap[k, ksec] = name
                    files.add(name)
                    fields.remove(vsec)
                else:
                    low[ksec.lower()] = ksec
            for fname in fields:
                lowf = low[fname.lower()]
                name = constants.SUBFILE_DELIMITER.join((k, fname))
                tablemap[k, lowf] = name
                tables.add(name)
                filemap[k, lowf] = name
                files.add(name)
            name = (
                constants.SUBFILE_DELIMITER.join(
                    (primary, V3_BITMAP_SEGMENT_SUFFIX)
                ),
                constants.SUBFILE_DELIMITER.join(
                    (primary, V3_LIST_SEGMENT_SUFFIX)
                ),
            )
            segmentmap[
                k,
            ] = name
            segments.add(name)
            fnt = (
                primary,
                constants.SUBFILE_DELIMITER.join(
                    (primary, base_3_to_4.V3_EXISTENCE_BITMAP_SUFFIX)
                ),
                constants.SUBFILE_DELIMITER.join(
                    (primary, V3_BITMAP_SEGMENT_SUFFIX)
                ),
                constants.SUBFILE_DELIMITER.join(
                    (primary, V3_LIST_SEGMENT_SUFFIX)
                ),
                {},
            )
            filemap[
                k,
            ] = fnt
            files.update(fnt[:-1])
        for k, value in filemap.items():
            if len(k) == 1:
                v3filemap[k] = value
        for k, value in filemap.items():
            if len(k) != 1:
                v3filemap[k[0],][
                    -1
                ][k[1]] = value

    def _generate_v4_names(self):
        super()._generate_v4_names()
        segmentmap = self.v4segmentmap
        segments = self.v4segments
        for k in self.filespec:
            name = constants.SUBFILE_DELIMITER.join(
                (k, constants.SEGMENT_SUFFIX)
            )
            segmentmap[
                k,
            ] = name
            segments.add(name)
