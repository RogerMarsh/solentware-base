# base_3_to_4_db.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Upgrade a solentware_base version 3 database on Berkeley DB to version 4.

"""
import os

from ..core.segmentsize import SegmentSize
from ..core import constants
from . import base_3_to_4

# Not defined in .core.constants at version 3 and defined here with V3_ prefix.
# '_bits' and '_list' are combined in '_segment' at version 4.
V3_BITMAP_SEGMENT_SUFFIX = '_bits'
V3_LIST_SEGMENT_SUFFIX = '_list'


class Base_3_to_4_db(base_3_to_4.Base_3_to_4):
    
    """Convert solentware version 3 database to version 4 for Berkeley DB.
    """

    # Version 3 has a set of files each containing a table.
    @property
    def database_path_v3(self):
        return os.path.join(self.database)

    # Version 4 has a file containing all the tables. (Like SQLite 3).
    @property
    def database_path_v4(self):
        return os.path.join(self.database, os.path.basename(self.database))
    
    def get_missing_v3_tables(self):
        dbe = self.engine
        missing_tables = []
        for k, v in self.v3filemap.items():
            for n in v[:-1]:
                p = os.path.join(self.database_path_v3, n)
                if not os.path.exists(p):
                    missing_tables.append(n)
                    continue
                t = dbe.DB()
                try:
                    t.open(p, n, flags=dbe.DB_RDONLY)
                    t.close()
                except dbe.DBNoSuchFileError:
                    missing_tables.append(n)
                del t
            for n in v[-1].values():
                p = os.path.join(self.database_path_v3, n)
                if not os.path.exists(p):
                    missing_tables.append(n)
                    continue
                t = dbe.DB()
                try:
                    t.open(p,
                           n.partition(k[0] + constants.SUBFILE_DELIMITER)[-1],
                           flags=dbe.DB_RDONLY)
                    t.close()
                except dbe.DBNoSuchFileError:
                    missing_tables.append(n)
                del t
        return missing_tables
    
    # This does not matter at present because the existence of the file, where
    # the tables would be put, is sufficient to prevent the upgrade.
    def get_existing_v4_tables(self):
        dbe = self.engine
        existing_tables = []
        p = self.database_path_v4
        c = {constants.CONTROL_FILE: constants.CONTROL_FILE}
        for t4 in self.v4tablemap, self.v4existmap, self.v4segmentmap, c:
            for n in t4.values():
                t = dbe.DB()
                try:
                    t.open(p, n, flags=dbe.DB_RDONLY)
                    t.close()
                    existing_tables.append(n)
                except dbe.DBNoSuchFileError:
                    pass
                del t
        return existing_tables
    
    # Should be able to use the db_dump and db_load utilities for the first two
    # loops.  Cannot do this for the other two loops as the 'list' and 'bits'
    # databases are being merged into one 'segment' database for each table.
    # Records in the last loop's databases refer to 'segment' database records.
    def convert_v3_to_v4(self):
        dbe = self.engine
        v4tablemap = self.v4tablemap
        v4existmap = self.v4existmap
        v4segmentmap = self.v4segmentmap
        v3filemap = self.v3filemap
        v4p = self.database_path_v4
        v3p = self.database_path_v3
        for k3, v3 in self.v3filemap.items():

            # Data tables.
            t3 = dbe.DB()
            t3.open(os.path.join(v3p, v3[0]), v3[0], flags=dbe.DB_RDONLY)
            t4 = dbe.DB()
            t4.open(
                v4p, v4tablemap[k3], dbtype=dbe.DB_RECNO, flags=dbe.DB_CREATE)
            c3 = t3.cursor()
            while True:
                r3 = c3.next()
                if r3 is None:
                    break
                t4.put(*r3)
            c3.close()
            del c3
            t4.close()
            del t4
            t3.close()
            del t3

            # Existence bitmap segments.
            t3 = dbe.DB()
            t3.open(os.path.join(v3p, v3[1]), v3[1], flags=dbe.DB_RDONLY)
            t4 = dbe.DB()
            t4.set_re_pad(0)
            t4.set_re_len(SegmentSize.db_segment_size_bytes)
            t4.open(
                v4p, v4existmap[k3], dbtype=dbe.DB_RECNO, flags=dbe.DB_CREATE)
            c3 = t3.cursor()
            while True:
                r3 = c3.next()
                if r3 is None:
                    break
                t4.put(*r3)
            c3.close()
            t4.close()
            del t4
            t3.close()
            del t3

            # List segments.
            # These are in same database as bitmap segments at version 4.
            # List segment records retain their version 3 record numbers.
            t3 = dbe.DB()
            t3.open(os.path.join(v3p, v3[3]), v3[3], flags=dbe.DB_RDONLY)
            t4 = dbe.DB()
            t4.open(v4p,
                    v4segmentmap[k3],
                    dbtype=dbe.DB_RECNO,
                    flags=dbe.DB_CREATE)
            c3 = t3.cursor()
            while True:
                r3 = c3.next()
                if r3 is None:
                    break
                t4.put(*r3)
            c3.close()
            t4.close()
            del t4
            t3.close()
            del t3

            # Bitmap segments.
            # These are in same database as list segments at version 4.
            # Bitmap segment records get a new record number at version 4 so
            # the (version 3 key: version 4 key) mapping is noted for the
            # index stage where the version 4 key replaces the version key 3.
            t3 = dbe.DB()
            t3.open(os.path.join(v3p, v3[2]), v3[2], flags=dbe.DB_RDONLY)
            segment_recnum_map = {}
            t4 = dbe.DB()
            t4.open(v4p, v4segmentmap[k3])
            c3 = t3.cursor()
            while True:
                r3 = c3.next()
                if r3 is None:
                    break
                segment_recnum_map[r3[0]] = t4.append(r3[1])
            c3.close()
            t4.close()
            del t4
            t3.close()
            del t3

            # Index tables.
            # References to bitmap segment records are
            # constants.LENGTH_SEGMENT_BITARRAY_REFERENCE bytes (11) long.
            for name, table in v3[-1].items():
                n3 = table.partition(k3[0] + constants.SUBFILE_DELIMITER)[-1]
                t3 = dbe.DB()
                t3.open(os.path.join(v3p, table), n3, flags=dbe.DB_RDONLY)
                t4 = dbe.DB()
                t4.set_flags(dbe.DB_DUPSORT)
                t4.open(v4p, '_'.join((k3[0], name)),
                        dbtype=dbe.DB_BTREE,
                        flags=dbe.DB_CREATE)
                c3 = t3.cursor()
                while True:
                    r3 = c3.next()
                    if r3 is None:
                        break
                    d = r3[1]
                    if len(d) == constants.LENGTH_SEGMENT_BITARRAY_REFERENCE:
                        ref3 = int.from_bytes(d[-4:], byteorder='big')
                        ref4 = segment_recnum_map[ref3]
                        del segment_recnum_map[ref3]
                        d = d[:-4] + ref4.to_bytes(4, byteorder='big')
                    t4.put(r3[0], d)
                c3.close()
                del c3
                t4.close()
                del t4
                t3.close()
                del t3

        # Control table.
        t3 = dbe.DB()
        t3.open(os.path.join(v3p, constants.CONTROL_FILE),
                constants.CONTROL_FILE,
                flags=dbe.DB_RDONLY)
        t4 = dbe.DB()
        t4.set_flags(dbe.DB_DUPSORT)
        t4.open(v4p, constants.CONTROL_FILE,
                dbtype=dbe.DB_BTREE,
                flags=dbe.DB_CREATE)
        c3 = t3.cursor()
        while True:
            r3 = c3.next()
            if r3 is None:
                break
            t4.put(*r3)
        c3.close()
        del c3
        t4.put(constants.SPECIFICATION_KEY, repr(self.filespec))
        t4.put(constants.SEGMENT_SIZE_BYTES_KEY,
               repr(SegmentSize.db_segment_size_bytes))
        t4.close()
        t3.close()
        del t3

        # Delete files containing version 3 tables.
        # Use DB.remove() to be sure the file is a Berkeley DB database.
        for k3, v3 in self.v3filemap.items():

            # Data tables.
            dbe.DB().remove(os.path.join(v3p, v3[0]))

            # Existence bitmap segments.
            dbe.DB().remove(os.path.join(v3p, v3[1]))

            # List segments.
            # These are in same database as bitmap segments at version 4.
            # List segment records retain their version 3 record numbers.
            dbe.DB().remove(os.path.join(v3p, v3[3]))

            # Bitmap segments.
            # These are in same database as list segments at version 4.
            # Bitmap segment records get a new record number at version 4 so
            # the (version 3 key: version 4 key) mapping is noted for the
            # index stage where the version 4 key replaces the version key 3.
            dbe.DB().remove(os.path.join(v3p, v3[2]))

            # Index tables.
            # References to bitmap segment records are
            # constants.LENGTH_SEGMENT_BITARRAY_REFERENCE bytes (11) long.
            for name, table in v3[-1].items():
                dbe.DB().remove(os.path.join(v3p, table))

        # Control table.
        dbe.DB().remove(os.path.join(v3p, constants.CONTROL_FILE))
    
    def get_v3_segment_size(self):
        sizes = set()
        dbe = self.engine
        for em in self.v3existmap.values():
            t3 = dbe.DB()
            t3.open(os.path.join(self.database_path_v3, em),
                    em,
                    flags=dbe.DB_RDONLY)
            c3 = t3.cursor()
            while True:
                r3 = c3.next()
                if r3 is None:
                    break
                sizes.add(len(r3[1]))
            c3.close()
            t3.close()
            del t3
        if len(sizes) == 1:
            self.segment_size = sizes.pop()

    def _generate_v3_names(self):
        super()._generate_v3_names()
        tablemap = self.v3tablemap
        tables = self.v3tables
        v3filemap = self.v3filemap
        filemap = {}
        files = self.v3files
        existmap = self.v3existmap
        exists = self.v3exists
        segmentmap = self.v3segmentmap
        segments = self.v3segments
        files.add(constants.CONTROL_FILE)
        for k, v in self.filespec.items():
            primary = v[constants.PRIMARY]
            secondary = v[constants.SECONDARY]
            fields = set(v[constants.FIELDS].keys())
            fields.remove(primary)
            low = {}
            for ks, vs in secondary.items():
                if vs:
                    t = constants.SUBFILE_DELIMITER.join((k, vs))
                    tablemap[k, ks] = vs
                    tables.add(vs)
                    filemap[k, ks] = t
                    files.add(t)
                    fields.remove(vs)
                else:
                    low[ks.lower()] = ks
            for f in fields:
                lowf = low[f.lower()]
                t = constants.SUBFILE_DELIMITER.join((k, f))
                tablemap[k, lowf] = t
                tables.add(t)
                filemap[k, lowf] = t
                files.add(t)
            t = (constants.SUBFILE_DELIMITER.join((primary,
                                                   V3_BITMAP_SEGMENT_SUFFIX)),
                 constants.SUBFILE_DELIMITER.join((primary,
                                                   V3_LIST_SEGMENT_SUFFIX)))
            segmentmap[k,] = t
            segments.add(t)
            fn = (primary,
                  constants.SUBFILE_DELIMITER.join(
                      (primary, base_3_to_4.V3_EXISTENCE_BITMAP_SUFFIX)),
                  constants.SUBFILE_DELIMITER.join(
                      (primary, V3_BITMAP_SEGMENT_SUFFIX)),
                  constants.SUBFILE_DELIMITER.join((primary,
                                                    V3_LIST_SEGMENT_SUFFIX)),
                  {},
                  )
            filemap[k,] = fn
            files.update(fn[:-1])
        for k, v in filemap.items():
            if len(k) == 1:
                v3filemap[k] = v
        for k, v in filemap.items():
            if len(k) != 1:
                v3filemap[k[0],][-1][k[1]] = v

    def _generate_v4_names(self):
        super()._generate_v4_names()
        segmentmap = self.v4segmentmap
        segments = self.v4segments
        for k, v in self.filespec.items():
            t = constants.SUBFILE_DELIMITER.join((k, constants.SEGMENT_SUFFIX))
            segmentmap[k,] = t
            segments.add(t)
