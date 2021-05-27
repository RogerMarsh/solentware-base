# base_3_to_4_sqlite.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Upgrade a solentware_base version 3 database on SQLite 3 to version 4.

"""
import os

from ..core.segmentsize import SegmentSize
from ..core import constants
from . import base_3_to_4


class Base_3_to_4_sqlite(base_3_to_4.Base_3_to_4):
    
    """Convert solentware version 3 database to version 4 for SQLite 3.
    """
    @property
    def database_path(self):
        return os.path.join(self.database, os.path.basename(self.database))
    
    # Calls to this method are, at present, the first time it becomes clear
    # a non-existent SQLite3 database is sought.
    def get_missing_v3_tables(self):
        dbe = self.engine
        missing_tables = []
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute('begin')
        try:
            for tm in self.v3tablemap, self.v3existmap, self.v3segmentmap:
                for t in tm.values():
                    try:
                        cursor.execute(
                            ' '.join(('create table', t, '(', t, ')')))
                        cursor.execute(' '.join(('drop table', t)))
                        missing_tables.append(t)
                    except Exception as exc:
                        if not str(exc).endswith(
                            t.join(('table ', ' already exists'))):
                            raise
        finally:
            cursor.execute('rollback')
            cursor.close()
            conn.close()
        return missing_tables
    
    # This matters because the tables are being renamed, in the same file, and
    # existence of a table with the new name is sufficient to prevent the
    # upgrade.  Existence of tables which have neither an old name nor a new
    # name is ignored.
    def get_existing_v4_tables(self):
        dbe = self.engine
        existing_tables = []
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute('begin')
        for t4, t3 in ((self.v4tablemap, self.v3tablemap),
                       (self.v4existmap, self.v3existmap),
                       (self.v4segmentmap, self.v3segmentmap)):
            for k, v in t4.items():
                try:
                    cursor.execute(' '.join(('create table', v, '(', v, ')')))
                    cursor.execute(' '.join(('drop table', v)))
                except Exception as exc:
                    if not str(exc).endswith(
                        v.join(('table ', ' already exists'))):
                        raise

                    # Table might not change name when case is ignored.
                    if not v.lower() == t3[k].lower():
                        existing_tables.append(
                            constants.SUBFILE_DELIMITER.join(k))

        cursor.execute('rollback')
        cursor.close()
        conn.close()
        return existing_tables
    
    def compose_sql_to_convert_v3_to_v4(self):

        # Alter table and column names: new at SQLite 3.25.0 and I have 3.27.0
        # but 3.7.9 documentation (need to look online!).
        # ALTER TABLE t RENAME TO nt
        # ALTER TABLE nt RENAME COLUMN c TO nc
        # COLUMN keyword is optional.
        sql = []
        v3t = self.v3tablemap
        v4t = self.v4tablemap
        for k, v in sorted(v3t.items()):
            vn = v4t[k]
            if v.lower() != vn.lower():
                sql.append(' '.join(('alter table', v, 'rename to', vn)))
            old = v3t[k[0],]
            new = v4t[k[0],]
            if len(k) > 1:
                if old.lower() != new.lower():
                    sql.append(
                        ' '.join(('alter table', vn, 'rename', old, 'to', new)))
                old = v.partition(old + constants.SUBFILE_DELIMITER)[-1]
                new = vn.partition(new + constants.SUBFILE_DELIMITER)[-1]
                if old.lower() != new.lower():
                    sql.append(
                        ' '.join(('alter table', vn, 'rename', old, 'to', new)))
            else:
                if old.lower() != new.lower():
                    sql.append(
                        ' '.join(('alter table', vn, 'rename', old, 'to', new)))
        v3e = self.v3existmap
        v4e = self.v4existmap
        for k, v in sorted(v3e.items()):
            vn = v4e[k]
            if v.lower() != vn.lower():
                sql.append(' '.join(('alter table', v, 'rename to', vn)))
                sql.append(' '.join(('alter table', vn, 'rename', v, 'to', vn)))
        v3s = self.v3segmentmap
        v4s = self.v4segmentmap
        for k, v in sorted(v3s.items()):
            vn = v4s[k]
            if v.lower() != vn.lower():
                sql.append(' '.join(('alter table', v, 'rename to', vn)))
        return sql
    
    def convert_v3_to_v4(self, sql):
        dbe = self.engine
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute('begin')
        for s in sql:
            try:
                cursor.execute(s)
            except Exception as exc:
                print(exc)
                pass
        statement = ' '.join((
            'insert into',
            constants.CONTROL_FILE,
            '(',
            constants.CONTROL_FILE, ',',
            constants.SQLITE_VALUE_COLUMN,
            ')',
            'values ( ? , ? )',
            ))
        cursor.execute(
            statement,
            (constants.SPECIFICATION_KEY, repr(self.filespec)))
        cursor.execute(
            statement,
            (constants.SEGMENT_SIZE_BYTES_KEY,
             repr(SegmentSize.db_segment_size_bytes)))
        cursor.execute('commit')
        cursor.close()
        conn.close()
    
    def get_v3_segment_size(self):
        sizes = set()
        dbe = self.engine
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        for em in self.v3existmap.values():
            statement = ' '.join((
                'select',
                constants.SQLITE_VALUE_COLUMN,
                'from',
                em,
                ))
            cursor.execute(statement)
            sizes.update(set(len(r[0]) for r in cursor.fetchall()))
        if len(sizes) == 1:
            self.segment_size = sizes.pop()

    def _generate_v3_names(self):
        super()._generate_v3_names()
        tablemap = self.v3tablemap
        tables = self.v3tables
        indexmap = self.v3indexmap
        indicies = self.v3indicies
        existmap = self.v3existmap
        exists = self.v3exists
        segmentmap = self.v3segmentmap
        segments = self.v3segments
        for k, v in self.filespec.items():
            primary = v[constants.PRIMARY]
            secondary = v[constants.SECONDARY]
            fields = set(v[constants.FIELDS].keys())
            fields.remove(primary)
            low = {}
            for ks, vs in secondary.items():
                if vs:
                    t = constants.SUBFILE_DELIMITER.join((primary, vs))
                    tablemap[k, ks] = t
                    tables.add(t)
                    it = ''.join((constants.INDEXPREFIX, t))
                    indexmap[k, ks] = it
                    indicies.add(it)
                    fields.remove(vs)
                else:
                    low[ks.lower()] = ks
            for f in fields:
                lowf = low[f.lower()]
                t = constants.SUBFILE_DELIMITER.join((primary, f))
                tablemap[k, lowf] = t
                tables.add(t)
                it = ''.join((constants.INDEXPREFIX, t))
                indexmap[k, lowf] = it
                indicies.add(it)
            t = ''.join((constants.SEGMENTPREFIX, primary))
            segmentmap[k,] = t
            segments.add(t)

    def _generate_v4_names(self):
        super()._generate_v4_names()
        indexmap = self.v4indexmap
        indicies = self.v4indicies
        segmentmap = self.v4segmentmap
        segments = self.v4segments
        for k, v in self.filespec.items():
            secondary = v[constants.SECONDARY]
            for ks, vs in secondary.items():
                it = ''.join((constants.INDEXPREFIX,
                              constants.SUBFILE_DELIMITER.join((k, ks))))
                indexmap[k, ks] = it
                indicies.add(it)
            t = constants.SUBFILE_DELIMITER.join((k, constants.SEGMENT_SUFFIX))
            segmentmap[k,] = t
            segments.add(t)
