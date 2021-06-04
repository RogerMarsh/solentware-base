# base_3_to_4_sqlite.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Upgrade a solentware_base version 3 database on SQLite 3 to version 4."""
import os

from ..core.segmentsize import SegmentSize
from ..core import constants
from . import base_3_to_4


class Base_3_to_4_sqlite(base_3_to_4.Base_3_to_4):
    """Convert solentware version 3 database to version 4 for SQLite 3."""

    @property
    def database_path(self):
        """Return path to database."""
        return os.path.join(self.database, os.path.basename(self.database))

    # Calls to this method are, at present, the first time it becomes clear
    # a non-existent SQLite3 database is sought.
    def get_missing_v3_tables(self):
        """Return specified solentware_base 3 table names that do not exist."""
        dbe = self.engine
        missing_tables = []
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute("begin")
        try:
            for v3m in self.v3tablemap, self.v3existmap, self.v3segmentmap:
                for name in v3m.values():
                    try:
                        cursor.execute(
                            " ".join(("create table", name, "(", name, ")"))
                        )
                        cursor.execute(" ".join(("drop table", name)))
                        missing_tables.append(name)
                    except Exception as exc:
                        if not str(exc).endswith(
                            name.join(("table ", " already exists"))
                        ):
                            raise
        finally:
            cursor.execute("rollback")
            cursor.close()
            conn.close()
        return missing_tables

    # This matters because the tables are being renamed, in the same file, and
    # existence of a table with the new name is sufficient to prevent the
    # upgrade.  Existence of tables which have neither an old name nor a new
    # name is ignored.
    def get_existing_v4_tables(self):
        """Return table names already in solentware_base 4 format."""
        dbe = self.engine
        existing_tables = []
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute("begin")
        for t4m, t3m in (
            (self.v4tablemap, self.v3tablemap),
            (self.v4existmap, self.v3existmap),
            (self.v4segmentmap, self.v3segmentmap),
        ):
            for k, value in t4m.items():
                try:
                    cursor.execute(
                        " ".join(("create table", value, "(", value, ")"))
                    )
                    cursor.execute(" ".join(("drop table", value)))
                except Exception as exc:
                    if not str(exc).endswith(
                        value.join(("table ", " already exists"))
                    ):
                        raise

                    # Table might not change name when case is ignored.
                    if not value.lower() == t3m[k].lower():
                        existing_tables.append(
                            constants.SUBFILE_DELIMITER.join(k)
                        )

        cursor.execute("rollback")
        cursor.close()
        conn.close()
        return existing_tables

    def compose_sql_to_convert_v3_to_v4(self):
        """Return SQL to covert table names to solentware_base 4 format."""
        # Alter table and column names: new at SQLite 3.25.0 and I have 3.27.0
        # but 3.7.9 documentation (need to look online!).
        # ALTER TABLE t RENAME TO nt
        # ALTER TABLE nt RENAME COLUMN c TO nc
        # COLUMN keyword is optional.
        sql = []
        v3t = self.v3tablemap
        v4t = self.v4tablemap
        for k, value in sorted(v3t.items()):
            vname = v4t[k]
            if value.lower() != vname.lower():
                sql.append(
                    " ".join(("alter table", value, "rename to", vname))
                )
            old = v3t[
                k[0],
            ]
            new = v4t[
                k[0],
            ]
            if len(k) > 1:
                if old.lower() != new.lower():
                    sql.append(
                        " ".join(
                            ("alter table", vname, "rename", old, "to", new)
                        )
                    )
                old = value.partition(old + constants.SUBFILE_DELIMITER)[-1]
                new = vname.partition(new + constants.SUBFILE_DELIMITER)[-1]
                if old.lower() != new.lower():
                    sql.append(
                        " ".join(
                            ("alter table", vname, "rename", old, "to", new)
                        )
                    )
            else:
                if old.lower() != new.lower():
                    sql.append(
                        " ".join(
                            ("alter table", vname, "rename", old, "to", new)
                        )
                    )
        v3e = self.v3existmap
        v4e = self.v4existmap
        for k, value in sorted(v3e.items()):
            vname = v4e[k]
            if value.lower() != vname.lower():
                sql.append(
                    " ".join(("alter table", value, "rename to", vname))
                )
                sql.append(
                    " ".join(
                        ("alter table", vname, "rename", value, "to", vname)
                    )
                )
        v3s = self.v3segmentmap
        v4s = self.v4segmentmap
        for k, value in sorted(v3s.items()):
            vname = v4s[k]
            if value.lower() != vname.lower():
                sql.append(
                    " ".join(("alter table", value, "rename to", vname))
                )
        return sql

    def convert_v3_to_v4(self, sql):
        """Convert database to solentwareware_base 4 format.

        Store the database specification and segment size in control table.

        """
        dbe = self.engine
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        cursor.execute("begin")
        for statement in sql:
            try:
                cursor.execute(statement)
            except Exception as exc:
                print(exc)
                pass
        statement = " ".join(
            (
                "insert into",
                constants.CONTROL_FILE,
                "(",
                constants.CONTROL_FILE,
                ",",
                constants.SQLITE_VALUE_COLUMN,
                ")",
                "values ( ? , ? )",
            )
        )
        cursor.execute(
            statement, (constants.SPECIFICATION_KEY, repr(self.filespec))
        )
        cursor.execute(
            statement,
            (
                constants.SEGMENT_SIZE_BYTES_KEY,
                repr(SegmentSize.db_segment_size_bytes),
            ),
        )
        cursor.execute("commit")
        cursor.close()
        conn.close()

    def get_v3_segment_size(self):
        """Set database segment size recorded on database.

        self.segment_size is set to the segment size provided only one
        segment size is recorded on database.

        """
        sizes = set()
        dbe = self.engine
        conn = dbe.Connection(self.database_path)
        cursor = conn.cursor()
        for emv in self.v3existmap.values():
            statement = " ".join(
                (
                    "select",
                    constants.SQLITE_VALUE_COLUMN,
                    "from",
                    emv,
                )
            )
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
        segmentmap = self.v3segmentmap
        segments = self.v3segments
        for k, value in self.filespec.items():
            primary = value[constants.PRIMARY]
            secondary = value[constants.SECONDARY]
            fields = set(value[constants.FIELDS].keys())
            fields.remove(primary)
            low = {}
            for ksec, vsec in secondary.items():
                if vsec:
                    name = constants.SUBFILE_DELIMITER.join((primary, vsec))
                    tablemap[k, ksec] = name
                    tables.add(name)
                    item = "".join((constants.INDEXPREFIX, name))
                    indexmap[k, ksec] = item
                    indicies.add(item)
                    fields.remove(vsec)
                else:
                    low[ksec.lower()] = ksec
            for fname in fields:
                lowf = low[fname.lower()]
                name = constants.SUBFILE_DELIMITER.join((primary, fname))
                tablemap[k, lowf] = name
                tables.add(name)
                item = "".join((constants.INDEXPREFIX, name))
                indexmap[k, lowf] = item
                indicies.add(item)
            name = "".join((constants.SEGMENTPREFIX, primary))
            segmentmap[
                k,
            ] = name
            segments.add(name)

    def _generate_v4_names(self):
        super()._generate_v4_names()
        indexmap = self.v4indexmap
        indicies = self.v4indicies
        segmentmap = self.v4segmentmap
        segments = self.v4segments
        for k, value in self.filespec.items():
            secondary = value[constants.SECONDARY]
            for ksec in secondary:
                item = "".join(
                    (
                        constants.INDEXPREFIX,
                        constants.SUBFILE_DELIMITER.join((k, ksec)),
                    )
                )
                indexmap[k, ksec] = item
                indicies.add(item)
            name = constants.SUBFILE_DELIMITER.join(
                (k, constants.SEGMENT_SUFFIX)
            )
            segmentmap[
                k,
            ] = name
            segments.add(name)
