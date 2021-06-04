# _nosqldu.py
# Copyright (c) 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a supported nosql database with the appropriate module.

UnQLite, Vedis, dbm, and ndbm, are supported.

The dbm and ndbm modules are provided with Python; the unqlite and vedis
modules can be found in PyPI.

"""
from ast import literal_eval
from bisect import bisect_right

from .bytebit import Bitarray
from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    SEGMENT_VALUE_SUFFIX,
    SEGMENT_KEY_SUFFIX,
    LIST_BYTES,
    BITMAP_BYTES,
)
from .segmentsize import SegmentSize
from . import _databasedu
from .recordset import RecordsetSegmentList


class DatabaseError(Exception):
    """Exception for Database class."""

    pass


class Database(_databasedu.Database):
    """Customise _nosql.Database for deferred update.

    The class which chooses the interface to a nosql database must include
    this class earlier in the Method Resolution Order than _nosql.Database.

    Normally deferred updates are synchronised with adding the last record
    number to a segment.  Sometimes memory constraints will force deferred
    updates to be done more frequently, but this will likely increase the time
    taken to do the deferred updates for the second and later points in a
    segment.
    """

    def __init__(self, *a, **kw):
        """Extend and initialize deferred update data structures."""
        super().__init__(*a, **kw)
        self.deferred_update_points = None
        self.first_chunk = {}
        self.high_segment = {}
        self.initial_high_segment = {}
        self.existence_bit_maps = {}
        self.value_segments = {}  # was values in secondarydu.Secondary
        self._int_to_bytes = None

    def database_cursor(self, file, field, keyrange=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def start_transaction(self):
        """Do not start transaction in deferred update mode."""

    def backout(self):
        """Do not backout transaction in deferred update mode."""

    def commit(self):
        """Do not commit transaction in deferred update mode."""

    def do_final_segment_deferred_updates(self):
        """Do deferred updates for partially filled final segment."""
        # Write the final deferred segment database for each index
        for file in self.existence_bit_maps:
            high_record = self.ebm_control[file].high_record_number
            if high_record < 0:
                continue
            segment, record_number = divmod(
                high_record, SegmentSize.db_segment_size
            )
            if record_number in self.deferred_update_points:
                continue  # Assume put_instance did deferred updates
            self.write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self._int_to_bytes = [
            n.to_bytes(2, byteorder="big")
            for n in range(SegmentSize.db_segment_size)
        ]
        self.start_transaction()

        for file in self.specification:
            high_record = self.ebm_control[file].high_record_number
            if high_record is None:
                self.initial_high_segment[file] = None
                self.high_segment[file] = None
                self.first_chunk[file] = None
                continue
            segment, record = divmod(high_record, SegmentSize.db_segment_size)
            self.initial_high_segment[file] = segment
            self.high_segment[file] = segment
            self.first_chunk[file] = record < min(self.deferred_update_points)

    def unset_defer_update(self):
        """Tidy-up at end of deferred update run."""
        self._int_to_bytes = None
        self.first_chunk.clear()
        self.high_segment.clear()
        self.initial_high_segment.clear()
        self.commit()

    def write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment in file."""
        assert file in self.specification
        ebmc = self.ebm_control[file]
        tes = ebmc.table_ebm_segments
        insertion_point = bisect_right(tes, segment)
        self.dbenv[
            SUBFILE_DELIMITER.join((ebmc.ebm_table, str(segment)))
        ] = repr(self.existence_bit_maps[file][segment].tobytes())
        if not (tes and tes[insertion_point - 1] == segment):
            tes.insert(insertion_point, segment)
            self.dbenv[ebmc.ebm_table] = repr(tes)

    def sort_and_write(self, file, field, segment):
        """Sort the segment deferred updates before writing to database.

        Index updates are serialized as much as practical: meaning the lists
        or bitmaps of record numbers are put in a subsidiary table and the
        tables are written one after the other.

        """
        # Anything to do?
        if field not in self.value_segments[file]:
            return

        # Lookup table is much quicker, and noticeable, in bulk use.
        int_to_bytes = self._int_to_bytes

        segvalues = self.value_segments[file][field]

        # New records go into temporary databases, one for each segment, except
        # when filling the segment which was high when this update started.
        if (
            self.first_chunk[file]
            and self.initial_high_segment[file] != segment
        ):
            self.new_deferred_root(file, field)

        # The low segment in the import may have to be merged with an existing
        # high segment on the database, or the current segment in the import
        # may be done in chunks of less than a complete segment.
        # Note the difference between this code, and the similar code in module
        # apswduapi.py, and the code in module dbduapi.py: the Berkeley DB
        # code updates the main index directly if an entry already exists, but
        # the Sqlite code always updates a temporary table and merges into the
        # main table later.
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        tablename = self.table[fieldkey][-1]
        if fieldkey in self.trees:
            fieldtree = self.trees[fieldkey]
        else:
            fieldtree = None
        table_prefix = SUBFILE_DELIMITER.join((tablename, SEGMENT_KEY_SUFFIX))
        value_prefix = SUBFILE_DELIMITER.join(
            (tablename, SEGMENT_VALUE_SUFFIX, str(segment))
        )
        db = self.dbenv
        for k, value in segvalues.items():
            segment_key = SUBFILE_DELIMITER.join((value_prefix, k))
            table_key = SUBFILE_DELIMITER.join((table_prefix, k))
            if table_key not in db:
                if fieldtree:
                    fieldtree.insert(k)
                if isinstance(value, list):
                    db[table_key] = repr({segment: (LIST_BYTES, len(value))})
                    db[segment_key] = repr(
                        b"".join([int_to_bytes[n] for n in value])
                    )
                elif isinstance(value, Bitarray):
                    db[table_key] = repr(
                        {segment: (BITMAP_BYTES, value.count())}
                    )
                    db[segment_key] = repr(value.tobytes())
                else:
                    db[table_key] = repr({segment: (value, 1)})
                continue
            segment_table = literal_eval(db[table_key].decode())
            if segment in segment_table:
                type_, ref = segment_table[segment]
                if type_ == BITMAP_BYTES:
                    current_segment = self.populate_segment(
                        segment, literal_eval(db[segment_key].decode()), file
                    )
                elif type_ == LIST_BYTES:
                    current_segment = self.populate_segment(
                        segment, literal_eval(db[segment_key].decode()), file
                    )
                else:
                    current_segment = self.populate_segment(segment, ref, file)
                if isinstance(value, list):
                    segref = len(value), b"".join(
                        [int_to_bytes[n] for n in value]
                    )
                elif isinstance(value, Bitarray):
                    segref = value.count(), value.tobytes()
                else:
                    segref = 1, value
                seg = (
                    self.make_segment(k, segment, *segref) | current_segment
                ).normalize()
                if isinstance(seg, RecordsetSegmentList):
                    segment_table[segment] = LIST_BYTES, segref[0] + ref
                else:
                    segment_table[segment] = BITMAP_BYTES, segref[0] + ref
                db[table_key] = repr(segment_table)
                db[segment_key] = repr(seg.tobytes())
                continue
            if isinstance(value, list):
                segment_table[segment] = LIST_BYTES, len(value)
                db[segment_key] = repr(
                    b"".join([int_to_bytes[n] for n in value])
                )
            elif isinstance(value, Bitarray):
                segment_table[segment] = BITMAP_BYTES, value.count()
                db[segment_key] = repr(value.tobytes())
            else:
                segment_table[segment] = value, 1
            db[table_key] = repr(segment_table)
            continue
        segvalues.clear()

    def new_deferred_root(self, file, field):
        """Do nothing.

        Do nothing because populating main database will not be worse than
        using a sequence of small staging areas.

        Deferred update always uses the '-1' database so the main database is
        accessed automatically since it is the '0' database.
        """
        # The staging area technique may not make sense in the NoSQL situation.
        # The values, that's segments, have hash keys so there is no advantage
        # in staging the updates in a series of smaller trees and then updating
        # the main tree in key order.
        # Each key exists once in the main tree.  There seems little point in
        # duplicating common keys in many staging areas, and figuring which to
        # ignore when merging.  There will be a separate single record holding
        # a list of segment numbers, rather than many (key, value) records with
        # the same key.

    def merge(self, file, field):
        """Do nothing: there is nothing to do in _nosqldu module."""

    def get_ebm_segment(self, ebm_control, key):
        """Return existence bitmap for segment number 'key'."""
        return ebm_control.get_ebm_segment(key, self.dbenv)
