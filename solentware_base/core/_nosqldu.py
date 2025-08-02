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


class DatabaseError(_databasedu.DatabaseduError):
    """Exception for Database class."""


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

    def database_cursor(self, file, field, keyrange=None, recordset=None):
        """Not implemented for deferred update."""
        raise DatabaseError("database_cursor not implemented")

    def deferred_update_housekeeping(self):
        """Override to commit transaction for segment.

        In the various engines with the _nosqldu API this is not essential,
        but is done for compatibility with Berkeley DB where it is necessary
        to prune log files frequently.  In some engines start_transaction
        does nothing.  In some engines commit either does nothing or just
        synchronizes the database with memory.

        Applications should extend this method as required: perhaps to
        record progress at commit time to assist restart.

        """
        self._commit_on_housekeeping()

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
            self._write_existence_bit_map(file, segment)
            for secondary in self.specification[file][SECONDARY]:
                self.sort_and_write(file, secondary, segment)
                self.merge(file, secondary)

    def set_defer_update(self):
        """Prepare to do deferred update run."""
        self.set_int_to_bytes_lookup(lookup=True)
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
        self.set_int_to_bytes_lookup(lookup=False)
        self.first_chunk.clear()
        self.high_segment.clear()
        self.initial_high_segment.clear()
        self.commit()

    def _write_existence_bit_map(self, file, segment):
        """Write the existence bit map for segment in file."""
        assert file in self.specification
        ebmc = self.ebm_control[file]
        tes = ebmc.table_ebm_segments
        insertion_point = bisect_right(tes, segment)
        self.dbenv[SUBFILE_DELIMITER.join((ebmc.ebm_table, str(segment)))] = (
            repr(self.existence_bit_maps[file][segment].tobytes())
        )
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
        # Note this module implements sort_and_write in a different way to
        # _dbdu, _dbdu_tkinter, _lmdbdu, and _sqlitedu.
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        tablename = self.table[fieldkey]
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
                    if len(value) == 1:
                        db[table_key] = repr({segment: (value[-1], 1)})
                        continue
                    db[table_key] = repr({segment: (LIST_BYTES, len(value))})
                    db[segment_key] = repr(
                        b"".join([int_to_bytes[n] for n in value])
                    )
                    continue
                db[table_key] = repr({segment: (BITMAP_BYTES, value.count())})
                db[segment_key] = repr(value.tobytes())
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
                    if len(value) == 1:
                        segref = (1, value[-1])
                    else:
                        segref = len(value), b"".join(
                            [int_to_bytes[n] for n in value]
                        )
                else:
                    segref = value.count(), value.tobytes()
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
                if len(value) == 1:
                    segment_table[segment] = (value[-1], 1)
                else:
                    segment_table[segment] = LIST_BYTES, len(value)
                    db[segment_key] = repr(
                        b"".join([int_to_bytes[n] for n in value])
                    )
            else:
                segment_table[segment] = BITMAP_BYTES, value.count()
                db[segment_key] = repr(value.tobytes())
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
