# sortsequential.py
# Copyright 2026 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Create sorted index sequential files from database indicies.

Two classes are provided:

SortIndiciesToSequentialFiles
SortDPTIndiciesToSequentialFiles
"""

import os

from .segmentsize import SegmentSize
from .constants import SECONDARY, NEW_SEGMENT_CONTENT


class SortIndiciesToSequentialFiles:
    """Sort indicies by key and segment and write to sequential files.

    A file per segment per index, with the entries in each file in
    ascending key order, is written.

    The output files are in a format convenient for applying deferred index
    updates to all supported databases except DPT.
    """

    def __init__(self, database, file, ignore=None):
        """Extend and initialize deferred update data structures."""
        self.database = database
        self.file = file
        self.segment = None
        indicies = set(database.specification[file][SECONDARY])
        if ignore is not None:
            indicies.difference_update(ignore)
        self.indicies = {index: {} for index in indicies}

    def add_instance(self, instance):
        """Add the index references for instance."""
        value = instance.value.pack()[1]
        segment, key = divmod(instance.key.recno, SegmentSize.db_segment_size)
        count = None
        if segment != self.segment:
            if self.segment is not None:
                for index, reference in self.indicies.items():
                    self.write_segment_to_sequential_file(index, reference)
                    reference.clear()
                count = (self.segment + 1) * SegmentSize.db_segment_size
            self.segment = segment
        indicies = self.indicies
        for index, values in value.items():
            reference = indicies.get(index)
            if reference is not None:
                for item in values:
                    reference.setdefault(item, []).append(key)
        return count

    def write_segment_to_sequential_file(self, index, reference):
        """Write index references for segment to sequential file."""
        dump_directory = os.path.join(
            self.database.get_merge_import_sort_area(),
            "_".join(
                (
                    os.path.basename(
                        self.database.generate_database_file_name(self.file)
                    ),
                    self.file,
                )
            ),
            index,
        )
        if not os.path.isdir(dump_directory):
            if not os.path.isdir(os.path.dirname(dump_directory)):
                os.mkdir(os.path.dirname(dump_directory))
            os.mkdir(dump_directory)
        self._write_segment_to_sequential_file(
            reference, os.path.join(dump_directory, str(self.segment))
        )

    def _write_segment_to_sequential_file(self, reference, dump_file):
        """Write index references for segment to sequential file."""
        encode_record_selector = self.database.encode_record_selector
        encode_number = self.database.encode_number_for_sequential_file_dump
        encode_segment = self.database.encode_segment_for_sequential_file_dump
        segment = encode_number(self.segment, 4)
        with open(dump_file, mode="w", encoding="utf-8") as output:
            for key, value in sorted(reference.items()):
                output.write(
                    repr(
                        [
                            encode_record_selector(key),
                            segment,
                            NEW_SEGMENT_CONTENT,
                            encode_number(len(value), 2),
                            encode_segment(value),
                        ]
                    )
                    + "\n"
                )

    def write_final_segments_to_sequential_file(self):
        """Write final segments to sequential file."""
        for index, reference in self.indicies.items():
            self.write_segment_to_sequential_file(index, reference)
            reference.clear()
        guard_file = os.path.join(
            self.database.get_merge_import_sort_area(),
            "_".join(
                (
                    os.path.basename(
                        self.database.generate_database_file_name(self.file)
                    ),
                    self.file,
                )
            ),
            "0",
        )
        try:
            with open(guard_file, mode="wb"):
                pass
        except FileExistsError:
            pass


class SortDPTIndiciesToSequentialFiles(SortIndiciesToSequentialFiles):
    """Sort indicies by key and segment and write to sequential files.

    A file per segment per index, with the entries in each file in
    ascending key order, is written.

    The output files are in a format convenient for applying deferred index
    updates to a DPT database.
    """

    def _write_segment_to_sequential_file(self, reference, dump_file):
        """Write index references for segment to sequential file."""
        encode_record_selector = self.database.encode_record_selector
        segment = self.segment
        with open(dump_file, mode="w", encoding="utf-8") as output:
            for key, value in sorted(reference.items()):
                output.write(
                    repr(
                        [
                            encode_record_selector(key),
                            segment,
                            value,
                        ]
                    )
                    + "\n"
                )
