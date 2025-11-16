# merge.py
# Copyright 2024 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Merge sorted index sequential files and populate database indicies."""

import os
import heapq
from ast import literal_eval

from .segmentsize import SegmentSize
from .constants import SECONDARY, NEW_SEGMENT_CONTENT


class _Reader:
    """Yield lines read from dump_file."""

    def __init__(self, dump_file):
        """Set dump file name."""
        self.dump_file = dump_file
        self.file = None

    def open_file(self):
        """Yield line read from file."""
        # pylint message R1732, 'consider-using-with' ignored for now.
        # Is it possible to work this into the Merge.sorter() method?
        self.file = open(self.dump_file, mode="r", encoding="utf-8")


class Merge:
    """Merge index files in directory.

    The index files are those with digit names and the one named with the
    basename of dump_directory.
    """

    def __init__(self, dump_directory):
        """Set merge file names."""
        directory = os.path.basename(dump_directory)
        dumps = [
            name
            for name in os.listdir(dump_directory)
            if (
                (name == directory or name.isdigit())
                and os.path.isfile(os.path.join(dump_directory, name))
            )
        ]
        self.readers = {
            name: _Reader(os.path.join(dump_directory, name)) for name in dumps
        }

    def sorter(self):
        """Yield lines in sorted order."""
        heappush = heapq.heappush
        heappop = heapq.heappop
        empty = set()
        items = []
        for name, reader in self.readers.items():
            reader.open_file()
            line = reader.file.readline()
            if not line:
                reader.file.close()
                empty.add(name)
                continue
            heappush(items, (literal_eval(line), name))
        for name in empty:
            del self.readers[name]
        readers = self.readers
        while True:
            try:
                item, name = heappop(items)
            except IndexError:
                break
            yield item
            line = readers[name].file.readline()
            if not line:
                readers[name].file.close()
                del self.readers[name]
                continue
            heappush(items, (literal_eval(line), name))


class SortIndiciesToSequentialFiles:
    """Sort indicies by key and segment and write to sequential files.

    A file per segment per index, with the entries in each file in
    ascending key order, is written.
    """

    def __init__(self, database, file, ignore=None):
        """Extend and initialize deferred update data structures."""
        self.database = database
        self.file = file
        self.dump_file = None
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
        database = self.database
        encode_record_selector = database.encode_record_selector
        encode_number = database.encode_number_for_sequential_file_dump
        encode_segment = database.encode_segment_for_sequential_file_dump
        dump_directory = os.path.join(
            database.get_merge_import_sort_area(),
            "_".join((os.path.basename(database.database_file), self.file)),
            index,
        )
        if not os.path.isdir(dump_directory):
            if not os.path.isdir(os.path.dirname(dump_directory)):
                os.mkdir(os.path.dirname(dump_directory))
            os.mkdir(dump_directory)
        self.dump_file = os.path.join(dump_directory, str(self.segment))
        segment = self.segment
        with open(self.dump_file, mode="w", encoding="utf-8") as output:
            for key, value in sorted(reference.items()):
                output.write(
                    repr(
                        [
                            encode_record_selector(key),
                            encode_number(segment, 4),
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
                (os.path.basename(self.database.database_file), self.file)
            ),
            "0",
        )
        try:
            with open(guard_file, mode="wb") as output:
                pass
        except FileExistsError:
            pass
