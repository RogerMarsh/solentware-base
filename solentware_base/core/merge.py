# merge.py
# Copyright 2024 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Merge sorted index sequential files and populate database indicies."""

import os
import heapq
from ast import literal_eval


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


def next_sorted_item(index_directory):
    """Yield sorted items from files in index_directory."""
    merger = Merge(index_directory)
    yield from merger.sorter()
