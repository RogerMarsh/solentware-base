# test__sqlitedu_encode.py
# Copyright 2025 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_sqlitedu _database tests for encoding data for index dump files.

Separated out from test__sqlitedu module because segment size cannot be
assumed to be the size defined on module import when unittest discovery
runs the tests.  Other tests make changes to the segment size for various
reasons.

This module must be in a directory without an __init__ module to remain
invisible to unittest discovery.

The tests are run by 'python -m <path to>.test__sqlite_encode'.
"""

import unittest

try:
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None

from .. import _sqlite
from .. import _sqlitedu
from ..segmentsize import SegmentSize


class _SQLitedu(unittest.TestCase):
    def setUp(self):
        self.__ssb = SegmentSize.db_segment_size_bytes

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


class DatabaseEncodeForDump(_SQLitedu):
    def setUp(self):
        super().setUp()
        self.database = self._D({}, folder="a")
        self.database.set_int_to_bytes_lookup()

    def t01_encode_number_for_sequential_file_dump(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_number_for_sequential_file_dump\(\) ",
                    r"missing 2 required positional arguments: ",
                    "'number' and 'bytes_'$",
                )
            ),
            self.database.encode_number_for_sequential_file_dump,
        )

    def t02_encode_number_for_sequential_file_dump(self):
        bytes_ = self.database.encode_number_for_sequential_file_dump(5, 3)
        self.assertEqual(bytes_, 5)

    def t03_encode_segment_for_sequential_file_dump(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_segment_for_sequential_file_dump\(\) ",
                    r"missing 1 required positional argument: ",
                    "'record_numbers'$",
                )
            ),
            self.database.encode_segment_for_sequential_file_dump,
        )

    def t04_encode_segment_for_sequential_file_dump(self):
        self.assertEqual(SegmentSize.db_upper_conversion_limit, 2000)
        bytes_ = self.database.encode_segment_for_sequential_file_dump([3, 4])
        self.assertEqual(bytes_, b"\x00\x03\x00\x04")

    def t05_encode_segment_for_sequential_file_dump(self):
        self.assertEqual(SegmentSize.db_upper_conversion_limit, 2000)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)
        recs = [n for n in range(SegmentSize.db_upper_conversion_limit + 1)]
        bytes_ = self.database.encode_segment_for_sequential_file_dump(recs)
        self.assertEqual(len(bytes_), SegmentSize.db_segment_size_bytes)

    def t06_encode_segment_for_sequential_file_dump(self):
        self.assertEqual(SegmentSize.db_upper_conversion_limit, 2000)
        bytes_ = self.database.encode_segment_for_sequential_file_dump([3])
        self.assertEqual(bytes_, 3)


if sqlite3:

    class _SQLiteduSqlite3(_SQLitedu):
        def setUp(self):
            super().setUp()

            class _D(_sqlitedu.Database, _sqlite.Database):
                def open_database(self, **k):
                    super().open_database(sqlite3, **k)

            self._D = _D

    class Database_encode_for_dumpSqlite3(_SQLiteduSqlite3):
        def setUp(self):
            super().setUp()
            self.database = self._D({}, folder="a")
            self.database.set_int_to_bytes_lookup()

        test_01 = (
            DatabaseEncodeForDump.t01_encode_number_for_sequential_file_dump
        )
        test_02 = (
            DatabaseEncodeForDump.t02_encode_number_for_sequential_file_dump
        )
        test_03 = (
            DatabaseEncodeForDump.t03_encode_segment_for_sequential_file_dump
        )
        test_04 = (
            DatabaseEncodeForDump.t04_encode_segment_for_sequential_file_dump
        )
        test_05 = (
            DatabaseEncodeForDump.t05_encode_segment_for_sequential_file_dump
        )
        test_06 = (
            DatabaseEncodeForDump.t06_encode_segment_for_sequential_file_dump
        )


if apsw:

    class _SQLiteduApsw(_SQLitedu):
        def setUp(self):
            super().setUp()

            class _D(_sqlitedu.Database, _sqlite.Database):
                def open_database(self, **k):
                    super().open_database(apsw, **k)

            self._D = _D

    class Database_encode_for_dumpApsw(_SQLiteduApsw):
        def setUp(self):
            super().setUp()
            self.database = self._D({}, folder="a")
            self.database.set_int_to_bytes_lookup()

        test_01 = (
            DatabaseEncodeForDump.t01_encode_number_for_sequential_file_dump
        )
        test_02 = (
            DatabaseEncodeForDump.t02_encode_number_for_sequential_file_dump
        )
        test_03 = (
            DatabaseEncodeForDump.t03_encode_segment_for_sequential_file_dump
        )
        test_04 = (
            DatabaseEncodeForDump.t04_encode_segment_for_sequential_file_dump
        )
        test_05 = (
            DatabaseEncodeForDump.t05_encode_segment_for_sequential_file_dump
        )
        test_06 = (
            DatabaseEncodeForDump.t06_encode_segment_for_sequential_file_dump
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if sqlite3:
        runner().run(loader(Database_encode_for_dumpSqlite3))
    if apsw:
        runner().run(loader(Database_encode_for_dumpApsw))
