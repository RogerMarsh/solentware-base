# test__nosql_segmentset.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_unqlite.SegmentsetCursor tests"""

import unittest
import os
from ast import literal_eval

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis = None

try:
    from ... import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from ... import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
from .. import _nosql
from .. import filespec
from ..segmentsize import SegmentSize
from .. import recordset

_NDBM_TEST_ROOT = "___ndbm_test_nosql_segmentset"
_GNU_TEST_ROOT = "___gnu_test_nosql_segmentset"


if ndbm_module:

    class Ndbm(ndbm_module.Ndbm):
        # test__nosql assumes database modules support memory-only databases,
        # but ndbm does not support them.
        def __init__(self, path=None):
            if path is None:
                path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            super().__init__(path=path)


if gnu_module:

    class Gnu(gnu_module.Gnu):
        # test__nosql assumes database modules support memory-only databases,
        # but gnu does not support them.
        def __init__(self, path=None):
            if path is None:
                path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            super().__init__(path=path)


class _NoSQL(unittest.TestCase):
    def setUp(self):
        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set differently for these engines.

        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(_nosql.Database):
            pass

        self._D = _D
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": {"field2"}}),
            segment_size_bytes=None,
        )
        self.database.specification["file2"]["fields"]["Field2"][
            "access_method"
        ] = "hash"
        self.database.open_database(*self._oda)

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb


class SegmentsetCursor:
    def setup_detail(self):
        segments = (
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"\x00\x42\x00\x43\x00\x44",
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        key = "a_o"
        db = self.database.dbenv
        self.database.trees["file1_field1"].insert(key)
        for e, s in enumerate(segments):
            db["1_1_1_" + str(e * 2 + 1) + "_" + key] = repr(segments[e])
        db["1_1_0_" + key] = repr(
            {1: ("B", 32), 3: ("L", 3), 5: ("B", 24), 7: (50, 1)}
        )
        self.segmentset = _nosql.SegmentsetCursor(
            self.database.dbenv, "1_1_0_", "1_1_1_", key
        )

    def t01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 5 positional arguments ",
                    "but 6 were given$",
                )
            ),
            _nosql.SegmentsetCursor,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"first\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"last\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"next\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"prev\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_current_segment\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.segmentset.get_current_segment,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"setat\(\) missing 1 required ",
                    "positional argument: 'segment_number'$",
                )
            ),
            self.segmentset.setat,
        )

    def t02_close(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.current_segment_number, None)
        self.assertIs(ss._dbenv, self.database.dbenv)
        ae(ss._index, "1_1_0_a_o")
        ae(ss._values_index, ("1_1_1_", "_a_o"))
        ae(ss.segments, {1: ("B", 32), 3: ("L", 3), 5: ("B", 24), 7: (50, 1)})
        ae(ss.sorted_segment_numbers, [1, 3, 5, 7])
        ae(ss.close(), None)
        ae(ss.current_segment_number, None)
        ae(ss._dbenv, None)
        ae(ss._index, None)
        ae(ss._values_index, None)
        ae(ss.segments, None)
        ae(ss.sorted_segment_numbers, None)

    def t03_first(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.first(), 1)
        ss.sorted_segment_numbers = []
        ae(ss.first(), None)

    def t04_last(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.last(), 7)
        ss.sorted_segment_numbers = []
        ae(ss.last(), None)

    def t05_next(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.next(), 1)
        ae(ss.current_segment_number, 1)
        ae(ss.next(), 3)
        ae(ss.current_segment_number, 3)
        ae(ss.next(), 5)
        ae(ss.current_segment_number, 5)
        ae(ss.next(), 7)
        ae(ss.current_segment_number, 7)
        ae(ss.next(), None)
        ae(ss.current_segment_number, 7)

    def t06_prev(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.prev(), 7)
        ae(ss.current_segment_number, 7)
        ae(ss.prev(), 5)
        ae(ss.current_segment_number, 5)
        ae(ss.prev(), 3)
        ae(ss.current_segment_number, 3)
        ae(ss.prev(), 1)
        ae(ss.current_segment_number, 1)
        ae(ss.prev(), None)
        ae(ss.current_segment_number, 1)

    def t07_setat(self):
        ae = self.assertEqual
        ss = self.segmentset
        aii = self.assertIsInstance
        ae(ss.setat(2), None)
        ae(ss.current_segment_number, None)
        ae(ss.prev(), 7)
        ae(ss.current_segment_number, 7)
        ae(ss.setat(2), None)
        ae(ss.current_segment_number, 7)
        aii(ss.setat(3), recordset.RecordsetSegmentList)

    def t08_get_current_segment(self):
        ae = self.assertEqual
        aii = self.assertIsInstance
        ss = self.segmentset
        ss.next()
        s = ss.get_current_segment()
        aii(s, recordset.RecordsetSegmentBitarray)
        ae(s.index_key, "a_o")
        ae(s.segment_number, 1)
        ae(
            s.tobytes(),
            b"".join(
                (
                    b"\xff\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        ss.next()
        s = ss.get_current_segment()
        aii(s, recordset.RecordsetSegmentList)
        ae(s.index_key, "a_o")
        ae(s.tobytes(), b"\x00\x42\x00\x43\x00\x44")
        ae(s.segment_number, 3)
        ae(s.list, [66, 67, 68])
        ss.next()
        s = ss.get_current_segment()
        aii(s, recordset.RecordsetSegmentBitarray)
        ae(s.index_key, "a_o")
        ae(s.segment_number, 5)
        ae(
            s.tobytes(),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        ss.next()
        s = ss.get_current_segment()
        aii(s, recordset.RecordsetSegmentInt)
        ae(s.index_key, "a_o")
        ae(s.segment_number, 7)
        ae(s.record_number, 50)

    def t09_count_records(self):
        ae = self.assertEqual
        ss = self.segmentset
        ae(ss.count_records(), 60)

    def t10_count_current_segment_records(self):
        ae = self.assertEqual
        ss = self.segmentset
        ss.next()
        ae(ss.count_current_segment_records(), 32)
        ss.next()
        ae(ss.count_current_segment_records(), 3)
        ss.next()
        ae(ss.count_current_segment_records(), 24)
        ss.next()
        ae(ss.count_current_segment_records(), 1)
        ae(ss.next(), None)


if gnu_module:

    class _NoSQLGnu(_NoSQL):
        def setUp(self):
            self._oda = gnu_module, Gnu, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
            path = os.path.join(os.path.dirname(__file__), _GNU_TEST_ROOT)
            if os.path.isfile(path):
                os.remove(path)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

    class SegmentsetCursorGnu(_NoSQLGnu):
        def setUp(self):
            super().setUp()
            SegmentsetCursor.setup_detail(self)

        def tearDown(self):
            self.segmentset.close()
            self.database.commit()
            super().tearDown()

        test_01 = SegmentsetCursor.t01
        test_02 = SegmentsetCursor.t02_close
        test_03 = SegmentsetCursor.t03_first
        test_04 = SegmentsetCursor.t04_last
        test_05 = SegmentsetCursor.t05_next
        test_06 = SegmentsetCursor.t06_prev
        test_07 = SegmentsetCursor.t07_setat
        test_08 = SegmentsetCursor.t08_get_current_segment
        test_09 = SegmentsetCursor.t09_count_records
        test_10 = SegmentsetCursor.t10_count_current_segment_records


if ndbm_module:

    class _NoSQLNdbm(_NoSQL):
        def setUp(self):
            self._oda = ndbm_module, Ndbm, None
            super().setUp()

        def tearDown(self):
            super().tearDown()
            # I have no idea why database teardown for gnu has to be like so:
            path = os.path.join(
                os.path.dirname(__file__), ".".join((_NDBM_TEST_ROOT, "db"))
            )
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)
            elif os.path.isfile(
                path
            ):  # Most tests, other two each have a few.
                os.remove(path)
            path = os.path.join(os.path.dirname(__file__), _NDBM_TEST_ROOT)
            if os.path.isdir(path):
                for f in os.listdir(path):
                    os.remove(os.path.join(path, f))
                os.rmdir(path)

    class SegmentsetCursorNdbm(_NoSQLNdbm):
        def setUp(self):
            super().setUp()
            SegmentsetCursor.setup_detail(self)

        def tearDown(self):
            self.segmentset.close()
            self.database.commit()
            super().tearDown()

        test_01 = SegmentsetCursor.t01
        test_02 = SegmentsetCursor.t02_close
        test_03 = SegmentsetCursor.t03_first
        test_04 = SegmentsetCursor.t04_last
        test_05 = SegmentsetCursor.t05_next
        test_06 = SegmentsetCursor.t06_prev
        test_07 = SegmentsetCursor.t07_setat
        test_08 = SegmentsetCursor.t08_get_current_segment
        test_09 = SegmentsetCursor.t09_count_records
        test_10 = SegmentsetCursor.t10_count_current_segment_records


if unqlite:

    class _NoSQLUnqlite(_NoSQL):
        def setUp(self):
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()

    class SegmentsetCursorUnqlite(_NoSQLUnqlite):
        def setUp(self):
            super().setUp()
            SegmentsetCursor.setup_detail(self)

        def tearDown(self):
            self.segmentset.close()
            self.database.commit()
            super().tearDown()

        test_01 = SegmentsetCursor.t01
        test_02 = SegmentsetCursor.t02_close
        test_03 = SegmentsetCursor.t03_first
        test_04 = SegmentsetCursor.t04_last
        test_05 = SegmentsetCursor.t05_next
        test_06 = SegmentsetCursor.t06_prev
        test_07 = SegmentsetCursor.t07_setat
        test_08 = SegmentsetCursor.t08_get_current_segment
        test_09 = SegmentsetCursor.t09_count_records
        test_10 = SegmentsetCursor.t10_count_current_segment_records


if vedis:

    class _NoSQLVedis(_NoSQL):
        def setUp(self):
            self._oda = vedis, vedis.Vedis, None
            super().setUp()

    class SegmentsetCursorVedis(_NoSQLVedis):
        def setUp(self):
            super().setUp()
            SegmentsetCursor.setup_detail(self)

        def tearDown(self):
            self.segmentset.close()
            self.database.commit()
            super().tearDown()

        test_01 = SegmentsetCursor.t01
        test_02 = SegmentsetCursor.t02_close
        test_03 = SegmentsetCursor.t03_first
        test_04 = SegmentsetCursor.t04_last
        test_05 = SegmentsetCursor.t05_next
        test_06 = SegmentsetCursor.t06_prev
        test_07 = SegmentsetCursor.t07_setat
        test_08 = SegmentsetCursor.t08_get_current_segment
        test_09 = SegmentsetCursor.t09_count_records
        test_10 = SegmentsetCursor.t10_count_current_segment_records


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if gnu_module:
        runner().run(loader(SegmentsetCursorGnu))
    if ndbm_module:
        runner().run(loader(SegmentsetCursorNdbm))
    if unqlite:
        runner().run(loader(SegmentsetCursorUnqlite))
    if vedis:
        runner().run(loader(SegmentsetCursorVedis))
