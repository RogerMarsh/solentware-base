# test_recordset.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for RecordsetCursor class"""

import unittest

from .. import recordset


class RecordsetCursor(unittest.TestCase):
    def setUp(self):
        class DB:
            pass

        self.DB = DB

        class D:
            def __init__(self):
                # The idiom best representing Berkeley DB and DPT is
                # "self.d = {'file1':DB(), 'file2':DB()}".
                # The idiom implemented best represents SQLite and allows the
                # bitwise operator tests, __or__ and so forth, to test cases
                # where more than one 'D' object exists.
                db = DB()
                self.d = {"file1": db, "file2": db}

            # Planned to become 'def get_table(self, file)'.
            # See .._db .._dpt and .._sqlite modules.
            # Need to look at 'exists' too.
            def get_table_connection(self, file):
                return self.d.get(file)

            def exists(self, file, field):
                return bool(self.get_table_connection(file))

        self.D = D
        self.d = D()
        self.rs = recordset._Recordset(self.d, "file1")
        self.rsc = recordset.RecordsetCursor(self.rs)
        self.rsl = recordset.RecordsetSegmentList(
            2, "key", records=b"\x00A\x00B\x00C"
        )

        class RC(recordset.RecordsetCursor):
            # The implementations of _get_record are different for DPT, SQLite,
            # and Berkeley DB.
            def _get_record(self, record_number, use_cache=False):
                if record_number is not None:
                    return "sample key", "sample value"

        self.RC = RC

    def tearDown(self):
        self.rs = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "close\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "count_records\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.count_records,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "database_cursor_exists\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.database_cursor_exists,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsc.get_position_of_record,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_at_position\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsc.get_record_at_position,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "nearest\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.rsc.nearest,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.next,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "prev\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsc.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "setat\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.rsc.setat,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_get_record\(\) missing 1 required positional argument: ",
                    "'record_number'",
                )
            ),
            self.rsc._get_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_get_record\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.rsc._get_record,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "refresh_recordset\(\) takes from 1 to 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.rsc.refresh_recordset,
            *(None, None),
        )

    def test_close(self):
        self.assertEqual(self.rsc.close(), None)

    def test_count_records(self):
        self.assertEqual(self.rsc.count_records(), 0)

    def test_database_cursor_exists(self):
        self.assertEqual(self.rsc.database_cursor_exists(), True)

    def test_first(self):
        self.assertEqual(self.rsc.first(), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(rsc.first(), ("sample key", "sample value"))

    def test_get_position_of_record(self):
        self.assertEqual(self.rsc.get_position_of_record((65601, "")), 0)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rsc.get_position_of_record((65601, "")), 0)

    def test_get_record_at_position(self):
        self.rs[0] = recordset.RecordsetSegmentList(
            0, "key", records=b"\x00A\x00B\x00C"
        )
        self.rs[1] = recordset.RecordsetSegmentBitarray(
            1, "key", records=b"\x00\x7e\xe0" + b"\x00" * 13
        )
        self.rs[3] = recordset.RecordsetSegmentInt(3, "key", records=b"\x04")
        answer = "sample key", "sample value"
        self.assertEqual(self.rs.count_records(), 13)
        self.assertEqual(self.rs.sorted_segnums, [0, 1, 3])
        rsc = self.RC(self.rs)
        self.assertEqual(rsc.get_record_at_position(0), answer)
        self.assertEqual(rsc.get_record_at_position(1), answer)
        self.assertEqual(rsc.get_record_at_position(2), answer)
        self.assertEqual(rsc.get_record_at_position(3), answer)
        self.assertEqual(rsc.get_record_at_position(4), answer)
        self.assertEqual(rsc.get_record_at_position(5), answer)
        self.assertEqual(rsc.get_record_at_position(6), answer)
        self.assertEqual(rsc.get_record_at_position(7), answer)
        self.assertEqual(rsc.get_record_at_position(8), answer)
        self.assertEqual(rsc.get_record_at_position(9), answer)
        self.assertEqual(rsc.get_record_at_position(10), answer)
        self.assertEqual(rsc.get_record_at_position(11), answer)
        self.assertEqual(rsc.get_record_at_position(12), answer)
        self.assertEqual(rsc.get_record_at_position(13), None)
        self.assertEqual(rsc.get_record_at_position(-1), answer)
        self.assertEqual(rsc.get_record_at_position(-2), answer)
        self.assertEqual(rsc.get_record_at_position(-3), answer)
        self.assertEqual(rsc.get_record_at_position(-4), answer)
        self.assertEqual(rsc.get_record_at_position(-5), answer)
        self.assertEqual(rsc.get_record_at_position(-6), answer)
        self.assertEqual(rsc.get_record_at_position(-7), answer)
        self.assertEqual(rsc.get_record_at_position(-8), answer)
        self.assertEqual(rsc.get_record_at_position(-9), answer)
        self.assertEqual(rsc.get_record_at_position(-10), answer)
        self.assertEqual(rsc.get_record_at_position(-11), answer)
        self.assertEqual(rsc.get_record_at_position(-12), answer)
        self.assertEqual(rsc.get_record_at_position(-13), answer)
        self.assertEqual(rsc.get_record_at_position(-14), None)

    def test_last(self):
        self.assertEqual(self.rsc.last(), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(rsc.last(), ("sample key", "sample value"))

    def test_nearest(self):
        self.assertEqual(self.rsc.nearest(65602), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(rsc.nearest(65602), ("sample key", "sample value"))

    def test_next(self):
        self.assertEqual(self.rsc.next(), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(rsc.next(), ("sample key", "sample value"))
        self.assertEqual(rsc.next(), ("sample key", "sample value"))
        self.assertEqual(rsc.next(), ("sample key", "sample value"))
        self.assertEqual(rsc.next(), None)

    def test_prev(self):
        self.assertEqual(self.rsc.prev(), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(rsc.prev(), ("sample key", "sample value"))
        self.assertEqual(rsc.prev(), ("sample key", "sample value"))
        self.assertEqual(rsc.prev(), ("sample key", "sample value"))
        self.assertEqual(rsc.prev(), None)

    def test_setat(self):
        self.assertEqual(self.rsc.setat((65602, "")), None)
        rsc = self.RC(self.rs)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(
            rsc.setat((65602, "")), ("sample key", "sample value")
        )
        self.assertEqual(rsc.setat((65600, "")), None)

    def test__get_record(self):
        try:
            self.rsc._get_record(1)
        except recordset.RecordsetError as exc:
            self.assertEqual(
                str(exc), "_get_record must be implemented in a subclass"
            )

    def test_refresh_recordset(self):
        # K and I provide enough of ..record.Record class to allow test of
        # refresh_recordset method.
        class K:
            def __init__(self, k):
                self.recno = k

        class I:
            def __init__(self, k, nr):
                self.key = K(k)
                self.newrecord = nr

        self.assertEqual(self.rsc.refresh_recordset(), None)
        self.rs[self.rsl.segment_number] = self.rsl
        self.assertEqual(self.rs.count_records(), 3)
        self.assertEqual(self.rsc.refresh_recordset(I(65602, None)), None)
        self.assertEqual(self.rs.count_records(), 2)
        try:
            self.rsc.refresh_recordset(I(65603, True))
        except recordset.RecordsetError as exc:
            self.assertEqual(str(exc), "refresh_recordset not implemented")


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(RecordsetCursor))
