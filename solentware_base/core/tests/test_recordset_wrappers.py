# test_recordset_wrappers.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for _RecordSetBase, RecordList, and FoundSet classes."""

import unittest
import sys

from .. import recordset
from ..segmentsize import SegmentSize


class _RecordSetBase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__assumptions(self):
        self.assertRaisesRegex(
            TypeError,
            r"_RecordSetBase\(\) takes no arguments$",
            recordset._RecordSetBase,
            *(None,),
        )


class FoundSet(unittest.TestCase):
    # Tests for methods in recordset._RecordSetBase and the __init__
    # method of FoundSet.
    # FoundSet does not add any methods: it is significant for the methods
    # added in RecordList not present in FoundSet, and the unimplemented
    # FoundSet methods which would not be present in RecordList.
    def setUp(self):
        class DB:
            pass

        self.DB = DB

        class RC:
            pass

        self.RC = RC

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

            def create_recordset_cursor(self, rs):
                return RC()

        self.D = D
        self.d = D()
        self.rs = recordset._Recordset(self.d, "file1")
        self.fs1 = recordset.FoundSet(self.rs)

    def tearDown(self):
        self.fs1 = None
        self.rs = None

    def test__assumptions(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'recordset'$",
                )
            ),
            recordset.FoundSet,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 2 positional arguments ",
                    "but 3 were given$",
                )
            ),
            recordset.FoundSet,
            *(None, None),
        )
        if sys.version_info[:2] < (3, 6):
            excmsg = r"(unorderable types: str\(\) [<>] int\(\))"
        else:
            excmsg = "".join(
                (
                    "('[<>]' not supported between instances of ",
                    "'str' and 'int')|",
                )
            )
        self.assertRaisesRegex(
            TypeError,
            excmsg,
            recordset._Recordset,
            *(self.d, "file1"),
            **dict(cache_size="a"),
        )
        self.assertEqual(sorted(self.fs1.__dict__.keys()), ["recordset"])
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__setitem__\(\) missing 2 required ",
                    "positional arguments: ",
                    "'key' and 'value'$",
                )
            ),
            self.fs1.__setitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__getitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'key'$",
                )
            ),
            self.fs1.__getitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__delitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'$",
                )
            ),
            self.fs1.__delitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__contains__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'$",
                )
            ),
            self.fs1.__contains__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__len__\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.fs1.__len__,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.fs1.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"insort_left_nodup\(\) missing 1 required ",
                    "positional argument: 'segment'$",
                )
            ),
            self.fs1.insort_left_nodup,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__or__\(\) missing 1 required positional argument: ",
                    "'other'$",
                )
            ),
            self.fs1.__or__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__and__\(\) missing 1 required positional argument: ",
                    "'other'$",
                )
            ),
            self.fs1.__and__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__xor__\(\) missing 1 required positional argument: ",
                    "'other'$",
                )
            ),
            self.fs1.__xor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"normalize\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.fs1.normalize,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"is_record_number_in_record_set\(\) missing 1 required ",
                    "positional argument: 'record_number'$",
                )
            ),
            self.fs1.is_record_number_in_record_set,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"create_recordsetbase_cursor\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.fs1.create_recordsetbase_cursor,
            *(None, None),
        )

    def test___init__(self):
        self.assertIsInstance(self.fs1, recordset.FoundSet)
        self.assertIsInstance(self.fs1.recordset, recordset._Recordset)

    def test___setitem__(self):
        self.assertEqual(self.fs1.__setitem__(0, True), None)

    def test___getitem__(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0$",)),
            self.fs1.__getitem__,
            *(0,),
        )

    def test___delitem__(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0$",)),
            self.fs1.__delitem__,
            *(0,),
        )

    def test___contains__(self):
        self.assertEqual(0 in self.fs1, False)

    def test___len__(self):
        self.assertEqual(self.fs1.__len__(), 0)

    def test_count_records(self):
        self.assertEqual(self.fs1.count_records(), 0)

    def test_close(self):
        self.assertEqual(self.fs1.close(), None)

    def test_insort_left_nodup(self):
        self.assertEqual(self.fs1.insort_left_nodup(2), None)

    def test___or__(self):
        fs2 = recordset.FoundSet(self.rs)
        fs = self.fs1 | fs2
        self.assertIsInstance(fs, recordset.RecordList)
        self.assertIsNot(fs, self.fs1)
        self.assertIsNot(fs, fs2)

    def test___and__(self):
        fs2 = recordset.FoundSet(self.rs)
        fs = self.fs1 & fs2
        self.assertIsInstance(fs, recordset.RecordList)
        self.assertIsNot(fs, self.fs1)
        self.assertIsNot(fs, fs2)

    def test___xor__(self):
        fs2 = recordset.FoundSet(self.rs)
        fs = self.fs1 ^ fs2
        self.assertIsInstance(fs, recordset.RecordList)
        self.assertIsNot(fs, self.fs1)
        self.assertIsNot(fs, fs2)

    def test_normalize(self):
        self.assertEqual(self.fs1.normalize(), None)

    def test_is_record_number_in_record_set(self):
        self.assertEqual(self.fs1.is_record_number_in_record_set(1), False)


class RecordList(unittest.TestCase):
    # Tests for RecordList methods not in recordset._RecordSetBase.
    def setUp(self):
        class DB:
            pass

        self.DB = DB

        class EBM:
            def __init__(self):
                self.record_number_in_ebm = True

            def is_record_number_in_record_set(self, *a):
                return self.record_number_in_ebm

        class RC:
            pass

        self.RC = RC

        class D:
            def __init__(self):
                # The idiom best representing Berkeley DB and DPT is
                # "self.d = {'file1':DB(), 'file2':DB()}".
                # The idiom implemented best represents SQLite and allows the
                # bitwise operator tests, __or__ and so forth, to test cases
                # where more than one 'D' object exists.
                db = DB()
                self.ebm = EBM()
                self.d = {"file1": db, "file2": db}

            # Planned to become 'def get_table(self, file)'.
            # See .._db .._dpt and .._sqlite modules.
            # Need to look at 'exists' too.
            def get_table_connection(self, file):
                return self.d.get(file)

            def exists(self, file, field):
                return bool(self.get_table_connection(file))

            def create_recordset_cursor(self, rs):
                return RC()

            def recordlist_ebm(self, *a):
                return self.ebm

        self.D = D
        self.d = D()
        self.rsl1 = recordset.RecordList(self.d, "file1")

    def tearDown(self):
        self.rsl1 = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 2 required positional arguments: ",
                    "'dbhome' and 'dbset'$",
                )
            ),
            recordset.RecordList,
        )

    def test___ior__(self):
        rsl2 = recordset.RecordList(self.d, "file1")
        rsl = rsl2
        rsl2 |= self.rsl1
        self.assertIsInstance(rsl2, recordset.RecordList)
        self.assertIsNot(rsl2, self.rsl1)
        self.assertIs(rsl2, rsl)

    def test___iand__(self):
        rsl2 = recordset.RecordList(self.d, "file1")
        rsl = rsl2
        rsl2 &= self.rsl1
        self.assertIsInstance(rsl2, recordset.RecordList)
        self.assertIsNot(rsl2, self.rsl1)
        self.assertIs(rsl2, rsl)

    def test___ixor__(self):
        rsl2 = recordset.RecordList(self.d, "file1")
        rsl = rsl2
        rsl2 ^= self.rsl1
        self.assertIsInstance(rsl2, recordset.RecordList)
        self.assertIsNot(rsl2, self.rsl1)
        self.assertIs(rsl2, rsl)

    def test_clear_recordset(self):
        self.assertEqual(self.rsl1.clear_recordset(), None)

    def test_place_record_number(self):
        self.assertEqual(self.d.ebm.is_record_number_in_record_set(), True)
        self.assertEqual(self.rsl1.place_record_number(10), None)
        self.d.ebm.record_number_in_ebm = False
        self.assertEqual(self.d.ebm.is_record_number_in_record_set(), False)
        self.assertEqual(self.rsl1.place_record_number(10), None)

    def test_remove_record_number(self):
        self.assertEqual(self.d.ebm.is_record_number_in_record_set(), True)
        self.assertEqual(self.rsl1.remove_record_number(20), None)
        self.d.ebm.record_number_in_ebm = False
        self.assertEqual(self.d.ebm.is_record_number_in_record_set(), False)
        self.assertEqual(self.rsl1.remove_record_number(20), None)

    def test_remove_recordset(self):
        rsl2 = recordset.RecordList(self.d, "file1")
        self.assertEqual(self.rsl1.remove_recordset(rsl2), None)

    def test_replace_records(self):
        rsl2 = recordset.RecordList(self.d, "file1")
        self.assertEqual(self.rsl1.replace_records(rsl2), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(_RecordSetBase))
    runner().run(loader(FoundSet))
    runner().run(loader(RecordList))
