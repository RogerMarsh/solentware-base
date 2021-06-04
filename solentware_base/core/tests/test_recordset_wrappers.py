# test_recordset_wrappers.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""recordset tests for _RecordSetBase, RecordList, and FoundSet classes."""

import unittest
from collections import deque
import sys

from .. import recordset
from ..segmentsize import SegmentSize


class _RecordSetBase(unittest.TestCase):
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
        self.rsb1 = recordset._RecordSetBase(self.d, "file1")

    def tearDown(self):
        self.rsb1 = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'dbhome' and 'dbset'",
                )
            ),
            recordset._RecordSetBase,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("__init__\(\) got an unexpected keyword argument 'xxxxx'",)
            ),
            recordset._RecordSetBase,
            *(None, None),
            **dict(xxxxx=None),
        )
        if sys.version_info[:2] < (3, 6):
            excmsg = "(unorderable types: str\(\) [<>] int\(\))"
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
        self.assertEqual(sorted(self.rsb1.__dict__.keys()), ["recordset"])
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__setitem__\(\) missing 2 required ",
                    "positional arguments: ",
                    "'key' and 'value'",
                )
            ),
            self.rsb1.__setitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__getitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'key'",
                )
            ),
            self.rsb1.__getitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__delitem__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'",
                )
            ),
            self.rsb1.__delitem__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__contains__\(\) missing 1 required ",
                    "positional argument: ",
                    "'segment'",
                )
            ),
            self.rsb1.__contains__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__len__\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.__len__,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "close\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.close,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_record_number\(\) missing 1 required ",
                    "positional argument: 'recnum'",
                )
            ),
            self.rsb1.get_position_of_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_record_number_at_position\(\) missing 1 required ",
                    "positional argument: 'position'",
                )
            ),
            self.rsb1.get_record_number_at_position,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "insort_left_nodup\(\) missing 1 required positional argument: ",
                    "'segment'",
                )
            ),
            self.rsb1.insort_left_nodup,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.first,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.last,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.next,
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
            self.rsb1.prev,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "current\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.current,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                ("setat\(\) missing 1 required positional argument: 'record'",)
            ),
            self.rsb1.setat,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__or__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsb1.__or__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__and__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsb1.__and__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__xor__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.rsb1.__xor__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "normalize\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given",
                )
            ),
            self.rsb1.normalize,
            *(None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "is_record_number_in_record_set\(\) missing 1 required ",
                    "positional argument: 'record_number'",
                )
            ),
            self.rsb1.is_record_number_in_record_set,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "create_recordset_cursor\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.rsb1.create_recordset_cursor,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(self.rsb1, recordset._RecordSetBase)
        self.assertIsInstance(self.rsb1.recordset, recordset._Recordset)

    def test___setitem__(self):
        self.assertEqual(self.rsb1.__setitem__(0, True), None)

    def test___getitem__(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0",)),
            self.rsb1.__getitem__,
            *(0,),
        )

    def test___delitem__(self):
        self.assertRaisesRegex(
            KeyError,
            "".join(("0",)),
            self.rsb1.__delitem__,
            *(0,),
        )

    def test___contains__(self):
        self.assertEqual(0 in self.rsb1, False)

    def test___len__(self):
        self.assertEqual(self.rsb1.__len__(), 0)

    def test_count_records(self):
        self.assertEqual(self.rsb1.count_records(), 0)

    def test_close(self):
        self.assertEqual(self.rsb1.close(), None)

    def test_get_position_of_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "unsupported operand type\(s\) for divmod\(\): 'str' and 'int'",
                )
            ),
            self.rsb1.get_position_of_record_number,
            *("a",),
        )
        self.assertEqual(self.rsb1.get_position_of_record_number(5), 0)

    def test_get_record_number_at_position(self):
        if sys.version_info[:2] < (3, 6):
            excmsg = "(unorderable types: str\(\) [<>] int\(\))"
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
            self.rsb1.get_record_number_at_position,
            *("a",),
        )
        self.assertEqual(self.rsb1.get_record_number_at_position(2), None)

    def test_insort_left_nodup(self):
        self.assertEqual(self.rsb1.insort_left_nodup(2), None)

    def test_first(self):
        self.assertEqual(self.rsb1.first(), None)

    def test_last(self):
        self.assertEqual(self.rsb1.last(), None)

    def test_next(self):
        self.assertEqual(self.rsb1.next(), None)

    def test_prev(self):
        self.assertEqual(self.rsb1.prev(), None)

    def test_current(self):
        self.assertEqual(self.rsb1.current(), None)

    def test_setat(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "unsupported operand type\(s\) for divmod\(\): 'str' ",
                    "and 'int'",
                )
            ),
            self.rsb1.setat,
            *("a",),
        )
        self.assertEqual(self.rsb1.setat(2), None)

    def test___or__(self):
        rsb2 = recordset._RecordSetBase(self.d, "file1")
        rsb = self.rsb1 | rsb2
        self.assertIsInstance(rsb, recordset._RecordSetBase)
        self.assertIsNot(rsb, self.rsb1)
        self.assertIsNot(rsb, rsb2)

    def test___and__(self):
        rsb2 = recordset._RecordSetBase(self.d, "file1")
        rsb = self.rsb1 & rsb2
        self.assertIsInstance(rsb, recordset._RecordSetBase)
        self.assertIsNot(rsb, self.rsb1)
        self.assertIsNot(rsb, rsb2)

    def test___xor__(self):
        rsb2 = recordset._RecordSetBase(self.d, "file1")
        rsb = self.rsb1 ^ rsb2
        self.assertIsInstance(rsb, recordset._RecordSetBase)
        self.assertIsNot(rsb, self.rsb1)
        self.assertIsNot(rsb, rsb2)

    def test_normalize(self):
        self.assertEqual(self.rsb1.normalize(), None)

    def test_is_record_number_in_record_set(self):
        self.assertEqual(self.rsb1.is_record_number_in_record_set(1), False)

    def test_create_recordset_cursor(self):
        self.assertIsInstance(self.rsb1.create_recordset_cursor(), self.RC)


class RecordList(unittest.TestCase):
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
        self.rsb1 = recordset.RecordList(self.d, "file1")

    def tearDown(self):
        self.rsb1 = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'dbhome' and 'dbset'",
                )
            ),
            recordset.RecordList,
        )

    def test___ior__(self):
        rsb2 = recordset.RecordList(self.d, "file1")
        x = rsb2
        rsb2 |= self.rsb1
        self.assertIsInstance(rsb2, recordset.RecordList)
        self.assertIsNot(rsb2, self.rsb1)
        self.assertIs(rsb2, x)

    def test___iand__(self):
        rsb2 = recordset.RecordList(self.d, "file1")
        x = rsb2
        rsb2 &= self.rsb1
        self.assertIsInstance(rsb2, recordset.RecordList)
        self.assertIsNot(rsb2, self.rsb1)
        self.assertIs(rsb2, x)

    def test___ixor__(self):
        rsb2 = recordset.RecordList(self.d, "file1")
        x = rsb2
        rsb2 ^= self.rsb1
        self.assertIsInstance(rsb2, recordset.RecordList)
        self.assertIsNot(rsb2, self.rsb1)
        self.assertIs(rsb2, x)

    def test_clear_recordset(self):
        self.assertEqual(self.rsb1.clear_recordset(), None)

    def test_place_record_number(self):
        self.assertEqual(self.rsb1.place_record_number(10), None)

    def test_remove_record_number(self):
        self.assertEqual(self.rsb1.remove_record_number(20), None)

    def test_remove_recordset(self):
        rsb2 = recordset.RecordList(self.d, "file1")
        self.assertEqual(self.rsb1.remove_recordset(rsb2), None)

    def test_replace_records(self):
        rsb2 = recordset.RecordList(self.d, "file1")
        self.assertEqual(self.rsb1.replace_records(rsb2), None)


class FoundSet(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'dbhome' and 'dbset'",
                )
            ),
            recordset.FoundSet,
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(_RecordSetBase))
    runner().run(loader(RecordList))
    runner().run(loader(FoundSet))
