# test_recordset_basecursor.py
# Copyright 2024 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Tests for Location and RecordSetBaseCursor classes."""

import unittest

from .. import recordset
from .. import recordsetbasecursor


class Location(unittest.TestCase):
    def test_01___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            recordset.Location,
            *(None,),
        )

    def test_01___init___02(self):
        location = recordset.Location()
        self.assertEqual(
            sorted(location.__dict__.keys()),
            [
                "current_position_in_segment",
                "current_segment",
            ],
        )
        self.assertEqual(location.current_segment, None)
        self.assertEqual(location.current_position_in_segment, None)

    def test_02_clear_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            recordset.Location,
            *(None,),
        )

    def test_02_clear_02(self):
        location = recordset.Location()
        self.assertEqual(location.clear(), None)
        self.assertEqual(
            sorted(location.__dict__.keys()),
            [
                "current_position_in_segment",
                "current_segment",
            ],
        )
        self.assertEqual(location.current_segment, None)
        self.assertEqual(location.current_position_in_segment, None)


class RecordSetBaseCursor___init___fail(unittest.TestCase):
    def test_01___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'recordset'$",
                )
            ),
            recordsetbasecursor.RecordSetBaseCursor,
        )

    def test_01___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given$",
                )
            ),
            recordsetbasecursor.RecordSetBaseCursor,
            *(None, None, None),
        )

    def test_01___init___03(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (r"__init__\(\) got an unexpected keyword argument 'xxxxx'$",)
            ),
            recordsetbasecursor.RecordSetBaseCursor,
            *(None, None),
            **dict(xxxxx=None),
        )


class _RecordSetBaseCursor(unittest.TestCase):
    def setUp(self):
        class DB:
            pass

        class D:
            def __init__(self):
                db = DB()
                self.d = {"file1": db, "file2": db}

            def get_table_connection(self, file):
                return self.d.get(file)

            def exists(self, file, field):
                return bool(self.get_table_connection(file))

            def get_primary_record(self, file, key):
                if key is None:
                    return None
                return (key, "value")

        self.D = D


class RecordSetBaseCursor___init__(_RecordSetBaseCursor):
    def test_01___init___04(self):
        self.assertEqual(
            isinstance(
                recordsetbasecursor.RecordSetBaseCursor(
                    recordset._Recordset(self.D(), None)
                ),
                recordsetbasecursor.RecordSetBaseCursor,
            ),
            True,
        )

    def test_01___init___05(self):
        self.assertEqual(
            isinstance(
                recordsetbasecursor.RecordSetBaseCursor(
                    recordset._Recordset(self.D(), None),
                    location=recordset.Location(),
                ),
                recordsetbasecursor.RecordSetBaseCursor,
            ),
            True,
        )


class _RecordSetBaseCursorLocation(_RecordSetBaseCursor):
    def setUp(self):
        super().setUp()
        self.location = recordset.Location()
        self.rsbc = recordsetbasecursor.RecordSetBaseCursor(
            recordset._Recordset(self.D(), "file1")
        )
        self.rsbcl = recordsetbasecursor.RecordSetBaseCursor(
            recordset._Recordset(self.D(), "file2"),
            location=recordset.Location(),
        )


class RecordSetBaseCursor(_RecordSetBaseCursorLocation):
    def test_02_close_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.close,
            *(None,),
        )

    def test_02_close_02(self):
        self.assertEqual(self.rsbc.close(), None)

    def test_03_close_02(self):
        self.assertEqual(self.rsbcl.close(), None)

    def test_03_get_position_of_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_position_of_record_number\(\) missing 1 required ",
                    "positional argument: 'recnum'$",
                )
            ),
            self.rsbc.get_position_of_record_number,
        )

    def test_03_get_position_of_record_number_02(self):
        self.assertEqual(self.rsbc.get_position_of_record_number(2), 0)

    def test_03_get_position_of_record_number_03(self):
        self.assertEqual(self.rsbcl.get_position_of_record_number(2), 0)

    def test_04_get_record_number_at_position_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_record_number_at_position\(\) missing 1 required ",
                    "positional argument: 'position'$",
                )
            ),
            self.rsbc.get_record_number_at_position,
        )

    def test_04_get_record_number_at_position_02(self):
        self.assertEqual(self.rsbc.get_record_number_at_position(1), None)

    def test_04_get_record_number_at_position_03(self):
        self.assertEqual(self.rsbcl.get_record_number_at_position(1), None)

    def test_05_first_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"first\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.first,
            *(None,),
        )

    def test_05_first_02(self):
        self.assertEqual(self.rsbc.first(), None)

    def test_05_first_03(self):
        self.assertEqual(self.rsbcl.first(), None)

    def test_06_last_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"last\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.last,
            *(None,),
        )

    def test_06_last_02(self):
        self.assertEqual(self.rsbc.last(), None)

    def test_06_last_03(self):
        self.assertEqual(self.rsbcl.last(), None)

    def test_07_next_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"next\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.next,
            *(None,),
        )

    def test_07_next_02(self):
        self.assertEqual(self.rsbc.next(), None)

    def test_07_next_03(self):
        self.assertEqual(self.rsbcl.next(), None)

    def test_08_prev_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"prev\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.prev,
            *(None,),
        )

    def test_08_prev_02(self):
        self.assertEqual(self.rsbc.prev(), None)

    def test_08_prev_03(self):
        self.assertEqual(self.rsbcl.prev(), None)

    def test_09_current_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"current\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.current,
            *(None,),
        )

    def test_09_current_02(self):
        self.assertEqual(self.rsbc.current(), None)

    def test_09_current_03(self):
        self.assertEqual(self.rsbcl.current(), None)

    def test_10_setat_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"setat\(\) missing 1 required positional ",
                    "argument: 'record'$",
                )
            ),
            self.rsbc.setat,
        )

    def test_10_setat_02(self):
        self.assertEqual(self.rsbc.setat(1), None)

    def test_10_setat_03(self):
        self.assertEqual(self.rsbcl.setat(1), None)

    def test_11__get_record_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_get_record\(\) missing 1 required positional ",
                    "argument: 'reference'$",
                )
            ),
            self.rsbc._get_record,
        )

    def test_11__get_record_02(self):
        self.assertEqual(self.rsbc._get_record(None), None)

    def test_11__get_record_03(self):
        self.assertEqual(self.rsbc._get_record((None, 3)), (3, "value"))

    def test_11__get_record_04(self):
        self.assertEqual(self.rsbcl._get_record(None), None)

    def test_11__get_record_05(self):
        self.assertEqual(self.rsbcl._get_record((None, 3)), (3, "value"))

    def test_12_first_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"first_record_number\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.first_record_number,
            *(None,),
        )

    def test_12_first_record_number_02(self):
        self.assertEqual(self.rsbc.first_record_number(), None)

    def test_12_first_record_number_03(self):
        self.assertEqual(self.rsbcl.first_record_number(), None)

    def test_13_last_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"last_record_number\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.last_record_number,
            *(None,),
        )

    def test_13_last_record_number_02(self):
        self.assertEqual(self.rsbc.last_record_number(), None)

    def test_13_last_record_number_03(self):
        self.assertEqual(self.rsbcl.last_record_number(), None)

    def test_14_next_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"next_record_number\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.next_record_number,
            *(None,),
        )

    def test_14_next_record_number_02(self):
        self.assertEqual(self.rsbc.next_record_number(), None)

    def test_14_next_record_number_03(self):
        self.assertEqual(self.rsbcl.next_record_number(), None)

    def test_15_prev_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"prev_record_number\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.prev_record_number,
            *(None,),
        )

    def test_15_prev_record_number_02(self):
        self.assertEqual(self.rsbc.prev_record_number(), None)

    def test_15_prev_record_number_03(self):
        self.assertEqual(self.rsbcl.prev_record_number(), None)

    def test_16_current_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"current_record_number\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.rsbc.current_record_number,
            *(None,),
        )

    def test_16_current_record_number_02(self):
        self.assertEqual(self.rsbc.current_record_number(), None)

    def test_16_current_record_number_03(self):
        self.assertEqual(self.rsbcl.current_record_number(), None)

    def test_17_setat_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"setat_record_number\(\) missing 1 required positional ",
                    "argument: 'record'$",
                )
            ),
            self.rsbc.setat_record_number,
        )

    def test_17_setat_record_number_02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"unsupported operand type\(s\) for divmod\(\): 'str' ",
                    "and 'int'$",
                )
            ),
            self.rsbc.setat_record_number,
            *("a",),
        )

    def test_17_setat_record_number_03(self):
        self.assertEqual(self.rsbc.setat_record_number(2), None)

    def test_17_setat_record_number_04(self):
        self.assertEqual(self.rsbcl.setat_record_number(2), None)

    def test_18__get_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_get_record_number\(\) missing 1 required positional ",
                    "argument: 'reference'$",
                )
            ),
            self.rsbc._get_record_number,
        )

    def test_18__get_record_number_02(self):
        self.assertEqual(self.rsbc._get_record_number((None, 1)), 1)

    def test_18__get_record_number_03(self):
        self.assertEqual(self.rsbcl._get_record_number((None, 1)), 1)

    def test_18__get_record_number_04(self):
        self.assertEqual(self.rsbcl._get_record_number(None), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Location))
    runner().run(loader(RecordSetBaseCursor___init___fail))
    runner().run(loader(RecordSetBaseCursor___init__))
    runner().run(loader(RecordSetBaseCursor))
