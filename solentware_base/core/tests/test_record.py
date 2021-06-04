# test_record.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""record tests"""

import unittest

from .. import record


class _Comparison(unittest.TestCase):
    def setUp(self):
        self.comparison = record._Comparison()
        self.ocomparison = record._Comparison()

    def tearDown(self):
        self.comparison = None
        self.ocomparison = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__eq__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__eq__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ge__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__ge__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__gt__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__gt__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__le__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__le__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__lt__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__lt__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ne__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.comparison.__ne__,
        )

    def test___eq___01(self):
        self.assertEqual(self.comparison == self.ocomparison, True)

    def test___eq___02(self):
        self.comparison.extra = None
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___eq___03(self):
        self.comparison.extra = None
        self.ocomparison.oextra = None
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___eq___04(self):
        self.comparison.extra = None
        self.ocomparison.extra = False
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___ge___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison >= self.ocomparison, False)

    def test___ge___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison >= self.ocomparison, False)

    def test___ge___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___gt___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison > self.ocomparison, False)

    def test___gt___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison > self.ocomparison, False)

    def test___gt___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___le___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison <= self.ocomparison, False)

    def test___le___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison <= self.ocomparison, False)

    def test___le___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___lt___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison < self.ocomparison, False)

    def test___lt___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison < self.ocomparison, False)

    def test___lt___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___ne___01(self):
        self.assertEqual(self.comparison != self.ocomparison, False)

    def test___ne___02(self):
        self.ocomparison.oextra = None
        self.assertEqual(self.comparison != self.ocomparison, True)

    def test___ne___03(self):
        self.ocomparison.oextra = None
        self.comparison.extra = None
        self.assertEqual(self.comparison != self.ocomparison, True)

    def test___ne___04(self):
        self.ocomparison.extra = False
        self.comparison.extra = None
        self.assertEqual(self.comparison != self.ocomparison, True)


class _Comparison_attributes(unittest.TestCase):
    def setUp(self):
        class _ComparisonAttributes(record._Comparison):
            _attributes = ("other",)

        self.comparison = record._Comparison()
        self.ocomparison = _ComparisonAttributes()

    def tearDown(self):
        self.comparison = None
        self.ocomparison = None

    def test___eq___01(self):
        self.assertEqual(self.comparison == self.ocomparison, True)

    def test___eq___02(self):
        self.comparison.extra = None
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___eq___03(self):
        self.comparison.extra = None
        self.ocomparison.oextra = None
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___eq___04(self):
        self.comparison.extra = None
        self.ocomparison.extra = False
        self.assertEqual(self.comparison == self.ocomparison, False)

    def test___ge___01(self):
        self.comparison.extra = "b"
        self.ocomparison.extra = "a"
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison >= self.ocomparison, False)

    def test___ge___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___ge___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison >= self.ocomparison, True)

    def test___gt___01(self):
        self.comparison.extra = "b"
        self.ocomparison.extra = "a"
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison > self.ocomparison, False)

    def test___gt___03(self):
        self.comparison.extra = 2
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___gt___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison > self.ocomparison, True)

    def test___le___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison <= self.ocomparison, False)

    def test___le___03(self):
        self.comparison.extra = 1
        self.ocomparison.extra = 2
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___le___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison <= self.ocomparison, True)

    def test___lt___01(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___02(self):
        self.comparison.extra = "a"
        self.ocomparison.extra = None
        self.assertEqual(self.comparison < self.ocomparison, False)

    def test___lt___03(self):
        self.comparison.extra = 1
        self.ocomparison.extra = 2
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___04(self):
        self.ocomparison.extra = "b"
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___05(self):
        self.ocomparison.extra = None
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___lt___06(self):
        self.ocomparison.extra = 1
        self.assertEqual(self.comparison < self.ocomparison, True)

    def test___ne___01(self):
        self.assertEqual(self.comparison != self.ocomparison, False)

    def test___ne___02(self):
        self.ocomparison.oextra = None
        self.assertEqual(self.comparison != self.ocomparison, True)

    def test___ne___03(self):
        self.ocomparison.oextra = None
        self.comparison.extra = None
        self.assertEqual(self.comparison != self.ocomparison, True)

    def test___ne___04(self):
        self.ocomparison.extra = False
        self.comparison.extra = None
        self.assertEqual(self.comparison != self.ocomparison, True)


class Key(unittest.TestCase):
    def setUp(self):
        self.key = record.Key()
        self.okey = record.Key()

    def tearDown(self):
        self.key = None
        self.okey = None

    def test__assumptions(self):
        # Matches "__init__ ..." before Python 3.8 but "Value() ..." otherwise.
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 1 positional argument ",
                    "but 2 were given",
                    "|",
                    "Key\(\) takes no arguments",
                )
            ),
            record.Key,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.key.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.key.pack,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(record.Key(), record.Key)

    def test_load(self):
        self.assertEqual(self.key.__dict__, {})
        self.assertEqual(self.key.load("{'b': 1}"), None)
        self.assertEqual(self.key.__dict__, {"b": 1})

    def test_pack(self):
        self.okey.__dict__["a"] = None
        self.assertEqual(self.key.pack(), "{}")
        self.assertEqual(self.okey.pack(), "{'a': None}")


class KeyData(unittest.TestCase):
    def setUp(self):
        self.key = record.KeyData()

    def tearDown(self):
        self.key = None

    def test__assumptions(self):
        # Matches "__init__ ..." before Python 3.8 but "Key() ..." otherwise.
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 1 positional argument ",
                    "but 2 were given",
                    "|",
                    "Key\(\) takes no arguments",
                )
            ),
            record.Key,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.key.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.key.pack,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(record.KeyData(), record.KeyData)

    def test_load(self):
        self.assertEqual(self.key.recno, None)
        self.assertEqual(self.key.load(2), None)
        self.assertEqual(self.key.recno, 2)

    def test_pack(self):
        self.assertEqual(self.key.pack(), None)


class KeydBaseIII(unittest.TestCase):
    def setUp(self):
        self.key = record.KeydBaseIII()

    def tearDown(self):
        self.key = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertIsInstance(self.key, record.KeydBaseIII)


class KeyText(unittest.TestCase):
    def setUp(self):
        self.key = record.KeyText()

    def tearDown(self):
        self.key = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertIsInstance(self.key, record.KeyText)


class Value(unittest.TestCase):
    def setUp(self):
        self.value = record.Value()
        self.ovalue = record.Value()

    def tearDown(self):
        self.value = None
        self.ovalue = None

    def test__assumptions(self):
        # Matches "__init__ ..." before Python 3.8 but "Value() ..." otherwise.
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 1 positional argument ",
                    "but 2 were given",
                    "|",
                    "Value\(\) takes no arguments",
                )
            ),
            record.Value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "empty\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.empty,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'value'",
                )
            ),
            self.value.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.pack,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack_value\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.pack_value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_value\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.value.get_field_value,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_values\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.value.get_field_values,
            *(None, None),
        )

    def test___init__(self):
        self.assertIsInstance(record.Value(), record.Value)

    def test_empty(self):
        self.assertEqual(self.value.empty(), None)
        self.assertEqual(self.value.__dict__, {})

    def test_load(self):
        self.assertEqual(self.value.__dict__, {})
        self.assertEqual(self.value.load("{'b': 1}"), None)
        self.assertEqual(self.value.__dict__, {"b": 1})

    def test_pack(self):
        self.ovalue.__dict__["a"] = None
        self.assertEqual(self.value.pack(), ("{}", {}))
        self.assertEqual(self.ovalue.pack(), ("{'a': None}", {}))

    def test_pack_value(self):
        self.ovalue.__dict__["a"] = None
        self.assertEqual(self.value.pack_value(), "{}")
        self.assertEqual(self.ovalue.pack_value(), "{'a': None}")

    def test_pack_get_field_value(self):
        self.assertEqual(self.value.get_field_value("a"), None)

    def test_pack_get_field_values(self):
        value1 = record.Value()
        value1.__dict__["a"] = None
        self.assertEqual(self.value.get_field_values("a"), None)
        self.assertEqual(value1.get_field_values("a"), None)
        value = record.Value()
        value.__dict__["a"] = "value"
        self.assertEqual(value.get_field_values("a"), ("value",))


class ValueData(unittest.TestCase):
    def setUp(self):
        self.value = record.ValueData()

    def tearDown(self):
        self.value = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            record.ValueData,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "empty\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.empty,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'value'",
                )
            ),
            self.value.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack_value\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.pack_value,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(record.ValueData(), record.ValueData)

    def test_empty(self):
        self.assertEqual(self.value.empty(), None)
        self.assertEqual(self.value.data, None)

    def test_load(self):
        self.assertEqual(self.value.data, None)
        self.assertEqual(self.value.load("{'b': 1}"), None)
        self.assertEqual(self.value.data, {"b": 1})

    def test_pack_value(self):
        self.assertEqual(self.value.pack_value(), "None")
        self.value.data = {"a": None}
        self.assertEqual(self.value.pack_value(), "{'a': None}")


class ValueDict(unittest.TestCase):
    def setUp(self):
        self.value = record.ValueDict()

    def tearDown(self):
        self.value = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertIsInstance(self.value, record.ValueDict)


class ValueList(unittest.TestCase):
    def setUp(self):
        self.value = record.ValueList()

    def tearDown(self):
        self.value = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            record.ValueList,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'value'",
                )
            ),
            self.value.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack_value\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.pack_value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_empty\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value._empty,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_value\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.value.get_field_value,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_values\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.value.get_field_values,
            *(None, None),
        )

    def test___init__(self):
        self.assertIsInstance(record.ValueList(), record.ValueList)

    def test_empty(self):
        self.assertEqual(self.value.empty(), None)
        self.assertEqual(self.value.__dict__, {})

    def test_load(self):
        self.assertEqual(self.value.__dict__, {})
        self.assertEqual(self.value.load("{'b': 1}"), None)
        self.assertEqual(self.value.__dict__, {})

    def test_pack_value(self):
        self.assertEqual(self.value.pack_value(), "[]")

    def test__empty(self):
        class VL(record.ValueList):
            attributes = dict(a=set, b="b")

        vl = VL()
        vl.a.add(1)
        vl.b = ""
        self.assertEqual(vl.__dict__, {"a": {1}, "b": ""})
        self.assertEqual(vl._empty(), None)
        self.assertEqual(vl.__dict__, {"a": set(), "b": "b"})
        self.value.c = 1
        self.assertEqual(self.value.__dict__, {"c": 1})
        self.assertEqual(self.value._empty(), None)
        self.assertEqual(self.value.__dict__, {"c": 1})

    def test_pack_get_field_value(self):
        self.assertEqual(self.value.get_field_value("a"), None)

    def test_pack_get_field_values(self):
        self.assertEqual(self.value.get_field_values("a"), ())


class ValueText(unittest.TestCase):
    def setUp(self):
        self.value = record.ValueText()

    def tearDown(self):
        self.value = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertIsInstance(self.value, record.ValueText)
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load\(\) missing 1 required positional argument: ",
                    "'value'",
                )
            ),
            self.value.load,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "pack_value\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.value.pack_value,
            *(None,),
        )

    def test_load(self):
        self.assertEqual(hasattr(self.value, "text"), False)
        self.assertEqual(self.value.load("{'b': 1}"), None)
        self.assertEqual(self.value.text, "{'b': 1}")

    def test_pack_value(self):
        self.assertEqual(hasattr(self.value, "text"), False)
        self.value.text = "{'a': None}"
        self.assertEqual(self.value.pack_value(), "{'a': None}")


class Record(unittest.TestCase):
    def setUp(self):
        self.record = record.Record()
        self.orecord = record.Record()

    def stub_database(self):
        # Enough of database.Database class to allow .record.Record.edit_record
        # to work, but no more.
        class D:
            def delete_instance(self, dbset, instance):
                pass

            def edit_instance(self, dbset, instance):
                pass

            def put_instance(self, dbset, instance):
                pass

            def decode_record_number(self, srkey):
                return srkey

            def get_primary_record(self, dbset, record_number):
                if isinstance(record_number, str):
                    return None
                else:
                    return record_number, "{}"

            def is_primary(self, dbset, dbname):
                return True if dbset == "p" else False

        self.D = D

    def stub_datasource(self):
        # Enough of solentware_grid.dataclient.DataSource class to allow
        # .record.Record.get_keys to work, but no more.
        class DS:
            def __init__(self, primary, dbname):
                self.primary = primary
                self.dbname = dbname

        self.DS = DS

    def tearDown(self):
        self.record = None
        self.orecord = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 1 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            record.Record,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__eq__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__eq__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ge__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__ge__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__gt__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__gt__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__le__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__le__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__lt__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__lt__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__ne__\(\) missing 1 required positional argument: ",
                    "'other'",
                )
            ),
            self.record.__ne__,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "clone\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.record.clone,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete_record\(\) missing 2 required positional arguments: ",
                    "'database' and 'dbset'",
                )
            ),
            self.record.delete_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "edit_record\(\) missing 4 required positional arguments: ",
                    "'database', 'dbset', 'dbname', and 'newrecord'",
                )
            ),
            self.record.edit_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "empty\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.record.empty,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_primary_key_from_index_record\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.get_primary_key_from_index_record,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_keys\(\) takes from 1 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.record.get_keys,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load_instance\(\) missing 4 required positional arguments: ",
                    "'database', 'dbset', 'dbname', and 'record'",
                )
            ),
            self.record.load_instance,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load_key\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.record.load_key,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load_record\(\) missing 1 required positional argument: ",
                    "'record'",
                )
            ),
            self.record.load_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "load_value\(\) missing 1 required positional argument: ",
                    "'value'",
                )
            ),
            self.record.load_value,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_packed_value_and_indexes\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.set_packed_value_and_indexes,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "put_record\(\) missing 2 required positional arguments: ",
                    "'database' and 'dbset'",
                )
            ),
            self.record.put_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "set_database\(\) missing 1 required positional argument: ",
                    "'database'",
                )
            ),
            self.record.set_database,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "packed_key\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.packed_key,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "packed_value\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.packed_value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_srvalue\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.get_srvalue,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_value\(\) missing 1 required positional argument: ",
                    "'fieldname'",
                )
            ),
            self.record.get_field_value,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_value\(\) takes from 2 to 3 positional ",
                    "arguments but 4 were given",
                )
            ),
            self.record.get_field_value,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_field_values\(\) missing 1 required positional argument: ",
                    "'fieldname'",
                )
            ),
            self.record.get_field_values,
        )

    def test___init__(self):
        self.assertIsInstance(self.record, record.Record)
        self.assertIsInstance(self.record.key, record.KeyData)
        self.assertIsInstance(self.record.value, record.Value)
        r = record.Record(keyclass=record.Key, valueclass=record.ValueData)
        self.assertIsInstance(r.key, record.Key)
        self.assertIsInstance(r.value, record.ValueData)
        r = record.Record(keyclass=str, valueclass=str)
        self.assertIsInstance(r.key, record.Key)
        self.assertIsInstance(r.value, record.Value)

    def test___eq__01(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record == self.orecord, True)

    def test___eq__02(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record == self.orecord, False)

    def test___eq__03(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record == self.orecord, False)

    def test___eq__04(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record == self.orecord, False)

    def test___ge___01(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record >= self.orecord, True)

    def test___ge___02(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record >= self.orecord, True)

    def test___ge___03(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 2
        self.assertEqual(self.record >= self.orecord, False)

    def test___gt___01(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record > self.orecord, True)

    def test___gt___02(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record > self.orecord, True)

    def test___gt___03(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record > self.orecord, False)

    def test___le___01(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "b"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record <= self.orecord, True)

    def test___le___02(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record <= self.orecord, True)

    def test___le___03(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record <= self.orecord, False)

    def test___lt___01(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "b"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record < self.orecord, True)

    def test___lt___02(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 2
        self.assertEqual(self.record < self.orecord, True)

    def test___lt___03(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record < self.orecord, False)

    def test___ne___01(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record != self.orecord, False)

    def test___ne___02(self):
        self.record.value.extra = "a"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record != self.orecord, True)

    def test___ne___03(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 2
        self.orecord.key.recno = 1
        self.assertEqual(self.record != self.orecord, True)

    def test___ne___04(self):
        self.record.value.extra = "b"
        self.orecord.value.extra = "a"
        self.record.key.recno = 1
        self.orecord.key.recno = 1
        self.assertEqual(self.record != self.orecord, True)

    def test_clone(self):
        c = self.record.clone()
        self.assertEqual(c, self.record)
        self.assertIsNot(c, self.record)

    def test_delete_record(self):
        self.stub_database()
        d = self.D()
        self.assertEqual(self.record.delete_record(d, ""), None)

    def test_edit_record_01(self):
        self.stub_database()
        d = self.D()
        self.record.srkey = "1"
        self.orecord.srkey = "1"
        self.assertEqual(
            self.record.edit_record(d, "p", "", self.orecord), None
        )

    def test_edit_record_02(self):
        self.stub_database()
        d = self.D()
        self.record.srkey = "1"
        self.orecord.srkey = "2"
        self.assertEqual(
            self.record.edit_record(d, "p", "", self.orecord), None
        )

    def test_edit_record_03(self):
        self.stub_database()
        d = self.D()
        self.record.srkey = "1"
        self.orecord.srkey = 2
        self.assertEqual(
            self.record.edit_record(d, "p", "", self.orecord), None
        )

    def test_empty(self):
        self.assertEqual(self.record.empty(), None)

    def test_get_primary_key_from_index_record(self):
        self.record.record = 1, None
        self.assertEqual(self.record.get_primary_key_from_index_record(), None)

    def test_get_keys_01(self):
        self.assertEqual(self.record.get_keys(partial=True), [])

    def test_get_keys_02(self):
        self.stub_datasource()
        ds = self.DS(True, None)
        self.assertEqual(self.record.get_keys(datasource=ds), [(None, None)])

    def test_get_keys_03(self):
        self.stub_datasource()
        ds = self.DS(False, "f")
        self.record.value.__dict__["f"] = False
        self.assertEqual(self.record.get_keys(datasource=ds), [(False, None)])

    def test_get_keys_04(self):
        self.assertEqual(self.record.get_keys(), [])

    def test_load_instance_01(self):
        self.stub_database()
        d = self.D()
        self.assertEqual(
            self.record.load_instance(d, "p", "", (1, "{}")), None
        )

    def test_load_instance_02(self):
        self.stub_database()
        d = self.D()
        self.assertEqual(self.record.load_instance(d, "q", "", ("a", 1)), None)

    def test_load_key(self):
        self.assertEqual(self.record.load_key(1), None)

    def test_load_record(self):
        self.assertEqual(self.record.load_record((1, "{}")), None)

    def test_load_value(self):
        self.assertEqual(self.record.load_value("{}"), None)

    def test_set_packed_value_and_indexes(self):
        self.assertEqual(self.record.set_packed_value_and_indexes(), None)

    def test_put_record(self):
        self.stub_database()
        d = self.D()
        self.record.srkey = "1"
        self.assertEqual(self.record.put_record(d, "p"), None)

    def test_set_database(self):
        self.assertEqual(self.record.set_database("p"), None)

    def test_packed_key(self):
        self.record.key.__dict__ = {"recno": "p"}
        self.assertEqual(self.record.packed_key(), b"p")

    def test_packed_value(self):
        self.assertEqual(self.record.packed_value(), ("{}", {}))

    def test_get_srvalue(self):
        self.record.srvalue = "1"
        self.assertEqual(self.record.get_srvalue(), 1)

    def test_get_get_field_value(self):
        self.assertEqual(self.record.get_field_value("f"), None)

    def test_get_get_field_values(self):
        self.assertEqual(self.record.get_field_values("f"), None)


class RecorddBaseIII(unittest.TestCase):
    def setUp(self):
        self.record = record.RecorddBaseIII()

    def tearDown(self):
        self.record = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 1 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            record.RecorddBaseIII,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "packed_value\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.packed_value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_srvalue\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.get_srvalue,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(self.record, record.RecorddBaseIII)
        self.assertIsInstance(self.record.key, record.KeydBaseIII)
        self.assertIsInstance(self.record.value, record.Value)
        r = record.RecorddBaseIII(
            keyclass=record.KeydBaseIII, valueclass=record.ValueData
        )
        self.assertIsInstance(r.key, record.KeydBaseIII)
        self.assertIsInstance(r.value, record.ValueData)
        r = record.RecorddBaseIII(keyclass=record.Key, valueclass=str)
        self.assertIsInstance(r.key, record.KeydBaseIII)
        self.assertIsInstance(r.value, record.Value)

    def test_packed_value(self):
        self.assertEqual(self.record.packed_value(), (b"{}", {}))

    def test_get_srvalue(self):
        self.record.srvalue = "1"
        self.assertEqual(self.record.get_srvalue(), "1")


class RecordText(unittest.TestCase):
    def setUp(self):
        self.record = record.RecordText()

    def tearDown(self):
        self.record = None

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 1 to 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            record.RecordText,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "packed_value\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.packed_value,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_srvalue\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.record.get_srvalue,
            *(None,),
        )

    def test___init__(self):
        self.assertIsInstance(self.record, record.RecordText)
        self.assertIsInstance(self.record.key, record.KeyText)
        self.assertIsInstance(self.record.value, record.ValueText)
        r = record.RecordText(
            keyclass=record.KeyText, valueclass=record.ValueText
        )
        self.assertIsInstance(r.key, record.KeyText)
        self.assertIsInstance(r.value, record.ValueText)
        r = record.RecordText(keyclass=record.Key, valueclass=str)
        self.assertIsInstance(r.key, record.KeyText)
        self.assertIsInstance(r.value, record.ValueText)

    def test_packed_value(self):
        self.record.value.text = "a"
        self.assertEqual(self.record.packed_value(), ("a", {}))

    def test_get_srvalue(self):
        self.record.srvalue = "1"
        self.assertEqual(self.record.get_srvalue(), "1")


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Key))
    runner().run(loader(KeyData))
    runner().run(loader(KeydBaseIII))
    runner().run(loader(KeyText))
    runner().run(loader(Value))
    runner().run(loader(ValueData))
    runner().run(loader(ValueDict))
    runner().run(loader(ValueList))
    runner().run(loader(ValueText))
    runner().run(loader(Record))
    runner().run(loader(RecorddBaseIII))
    runner().run(loader(RecordText))
    runner().run(loader(_Comparison))
    runner().run(loader(_Comparison_attributes))
