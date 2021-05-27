# definition_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""definition tests"""

import unittest

from .. import definition


class Definition_00(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__01(self):
        self.assertRaisesRegex(
            definition.DefinitionError,
            ' '.join(['Cannot construct definition from',
                      'specification',
                      'because a mandatory argument is missing']),
            definition.Definition)

    def test___init__02(self):
        self.assertRaisesRegex(
            definition.DefinitionError,
            ' '.join(['Cannot construct definition from',
                      'specification',
                      'because a mandatory argument is missing']),
            definition.Definition,
            **dict(dbset=None,
                   specification=None,
                   primary_class=None,
                   secondary_class=None,
                   field_name_converter=None))

    def test___init__03(self):
        self.assertRaisesRegex(
            Exception,
            "'bool' object is not subscriptable",
            definition.Definition,
            **dict(dbset=True,
                   specification=True,
                   primary_class=True,
                   secondary_class=True,
                   field_name_converter=True))

    def test___init__04(self):
        self.assertRaisesRegex(
            Exception,
            "'primary'",
            definition.Definition,
            **dict(dbset=True,
                   specification={},
                   primary_class=True,
                   secondary_class=True,
                   field_name_converter=True))

    def test___init__05(self):
        self.assertRaisesRegex(
            TypeError,
            "'bool' object is not callable",
            definition.Definition,
            **dict(dbset=True,
                   specification={'primary': None},
                   primary_class=True,
                   secondary_class=True,
                   field_name_converter=True))


class Definition_01(unittest.TestCase):

    def setUp(self):
        class PC:
            def __init__(self, *a, **k):
                pass
        self.pc = PC

    def tearDown(self):
        self.pc = None

    def test___init__01(self):
        self.assertRaisesRegex(
            Exception,
            "'NoneType' object has no attribute 'items'",
            definition.Definition,
            **dict(dbset=True,
                   specification={'primary': None, 'secondary': None},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=True))

    def test___init__02(self):
        self.assertRaisesRegex(
            definition.DefinitionError,
            "Secondary name for p must be a string",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': None, 'secondary': {1: None}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=True))

    def test___init__03(self):
        self.assertRaisesRegex(
            TypeError,
            "'bool' object is not callable",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': None, 'secondary': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=True))

    def test___init__04(self):
        self.assertRaisesRegex(
            definition.DefinitionError,
            "Secondary name t for p cannot be same as primary",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': 't', 'secondary': {'s': 't'}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=True))


class Definition_02(unittest.TestCase):

    def setUp(self):
        class PC:
            def __init__(self, *a, **k):
                pass
        def fnc(a):
            return a
        self.pc = PC
        self.fnc = fnc

    def tearDown(self):
        self.pc = None
        self.fnc = None

    def test___init__01(self):
        self.assertRaisesRegex(
            KeyError,
            "fields",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': 't', 'secondary': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=self.fnc))

    def test___init__02(self):
        self.assertRaisesRegex(
            definition.DefinitionError,
            "Secondary name s for p does not have a description",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None},
                                  'fields': {}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=self.fnc))

    def test___init__03(self):
        self.assertRaisesRegex(
            AttributeError,
            "'Definition' object has no attribute 'make_secondary_class'",
            definition.Definition,
            **dict(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None},
                                  'fields': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=self.fnc))


class Definition_03(unittest.TestCase):

    def setUp(self):
        class PC:
            def __init__(self, *a, **k):
                pass
        def fnc(a):
            return a
        class D(definition.Definition):
            def make_secondary_class(self, sc, *a, **k):
                return sc(*a, **k)
        class SC:
            def __init__(self, *a, **k):
                pass
        self.pc = PC
        self.fnc = fnc
        self.d = D
        self.sc = SC

    def tearDown(self):
        self.pc = None
        self.fnc = None
        self.d = None
        self.sc = None

    def test___init__00(self):
        self.assertRaisesRegex(
            TypeError,
            "'bool' object is not callable",
            self.d,
            **dict(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None},
                                  'fields': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=True,
                   field_name_converter=self.fnc))

    def test___init__01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'SC' object has no attribute 'set_primary_database'",
            self.d,
            **dict(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None},
                                  'fields': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=self.sc,
                   field_name_converter=self.fnc))


class Definition_04(unittest.TestCase):

    def setUp(self):
        class PC:
            def __init__(self, *a, **k):
                pass
        def fnc(a):
            return a
        class D(definition.Definition):
            def make_secondary_class(self, sc, *a, **k):
                return sc(*a, **k)
        class SC:
            def __init__(self, *a, **k):
                pass
            def set_primary_database(self, p):
                self.p = p
        self.pc = PC
        self.fnc = fnc
        self.d = D
        self.sc = SC

    def tearDown(self):
        self.pc = None
        self.fnc = None
        self.d = None
        self.sc = None

    def test___init__00(self):
        d = self.d(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None},
                                  'fields': {'s': None}},
                   primary_class=self.pc,
                   secondary_class=self.sc,
                   field_name_converter=self.fnc)
        self.assertEqual(d._dbset, 'p')
        self.assertIsInstance(d.primary, self.pc)
        self.assertEqual(d.dbname_to_secondary_key, {'s': 's'})
        self.assertEqual(list(d.secondary.keys()), ['s'])
        self.assertEqual(d.associate('p'), d.primary)
        self.assertEqual(d.associate('s'),
                         d.secondary[d.dbname_to_secondary_key['s']])
        self.assertIsInstance(d.associate('s'), self.sc)
        self.assertIs(d.associate('s').p, d.primary)

    def test___init__01(self):
        d = self.d(dbset='p',
                   specification={'primary': 't',
                                  'secondary': {'s': None, 'u': None},
                                  'fields': {'s': None, 'u': None}},
                   primary_class=self.pc,
                   secondary_class=self.sc,
                   field_name_converter=self.fnc)
        self.assertEqual(
            len(d.secondary),
            len(set(v for v in d.secondary.values())))


if __name__ == '__main__':
    unittest.main()
