# filespec_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""filespec tests"""

import unittest

from .. import filespec


class FileSpec(unittest.TestCase):

    def setUp(self):
        self.filespec = filespec.FileSpec()

    def tearDown(self):
        self.filespec = None

    def test_dpt_dsn(self):
        for n in (('AAAA', 'aaaa.dpt'),
                  ('aaaa', 'aaaa.dpt'),
                  ('aAAA', 'aaaa.dpt'),
                  ('Aaaa', 'aaaa.dpt'),
                  ('anyOldName', 'anyoldname.dpt'),
                  ):
            with self.subTest(n=n[0]):
                self.assertEqual(n[1], self.filespec.dpt_dsn(n[0]))

    def test_field_name(self):
        for n in (('AAAA', 'AAAA'),
                  ('aaaa', 'Aaaa'),
                  ('aAAA', 'AAAA'),
                  ('Aaaa', 'Aaaa'),
                  ('anyOldName', 'AnyOldName'),
                  ):
            with self.subTest(n=n[0]):
                self.assertEqual(n[1], self.filespec.field_name(n[0]))

    def test___init__(self):
        self.assertEqual(self.filespec, {})


class FileSpec_01(unittest.TestCase):

    def setUp(self):
        self.filespec = filespec.FileSpec(a=('b',))

    def tearDown(self):
        self.filespec = None

    def test___init__(self):
        self.assertEqual(
            self.filespec,
            {'a': {'primary': 'a',
                   'ddname': 'DDNAME1',
                   'file': 'a.dpt',
                   'secondary': {'b': None},
                   'fields': {'a': None, 'B': None},
                   'default_records': 200,
                   'filedesc': {'brecppg': 10,
                                'fileorg': 36,
                                'bsize': 20,
                                'dsize': 160},
                   'btod_factor': 8},
             })


class FileSpec_02(unittest.TestCase):

    def setUp(self):
        self.filespec = filespec.FileSpec(a=('b',), atwo=('btwo',))

    def tearDown(self):
        self.filespec = None

    def test___init__(self):
        self.assertEqual(
            self.filespec,
            {'a': {'primary': 'a',
                   'ddname': 'DDNAME1',
                   'file': 'a.dpt',
                   'secondary': {'b': None},
                   'fields': {'a': None, 'B': None},
                   'default_records': 200,
                   'filedesc': {'brecppg': 10,
                                'fileorg': 36,
                                'bsize': 20,
                                'dsize': 160},
                   'btod_factor': 8},
             'atwo': {'primary': 'atwo',
                      'ddname': 'DDNAME2',
                      'file': 'atwo.dpt',
                      'secondary': {'btwo': None},
                      'fields': {'atwo': None, 'Btwo': None},
                      'default_records': 200,
                      'filedesc': {'brecppg': 10,
                                   'fileorg': 36,
                                   'bsize': 20,
                                   'dsize': 160},
                      'btod_factor': 8},
             })


class FileSpec_03(unittest.TestCase):

    def setUp(self):
        self.filespec = filespec.FileSpec(
            a=('b',), atwo=('btwo',), use_specification_items= {'a'})

    def tearDown(self):
        self.filespec = None

    def test___init__(self):
        self.assertEqual(
            self.filespec,
            {'a': {'primary': 'a',
                   'ddname': 'DDNAME1',
                   'file': 'a.dpt',
                   'secondary': {'b': None},
                   'fields': {'a': None, 'B': None},
                   'default_records': 200,
                   'filedesc': {'brecppg': 10,
                                'fileorg': 36,
                                'bsize': 20,
                                'dsize': 160},
                   'btod_factor': 8},
             })


class FileSpec_04(unittest.TestCase):

    def setUp(self):
        self.filespec = filespec.FileSpec(
            a=('b',), atwo=('btwo',), dpt_records=dict(a=500))

    def tearDown(self):
        self.filespec = None

    def test___init__(self):
        self.assertEqual(
            self.filespec,
            {'a': {'primary': 'a',
                   'ddname': 'DDNAME1',
                   'file': 'a.dpt',
                   'secondary': {'b': None},
                   'fields': {'a': None, 'B': None},
                   'default_records': 500,
                   'filedesc': {'brecppg': 10,
                                'fileorg': 36,
                                'bsize': 50,
                                'dsize': 400},
                   'btod_factor': 8},
             'atwo': {'primary': 'atwo',
                      'ddname': 'DDNAME2',
                      'file': 'atwo.dpt',
                      'secondary': {'btwo': None},
                      'fields': {'atwo': None, 'Btwo': None},
                      'default_records': 200,
                      'filedesc': {'brecppg': 10,
                                   'fileorg': 36,
                                   'bsize': 20,
                                   'dsize': 160},
                      'btod_factor': 8},
             })


class FileSpec_05(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__01(self):
        self.assertRaisesRegex(
            filespec.FileSpecError,
            "dpt_default_records must be a dict",
            filespec.FileSpec,
            **dict(dpt_records=200))

    def test___init__02(self):
        self.assertRaisesRegex(
            filespec.FileSpecError,
            ''.join(
                    ('number of records must be a positive integer for item ',
                     'a',
                     ' in filespec.',
                     )),
            filespec.FileSpec,
            **dict(a={'primary': 'a',
                      'ddname': 'DDNAME1',
                      'file': 'a.dpt',
                      'secondary': {'b': None},
                      'fields': {'a': None, 'B': None},
                      'filedesc': {'brecppg': 10,
                                   'fileorg': 36,
                                   'bsize': 20,
                                   'dsize': 160},
                      'btod_factor': 8},
                   dpt_records=dict(a='1')))

    def test___init__03(self):
        self.assertRaisesRegex(
            filespec.FileSpecError,
            ''.join(('number of records must be a positive integer for item ',
                     'a',
                     ' in filespec.',
                     )),
            filespec.FileSpec,
            **dict(a={'primary': 'a',
                      'ddname': 'DDNAME1',
                      'file': 'a.dpt',
                      'secondary': {'b': None},
                      'fields': {'a': None, 'B': None},
                      'filedesc': {'brecppg': 10,
                                   'fileorg': 36,
                                   'bsize': 20,
                                   'dsize': 160},
                      'btod_factor': 8},
                   dpt_records=dict(a=-1)))

    def test___init__04(self):
        self.assertRaisesRegex(
            filespec.FileSpecError,
            ''.join(("Secondary name '",
                     'a',
                     "' cannot be same as ",
                     "primary name '",
                     'a',
                     "' in filespec.",
                     )),
            filespec.FileSpec,
            **dict(a={'a'}))

    def test___init__05(self):
        self.assertRaisesRegex(
            filespec.FileSpecError,
            ''.join(("Secondary name '",
                     'A',
                     "' cannot be same as ",
                     "primary name '",
                     'a',
                     "' in filespec.",
                     )),
            filespec.FileSpec,
            **dict(a={'b', 'A'}))


if __name__ == '__main__':
    unittest.main()
