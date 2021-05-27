# database_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""database tests"""

import unittest
import os
import sys

from .. import database
from .. import recordset
from ..find import Find
from ..where import Where
from ..findvalues import FindValues
from ..wherevalues import WhereValues


class Database_00(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__00(self):
        self.assertRaisesRegex(
            TypeError,
            ' '.join(['__init__\(\) missing 2 required positional arguments:',
                      "'database_specification' and 'databasefolder'",
                      ]),
            database.Database)

    def test___init__01(self):
        if sys.platform == 'win32':
            err = 'Database specification must be a dictionary'
        else:
            err = 'Database folder name None is not valid'
        self.assertRaisesRegex(
            database.DatabaseError,
            err,
            database.Database,
            *(None, None))

    def test___init__02(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'Database specification must be a dictionary',
            database.Database,
            *(None, 'foldername'))

    def test___init__03(self):
        d = database.Database({}, 'foldername')
        self.assertEqual(d._dbspec, {})
        self.assertEqual(d._dbservices, None)
        self.assertEqual(os.path.basename(d._home_directory), 'foldername')
        self.assertEqual(d._home, os.path.join(d._home_directory, 'foldername'))
        self.assertEqual(len(d.__dict__), 4)
        self.assertEqual(
            len([m for m in dir(d.__class__) if not m.startswith('__')]), 64)
        self.assertEqual(
            [m for m in dir(d.__class__) if not m.startswith('__')],
            ['backout',
             'close_context',
             'close_contexts',
             'close_database',
             'commit',
             'create_default_parms',
             'create_recordsetlist_cursor',
             'database_cursor',
             'database_definition',
             'database_specification',
             'db_compatibility_hack',
             'dbservices',
             'decode_as_primary_key',
             'deferred_update_housekeeping',
             'delete_instance',
             'edit_instance',
             'encode_primary_key',
             'exists',
             'file_records_under',
             'get_associated_indicies',
             'get_database',
             'get_database_filenames',
             'get_database_folder',
             'get_database_home',
             'get_database_increase',
             'get_database_instance',
             'get_database_parameters',
             'get_dptsysfolder',
             'get_first_primary_key_for_index_key',
             'get_packed_key',
             'get_parms',
             'get_primary_record',
             'get_sfserv',
             'get_table_index',
             'get_table_indicies',
             'get_transaction',
             'increase_database_size',
             'initial_database_size',
             'is_primary',
             'is_primary_recno',
             'is_recno',
             'make_connection',
             'make_recordset',
             'make_recordset_all',
             'make_recordset_key',
             'make_recordset_key_like',
             'make_recordset_key_range',
             'make_recordset_key_startswith',
             'make_root',
             'open_context',
             'open_context_allocated',
             'open_context_normal',
             'open_contexts',
             'put_instance',
             'record_finder',
             'record_selector',
             'repair_cursor',
             'set_defer_update',
             'start_transaction',
             'unfile_records_under',
             'unset_defer_update',
             'use_deferred_update_process',
             'values_finder',
             'values_selector',
             ])


class Database_01(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__00(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 'p1' must be a dictionary",
            database.Database,
            *({'p1': None}, 'foldername'))

    def test___init__01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 'p1' must contain a primary name",
            database.Database,
            *({'p1': {}}, 'foldername'))

    def test___init__02(self):
        self.assertRaisesRegex(
            TypeError,
            "argument of type 'NoneType' is not iterable",
            database.Database,
            *({'p1': {'primary': None}},
              'foldername'))

    def test___init__03(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Primary name _ contains '_', which is not allowed",
            database.Database,
            *({'p1': {'primary': '_'}},
              'foldername'))

    def test___init__04(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ' '.join(("Field definitions must be present in specification",
                      "for primary fields")),
            database.Database,
            *({'p1': {'primary': 'p1n'}},
              'foldername'))

    def test___init__05(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Primary name p1n for p1 must be in fields definition",
            database.Database,
            *({'p1': {'primary': 'p1n', 'fields': {}}},
              'foldername'))

    def test___init__06(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for p1 must have a DD name",
            database.Database,
            *({'p1': {'primary': 'p1n', 'fields': {'p1n': None}}},
              'foldername'))

    def test___init__07(self):
        self.assertRaisesRegex(
            TypeError,
            "object of type 'NoneType' has no len()",
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': None}},
              'foldername'))

    def test___init__08(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "DD name '' for p1 is zero length",
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': ''}},
              'foldername'))

    def test___init__09(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "DD name 123456789 for p1 is over 8 characters",
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': '123456789'}},
              'foldername'))

    def test___init__10(self):
        for n in ('abcd_678', 'aBCD5678', 'Abcd5678', 'a', '1'):
            with self.subTest(n=n):
                self.assertRaisesRegex(
                    database.DatabaseError,
                    ' '.join(['DD name', n,
                              'for', 'p1',
                              'must be upper case alphanum',
                              'starting with alpha']),
                    database.Database,
                    *({'p1': {'primary': 'p1n',
                              'fields': {'p1n': None},
                              'ddname': n}},
                      'foldername'))

    def test___init__11(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'Full path name of DPT file for p1 is invalid',
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1'}},
              'foldername'))

    def test___init__12(self):
        d = database.Database({'p1': {'primary': 'p1n',
                                      'fields': {'p1n': None},
                                      'ddname': 'DATASET1',
                                      'file': 'p1f.dpt'}},
                              'foldername')
        self.assertEqual(d._dbspec,
                         {'p1': {'primary': 'p1n',
                                 'fields': {'p1n': None},
                                 'ddname': 'DATASET1',
                                 'file': 'p1f.dpt'}})

    def test___init__13(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'Full path name of DPT file for p1 is invalid',
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt',
                      'folder': True}},
              'foldername'))

    def test___init__14(self):
        d = database.Database({'p1': {'primary': 'p1n',
                                      'fields': {'p1n': None},
                                      'ddname': 'DATASET1',
                                      'file': 'p1f.dpt',
                                      'folder': 'folder1'}},
                              'foldername')
        self.assertEqual(d._dbspec,
                         {'p1': {'primary': 'p1n',
                                 'fields': {'p1n': None},
                                 'ddname': 'DATASET1',
                                 'file': 'p1f.dpt',
                                 'folder': 'folder1'}})


class Database_02(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__00(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'Primary name p1n for p2 already used',
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt'},
               'p2': {'primary': 'p1n',
                      'fields': {'p2n': None},
                      'ddname': 'DATASET2',
                      'file': 'p2f.dpt'},
               },
              'foldername'))

    def test___init__01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'File name p1f.dpt linked to p1 cannot link to p2',
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt'},
               'p2': {'primary': 'p2n',
                      'fields': {'p2n': None},
                      'ddname': 'DATASET2',
                      'file': 'p1f.dpt'},
               },
              'foldername'))

    def test___init__02(self):
        d = database.Database({'p1': {'primary': 'p1n',
                                      'fields': {'p1n': None},
                                      'ddname': 'DATASET1',
                                      'file': 'p1f.dpt'},
                               'p2': {'primary': 'p2n',
                                      'fields': {'p2n': None},
                                      'ddname': 'DATASET2',
                                      'file': 'p2f.dpt'},
                               },
                              'foldername')
        self.assertEqual(d._dbspec,
                         {'p1': {'primary': 'p1n',
                                 'fields': {'p1n': None},
                                 'ddname': 'DATASET1',
                                 'file': 'p1f.dpt'},
                          'p2': {'primary': 'p2n',
                                 'fields': {'p2n': None},
                                 'ddname': 'DATASET2',
                                 'file': 'p2f.dpt'},
                          })


class Database_03(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__00(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'items'",
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'secondary': None,
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt'},
               'p2': {'primary': 'p2n',
                      'fields': {'p2n': None},
                      'ddname': 'DATASET2',
                      'file': 'p2f.dpt'},
               },
              'foldername'))

    def test___init__01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ' '.join(['Primary name p1n',
                      'for p1',
                      'must not be in secondary definition',
                      '\(ignoring case\)']),
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'secondary': {'s1n': 'P1n'},
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt'},
               'p2': {'primary': 'p2n',
                      'fields': {'p2n': None},
                      'ddname': 'DATASET2',
                      'file': 'p2f.dpt'},
               },
              'foldername'))

    def test___init__02(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ' '.join(['Primary name p2n',
                      'for p2',
                      'must not be in secondary definition',
                      '\(ignoring case\)']),
            database.Database,
            *({'p1': {'primary': 'p1n',
                      'secondary': {'s1n': 'S1n'},
                      'fields': {'p1n': None},
                      'ddname': 'DATASET1',
                      'file': 'p1f.dpt'},
               'p2': {'primary': 'p2n',
                      'secondary': {'p2n': None},
                      'fields': {'p2n': None},
                      'ddname': 'DATASET2',
                      'file': 'p2f.dpt'},
               },
              'foldername'))

    def test___init__03(self):
        d = database.Database({'p1': {'primary': 'p1n',
                                      'secondary': {'s1n': 'S1n'},
                                      'fields': {'p1n': None},
                                      'ddname': 'DATASET1',
                                      'file': 'p1f.dpt'},
                               'p2': {'primary': 'p2n',
                                      'secondary': {'s2n': 'p1n'},
                                      'fields': {'p2n': None},
                                      'ddname': 'DATASET2',
                                      'file': 'p2f.dpt'},
                               },
                              'foldername')
        self.assertEqual(d._dbspec,
                         {'p1': {'primary': 'p1n',
                                 'secondary': {'s1n': 'S1n'},
                                 'fields': {'p1n': None},
                                 'ddname': 'DATASET1',
                                 'file': 'p1f.dpt'},
                          'p2': {'primary': 'p2n',
                                 'secondary': {'s2n': 'p1n'},
                                 'fields': {'p2n': None},
                                 'ddname': 'DATASET2',
                                 'file': 'p2f.dpt'},
                          })


class Database_04(unittest.TestCase):
    # Test methods which must be overridden in a sublass of database.Database
    # using a minimal database.Database instance.

    def setUp(self):
        self.d = database.Database({}, 'f')

    def tearDown(self):
        self.d = None

    def test_backout_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'backout not implemented',
            self.d.backout)

    def test_close_context_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'close_context not implemented',
            self.d.close_context)

    def test_close_database_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'close_database not implemented',
            self.d.close_database)

    def test_commit_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'commit not implemented',
            self.d.commit)

    def test_db_compatibility_hack_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'db_compatibility_hack not implemented',
            self.d.db_compatibility_hack,
            *(None, None))

    def test_get_database_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_database not implemented',
            self.d.get_database,
            *(None, None))

    def test_is_recno_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'is_recno not implemented',
            self.d.is_recno,
            *(None, None))

    def test_open_context_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'open_context not implemented',
            self.d.open_context)

    def test_get_packed_key_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_packed_key not implemented',
            self.d.get_packed_key,
            *(None, None))

    def test_decode_as_primary_key_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'decode_as_primary_key not implemented',
            self.d.decode_as_primary_key,
            *(None, None))

    def test_encode_primary_key_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'encode_primary_key not implemented',
            self.d.encode_primary_key,
            *(None, None))

    def test_start_transaction_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'start_transaction not implemented',
            self.d.start_transaction)

    def test_make_root_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'make_root not implemented',
            self.d.make_root,
            *(),
            **{})

    def test_use_deferred_update_process_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'use_deferred_update_process not implemented',
            self.d.use_deferred_update_process,
            **{})

    def test_make_connection_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'make_connection not implemented: specific to Sqlite3 interfaces',
            self.d.make_connection)

    def test_get_transaction_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join(('get_transaction not implemented: specific to Berkeley ',
                     'DB interfaces')),
            self.d.get_transaction)

    def test_create_default_parms_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'create_default_parms not implemented',
            self.d.create_default_parms)

    def test_create_recordsetlist_cursor_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'create_recordsetlist_cursor not implemented',
            self.d.create_recordsetlist_cursor,
            *(None, None, None, None))

    def test_get_database_filenames_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_database_filenames not implemented',
            self.d.get_database_filenames)

    def test_get_database_increase_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_database_increase not implemented',
            self.d.get_database_increase)

    def test_get_database_parameters_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_database_parameters not implemented',
            self.d.get_database_parameters)

    def test_get_dptsysfolder_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_dptsysfolder not implemented',
            self.d.get_dptsysfolder)

    def test_get_parms_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_parms not implemented',
            self.d.get_parms)

    def test_get_sfserv_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'get_sfserv not implemented',
            self.d.get_sfserv)

    def test_open_context_allocated_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'open_context_allocated not implemented',
            self.d.open_context_allocated,
            **{})

    def test_open_context_normal_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            'open_context_normal not implemented',
            self.d.open_context_normal,
            **{})


class Database_05(unittest.TestCase):
    # Test methods which work in a database.Database.Database instance: they
    # do not require a subclass of database.Database and are not usually
    # overridden in such subclasses.

    def setUp(self):
        self.d = database.Database(
            {'p1': {'primary': 'p1n',
                    'secondary': {'s1n': 'S1n'},
                    'fields': {'p1n': None},
                    'ddname': 'DATASET1',
                    'file': 'p1f.dpt'},
             'p2': {'primary': 'p2n',
                    'secondary': {'s2n': None},
                    'fields': {'p2n': None},
                    'ddname': 'DATASET2',
                    'file': 'p2f.dpt'},
             },
            'foldername')

    def tearDown(self):
        self.d = None

    def test_dbservices_01(self):
        self.assertIs(self.d.dbservices, self.d._dbservices)

    def test_get_database_folder_01(self):
        self.assertIs(self.d.get_database_folder(), self.d._home_directory)

    def test_get_database_home_01(self):
        self.assertIs(self.d.get_database_home(), self.d._home)

    def test_deferred_update_housekeeping_01(self):
        self.assertEqual(self.d.deferred_update_housekeeping(), None)

    def test_database_specification_01(self):
        self.assertIs(self.d.database_specification, self.d._dbspec)

    def test_close_contexts_01(self):
        self.assertEqual(self.d.close_contexts(None), None)

    def test_repair_cursor_01(self):
        def c():
            pass
        self.assertIs(self.d.repair_cursor(c), c)

    def test_is_primary_recno_01(self):
        self.assertEqual(self.d.is_primary_recno(None), True)

    def test_open_contexts_01(self):
        self.assertEqual(self.d.open_contexts(None), None)

    def test_initial_database_size_01(self):
        self.assertEqual(self.d.initial_database_size(), True)

    def test_increase_database_size_01(self):
        self.assertEqual(self.d.increase_database_size(**{}), None)


class Database_06(unittest.TestCase):
    # Define just enough of a database in a subclass of database.Database to
    # allow methods, which expect an environment provided by such a subclass,
    # not to raise exceptions but exercise each path through method.
    # This implementation may not resemble any particular live implementation.

    def setUp(self):
        class P:
            def __init__(self):
                self._recordsets = {}
            def is_primary(self):
                return True
            def get_primary_record(self, *a):
                return 'record'
            def make_cursor(self, *a):
                return 'pricur'
            def populate_recordset_key_like(self, *a):
                pass
            def populate_recordset_key(self, *a):
                pass
            def populate_recordset_key_startswith(self, *a):
                pass
            def populate_recordset_key_range(self, *a):
                pass
            def populate_recordset_all(self, *a):
                pass
            def file_records_under(self, *a):
                pass
            def unfile_records_under(self, *a):
                pass
        class S:
            def is_primary(self):
                return False
            def get_first_primary_key_for_index_key(self, *a):
                return 1234
            def make_cursor(self, *a):
                return 'seccur'
            def populate_recordset_key_like(self, *a):
                pass
            def populate_recordset_key(self, *a):
                pass
            def populate_recordset_key_startswith(self, *a):
                pass
            def populate_recordset_key_range(self, *a):
                pass
            def populate_recordset_all(self, *a):
                pass
            def file_records_under(self, *a):
                pass
            def unfile_records_under(self, *a):
                pass
        p = P()
        s = S()
        class DD:
            def __init__(self):
                self.primary = p
                self.secondary = {'S1n': s}
                self._dbset = 'p1'
            def associate(self, dbname):
                if dbname in self.secondary:
                    return self.secondary['S1n']
                return self.primary
        class D(database.Database):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                self._dbdef = None
            def close_context(self):
                pass
            def open_context(self):
                pass
            def get_database(self, *a):
                pass
        self.d = D({'p1': {'primary': 'p1n',
                           'secondary': {'s1n': 'S1n'},
                           'fields': {'p1n': None},
                           'ddname': 'DATASET1',
                           'file': 'p1f.dpt'},
                    'p2': {'primary': 'p2n',
                           'secondary': {'s2n': None},
                           'fields': {'p2n': None},
                           'ddname': 'DATASET2',
                           'file': 'p2f.dpt'},
                    },
                   'foldername')
        self.dd = DD()
        self.p = p
        self.s = s

    def tearDown(self):
        self.d = None
        self.dd = None
        self.p = None
        self.s = None

    def test_database_definition_01(self):
        self.assertIs(self.d.database_definition, self.d._dbdef)

    def test_exists_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.exists('p1', 'p1'), True)
        self.assertEqual(self.d.exists('p1', 's1n'), True)

    def test_database_cursor_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.database_cursor('p1', 'p1'), 'pricur')
        self.assertEqual(self.d.database_cursor('p1', 'S1n'), 'seccur')

    def test_get_database_instance_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIs(self.d.get_database_instance('p1', 'p1'), self.p)
        self.assertIs(self.d.get_database_instance('p1', 'S1n'), self.s)

    def test_get_first_primary_key_for_index_key_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(
            self.d.get_first_primary_key_for_index_key('p1', 'S1n', 'key'),
            1234)

    def test_get_primary_record_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.get_primary_record('p1', 'key'), 'record')

    def test_get_table_index_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("get_table_index\(\) missing 2 required positional ",
                     "arguments: 'dbset' and 'dbname'")),
            self.d.get_table_index)
        self.assertEqual(self.d.get_table_index('p1', 'p1'), 'p1n')
        self.assertEqual(self.d.get_table_index('p1', 's1n'), 'S1n')
        self.assertRaisesRegex(
            KeyError,
            'p3',
            self.d.get_table_index,
            *('p3', 's3'))
        self.assertEqual(self.d.get_table_index('p1', 's3'), 's3')

    def test_get_table_indicies_01(self):
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not iterable",
            self.d.get_table_indicies)
        self.d._dbdef = {'a':None}
        self.assertEqual(self.d.get_table_indicies(), frozenset(('a',)))

    def test_is_primary_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.is_primary('p1', 'p1'), True)
        self.assertEqual(self.d.is_primary('p1', 'S1n'), False)

    def test_set_defer_update_01(self):
        self.assertEqual(self.d.set_defer_update(), False)

    def test_set_defer_update_02(self):
        self.assertEqual(self.d.set_defer_update(duallowed=True), True)

    def test_unset_defer_update_01(self):
        self.assertEqual(self.d.unset_defer_update(), None)

    def test_make_recordset_key_like_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset_key_like('p1', 'p1'), recordset.Recordset)
        self.assertIsInstance(
            self.d.make_recordset_key_like('p1', 'S1n'), recordset.Recordset)

    def test_make_recordset_key_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset_key('p1', 'p1'), recordset.Recordset)
        self.assertIsInstance(
            self.d.make_recordset_key('p1', 'S1n'), recordset.Recordset)

    def test_make_recordset_key_startswith_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset_key_startswith('p1', 'p1'
                                                 ), recordset.Recordset)
        self.assertIsInstance(
            self.d.make_recordset_key_startswith('p1', 'S1n'
                                                 ), recordset.Recordset)

    def test_make_recordset_key_range_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset_key_range('p1', 'p1'), recordset.Recordset)
        self.assertIsInstance(
            self.d.make_recordset_key_range('p1', 'S1n'), recordset.Recordset)

    def test_make_recordset_all_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset_all('p1', 'p1'), recordset.Recordset)
        self.assertIsInstance(
            self.d.make_recordset_all('p1', 'S1n'), recordset.Recordset)

    def test_make_recordset_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(
            self.d.make_recordset('p1'), recordset.Recordset)

    def test_file_records_under_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(
            self.d.file_records_under(
                'p1', 'p1', recordset.Recordset(self.d, 'p1'), 'k'), None)
        self.assertEqual(
            self.d.file_records_under(
                'p1', 'S1n', recordset.Recordset(self.d, 'p1'), 'k'), None)

    def test_file_records_under_02(self):
        self.d._dbdef = {'p1': self.dd, 'p2': self.dd}
        self.assertRaisesRegex(
            database.DatabaseError,
            'Record set was not created from dbset database',
            self.d.file_records_under,
            *('p1', 'p1', recordset.Recordset(self.d, 'p2'), 'k'))
        self.assertRaisesRegex(
            database.DatabaseError,
            'Record set was not created from dbset database',
            self.d.file_records_under,
            *('p1', 'S1n', recordset.Recordset(self.d, 'p2'), 'k'))

    def test_file_records_under_03(self):
        self.d._dbdef = {'p1': self.dd}
        r = recordset.Recordset(self.d, 'p1')
        r._database = True # or anything not returned by id(get_database())
        self.assertRaisesRegex(
            database.DatabaseError,
            'Record set was not created from this database instance',
            self.d.file_records_under,
            *('p1', 'p1', r, 'k'))
        self.assertRaisesRegex(
            database.DatabaseError,
            'Record set was not created from this database instance',
            self.d.file_records_under,
            *('p1', 'S1n', r, 'k'))

    def test_unfile_records_under_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.unfile_records_under('p1', 'p1', 'key'), None)
        self.assertEqual(self.d.unfile_records_under('p1', 'S1n', 'key'), None)

    def test_record_finder_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(self.d.record_finder('p1'), Find)

    def test_record_selector_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(self.d.record_selector('p1'), Where)

    def test_values_finder_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(self.d.values_finder('p1'), FindValues)

    def test_values_selector_01(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertIsInstance(self.d.values_selector('p1'), WhereValues)

    def test_get_associated_indicies_01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("get_associated_indicies\(\) missing 1 required ",
                     "positional argument: 'dbset'")),
            self.d.get_associated_indicies)

    def test_get_associated_indicies_02(self):
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not subscriptable",
            self.d.get_associated_indicies,
            *(None,))

    def test_get_associated_indicies_03(self):
        self.d._dbdef = {'p1': self.dd}
        self.assertEqual(self.d.get_associated_indicies('p1'),
                         frozenset({'p1', 'S1n'}))


if __name__ == '__main__':
    unittest.main()
