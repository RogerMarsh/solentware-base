# same_interface_test.py
# Copyright 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dbapi, dptapi, sqlt3api, and apswapi must present the same interface"""

import unittest
from copy import copy, deepcopy
import os

try:
    from .. import dbapi
    bsddb = dbapi
except:
    bsddb = False
try:
    from .. import sqlite3api
    sqlite3 = sqlite3api
except:
    sqlite3 = False
try:
    from .. import apswapi
    apsw = True
except:
    apsw = apswapi
try:
    from .. import dptbase
    dpt = dptbase
except:
    dpt = False


class SameInterfaceTC(unittest.TestCase):

    def setUp(self):
        self.bsddb = bsddb
        self.sqlite3 = sqlite3
        self.apsw = apsw
        self.dpt = dpt
        self.methods = frozenset((
            'allocate_and_open_contexts',
            'backout',
            'cede_contexts_to_process',
            'close_context',
            'close_contexts',
            'close_database',
            'commit',
            'create_default_parms',
            'create_recordset_cursor',
            'create_recordsetlist_cursor',
            'database_cursor',
            'database_definition',
            'database_specification',
            'db_compatibility_hack',
            'dbservices',
            'decode_as_primary_key',
            'decode_record_number',
            'deferred_update_housekeeping',
            'delete_instance',
            'do_database_task',
            'do_deferred_updates',
            'edit_instance',
            'encode_primary_key',
            'encode_record_key',
            'encode_record_number',
            'encode_record_selector',
            'exists',
            'file_records_under',
            'files_exist',
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
            'values_selector'))

    def tearDown(self):
        pass

    def test_interface_01(self):
        api = {}
        if self.bsddb:
            api[bsddb] = {a for a in dir(dbapi.DBapi)
                          if not a.startswith('__') and not a.endswith('__')}
        if self.sqlite3:
            api[sqlite3] = {a for a in dir(sqlite3api.Sqlite3api)
                            if not a.startswith('__') and not a.endswith('__')}
        if self.apsw:
            api[apsw] = {a for a in dir(apswapi.Sqlite3api)
                         if not a.startswith('__') and not a.endswith('__')}
        if self.dpt:
            api[dpt] = {a for a in dir(dptbase.Database)
                        if not a.startswith('__') and not a.endswith('__')}
        for api_a, api_b in ((self.bsddb, self.sqlite3),
                             (self.bsddb, self.apsw),
                             (self.bsddb, self.dpt),
                             (self.sqlite3, self.apsw),
                             (self.sqlite3, self.dpt),
                             (self.apsw, self.dpt),
                             ):
            if api_a and api_b:
                self.assertEqual(api[api_a], api[api_b])
        self.assertNotEqual(len(api), 0)
        for v in api.values():
            self.assertEqual(frozenset(v), self.methods)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(SameInterfaceTC))

