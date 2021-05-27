# dbduapi_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Tests for each function and class method in each module in ../test defining
stubs where necessary."""

import unittest
import os
import sys
import shutil

from ..api.segmentsize import SegmentSize
from ..api.bytebit import Bitarray
from ..api.databasedu import DatabaseduError
try:
    from .. import dbapi
    try:
        import bsddb3 as bsddb
    except ImportError:
        import bsddb
except:
    if sys.version_info.major < 3:
        print('dbapi not imported: ok if neither bsddb3 nor bsddb installed.')
    else:
        print('dbapi not imported: ok if bsddb3 not installed.')
    dbapi = False
try:
    from .. import dbduapi
    try:
        import bsddb3 as bsddb
    except ImportError:
        import bsddb
except:
    if sys.version_info.major < 3:
        print(''.join(('dbduapi not imported: ',
                       'ok if neither bsddb3 nor bsddb installed.')))
    else:
        print('dbduapi not imported: ok if bsddb3 not installed.')
    dbduapi = False
if not (dbapi or dbduapi):
    print(''.join(('\n',
                   'Nothing was imported: ',
                   'does current working directory contain solentware_base?',
                   '\n')))


def _folder(name):
    directory = os.path.expanduser(
        os.path.join('~', 'solentwarebase_tests', name))
    os.makedirs(directory)
    return os.path.join(directory)


def _delete_folder(name):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'solentwarebase_tests', name)),
        ignore_errors=True)


class TCdbduapi(unittest.TestCase):

    def setUp(self):
        self.folder = _folder('TCdbduapi')
        self.database_specification = {
            'File1': {'primary': 'P',
                      'fields': {'P': {}, 'S': {}},
                      'ddname': 'DDP',
                      'file': 'p',
                      'secondary': {'S': None},
                      },
            }
        class FileSpec(dict):
            def __init__(self, **k):
                super().__init__(**k)
                self.field_name = lambda a: a
        self.fsc = FileSpec
        class DI:
            def get_transaction(self):
                pass
        self.di = DI

    def tearDown(self):
        _delete_folder('TCdbduapi')

    def test_DBduapi___init___01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertIsInstance(d, dbduapi.DBduapi)

    def test_DBduapi___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'database_specification'")),
            dbduapi.DBduapi)

    def test_DBduapi___init___03(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 2 required positional arguments: ",
                     "'databasefolder' and 'DBenvironment'")),
            dbduapi.DBduapi,
            *({},))

    def test_DBduapi___init___04(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertIsInstance(d, dbduapi.DBduapi)

    def test_DBduapi_start_transaction_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.start_transaction(), None)

    def test_DBduapi_use_deferred_update_process_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            DatabaseduError,
            'Query use of du when in deferred update mode',
            d.use_deferred_update_process,
            **{})

    def test_DBduapi_reset_defer_limit_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.reset_defer_limit(), None)

    def test_DBduapi_set_defer_limit_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_limit(12000), None)

    def test_DBduapi_set_defer_limit_02(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            TypeError,
            ''.join(("set_defer_limit\(\) missing 1 required positional ",
                     "argument: 'limit'")),
            d.set_defer_limit)

    def test_DBduapi_close_context_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.close_context(), None)

    def test_DBduapi_open_context_02(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            bsddb.db.DBNoSuchFileError,
            "\(2, 'No such file or directory'\)",
            d.open_context)

    def test_DBduapi_open_context_03(self):
        d = dbduapi.DBduapi({}, self.folder, {'flags': bsddb.db.DB_CREATE})
        self.assertRaisesRegex(
            bsddb.db.DBInvalidArgError,
            ''.join(("\(22, 'Invalid argument -- environment did not include ",
                     "a memory pool'\)")),
            d.open_context)

    def test_DBduapi_open_context_04(self):
        d = dbduapi.DBduapi({},
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        self.assertEqual(d.open_context(), True)

    def test_DBduapi__get_deferable_update_files_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            TypeError,
            ''.join(("_get_deferable_update_files\(\) missing 1 required ",
                     "positional argument: 'db'")),
            d._get_deferable_update_files)

    def test_DBduapi__get_deferable_update_files_02(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            TypeError,
            ''.join(("_get_deferable_update_files\(\) takes 2 positional ",
                     "arguments but 3 were given")),
            d._get_deferable_update_files,
            *(None, None))

    def test_DBduapi__get_deferable_update_files_03(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d._get_deferable_update_files(None), False)

    def test_DBduapi__get_deferable_update_files_04(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files(None),
                         {'File1': ['S']})

    def test_DBduapi__get_deferable_update_files_05(self):
        database_specification = {
            'File1': {'primary': 'P',
                      'fields': {'P': {}},
                      'ddname': 'DDP',
                      'file': 'p',
                      'secondary': {},
                      },
            }
        d = dbduapi.DBduapi(
            self.fsc(**database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files(None), False)

    def test_DBduapi__get_deferable_update_files_06(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files('File1'),
                         {'File1': ['S']})

    def test_DBduapi__get_deferable_update_files_07(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files('File2'), {})

    def test_DBduapi__get_deferable_update_files_08(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files(['File1', 'File2']),
                         {'File1': ['S']})

    def test_DBduapi__get_deferable_update_files_09(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d._get_deferable_update_files({'File2', 'File3'}), {})

    def test_DBduapi_set_defer_update_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_update(), None)

    def test_DBduapi_set_defer_update_02(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_update(duallowed=False), None)

    def test_DBduapi_set_defer_update_03(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_update(duallowed=True), None)

    def test_DBduapi_set_defer_update_04(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_update(db='File1', duallowed=False), None)

    def test_DBduapi_set_defer_update_05(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.set_defer_update(db='File1', duallowed=True), None)

    def test_DBduapi_set_defer_update_06(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            d.set_defer_update,
            **dict(db='File1', duallowed=False))

    def test_DBduapi_set_defer_update_07(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            d.set_defer_update,
            **dict(db='File1', duallowed=True))

    def test_DBduapi_set_defer_update_08(self):
        # Compare with test_set_defer_update_06.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        self.assertEqual(d.set_defer_update(db='File1', duallowed=False), False)

    def test_DBduapi_set_defer_update_09(self):
        # Compare with test_set_defer_update_07.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        self.assertEqual(d.set_defer_update(db='File1', duallowed=True), True)

    def test_DBduapi_set_defer_update_10(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d.set_defer_update(db='File2', duallowed=False), None)

    def test_DBduapi_set_defer_update_11(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d.set_defer_update(db='File2', duallowed=True), None)

    def test_DBduapi_unset_defer_update_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.unset_defer_update(), None)

    def test_DBduapi_unset_defer_update_02(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d.unset_defer_update(), None)

    def test_DBduapi_unset_defer_update_03(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d.unset_defer_update(db='File1'), None)

    def test_DBduapi_unset_defer_update_04(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertEqual(d.unset_defer_update(db=('File2', 'File3')), None)

    def test_DBduapi_do_final_segment_deferred_updates_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertEqual(d.do_final_segment_deferred_updates(), None)

    def test_DBduapi_do_final_segment_deferred_updates_02(self):
        d = dbduapi.DBduapi(
            self.fsc(**self.database_specification), self.folder, {})
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            d.do_final_segment_deferred_updates,
            **dict(db='File1'))

    def test_DBduapi_do_final_segment_deferred_updates_03(self):
        # Compare with test_do_final_segment_deferred_updates_02.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        self.assertEqual(d.do_final_segment_deferred_updates(db='File1'), None)

    # Further tests of DBduapi.do_final_segment_deferred_updates() will assume
    # a working DBduapi.put_instance() method to provide deferred updates.

    def test_DBduapi_put_instance_01(self):
        d = dbduapi.DBduapi({}, self.folder, {})
        self.assertRaisesRegex(
            TypeError,
            ''.join(("put_instance\(\) missing 2 required positional ",
                     "arguments: 'dbset' and 'instance'")),
            d.put_instance)

    def test_DBduapi_put_instance_02(self):
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
        class R:
            def __init__(self, k=None, v=None):
                self.key = K()
            def set_packed_value_and_indexes(self):
                pass
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        r = R()
        self.assertRaisesRegex(
            DatabaseduError,
            "Cannot reuse record number in deferred update.",
            d.put_instance,
            *('File1', r))

    def test_DBduapi_put_instance_03(self):
        # *_03, *_04, and *_05, differ in V.pack() method.
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict()
            def pack_value(self):
                return repr(self._v)
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)

    def test_DBduapi_put_instance_04(self):
        # *_03, *_04, and *_05, differ in V.pack() method.
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict(S=[])
            def pack_value(self):
                return repr(self._v)
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)

    def test_DBduapi_put_instance_05(self):
        # *_03, *_04, and *_05, differ in V.pack() method.
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict(S=['kvone', 'kvtwo'])
            def pack_value(self):
                return repr(self._v)
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)

    def test_DBduapi_put_instance_06(self):
        # Extend *_05 to do lots of records and add close_context() to do
        # things properly avoiding some exceptions which do not happen at
        # much lower volumes.
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict(S=self.index_function())
            def pack_value(self):
                return repr(self._v)
            def index_function(self):
                if int(self._v[-1]) % 2:
                    return 'odd'
                else:
                    return 'even'
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        d.open_context()
        for i in range(16000):
            r = R()
            r.key._k = 0
            r.value._v = 'two words ' + str(i)
            d.put_instance('File1', r)
        # Up to a point an 'Exception ignored' in __del__ does not occur.
        # Adding close_context(), which should be done anyway, seems to stop
        # the ignored exception but causes an exception in txn_checkpoint.
        # Adding the DB_INIT_TXN flag cures this, and brings the flags used
        # closer to the settings actually used in the real thing.
        # Creating 16000 records is enough to see this at time of writing.
        d.close_context()

    def test_DBduapi_put_instance_07(self):
        # Create enough records so both do_final_segment_deferred_updates and
        # do_segment_deferred_updates are used.  Change V._v each record.
        # Note addition of DB_INIT_TXN to DBEnv flags and the close_context()
        # call compared with *_05.
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict(S=self.index_function())
            def pack_value(self):
                return repr(self._v)
            def index_function(self):
                if int(self._v[-1]) % 2:
                    return 'odd'
                else:
                    return 'even'
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        d.open_context()
        minupd = min(d.deferred_update_points)
        for i in range(minupd + max(1, minupd // 2)):
            r = R()
            r.key._k = 0
            r.value._v = 'two words ' + str(i)
            d.put_instance('File1', r)
        d.do_final_segment_deferred_updates()
        d.close_context()

    def test_Primary___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 3 required positional arguments: ",
                     "'dbfile', 'dbdesc', and 'dbname'")),
            dbduapi.Primary)

    def test_Primary___init___02(self):
        self.assertIsInstance(
            dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di()),
            dbduapi.Primary)

    def test_Primary_defer_put_01(self):
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 2 required positional arguments: ",
                     "'segment' and 'record_number'")),
            p.defer_put)

    def test_Primary_defer_put_02(self):
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        p.existence_bit_maps[0] = SegmentSize.empty_bitarray.copy()
        p.defer_put(0, 3)

    def test_Primary_defer_put_03(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        p._existence_bits._segment_link = d.database_definition[
            'File1'].primary._existence_bits._segment_link
        p.defer_put(0, 3)

    def test_Primary_defer_put_04(self):
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict()
            def pack_value(self):
                return repr(self._v)
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        # Create the Primary instance to be tested.
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        # Set up a database with one record.
        # (This goes through a Primary instance by the route in test_*_03).
        # (See test_*_05 for database setup without deferred updates)
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)
        d.do_final_segment_deferred_updates()
        d.close_context()
        # Add element of one record using the Primary instance to be tested.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        p._existence_bits._segment_link = d.database_definition[
            'File1'].primary._existence_bits._segment_link
        p.defer_put(0, 3)

    def test_Primary_defer_put_05(self):
        class K:
            def __init__(self):
                self._k = None
            def pack(self):
                return self._k
            def load(self, k):
                self._k = k
        class V:
            def __init__(self):
                self._v = None
            def pack(self):
                return self.pack_value(), dict()
            def pack_value(self):
                return repr(self._v)
        class R:
            def __init__(self):
                self.key = K()
                self.value = V()
                self.srvalue = None
            def set_packed_value_and_indexes(self):
                self.srvalue, self.srindex = self.packed_value()
            def packed_value(self):
                return self.value.pack()
            _putcallbacks = dict()
        # Create the Primary instance to be tested.
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        # Set up a database with one record.
        # (This does not use a dbduapi.Primary instance).
        # (See test_*_04 for database setup with deferred updates)
        d = dbapi.DBapi(self.fsc(**self.database_specification),
                        self.folder,
                        {'flags': (bsddb.db.DB_CREATE |
                                   bsddb.db.DB_INIT_MPOOL |
                                   bsddb.db.DB_INIT_TXN)})
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)
        d.close_context()
        # Add element of one record using the Primary instance to be tested.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        p._existence_bits._segment_link = d.database_definition[
            'File1'].primary._existence_bits._segment_link
        p.defer_put(0, 3)

    def test_Primary_write_existence_bit_map_01(self):
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("write_existence_bit_map\(\) missing 1 required ",
                     "positional argument: 'segment'")),
            p.write_existence_bit_map)

    def test_Primary_write_existence_bit_map_02(self):
        # Repeat test_Primary_defer_put_03 and do write_existence_bit_map call.
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL)})
        d.open_context()
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        p._existence_bits._segment_link = d.database_definition[
            'File1'].primary._existence_bits._segment_link
        p.defer_put(0, 3)
        p.write_existence_bit_map(0)

    def test_Primary_make_cursor_01(self):
        # Both tests in this test case.
        p = dbduapi.Primary('File1',
                            self.fsc(**self.database_specification)['File1'],
                            'P',
                            database_instance=self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("make_cursor\(\) missing 2 required positional ",
                     "arguments: 'dbobject' and 'keyrange'")),
            p.make_cursor)
        self.assertRaisesRegex(
            dbduapi.DBduapiError,
            'make_cursor not implemented',
            p.make_cursor,
            *(None, None))

    def test_Secondary___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 3 required positional arguments: ",
                     "'dbfile', 'dbdesc', and 'dbname'")),
            dbduapi.Secondary)

    def test_Secondary___init___02(self):
        self.assertIsInstance(
            dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di()),
            dbduapi.Secondary)

    def test_Secondary_defer_put_01(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 1 required positional argument: ",
                     "'key'")),
            s.defer_put)

    def test_Secondary_defer_put_02(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'encode'",
            s.defer_put,
            *(None, None, None))

        # Need to devise a way of enforcing both None arguments to generate an
        # exception, or give some assurance the arguments are integers.
        # Perhaps a single namedtuple argument containing key, segment, and
        # record number.
        s.defer_put('odd', None, None)
        #for i in range(SegmentSize.db_upper_conversion_limit+1):
        #    s.defer_put('odd', None, i)

    def test_Secondary_new_deferred_root_01(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("new_deferred_root\(\) missing 1 required positional ",
                     "argument: 'dbenv'")),
            s.new_deferred_root)

    def test_Secondary_new_deferred_root_02(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        # There is usually a DBEnv object, but None is legal.
        dbenv = None
        # One _table_link entry normally exists before any new_deferred_root()
        # calls.
        # Create a cursor to test if DB is closed or open.
        db = bsddb.db.DB(dbenv)
        s._table_link = [db]
        self.assertEqual(len(s._table_link), 1)
        s._table_link[-1].open(
            'gggg', s._keyvalueset_name, bsddb.db.DB_BTREE, bsddb.db.DB_CREATE)
        # First deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 2)
        s._table_link[-1].cursor()
        s._table_link[-2].cursor()
        # Second deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 3)
        s._table_link[-3].cursor()
        s._table_link[-1].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-2].cursor)
        # Third deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 4)
        s._table_link[-4].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-3].cursor)
        s._table_link[-1].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-2].cursor)
        # Force the exception when DB.open() call fails.
        # All open DBs are closed.
        db = s._table_link[0]
        s._dataname = None
        self.assertRaisesRegex(
            TypeError,
            "sequence item 1: expected str instance, NoneType found",
            s.new_deferred_root,
            *(None,))
        self.assertEqual(s._table_link, None)
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            db.cursor)
        for f in ('1_File1', '2_File1', '3_File1', 'gggg'):
            os.remove(f)

    def test_Secondary_new_deferred_root_03(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        # There is usually a DBEnv object, but None is legal.
        dbenv = bsddb.db.DBEnv()
        d = dbduapi.DBduapi({},
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        d.open_context()
        dbenv.open(d._home_directory)
        # One _table_link entry normally exists before any new_deferred_root()
        # calls.
        # Create a cursor to test if DB is closed or open.
        db = bsddb.db.DB(dbenv)
        s._table_link = [db]
        self.assertEqual(len(s._table_link), 1)
        s._table_link[-1].open(
            'gggg', s._keyvalueset_name, bsddb.db.DB_BTREE, bsddb.db.DB_CREATE)
        # First deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 2)
        s._table_link[-1].cursor()
        s._table_link[-2].cursor()
        # Second deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 3)
        s._table_link[-3].cursor()
        s._table_link[-1].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-2].cursor)
        # Third deferred DB.
        s.new_deferred_root(dbenv)
        self.assertEqual(len(s._table_link), 4)
        s._table_link[-4].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-3].cursor)
        s._table_link[-1].cursor()
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            s._table_link[-2].cursor)
        # Force the exception when DB.open() call fails.
        # All open DBs are closed.
        db = s._table_link[0]
        s._dataname = None
        self.assertRaisesRegex(
            TypeError,
            "sequence item 1: expected str instance, NoneType found",
            s.new_deferred_root,
            *(None,))
        self.assertEqual(s._table_link, None)
        self.assertRaisesRegex(
            bsddb.db.DBError,
            "\(0, 'DB object has been closed'\)",
            db.cursor)
        #for f in ('1_File1', '2_File1', '3_File1', 'gggg'):
        #    os.remove(f)

    def test_Secondary_sort_and_write_01(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("sort_and_write\(\) missing 2 required positional ",
                     "arguments: 'dbenv' and 'segment'")),
            s.sort_and_write)

    def test_Secondary_sort_and_write_02(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'get_segment_bits_database'",
            s.sort_and_write,
            *(None, None))

    def test_Secondary_sort_and_write_03(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.sort_and_write(None, 0), None)

    def test_Secondary_sort_and_write_04(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(d._dbservices.__class__, bsddb.db.DBEnv().__class__)
        self.assertEqual(s.sort_and_write(d._dbservices, 0), None)

    def test_Secondary_sort_and_write_05(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        s.values = {'odd': 10}
        self.assertRaisesRegex(
            TypeError,
            "Bytes or Integer object expected for key, str found",
            s.sort_and_write,
            *(d._dbservices, 0))

    def test_Secondary_sort_and_write_06(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.values = {b'odd': 10}
        self.assertEqual(s.sort_and_write(d._dbservices, 0), None)

    def test_Secondary_sort_and_write_07(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.values = {b'odd': [10, 12]}
        self.assertEqual(s.sort_and_write(d._dbservices, 0), None)

    def test_Secondary_sort_and_write_08(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {b'odd': ba}
        self.assertEqual(s.sort_and_write(d._dbservices, 1), None)

    def test_Secondary_sort_and_write_09(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        s.values = {b'odd': 10}
        self.assertEqual(s.sort_and_write(d._dbservices, 0), None)

    def test_Secondary_sort_and_write_10(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        s.values = {b'odd': [10, 12]}
        self.assertEqual(s.sort_and_write(d._dbservices, 0), None)

    def test_Secondary_sort_and_write_11(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {b'odd': ba}
        self.assertEqual(s.sort_and_write(d._dbservices, 1), None)

    def test_Secondary_merge_01(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("merge\(\) missing 1 required positional ",
                     "argument: 'dbenv'")),
            s.merge)

    def test_Secondary_merge_02(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        s._table_link = []
        self.assertEqual(len(s.table_connection_list), 0)
        self.assertRaisesRegex(
            IndexError,
            "list index out of range",
            s.merge,
            *(None,))

    def test_Secondary_merge_03(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        s._table_link = [None]
        self.assertEqual(len(s.table_connection_list), 1)
        self.assertEqual(s.merge(None), None)

    def test_Secondary_merge_04(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        s._table_link = [None, None]
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'get_dbname'",
            s.merge,
            *(None,))

    def test_Secondary_merge_05(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        self.assertEqual(len(s.table_connection_list), 1)
        self.assertEqual(s.merge(None), None)

    def test_Secondary_merge_06(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        s.values = {b'odd': 10}
        s.sort_and_write(d._dbservices, 0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'dbrename'",
            s.merge,
            *(None,))

    def test_Secondary_merge_07(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        s.values = {b'odd': 10}
        s.sort_and_write(d._dbservices, 0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(d._dbservices.__class__, bsddb.db.DBEnv().__class__)
        self.assertEqual(s.merge(d._dbservices), None)
        self.assertEqual(len(s.table_connection_list), 3)

    def test_Secondary_merge_08(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        s.values = {b'odd': [10, 12]}
        s.sort_and_write(d._dbservices, 0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(d._dbservices.__class__, bsddb.db.DBEnv().__class__)
        self.assertEqual(s.merge(d._dbservices), None)
        self.assertEqual(len(s.table_connection_list), 3)

    def test_Secondary_merge_09(self):
        d = dbduapi.DBduapi(self.fsc(**self.database_specification),
                            self.folder,
                            {'flags': (bsddb.db.DB_CREATE |
                                       bsddb.db.DB_INIT_MPOOL |
                                       bsddb.db.DB_INIT_TXN)})
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, dbduapi.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {b'odd': ba}
        s.sort_and_write(d._dbservices, 0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(d._dbservices.__class__, bsddb.db.DBEnv().__class__)
        self.assertEqual(s.merge(d._dbservices), None)
        self.assertEqual(len(s.table_connection_list), 3)

    def test_Secondary_make_cursor_01(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            TypeError,
            ''.join(("make_cursor\(\) missing 2 required positional ",
                     "arguments: 'dbobject' and 'keyrange'")),
            s.make_cursor)

    def test_Secondary_make_cursor_02(self):
        s = dbduapi.Secondary('File1',
                              self.fsc(**self.database_specification)['File1'],
                              'S',
                              self.di())
        self.assertRaisesRegex(
            dbduapi.DBduapiError,
            "make_cursor not implemented",
            s.make_cursor,
            *(None, None))


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if dbduapi:
        runner().run(loader(TCdbduapi))
