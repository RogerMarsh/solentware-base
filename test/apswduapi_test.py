# apswduapi_test.py
# Copyright 2016 Roger Marsh
# License: See LICENSE.TXT (BSD licence)

"""apswduapi tests.

Each test is in it's own class for consistency with sqlite3duapi tests.  There
are only two errors, not database locks, which appear when run on Microsoft
Windows.

apsw tests take nearly 15 minutes compared with a few seconds for sqlite3 tests.

"""


import unittest
import os
import sys
import shutil

from ..api.segmentsize import SegmentSize
from ..api.bytebit import Bitarray
from ..api.databasedu import DatabaseduError
from .. import _sqlite
from .. import _sqlitedu
from .. import apswapi
from .. import apswduapi


def _folder(name):
    directory = os.path.expanduser(
        os.path.join('~', 'solentwarebase_tests', name))

    # Workaround ignored 'file in use in another process' exception in
    # _delete_folder() on Microsoft Windows.
    # _delete_folder() usually called in test's tearDown() method.
    os.makedirs(directory, exist_ok=sys.platform=='win32')

    return os.path.join(directory)


def _delete_folder(name):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'solentwarebase_tests', name)),
        ignore_errors=True)


class TCapswapi(unittest.TestCase):

    def setUp(self):
        self.folder = _folder('TCapswapi')

    def tearDown(self):
        _delete_folder('TCapswapi')

    def test_Database_make_connection_01(self):
        class C:
            def __init__(self, *a, **b):
                pass
        d = apswapi.Database(C, C, {}, self.folder)
        self.assertEqual(d._dbservices, None)
        d.make_connection()
        self.assertIsInstance(d._dbservices, apswapi.apsw.Connection)
        dbs = d._dbservices
        d.make_connection()
        self.assertIs(dbs, d._dbservices)

    def test_Sqlite3api___init___01(self):
        d = apswapi.Sqlite3api({}, self.folder)
        self.assertIsInstance(d, apswapi.Sqlite3api)

    def test_Sqlite3api___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'database_specification'")),
            apswapi.Sqlite3api)

    def test_Sqlite3api___init___03(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'databasefolder'")),
            apswapi.Sqlite3api,
            *({},))


class TCapswduapi(unittest.TestCase):

    def setUp(self):
        self.folder = _folder(self.name())
        self.database_specification = {
            'File1': {'primary': 'P',
                      'fields': {'P': {}, 'S': {}},
                      'ddname': 'DDP',
                      'file': 'p',
                      'secondary': {'S': None},
                      'filedesc': {'fileorg': 'eo'},
                      },
            }
        class FileSpec(dict):
            def __init__(self, **k):
                super().__init__(**k)
                self.field_name = lambda a: a
        self.fsc = FileSpec

    def tearDown(self):
        _delete_folder(self.name())

    def name(self):
        return self.__class__.__name__


class TC__01(TCapswduapi):

    def test_Sqlite3duapi___init___01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertIsInstance(d, apswduapi.Sqlite3duapi)


class TC__02(TCapswduapi):

    def test_Sqlite3duapi___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'database_specification'")),
            apswduapi.Sqlite3duapi)


class TC__03(TCapswduapi):

    def test_Sqlite3duapi___init___03(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 1 required positional argument: ",
                     "'databasefolder'")),
            apswduapi.Sqlite3duapi,
            *({},))


class TC__04(TCapswduapi):

    def test_Sqlite3duapi___init___04(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertIsInstance(d, apswduapi.Sqlite3duapi)


class TC__05(TCapswduapi):

    def test_Sqlite3duapi_close_context_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.close_context(), None)


class TC__06(TCapswduapi):

    def test_Sqlite3duapi_open_context_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.open_context(), True)


class TC__07(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertRaisesRegex(
            TypeError,
            ''.join(("_get_deferable_update_files\(\) missing 1 required ",
                     "positional argument: 'db'")),
            d._get_deferable_update_files)


class TC__08(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_02(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertRaisesRegex(
            TypeError,
            ''.join(("_get_deferable_update_files\(\) takes 2 positional ",
                     "arguments but 3 were given")),
            d._get_deferable_update_files,
            *(None, None))


class TC__09(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_03(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d._get_deferable_update_files(None), False)


class TC__10(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_04(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files(None),
                         {'File1': ['S']})


class TC__11(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_05(self):
        database_specification = {
            'File1': {'primary': 'P',
                      'fields': {'P': {}},
                      'ddname': 'DDP',
                      'file': 'p',
                      'secondary': {},
                      'filedesc': {'fileorg': 'eo'},
                      },
            }
        d = apswduapi.Sqlite3duapi(
            self.fsc(**database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files(None), False)


class TC__12(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_06(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files('File1'),
                         {'File1': ['S']})


class TC__13(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_07(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files('File2'), {})


class TC__14(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_08(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files(['File1', 'File2']),
                         {'File1': ['S']})


class TC__15(TCapswduapi):

    def test_Sqlite3duapi__get_deferable_update_files_09(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d._get_deferable_update_files({'File2', 'File3'}), {})


class TC__16(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.set_defer_update(), None)


class TC__17(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_02(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.set_defer_update(duallowed=False), None)


class TC__18(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_03(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.set_defer_update(duallowed=True), None)


class TC__19(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_04(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.set_defer_update(db='File1', duallowed=False), None)


class TC__20(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_05(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.set_defer_update(db='File1', duallowed=True), None)


class TC__21(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_06(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            d.set_defer_update,
            **dict(db='File1', duallowed=False))


class TC__22(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_07(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            d.set_defer_update,
            **dict(db='File1', duallowed=True))


class TC__23(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_08(self):
        # Compare with test_set_defer_update_06.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        self.assertEqual(d.set_defer_update(db='File1', duallowed=False), False)


class TC__24(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_09(self):
        # Compare with test_set_defer_update_07.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        self.assertEqual(d.set_defer_update(db='File1', duallowed=True), True)


class TC__25(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_10(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d.set_defer_update(db='File2', duallowed=False), None)


class TC__26(TCapswduapi):

    def test_Sqlite3duapi_set_defer_update_11(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d.set_defer_update(db='File2', duallowed=True), None)


class TC__27(TCapswduapi):

    def test_Sqlite3duapi_unset_defer_update_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.unset_defer_update(), None)


class TC__28(TCapswduapi):

    def test_Sqlite3duapi_unset_defer_update_02(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d.unset_defer_update(), None)


class TC__29(TCapswduapi):

    def test_Sqlite3duapi_unset_defer_update_03(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d.unset_defer_update(db='File1'), None)


class TC__30(TCapswduapi):

    def test_Sqlite3duapi_unset_defer_update_04(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertEqual(d.unset_defer_update(db=('File2', 'File3')), None)


class TC__31(TCapswduapi):

    def test_Sqlite3duapi_do_final_segment_deferred_updates_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertEqual(d.do_final_segment_deferred_updates(), None)


class TC__32(TCapswduapi):

    def test_Sqlite3duapi_do_final_segment_deferred_updates_02(self):
        d = apswduapi.Sqlite3duapi(
            self.fsc(**self.database_specification), self.folder)
        self.assertRaisesRegex(
            TypeError,
            "sequence item 3: expected str instance, NoneType found",
            d.do_final_segment_deferred_updates,
            **dict(db='File1'))


class TC__33(TCapswduapi):

    def test_Sqlite3duapi_do_final_segment_deferred_updates_03(self):
        # Compare with test_do_final_segment_deferred_updates_02.
        # See superclass tests for test of open_context() use here.
        # In this class' open_context() tests a simpler database specification
        # is good enough.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        self.assertEqual(d.do_final_segment_deferred_updates(db='File1'), None)

    # Further tests of DBduapi.do_final_segment_deferred_updates() will assume
    # a working DBduapi.put_instance() method to provide deferred updates.


class TC__34(TCapswduapi):

    def test_Sqlite3duapi_put_instance_01(self):
        d = apswduapi.Sqlite3duapi({}, self.folder)
        self.assertRaisesRegex(
            TypeError,
            ''.join(("put_instance\(\) missing 2 required positional ",
                     "arguments: 'dbset' and 'instance'")),
            d.put_instance)


class TC__35(TCapswduapi):

    def test_Sqlite3duapi_put_instance_02(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        r = R()
        self.assertRaisesRegex(
            DatabaseduError,
            "Cannot reuse record number in deferred update.",
            d.put_instance,
            *('File1', r))


class TC__36(TCapswduapi):

    def test_Sqlite3duapi_put_instance_03(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)


class TC__37(TCapswduapi):

    def test_Sqlite3duapi_put_instance_04(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)


class TC__38(TCapswduapi):

    def test_Sqlite3duapi_put_instance_05(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)


class TC__39(TCapswduapi):

    def test_Sqlite3duapi_put_instance_06(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
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


class TC__40(TCapswduapi):

    def test_Sqlite3duapi_put_instance_07(self):
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
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        minupd = min(d.deferred_update_points)
        for i in range(minupd + max(1, minupd // 2)):
            r = R()
            r.key._k = 0
            r.value._v = 'two words ' + str(i)
            d.put_instance('File1', r)
        d.do_final_segment_deferred_updates()
        d.close_context()


class TC__41(TCapswduapi):

    def test_Primary___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 3 required positional arguments: ",
                     "'name', 'sqlite3desc', and 'primaryname'")),
            _sqlitedu.Primary)


class TC__42(TCapswduapi):

    def test_Primary___init___02(self):
        self.assertIsInstance(
            _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P'),
            _sqlitedu.Primary)


class TC__43(TCapswduapi):

    def test_Primary_defer_put_01(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 2 required positional arguments: ",
                     "'segment' and 'record_number'")),
            p.defer_put)


class TC__44(TCapswduapi):

    def test_Primary_defer_put_02(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        p.existence_bit_maps[0] = SegmentSize.empty_bitarray.copy()
        p.defer_put(0, 3)


class TC__45(TCapswduapi):

    def test_Primary_defer_put_03(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        p._existence_bits._segment_dbservices = d._dbservices
        p.defer_put(0, 3)


class TC__46(TCapswduapi):

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
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        # Set up a database with one record.
        # (This goes through a Primary instance by the route in test_*_03).
        # (See test_*_05 for database setup without deferred updates)
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)
        d.do_final_segment_deferred_updates()
        d.close_context()
        # Add element of one record using the Primary instance to be tested.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        p._existence_bits._segment_dbservices = d._dbservices
        p.defer_put(0, 3)


class TC__47(TCapswduapi):

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
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        # Set up a database with one record.
        # (This does not use a dbduapi.Primary instance).
        # (See test_*_04 for database setup with deferred updates)
        d = apswapi.Sqlite3api(self.fsc(**self.database_specification),
                               self.folder)
        d.open_context()
        r = R()
        r.key._k = 0
        d.put_instance('File1', r)
        d.close_context()
        # Add element of one record using the Primary instance to be tested.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        p._existence_bits._segment_dbservices = d._dbservices
        p.defer_put(0, 3)


class TC__48(TCapswduapi):

    def test_Primary_write_existence_bit_map_01(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("write_existence_bit_map\(\) missing 1 required ",
                     "positional argument: 'segment'")),
            p.write_existence_bit_map)


class TC__49(TCapswduapi):

    def test_Primary_write_existence_bit_map_02(self):
        # Repeat test_Primary_defer_put_03 and do write_existence_bit_map call.
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        p._existence_bits._segment_dbservices = d._dbservices
        p.defer_put(0, 3)
        p.write_existence_bit_map(0)


class TC__50(TCapswduapi):

    def test_Primary_make_cursor_01(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("make_cursor\(\) missing 2 required positional ",
                     "arguments: 'dbobject' and 'keyrange'")),
            p.make_cursor)


class TC__51(TCapswduapi):

    def test_Primary_make_cursor_02(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'table_connection_list'",
            p.make_cursor,
            *(None, None))

    # Something is very wrong!!
    # Expecting the 'make_cursor not implemented' error that happens when
    # Sqlite3duapi gets involved.

class TC__52(TCapswduapi):

    def test_Primary_make_cursor_03(self):
        p = _sqlitedu.Primary('P',
                              self.fsc(**self.database_specification)['File1'],
                              'P')
        self.assertIsInstance((p.make_cursor(p, None)),
                              _sqlite.CursorPrimary)
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        self.assertRaisesRegex(
            _sqlitedu.Sqlite3duapiError,
            'make_cursor not implemented',
            d.make_cursor,
            *(p,))


class TC__53(TCapswduapi):

    def test_Secondary___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            ''.join(("__init__\(\) missing 3 required positional arguments: ",
                     "'name', 'sqlite3desc', and 'primaryname'")),
            _sqlitedu.Secondary)


class TC__54(TCapswduapi):

    def test_Secondary___init___02(self):
        self.assertIsInstance(
            _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P'),
            _sqlitedu.Secondary)


class TC__55(TCapswduapi):

    def test_Secondary_defer_put_01(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("defer_put\(\) missing 3 required positional arguments: ",
                     "'key', 'segment', and 'record_number'")),
            s.defer_put)


class TC__56(TCapswduapi):

    def test_Secondary_defer_put_02(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        #self.assertRaisesRegex(
        #    AttributeError,
        #    "'NoneType' object has no attribute 'encode'",
        #    s.defer_put,
        #    *(None, None, None))
        s.defer_put(None, None, None)

        # Need to devise a way of enforcing both None arguments to generate an
        # exception, or give some assurance the arguments are integers.
        # Perhaps a single namedtuple argument containing key, segment, and
        # record number.
        s.defer_put('odd', None, None)
        #for i in range(SegmentSize.db_upper_conversion_limit+1):
        #    s.defer_put('odd', None, i)


class TC__57(TCapswduapi):

    def test_Secondary_new_deferred_root_01(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        s._table_dbservices = d._dbservices
        s._table_link = []
        self.assertRaisesRegex(
            apswapi.apsw.SQLError,
            'SQLError: near "-": syntax error',
            s.new_deferred_root)


class TC__58(TCapswduapi):

    def test_Secondary_new_deferred_root_02(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'append'",
            s.new_deferred_root)


class TC__59(TCapswduapi):

    def test_Secondary_new_deferred_root_03(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        d.open_context()
        self.assertEqual(s._table_link, None)
        s.open_root(d._dbservices)
        self.assertEqual(s._table_link, ['P_S'])
        # First deferred DB.
        s.new_deferred_root()
        self.assertEqual(s._table_link, ['P_S', 't_0_S'])
        # Second deferred DB.
        s.new_deferred_root()
        self.assertEqual(s._table_link, ['P_S', 't_0_S', 't_1_S'])
        # Third deferred DB.
        s.new_deferred_root()
        self.assertEqual(s._table_link, ['P_S', 't_0_S', 't_1_S', 't_2_S'])


class TC__60(TCapswduapi):

    def test_Secondary_sort_and_write_01(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("sort_and_write\(\) missing 1 required positional ",
                     "argument: 'segment'")),
            s.sort_and_write)


class TC__61(TCapswduapi):

    def test_Secondary_sort_and_write_02(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'first_chunk'",
            s.sort_and_write,
            *(None,))


class TC__62(TCapswduapi):

    def test_Secondary_sort_and_write_03(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.sort_and_write(0), None)


class TC__63(TCapswduapi):

    def test_Secondary_sort_and_write_05(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        s.values = {(): 10}
        self.assertRaisesRegex(
            TypeError,
            "Bad binding argument type supplied - argument #1: type tuple",
            s.sort_and_write,
            *(0,))


class TC__64(TCapswduapi):

    def test_Secondary_sort_and_write_06(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.values = {'odd': 10}
        self.assertEqual(s.sort_and_write(0), None)


class TC__65(TCapswduapi):

    def test_Secondary_sort_and_write_07(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.values = {'odd': [10, 12]}
        self.assertEqual(s.sort_and_write(0), None)


class TC__66(TCapswduapi):

    def test_Secondary_sort_and_write_08(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {'odd': ba}
        self.assertEqual(s.sort_and_write(1), None)


class TC__67(TCapswduapi):

    def test_Secondary_sort_and_write_09(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        s.values = {'odd': 10}
        self.assertEqual(s.sort_and_write(0), None)


class TC__68(TCapswduapi):

    def test_Secondary_sort_and_write_10(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        s.values = {'odd': [10, 12]}
        self.assertEqual(s.sort_and_write(0), None)


class TC__69(TCapswduapi):

    def test_Secondary_sort_and_write_11(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(s.get_primary_database().first_chunk, None)
        s.get_primary_database().first_chunk = True
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {'odd': ba}
        self.assertEqual(s.sort_and_write(1), None)


class TC__70(TCapswduapi):

    def test_Secondary_merge_01(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("merge\(\) takes 1 positional argument but 2 were given")),
            s.merge,
            *(None,))


class TC__71(TCapswduapi):

    def test_Secondary_merge_02(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        s._table_link = []
        self.assertEqual(len(s.table_connection_list), 0)
        self.assertRaisesRegex(
            IndexError,
            "list index out of range",
            s.merge)


class TC__72(TCapswduapi):

    def test_Secondary_merge_03(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        s._table_link = [None]
        self.assertEqual(len(s.table_connection_list), 1)
        self.assertEqual(s.merge(), None)


class TC__73(TCapswduapi):

    def test_Secondary_merge_04(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        s._table_link = [None, None]
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertRaisesRegex(
            TypeError,
            "sequence item 9: expected str instance, NoneType found",
            s.merge)


class TC__74(TCapswduapi):

    def test_Secondary_merge_05(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        self.assertEqual(len(s.table_connection_list), 1)
        self.assertEqual(s.merge(), None)


class TC__75(TCapswduapi):

    # The tests in this class need the folder to not exist.
    # An apsw.ConstraintError is raised if stuff from earlier run of this test
    # is still present.
    def setUp(self):
        if sys.platform == 'win32':
            _delete_folder(self.name())
        super().setUp()

    def test_Secondary_merge_06(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        s.values = {'odd': 10}
        s.sort_and_write(0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(s.merge(), None)
        self.assertEqual(len(s.table_connection_list), 2)


class TC__76(TCapswduapi):

    # The tests in this class need the folder to not exist.
    # An apsw.ConstraintError is raised if stuff from earlier run of this test
    # is still present.
    def setUp(self):
        if sys.platform == 'win32':
            _delete_folder(self.name())
        super().setUp()

    def test_Secondary_merge_08(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        s.values = {'odd': [10, 12]}
        s.sort_and_write(0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(s.merge(), None)
        self.assertEqual(len(s.table_connection_list), 2)


class TC__77(TCapswduapi):

    # The tests in this class need the folder to not exist.
    # An apsw.ConstraintError is raised if stuff from earlier run of this test
    # is still present.
    def setUp(self):
        if sys.platform == 'win32':
            _delete_folder(self.name())
        super().setUp()

    def test_Secondary_merge_09(self):
        d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                   self.folder)
        s = d._dbdef['File1'].secondary['S']
        self.assertIsInstance(s, _sqlitedu.Secondary)
        d.open_context()
        s.get_primary_database().first_chunk = True
        ba = Bitarray(SegmentSize.db_segment_size)
        for i in range(SegmentSize.db_upper_conversion_limit + 1):
            ba[i] = True
        s.values = {b'odd': ba}
        s.sort_and_write(0)
        self.assertEqual(len(s.table_connection_list), 2)
        self.assertEqual(s.merge(), None)
        self.assertEqual(len(s.table_connection_list), 2)


class TC__78(TCapswduapi):

    def test_Secondary__rows_01(self):
        s = _sqlitedu.Secondary(
            'S',
            self.fsc(**self.database_specification)['File1'],
            'P')
        self.assertRaisesRegex(
            TypeError,
            ''.join(("_rows\(\) missing 2 required positional ",
                     "arguments: 'ssv' and 's'")),
            s._rows)
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not iterable",
            next,
            *(s._rows(None, None),))
        self.assertRaisesRegex(
            KeyError,
            "'a'",
            next,
            *(s._rows(('a',), None),))
        s.values['a'] = None
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not subscriptable",
            next,
            *(s._rows(('a',), None),))
        s.values['a'] = [1, 2]
        self.assertEqual(next(s._rows(('a',), None)), ('a', None, 1, 2))
        self.assertEqual(next(s._rows(('a',), 'x')), ('a', 'x', 1, 2))


if __name__ == '__main__':
    unittest.main()
