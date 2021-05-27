# stub_method_test.py
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
    from .. import _sqlite
except:
    print('_sqlite not imported: something is wrong.')
    _sqlite = False
try:
    from .. import _sqlitedu
except:
    print('_sqlitedu not imported: something is wrong.')
    _sqlitedu = False
try:
    from .. import apswapi
except:
    print('apswapi not imported: ok if apsw not installed.')
    apswapi= False
try:
    from .. import apswduapi
except:
    print('apswduapi not imported: ok if apsw not installed.')
    apswduapi = False
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
try:
    from .. import dptapi
except:
    if sys.platform == 'win32':
        print('dptapi not imported: ok if dptdb not installed.')
    dptapi = False
try:
    from .. import dptbase
except:
    if sys.platform == 'win32':
        print('dptbase not imported: ok if dptdb not installed.')
    dptbase = False
try:
    from .. import dptduapi
except:
    if sys.platform == 'win32':
        print('dptduapi not imported: ok if dptdb not installed.')
    dptduapi = False
try:
    from .. import dptdumultiapi
except:
    if sys.platform == 'win32':
        print('dptdumultiapi not imported: ok if dptdb not installed.')
    dptdumultiapi = False
try:
    from .. import sqlite3api
except:
    print('sqlite3api not imported: ok if sqlite3 not installed.')
    sqlite3api = False
try:
    from .. import sqlite3duapi
except:
    print('sqlite3duapi not imported: ok if sqlite3 not installed.')
    sqlite3duapi = False
if not (_sqlite or _sqlitedu or apswapi or apswduapi or dbapi or dbduapi or
        dptapi or dptbase or dptduapi or dptdumultiapi or
        sqlite3api or sqlite3duapi):
    print(''.join(('\n',
                   'Nothing was imported: ',
                   'does current working directory contain solentware_base?',
                   '\n')))


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


class _TC(unittest.TestCase):

    def name(self):
        return self.__class__.__name__

if _sqlite:


    class TC_sqlite(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if _sqlitedu:


    class TC_sqlitedu(_TC):

        def setUp(self):
            self.folder = _folder(self.name())
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

        def tearDown(self):
            _delete_folder(self.name())

if apswapi:


    class TCapswapi(_TC):

        def setUp(self):
            self.folder = _folder(self.name())

        def tearDown(self):
            _delete_folder(self.name())

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


if apswduapi:


    class TCapswduapi(_TC):

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


    class TC_apsw_01(TCapswduapi):

        def test_Sqlite3duapi___init___01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertIsInstance(d, apswduapi.Sqlite3duapi)


    class TC_apsw_02(TCapswduapi):

        def test_Sqlite3duapi___init___02(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'database_specification'")),
                apswduapi.Sqlite3duapi)


    class TC_apsw_03(TCapswduapi):

        def test_Sqlite3duapi___init___03(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'databasefolder'")),
                apswduapi.Sqlite3duapi,
                *({},))


    class TC_apsw_04(TCapswduapi):

        def test_Sqlite3duapi___init___04(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertIsInstance(d, apswduapi.Sqlite3duapi)


    class TC_apsw_05(TCapswduapi):

        def test_Sqlite3duapi_close_context_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.close_context(), None)


    class TC_apsw_06(TCapswduapi):

        def test_Sqlite3duapi_open_context_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.open_context(), True)


    class TC_apsw_07(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("_get_deferable_update_files\(\) missing 1 required ",
                         "positional argument: 'db'")),
                d._get_deferable_update_files)


    class TC_apsw_08(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_02(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("_get_deferable_update_files\(\) takes 2 positional ",
                         "arguments but 3 were given")),
                d._get_deferable_update_files,
                *(None, None))


    class TC_apsw_09(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_03(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d._get_deferable_update_files(None), False)


    class TC_apsw_10(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_04(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files(None),
                             {'File1': ['S']})


    class TC_apsw_11(TCapswduapi):

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


    class TC_apsw_12(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_06(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files('File1'),
                             {'File1': ['S']})


    class TC_apsw_13(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_07(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files('File2'), {})


    class TC_apsw_14(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_08(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files(['File1', 'File2']),
                             {'File1': ['S']})


    class TC_apsw_15(TCapswduapi):

        def test_Sqlite3duapi__get_deferable_update_files_09(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files({'File2', 'File3'}), {})


    class TC_apsw_16(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(), None)


    class TC_apsw_17(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_02(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(duallowed=False), None)


    class TC_apsw_18(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_03(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(duallowed=True), None)


    class TC_apsw_19(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_04(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(db='File1', duallowed=False), None)


    class TC_apsw_20(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_05(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(db='File1', duallowed=True), None)


    class TC_apsw_21(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_06(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'cursor'",
                d.set_defer_update,
                **dict(db='File1', duallowed=False))


    class TC_apsw_22(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_07(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'cursor'",
                d.set_defer_update,
                **dict(db='File1', duallowed=True))


    class TC_apsw_23(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_08(self):
            # Compare with test_set_defer_update_06.
            # See superclass tests for test of open_context() use here.
            # In this class' open_context() tests a simpler database specification
            # is good enough.
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertEqual(d.set_defer_update(db='File1', duallowed=False), False)


    class TC_apsw_24(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_09(self):
            # Compare with test_set_defer_update_07.
            # See superclass tests for test of open_context() use here.
            # In this class' open_context() tests a simpler database specification
            # is good enough.
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertEqual(d.set_defer_update(db='File1', duallowed=True), True)


    class TC_apsw_25(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_10(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.set_defer_update(db='File2', duallowed=False), None)


    class TC_apsw_26(TCapswduapi):

        def test_Sqlite3duapi_set_defer_update_11(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.set_defer_update(db='File2', duallowed=True), None)


    class TC_apsw_27(TCapswduapi):

        def test_Sqlite3duapi_unset_defer_update_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.unset_defer_update(), None)


    class TC_apsw_28(TCapswduapi):

        def test_Sqlite3duapi_unset_defer_update_02(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(), None)


    class TC_apsw_29(TCapswduapi):

        def test_Sqlite3duapi_unset_defer_update_03(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(db='File1'), None)


    class TC_apsw_30(TCapswduapi):

        def test_Sqlite3duapi_unset_defer_update_04(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(db=('File2', 'File3')), None)


    class TC_apsw_31(TCapswduapi):

        def test_Sqlite3duapi_do_final_segment_deferred_updates_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.do_final_segment_deferred_updates(), None)


    class TC_apsw_32(TCapswduapi):

        def test_Sqlite3duapi_do_final_segment_deferred_updates_02(self):
            d = apswduapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                TypeError,
                "sequence item 3: expected str instance, NoneType found",
                d.do_final_segment_deferred_updates,
                **dict(db='File1'))


    class TC_apsw_33(TCapswduapi):

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


    class TC_apsw_34(TCapswduapi):

        def test_Sqlite3duapi_put_instance_01(self):
            d = apswduapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("put_instance\(\) missing 2 required positional ",
                         "arguments: 'dbset' and 'instance'")),
                d.put_instance)


    class TC_apsw_35(TCapswduapi):

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


    class TC_apsw_36(TCapswduapi):

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


    class TC_apsw_37(TCapswduapi):

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


    class TC_apsw_38(TCapswduapi):

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


    class TC_apsw_39(TCapswduapi):

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


    class TC_apsw_40(TCapswduapi):

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


    class TC_apsw_41(TCapswduapi):

        def test_Primary___init___01(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 3 required positional arguments: ",
                         "'name', 'sqlite3desc', and 'primaryname'")),
                _sqlitedu.Primary)


    class TC_apsw_42(TCapswduapi):

        def test_Primary___init___02(self):
            self.assertIsInstance(
                _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P'),
                _sqlitedu.Primary)


    class TC_apsw_43(TCapswduapi):

        def test_Primary_defer_put_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("defer_put\(\) missing 2 required positional arguments: ",
                         "'segment' and 'record_number'")),
                p.defer_put)


    class TC_apsw_44(TCapswduapi):

        def test_Primary_defer_put_02(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            p.existence_bit_maps[0] = SegmentSize.empty_bitarray.copy()
            p.defer_put(0, 3)


    class TC_apsw_45(TCapswduapi):

        def test_Primary_defer_put_03(self):
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            p._existence_bits._segment_dbservices = d._dbservices
            p.defer_put(0, 3)


    class TC_apsw_46(TCapswduapi):

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


    class TC_apsw_47(TCapswduapi):

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


    class TC_apsw_48(TCapswduapi):

        def test_Primary_write_existence_bit_map_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("write_existence_bit_map\(\) missing 1 required ",
                         "positional argument: 'segment'")),
                p.write_existence_bit_map)


    class TC_apsw_49(TCapswduapi):

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


    class TC_apsw_50(TCapswduapi):

        def test_Primary_make_cursor_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("make_cursor\(\) missing 2 required positional ",
                         "arguments: 'dbobject' and 'keyrange'")),
                p.make_cursor)


    class TC_apsw_51(TCapswduapi):

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

    class TC_apsw_52(TCapswduapi):

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


    class TC_apsw_53(TCapswduapi):

        def test_Secondary___init___01(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 3 required positional arguments: ",
                         "'name', 'sqlite3desc', and 'primaryname'")),
                _sqlitedu.Secondary)


    class TC_apsw_54(TCapswduapi):

        def test_Secondary___init___02(self):
            self.assertIsInstance(
                _sqlitedu.Secondary(
                    'S',
                    self.fsc(**self.database_specification)['File1'],
                    'P'),
                _sqlitedu.Secondary)


    class TC_apsw_55(TCapswduapi):

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


    class TC_apsw_56(TCapswduapi):

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


    class TC_apsw_57(TCapswduapi):

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


    class TC_apsw_58(TCapswduapi):

        def test_Secondary_new_deferred_root_02(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'append'",
                s.new_deferred_root)


    class TC_apsw_59(TCapswduapi):

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


    class TC_apsw_60(TCapswduapi):

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


    class TC_apsw_61(TCapswduapi):

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


    class TC_apsw_62(TCapswduapi):

        def test_Secondary_sort_and_write_03(self):
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.sort_and_write(0), None)


    class TC_apsw_63(TCapswduapi):

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


    class TC_apsw_64(TCapswduapi):

        def test_Secondary_sort_and_write_06(self):
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.values = {'odd': 10}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_apsw_65(TCapswduapi):

        def test_Secondary_sort_and_write_07(self):
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.values = {'odd': [10, 12]}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_apsw_66(TCapswduapi):

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


    class TC_apsw_67(TCapswduapi):

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


    class TC_apsw_68(TCapswduapi):

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


    class TC_apsw_69(TCapswduapi):

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


    class TC_apsw_70(TCapswduapi):

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


    class TC_apsw_71(TCapswduapi):

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


    class TC_apsw_72(TCapswduapi):

        def test_Secondary_merge_03(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            s._table_link = [None]
            self.assertEqual(len(s.table_connection_list), 1)
            self.assertEqual(s.merge(), None)


    class TC_apsw_73(TCapswduapi):

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


    class TC_apsw_74(TCapswduapi):

        def test_Secondary_merge_05(self):
            d = apswduapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(len(s.table_connection_list), 1)
            self.assertEqual(s.merge(), None)


    class TC_apsw_75(TCapswduapi):

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


    class TC_apsw_76(TCapswduapi):

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


    class TC_apsw_77(TCapswduapi):

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


    class TC_apsw_78(TCapswduapi):

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

if dbapi:


    class TCdbapi(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if dbduapi:


    class TCdbduapi(_TC):

        def setUp(self):
            self.folder = _folder(self.name())
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
            _delete_folder(self.name())

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

if dptapi:


    class TCdptapi(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if dptbase:


    class TCdptbase(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if dptduapi:


    class TCdptduapi(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if dptdumultiapi:


    class TCdptdumultiapi(_TC):

        def setUp(self):
            pass

        def tearDown(self):
            pass

if sqlite3api:


    class TCsqlite3api(_TC):

        def setUp(self):
            self.folder = _folder(self.name())

        def tearDown(self):
            _delete_folder(self.name())

        def test_Database_make_connection_01(self):
            class C:
                def __init__(self, *a, **b):
                    pass
            d = sqlite3api.Database(C, C, {}, self.folder)
            self.assertEqual(d._dbservices, None)
            d.make_connection()
            self.assertIsInstance(d._dbservices, sqlite3api.sqlite3.Connection)
            dbs = d._dbservices
            d.make_connection()
            self.assertIs(dbs, d._dbservices)

        def test_Sqlite3api___init___01(self):
            d = sqlite3api.Sqlite3api({}, self.folder)
            self.assertIsInstance(d, sqlite3api.Sqlite3api)

        def test_Sqlite3api___init___02(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'database_specification'")),
                sqlite3api.Sqlite3api)

        def test_Sqlite3api___init___03(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'databasefolder'")),
                sqlite3api.Sqlite3api,
                *({},))

if sqlite3duapi:


    class TCsqlite3duapi(_TC):

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


    class TC_sqlite3_01(TCsqlite3duapi):

        def test_Sqlite3duapi___init___01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertIsInstance(d, sqlite3duapi.Sqlite3duapi)


    class TC_sqlite3_02(TCsqlite3duapi):

        def test_Sqlite3duapi___init___02(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'database_specification'")),
                sqlite3duapi.Sqlite3duapi)


    class TC_sqlite3_03(TCsqlite3duapi):

        def test_Sqlite3duapi___init___03(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 1 required positional argument: ",
                         "'databasefolder'")),
                sqlite3duapi.Sqlite3duapi,
                *({},))


    class TC_sqlite3_04(TCsqlite3duapi):

        def test_Sqlite3duapi___init___04(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertIsInstance(d, sqlite3duapi.Sqlite3duapi)


    class TC_sqlite3_05(TCsqlite3duapi):

        def test_Sqlite3duapi_close_context_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.close_context(), None)


    class TC_sqlite3_06(TCsqlite3duapi):

        def test_Sqlite3duapi_open_context_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.open_context(), True)


    class TC_sqlite3_07(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("_get_deferable_update_files\(\) missing 1 required ",
                         "positional argument: 'db'")),
                d._get_deferable_update_files)


    class TC_sqlite3_08(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_02(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("_get_deferable_update_files\(\) takes 2 positional ",
                         "arguments but 3 were given")),
                d._get_deferable_update_files,
                *(None, None))


    class TC_sqlite3_09(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_03(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d._get_deferable_update_files(None), False)


    class TC_sqlite3_10(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_04(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files(None),
                             {'File1': ['S']})


    class TC_sqlite3_11(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files(None), False)


    class TC_sqlite3_12(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_06(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files('File1'),
                             {'File1': ['S']})


    class TC_sqlite3_13(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_07(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files('File2'), {})


    class TC_sqlite3_14(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_08(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files(['File1', 'File2']),
                             {'File1': ['S']})


    class TC_sqlite3_15(TCsqlite3duapi):

        def test_Sqlite3duapi__get_deferable_update_files_09(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d._get_deferable_update_files({'File2', 'File3'}), {})


    class TC_sqlite3_16(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(), None)


    class TC_sqlite3_17(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_02(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(duallowed=False), None)


    class TC_sqlite3_18(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_03(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(duallowed=True), None)


    class TC_sqlite3_19(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_04(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(db='File1', duallowed=False), None)


    class TC_sqlite3_20(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_05(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.set_defer_update(db='File1', duallowed=True), None)


    class TC_sqlite3_21(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_06(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'cursor'",
                d.set_defer_update,
                **dict(db='File1', duallowed=False))


    class TC_sqlite3_22(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_07(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'cursor'",
                d.set_defer_update,
                **dict(db='File1', duallowed=True))


    class TC_sqlite3_23(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_08(self):
            # Compare with test_set_defer_update_06.
            # See superclass tests for test of open_context() use here.
            # In this class' open_context() tests a simpler database specification
            # is good enough.
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertEqual(d.set_defer_update(db='File1', duallowed=False), False)


    class TC_sqlite3_24(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_09(self):
            # Compare with test_set_defer_update_07.
            # See superclass tests for test of open_context() use here.
            # In this class' open_context() tests a simpler database specification
            # is good enough.
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertEqual(d.set_defer_update(db='File1', duallowed=True), True)


    class TC_sqlite3_25(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_10(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.set_defer_update(db='File2', duallowed=False), None)


    class TC_sqlite3_26(TCsqlite3duapi):

        def test_Sqlite3duapi_set_defer_update_11(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.set_defer_update(db='File2', duallowed=True), None)


    class TC_sqlite3_27(TCsqlite3duapi):

        def test_Sqlite3duapi_unset_defer_update_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.unset_defer_update(), None)


    class TC_sqlite3_28(TCsqlite3duapi):

        def test_Sqlite3duapi_unset_defer_update_02(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(), None)


    class TC_sqlite3_29(TCsqlite3duapi):

        def test_Sqlite3duapi_unset_defer_update_03(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(db='File1'), None)


    class TC_sqlite3_30(TCsqlite3duapi):

        def test_Sqlite3duapi_unset_defer_update_04(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertEqual(d.unset_defer_update(db=('File2', 'File3')), None)


    class TC_sqlite3_31(TCsqlite3duapi):

        def test_Sqlite3duapi_do_final_segment_deferred_updates_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertEqual(d.do_final_segment_deferred_updates(), None)


    class TC_sqlite3_32(TCsqlite3duapi):

        def test_Sqlite3duapi_do_final_segment_deferred_updates_02(self):
            d = sqlite3duapi.Sqlite3duapi(
                self.fsc(**self.database_specification), self.folder)
            self.assertRaisesRegex(
                TypeError,
                "sequence item 3: expected str instance, NoneType found",
                d.do_final_segment_deferred_updates,
                **dict(db='File1'))


    class TC_sqlite3_33(TCsqlite3duapi):

        def test_Sqlite3duapi_do_final_segment_deferred_updates_03(self):
            # Compare with test_do_final_segment_deferred_updates_02.
            # See superclass tests for test of open_context() use here.
            # In this class' open_context() tests a simpler database specification
            # is good enough.
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertEqual(d.do_final_segment_deferred_updates(db='File1'), None)

        # Further tests of DBduapi.do_final_segment_deferred_updates() will assume
        # a working DBduapi.put_instance() method to provide deferred updates.


    class TC_sqlite3_34(TCsqlite3duapi):

        def test_Sqlite3duapi_put_instance_01(self):
            d = sqlite3duapi.Sqlite3duapi({}, self.folder)
            self.assertRaisesRegex(
                TypeError,
                ''.join(("put_instance\(\) missing 2 required positional ",
                         "arguments: 'dbset' and 'instance'")),
                d.put_instance)


    class TC_sqlite3_35(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            r = R()
            self.assertRaisesRegex(
                DatabaseduError,
                "Cannot reuse record number in deferred update.",
                d.put_instance,
                *('File1', r))


    class TC_sqlite3_36(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            r = R()
            r.key._k = 0
            d.put_instance('File1', r)


    class TC_sqlite3_37(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            r = R()
            r.key._k = 0
            d.put_instance('File1', r)


    class TC_sqlite3_38(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            r = R()
            r.key._k = 0
            d.put_instance('File1', r)


    class TC_sqlite3_39(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_40(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_41(TCsqlite3duapi):

        def test_Primary___init___01(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 3 required positional arguments: ",
                         "'name', 'sqlite3desc', and 'primaryname'")),
                _sqlitedu.Primary)


    class TC_sqlite3_42(TCsqlite3duapi):

        def test_Primary___init___02(self):
            self.assertIsInstance(
                _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P'),
                _sqlitedu.Primary)


    class TC_sqlite3_43(TCsqlite3duapi):

        def test_Primary_defer_put_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("defer_put\(\) missing 2 required positional arguments: ",
                         "'segment' and 'record_number'")),
                p.defer_put)


    class TC_sqlite3_44(TCsqlite3duapi):

        def test_Primary_defer_put_02(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            p.existence_bit_maps[0] = SegmentSize.empty_bitarray.copy()
            p.defer_put(0, 3)


    class TC_sqlite3_45(TCsqlite3duapi):

        def test_Primary_defer_put_03(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            p._existence_bits._segment_dbservices = d._dbservices
            p.defer_put(0, 3)


    class TC_sqlite3_46(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            r = R()
            r.key._k = 0
            d.put_instance('File1', r)
            d.do_final_segment_deferred_updates()
            d.close_context()
            # Add element of one record using the Primary instance to be tested.
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            p._existence_bits._segment_dbservices = d._dbservices
            p.defer_put(0, 3)


    class TC_sqlite3_47(TCsqlite3duapi):

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
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            p._existence_bits._segment_dbservices = d._dbservices
            p.defer_put(0, 3)


    class TC_sqlite3_48(TCsqlite3duapi):

        def test_Primary_write_existence_bit_map_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("write_existence_bit_map\(\) missing 1 required ",
                         "positional argument: 'segment'")),
                p.write_existence_bit_map)


    class TC_sqlite3_49(TCsqlite3duapi):

        def test_Primary_write_existence_bit_map_02(self):
            # Repeat test_Primary_defer_put_03 and do write_existence_bit_map call.
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            p._existence_bits._segment_dbservices = d._dbservices
            p.defer_put(0, 3)
            p.write_existence_bit_map(0)


    class TC_sqlite3_50(TCsqlite3duapi):

        def test_Primary_make_cursor_01(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertRaisesRegex(
                TypeError,
                ''.join(("make_cursor\(\) missing 2 required positional ",
                         "arguments: 'dbobject' and 'keyrange'")),
                p.make_cursor)


    class TC_sqlite3_51(TCsqlite3duapi):

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

    class TC_sqlite3_52(TCsqlite3duapi):

        def test_Primary_make_cursor_03(self):
            p = _sqlitedu.Primary('P',
                                  self.fsc(**self.database_specification)['File1'],
                                  'P')
            self.assertIsInstance((p.make_cursor(p, None)),
                                  _sqlite.CursorPrimary)
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            self.assertRaisesRegex(
                _sqlitedu.Sqlite3duapiError,
                'make_cursor not implemented',
                d.make_cursor,
                *(p,))


    class TC_sqlite3_53(TCsqlite3duapi):

        def test_Secondary___init___01(self):
            self.assertRaisesRegex(
                TypeError,
                ''.join(("__init__\(\) missing 3 required positional arguments: ",
                         "'name', 'sqlite3desc', and 'primaryname'")),
                _sqlitedu.Secondary)


    class TC_sqlite3_54(TCsqlite3duapi):

        def test_Secondary___init___02(self):
            self.assertIsInstance(
                _sqlitedu.Secondary(
                    'S',
                    self.fsc(**self.database_specification)['File1'],
                    'P'),
                _sqlitedu.Secondary)


    class TC_sqlite3_55(TCsqlite3duapi):

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


    class TC_sqlite3_56(TCsqlite3duapi):

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


    class TC_sqlite3_57(TCsqlite3duapi):

        def test_Secondary_new_deferred_root_01(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            d.open_context()
            s._table_dbservices = d._dbservices
            s._table_link = []
            self.assertRaisesRegex(
                sqlite3api.sqlite3.OperationalError,
                'near "-": syntax error',
                s.new_deferred_root)


    class TC_sqlite3_58(TCsqlite3duapi):

        def test_Secondary_new_deferred_root_02(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            self.assertRaisesRegex(
                AttributeError,
                "'NoneType' object has no attribute 'append'",
                s.new_deferred_root)


    class TC_sqlite3_59(TCsqlite3duapi):

        def test_Secondary_new_deferred_root_03(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_60(TCsqlite3duapi):

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


    class TC_sqlite3_61(TCsqlite3duapi):

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


    class TC_sqlite3_62(TCsqlite3duapi):

        def test_Secondary_sort_and_write_03(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.sort_and_write(0), None)


    class TC_sqlite3_63(TCsqlite3duapi):

        def test_Secondary_sort_and_write_05(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            s.values = {(): 10}
            self.assertRaisesRegex(
                sqlite3api.sqlite3.InterfaceError,
                "Error binding parameter 0 - probably unsupported type",
                s.sort_and_write,
                *(0,))


    class TC_sqlite3_64(TCsqlite3duapi):

        def test_Secondary_sort_and_write_06(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.values = {'odd': 10}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_sqlite3_65(TCsqlite3duapi):

        def test_Secondary_sort_and_write_07(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.values = {'odd': [10, 12]}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_sqlite3_66(TCsqlite3duapi):

        def test_Secondary_sort_and_write_08(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_67(TCsqlite3duapi):

        def test_Secondary_sort_and_write_09(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.get_primary_database().first_chunk = True
            s.values = {'odd': 10}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_sqlite3_68(TCsqlite3duapi):

        def test_Secondary_sort_and_write_10(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(s.get_primary_database().first_chunk, None)
            s.get_primary_database().first_chunk = True
            s.values = {'odd': [10, 12]}
            self.assertEqual(s.sort_and_write(0), None)


    class TC_sqlite3_69(TCsqlite3duapi):

        def test_Secondary_sort_and_write_11(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_70(TCsqlite3duapi):

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


    class TC_sqlite3_71(TCsqlite3duapi):

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


    class TC_sqlite3_72(TCsqlite3duapi):

        def test_Secondary_merge_03(self):
            s = _sqlitedu.Secondary(
                'S',
                self.fsc(**self.database_specification)['File1'],
                'P')
            s._table_link = [None]
            self.assertEqual(len(s.table_connection_list), 1)
            self.assertEqual(s.merge(), None)


    class TC_sqlite3_73(TCsqlite3duapi):

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


    class TC_sqlite3_74(TCsqlite3duapi):

        def test_Secondary_merge_05(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
                                       self.folder)
            s = d._dbdef['File1'].secondary['S']
            self.assertIsInstance(s, _sqlitedu.Secondary)
            d.open_context()
            self.assertEqual(len(s.table_connection_list), 1)
            self.assertEqual(s.merge(), None)


    class TC_sqlite3_75(TCsqlite3duapi):

        def test_Secondary_merge_06(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_76(TCsqlite3duapi):

        def test_Secondary_merge_08(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_77(TCsqlite3duapi):

        def test_Secondary_merge_09(self):
            d = sqlite3duapi.Sqlite3duapi(self.fsc(**self.database_specification),
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


    class TC_sqlite3_78(TCsqlite3duapi):

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
