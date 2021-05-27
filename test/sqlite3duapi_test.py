# sqlite3duapi_test.py
# Copyright 2014 Roger Marsh
# License: See LICENSE.TXT (BSD licence)

"""sqlite3duapi tests"""

import unittest
import os
import shutil
import sqlite3
import subprocess

from .. import sqlite3duapi, sqlite3api
from ..api import filespec, record, recordset, database
from ..api.constants import (
    PRIMARY,
    SECONDARY,
    FIELDS,
    INV,
    ORD,
    RRN,
    FILEDESC,
    BTOD_FACTOR,
    BRECPPG,
    FILEORG,
    DB_SEGMENT_SIZE,
    )

GAMES_FILE_DEF = 'games'
GAME_FIELD_DEF = 'Game'
SOURCE_FIELD_DEF = 'source'
EVENT_FIELD_DEF = 'Event'
SITE_FIELD_DEF = 'Site'
DATE_FIELD_DEF = 'Date'
ROUND_FIELD_DEF = 'Round'
WHITE_FIELD_DEF = 'White'
BLACK_FIELD_DEF = 'Black'
RESULT_FIELD_DEF = 'Result'
POSITIONS_FIELD_DEF = 'positions'
PIECESQUAREMOVE_FIELD_DEF = 'piecesquaremove'
PARTIALPOSITION_FIELD_DEF = 'partialposition'
PGN_DATE_FIELD_DEF = 'pgndate'
_PIECESQUAREMOVE_FIELD_NAME = 'PieceSquareMove'
_PARTIALPOSITION_FIELD_NAME = 'PartialPosition'
_PGN_DATE_FIELD_NAME = 'PGNdate'


def _filespec():
    """Return a FileSpec for a trivial database"""
    class FileSpec(filespec.FileSpec):
        def __init__(self):
            dptdsn = FileSpec.dpt_dsn
            fn = FileSpec.field_name
            super().__init__(**{
                GAMES_FILE_DEF: {
                    PRIMARY: fn(GAME_FIELD_DEF),
                    SECONDARY : {
                        SOURCE_FIELD_DEF: SOURCE_FIELD_DEF.title(),
                        EVENT_FIELD_DEF: None,
                        SITE_FIELD_DEF: None,
                        DATE_FIELD_DEF: None,
                        ROUND_FIELD_DEF: None,
                        WHITE_FIELD_DEF: None,
                        BLACK_FIELD_DEF: None,
                        RESULT_FIELD_DEF: None,
                        POSITIONS_FIELD_DEF: POSITIONS_FIELD_DEF.title(),
                        PIECESQUAREMOVE_FIELD_DEF: _PIECESQUAREMOVE_FIELD_NAME,
                        PARTIALPOSITION_FIELD_DEF: _PARTIALPOSITION_FIELD_NAME,
                        PGN_DATE_FIELD_DEF: _PGN_DATE_FIELD_NAME,
                        },
                    FIELDS: {
                        fn(GAME_FIELD_DEF): None,
                        SOURCE_FIELD_DEF.title(): {INV:True, ORD:True},
                        WHITE_FIELD_DEF: {INV:True, ORD:True},
                        BLACK_FIELD_DEF: {INV:True, ORD:True},
                        EVENT_FIELD_DEF: {INV:True, ORD:True},
                        ROUND_FIELD_DEF: {INV:True, ORD:True},
                        DATE_FIELD_DEF: {INV:True, ORD:True},
                        RESULT_FIELD_DEF: {INV:True, ORD:True},
                        SITE_FIELD_DEF: {INV:True, ORD:True},
                        POSITIONS_FIELD_DEF.title(): {INV:True, ORD:True},
                        _PIECESQUAREMOVE_FIELD_NAME: {INV: True, ORD: True},
                        _PARTIALPOSITION_FIELD_NAME: {INV:True, ORD:True},
                        _PGN_DATE_FIELD_NAME: {INV:True, ORD:True},
                        },
                    FILEDESC: {
                        BRECPPG: 10,
                        FILEORG: RRN,
                        },
                    BTOD_FACTOR: 8,
                    },
                })
    return FileSpec()


def api(folder):
    return sqlite3duapi.Sqlite3bitduapi(
        _filespec(),
        os.path.expanduser(
            os.path.join('~', 'sqlite3duapi_tests', folder)))


def _delete_folder(folder):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'sqlite3duapi_tests', folder)),
        ignore_errors=True)


# Defined at module level for pickling.
class _Value(record.Value):
    def pack(self):
        v = super().pack()
        v[1]['Site'] = 'gash' # defined in _filespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in _filespec(), will be ignored
        return v


# Defined at module level for pickling.
class _ValueEdited(record.Value):
    def pack(self):
        v = super().pack()
        v[1]['Site'] = 'newgash' # defined in _filespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in _filespec(), will be ignored
        return v


class Sqlite3bitduapi_method_classes(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_method_classes_01(self):
        bit = sqlite3duapi.Sqlite3bitduapi
        api = sqlite3api._Sqlite3api
        self.assertIs(api.close_database, bit.close_database)
        self.assertIs(api.backout, bit.backout)
        self.assertIs(api.close_contexts, bit.close_contexts)
        self.assertIs(api.commit, bit.commit)
        self.assertIs(api.db_compatibility_hack, bit.db_compatibility_hack)
        self.assertIs(api.delete_instance, bit.delete_instance)
        self.assertIs(api.edit_instance, bit.edit_instance)
        self.assertIs(api.exists, bit.exists)
        self.assertIs(api.files_exist, bit.files_exist)
        self.assertIs(api.get_database_folder, bit.get_database_folder)
        self.assertIs(api.get_database, bit.get_database)
        self.assertIs(api.get_database_instance, bit.get_database_instance)
        self.assertIs(
            api.get_first_primary_key_for_index_key,
            bit.get_first_primary_key_for_index_key)
        self.assertIs(api.get_primary_record, bit.get_primary_record)
        self.assertIs(api.is_primary, bit.is_primary)
        self.assertIs(api.is_primary_recno, bit.is_primary_recno)
        self.assertIs(api.is_recno, bit.is_recno)
        self.assertIs(api.open_contexts, bit.open_contexts)
        self.assertIs(api.get_packed_key, bit.get_packed_key)
        self.assertIs(api.decode_as_primary_key, bit.decode_as_primary_key)
        self.assertIs(api.encode_primary_key, bit.encode_primary_key)
        self.assertIs(api.initial_database_size, bit.initial_database_size)
        self.assertIs(api.increase_database_size, bit.increase_database_size)
        self.assertIs(api.do_database_task, bit.do_database_task)
        self.assertIs(api.make_recordset_key, bit.make_recordset_key)
        self.assertIs(
            api.make_recordset_key_startswith,
            bit.make_recordset_key_startswith)
        self.assertIs(
            api.make_recordset_key_range,
            bit.make_recordset_key_range)
        self.assertIs(api.make_recordset_all, bit.make_recordset_all)
        self.assertIs(api.recordset_for_segment, bit.recordset_for_segment)
        self.assertIs(api.file_records_under, bit.file_records_under)

        self.assertIsNot(api.__init__, bit.__init__)
        self.assertIsNot(api.open_context, bit.open_context)
        self.assertIsNot(api.close_context, bit.close_context)

        self.assertIsNot(api.put_instance, bit.put_instance)
        self.assertIsNot(api.make_cursor, bit.make_cursor)
        self.assertIsNot(
            api.use_deferred_update_process,
            bit.use_deferred_update_process)

        self.assertEqual(hasattr(api, 'do_deferred_updates'), False)
        self.assertEqual(hasattr(bit, 'do_deferred_updates'), True)
        self.assertEqual(hasattr(api, 'do_segment_deferred_updates'), False)
        self.assertEqual(hasattr(bit, 'do_segment_deferred_updates'), True)
        self.assertEqual(hasattr(api, '_get_deferable_update_files'), False)
        self.assertEqual(hasattr(bit, '_get_deferable_update_files'), True)
        self.assertEqual(hasattr(api, 'set_defer_update'), False)
        self.assertEqual(hasattr(bit, 'set_defer_update'), True)
        self.assertEqual(hasattr(api, 'unset_defer_update'), False)
        self.assertEqual(hasattr(bit, 'unset_defer_update'), True)


class Sqlite3bitduapi_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'sqlite3tables'",
                )),
            sqlite3duapi.Sqlite3bitduapi,
            )
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Database file name None is not valid",
            sqlite3duapi.Sqlite3bitduapi,
            *(_filespec(),),
            **dict(sqlite3databasefolder=None)
            )
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Table definitions must be a dictionary",
            sqlite3duapi.Sqlite3bitduapi,
            *(None,),
            **dict(sqlite3databasefolder='')
            )

    def test___init___02(self):
        fs = dict(
            t=dict(
                primary='key',
                fields=dict(lock={}, key={}),
                filedesc=dict(fileorg=None),
                secondary=dict(key='lock'),
                ))
        api = sqlite3duapi.Sqlite3bitduapi(
            dict(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}),
                    filedesc=dict(fileorg=None),
                    secondary=dict(key='lock'),
                    )),
            sqlite3databasefolder='')
        p = os.path.abspath('')
        self.assertEqual(api._sqfile, os.path.join(p, os.path.basename(p)))
        self.assertEqual(api._sqconn, None)
        self.assertEqual(api._sqconn_cursor, None)
        self.assertEqual(api._associate, dict(t=dict(key='lock', t='key')))
        self.assertEqual(len(api._sqtables), 2)
        self.assertIsInstance(
            api._sqtables['key'], sqlite3api.Sqlite3bitPrimary)
        self.assertIsInstance(
            api._sqtables['lock'], sqlite3api.Sqlite3bitSecondary)
        self.assertEqual(api._sqlite3tables, fs)
        self.assertIsInstance(api._control, sqlite3api.Sqlite3bitControlFile)
        self.assertEqual(len(api.__dict__), 7)
        self.assertIsInstance(
            sqlite3duapi.Sqlite3bitduapi(_filespec(), sqlite3databasefolder=''),
            sqlite3duapi.Sqlite3bitduapi)
        self.assertIsInstance(
            sqlite3duapi.Sqlite3bitduapi(_filespec(), ''),
            sqlite3duapi.Sqlite3bitduapi)


class Sqlite3bitduapi_close_context(unittest.TestCase):

    def setUp(self):
        self.api = api('close_context')

    def tearDown(self):
        _delete_folder('close_context')

    def test_close_context_01(self):
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)
        self.assertEqual(self.api.close_context(), None)
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)

    def test_close_context_02(self):
        self.api.open_context()
        conn = self.api._sqconn
        cursor = self.api._sqconn_cursor
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertIsInstance(cursor, sqlite3.Cursor)
        self.assertEqual(self.api.close_context(), None)
        self.assertIs(self.api._sqconn, conn)
        self.assertIs(self.api._sqconn_cursor, cursor)
        self.assertEqual(self.api.close_context(), None)
        self.assertIs(self.api._sqconn, conn)
        self.assertIs(self.api._sqconn_cursor, cursor)


class Sqlite3bitduapi_open_context(unittest.TestCase):

    def setUp(self):
        self.api = api('open_context')

    def tearDown(self):
        _delete_folder('open_context')

    def test_open_context_01(self):
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)
        self.assertEqual(os.path.exists(self.api._sqfile), False)
        self.assertEqual(self.api.open_context(), True)
        conn = self.api._sqconn
        cursor = self.api._sqconn_cursor
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertIsInstance(cursor, sqlite3.Cursor)
        self.assertEqual(os.path.isfile(self.api._sqfile), True)
        self.assertEqual(self.api.open_context(), True)
        self.assertIs(self.api._sqconn, conn)
        self.assertIs(self.api._sqconn_cursor, cursor)
        self.assertEqual(os.path.isfile(self.api._sqfile), True)


class Sqlite3bitduapi_make_cursor(unittest.TestCase):

    def setUp(self):
        self.api = api('make_cursor')

    def tearDown(self):
        _delete_folder('make_cursor')

    def test_make_cursor_01(self):
        self.assertRaisesRegex(
            sqlite3duapi.Sqlite3duapiError,
            "make_cursor not implemented",
            self.api.make_cursor,
            *('Berkeley DB compatibility',))


class Sqlite3bitduapi_use_deferred_update_process(unittest.TestCase):

    def setUp(self):
        self.api = api('use_deferred_update_process')

    def tearDown(self):
        _delete_folder('use_deferred_update_process')

    def test_use_deferred_update_process_01(self):
        self.assertRaisesRegex(
            sqlite3duapi.Sqlite3duapiError,
            "Query use of du when in deferred update mode",
            self.api.use_deferred_update_process)


class Sqlite3bitduapi_set_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = api('set_defer_update')

    def tearDown(self):
        _delete_folder('set_defer_update')

    def test_set_defer_update_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.set_defer_update)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.set_defer_update,
            **dict(duallowed=True))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.set_defer_update,
            **dict(duallowed='xx'))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.set_defer_update,
            **dict(db='Berkeley DB compatibility'))

    def test_set_defer_update_02(self):
        self.api.open_context()
        self.assertEqual(self.api.set_defer_update(), None)
        self.assertEqual(self.api.set_defer_update(duallowed=True), None)
        self.assertEqual(self.api.set_defer_update(duallowed='xx'), None)
        self.assertEqual(
            self.api.set_defer_update(db='Berkeley DB compatibility'), None)


class Sqlite3bitduapi_unset_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = api('unset_defer_update')

    def tearDown(self):
        _delete_folder('unset_defer_update')

    def test_unset_defer_update_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.unset_defer_update,
            **dict(db='Berkeley DB compatibility'))

    def test_unset_defer_update_02(self):
        self.api.open_context()
        self.assertEqual(
            self.api.unset_defer_update(db='Berkeley DB compatibility'), None)


class Sqlite3bitduapi_put_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('put_instance')

    def tearDown(self):
        _delete_folder('put_instance')

    def test_put_instance_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'key'",
            self.api.put_instance,
            *(None, None))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 0
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.put_instance,
            *(None, instance))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 0
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.put_instance,
            *('games', instance))

    def test_put_instance_02(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 0
        self.api.open_context()
        self.assertEqual(self.api.put_instance('games', instance), None)

    def test_put_instance_03(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 4
        self.api.open_context()
        self.assertRaisesRegex(
            sqlite3duapi.Sqlite3duapiError,
            "Cannot reuse record number in deferred update",
            self.api.put_instance,
            *('games', instance))

    def test_put_instance_04(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 0
        self.api.open_context()
        self.assertEqual(self.api.put_instance('games', instance), None)


class Sqlite3bitduapi_do_deferred_updates(unittest.TestCase):

    def setUp(self):
        self.api = api('do_deferred_updates')

    def tearDown(self):
        _delete_folder('do_deferred_updates')

    def test_do_deferred_updates_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.do_deferred_updates)

    def test_do_deferred_updates_02(self):
        self.api.open_context()
        self.assertEqual(self.api.do_deferred_updates(), None)

    def test_do_deferred_updates_03(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 0
        self.api.open_context()
        self.api.put_instance('games', instance)
        self.assertEqual(self.api.do_deferred_updates(), None)


class Sqlite3bitduapi_do_segment_deferred_updates(unittest.TestCase):

    def setUp(self):
        self.api = api('do_segment_deferred_updates')

    def tearDown(self):
        _delete_folder('do_segment_deferred_updates')

    def test_do_segment_deferred_updates_01(self):
        self.api.open_context()
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 0
        self.api.put_instance('games', instance)

        # Replicate the call in put_instance, but when first record is added.
        db = self.api._associate['games']
        main = self.api._sqtables
        self.assertEqual(
            self.api.do_segment_deferred_updates(main, main[db['games']], 0),
            None)


class Sqlite3bitduapi__get_deferable_update_files(unittest.TestCase):

    def setUp(self):
        self.api = api('_get_deferable_update_files')

    def tearDown(self):
        _delete_folder('_get_deferable_update_files')

    def test__get_deferable_update_files_01(self):
        self.assertIsInstance(self.api._get_deferable_update_files(None), dict)
        self.assertEqual(self.api._get_deferable_update_files('xxxxx'), {})
        self.assertEqual(self.api._get_deferable_update_files(['xxxxx']), {})
        self.assertIsInstance(
            self.api._get_deferable_update_files('games'), dict)
        self.assertIsInstance(
            self.api._get_deferable_update_files(['games', 'Site']), dict)
        self.assertEqual(
            len(self.api._get_deferable_update_files(['games', 'Site'])), 1)
        self.assertEqual(
            len(self.api._get_deferable_update_files(['Event', 'Site'])), 0)
        self.assertEqual(
            len(self.api._get_deferable_update_files('games')), 1)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    # The Sqlite3bitduapi class tests.
    
    runner().run(loader(Sqlite3bitduapi_method_classes))
    runner().run(loader(Sqlite3bitduapi_init))
    runner().run(loader(Sqlite3bitduapi_close_context))
    runner().run(loader(Sqlite3bitduapi_open_context))
    runner().run(loader(Sqlite3bitduapi_make_cursor))
    runner().run(loader(Sqlite3bitduapi_use_deferred_update_process))
    runner().run(loader(Sqlite3bitduapi_set_defer_update))
    runner().run(loader(Sqlite3bitduapi_unset_defer_update))
    runner().run(loader(Sqlite3bitduapi_put_instance))
    runner().run(loader(Sqlite3bitduapi_do_deferred_updates))
    runner().run(loader(Sqlite3bitduapi_do_segment_deferred_updates))
    runner().run(loader(Sqlite3bitduapi__get_deferable_update_files))
