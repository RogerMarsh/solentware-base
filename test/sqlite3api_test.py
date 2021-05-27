# sqlite3api_test.py
# Copyright 2014 Roger Marsh
# License: See LICENSE.TXT (BSD licence)

"""sqlite3api tests"""

import unittest
import os
import shutil
import sqlite3
import subprocess

from .. import sqlite3api
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
                    },
                })
    return FileSpec()


def _api(folder):
    return sqlite3api._Sqlite3api(
        sqlite3api.Sqlite3bitPrimary,
        sqlite3api.Sqlite3bitSecondary,
        _filespec(),
        os.path.expanduser(
            os.path.join('~', 'sqlite3api_tests', folder)))


def api(folder):
    return sqlite3api.Sqlite3bitapi(
        _filespec(),
        os.path.expanduser(
            os.path.join('~', 'sqlite3api_tests', folder)))


def _delete_folder(folder):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'sqlite3api_tests', folder)),
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


class Sqlite3DatabaseEncoders(unittest.TestCase):

    def setUp(self):
        self.sde = sqlite3api.Sqlite3DatabaseEncoders()

    def tearDown(self):
        pass

    def test_encode_record_number_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'bytes' object has no attribute 'to_bytes'",
            self.sde.encode_record_number,
            b'dd',)
        self.assertEqual(self.sde.encode_record_number(57), b'AAAAOQ==')

    def test_decode_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "argument should be bytes or ASCII string, not int",
            self.sde.decode_record_number,
            57)
        self.assertEqual(self.sde.decode_record_number(b'AAAAOQ=='), 57)


class _Sqlite3api_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        p = os.path.abspath('')
        self.assertEqual(
            sqlite3api._Sqlite3api(None, None, {}, '').__dict__,
            dict(_sqtables={},
                 _sqfile=os.path.join(p, os.path.basename(p)),
                 _sqconn=None,
                 _sqconn_cursor=None,
                 _associate={},
                 _sqlite3tables={}))
        self.assertIs(
            sqlite3api._Sqlite3api(None, None, {}, '').engine_uses_bytes_or_str,
            sqlite3api._Sqlite3api.engine_uses_bytes_or_str)
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 4 required positional arguments: ",
                "'primary_class', 'secondary_class', 'sqlite3tables', and ",
                "'sqlite3databasefolder'",
                )),
            sqlite3api._Sqlite3api,
            )
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Database file name None is not valid",
            sqlite3api._Sqlite3api,
            *(None, None, None, None, None))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Table definitions must be a dictionary",
            sqlite3api._Sqlite3api,
            *(None, None, None, '', None))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Table definition for 't' must be a dictionary",
            sqlite3api._Sqlite3api,
            *(None, None, dict(t=None), '', None))
        self.assertRaisesRegex(
            KeyError,
            "'primary'",
            sqlite3api._Sqlite3api,
            *(None, None, dict(t={}), '', None))
        self.assertRaisesRegex(
            KeyError,
            "'fields'",
            sqlite3api._Sqlite3api,
            *(None,
              None,
              dict(t=dict(primary='key')),
              ''))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Primary column name key for t does not have a column description",
            sqlite3api._Sqlite3api,
            *(None,
              None,
              dict(t=dict(primary='key', fields=('lock',))),
              ''))
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not callable",
            sqlite3api._Sqlite3api,
            *(None,
              None,
              dict(t=dict(primary='key', fields=('lock', 'key'))),
              ''))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "".join((
                "Attributes for column 'key' in table 'key' must be a 'dict' ",
                "or 'None'")),
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key=True),
                      filedesc=dict(fileorg=None))),
              ''))
        self.assertIsInstance(
            sqlite3api._Sqlite3api(
                sqlite3api.Sqlite3bitPrimary,
                None,
                dict(
                    t=dict(
                        primary='key',
                        fields=dict(lock={}, key={}),
                        filedesc=dict(fileorg=None))),
                ''),
            sqlite3api._Sqlite3api)
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'items'",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=None)),
              ''))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Secondary table name 1 for 't' must be a string",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary={1:2})),
              ''))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Secondary table name key for t already used",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(key='key'),
                      )),
              ''))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Secondary table name key for t already used",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(lock='key'),
                      )),
              ''))

        # Now must have FileSpec which brings in some DPT related stuff.
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Secondary table name Key for t does not have a column description",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None, brecppg=100),
                      secondary=dict(key=None),
                      btod_factor=5,
                      )),
              ''))
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not callable",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}, Key={}),
                      filedesc=dict(fileorg=None, brecppg=100),
                      secondary=dict(key=None),
                      btod_factor=5,
                      )),
              ''))

        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not callable",
            sqlite3api._Sqlite3api,
            *(sqlite3api.Sqlite3bitPrimary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(key='lock'),
                      )),
              ''))

    def test___init___02(self):
        fs = dict(
            t=dict(
                primary='key',
                fields=dict(lock={}, key={}),
                filedesc=dict(fileorg=None),
                secondary=dict(key='lock'),
                ))
        api = sqlite3api._Sqlite3api(
            sqlite3api.Sqlite3bitPrimary,
            sqlite3api.Sqlite3bitSecondary,
            dict(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}),
                    filedesc=dict(fileorg=None),
                    secondary=dict(key='lock'),
                    )),
            '')
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
        self.assertEqual(len(api.__dict__), 6)


class _Sqlite3api_open_context(unittest.TestCase):

    def setUp(self):
        self.api = _api('open_context')

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


class _Sqlite3api_close_context(unittest.TestCase):

    def setUp(self):
        self.api = _api('close_context')

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


class _Sqlite3api_close_database(unittest.TestCase):

    def setUp(self):
        self.api = _api('close_database')

    def tearDown(self):
        _delete_folder('close_database')

    def test_close_database_01(self):
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)

    def test_close_database_02(self):
        self.api.open_context()
        self.assertIsInstance(self.api._sqconn, sqlite3.Connection)
        self.assertIsInstance(self.api._sqconn_cursor, sqlite3.Cursor)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api._sqconn_cursor, None)


class _Sqlite3api_backout(unittest.TestCase):
    # Nothing to backout, just check the calls work.

    def setUp(self):
        self.api = _api('backout')

    def tearDown(self):
        _delete_folder('backout')

    def test_backout_01(self):
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api.backout(), None)
        self.api.open_context()
        self.assertIsInstance(self.api._sqconn, sqlite3.Connection)
        self.assertEqual(self.api.backout(), None)
        self.assertIsInstance(self.api._sqconn, sqlite3.Connection)


class _Sqlite3api_close_contexts(unittest.TestCase):

    def setUp(self):
        self.api = _api('close_contexts')

    def tearDown(self):
        _delete_folder('close_contexts')

    def test_close_contexts_01(self):
        self.assertEqual(self.api.close_contexts('DPT compatibility'), None)


class _Sqlite3api_commit(unittest.TestCase):
    # Nothing to commit, just check the calls work.

    def setUp(self):
        self.api = _api('commit')

    def tearDown(self):
        _delete_folder('commit')

    def test_backout_01(self):
        self.assertEqual(self.api._sqconn, None)
        self.assertEqual(self.api.commit(), None)
        self.api.open_context()
        self.assertIsInstance(self.api._sqconn, sqlite3.Connection)
        self.assertEqual(self.api.commit(), None)
        self.assertIsInstance(self.api._sqconn, sqlite3.Connection)


class _Sqlite3api_db_compatibility_hack(unittest.TestCase):

    def setUp(self):
        self.api = _api('db_compatibility_hack')

    def tearDown(self):
        _delete_folder('db_compatibility_hack')

    def test_db_compatibility_hack_01(self):
        record = ('key', 'value')
        srkey = self.api.encode_record_number(3456)
        self.assertEqual(
            self.api.db_compatibility_hack(record, srkey), record)
        record = ('key', None)
        value = self.api.decode_record_number(srkey)
        self.assertEqual(
            self.api.db_compatibility_hack(record, srkey),
            ('key', value))


class _Sqlite3api_exists(unittest.TestCase):

    def setUp(self):
        self.api = _api('exists')

    def tearDown(self):
        _delete_folder('exists')

    def test_exists_01(self):
        self.assertEqual(self.api.exists(None, None), False)
        self.assertEqual(self.api.exists('games', None), False)
        self.assertEqual(self.api.exists('games', 'Site'), True)


class _Sqlite3api_files_exist(unittest.TestCase):

    def setUp(self):
        self.api = _api('files_exist')

    def tearDown(self):
        _delete_folder('files_exist')

    def test_exists_01(self):
        self.assertEqual(self.api.files_exist(), False)
        self.api.open_context()
        self.assertEqual(self.api.files_exist(), True)
        self.api.close_database()
        self.assertEqual(self.api.files_exist(), True)


class _Sqlite3api_make_cursor(unittest.TestCase):

    def setUp(self):
        self.api = _api('database_cursor')

    def tearDown(self):
        _delete_folder('database_cursor')

    def test_make_cursor_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.database_cursor,
            *(None, None))
        self.assertIsInstance(
            self.api.database_cursor('games', 'Site'),
            sqlite3api.CursorSqlite3bitSecondary)
        self.assertIsInstance(
            self.api.database_cursor('games', 'games'),
            sqlite3api.CursorSqlite3bitPrimary)


class _Sqlite3api_get_database_folder(unittest.TestCase):

    def setUp(self):
        self.api = _api('get_database_folder')

    def tearDown(self):
        _delete_folder('get_database_folder')

    def test_get_database_folder_01(self):
        self.assertEqual(
            self.api.get_database_folder(),
            os.path.expanduser(
                os.path.join('~', 'sqlite3api_tests', 'get_database_folder')))


class _Sqlite3api_get_database(unittest.TestCase):

    def setUp(self):
        self.api = _api('get_database')

    def tearDown(self):
        _delete_folder('get_database')

    def test_get_database_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.get_database,
            *(None, None))
        self.assertEqual(self.api.get_database('games', 'Site'), None)
        self.assertEqual(self.api.get_database('games', 'games'), None)
        self.api.open_context()
        self.assertIsInstance(
            self.api.get_database('games', 'Site'),
            sqlite3.Cursor)
        self.assertIsInstance(
            self.api.get_database('games', 'games'),
            sqlite3.Cursor)


class _Sqlite3api_get_database_instance(unittest.TestCase):

    def setUp(self):
        self.api = _api('get_database_instance')

    def tearDown(self):
        _delete_folder('get_database_instance')

    def test_get_database_instance_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.get_database_instance,
            *(None, None))
        self.assertIsInstance(
            self.api.get_database_instance('games', 'Site'),
            sqlite3api.Sqlite3bitSecondary)
        self.assertIsInstance(
            self.api.get_database_instance('games', 'games'),
            sqlite3api.Sqlite3bitPrimary)


class _Sqlite3api_get_first_primary_key_for_index_key(unittest.TestCase):
    # Test results are suspicious but may be correct.

    def setUp(self):
        self.api = _api('get_first_primary_key_for_index_key')

    def tearDown(self):
        _delete_folder('get_first_primary_key_for_index_key')

    def test_get_first_primary_key_for_index_key_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.get_first_primary_key_for_index_key,
            *(None, None, None))
        self.assertRaisesRegex(
            AttributeError,
            ''.join((
                "'Sqlite3bitSecondary' object has no attribute ",
                "'get_first_primary_key_for_index_key'")),
            self.api.get_first_primary_key_for_index_key,
            *('games', 'Site', 'k'))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "get_first_primary_key_for_index_key for primary table",
            self.api.get_first_primary_key_for_index_key,
            *('games', 'games', 'k'))
        self.api.open_context()
        #self.assertEqual(
        #    self.api.get_first_primary_key_for_index_key('games', 'games', 'k'),
        #    None)
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "get_first_primary_key_for_index_key for primary table",
            self.api.get_first_primary_key_for_index_key,
            *('games', 'games', 'k'))


class _Sqlite3api_get_primary_record(unittest.TestCase):

    def setUp(self):
        self.api = _api('get_first_primary_key_for_index_key')

    def tearDown(self):
        _delete_folder('get_first_primary_key_for_index_key')

    def test_get_primary_record_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.get_primary_record,
            *(None, None))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.get_primary_record,
            *('games', 'k'))
        self.api.open_context()
        self.assertEqual(self.api.get_primary_record('games', 'k'), None)


class _Sqlite3api_is_primary(unittest.TestCase):

    def setUp(self):
        self.api = _api('is_primary')

    def tearDown(self):
        _delete_folder('is_primary')

    def test_is_primary_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.is_primary,
            *(None, None))
        self.assertEqual(self.api.is_primary('games', 'games'), True)
        self.assertEqual(self.api.is_primary('games', 'Site'), False)


class _Sqlite3api_is_primary_recno(unittest.TestCase):

    def setUp(self):
        self.api = _api('is_primary_recno')

    def tearDown(self):
        _delete_folder('is_primary_recno')

    def test_is_primary_recno_01(self):
        self.assertEqual(
            self.api.is_primary_recno('Berkeley DB compatibility'), True)


class _Sqlite3api_is_recno(unittest.TestCase):

    def setUp(self):
        self.api = _api('is_recno')

    def tearDown(self):
        _delete_folder('is_recno')

    def test_is_recno_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.is_recno,
            *(None, None))
        self.assertEqual(self.api.is_recno('games', 'games'), True)
        self.assertEqual(self.api.is_recno('games', 'Site'), False)


class _Sqlite3api_open_contexts(unittest.TestCase):

    def setUp(self):
        self.api = _api('open_contexts')

    def tearDown(self):
        _delete_folder('open_contexts')

    def test_open_contexts_01(self):
        self.assertEqual(self.api.open_contexts('DPT compatibility'), None)


class _Sqlite3api_get_packed_key(unittest.TestCase):

    def setUp(self):
        self.api = _api('get_packed_key')

    def tearDown(self):
        _delete_folder('get_packed_key')

    def test_get_packed_key_01(self):
        instance = record.Record(record.Key, record.Value)
        self.assertEqual(
            self.api.get_packed_key('Berkeley DB compatibility', instance),
            instance.key.pack())


class _Sqlite3api_decode_as_primary_key(unittest.TestCase):

    def setUp(self):
        self.api = _api('decode_as_primary_key')

    def tearDown(self):
        _delete_folder('decode_as_primary_key')

    def test_decode_as_primary_key_01(self):
        self.assertEqual(self.api.decode_as_primary_key(
            'Ignored in sqlite3', 10), 10)
        srkey = self.api.encode_record_number(3456)
        self.assertEqual(self.api.decode_as_primary_key(
            'Ignored in sqlite3', srkey), 3456)


class _Sqlite3api_encode_primary_key(unittest.TestCase):

    def setUp(self):
        self.api = _api('encode_primary_key')

    def tearDown(self):
        _delete_folder('encode_primary_key')

    def test_encode_primary_key_01(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.load(23)
        self.assertEqual(
            self.api.encode_primary_key('games', instance),
            b'AAAAFw==')
        self.assertRaisesRegex(
            AttributeError,
            "'Key' object has no attribute 'to_bytes'",
            self.api.encode_primary_key,
            *('games', record.Record(record.Key, record.Value)))


class _Sqlite3api_use_deferred_update_process(unittest.TestCase):

    def setUp(self):
        self.api = _api('use_deferred_update_process')

    def tearDown(self):
        _delete_folder('use_deferred_update_process')

    def test_use_deferred_update_process_01(self):
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "use_deferred_update_process not implemented",
            self.api.use_deferred_update_process,
            **{'zzz': 2, 'd': None})


class _Sqlite3api_initial_database_size(unittest.TestCase):
    # initial_database_size does nothing, it is for DPT compatibility.

    def setUp(self):
        self.api = _api('initial_database_size')

    def tearDown(self):
        _delete_folder('initial_database_size')

    def test_initial_database_size_01(self):
        self.assertEqual(self.api.initial_database_size(), True)


class _Sqlite3api_increase_database_size(unittest.TestCase):

    def setUp(self):
        self.api = _api('increase_database_size')

    def tearDown(self):
        _delete_folder('increase_database_size')

    def test_increase_database_size_01(self):
        self.assertEqual(
            self.api.increase_database_size(**{'l':'DPT compatibility'}), None)


class _Sqlite3api_do_database_task(unittest.TestCase):

    def setUp(self):
        self.api = _api('do_database_task')

    def tearDown(self):
        _delete_folder('do_database_task')

    def test_do_database_task_01(self):
        def f(a, b, **c):
            pass
        self.assertEqual(self.api.do_database_task(f), None)


class _Sqlite3api_make_recordset_key(unittest.TestCase):

    def setUp(self):
        self.api = _api('make_recordset_key')

    def tearDown(self):
        _delete_folder('make_recordset_key')

    def test_make_recordset_key_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.make_recordset_key,
            *(None, None))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.make_recordset_key,
            *('games', 'games'))
        self.api.open_context()
        self.assertRaisesRegex(
            TypeError,
            "object of type 'NoneType' has no len\(\)",
            self.api.make_recordset_key,
            *('games', 'games'))
        self.assertIsInstance(
            self.api.make_recordset_key('games', 'Site'),
            recordset.Recordset)


class _Sqlite3api_make_recordset_key_startswith(unittest.TestCase):

    def setUp(self):
        self.api = _api('make_recordset_key_startswith')

    def tearDown(self):
        _delete_folder('make_recordset_key_startswith')

    def test_make_recordset_key_startswith_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.make_recordset_key_startswith,
            *(None, None))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            ''.join((
                "populate_recordset_key_startswith not available on ",
                "primary database")),
            self.api.make_recordset_key_startswith,
            *('games', 'games'))
        self.assertRaisesRegex(
            TypeError,
            'sequence item 0: expected bytes, NoneType found',
            self.api.make_recordset_key_startswith,
            *('games', 'Site'))
        self.api.open_context()
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            ''.join((
                "populate_recordset_key_startswith not available on ",
                "primary database")),
            self.api.make_recordset_key_startswith,
            *('games', 'games'))
        self.assertRaisesRegex(
            TypeError,
            'sequence item 0: expected bytes, NoneType found',
            self.api.make_recordset_key_startswith,
            *('games', 'Site'))


class _Sqlite3api_make_recordset_key_range(unittest.TestCase):

    def setUp(self):
        self.api = _api('make_recordset_key_range')

    def tearDown(self):
        _delete_folder('make_recordset_key_range')

    def test_make_recordset_key_range_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.make_recordset_key_range,
            *(None, None))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.make_recordset_key_range,
            *('games', 'games'))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.make_recordset_key_range,
            *('games', 'Site'))
        self.api.open_context()
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'games'),
            recordset.Recordset)
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'Site'),
            recordset.Recordset)


class _Sqlite3api_make_recordset_all(unittest.TestCase):

    def setUp(self):
        self.api = _api('make_recordset_all')

    def tearDown(self):
        _delete_folder('make_recordset_all')

    def test_make_recordset_all_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.make_recordset_all,
            *(None, None))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.make_recordset_all,
            *('games', 'games'))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.make_recordset_all,
            *('games', 'Site'))
        self.api.open_context()
        self.assertIsInstance(
            self.api.make_recordset_all('games', 'games'),
            recordset.Recordset)
        self.assertIsInstance(
            self.api.make_recordset_all('games', 'Site'),
            recordset.Recordset)


class _Sqlite3api_recordset_for_segment(unittest.TestCase):

    def setUp(self):
        self.api = _api('recordset_for_segment')

    def tearDown(self):
        _delete_folder('recordset_for_segment')

    def test_recordset_for_segment_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'dbset'",
            self.api.recordset_for_segment,
            *(None, None, None))
        rs = recordset.Recordset(dbhome=self.api, dbset='games')
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.recordset_for_segment,
            *(rs, None, None))
        self.assertIsInstance(
            self.api.recordset_for_segment(
                rs, 'games', (None, 5, 10, b'aaaaaa')),
            recordset.Recordset)
        self.assertIsInstance(
            self.api.recordset_for_segment(
                rs, 'Site', (None, 6, 12, b'cccccc')),
            recordset.Recordset)
        self.api.open_context()
        self.assertIsInstance(
            self.api.recordset_for_segment(
                rs, 'games', (None, 1, 18, b'kkkkkk')),
            recordset.Recordset)
        self.assertIsInstance(
            self.api.recordset_for_segment(
                rs, 'Site', (None, 2, 14, b'xxxxxxx')),
            recordset.Recordset)


class _Sqlite3api_file_records_under(unittest.TestCase):

    def setUp(self):
        self.api = _api('file_records_under')

    def tearDown(self):
        _delete_folder('file_records_under')

    def test_file_records_under_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'dbidentity'",
            self.api.file_records_under,
            *(None, None, None, None))
        rs = recordset.Recordset(dbhome=self.api, dbset='games')
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.file_records_under,
            *(None, None, rs, None))
        self.assertRaisesRegex(
            database.DatabaseError,
            "file_records_under not implemented for Sqlite3bitPrimary",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.file_records_under,
            *('games', 'Site', rs, b'dd'))
        self.api.open_context()
        self.assertRaisesRegex(
            database.DatabaseError,
            "Record set was not created from this database instance",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        rs = recordset.Recordset(dbhome=self.api, dbset='games')
        self.assertRaisesRegex(
            database.DatabaseError,
            "file_records_under not implemented for Sqlite3bitPrimary",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        rs._dbset = 'sssss'
        self.assertRaisesRegex(
            database.DatabaseError,
            "Record set was not created from dbset database",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        rs = recordset.Recordset(dbhome=self.api, dbset='games')
        self.assertEqual(
            self.api.file_records_under('games', 'Site', rs, b'dd'),
            None)


class Sqlite3bitapi_method_classes(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_method_classes_01(self):
        bit = sqlite3api.Sqlite3bitapi
        api = sqlite3api._Sqlite3api
        self.assertIs(api.close_database, bit.close_database)
        self.assertIs(api.backout, bit.backout)
        self.assertIs(api.close_contexts, bit.close_contexts)
        self.assertIs(api.commit, bit.commit)
        self.assertIs(api.db_compatibility_hack, bit.db_compatibility_hack)
        self.assertIs(api.exists, bit.exists)
        self.assertIs(api.files_exist, bit.files_exist)
        self.assertIs(api.database_cursor, bit.database_cursor)
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
        self.assertIs(
            api.use_deferred_update_process,
            bit.use_deferred_update_process)
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

        self.assertIsNot(api.delete_instance, bit.delete_instance)
        self.assertIsNot(api.edit_instance, bit.edit_instance)
        self.assertIsNot(api.put_instance, bit.put_instance)

        self.assertEqual(hasattr(api, 'do_deferred_updates'), False)
        self.assertEqual(hasattr(bit, 'do_deferred_updates'), True)
        self.assertEqual(hasattr(api, 'set_defer_update'), False)
        self.assertEqual(hasattr(bit, 'set_defer_update'), True)
        self.assertEqual(hasattr(api, 'unset_defer_update'), False)
        self.assertEqual(hasattr(bit, 'unset_defer_update'), True)


class Sqlite3bitapi_init(unittest.TestCase):

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
            sqlite3api.Sqlite3bitapi,
            )
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Database file name None is not valid",
            sqlite3api.Sqlite3bitapi,
            *(_filespec(),),
            **dict(sqlite3databasefolder=None)
            )
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Table definitions must be a dictionary",
            sqlite3api.Sqlite3bitapi,
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
        api = sqlite3api.Sqlite3bitapi(
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
            sqlite3api.Sqlite3bitapi(_filespec(), sqlite3databasefolder=''),
            sqlite3api.Sqlite3bitapi)
        self.assertIsInstance(
            sqlite3api.Sqlite3bitapi(_filespec(), ''),
            sqlite3api.Sqlite3bitapi)


class Sqlite3bitapi_close_context(unittest.TestCase):

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


class Sqlite3bitapi_open_context(unittest.TestCase):

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


class Sqlite3bitapi_set_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = api('set_defer_update')

    def tearDown(self):
        _delete_folder('set_defer_update')

    def test_set_defer_update_01(self):
        self.assertEqual(self.api.set_defer_update(), False)
        self.assertEqual(self.api.set_defer_update(duallowed=True), True)
        self.assertEqual(self.api.set_defer_update(duallowed='xx'), 'xx')
        self.assertEqual(
            self.api.set_defer_update(db='Berkeley DB compatibility'), False)


class Sqlite3bitapi_unset_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = api('unset_defer_update')

    def tearDown(self):
        _delete_folder('unset_defer_update')

    def test_unset_defer_update_01(self):
        self.assertEqual(
            self.api.unset_defer_update(db='Berkeley DB compatibility'), True)


class Sqlite3bitapi_do_deferred_updates(unittest.TestCase):

    def setUp(self):
        self.api = api('do_deferred_updates')

    def tearDown(self):
        _delete_folder('do_deferred_updates')

    def test_do_deferred_updates_01(self):
        self.api.open_context()
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "'pyscript' is not an existing file",
            self.api.do_deferred_updates,
            *('pyscript', 'filepath'))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "'filepath' is not an existing file",
            self.api.do_deferred_updates,
            *(self.api._sqfile, 'filepath'))
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "'filepath' is not an existing file",
            self.api.do_deferred_updates,
            *(self.api._sqfile, (self.api._sqfile, 'filepath')))
        script = os.path.join(os.path.dirname(self.api._sqfile), 'script')
        open(script, 'w').close()
        sp = self.api.do_deferred_updates(script, self.api._sqfile)
        self.assertIsInstance(sp, subprocess.Popen)
        sp.wait()


class Sqlite3bitapi_put_instance(unittest.TestCase):

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
        self.assertEqual(self.api.put_instance('games', instance), None)
        self.assertEqual(self.api.put_instance('games', instance), None)

    def test_put_instance_04(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 0
        self.api.open_context()
        self.assertEqual(self.api.put_instance('games', instance), None)


class Sqlite3bitapi_delete_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('delete_instance')

    def tearDown(self):
        _delete_folder('delete_instance')

    def test_delete_instance_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'key'",
            self.api.delete_instance,
            *(None, None))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.delete_instance,
            *(None, instance))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'execute'",
            self.api.delete_instance,
            *('games', instance))

    def test_delete_instance_02(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        self.api.open_context()
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Existence bit map for segment does not exist",
            self.api.delete_instance,
            *('games', instance))

    def test_delete_instance_03(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 0
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 100000
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Existence bit map for segment does not exist",
            self.api.delete_instance,
            *('games', instance))
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 10000
        self.assertEqual(self.api.delete_instance('games', instance), None)
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 1
        self.assertEqual(self.api.delete_instance('games', instance), None)
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 1
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Segment does not exist for key",
            self.api.delete_instance,
            *('games', instance))


class Sqlite3bitapi_edit_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('edit_instance')

    def tearDown(self):
        _delete_folder('edit_instance')

    def test_edit_instance_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'key'",
            self.api.edit_instance,
            *(None, None))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, record.Value)
        edited.key.recno = 2
        instance.newrecord = edited
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.edit_instance,
            *(None, instance))

    def test_edit_instance_02(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, record.Value)
        edited.key.recno = 2
        instance.newrecord = edited
        self.assertEqual(self.api.edit_instance('games', instance), None)

    def test_edit_instance_03(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 2
        instance.newrecord = edited
        self.api.open_context()
        self.assertRaisesRegex(
            sqlite3api.Sqlite3apiError,
            "Segment does not exist for key",
            self.api.edit_instance,
            *('games', instance))

    def test_edit_instance_04(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 2
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance.newrecord = edited
        self.assertEqual(self.api.edit_instance('games', instance), None)

    def test_edit_instance_05(self):
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 3
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance.newrecord = edited
        self.assertEqual(self.api.edit_instance('games', instance), None)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Sqlite3DatabaseEncoders))

    # The _Sqlite3api class tests.
    
    runner().run(loader(_Sqlite3api_init))

    # These three not in definition order.
    runner().run(loader(_Sqlite3api_open_context))
    runner().run(loader(_Sqlite3api_close_context))
    runner().run(loader(_Sqlite3api_close_database))

    # The rest in definition order.
    runner().run(loader(_Sqlite3api_backout))
    runner().run(loader(_Sqlite3api_close_contexts))
    runner().run(loader(_Sqlite3api_commit))
    runner().run(loader(_Sqlite3api_db_compatibility_hack))
    runner().run(loader(_Sqlite3api_exists))
    runner().run(loader(_Sqlite3api_files_exist))
    runner().run(loader(_Sqlite3api_make_cursor))
    runner().run(loader(_Sqlite3api_get_database_folder))
    runner().run(loader(_Sqlite3api_get_database))
    runner().run(loader(_Sqlite3api_get_database_instance))
    runner().run(loader(_Sqlite3api_get_first_primary_key_for_index_key))
    runner().run(loader(_Sqlite3api_get_primary_record))
    runner().run(loader(_Sqlite3api_is_primary))
    runner().run(loader(_Sqlite3api_is_primary_recno))
    runner().run(loader(_Sqlite3api_is_recno))
    runner().run(loader(_Sqlite3api_open_contexts))
    runner().run(loader(_Sqlite3api_get_packed_key))
    runner().run(loader(_Sqlite3api_decode_as_primary_key))
    runner().run(loader(_Sqlite3api_encode_primary_key))
    runner().run(loader(_Sqlite3api_use_deferred_update_process))
    runner().run(loader(_Sqlite3api_initial_database_size))
    runner().run(loader(_Sqlite3api_increase_database_size))
    runner().run(loader(_Sqlite3api_do_database_task))
    runner().run(loader(_Sqlite3api_make_recordset_key))
    runner().run(loader(_Sqlite3api_make_recordset_key_startswith))
    runner().run(loader(_Sqlite3api_make_recordset_key_range))
    runner().run(loader(_Sqlite3api_make_recordset_all))
    runner().run(loader(_Sqlite3api_recordset_for_segment))
    runner().run(loader(_Sqlite3api_file_records_under))

    # The Sqlite3bitapi class tests.
    
    runner().run(loader(Sqlite3bitapi_method_classes))
    runner().run(loader(Sqlite3bitapi_init))
    runner().run(loader(Sqlite3bitapi_close_context))
    runner().run(loader(Sqlite3bitapi_open_context))
    runner().run(loader(Sqlite3bitapi_set_defer_update))
    runner().run(loader(Sqlite3bitapi_unset_defer_update))
    runner().run(loader(Sqlite3bitapi_do_deferred_updates))
    runner().run(loader(Sqlite3bitapi_put_instance))
    runner().run(loader(Sqlite3bitapi_delete_instance))
    runner().run(loader(Sqlite3bitapi_edit_instance))
