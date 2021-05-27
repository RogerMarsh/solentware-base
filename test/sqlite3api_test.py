# sqlite3api_test.py
# Copyright 2014 Roger Marsh
# License: See LICENSE.TXT (BSD licence)

"""sqlite3api tests using a realistic FileSpec and the real methods defined in
the class hierarchy.  The delete_instance, edit_instance, and put_instance,
tests take a few minutes because each needs a few tens of thousands of records
to see the effects tested.
"""

import unittest
import os
import shutil
import sqlite3
import subprocess
import sys

from ..api.test._filespec import (
    samplefilespec,
    SampleValue,
    SampleRecord,
    GAMES_FILE_DEF,
    GAME_FIELD_DEF,
    EVENT_FIELD_DEF,
    SITE_FIELD_DEF,
    DATE_FIELD_DEF,
    ROUND_FIELD_DEF,
    WHITE_FIELD_DEF,
    BLACK_FIELD_DEF,
    RESULT_FIELD_DEF,
    SEVEN_TAG_ROSTER,
    PRIMARY,
    SECONDARY,
    )
from .. import sqlite3api, _sqlite
from ..api import filespec, record, recordset, database, primary, secondary
from ..api.segmentsize import SegmentSize
from . import database_interface


def base(folder):
    return sqlite3api.Database(
        _sqlite.Primary,
        _sqlite.Secondary,
        samplefilespec(),
        os.path.expanduser(
            os.path.join('~', 'sqlite3api_tests', folder)))


def api(folder):
    return sqlite3api.Sqlite3api(
        samplefilespec(),
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
        v[1]['Site'] = 'gash' # defined in samplefilespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in samplefilespec(), ignored
        return v


# Defined at module level for pickling.
class _ValueEdited(record.Value):
    def pack(self):
        v = super().pack()
        v[1]['Site'] = 'newgash' # defined in samplefilespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in samplefilespec(), ignored
        return v


class _DatabaseEncoders(unittest.TestCase):

    def setUp(self):
        self.sde = _sqlite._DatabaseEncoders()

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(repr(57), '57')

    def test_encode_record_number_01(self):
        self.assertEqual(self.sde.encode_record_number(57), '57')

    def test_encode_record_number_02(self):
        self.assertEqual(self.sde.encode_record_number('ab'), "'ab'")

    def test_decode_record_number_01(self):
        self.assertRaisesRegex(
            ValueError,
            "malformed node or string: 57",
            self.sde.decode_record_number,
            57)

    def test_decode_record_number_02(self):
        self.assertEqual(self.sde.decode_record_number('57'), 57)

    def test_encode_record_selector(self):
        self.assertEqual(self.sde.encode_record_selector('57'), '57')

    def test_encode_record_key(self):
        self.assertEqual(self.sde.encode_record_key('key'), 'key')


class Database_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        p = os.path.abspath('')
        ba = sqlite3api.Database(None, None, {}, '')
        self.assertEqual(ba._dbdef, {})
        self.assertEqual(ba._dbservices, None)
        self.assertEqual(ba._dbspec, {})
        self.assertEqual(ba._home_directory, p)
        self.assertEqual(ba._home,
                         os.path.join(p, os.path.basename(p)))
        self.assertIsInstance(ba._control, _sqlite.ControlFile)
        self.assertEqual(len(ba.__dict__), 6)

    def test___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 4 required positional arguments: ",
                "'primary_class', 'secondary_class', ",
                "'database_specification', and 'databasefolder'",
                )),
            sqlite3api.Database,
            )

    def test___init___03(self):
        if sys.platform == 'win32':
            err = "Database specification must be a dictionary"
        else:
            err = "Database folder name None is not valid"
        self.assertRaisesRegex(
            database.DatabaseError,
            err,
            sqlite3api.Database,
            *(None, None, None, None, None))

    def test___init___04(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database specification must be a dictionary",
            sqlite3api.Database,
            *(None, None, None, '', None))

    def test___init___05(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 't' must be a dictionary",
            sqlite3api.Database,
            *(None, None, dict(t=None), '', None))

    def test___init___06(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 't' must contain a primary name",
            sqlite3api.Database,
            *(None, None, dict(t={}), '', None))

    def test___init___07(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join(("Field definitions must be present in specification ",
                     "for primary fields")),
            sqlite3api.Database,
            *(None,
              None,
              dict(t=dict(primary='key')),
              '',
              None))

    def test___init___08(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Primary name key for t must be in fields definition",
            sqlite3api.Database,
            *(None,
              None,
              dict(t=dict(primary='key', fields=('lock',))),
              '',
              None))

    def test___init___09(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            sqlite3api.Database,
            *(None,
              None,
              filespec.FileSpec(t=dict(primary='key', fields=('lock', 'key'))),
              '',
              None))

    def test___init___10(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key=True),
                      filedesc=dict(fileorg=None))),
              '',
              None))

    def test___init___11(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            sqlite3api.Database,
            *(_sqlite.Primary,
              _sqlite.Secondary,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key=True),
                      filedesc=dict(fileorg=None))),
              '',
              None))

    def test___init___12(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Full path name of DPT file for t is invalid",
            sqlite3api.Database,
            *(_sqlite.Primary,
              _sqlite.Secondary,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key=True),
                      filedesc=dict(fileorg=None),
                      ddname='G')),
              '',
              None))

    def test___init___13(self):
        self.assertIsInstance(
            sqlite3api.Database(
                _sqlite.Primary,
                _sqlite.Secondary,
                filespec.FileSpec(
                    t=dict(
                        primary='key',
                        fields=dict(lock={}, key={}),
                        filedesc=dict(fileorg=None),
                        ddname='G',
                        file='H')),
                '',
                None),
            sqlite3api.Database)

    def test___init___14(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'items'",
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=None)),
              '',
              None))

    def test___init___15(self):
        self.assertRaisesRegex(
            AttributeError,
            "'int' object has no attribute 'lower'",
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary={1:2})),
              '',
              None))

    def test___init___16(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join((
                "Primary name key for t must not be in ",
                "secondary definition \(ignoring case\)")),
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(key='key'),
                      )),
              '',
              None))

    def test___init___17(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join((
                "Primary name key for t must not be in ",
                "secondary definition \(ignoring case\)")),
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              dict(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(lock='key'),
                      )),
              '',
              None))

    # Now must have FileSpec which brings in some DPT related stuff.

    def test___init___18(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join((
                "Primary name key for t must not be in ",
                "secondary definition \(ignoring case\)")),
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None, brecppg=100),
                      secondary=dict(key=None),
                      btod_factor=5,
                      )),
              '',
              None))

    def test___init___19(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join((
                "Primary name key for t must not be in ",
                "secondary definition \(ignoring case\)")),
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}, Key={}),
                      filedesc=dict(fileorg=None, brecppg=100),
                      secondary=dict(key=None),
                      btod_factor=5,
                      )),
              '',
              None))

    def test___init___20(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            sqlite3api.Database,
            *(_sqlite.Primary,
              None,
              filespec.FileSpec(
                  t=dict(
                      primary='key',
                      fields=dict(lock={}, key={}),
                      filedesc=dict(fileorg=None),
                      secondary=dict(key='lock'),
                      )),
              '',
              None))

    def test___init___21(self):
        api = sqlite3api.Database(
            _sqlite.Primary,
            _sqlite.Secondary,
            filespec.FileSpec(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}, Skey={}),
                    filedesc=dict(fileorg=None, brecppg=100),
                    secondary=dict(skey=None),
                    btod_factor=5,
                    ddname='KEY',
                    file='keyfile',
                    )),
            '',
            None)
        p = os.path.abspath('')
        self.assertEqual(api._home_directory, p)
        self.assertEqual(api._home, os.path.join(p, os.path.basename(p)))
        self.assertEqual(api._dbservices, None)
        self.assertEqual(api._dbspec,
                         dict(t={'fields':{'key':{}, 'lock':{}, 'Skey':{}},
                                 'filedesc':{'fileorg':None, 'bsize':2,
                                             'brecppg':100, 'dsize':10},
                                 'primary':'key',
                                 'secondary':{'skey':None},
                                 'btod_factor':5,
                                 'default_records':200,
                                 'ddname':'KEY',
                                 'file':'keyfile',
                                 })
                         )
        self.assertEqual(len(api._dbdef), 1)
        self.assertIsInstance(api._dbdef['t'], _sqlite.Definition)
        df = api._dbdef['t']
        self.assertEqual(df._dbset, 't')
        self.assertEqual(df.dbname_to_secondary_key, {'skey': 'Skey'})
        self.assertIsInstance(df.primary, _sqlite.Primary)
        self.assertIsInstance(df.secondary['Skey'], _sqlite.Secondary)
        self.assertEqual(len(df.secondary), 1)
        self.assertEqual(len(df.__dict__), 4)

    def test___init___22(self):
        # Differences between _sqlite and dbapi
        api = sqlite3api.Database(
            _sqlite.Primary,
            _sqlite.Secondary,
            filespec.FileSpec(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}, Skey={}),
                    filedesc=dict(fileorg=None, brecppg=100),
                    secondary=dict(skey=None),
                    btod_factor=5,
                    ddname='KEY',
                    file='keyfile',
                    )),
            '',
            None)
        self.assertIsInstance(api._control, _sqlite.ControlFile)
        self.assertEqual(len(api.__dict__), 6)


class Database_open_context(unittest.TestCase):

    def setUp(self):

        # Workaround ignored 'file in use in another process' exception in
        # _delete_folder() on Microsoft Windows.
        # _delete_folder() usually called in test's tearDown() method.
        if sys.platform == 'win32':
            _delete_folder('open_context')

        self.api = base('open_context')

    def tearDown(self):
        _delete_folder('open_context')

    def test_open_context_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(os.path.exists(self.api._home), False)
        self.assertEqual(self.api.open_context(), True)
        conn = self.api._dbservices
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertEqual(os.path.isfile(self.api._home), True)
        self.assertEqual(self.api.open_context(), True)
        self.assertIs(self.api._dbservices, conn)
        self.assertEqual(os.path.isfile(self.api._home), True)


class Database_close_context(unittest.TestCase):

    def setUp(self):
        self.api = base('close_context')

    def tearDown(self):
        _delete_folder('close_context')

    def test_close_context_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(self.api.close_context(), None)
        self.assertEqual(self.api._dbservices, None)

    def test_close_context_02(self):
        self.api.open_context()
        conn = self.api._dbservices
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertEqual(self.api.close_context(), None)
        self.assertIs(self.api._dbservices, conn)
        self.assertEqual(self.api.close_context(), None)
        self.assertIs(self.api._dbservices, conn)


class Database_close_database(unittest.TestCase):

    def setUp(self):
        self.api = base('close_database')

    def tearDown(self):
        _delete_folder('close_database')

    def test_close_database_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._dbservices, None)

    def test_close_database_02(self):
        self.api.open_context()
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(self.api.close_database(), None)
        self.assertEqual(self.api._dbservices, None)


class Database_get_database_filenames(unittest.TestCase):

    def setUp(self):
        self.api = base('api_get_database_filenames')
        self.api.open_context()

    def tearDown(self):
        _delete_folder('api_get_database_filenames')

    def test_get_database_filenames_01(self):
        self.assertEqual(sorted(self.api.get_database_filenames()),
                         sorted(['api_get_database_filenames']))


class Database_backout(unittest.TestCase):
    # Nothing to backout, just check the calls work.

    def setUp(self):
        self.api = base('backout')

    def tearDown(self):
        _delete_folder('backout')

    def test_backout_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(self.api.backout(), None)
        self.api.open_context()
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)
        self.api.start_transaction()
        self.assertEqual(self.api.backout(), None)
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)


class Database_close_contexts(unittest.TestCase):

    def setUp(self):
        self.api = base('close_contexts')

    def tearDown(self):
        _delete_folder('close_contexts')

    def test_close_contexts_01(self):
        self.assertEqual(self.api.close_contexts('DPT compatibility'), None)


class Database_commit(unittest.TestCase):
    # Nothing to commit, just check the calls work.

    def setUp(self):
        self.api = base('commit')

    def tearDown(self):
        _delete_folder('commit')

    def test_commit_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(self.api.commit(), None)
        self.api.open_context()
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)
        self.api.start_transaction()
        self.assertEqual(self.api.commit(), None)
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)


class Database_db_compatibility_hack(unittest.TestCase):

    def setUp(self):
        self.api = base('db_compatibility_hack')
        self.srkey = self.api.encode_record_number(3456)

    def tearDown(self):
        _delete_folder('db_compatibility_hack')

    def test_db_compatibility_hack_01(self):
        record = ('key', 'value')
        self.assertEqual(
            self.api.db_compatibility_hack(record, self.srkey), record)

    def test_db_compatibility_hack_02(self):
        # ('key', None) in Sqlite3 is converted to ('key', <repr(primarykey)>)
        # which is self.srkey in this test.
        record = ('key', None)
        value = self.api.decode_record_number(self.srkey)
        self.assertEqual(
            self.api.db_compatibility_hack(record, self.srkey),
            ('key', value))


class Database_exists(unittest.TestCase):

    def setUp(self):
        self.api = base('exists')

    def tearDown(self):
        _delete_folder('exists')

    def test_exists_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.exists,
            *(None, None))
        self.assertEqual(self.api.exists('games', None), False)
        self.assertEqual(self.api.exists('games', 'Site'), True)


class Database_files_exist(unittest.TestCase):

    def setUp(self):
        self.api = base('files_exist')

    def tearDown(self):
        _delete_folder('files_exist')

    def test_exists_01(self):
        self.assertEqual(self.api.files_exist(), False)
        self.api.open_context()
        self.assertEqual(self.api.files_exist(), True)
        self.api.close_database()
        self.assertEqual(self.api.files_exist(), True)


class Database_database_cursor(unittest.TestCase):

    def setUp(self):
        self.api = base('database_cursor')

    def tearDown(self):
        _delete_folder('database_cursor')

    def test_make_cursor_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.database_cursor,
            *(None, None))

    def test_make_cursor_02(self):
        self.assertIsInstance(
            self.api.database_cursor('games', 'Site'),
            _sqlite.CursorSecondary)

    def test_make_cursor_03(self):
        self.assertIsInstance(
            self.api.database_cursor('games', 'games'),
            _sqlite.CursorPrimary)


class Database_repair_cursor(unittest.TestCase):

    def setUp(self):
        self.api = base('database_cursor')

    def tearDown(self):
        _delete_folder('database_cursor')

    def test_repair_cursor_01(self):
        cursor = self.api.database_cursor('games', 'Site')
        self.assertIs(self.api.repair_cursor(cursor), cursor)


class Database_get_database_folder(unittest.TestCase):

    def setUp(self):
        self.api = base('get_database_folder')

    def tearDown(self):
        _delete_folder('get_database_folder')

    def test_get_database_folder_01(self):
        self.assertEqual(
            self.api.get_database_folder(),
            os.path.expanduser(
                os.path.join('~', 'sqlite3api_tests', 'get_database_folder')),
            msg="Failure due to path separators expected in 'msys' terminal.")


class Database_get_database(unittest.TestCase):

    def setUp(self):
        self.api = base('get_database')

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
            sqlite3.Connection)
        self.assertIsInstance(
            self.api.get_database('games', 'games'),
            sqlite3.Connection)


class Database_get_database_instance(unittest.TestCase):

    def setUp(self):
        self.api = base('get_database_instance')

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
            _sqlite.Secondary)
        self.assertIsInstance(
            self.api.get_database_instance('games', 'games'),
            _sqlite.Primary)


class Database_get_first_primary_key_for_index_key(unittest.TestCase):

    def setUp(self):
        self.api = api('get_first_primary_key_for_index_key')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('get_first_primary_key_for_index_key')

    def test_get_first_primary_key_for_index_key_01(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("get_first_primary_key_for_index_key not available ",
                     "on primary database")),
            self.api.get_first_primary_key_for_index_key,
            *('games', 'games', 'k'))

    def test_get_first_primary_key_for_index_key_02(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join(("get_first_primary_key_for_index_key not available ",
                     "on primary database")),
            self.api.get_first_primary_key_for_index_key,
            *('games', 'games', None))

    def test_get_first_primary_key_for_index_key_03(self):
        r = self.api.get_first_primary_key_for_index_key('games', 'Site', 'k')
        self.assertEqual(r, None)

    def test_get_first_primary_key_for_index_key_04(self):
        self.assertRaisesRegex(
            TypeError,
            "'NoneType' object is not iterable",
            self.api.get_first_primary_key_for_index_key,
            *('games', 'Site', None))

    def test_get_first_primary_key_for_index_key_05(self):
        r = self.api.get_first_primary_key_for_index_key('games', 'Site', b'k')
        self.assertEqual(r, None)


class Database_get_primary_record_ok(unittest.TestCase):
    # Differences with dbapi version suggest something is wrong.
    # Probably need to see results where record exists.

    def setUp(self):
        self.api = base('get_primary_record')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('get_primary_record')

    def test_get_primary_record_ok_01(self):
        self.assertEqual(self.api.get_primary_record('games', 'k'), None)

    def test_get_primary_record_ok_02(self):
        self.assertEqual(self.api.get_primary_record('games', None), None)

    def test_get_primary_record_ok_03(self):
        self.assertEqual(self.api.get_primary_record('games', 10), None)


class Database_get_primary_record_fail(unittest.TestCase):

    def setUp(self):
        self.api = base('get_primary_record')
        # Generally things do not work until the open_context() call.
        # See Database_get_primary_record_ok for this case.

    def tearDown(self):
        _delete_folder('get_primary_record')

    def test_get_primary_record_fail_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.get_primary_record,
            *(None, None))

    def test_get_primary_record_fail_02(self):
        self.assertRaisesRegex(
            TypeError,
            "sequence item 1: expected str instance, NoneType found",
            self.api.get_primary_record,
            *('games', 'k'))


class Database_is_primary(unittest.TestCase):

    def setUp(self):
        self.api = base('is_primary')

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


class Database_is_primary_recno(unittest.TestCase):

    def setUp(self):
        self.api = base('is_primary_recno')

    def tearDown(self):
        _delete_folder('is_primary_recno')

    def test_is_primary_recno_01(self):
        self.assertEqual(
            self.api.is_primary_recno('Berkeley DB compatibility'), True)


class Database_is_recno(unittest.TestCase):

    def setUp(self):
        self.api = base('is_recno')

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


class Database_open_contexts(unittest.TestCase):

    def setUp(self):
        self.api = base('open_contexts')

    def tearDown(self):
        _delete_folder('open_contexts')

    def test_open_contexts_01(self):
        self.assertEqual(self.api.open_contexts('DPT compatibility'), None)


class Database_allocate_and_open_contexts(unittest.TestCase):

    def setUp(self):
        self.api = base('allocate_and_open_contexts')

    def tearDown(self):
        _delete_folder('allocate_and_open_contexts')

    def test_open_contexts_01(self):
        self.assertEqual(self.api.allocate_and_open_contexts(
            'DPT compatibility'), None)


class Database_get_packed_key(unittest.TestCase):

    def setUp(self):
        self.api = base('get_packed_key')

    def tearDown(self):
        _delete_folder('get_packed_key')

    def test_get_packed_key_01(self):
        instance = record.Record(record.Key, record.Value)
        self.assertEqual(
            self.api.get_packed_key('Berkeley DB compatibility', instance),
            instance.key.pack())


class Database_decode_as_primary_key(unittest.TestCase):

    def setUp(self):
        self.api = base('decode_as_primary_key')

    def tearDown(self):
        _delete_folder('decode_as_primary_key')

    def test_decode_as_primary_key_01(self):
        self.assertEqual(self.api.decode_as_primary_key(
            'Ignored in sqlite3', 10), 10)

    def test_decode_as_primary_key_02(self):
        srkey = self.api.encode_record_number(3456)
        self.assertEqual(self.api.decode_as_primary_key(
            'Ignored in sqlite3', srkey), 3456)


class Database_encode_primary_key(unittest.TestCase):

    def setUp(self):
        self.api = base('encode_primary_key')

    def tearDown(self):
        _delete_folder('encode_primary_key')

    def test_encode_primary_key_01(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.load(23)
        self.assertEqual(
            self.api.encode_primary_key('games', instance),
            '23')


class Database_use_deferred_update_process(unittest.TestCase):

    def setUp(self):
        self.api = base('use_deferred_update_process')

    def tearDown(self):
        _delete_folder('use_deferred_update_process')

    def test_use_deferred_update_process_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "use_deferred_update_process not implemented",
            self.api.use_deferred_update_process,
            **{'zzz': 2, 'd': None})


class Database_initial_database_size(unittest.TestCase):
    # initial_database_size does nothing, it is for DPT compatibility.

    def setUp(self):
        self.api = base('initial_database_size')

    def tearDown(self):
        _delete_folder('initial_database_size')

    def test_initial_database_size_01(self):
        self.assertEqual(self.api.initial_database_size(), True)


class Database_increase_database_size(unittest.TestCase):

    def setUp(self):
        self.api = base('increase_database_size')

    def tearDown(self):
        _delete_folder('increase_database_size')

    def test_increase_database_size_01(self):
        self.assertEqual(
            self.api.increase_database_size(**{'l':'DPT compatibility'}), None)


class Database_do_database_task(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        _delete_folder('do_database_task')

    def test_do_database_task_01(self):
        def f(a, b, **c):
            pass
        self.api = base('do_database_task')
        self.assertRaisesRegex(
            TypeError,
            ''.join((
                "__init__\(\) missing 3 required positional arguments: ",
                "'secondary_class', 'database_specification', ",
                "and 'databasefolder'",
                )),
            self.api.do_database_task,
            *(f,))

    def test_do_database_task_02(self):
        def f(a, b, **c):
            pass
        class P(primary.Primary):
            def set_control_database(self, *a):
                pass
            def open_root(self, *a):
                pass
            def close(self):
                pass
        class S(secondary.Secondary):
            def set_primary_database(self, *a):
                pass
            def open_root(self, *a):
                pass
            def close(self):
                pass
        class C(sqlite3api.Database):
            def __init__(self, *a, **k):
                super().__init__(
                    P,
                    S,
                    samplefilespec(),
                    *a,
                    **k)
        self.api = C(
            os.path.expanduser(
                os.path.join('~', 'sqlite3api_tests', 'do_database_task')))
        self.assertEqual(self.api.do_database_task(f), None)


class Database_make_recordset_key(unittest.TestCase):

    def setUp(self):
        self.api = api('make_recordset_key')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('make_recordset_key')

    def test_make_recordset_key_01(self):
        self.assertIsInstance(
            self.api.make_recordset_key('games', 'games', key=1),
            recordset.Recordset)

    def test_make_recordset_key_02(self):
        self.assertIsInstance(
            self.api.make_recordset_key('games', 'games'),
            recordset.Recordset)

    def test_make_recordset_key_03(self):
        self.assertIsInstance(
            self.api.make_recordset_key('games', 'Site', key=b'a'),
            recordset.Recordset)

    def test_make_recordset_key_04(self):
        self.assertIsInstance(
            self.api.make_recordset_key('games', 'Site'),
            recordset.Recordset)


class Database_make_recordset_key_startswith(unittest.TestCase):

    def setUp(self):
        self.api = api('make_recordset_key_startswith')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('make_recordset_key_startswith')

    def test_make_recordset_key_startswith_01(self):
        self.assertIsInstance(
            self.api.make_recordset_key_startswith('games', 'Site', key=b'a'),
            recordset.Recordset)

    def test_make_recordset_key_startswith_02(self):
        self.assertIsInstance(
            self.api.make_recordset_key_startswith('games', 'Site'),
            recordset.Recordset)

    def test_make_recordset_key_startswith_03(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join((
                "populate_recordset_key_startswith not available on ",
                "primary database")),
            self.api.make_recordset_key_startswith,
            *('games', 'games'),
            **dict(key=15))

    def test_make_recordset_key_startswith_04(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            ''.join((
                "populate_recordset_key_startswith not available on ",
                "primary database")),
            self.api.make_recordset_key_startswith,
            *('games', 'games'))


class Database_make_recordset_key_range(unittest.TestCase):

    def setUp(self):
        self.api = api('make_recordset_key_range')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('make_recordset_key_range')

    def test_make_recordset_key_range_01(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'Site', key=b'c'),
            recordset.Recordset)

    def test_make_recordset_key_range_02(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'Site'),
            recordset.Recordset)

    def test_make_recordset_key_range_03(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'Site', keyend=b't'),
            recordset.Recordset)

    def test_make_recordset_key_range_04(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range(
                'games', 'Site', key=b'c', keyend=b't'),
            recordset.Recordset)

    def test_make_recordset_key_range_05(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'games', key=5),
            recordset.Recordset)

    def test_make_recordset_key_range_06(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'games'),
            recordset.Recordset)

    def test_make_recordset_key_range_07(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range('games', 'games', keyend=50),
            recordset.Recordset)

    def test_make_recordset_key_range_08(self):
        self.assertIsInstance(
            self.api.make_recordset_key_range(
                'games', 'games', key=5, keyend=50),
            recordset.Recordset)


class Database_make_recordset_all(unittest.TestCase):

    def setUp(self):
        self.api = api('make_recordset_all')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('make_recordset_all')

    def test_make_recordset_all_01(self):
        self.assertIsInstance(
            self.api.make_recordset_all('games', 'games'),
            recordset.Recordset)


class Database_file_records_under_01(unittest.TestCase):

    def setUp(self):
        self.api = base('file_records_under_01')
        self.rs = recordset.Recordset(dbhome=self.api, dbset='games')

    def tearDown(self):
        _delete_folder('file_records_under_01')

    def test_file_records_under_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'dbidentity'",
            self.api.file_records_under,
            *(None, None, None, None))

    def test_file_records_under_02(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.file_records_under,
            *(None, None, self.rs, None))

    def test_file_records_under_03(self):
        self.assertRaisesRegex(
            primary.PrimaryError,
            "file_records_under not available for primary database",
            self.api.file_records_under,
            *('games', 'games', self.rs, b'dd'))

    def test_file_records_under_04(self):
        self.assertRaisesRegex(
            TypeError,
            "sequence item 7: expected str instance, NoneType found",
            self.api.file_records_under,
            *('games', 'Site', self.rs, None))

    def test_file_records_under_05(self):
        self.assertRaisesRegex(
            TypeError,
            "sequence item 7: expected str instance, NoneType found",
            self.api.file_records_under,
            *('games', 'Site', self.rs, b'dd'))


class Database_file_records_under_02(unittest.TestCase):

    def setUp(self):
        self.api = base('file_records_under_02')
        self.api.open_context()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('file_records_under_02')

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
            primary.PrimaryError,
            "file_records_under not available for primary database",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        self.assertEqual(
            self.api.file_records_under(
                'games',
                'Site',
                recordset.Recordset(dbhome=self.api, dbset='games'),
                None),
            None)
        self.assertEqual(
            self.api.file_records_under(
                'games',
                'Site',
                recordset.Recordset(dbhome=self.api, dbset='games'),
                b'dd'),
            None)
        self.api.open_context()
        self.assertRaisesRegex(
            primary.PrimaryError,
            "file_records_under not available for primary database",
            self.api.file_records_under,
            *('games', 'games', rs, b'dd'))
        rs = recordset.Recordset(dbhome=self.api, dbset='games')
        self.assertRaisesRegex(
            primary.PrimaryError,
            "file_records_under not available for primary database",
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


class Database_start_transaction(unittest.TestCase):

    def setUp(self):
        self.api = base('start_transaction')

    def tearDown(self):
        _delete_folder('start_transaction')

    def test_start_transaction_01(self):
        self.assertEqual(self.api.start_transaction(), None)


class Database_cede_contexts_to_process(unittest.TestCase):

    def setUp(self):
        self.api = base('cede_contexts_to_process')

    def tearDown(self):
        _delete_folder('cede_contexts_to_process')

    def test_cede_contexts_to_process_01(self):
        self.assertEqual(self.api.cede_contexts_to_process({}), None)

    def test_cede_contexts_to_process_02(self):
        self.api.open_context()
        self.assertEqual(self.api.cede_contexts_to_process({}), None)


class Database_set_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = base('set_defer_update')

    def tearDown(self):
        _delete_folder('set_defer_update')

    def test_set_defer_update_01(self):
        self.assertEqual(self.api.set_defer_update(), False)
        self.assertEqual(self.api.set_defer_update(duallowed=True), True)
        self.assertEqual(self.api.set_defer_update(duallowed='xx'), 'xx')
        self.assertEqual(
            self.api.set_defer_update(db='Berkeley DB compatibility'), False)


class Database_unset_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = base('unset_defer_update')

    def tearDown(self):
        _delete_folder('unset_defer_update')

    def test_unset_defer_update_01(self):
        self.assertEqual(
            self.api.unset_defer_update(db='Berkeley DB compatibility'), True)


class Database_do_deferred_updates(unittest.TestCase):

    def setUp(self):
        self.api = api('do_deferred_updates')

    def tearDown(self):
        _delete_folder('do_deferred_updates')

    def test_do_deferred_updates_01(self):
        self.api.open_context()
        self.assertRaisesRegex(
            _sqlite.Sqlite3apiError,
            "'pyscript' is not an existing file",
            self.api.do_deferred_updates,
            *('pyscript', 'filepath'))
        self.assertRaisesRegex(
            _sqlite.Sqlite3apiError,
            "'filepath' is not an existing file",
            self.api.do_deferred_updates,
            *(self.api._home, 'filepath'))
        self.assertRaisesRegex(
            _sqlite.Sqlite3apiError,
            "'filepath' is not an existing file",
            self.api.do_deferred_updates,
            *(self.api._home, (self.api._home, 'filepath')))
        script = os.path.join(os.path.dirname(self.api._home), 'script')
        open(script, 'w').close()
        sp = self.api.do_deferred_updates(script, self.api._home)
        self.assertIsInstance(sp, subprocess.Popen)
        sp.wait()


class Sqlite3api_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'database_specification'",
                )),
            sqlite3api.Sqlite3api,
            )
        if sys.platform == 'win32':
            sqlite3api.Sqlite3api(
                *(samplefilespec(),), **dict(databasefolder=None))
            print(
                'An exception is expected, but not raised on Microsoft Windows')
            print(
                'Sqlite3api_init.test___init__01')
        else:
            self.assertRaisesRegex(
                database.DatabaseError,
                "Database folder name None is not valid",
                sqlite3api.Sqlite3api,
                *(samplefilespec(),),
                **dict(databasefolder=None)
                )
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database specification must be a dictionary",
            sqlite3api.Sqlite3api,
            *(None,),
            **dict(databasefolder='')
            )

    def test___init___02(self):
        api = sqlite3api.Sqlite3api(
            filespec.FileSpec(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}, Skey={}),
                    filedesc=dict(fileorg=None),
                    secondary=dict(skey=None),
                    ddname='G',
                    file='F',
                    )),
            databasefolder='')
        p = os.path.abspath('')
        self.assertEqual(api._home_directory, p)
        self.assertEqual(api._home, os.path.join(p, os.path.basename(p)))
        self.assertEqual(api._dbservices, None)
        self.assertIsInstance(api._control, _sqlite.ControlFile)
        self.assertEqual(api._dbspec,
                         dict(t={'fields':{'key':{}, 'lock':{}, 'Skey':{}},
                                 'filedesc':{'fileorg':None, 'bsize':20,
                                             'brecppg':10, 'dsize':160},
                                 'primary':'key',
                                 'secondary':{'skey':None},
                                 'btod_factor':8,
                                 'default_records':200,
                                 'ddname':'G',
                                 'file':'F',
                                 })
                         )
        self.assertEqual(len(api._dbdef), 1)
        self.assertIsInstance(api._dbdef['t'], _sqlite.Definition)
        df = api._dbdef['t']
        self.assertEqual(df._dbset, 't')
        self.assertEqual(df.dbname_to_secondary_key, {'skey': 'Skey'})
        self.assertIsInstance(df.primary, _sqlite.Primary)
        self.assertIsInstance(df.secondary['Skey'], _sqlite.Secondary)
        self.assertEqual(len(df.secondary), 1)
        self.assertEqual(len(df.__dict__), 4)
        self.assertEqual(len(api.__dict__), 6)


class Sqlite3api_make_connection(unittest.TestCase):

    def setUp(self):

        # Workaround ignored 'file in use in another process' exception in
        # _delete_folder() on Microsoft Windows.
        # _delete_folder() usually called in test's tearDown() method.
        if sys.platform == 'win32':
            _delete_folder('make_connection')

        self.api = api('make_connection')
        os.makedirs(self.api._home_directory)

    def tearDown(self):
        _delete_folder('make_connection')

    def test_make_connection_01(self):
        self.api.make_connection()
        self.assertIsInstance(self.api._dbservices, sqlite3.Connection)


class Sqlite3api_put_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('put_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
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
            "'NoneType' object has no attribute 'cursor'",
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
        self.assertRaisesRegex(
            KeyError,
            "'hhhhhh'",
            self.api.put_instance,
            *('games', instance))

    def test_put_instance_05(self):
        database_interface.test_put_instance_05(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_06(self):
        database_interface.test_put_instance_06(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_07(self):
        database_interface.test_put_instance_07(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_08(self):
        database_interface.test_put_instance_08(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_09(self):
        database_interface.test_put_instance_09(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_10(self):
        database_interface.test_put_instance_10(self,
                                                collect_counts,
                                                game_number_to_record_number)


class Sqlite3api_delete_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('delete_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
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
            TypeError,
            "sequence item 5: expected str instance, NoneType found",
            self.api.delete_instance,
            *('games', instance))
        #self.assertRaisesRegex(
        #    TypeError,
        #    "'NoneType' object is not subscriptable",
        #    self.api.delete_instance,
        #    *('games', instance))

    def test_delete_instance_02(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        self.api.open_context()
        self.assertRaisesRegex(
            _sqlite.Sqlite3apiError,
            "Existence bit map for segment does not exist",
            self.api.delete_instance,
            *('games', instance))

    def test_delete_instance_03(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 0
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance = record.Record(record.KeyData, _Value)
        instance.key.recno = 100000
        self.assertRaisesRegex(
            _sqlite.Sqlite3apiError,
            "Existence bit map for segment does not exist",
            self.api.delete_instance,
            *('games', instance))
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 10000
        self.assertEqual(self.api.delete_instance('games', instance), None)
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 1
        self.assertEqual(self.api.delete_instance('games', instance), None)
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 1
        self.assertEqual(self.api.delete_instance('games', instance), None)

    def test_delete_instance_04(self):
        database_interface.test_delete_instance_01(self,
                                                   collect_counts,
                                                   game_number_to_record_number)

    def test_delete_instance_05(self):
        database_interface.test_delete_instance_02(self,
                                                   collect_counts,
                                                   game_number_to_record_number)


class Sqlite3api_edit_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('edit_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
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
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 2
        instance.newrecord = edited
        self.api.open_context()
        self.assertRaisesRegex(
            KeyError,
            "'hhhhhh'",
            self.api.edit_instance,
            *('games', instance))

    def test_edit_instance_04(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 2
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance.newrecord = edited
        self.assertRaisesRegex(
            KeyError,
            "'hhhhhh'",
            self.api.edit_instance,
            *('games', instance))

    def test_edit_instance_05(self):
        instance = record.Record(record.KeyData, record.Value)
        instance.key.recno = 2
        edited = record.Record(record.KeyData, _ValueEdited)
        edited.key.recno = 3
        self.api.open_context()
        self.api.put_instance('games', instance)
        instance.newrecord = edited
        self.assertRaisesRegex(
            KeyError,
            "'hhhhhh'",
            self.api.edit_instance,
            *('games', instance))

    def test_edit_instance_06(self):
        database_interface.test_edit_instance_06(self,
                                                 collect_counts,
                                                 game_number_to_record_number)

    def test_edit_instance_07(self):
        database_interface.test_edit_instance_07(self,
                                                 collect_counts,
                                                 game_number_to_record_number)


class Sqlite3api_put_instance_with_holes(unittest.TestCase):

    def setUp(self):

        # DPT segment size is 65280 (8k bytes - 32 reserved) while apsw, bsddb3,
        # and sqlite3, segment size is 32768 (4k bytes).  Reuse record number
        # tests cannot produce the same answer in general on the two segment
        # sizes, even if the DPT algorithm deciding when to fill a hole is the
        # one used for apsw, bsddb3, and sqlite3.
        # The api() function calls samplefilespec with the default fileorg
        # argument because DPT is not the database engine.
        self.dpt_reuse_record_numbers = False
        
        self.api = api('put_instance_holes')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('put_instance_holes')

    def test_holes_01(self):
        database_interface.test_holes_01(self,
                                         collect_counts,
                                         game_number_to_record_number)


def game_number_to_record_number(game_number):
    # Record numbers start at 1 and game numbers start at 1
    return game_number


def record_number_to_game_number(record_number):
    # Record numbers start at 1 and game numbers start at 1
    return record_number


def collect_counts(testcase,
                   record_adder,
                   data_adder,
                   answers,
                   counts,
                   segments,
                   recordsets,
                   rowexceptions):
    db_directory = testcase.api.get_database_folder()
    conn = sqlite3.connect(
        os.path.join(db_directory, os.path.split(db_directory)[-1]))
    cursor = conn.cursor()
    filespec = samplefilespec()
    primaries = {filespec[t][PRIMARY]:t for t in filespec}
    for t in answers['data']:
        testcase.assertEqual(t in primaries, True)
        segments[t] = dict()
        rowexceptions[t] = dict()
        cursor.execute(t.join(('select * from ', '')))
        while True:
            r = cursor.fetchone()
            if not r:
                break
            # Allow for place holder null record for reuse record number
            if r[1] is None:
                continue
            data_adder(rowexceptions[t],
                       segments[t],
                       (r[0], r[1]),
                       answers['data'][t]['defaultrow'],
                       record_number_to_game_number)
    for t in answers['data']:
        for u in answers['records']:
            if u not in filespec[primaries[t]][SECONDARY]:
                continue
            segments[u] = dict()
            recordsets[u] = dict()
            cursor.execute(u.join(('select * from Game_', '')))
            segment_cursor = conn.cursor()
            while True:
                r = cursor.fetchone()
                if not r:
                    break
                segbase = r[1] * SegmentSize.db_segment_size
                if r[2] == 1:
                    record_adder(recordsets[u],
                                 segments[u],
                                 (r[0], segbase + r[3]),
                                 record_number_to_game_number)
                    continue
                segment_cursor.execute(
                    ''.join(('select * from sgGame where rowid = ',
                             str(r[3]))))
                while True:
                    sr = segment_cursor.fetchone()
                    if not sr:
                        break
                    if len(sr[0]) == SegmentSize.db_segment_size_bytes:
                        for e, b in enumerate(sr[0]):
                            if not b:
                                continue
                            for eb, bitvalue in enumerate(
                                (128, 64, 32, 16, 8, 4, 2, 1)):
                                if bitvalue & b:
                                    record_adder(
                                        recordsets[u],
                                        segments[u],
                                        (r[0], segbase + eb + e * 8),
                                        record_number_to_game_number)
                    else:
                        rnl = sr[0]
                        for i in range(0, len(rnl), 2):
                            record_adder(
                                recordsets[u],
                                segments[u],
                                (r[0],
                                 segbase + int.from_bytes(rnl[i:i+2], 'big')),
                                record_number_to_game_number)
            del segment_cursor
    del cursor
    conn.close()


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    
    runner().run(loader(_DatabaseEncoders))

    # The Database class tests.
    
    runner().run(loader(Database_init))

    runner().run(loader(Database_open_context))
    runner().run(loader(Database_close_context))
    runner().run(loader(Database_close_database))
    runner().run(loader(Database_get_database_filenames))

    runner().run(loader(Database_backout))
    runner().run(loader(Database_close_contexts))
    runner().run(loader(Database_commit))
    runner().run(loader(Database_db_compatibility_hack))
    runner().run(loader(Database_exists))
    runner().run(loader(Database_files_exist))
    runner().run(loader(Database_database_cursor))
    runner().run(loader(Database_repair_cursor))
    runner().run(loader(Database_get_database_folder))
    runner().run(loader(Database_get_database))
    runner().run(loader(Database_get_database_instance))
    runner().run(loader(Database_get_first_primary_key_for_index_key))
    runner().run(loader(Database_get_primary_record_ok))
    runner().run(loader(Database_get_primary_record_fail))
    runner().run(loader(Database_is_primary))
    runner().run(loader(Database_is_primary_recno))
    runner().run(loader(Database_is_recno))
    runner().run(loader(Database_open_contexts))
    runner().run(loader(Database_allocate_and_open_contexts))
    runner().run(loader(Database_get_packed_key))
    runner().run(loader(Database_decode_as_primary_key))
    runner().run(loader(Database_encode_primary_key))
    runner().run(loader(Database_use_deferred_update_process))
    runner().run(loader(Database_initial_database_size))
    runner().run(loader(Database_increase_database_size))
    runner().run(loader(Database_do_database_task))
    runner().run(loader(Database_make_recordset_key))
    runner().run(loader(Database_make_recordset_key_startswith))
    runner().run(loader(Database_make_recordset_key_range))
    runner().run(loader(Database_make_recordset_all))
    runner().run(loader(Database_file_records_under_01))
    runner().run(loader(Database_file_records_under_02))
    runner().run(loader(Database_start_transaction))
    runner().run(loader(Database_cede_contexts_to_process))
    
    runner().run(loader(Database_set_defer_update))
    runner().run(loader(Database_unset_defer_update))
    runner().run(loader(Database_do_deferred_updates))

    # The Sqlite3api class tests.  These take a total of about 6 minutes.

    runner().run(loader(Sqlite3api_init))
    runner().run(loader(Sqlite3api_make_connection))

    runner().run(loader(Sqlite3api_put_instance))
    runner().run(loader(Sqlite3api_delete_instance))
    runner().run(loader(Sqlite3api_edit_instance))
    
    runner().run(loader(Sqlite3api_put_instance_with_holes))

    # At 17 March 2016, FreeBSD 10.1 with portsnap dated 7 February 2016, and
    # 17 August 2017, FreeBSD 10.3 with portsnap dated 8 April 2017, the test
    # Sqlite3api_delete_instance.test_delete_instance_05 usually failed
    # 'sqlite3.OperationalError: disk I/O error' at different points in
    # processing from run to run.  This test passed sometimes.
    # At 26 August 2017 the tests were changed so the apsw, sqlite3, bsddb3,
    # and dptdb, versions of the tests used the 'database_interface.py' common
    # code rather than separate copies.  It was immediately apparent that the
    # OperationalError exception was not happening any more, well at least for
    # three consecutive runs which had not been seen before.
    # I do not know why the change has this effect: but leave these comments
    # in case the problem comes back somehow.

    # Test Sqlite3api_put_instance_with_holes.test_holes_01 starts with the
    # code in Sqlite3api_delete_instance.test_delete_instance_05 but has never
    # suffered this error.

    # The equivalent tests for apsw do not suffer the error.
