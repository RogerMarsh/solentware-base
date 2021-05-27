# dbapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dbapi tests using a realistic FileSpec and the real methods defined in the
class hierarchy.  The delete_instance, edit_instance, and put_instance, tests
take a few minutes because each needs a few tens of thousands of records to see
the effects tested.
"""

import unittest
import os
import shutil
import subprocess
import ast

from bsddb3.db import (
    DB_CREATE,
    DB_INIT_MPOOL,
    DB_INIT_LOCK,
    DB_INIT_LOG,
    DB_INIT_TXN,
    DB, DB_RDONLY,
    DBEnv,
    DB_RECNO,
    DB_BTREE,
    )

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
from .. import dbapi
from ..api import (
    database, filespec, record, recordset, constants, primary, secondary)
from ..api.segmentsize import SegmentSize
from . import database_interface


def base(folder, environment=None):
    if environment is None:
        environment = {'flags': DB_CREATE | DB_INIT_TXN | DB_INIT_MPOOL}
    return dbapi.Database(
        dbapi.Primary,
        dbapi.Secondary,
        samplefilespec(),
        os.path.expanduser(
            os.path.join('~', 'dbapi_tests', folder)),
        environment)


def api(folder):
    return dbapi.DBapi(
        samplefilespec(),
        os.path.expanduser(
            os.path.join('~', 'dbapi_tests', folder)),
        {'flags': (DB_CREATE |
                   DB_INIT_MPOOL |
                   DB_INIT_LOCK |
                   DB_INIT_LOG |
                   DB_INIT_TXN)})


def _delete_folder(folder):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'dbapi_tests', folder)),
        ignore_errors=True)


class SampleRecordAssumptions(unittest.TestCase):

    def setUp(self):
        self.sr = SampleRecord()
        self.sv = SampleValue()

    def tearDown(self):
        self.sr = None
        self.sv = None

    def test____assumptions_01(self):
        self.assertEqual(len(self.sr.__dict__), 8)
        self.assertIsInstance(self.sr.key, record.KeyData)
        self.assertIsInstance(self.sr.value, record.Value)
        self.assertIsInstance(self.sr.value, SampleValue)
        self.assertEqual(self.sr.record, None)
        self.assertEqual(self.sr.database, None)
        self.assertEqual(self.sr.dbname, None)
        self.assertEqual(self.sr.srkey, None)
        self.assertEqual(self.sr.srvalue, None)
        self.assertEqual(self.sr.srindex, None)
        self.assertEqual(len(self.sv.__dict__), 0)
        self.assertIsInstance(self.sv, record.Value)
        self.assertIsInstance(self.sv, SampleValue)

    def test____assumptions_02(self):
        self.assertRaisesRegex(
            ValueError,
            "malformed node or string: \[\]",
            self.sr.load_record,
            *((0, []),))
        self.assertRaisesRegex(
            TypeError,
            "__dict__ must be set to a dictionary, not a 'list'",
            self.sr.load_record,
            *((0, '[]'),))
        self.sr.load_record((0, '{}'))
        self.assertEqual(len(self.sr.key.__dict__), 1)
        self.assertEqual(self.sr.key.recno, 0)
        self.assertEqual(len(self.sr.value.__dict__), 0)

    def test____assumptions_03(self):
        self.sr.load_record((0, repr({})))
        self.assertEqual(len(self.sr.value.__dict__), 0)

    def test____assumptions_04(self):
        # This happens, but will not work in database.
        self.sr.load_record(('0', '{}'))
        self.assertEqual(self.sr.key.recno, b'0')

    def test____assumptions_05(self):
        # This happens, but will not work in database.
        self.sr.load_record(([], '{}'))
        self.assertEqual(self.sr.key.recno, [])

    def test____assumptions_06(self):
        self.assertEqual(SEVEN_TAG_ROSTER,
                         (EVENT_FIELD_DEF,
                          SITE_FIELD_DEF,
                          DATE_FIELD_DEF,
                          ROUND_FIELD_DEF,
                          WHITE_FIELD_DEF,
                          BLACK_FIELD_DEF,
                          RESULT_FIELD_DEF,
                          ))
        value = {GAME_FIELD_DEF:'game score',
                 WHITE_FIELD_DEF:'A Jones',
                 BLACK_FIELD_DEF:'B Smith',
                 EVENT_FIELD_DEF:'Match',
                 ROUND_FIELD_DEF:'1',
                 DATE_FIELD_DEF:'today',
                 RESULT_FIELD_DEF:'1-0',
                 SITE_FIELD_DEF:'club venue',
                 'attribute':None,
                 }
        self.sr.load_record((0, repr(value)))
        self.assertEqual(self.sr.value.__dict__, value)
        pv = self.sr.value.pack()
        # pv[0] is not necessarely equal to repr(value)
        self.assertEqual(ast.literal_eval(pv[0]),
                         ast.literal_eval(repr(value)))
        self.assertEqual(pv[1],
                         {WHITE_FIELD_DEF:['A Jones'],
                          BLACK_FIELD_DEF:['B Smith'],
                          EVENT_FIELD_DEF:['Match'],
                          ROUND_FIELD_DEF:['1'],
                          DATE_FIELD_DEF:['today'],
                          RESULT_FIELD_DEF:['1-0'],
                          SITE_FIELD_DEF:['club venue'],
                          })

    def test____assumptions_07(self):
        value = {WHITE_FIELD_DEF:'A Jones',
                 BLACK_FIELD_DEF:'B Smith',
                 }
        self.sr.load_record((0, repr(value)))
        pv = self.sr.value.pack()
        self.assertEqual(pv[1],
                         {WHITE_FIELD_DEF:['A Jones'],
                          BLACK_FIELD_DEF:['B Smith'],
                          })


class _DatabaseEncoders(unittest.TestCase):

    def setUp(self):
        self.sde = dbapi._DatabaseEncoders()

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(repr(57), '57')

    def test_encode_record_number_01(self):
        self.assertEqual(self.sde.encode_record_number(57), b'57')

    def test_encode_record_number_02(self):
        self.assertEqual(self.sde.encode_record_number('ab'), b"'ab'")

    def test_decode_record_number_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'int' object has no attribute 'decode'",
            self.sde.decode_record_number,
            57)

    def test_decode_record_number_02(self):
        self.assertEqual(self.sde.decode_record_number(b'57'), 57)

    def test_encode_record_selector(self):
        self.assertEqual(self.sde.encode_record_selector('57'), b'57')

    def test_encode_record_key_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'encode'",
            self.sde.encode_record_key,
            *(None,))

    def test_encode_record_key_02(self):
        self.assertEqual(self.sde.encode_record_key('key'), b'key')

    def test_encode_record_key_03(self):
        self.assertEqual(
            self.sde.encode_record_key(repr(('d', 'e', 'f'))),
            b"('d', 'e', 'f')")


class Database_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        p = os.path.abspath('')
        ba = dbapi.Database(None, None, {}, '', {})
        self.assertEqual(ba._dbdef, {})
        self.assertEqual(ba._dbservices, None)
        self.assertEqual(ba._dbspec, {})
        self.assertEqual(ba._home_directory, p)
        self.assertEqual(ba._home, os.path.join(p, os.path.split(p)[-1]))
        self.assertEqual(ba._control, None)
        self.assertEqual(ba._control_file, '___control')
        self.assertEqual(ba._dbtxn, None)
        self.assertEqual(ba._DBenvironment, {})
        self.assertEqual(len(ba.__dict__), 9)

    def test___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 5 required positional arguments: ",
                "'primary_class', 'secondary_class', ",
                "'database_specification', 'databasefolder', and ",
                "'DBenvironment'"
                )),
            dbapi.Database,
            )

    def test___init___03(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database folder name None is not valid",
            dbapi.Database,
            *(None, None, None, None, None))

    def test___init___04(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database specification must be a dictionary",
            dbapi.Database,
            *(None, None, None, '', None))

    def test___init___05(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 't' must be a dictionary",
            dbapi.Database,
            *(None, None, dict(t=None), '', None))

    def test___init___06(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for 't' must contain a primary name",
            dbapi.Database,
            *(None, None, dict(t={}), '', None))

    def test___init___07(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            ''.join(("Field definitions must be present in specification ",
                     "for primary fields")),
            dbapi.Database,
            *(None,
              None,
              dict(t=dict(primary='key')),
              '',
              None))

    def test___init___08(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Primary name key for t must be in fields definition",
            dbapi.Database,
            *(None,
              None,
              dict(t=dict(primary='key', fields=('lock',))),
              '',
              None))

    def test___init___09(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            dbapi.Database,
            *(None,
              None,
              filespec.FileSpec(t=dict(primary='key', fields=('lock', 'key'))),
              '',
              None))

    def test___init___10(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "Specification for t must have a DD name",
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
              secondary.Secondary,
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
            dbapi.Database,
            *(primary.Primary,
              secondary.Secondary,
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
            dbapi.Database(
                primary.Primary,
                secondary.Secondary,
                filespec.FileSpec(
                    t=dict(
                        primary='key',
                        fields=dict(lock={}, key={}),
                        filedesc=dict(fileorg=None),
                        ddname='G',
                        file='H')),
                '',
                None),
            dbapi.Database)

    def test___init___14(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'items'",
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
            dbapi.Database,
            *(primary.Primary,
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
        api = dbapi.Database(
            dbapi.Primary,
            dbapi.Secondary,
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
        self.assertIsInstance(api._dbdef['t'], dbapi.Definition)
        df = api._dbdef['t']
        self.assertEqual(df._dbset, 't')
        self.assertEqual(df.dbname_to_secondary_key, {'skey': 'Skey'})
        self.assertIsInstance(df.primary, dbapi.Primary)
        self.assertIsInstance(df.secondary['Skey'], dbapi.Secondary)
        self.assertEqual(len(df.secondary), 1)
        self.assertEqual(len(df.__dict__), 4)

    def test___init___22(self):
        # Differences between _sqlite and dbapi
        api = dbapi.Database(
            dbapi.Primary,
            dbapi.Secondary,
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
        self.assertEqual(api._control, None)
        self.assertEqual(api._control_file, '___control')
        self.assertEqual(len(api.__dict__), 9)


class Database_open_context(unittest.TestCase):

    def setUp(self):
        self.api = base('open_context')

    def tearDown(self):
        _delete_folder('open_context')

    def test_open_context_01(self):
        self.assertEqual(self.api._dbservices, None)
        self.assertEqual(os.path.exists(self.api._home_directory), False)
        self.assertEqual(self.api.open_context(), True)
        self.assertEqual(os.path.isdir(self.api._home_directory), True)
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)


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
        self.assertEqual(self.api.open_context(), True)
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)
        self.assertEqual(self.api.close_context(), None)
        self.assertEqual(self.api._dbservices, None)


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
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)
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
                         sorted(['___control',
                                 'games_Event',
                                 'games_Round',
                                 'games_Date',
                                 'games_White',
                                 'games_Name',
                                 'Game',
                                 'Game__exist',
                                 'Game__list',
                                 'Game__bits',
                                 'games_Black',
                                 'games_Result',
                                 'games_Site',
                                 'log.0000000001',
                                 ]))


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
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)
        self.api.start_transaction()
        self.assertEqual(self.api.backout(), None)
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)


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
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)
        self.api.start_transaction()
        self.assertEqual(self.api.commit(), None)
        self.assertEqual(self.api._dbservices.__class__, DBEnv().__class__)


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
        # ('key', None) should not happen in Berkeley DB, but it is passed back
        # by db_compatibility_hack unchanged to fit what would have occurred
        # without the intervention.
        record = ('key', None)
        value = self.api.decode_record_number(self.srkey)
        self.assertEqual(
            self.api.db_compatibility_hack(record, self.srkey),
            ('key', None))


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

    def test_database_cursor_01(self):
        self.assertRaisesRegex(
            KeyError,
            "None",
            self.api.database_cursor,
            *(None, None))

    def test_make_cursor_02(self):
        self.assertIsInstance(
            self.api.database_cursor('games', 'Site'),
            dbapi.CursorSecondary)

    def test_make_cursor_03(self):
        self.assertIsInstance(
            self.api.database_cursor('games', 'games'),
            dbapi.CursorPrimary)


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
                os.path.join('~', 'dbapi_tests', 'get_database_folder')),
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
        self.assertEqual(
            self.api.get_database('games', 'Site').__class__,
            DB().__class__)
        self.assertEqual(
            self.api.get_database('games', 'games').__class__,
            DB().__class__)


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
            dbapi.Secondary)
        self.assertIsInstance(
            self.api.get_database_instance('games', 'games'),
            dbapi.Primary)


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
            "'NoneType' object is not subscriptable",
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
        self.assertRaisesRegex(
            TypeError,
            "Bytes keys not allowed for Recno and Queue DB's",
            self.api.get_primary_record,
            *('games', b'k'))

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
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
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
        for v in self.api._dbdef.values():
            v.open_contexts = lambda a : None

    def tearDown(self):
        _delete_folder('open_contexts')

    def test_open_contexts_01(self):
        self.assertEqual(self.api.open_contexts('DPT compatibility'), None)


class Database_allocate_and_open_contexts(unittest.TestCase):

    def setUp(self):
        self.api = base('allocate_and_open_contexts')
        for v in self.api._dbdef.values():
            v.allocate_and_open_contexts = lambda a : None

    def tearDown(self):
        _delete_folder('allocate_and_open_contexts')

    def test_allocate_and_open_contexts_01(self):
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
            self.api.get_packed_key('Berkeley DB compatibility',
                                    instance).decode(),
            repr(instance.key.pack()))


class Database_decode_as_primary_key(unittest.TestCase):

    def setUp(self):
        self.api = base('decode_as_primary_key')

    def tearDown(self):
        _delete_folder('decode_as_primary_key')

    def test_decode_as_primary_key_01(self):
        self.assertRaisesRegex(
            AttributeError,
            "'int' object has no attribute 'decode'",
            self.api.decode_as_primary_key,
            *('Ignored in dbapi', 10))

    def test_decode_as_primary_key_02(self):
        srkey = self.api.encode_record_number(3456)
        self.assertEqual(self.api.decode_as_primary_key(
            'Ignored in dbapi', srkey), 3456)


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
            b'23')


class Database_use_deferred_update_process(unittest.TestCase):

    def setUp(self):
        self.api = base('use_deferred_update_process')

    def tearDown(self):
        _delete_folder('use_deferred_update_process')

    def test_unset_defer_update_01(self):
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

    def test_initial_database_size_01(self):
        self.assertEqual(
            self.api.increase_database_size(**{'l':'DPT compatibility'}), None)


class Database_do_database_task(unittest.TestCase):

    def setUp(self):
        self.api = base('do_database_task')

    def tearDown(self):
        _delete_folder('do_database_task')

    def test_do_database_task_01(self):
        def f(a, b, **c):
            pass
        self.assertEqual(self.api.do_database_task(f), None)

    # The previous one works so this scaffolding is not needed, and the real
    # procedure does not do it.  Different in sqlite3api and apswapi.
    def test_do_database_task_02(self):
        def f(a, b, **c):
            pass
        class S(secondary.Secondary):
            def set_primary_database(self, *a):
                pass
        class C(dbapi.Database):
            def __init__(self, *a, **k):
                super().__init__(
                    primary.Primary,
                    S,
                    samplefilespec(),
                    *a,
                    **k)
        self.api = C(
            os.path.expanduser(
                os.path.join('~', 'sqlite3api_tests', 'do_database_task')),
            {})
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
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            self.api.file_records_under,
            *('games', 'Site', self.rs, None))

    def test_file_records_under_05(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'cursor'",
            self.api.file_records_under,
            *('games', 'Site', self.rs, b'dd'))


class Database_file_records_under_02(unittest.TestCase):

    def setUp(self):
        self.api = api('file_records_under_02')
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
            database.DatabaseError,
            "Record set was not created from this database instance",
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
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'txn_begin'",
            self.api.start_transaction)

    def test_start_transaction_02(self):
        self.api.open_context()
        self.assertEqual(self.api.start_transaction(), None)
        # To avoid a RuntimeWarning message about exiting while Txn active.
        self.api.backout()


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
        for v in self.api._dbdef.values():
            v.close = lambda : None

    def tearDown(self):
        _delete_folder('set_defer_update')

    def test_set_defer_update_01(self):
        self.assertEqual(
            self.api.set_defer_update(duallowed='kkk'),
            'kkk')


class Database_unset_defer_update(unittest.TestCase):

    def setUp(self):
        self.api = base('unset_defer_update')
        self.api._DBenvironment = {'flags': (DB_CREATE |
                                             DB_INIT_MPOOL |
                                             DB_INIT_LOCK |
                                             DB_INIT_LOG |
                                             DB_INIT_TXN)}
        for v in self.api._dbdef.values():
            v.open_root = lambda a : None

    def tearDown(self):
        _delete_folder('unset_defer_update')

    def test_unset_defer_update_01(self):
        self.assertEqual(
            self.api.unset_defer_update(),
            True)


class Database_do_deferred_updates(unittest.TestCase):

    def setUp(self):
        self.api = base('do_deferred_updates')

    def tearDown(self):
        _delete_folder('do_deferred_updates')

    def test_do_deferred_updates_01(self):
        self.assertRaisesRegex(
            dbapi.DBapiError,
            "'pyscript' is not an existing file",
            self.api.do_deferred_updates,
            *('pyscript', 'filepath'))
        self.assertRaisesRegex(
            dbapi.DBapiError,
            ''.join(("'/home/roger/dbapi_tests/do_deferred_updates' is ",
                     "not an existing file",
                     )),
            self.api.do_deferred_updates,
            *(self.api._home_directory, 'filepath'))
        self.assertRaisesRegex(
            dbapi.DBapiError,
            ''.join(("'/home/roger/dbapi_tests/do_deferred_updates' is ",
                     "not an existing file",
                     )),
            self.api.do_deferred_updates,
            *(self.api._home_directory, (self.api._home_directory, 'filepath')))
        script = os.path.join(self.api._home_directory, 'script')
        os.makedirs(self.api._home_directory)
        open(script, 'w').close()
        sp = self.api.do_deferred_updates(script, self.api._home_directory)
        self.assertIsInstance(sp, subprocess.Popen)
        sp.wait()


class DBapi_init(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 3 required positional arguments: ",
                "'database_specification', 'databasefolder', ",
                "and 'DBenvironment'",
                )),
            dbapi.DBapi,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 2 required positional arguments: ",
                "'databasefolder' and 'DBenvironment'",
                )),
            dbapi.DBapi,
            *(samplefilespec(),))
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database folder name None is not valid",
            dbapi.DBapi,
            *(samplefilespec(),),
            **dict(DBenvironment='', databasefolder=None))
        self.assertRaisesRegex(
            database.DatabaseError,
            "Database folder name None is not valid",
            dbapi.DBapi,
            *(samplefilespec(), None, ''))

    def test___init___02(self):
        api = dbapi.DBapi(
            filespec.FileSpec(
                t=dict(
                    primary='key',
                    fields=dict(lock={}, key={}, Skey={}),
                    filedesc=dict(fileorg=None),
                    secondary=dict(skey=None),
                    ddname='G',
                    file='F',
                    )),
            databasefolder='',
            DBenvironment=None)
        p = os.path.abspath('')
        self.assertEqual(api._home_directory, p)
        self.assertEqual(api._home, os.path.join(p, os.path.split(p)[-1]))
        self.assertEqual(api._dbservices, None)
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
        self.assertIsInstance(api._dbdef['t'], dbapi.Definition)
        df = api._dbdef['t']
        self.assertEqual(df._dbset, 't')
        self.assertEqual(df.dbname_to_secondary_key, {'skey': 'Skey'})
        self.assertIsInstance(df.primary, dbapi.Primary)
        self.assertIsInstance(df.secondary['Skey'], dbapi.Secondary)
        self.assertEqual(len(df.secondary), 1)
        self.assertEqual(len(df.__dict__), 4)
        self.assertEqual(len(api.__dict__), 9)
        self.assertIsInstance(
            dbapi.DBapi(samplefilespec(), '', None),
            dbapi.DBapi)
        self.assertIsInstance(
            dbapi.DBapi(database_specification=samplefilespec(),
                           databasefolder='',
                           DBenvironment=None),
            dbapi.DBapi)


class DBapi_make_connection(unittest.TestCase):

    def setUp(self):
        self.api = base('make_connection')

    def tearDown(self):
        _delete_folder('make_connection')

    def test_make_connection_01(self):
        self.assertRaisesRegex(
            database.DatabaseError,
            "not implemented: specific to Sqlite3 interfaces",
            self.api.make_connection)


class DBapi_put_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('put_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('put_instance')

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


class DBapi_delete_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('delete_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('delete_instance')

    def test_delete_instance_04(self):
        database_interface.test_delete_instance_01(self,
                                                   collect_counts,
                                                   game_number_to_record_number)

    def test_delete_instance_05(self):
        database_interface.test_delete_instance_02(self,
                                                   collect_counts,
                                                   game_number_to_record_number)


class DBapi_edit_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('edit_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('edit_instance')

    def test_edit_instance_06(self):
        database_interface.test_edit_instance_06(self,
                                                 collect_counts,
                                                 game_number_to_record_number)

    def test_edit_instance_07(self):
        database_interface.test_edit_instance_07(self,
                                                 collect_counts,
                                                 game_number_to_record_number)


class DBapi_put_instance_with_holes(unittest.TestCase):

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
    list_seg_length = constants.LENGTH_SEGMENT_LIST_REFERENCE
    bits_seg_length = constants.LENGTH_SEGMENT_BITARRAY_REFERENCE
    files = dict(Round='games_Round',
                 White='games_White',
                 Event='games_Event',
                 Site='games_Site',
                 Black='games_Black',
                 Date='games_Date',
                 Result='games_Result',
                 )
    db_directory = testcase.api.get_database_folder()
    dbenv = DBEnv()
    dbenv.open(db_directory)
    filespec = samplefilespec()
    primaries = {filespec[t][PRIMARY]:t for t in filespec}
    for t in answers['data']:
        testcase.assertEqual(t in primaries, True)
        segments[t] = dict()
        rowexceptions[t] = dict()
        db = DB(dbenv)
        db.open(t, dbname=t, flags=DB_RDONLY)
        stat = db.stat()
        testcase.assertEqual(stat['ndata'], answers['record_counts'][t])
        cursor = db.cursor()
        while True:
            r = cursor.next()
            if not r:
                break
            # Allow for place holder null record for reuse record number
            if r[1] is None:
                continue
            data_adder(rowexceptions[t],
                       segments[t],
                       (r[0], r[1].decode()),
                       answers['data'][t]['defaultrow'],
                       record_number_to_game_number)
        cursor.close()
        db.close()
    for t in answers['data']:
        for u in answers['records']:
            if u not in filespec[primaries[t]][SECONDARY]:
                continue
            segments[u] = dict()
            recordsets[u] = dict()
            db = DB(dbenv)
            db.open(files.get(u), dbname=u, flags=DB_RDONLY)
            ubits = ''.join((t, '__bits'))
            dbbits = DB(dbenv)
            dbbits.open(ubits, dbname=ubits, flags=DB_RDONLY)
            ulist = ''.join((t, '__list'))
            dblist = DB(dbenv)
            dblist.open(ulist, dbname=ulist, flags=DB_RDONLY)
            uexist = ''.join((t, '__exist'))
            cursor = db.cursor()
            while True:
                r = cursor.next()
                if not r:
                    break
                rk, rv = r
                if len(r[1]) == list_seg_length:
                    # List of record numbers.
                    srn = int.from_bytes(rv[6:], byteorder='big')
                    seg = int.from_bytes(rv[:4], byteorder='big')
                    rct = int.from_bytes(rv[4:6], byteorder='big')
                    slist = dblist.get(srn)
                    segbase = seg * SegmentSize.db_segment_size
                    for i in range(0, len(slist), 2):
                        record_adder(
                            recordsets[u],
                            segments[u],
                            (rk.decode(),
                             segbase + int.from_bytes(slist[i:i+2], 'big')),
                            record_number_to_game_number)
                elif len(r[1]) == bits_seg_length:
                    # Bitmap of record numbers.
                    srn = int.from_bytes(rv[7:], byteorder='big')
                    seg = int.from_bytes(rv[:4], byteorder='big')
                    rct = int.from_bytes(rv[4:7], byteorder='big')
                    sbits = dbbits.get(srn)
                    segbase = seg * SegmentSize.db_segment_size
                    for e, b in enumerate(sbits):
                        if not b:
                            continue
                        for eb, bitvalue in enumerate(
                            (128, 64, 32, 16, 8, 4, 2, 1)):
                            if bitvalue & b:
                                record_adder(
                                    recordsets[u],
                                    segments[u],
                                    (rk.decode(), segbase + eb + e * 8),
                                    record_number_to_game_number)
                else:
                    # Direct record number.
                    srn = int.from_bytes(rv[4:], byteorder='big')
                    seg = int.from_bytes(rv[:4], byteorder='big')
                    rct = 1
                    segbase = seg * SegmentSize.db_segment_size
                    record_adder(recordsets[u],
                                 segments[u],
                                 (rk.decode(),
                                  seg * SegmentSize.db_segment_size + srn),
                                 record_number_to_game_number)
            cursor.close()
            dblist.close()
            dbbits.close()
            db.close()
    dbenv.close()


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    
    runner().run(loader(SampleRecordAssumptions))
    
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

    # The DBapi class tests. These take a total of nearly 15 minutes.

    runner().run(loader(DBapi_init))
    runner().run(loader(DBapi_make_connection))

    runner().run(loader(DBapi_put_instance))
    runner().run(loader(DBapi_delete_instance))
    runner().run(loader(DBapi_edit_instance))

    runner().run(loader(DBapi_put_instance_with_holes))
