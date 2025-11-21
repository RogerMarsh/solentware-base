# test__lmdb.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_lmdb _database tests."""

import unittest
import os
import shutil

import lmdb

from .. import _lmdb
from .. import filespec
from .. import recordset
from .. import recordsetcursor
from .. import recordsetbasecursor
from ..constants import SECONDARY, CONTROL_FILE
from ..segmentsize import SegmentSize
from ..wherevalues import ValuesClause
from ..bytebit import Bitarray

from .test__lmdb_setUp_tearDown import (
    LMDB_TEST_ROOT,
    HOME,
    DATA,
    LOCK,
    HOME_DATA,
    HOME_LOCK,
    HomeNull,
    HomeExists,
    EnvironmentExists,
    EnvironmentExistsReadOnly,
    EnvironmentExistsOneDb,
    DB,
    DBDir,
    DBExist,
)
from ._test_case_constants import (
    DATABASE_MAKE_RECORDSET_KEYS,
    DATABASE_MAKE_RECORDSET_SEGMENTS,
    database_make_recordset,
)


class _Module:
    def _dbe_module(self):
        return lmdb


class _Specification:
    def set_specification(self, specification=None):
        if specification is None:
            specification = {}
        self.database = self._D(specification)
        self.specification = specification

    def check_specification(self, files=None):
        # files must be same as files argument in "open_database" call.
        # Default is all files in specification.
        if files is None:
            files = set(self.specification.keys())
        d = self.database
        specset = set()
        tableset = set(("___control", "___design"))
        filespecinst = isinstance(self.specification, filespec.FileSpec)
        for key, values in self.specification.items():
            if key not in files:
                continue
            specset.add(key)
            tableset.add(key)
            if not filespecinst:
                for value in values:
                    tableset.add("_".join((key, value)))
            else:
                for value in values[SECONDARY]:
                    tableset.add("_".join((key, value)))
        self.assertEqual(set(d.table), tableset)
        self.assertEqual(set(d.segment_table), specset)
        self.assertEqual(set(d.ebm_control), specset)
        for v in d.ebm_control.values():
            self.assertIsInstance(v, _lmdb.ExistenceBitmapControl)
        c = 0
        o = set()
        for t in (
            d.table,
            d.segment_table,
        ):
            for v in t.values():
                if isinstance(v, list):
                    for i in v:
                        self.assertEqual(i.__class__.__name__, "_Datastore")
                        c += 1
                        o.add(i)
                else:
                    self.assertEqual(v.__class__.__name__, "_Datastore")
                    c += 1
                    o.add(v)
        for t in (d.ebm_control,):
            for v in t.values():
                self.assertEqual(v.ebm_table.__class__.__name__, "_Datastore")
                c += 1
                o.add(v)
        self.assertEqual(c, len(o))


class _DBtxn(unittest.TestCase):
    def test_01___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            _lmdb._DBtxn,
            *(None,),
        )

    def test_01___init___02(self):
        txn = _lmdb._DBtxn()
        keys = txn.__dict__.keys()
        self.assertEqual(len(keys), 2)
        self.assertEqual(
            sorted(keys), sorted(("_write_requested", "_transaction"))
        )
        self.assertEqual(txn._write_requested, False)
        self.assertEqual(txn._transaction, None)

    def test_02_transaction_01(self):
        txn = _lmdb._DBtxn()
        self.assertIs(txn.transaction, txn._transaction)

    def test_03_start_transaction_01(self):
        txn = _lmdb._DBtxn()
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_transaction\(\) missing 2 required positional ",
                    "arguments: 'dbenv' and 'write'$",
                )
            ),
            txn.start_transaction,
        )

    def test_04_end_transaction_01(self):
        txn = _lmdb._DBtxn()
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"end_transaction\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            txn.end_transaction,
            *(None,),
        )

    def test_05_end_transaction_01(self):
        txn = _lmdb._DBtxn()
        txn._transaction = True
        txn.end_transaction()
        self.assertEqual(txn._write_requested, False)
        self.assertEqual(txn._transaction, None)
        self.assertEqual(
            sorted(txn.__dict__.keys()),
            sorted(("_write_requested", "_transaction")),
        )

    def test_05_end_transaction_02(self):
        txn = _lmdb._DBtxn()
        txn._write_requested = True
        txn.end_transaction()
        self.assertEqual(txn._write_requested, True)
        self.assertEqual(txn._transaction, None)
        self.assertEqual(
            sorted(txn.__dict__.keys()),
            sorted(("_write_requested", "_transaction")),
        )


class _DBtxn_start_transaction(_Module, EnvironmentExists):
    def test_01_start_transaction_01(self):
        txn = _lmdb._DBtxn()
        txn.start_transaction(self.env, False)
        self.assertEqual(txn._write_requested, False)
        self.assertIsInstance(txn._transaction, self.dbe_module.Transaction)
        self.assertEqual(
            sorted(txn.__dict__.keys()),
            sorted(("_write_requested", "_transaction")),
        )

    def test_01_start_transaction_02(self):
        txn = _lmdb._DBtxn()
        txn.start_transaction(self.env, True)
        self.assertEqual(txn._write_requested, True)
        self.assertIsInstance(txn._transaction, self.dbe_module.Transaction)
        self.assertEqual(
            sorted(txn.__dict__.keys()),
            sorted(("_write_requested", "_transaction")),
        )

    def test_01_start_transaction_03(self):
        # bool(txn._write_requested) is what matters.
        txn = _lmdb._DBtxn()
        txn.start_transaction(self.env, 5)
        self.assertEqual(txn._write_requested, 5)
        self.assertIsInstance(txn._transaction, self.dbe_module.Transaction)
        self.assertEqual(
            sorted(txn.__dict__.keys()),
            sorted(("_write_requested", "_transaction")),
        )


class _Datastore___init__(unittest.TestCase):
    def test_01___init___01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes 2 positional arguments ",
                    "but 3 were given$",
                )
            ),
            _lmdb._Datastore,
            *(None, None),
        )

    def test_01___init___02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'datastorename'$",
                )
            ),
            _lmdb._Datastore,
        )

    def test_01___init___03(self):
        store = _lmdb._Datastore(None, a=None, b=None)
        self.assertEqual(store._name, None)
        self.assertEqual(store._flags, dict(a=None, b=None))
        self.assertEqual(store._datastore, None)
        self.assertEqual(
            sorted(store.__dict__.keys()),
            sorted(("_name", "_flags", "_datastore")),
        )
        self.assertIs(store.datastore, store._datastore)


class _Datastore(unittest.TestCase):
    def setUp(self):
        self.store = _lmdb._Datastore(b"store")

    def test_01_open_datastore_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"open_datastore\(\) takes from 2 to 3 positional ",
                    "arguments but 4 were given$",
                )
            ),
            self.store.open_datastore,
            *(None, None, None),
        )


class _Datastore_open_datastore(_Module, EnvironmentExistsOneDb):
    def setUp(self):
        super().setUp()
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        self.env.close()

    def tearDown(self):
        super().tearDown()

    def test_01_open_datastore_01(self):
        # LMDB_TEST_ROOT exists so a read-only transaction can be started
        # straight away in the new environment.
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        self.assertEqual(store._name, b"store")
        self.assertEqual(store._flags, {})

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: no attribute.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # self.assertEqual(store._datastore, self.dbe_module._Database)
        self.assertEqual(store._datastore is None, False)
        self.assertEqual(
            sorted(store.__dict__.keys()),
            sorted(("_name", "_flags", "_datastore")),
        )

    def test_01_open_datastore_02(self):
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        spare = _lmdb._Datastore(b"spare")

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: message mismatch.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # Dated 2025-08-01.
        # Part of the exception message may be enclosed in "b''", possibly
        # aligned with the datastore objects being lmdb.cffi._Database or
        # lmdb._Database instances.
        self.assertRaisesRegex(
            self.dbe_module.DbsFullError,
            "".join(
                (
                    r"mdb_dbi_open: (?:b')?MDB_DBS_FULL: Environment ",
                    r"maxdbs limit reached(?:' ",
                    r"\(Please use a larger Environment\(max_dbs=\) ",
                    r"parameter\))?$",
                )
            ),
            spare.open_datastore,
            *(self.env,),
        )


class _Datastore_open_datastore_write(_Module, EnvironmentExistsOneDb):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_01_open_datastore_write_01(self):
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)

    def test_01_open_datastore_write_02(self):
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        txn = self.env.begin()
        self.assertRaisesRegex(
            AssertionError,
            "",
            store.open_datastore,
            *(self.env,),
        )

    def test_01_open_datastore_write_03(self):
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        store._datastore = None
        txn = self.env.begin()
        store.open_datastore(self.env)


class _Datastore_close_datastore(_Module, EnvironmentExistsOneDb):
    def setUp(self):
        super().setUp()
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        self.env.close()

    def tearDown(self):
        super().tearDown()

    def test_01_release_datastore_handle_01(self):
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        store.close_datastore()
        self.assertEqual(store._name, b"store")
        self.assertEqual(store._flags, {})
        self.assertEqual(store._datastore, None)
        self.assertEqual(
            sorted(store.__dict__.keys()),
            sorted(("_name", "_flags", "_datastore")),
        )

    def test_01_release_datastore_handle_02(self):
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        store.close_datastore()
        store.open_datastore(self.env)

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: no attribute.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # self.assertIsInstance(store._datastore, self.dbe_module._Database)
        self.assertEqual(store._datastore is None, False)

    def test_01_release_datastore_handle_03(self):
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)
        store = _lmdb._Datastore(b"store")
        store.open_datastore(self.env)
        store.close_datastore()
        store.open_datastore(self.env)
        spare = _lmdb._Datastore(b"spare")

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: message mismatch.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # Dated 2025-08-01.
        # Part of the exception message may be enclosed in "b''", possibly
        # aligned with the datastore objects being lmdb.cffi._Database or
        # lmdb._Database instances.
        self.assertRaisesRegex(
            self.dbe_module.DbsFullError,
            "".join(
                (
                    r"mdb_dbi_open: (?:b')?MDB_DBS_FULL: Environment ",
                    r"maxdbs limit reached(?:' ",
                    r"\(Please use a larger Environment\(max_dbs=\) ",
                    r"parameter\))?$",
                )
            ),
            spare.open_datastore,
            *(self.env,),
        )


class Database___init__(DB):
    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) takes from 2 to 3 positional arguments ",
                    "but 4 were given$",
                )
            ),
            self._D,
            *(None, None, None),
        )

    def test_02(self):
        t = r"(?:type object|solentware_base\.core\.filespec\.FileSpec\(\))"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    t,
                    r" argument after \*\* must be a mapping, ",
                    "not NoneType$",
                )
            ),
            self._D,
            *(None,),
        )
        self.assertIsInstance(self._D({}), self._D)
        self.assertIsInstance(self._D(filespec.FileSpec()), self._D)

    def test_03(self):
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "".join(("Database folder name {} is not valid$",)),
            self._D,
            *({},),
            **dict(folder={}),
        )

    def test_04(self):
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "".join(("Database environment must be a dictionary$",)),
            self._D,
            *({},),
            **dict(environment=[]),
        )

    def test_05(self):
        database = self._D({}, folder="a")
        self.assertIsInstance(database, self._D)
        self.assertEqual(os.path.basename(database.home_directory), "a")
        self.assertEqual(os.path.basename(database.database_file), "a")
        # This code for "subdir==False"
        # self.assertEqual(
        #    os.path.basename(os.path.dirname(database.database_file)), "a"
        # )
        # No code for "subdir==True" because database_file and home_directory
        # are same in this case (see test_06).
        self.assertEqual(database.specification, {})
        self.assertEqual(database.segment_size_bytes, 4000)
        self.assertEqual(database.dbenv, None)
        self.assertEqual(database.table, {})
        self.assertIsInstance(database.dbtxn, _lmdb._DBtxn)
        self.assertEqual(database._dbe, None)
        self.assertEqual(database.segment_table, {})
        self.assertEqual(database.ebm_control, {})
        self.assertEqual(database._real_segment_size_bytes, False)
        self.assertEqual(database._initial_segment_size_bytes, 4000)
        #self.assertEqual(SegmentSize.db_segment_size_bytes, 4096)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)

    def test_06(self):
        database = self._D({})
        self.assertEqual(
            os.path.basename(database.home_directory), "___test_lmdb"
        )
        # This code for "subdir==False"
        self.assertEqual(
            os.path.dirname(database.database_file), database.home_directory
        )
        # This code for "subdir==True"
        # self.assertEqual(
        #    database.database_file, database.home_directory
        # )
        self.assertEqual(
            os.path.basename(database.database_file), "___test_lmdb"
        )

    # This combination of folder and segment_size_bytes arguments is used for
    # unittests, except for one to see a non-memory database with a realistic
    # segment size.
    def test_07(self):
        database = self._D({}, segment_size_bytes=None)
        self.assertEqual(database.segment_size_bytes, None)
        database.set_segment_size()
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)


# Transaction methods, except start_transaction, do not raise exceptions if
# called when no database open but do nothing.
class Database_transaction_bad_calls(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_02_transaction_bad_calls_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_transaction\(\) takes 1 positional ",
                    "argument but 2 were given$",
                )
            ),
            self.database.start_transaction,
            *(None,),
        )

    def test_02_transaction_bad_calls_02(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"backout\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.backout,
            *(None,),
        )

    def test_02_transaction_bad_calls_03(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"commit\(\) takes 1 positional argument ",
                    "but 2 were given$",
                )
            ),
            self.database.commit,
            *(None,),
        )

    def test_02_transaction_bad_calls_04(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"start_read_only_transaction\(\) takes 1 positional ",
                    "argument but 2 were given$",
                )
            ),
            self.database.start_read_only_transaction,
            *(None,),
        )

    def test_02_transaction_bad_calls_04(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"end_read_only_transaction\(\) takes 1 positional ",
                    "argument but 2 were given$",
                )
            ),
            self.database.end_read_only_transaction,
            *(None,),
        )


class Database_start_transaction(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)

    def test_01_start_transaction_01(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, True)

    def test_01_start_transaction_02(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, True)

    def test_01_start_transaction_03(self):
        sdb = self.database
        ae = self.assertEqual
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        sdb.start_read_only_transaction()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertIsInstance(
            sdb.dbtxn._transaction, self.dbe_module.Transaction
        )
        self.assertEqual(sdb.dbtxn._write_requested, False)


class Database_backout_and_commit(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)
        self.database.dbtxn._transaction = self.database.dbenv.begin()

    def test_01_backout_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdb.backout()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertEqual(sdb.dbtxn._transaction, None)
        self.assertEqual(sdb.dbtxn._write_requested, False)

    def test_02_commit_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdb.commit()
        ae(len(sdb.table), 0)
        ae(len(sdb.segment_table), 0)
        ae(len(sdb.ebm_control), 0)
        self.assertEqual(sdb.dbtxn._transaction, None)
        self.assertEqual(sdb.dbtxn._write_requested, False)


class Database_database_contexts_bad_calls(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_01_open_database_contexts_bad_call_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"open_database_contexts\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.database.open_database_contexts,
            *(None, None),
        )

    def test_02_close_database_contexts_bad_call_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close_database_contexts\(\) takes from 1 to 2 ",
                    "positional arguments but 3 were given$",
                )
            ),
            self.database.close_database_contexts,
            *(None, None),
        )


# Methods which do not require database to be open.
class DatabaseInstance(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})

    def test_01_validate_segment_size_bytes(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_validate_segment_size_bytes\(\) missing 1 required ",
                    "positional argument: 'segment_size_bytes'$",
                )
            ),
            self.database._validate_segment_size_bytes,
        )
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "".join(("Database segment size must be an int$",)),
            self.database._validate_segment_size_bytes,
            *("a",),
        )
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "".join(("Database segment size must be more than 0$",)),
            self.database._validate_segment_size_bytes,
            *(0,),
        )
        self.assertEqual(
            self.database._validate_segment_size_bytes(None), None
        )
        self.assertEqual(self.database._validate_segment_size_bytes(1), None)

    def test_02_environment_flags(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"environment_flags\(\) missing 1 required ",
                    "positional argument: 'dbe'$",
                )
            ),
            self.database.environment_flags,
        )
        self.assertEqual(
            self.database.environment_flags(lmdb),
            dict(subdir=False, readahead=False),
        )

    def test_03_encode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_record_number\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.database.encode_record_number,
        )
        self.assertEqual(self.database.encode_record_number(1), b"1")

    def test_04_decode_record_number(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"decode_record_number\(\) missing 1 required ",
                    "positional argument: 'skey'$",
                )
            ),
            self.database.decode_record_number,
        )
        self.assertEqual(self.database.decode_record_number(b"1"), 1)

    def test_05_encode_record_selector(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"encode_record_selector\(\) missing 1 required ",
                    "positional argument: 'key'$",
                )
            ),
            self.database.encode_record_selector,
        )
        self.assertEqual(self.database.encode_record_selector("a"), b"a")

    def test_06_make_recordset(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_nil\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.recordlist_nil,
            *(None, None, None),
        )
        self.assertIsInstance(
            self.database.recordlist_nil("a"), recordset.RecordList
        )


class Database_open_database(DB, _Specification):
    def test_01_open_database(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"open_database\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.open_database,
            *(None, None, None),
        )

    def test_02_open_database(self):
        self.database = self._D({})
        self.database.open_database(self.dbe_module)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
        self.assertEqual(
            self.database.home_directory,
            HOME,
        )
        self.assertEqual(
            self.database.database_file,
            HOME_DATA,
        )
        self.assertEqual(os.path.isfile(HOME_DATA), True)
        self.assertEqual(os.path.isfile(HOME_LOCK), True)
        self.assertEqual(self.database.dbenv.__class__.__name__, "Environment")

    def test_03_open_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(self.dbe_module)
        self.assertEqual(SegmentSize.db_segment_size_bytes, 16)
        self.assertEqual(
            self.database.home_directory,
            HOME,
        )
        self.assertEqual(
            self.database.database_file,
            HOME_DATA,
        )
        self.assertEqual(os.path.isfile(HOME_DATA), True)
        self.assertEqual(os.path.isfile(HOME_LOCK), True)
        self.assertEqual(self.database.dbenv.__class__.__name__, "Environment")

    def test_06_open_database(self):
        self.set_specification(specification={"file1": {"field1"}})
        self.database.open_database(self.dbe_module)
        self.check_specification()

    def test_07_open_database(self):
        self.set_specification(
            specification=filespec.FileSpec(**{"file1": {"field1"}})
        )
        self.database.open_database(self.dbe_module)
        self.check_specification()

    def test_08_open_database(self):
        self.set_specification(
            specification=filespec.FileSpec(
                **{"file1": {"field1"}, "file2": {"field2"}}
            )
        )
        openfiles = {"file1"}
        self.database.open_database(self.dbe_module, files=openfiles)
        self.check_specification(files=openfiles)

    def test_09_open_database(self):
        self.set_specification(
            specification=filespec.FileSpec(
                **{"file1": {"field1"}, "file2": ()}
            )
        )
        self.database.open_database(self.dbe_module)
        self.check_specification()

    # .test__db has test_10_file_name_for_database(self).
    def test_10_encoded_database_name(self):
        self.database = self._D({})
        self.assertEqual(
            self.database._encoded_database_name("file1"), b"file1"
        )

    def test_11_checkpoint_before_close_dbenv(self):
        self.database = self._D(filespec.FileSpec())
        d = self.database
        d.open_database(self.dbe_module)
        self.assertEqual(d.checkpoint_before_close_dbenv(), None)

    # Comment in _lmdb.py suggests this method is not needed.
    def test_12_is_database_file_active(self):
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}, "file2": ()})
        )
        d = self.database
        self.assertRaisesRegex(
            KeyError,
            "'file1'$",
            d.is_database_file_active,
            *("file1",),
        )
        d.open_database(self.dbe_module)
        self.assertEqual(d.is_database_file_active("file1"), True)
        x = d.table["file1"]
        d.table["file1"] = None
        self.assertEqual(d.is_database_file_active("file1"), False)
        d.table["file1"] = x


class Database_add_field_to_existing_database(DB, _Specification):

    def test_13_add_field_to_open_database(self):
        self.set_specification(specification={"file1": {"field1"}})
        self.database.open_database(self.dbe_module)
        self.check_specification()
        self.assertEqual(self.specification, {"file1": {"field1"}})
        self.assertEqual(
            set(self.database.table.keys()),
            set(["___design", "___control", "file1", "file1_field1"]),
        )
        self.database.close_database()
        self.specification["file1"].add("newfield")
        self.database.open_database(self.dbe_module)
        self.assertEqual(self.specification, {"file1": {"newfield", "field1"}})
        self.assertEqual(
            set(self.database.table.keys()),
            set(["___design", "___control", "file1", "file1_field1"]),
        )


class DatabaseDir_open_database(DBDir, _Specification):
    def test_06_open_database_dir(self):
        self.set_specification(specification={"file1": {"field1"}})
        self.database.open_database(self.dbe_module)
        self.check_specification()


class DatabaseExist_open_database(DBExist, _Specification):
    def test_06_open_database_exist(self):
        self.set_specification(specification={"file1": {"field1"}})
        self.database.open_database(self.dbe_module)
        self.check_specification()


class Database_close_database(DB):
    def test_01(self):
        self.database = self._D({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"close_database\(\) takes 1 positional ",
                    "argument but 2 were given$",
                )
            ),
            self.database.close_database,
            *(None,),
        )

    def test_04_close_database(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(self.dbe_module)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)
        self.database.close_database()
        self.assertEqual(self.database.dbenv, None)

    def test_05_close_database_contexts(self):
        self.database = self._D({}, segment_size_bytes=None)
        self.database.open_database(self.dbe_module)
        self.database.close_database_contexts()
        self.assertEqual(
            isinstance(self.database.dbenv, self.dbe_module.Environment),
            False,
        )
        self.database.close_database_contexts()
        self.assertEqual(
            isinstance(self.database.dbenv, self.dbe_module.Environment),
            False,
        )


class Database_open_database_contexts(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({**{"file1": {"field1"}, "file2": {"field2"}}})
        self.database.dbenv = self.dbe_module.open(HOME, max_dbs=7)
        self.database.open_database(self.dbe_module)
        self.database.close_database_context_files()

    def test_01_open_database_contexts_all_files_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdt = self.database.table
        sst = self.database.segment_table
        sec = self.database.ebm_control
        dbem = self.dbe_module

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: no attribute.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # Dated 2025-08-01.
        # The object is either a lmdb.cffi._Database instance or a
        # dbem._Database object.
        # This and all other similar tests are changed to 'is None' tests.
        # ae(isinstance(sdt["___design"].datastore, dbem._Database), False)

        ae(sdt["___design"].datastore is None, True)
        ae(sdt["___control"].datastore is None, True)
        ae(sdt["file1"].datastore is None, True)
        ae(sdt["file1_field1"].datastore is None, True)
        ae(sdt["file2"].datastore is None, True)
        ae(sdt["file2_field2"].datastore is None, True)
        ae(len(sdt), 6)
        ae(sst["file1"].datastore is None, True)
        ae(sst["file2"].datastore is None, True)
        ae(len(sec), 2)
        ae(sec["file1"].ebm_table.datastore is None, True)
        ae(sec["file2"].ebm_table.datastore is None, True)
        ae(len(sst), 2)
        sdb.open_database_contexts()
        ae(sdt["___design"].datastore is None, True)
        ae(sdt["___control"].datastore is None, False)
        ae(sdt["file1"].datastore is None, False)
        ae(sdt["file1_field1"].datastore is None, False)
        ae(sdt["file2"].datastore is None, False)
        ae(sdt["file2_field2"].datastore is None, False)
        ae(len(sdt), 6)
        ae(sst["file1"].datastore is None, False)
        ae(sst["file2"].datastore is None, False)
        ae(len(sec), 2)
        ae(sec["file1"].ebm_table.datastore is None, False)
        ae(sec["file2"].ebm_table.datastore is None, False)
        ae(len(sst), 2)

    def test_02_open_database_contexts_no_files_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdt = self.database.table
        sst = self.database.segment_table
        sec = self.database.ebm_control
        dbem = self.dbe_module
        sdb.open_database_contexts(files=False)

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: no attribute.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # ae(isinstance(sdt["___design"].datastore, dbem._Database), False)
        ae(sdt["___design"].datastore is None, True)

        ae(sdt["___control"].datastore is None, False)
        ae(sdt["file1"].datastore is None, True)
        ae(sdt["file1_field1"].datastore is None, True)
        ae(sdt["file2"].datastore is None, True)
        ae(sdt["file2_field2"].datastore is None, True)
        ae(len(sdt), 6)
        ae(sst["file1"].datastore is None, True)
        ae(sst["file2"].datastore is None, True)
        ae(len(sec), 2)
        ae(sec["file1"].ebm_table.datastore is None, True)
        ae(sec["file2"].ebm_table.datastore is None, True)
        ae(len(sst), 2)

    def test_03_open_database_contexts_one_file_01(self):
        sdb = self.database
        ae = self.assertEqual
        sdt = self.database.table
        sst = self.database.segment_table
        sec = self.database.ebm_control
        dbem = self.dbe_module
        sdb.open_database_contexts(files={"file2"})

        # Dated 2023-07-12.
        # Fails in Python3.9 with FreeBSD port py39-lmdb: no attribute.
        # Ok in Python3.10 on FreeBSD with pip ... --user install.
        # Ok in Python3.10 on OpenBSD with pip ... --user install.
        # ae(isinstance(sdt["___design"].datastore, dbem._Database), False)
        ae(sdt["___design"].datastore is None, True)

        ae(sdt["___control"].datastore is None, False)
        ae(sdt["file1"].datastore is None, True)
        ae(sdt["file1_field1"].datastore is None, True)
        ae(sdt["file2"].datastore is None, False)
        ae(sdt["file2_field2"].datastore is None, False)
        ae(len(sdt), 6)
        ae(sst["file1"].datastore is None, True)
        ae(sst["file2"].datastore is None, False)
        ae(len(sec), 2)
        ae(sec["file1"].ebm_table.datastore is None, True)
        ae(sec["file2"].ebm_table.datastore is None, False)
        ae(len(sst), 2)


# This one has to look like a real application (almost).
# Do not need to catch the self.__class__.SegmentSizeError exception in
# _ED.open_database() method.
class Database_do_database_task(unittest.TestCase):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        class _ED(_lmdb.Database):
            def __init__(self, specification, folder=None, **kwargs):
                super().__init__(
                    specification,
                    folder=folder if folder is not None else HOME,
                    **kwargs,
                )

            def open_database(self, **k):
                super().open_database(lmdb, **k)

        class _AD(_ED):
            def __init__(self, folder, **k):
                super().__init__({}, folder, **k)

        self._AD = _AD

    def tearDown(self):
        if hasattr(self, "database"):
            if hasattr(self.database, "home_directory"):
                if os.path.isdir(self.database.home_directory):
                    if (
                        os.path.basename(self.database.home_directory)
                        == LMDB_TEST_ROOT
                    ):
                        shutil.rmtree(self.database.home_directory)
        self.database = None
        self._AD = None

    def test_01_do_database_task(self):
        def m(*a, **k):
            pass

        self.database = self._AD(None)
        d = self.database
        # At this point LMDB_TEST_ROOT does not exist as a directory.
        # d.open_database() gives 'No such file or directory' exception.
        d.open_database()
        # At this point LMDB_TEST_ROOT exists as a directory.
        # db.open_database() in do_database_task(m)
        # gives 'Not a directory' exception.
        # Not sure what this means yet!
        # Is the open_database() in call below expecting the LMDB_TEST_ROOT
        # file in the LMDB_TEST_ROOT directory to be a directory itself?
        self.assertEqual(d.do_database_task(m), None)


# Use the 'testing only' segment size for convenience of setup and eyeballing.
class _DBOpen(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D(
            filespec.FileSpec(**{"file1": {"field1"}}), segment_size_bytes=None
        )
        self.database.open_database(self.dbe_module)

    def tearDown(self):
        self.database.close_database()
        super().tearDown()


class DatabaseTransactions(_DBOpen):
    # Because both reads and writes must be in transactions there is a case
    # for not allowing bare backout and commit; and perhaps not allowing
    # repeated begin transaction.  This is different from _db, _sqlite, and
    # _nosql, siblings.

    # Second start_transaction does nothing.
    def test_01(self):
        self.database.start_transaction()
        self.database.start_transaction()

    def test_02(self):
        self.database.start_transaction()
        self.database.backout()

    def test_03(self):
        self.database.start_transaction()
        self.database.commit()

    # Bare backout does nothing.
    def test_04(self):
        self.database.backout()

    # Bare commit does nothing.
    def test_05(self):
        self.database.commit()


# All actions must be within a transaction.
class Database_put_replace_delete(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"put\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'$",
                )
            ),
            self.database.put,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"replace\(\) missing 4 required positional arguments: ",
                    "'file', 'key', 'oldvalue', and 'newvalue'$",
                )
            ),
            self.database.replace,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"delete\(\) missing 3 required positional arguments: ",
                    "'file', 'key', and 'value'$",
                )
            ),
            self.database.delete,
        )

    def test_02_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)

    def test_03_put(self):
        self.assertEqual(self.database.put("file1", 2, "new value"), None)
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 3)

    def test_04_put(self):
        recno = self.database.put("file1", None, "new value")
        self.assertEqual(recno, 0)
        self.assertEqual(self.database.put("file1", 1, "renew value"), None)
        recno = self.database.put("file1", None, "other value")
        self.assertEqual(recno, 2)

    def test_05_replace(self):
        self.assertEqual(
            self.database.replace("file1", 1, "new value", "renew value"), None
        )

    def test_06_delete(self):
        self.assertEqual(self.database.delete("file1", 1, "new value"), None)


# These methods must be in transaction in py-lmdb.
class Database_methods(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_primary_record\(\) missing 2 required positional ",
                    "arguments: 'file' and 'key'$",
                )
            ),
            self.database.get_primary_record,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"remove_record_from_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'deletekey'$",
                )
            ),
            self.database.remove_record_from_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"add_record_to_ebm\(\) missing 2 required ",
                    "positional arguments: 'file' and 'putkey'$",
                )
            ),
            self.database.add_record_to_ebm,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_high_record_number\(\) missing 1 required ",
                    "positional argument: 'file'$",
                )
            ),
            self.database.get_high_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_record_number\(\) takes from 2 to 4 ",
                    "positional arguments but 5 were given$",
                )
            ),
            self.database.recordlist_record_number,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_record_number_range\(\) takes from 2 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_record_number_range,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_ebm\(\) takes from 2 to 3 ",
                    "positional arguments but 4 were given$",
                )
            ),
            self.database.recordlist_ebm,
            *(None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_table_connection\(\) missing 1 required positional ",
                    "argument: 'file'$",
                )
            ),
            self.database.get_table_connection,
        )

    def test_02_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", None), None)

    def test_03_get_primary_record(self):
        self.assertEqual(self.database.get_primary_record("file1", 1), None)

    def test_04_get_primary_record(self):
        self.database.put("file1", None, "new value")
        self.assertEqual(
            self.database.get_primary_record("file1", 0), (0, "new value")
        )

    def test_05_remove_record_from_ebm(self):
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "Existence bit map for segment does not exist$",
            self.database.remove_record_from_ebm,
            *("file1", 2),
        )

    def test_06_remove_record_from_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(
            self.database.remove_record_from_ebm("file1", 2), (0, 2)
        )

    def test_07_add_record_to_ebm(self):
        self.assertEqual(self.database.add_record_to_ebm("file1", 2), (0, 2))
        self.assertEqual(self.database.add_record_to_ebm("file1", 4), (0, 4))

    def test_08_get_high_record(self):
        self.assertEqual(self.database.get_high_record_number("file1"), None)

    def test_14_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1"),
            recordset.RecordList,
        )

    def test_15_recordset_record_number(self):
        self.assertIsInstance(
            self.database.recordlist_record_number("file1", key=2),
            recordset.RecordList,
        )

    def test_16_recordset_record_number(self):
        self.database.dbtxn.transaction.put(
            int(1).to_bytes(4, byteorder="big"),
            encode("Some value"),
            db=self.database.table["file1"].datastore,
        )
        values = b"\x40" + b"\x00" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbtxn.transaction.put(
            int(0).to_bytes(4, byteorder="big"),
            values,
            db=self.database.ebm_control["file1"].ebm_table.datastore,
        )
        rl = self.database.recordlist_record_number("file1", key=1)
        self.assertIsInstance(rl, recordset.RecordList)
        self.assertEqual(rl.count_records(), 1)

    def test_17_recordset_record_number_range(self):
        self.assertIsInstance(
            self.database.recordlist_record_number_range("file1"),
            recordset.RecordList,
        )

    def test_18_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=0, keyend=2000
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x7f\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )

    def test_19_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keystart=10)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x00\x3f\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )

    def test_20_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range("file1", keyend=35)
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x7f\xff\xff\xff\xf0\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )

    def test_21_recordset_record_number_range(self):
        self.create_ebm()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=10, keyend=35
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(
            rs[0].tobytes(),
            b"".join(
                (
                    b"\x00\x3f\xff\xff\xf0\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )

    def test_22_recordset_record_number_range(self):
        self.create_ebm()
        self.create_ebm_extra()
        self.create_ebm_extra()
        self.create_ebm_extra()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=170, keyend=350
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(
            rs[1].tobytes(),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x3f\xff\xff",
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                )
            ),
        )
        self.assertEqual(
            rs[2].tobytes(),
            b"".join(
                (
                    b"\xff\xff\xff\xff\xff\xff\xff\xff",
                    b"\xff\xff\xff\xfe\x00\x00\x00\x00",
                )
            ),
        )

    def test_23_recordset_record_number_range(self):
        self.create_ebm()
        self.create_ebm_extra()
        self.create_ebm_extra()
        self.create_ebm_extra()
        rs = self.database.recordlist_record_number_range(
            "file1", keystart=350, keyend=170
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_24_recordset_ebm(self):
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def test_25_recordset_ebm(self):
        self.create_ebm()
        self.assertIsInstance(
            self.database.recordlist_ebm("file1"), recordset.RecordList
        )

    def test_26_get_table_connection(self):
        self.assertEqual(
            self.database.get_table_connection("file1").__class__.__name__,
            "_Database",
        )


class Database_find_values_empty(_DBOpen):
    def setUp(self):
        super().setUp()
        # Need a cursor in a transaction to do 'find_values'.
        # Most tests can be done with read-only.
        self.database.start_read_only_transaction()
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"

    def test_01_find_values(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"find_values\(\) missing 2 required ",
                    "positional arguments: 'valuespec' and 'file'$",
                )
            ),
            self.database.find_values,
        )

    def test_02_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_03_find_values(self):
        self.valuespec.above_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_04_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_05_find_values(self):
        self.valuespec.from_value = "b"
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_06_find_values(self):
        self.valuespec.above_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_07_find_values(self):
        self.valuespec.from_value = "b"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_08_find_values(self):
        self.valuespec.to_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_09_find_values(self):
        self.valuespec.below_value = "d"
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_10_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_12_find_values(self):
        self.valuespec.above_value = ""
        self.valuespec.below_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_13_find_values(self):
        self.valuespec.above_value = ""
        self.valuespec.to_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_14_find_values(self):
        self.valuespec.from_value = ""
        self.valuespec.to_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_15_find_values(self):
        self.valuespec.from_value = ""
        self.valuespec.below_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_16_find_values(self):
        self.valuespec.above_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_17_find_values(self):
        self.valuespec.from_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_18_find_values(self):
        self.valuespec.to_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )

    def test_19_find_values(self):
        self.valuespec.below_value = ""
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")], []
        )


class Database_find_values(_DBOpen):
    def setUp(self):
        super().setUp()
        sdb = self.database
        sdb.start_transaction()
        txn = sdb.dbtxn.transaction
        txn.put(b"d", encode("values"), db=sdb.table["file1_field1"].datastore)
        sdb.commit()
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"
        # Need a cursor in a transaction to do 'find_values'.
        # Most tests can be done with read-only.
        sdb.start_read_only_transaction()

    def test_11_find_values(self):
        self.assertEqual(
            [i for i in self.database.find_values(self.valuespec, "file1")],
            ["d"],
        )


class _Database_recordset(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
        self.segments = {}
        self.keyvalues = {}
        self.references = set()
        self.keyrefmap = {}
        self.newrefs = set()
        for s in DATABASE_MAKE_RECORDSET_SEGMENTS:
            with self.database.dbtxn.transaction.cursor(
                self.database.segment_table["file1"].datastore
            ) as cursor:
                if cursor.last():
                    key = int.from_bytes(cursor.key(), byteorder="big") + 1
                else:
                    key = 0
                keyb = key.to_bytes(4, byteorder="big")
                record = (keyb, s)
                cursor.put(*record, overwrite=False)
                self.references.add((record))
                self.segments[key] = s
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1_field1"].datastore
        ) as cursor:
            for e, k in enumerate(DATABASE_MAKE_RECORDSET_KEYS):
                self.keyvalues[k] = e
                reference = b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(32 if e else 31).to_bytes(2, byteorder="big"),
                        self.keyvalues[k].to_bytes(4, byteorder="big"),
                    )
                )
                record = (k.encode(), reference)
                cursor.put(*record)
                self.references.add((record))
                self.keyrefmap[k] = {0: reference}
            k = "tww"
            self.keyvalues[k] = 7
            reference = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(2).to_bytes(2, byteorder="big"),
                    self.keyvalues[k].to_bytes(4, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add((record))
            self.keyrefmap[k] = {0: reference}
            k = "twy"
            self.keyvalues[k] = 8
            reference = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(3).to_bytes(2, byteorder="big"),
                    self.keyvalues[k].to_bytes(4, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add((record))
            self.keyrefmap[k] = {0: reference}
            k = "one"
            reference = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(50).to_bytes(2, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add((record))
            self.keyrefmap[k] = {0: reference}
            k = "nin"
            reference = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(100).to_bytes(2, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add((record))
            self.keyrefmap[k] = reference

            # This pair of puts wrote their records to different files before
            # solentware-base-4.0, one for lists and one for bitmaps.
            # At solentware-base-4.0 the original test_55_file_records_under
            # raises a lmdb.db.DBKeyEmptyError exception when attempting to
            # delete the second record referred to by self.keyvalues['twy'].
            # The test is changed to expect the exception.
            k = "www"
            reference = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(3).to_bytes(2, byteorder="big"),
                    self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add(record)
            self.keyrefmap[k] = {0: reference}
            reference = b"".join(
                (
                    b"\x00\x00\x00\x01",
                    int(3).to_bytes(2, byteorder="big"),
                    self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                )
            )
            record = (k.encode(), reference)
            cursor.put(*record)
            self.references.add(record)
            self.keyrefmap[k][1] = reference
        self.database.commit()

    def tearDown(self):
        self.database.commit()
        super().tearDown()


class Database_make_recordset(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def verify_records(self, key, test):
        key = encode(key)
        for db in (
            self.database.segment_table["file1"].datastore,
            self.database.table["file1_field1"].datastore,
        ):
            with self.database.dbtxn.transaction.cursor(db) as cursor:
                record = cursor.first()
                while record:
                    record = cursor.item()
                    if key != record[0]:
                        self.references.discard(record)
                    else:
                        self.newrefs.add(record)
                    record = cursor.next()
        self.assertEqual(
            self.references,
            {encode_test_record(r) for r in database_make_recordset[test][0]},
        )
        self.assertEqual(
            self.newrefs,
            {encode_test_record(r) for r in database_make_recordset[test][1]},
        )
        for key, value in database_make_recordset[test][2]:
            if isinstance(key, int):
                db = self.database.segment_table["file1"].datastore
            elif isinstance(key, str):
                db = self.database.table["file1_field1"].datastore
            else:
                continue
            self.assertEqual(
                self.database.dbtxn.transaction.get(
                    encode_test_key(key),
                    db=db,
                ),
                value,
            )

    def test_01_exceptions(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"add_record_to_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', ",
                    "'segment', and 'record_number'$",
                )
            ),
            self.database.add_record_to_field_value,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"remove_record_from_field_value\(\) missing 5 required ",
                    "positional arguments: 'file', 'field', 'key', ",
                    "'segment', and 'record_number'$",
                )
            ),
            self.database.remove_record_from_field_value,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"populate_segment\(\) missing 2 required ",
                    "positional arguments: 'segment_reference' and 'file'$",
                )
            ),
            self.database.populate_segment,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_like\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key_like,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_startswith\(\) takes from 3 to 5 ",
                    "positional arguments but 6 were given$",
                )
            ),
            self.database.recordlist_key_startswith,
            *(None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_key_range\(\) takes from 3 to 8 ",
                    "positional arguments but 9 were given$",
                )
            ),
            self.database.recordlist_key_range,
            *(None, None, None, None, None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"recordlist_all\(\) takes from 3 to 4 ",
                    "positional arguments but 5 were given$",
                )
            ),
            self.database.recordlist_all,
            *(None, None, None, None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"unfile_records_under\(\) missing 3 required ",
                    "positional arguments: 'file', 'field', and 'key'$",
                )
            ),
            self.database.unfile_records_under,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"file_records_under\(\) missing 4 required positional ",
                    "arguments: 'file', 'field', 'recordset', and 'key'$",
                )
            ),
            self.database.file_records_under,
        )

    def test_01_verify_setup_records(self):
        for db in (
            self.database.segment_table["file1"].datastore,
            self.database.table["file1_field1"].datastore,
        ):
            with self.database.dbtxn.transaction.cursor(db) as cursor:
                record = cursor.first()
                while record:
                    self.references.remove(cursor.item())
                    record = cursor.next()
        self.assertEqual(self.references, set())

    def test_05_add_record_to_field_value_01(self):
        key = "indexvalue"
        self.database.add_record_to_field_value("file1", "field1", key, 1, 0)
        self.verify_records(key, "05_01")

    def test_05_add_record_to_field_value_02(self):
        key = "nin"
        self.database.add_record_to_field_value("file1", "field1", key, 0, 99)
        self.verify_records(key, "05_02")

    def test_05_add_record_to_field_value_03(self):
        key = "twy"
        self.database.add_record_to_field_value("file1", "field1", key, 0, 99)
        self.verify_records(key, "05_03")

    def test_05_add_record_to_field_value_04(self):
        key = "aa_o"
        self.database.add_record_to_field_value("file1", "field1", key, 0, 99)
        self.verify_records(key, "05_04")

    def test_05_add_record_to_field_value_05(self):
        key = "tww"
        self.database.add_record_to_field_value("file1", "field1", key, 0, 47)
        self.verify_records(key, "05_05")

    def test_05_add_record_to_field_value_06(self):
        key = "twy"
        for record in range(99, 103):
            self.database.add_record_to_field_value(
                "file1", "field1", key, 0, record
            )
        self.verify_records(key, "05_06")

    def test_05_add_record_to_field_value_07(self):
        key = "twy"
        for record in range(99, 103):
            self.database.add_record_to_field_value(
                "file1", "field1", key, 0, record
            )
        self.database.add_record_to_field_value("file1", "field1", key, 0, 110)
        self.verify_records(key, "05_07")

    def test_05_add_record_to_field_value_08(self):
        key = "ten"
        self.database.add_record_to_field_value("file1", "field1", key, 0, 50)
        self.database.add_record_to_field_value("file1", "field1", key, 1, 51)
        self.verify_records(key, "05_08")

    def test_05_add_record_to_field_value_09(self):
        # Force through 'len(value) == SEGMENT_HEADER_LENGTH' path even if
        # there are no segments for the segment header to point at.
        # The _lmdb module explicitly sets segment to 0 if the cursor.last()
        # statement to find the highest segment number finds no segments.
        # The equivalent code in the _db module simply appends the segment
        # record to the database and accepts whatever key is assigned,
        # oblivious to the (impossible) possibility that the returned key
        # would be 1 were no segments present.
        key = "one"
        for db in (self.database.segment_table["file1"].datastore,):
            with self.database.dbtxn.transaction.cursor(db) as cursor:
                record = cursor.last()
                while record:
                    self.references.remove(cursor.item())
                    cursor.delete()
                    record = cursor.prev()
        self.database.add_record_to_field_value("file1", "field1", key, 0, 99)
        self.verify_records(key, "05_09")

    def test_11_remove_record_from_field_value_01(self):
        key = "indexvalue"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 1, 0
        )
        self.verify_records(key, "11_01")

    def test_11_remove_record_from_field_value_02(self):
        key = "nin"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 99
        )
        self.verify_records(key, "11_02")

    def test_11_remove_record_from_field_value_03(self):
        key = "twy"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 68
        )
        self.verify_records(key, "11_03")

    def test_11_remove_record_from_field_value_04(self):
        key = "bb_o"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 68
        )
        self.verify_records(key, "11_04")

    def test_11_remove_record_from_field_value_05(self):
        key = "tww"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 65
        )
        self.verify_records(key, "11_05")

    def test_11_remove_record_from_field_value_06(self):
        key = "one"
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 50
        )
        self.verify_records(key, "11_06")

    def test_11_remove_record_from_field_value_07(self):
        key = "a_o"
        for record in range(5, 31):
            self.database.remove_record_from_field_value(
                "file1", "field1", key, 0, record
            )
        self.verify_records(key, "11_07")

    def test_11_remove_record_from_field_value_08(self):
        key = "a_o"
        for record in range(5, 31):
            self.database.remove_record_from_field_value(
                "file1", "field1", key, 0, record
            )
        self.database.remove_record_from_field_value(
            "file1", "field1", key, 0, 2
        )
        self.verify_records(key, "11_08")


class Database_populate_recordset(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_read_only_transaction()

    def test_12_populate_segment_01(self):
        s = self.database.populate_segment(
            b"\x00\x00\x00\x02\x00\x03", "file1"
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentInt)

    def test_12_populate_segment_02(self):
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1_field1"].datastore
        ) as cursor:
            while True:
                if not cursor.next():
                    break
                k, v = cursor.item()
                if k.decode() == "one":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(s, recordset.RecordsetSegmentInt)
                        break

    def test_12_populate_segment_03(self):
        s = self.database.populate_segment(
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x02",
                    self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                )
            ),
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentList)
        self.assertEqual(s.count_records(), 2)

    def test_12_populate_segment_04(self):
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1_field1"].datastore
        ) as cursor:
            while True:
                if not cursor.next():
                    break
                k, v = cursor.item()
                if k.decode() == "tww":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(
                            s, recordset.RecordsetSegmentList
                        )
                        self.assertEqual(s.count_records(), 2)
                        break

    def test_12_populate_segment_05(self):
        s = self.database.populate_segment(
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x18",
                    self.keyvalues["c_o"].to_bytes(4, byteorder="big"),
                )
            ),
            "file1",
        )
        self.assertIsInstance(s, recordset.RecordsetSegmentBitarray)
        self.assertEqual(s.count_records(), 24)

    def test_12_populate_segment_06(self):
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1_field1"].datastore
        ) as cursor:
            while True:
                if not cursor.next():
                    break
                k, v = cursor.item()
                if k.decode() == "c_o":
                    if v[:4] == b"\x00\x00\x00\x00":
                        s = self.database.populate_segment(v, "file1")
                        self.assertIsInstance(
                            s, recordset.RecordsetSegmentBitarray
                        )
                        self.assertEqual(s.count_records(), 24)
                        break

    def test_12_populate_segment_07(self):
        self.assertRaisesRegex(
            _lmdb.DatabaseError,
            "Segment record missing$",
            self.database.populate_segment,
            *(b"invalid key", "file1"),
        )


class Database_make_recordset_key(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_read_only_transaction()

    def test_18_make_recordset_key_like_01(self):
        rs = self.database.recordlist_key_like("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_18_make_recordset_key_like_02(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"z")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_18_make_recordset_key_like_03(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"n")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_18_make_recordset_key_like_04(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"w")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_18_make_recordset_key_like_05(self):
        rs = self.database.recordlist_key_like("file1", "field1", keylike=b"e")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 41)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_23_make_recordset_key_01(self):
        rs = self.database.recordlist_key("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_23_make_recordset_key_02(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_23_make_recordset_key_03(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"tww")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)

    def test_23_make_recordset_key_04(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"a_o")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 31)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_23_make_recordset_key_05(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_27_make_recordset_key_startswith_01(self):
        rs = self.database.recordlist_key_startswith("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_28_make_recordset_key_startswith_02(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"ppp"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_28_make_recordset_key_startswith_03(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"o"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentInt)

    def test_28_make_recordset_key_startswith_04(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"tw"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_28_make_recordset_key_startswith_05(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b"d"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 24)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_28_make_recordset_key_startswith_06(self):
        rs = self.database.recordlist_key_startswith(
            "file1", "field1", keystart=b""
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_01(self):
        rs = self.database.recordlist_key_range("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_02(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"ppp", le=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_32_make_recordset_key_range_03(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"n", le=b"q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_04(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"t", le=b"tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_05(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"c", le=b"cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_06(self):
        rs = self.database.recordlist_key_range("file1", "field1", ge=b"c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_07(self):
        rs = self.database.recordlist_key_range("file1", "field1", le=b"cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_08(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", ge=b"ppp", lt=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_32_make_recordset_key_range_09(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"ppp", lt=b"qqq"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 0)

    def test_32_make_recordset_key_range_10(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"n", le=b"q"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_11(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"t", le=b"tz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 5)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_12(self):
        rs = self.database.recordlist_key_range(
            "file1", "field1", gt=b"c", lt=b"cz"
        )
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 40)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_13(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt=b"c")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 62)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_14(self):
        rs = self.database.recordlist_key_range("file1", "field1", lt=b"cz")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 1)
        self.assertEqual(rs[0].count_records(), 111)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)

    def test_32_make_recordset_key_range_15(self):
        rs = self.database.recordlist_key_range("file1", "field1", gt=b"")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertIsInstance(rs[1], recordset.RecordsetSegmentList)

    def test_46_make_recordset_all_01(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.assertIsInstance(rs, recordset.RecordList)
        self.assertEqual(len(rs), 2)
        self.assertEqual(rs[0].count_records(), 127)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)


class Database_file_records(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def test_47_unfile_records_under_01(self):
        self.database.unfile_records_under("file1", "field1", b"aa_o")

    def test_47_unfile_records_under_02(self):
        self.database.unfile_records_under("file1", "field1", b"kkkk")

    # The set_range() call for key b"" in unfile_records_under() has been
    # done and the subsequent delete fails.
    # This test is added dealing with issue 8 (Github) on set_range_dup.
    # It is not clear silently ignoring the delete for key b"" is ever or
    # always the correct action, so let the exception happen.
    # Dated 2025-08-01.
    # Part of the exception message may be enclosed in "b''", possibly
    # aligned with the datastore objects being lmdb.cffi._Database or
    # lmdb._Database instances.
    def test_47_unfile_records_under_03(self):
        self.assertRaisesRegex(
            lmdb.BadValsizeError,
            "".join(
                (
                    r"mdb_del: (?:b')?MDB_BAD_VALSIZE: Unsupported size ",
                    "of key/DB ",
                    "name/data, or wrong DUPFIXED size(?:')?$",
                )
            ),
            self.database.unfile_records_under,
            *("file1", "field1", b""),
        )

    def test_49_file_records_under_01(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_49_file_records_under_02(self):
        rs = self.database.recordlist_all("file1", "field1")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    def test_49_file_records_under_03(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"twy")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_49_file_records_under_04(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"twy")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    def test_49_file_records_under_05(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.database.file_records_under("file1", "field1", rs, b"aa_o")

    def test_49_file_records_under_06(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.database.file_records_under("file1", "field1", rs, b"rrr")

    # This block comment retained from _db module for Berkeley DB, except to
    # note no exception encountered for the _lmdb module.
    # Changed at solentware-base-4.0, see comments in setUp() for put records.
    # Did I really miss the change in error message? Or change something which
    # causes a different error?  Spotted while working on _nosql.py.
    # There has been a FreeBSD OS and ports upgrade since solentware-base-4.0.
    # Changed back after rebuild at end of March 2020.
    # When doing some testing on OpenBSD in September 2020 see that the -30997
    # exception is raised.
    def test_49_file_records_under_07(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"ba_o")
        self.database.file_records_under("file1", "field1", rs, b"www")

    # The first thing file_records_under() does is call unfile_records_under().
    # Assume the task is to file whatever is under key b'one' as b"" too.
    # Dated 2025-08-01.
    # Part of the exception message may be enclosed in "b''", possibly
    # aligned with the datastore objects being lmdb.cffi._Database or
    # lmdb._Database instances.
    def test_49_file_records_under_08(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"one")
        self.assertRaisesRegex(
            lmdb.BadValsizeError,
            "".join(
                (
                    r"mdb_del: (?:b')?MDB_BAD_VALSIZE: Unsupported size ",
                    "of key/DB ",
                    "name/data, or wrong DUPFIXED size(?:')?$",
                )
            ),
            self.database.file_records_under,
            *("file1", "field1", rs, b""),
        )


class Database__get_segment_record_numbers(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_read_only_transaction()

    # No exception encountered for the _lmdb module when key is 0 because
    # b"\x00\x00" is a valid key in Symas LMMD.  For Berkeley DB bit 0 of
    # segment record 1 (in a RECNO database) is never set.  For Symas LMMD
    # bit 0 of segment record 0 is set, or unset, as needed.  Similar for list
    # and integer representations, where appropriate, of a segment.
    # The rest of block comment is retained from _db module for Berkeley DB.
    # Did I really miss the change in error message? Or change something which
    # causes a different error?  Spotted while working on _nosql.py.
    # There has been a FreeBSD OS and ports upgrade since solentware-base-4.0.
    # Changed back after rebuild at end of March 2020.
    # When doing some testing on OpenBSD in September 2020 see that BDB1002
    # is omitted from the exception text.
    def test_56__get_segment_record_numbers(self):
        self.assertIsInstance(
            self.database._get_segment_record_numbers("file1", 6), Bitarray
        )
        self.assertIsInstance(
            self.database._get_segment_record_numbers("file1", 7), list
        )
        self.assertIsInstance(
            self.database._get_segment_record_numbers("file1", 0), Bitarray
        )
        self.assertRaisesRegex(
            TypeError,
            r"object of type 'NoneType' has no len\(\)$",
            self.database._get_segment_record_numbers,
            *("file1", 9),
        )


class Database_populate_recordset_segment(_Database_recordset):
    def setUp(self):
        super().setUp()
        self.database.start_read_only_transaction()

    def test_57__populate_recordset_segment(self):
        d = self.database
        bs = self.keyrefmap["c_o"][0]
        self.assertEqual(len(bs), 10)
        bl = self.keyrefmap["tww"][0]
        self.assertEqual(len(bl), 10)
        rs = recordset.RecordList(d, "file1")
        self.assertEqual(len(rs), 0)
        self.assertEqual(d.populate_recordset_segment(rs, bl), None)
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentList)
        self.assertEqual(d.populate_recordset_segment(rs, bs), None)
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertEqual(
            d.populate_recordset_segment(rs, b"\x00\x00\x00\x00\x00\x01"), None
        )
        self.assertEqual(len(rs), 1)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertEqual(
            d.populate_recordset_segment(rs, b"\x00\x00\x00\x01\x00\x05"), None
        )
        self.assertEqual(len(rs), 2)
        self.assertIsInstance(rs[0], recordset.RecordsetSegmentBitarray)
        self.assertIsInstance(rs[1], recordset.RecordsetSegmentInt)


class Database_database_cursor(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_read_only_transaction()

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_58_database_cursor(self):
        d = self.database
        self.assertIsInstance(
            d.database_cursor("file1", "file1"), _lmdb.CursorPrimary
        )
        self.assertIsInstance(
            d.database_cursor("file1", "field1"), _lmdb.CursorSecondary
        )
        rs = recordset.RecordList(d, "field1")
        self.assertIsInstance(
            d.database_cursor("file1", "field1", recordset=rs),
            recordsetbasecursor.RecordSetBaseCursor,
        )

    def test_59_create_recordset_cursor(self):
        d = self.database
        rs = self.database.recordlist_key("file1", "field1", key=b"ba_o")
        self.assertIsInstance(
            d.create_recordset_cursor(rs), recordsetcursor.RecordsetCursor
        )


class Database_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
        for i in range(SegmentSize.db_segment_size * 3):
            self.database.dbtxn.transaction.put(
                i.to_bytes(4, byteorder="big"),
                encode("value"),
                db=self.database.table["file1"].datastore,
            )
            self.database.add_record_to_ebm("file1", i)
        self.database.commit()
        self.database.start_transaction()
        self.high_record = self.database.get_high_record_number("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record, SegmentSize.db_segment_size
        )[0]

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_01_freed_record_number_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"get_lowest_freed_record_number\(\) missing 1 required ",
                    "positional argument: 'dbset'$",
                )
            ),
            self.database.get_lowest_freed_record_number,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"note_freed_record_number_segment\(\) missing 4 ",
                    "required positional arguments: 'dbset', 'segment', ",
                    "'record_number_in_segment', and 'high_record_number'$",
                )
            ),
            self.database.note_freed_record_number_segment,
        )

    def test_02_note_freed_record_number_segment_01(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (
            100,
            101,
            200,
            300,
        ):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages,
            [0, 1, 2],
        )
        records = []
        with self.database.dbtxn.transaction.cursor(
            self.database.table[CONTROL_FILE].datastore
        ) as cursor:
            record = cursor.first()
            while record:
                record = cursor.item()
                records.append(record)
                record = cursor.next()
        self.assertEqual(len(records), 3)
        self.assertEqual(
            records,
            [(b"Efile1", b"\x00"), (b"Efile1", b"\x01"), (b"Efile1", b"\x02")],
        )

    def test_02_note_freed_record_number_segment_02(self):
        # This test split off from 'test_02..._01' when initial careless
        # coding of lmdb version of open_database and open_database_contexts
        # caused a puzzling failure.
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (
            100,
            101,
            200,
            300,
        ):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        # Identical to 'test_02..._01' up to first assertEqual.
        # Throw the freed list away and delete another record to see the full
        # list regenerated.
        self.database.ebm_control["file1"].freed_record_number_pages = None
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (201,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages,
            [0, 1, 2],
        )

    def test_07_get_lowest_freed_record_number_01(self):
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def test_07_get_lowest_freed_record_number_02(self):
        for i in (
            100,
            101,
            200,
            300,
        ):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 100)

    def test_07_get_lowest_freed_record_number_03(self):
        for i in (380,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)

    def test_07_get_lowest_freed_record_number_04(self):
        for i in (110,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 110)

    # The freed record number in segment number 2, 'divmod(380, 128)', is not
    # seen until segment number 4 has records.
    # Segment 2 is not deleted from the 'freed record number' list until the
    # first search of the segment after all freed record numbers have been
    # re-used.
    def test_07_get_lowest_freed_record_number_05(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        for i in (380,):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, None)
        i = self.high_record
        for i in range(i, i + 129):
            with self.database.dbtxn.transaction.cursor(
                self.database.table["file1"].datastore
            ) as cursor:
                if cursor.last():
                    key = int.from_bytes(cursor.key(), byteorder="big") + 1
                else:
                    key = 0
            self.database.dbtxn.transaction.put(
                key.to_bytes(4, byteorder="big"),
                encode("value"),
                db=self.database.table["file1"].datastore,
            )
            self.database.add_record_to_ebm("file1", key)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        self.high_record = self.database.get_high_record_number("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record, SegmentSize.db_segment_size
        )[0]
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 380)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            1,
        )
        self.database.add_record_to_ebm("file1", 380)
        # self.database.start_transaction()
        rn = self.database.get_lowest_freed_record_number("file1")
        # self.database.commit()
        self.assertEqual(rn, None)
        self.assertEqual(
            len(self.database.ebm_control["file1"].freed_record_number_pages),
            0,
        )

    def test_07_get_lowest_freed_record_number_06(self):
        for i in (0, 1):
            self.database.delete("file1", i, "_".join((str(i), "value")))
            sn, rn = self.database.remove_record_from_ebm("file1", i)
            self.database.note_freed_record_number_segment(
                "file1", sn, rn, self.high_record
            )
        rn = self.database.get_lowest_freed_record_number("file1")
        self.assertEqual(rn, 0)


# Does this test add anything beyond Database_freed_record_number?
class Database_empty_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.commit()
        self.database.start_transaction()
        self.high_record = self.database.get_high_record_number("file1")

    def tearDown(self):
        self.database.commit()
        super().tearDown()

    def test_01(self):
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        self.database.note_freed_record_number_segment(
            "file1", 0, 100, self.high_record
        )
        self.assertEqual(
            self.database.ebm_control["file1"].freed_record_number_pages, None
        )
        self.assertEqual(
            self.database.get_high_record_number("file1"), self.high_record
        )


class RecordsetCursor(_DBOpen):
    def setUp(self):
        super().setUp()
        segments = (
            b"".join(
                (
                    b"\x7f\xff\xff\xff\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                )
            ),
        )
        keys = ("a_o",)
        self.database.start_transaction()
        for i in range(380):
            self.database.dbtxn.transaction.put(
                i.to_bytes(4, byteorder="big"),
                encode(str(i) + "Any value"),
                db=self.database.table["file1"].datastore,
            )
            self.database.add_record_to_ebm("file1", i)
        bits = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbtxn.transaction.put(
            int(0).to_bytes(4, byteorder="big"),
            bits,
            db=self.database.ebm_control["file1"].ebm_table.datastore,
        )
        bits = b"\xff" * SegmentSize.db_segment_size_bytes
        self.database.dbtxn.transaction.put(
            int(1).to_bytes(4, byteorder="big"),
            bits,
            db=self.database.ebm_control["file1"].ebm_table.datastore,
        )
        self.database.dbtxn.transaction.put(
            int(2).to_bytes(4, byteorder="big"),
            bits,
            db=self.database.ebm_control["file1"].ebm_table.datastore,
        )
        for s in segments:
            with self.database.dbtxn.transaction.cursor(
                self.database.segment_table["file1"].datastore
            ) as cursor:
                if cursor.last():
                    key = int.from_bytes(cursor.key(), byteorder="big") + 1
                else:
                    key = 0
            self.database.dbtxn.transaction.put(
                key.to_bytes(4, byteorder="big"),
                s,
                db=self.database.segment_table["file1"].datastore,
            )
        self.database.commit()
        self.database.start_transaction()
        with self.database.dbtxn.transaction.cursor(
            self.database.table["file1_field1"].datastore
        ) as cursor:
            for e in range(len(segments)):
                cursor.put(
                    b"a_o",
                    b"".join(
                        (
                            e.to_bytes(4, byteorder="big"),
                            (128).to_bytes(2, byteorder="big"),
                            e.to_bytes(4, byteorder="big"),
                        )
                    ),
                )

    def test_01_exceptions_01(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required ",
                    "positional argument: 'recordset'$",
                )
            ),
            _lmdb.RecordsetCursor,
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"_get_record\(\) missing 1 required ",
                    "positional argument: 'record_number'$",
                )
            ),
            _lmdb.RecordsetCursor(None, None)._get_record,
        )

    def test_03___init__01(self):
        rc = _lmdb.RecordsetCursor(None)
        self.assertEqual(rc._transaction, None)
        self.assertEqual(rc._database, None)

    def test_03___init__02(self):
        rs = self.database.recordlist_key("file1", "field1", key=b"a_o")
        rc = _lmdb.RecordsetCursor(rs)
        self.assertIs(rc._dbset, rs)

    def test_04__get_record(self):
        rc = _lmdb.RecordsetCursor(
            self.database.recordlist_key("file1", "field1", key=b"a_o"),
            transaction=self.database.dbtxn,
            database=self.database.table["file1"],
        )
        self.assertEqual(rc._get_record(4000), None)
        self.assertEqual(rc._get_record(120), None)
        self.assertEqual(rc._get_record(10), (10, "10Any value"))
        self.assertEqual(rc._get_record(155), (155, "155Any value"))


def encode(value):
    return value.encode()


def encode_test_key(key):
    if isinstance(key, int):
        return key.to_bytes(4, byteorder="big")
    return encode(key)


def encode_test_record(record):
    return (encode_test_key(record[0]), record[1])


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(_DBtxn))
    runner().run(loader(_DBtxn_start_transaction))
    runner().run(loader(_Datastore___init__))
    runner().run(loader(_Datastore))
    runner().run(loader(_Datastore_open_datastore))
    runner().run(loader(_Datastore_open_datastore_write))
    runner().run(loader(_Datastore_close_datastore))
    runner().run(loader(Database___init__))
    runner().run(loader(Database_transaction_bad_calls))
    runner().run(loader(Database_start_transaction))
    runner().run(loader(Database_backout_and_commit))
    runner().run(loader(Database_database_contexts_bad_calls))
    runner().run(loader(DatabaseInstance))
    runner().run(loader(Database_open_database))
    runner().run(loader(Database_add_field_to_existing_database))
    runner().run(loader(DatabaseDir_open_database))
    runner().run(loader(DatabaseExist_open_database))
    runner().run(loader(Database_close_database))
    runner().run(loader(Database_open_database_contexts))
    runner().run(loader(Database_do_database_task))
    runner().run(loader(DatabaseTransactions))
    runner().run(loader(Database_put_replace_delete))
    runner().run(loader(Database_methods))
    runner().run(loader(Database_find_values_empty))
    runner().run(loader(Database_find_values))
    runner().run(loader(Database_make_recordset))
    runner().run(loader(Database_populate_recordset))
    runner().run(loader(Database_make_recordset_key))
    runner().run(loader(Database_file_records))
    runner().run(loader(Database__get_segment_record_numbers))
    runner().run(loader(Database_populate_recordset_segment))
    runner().run(loader(Database_database_cursor))
    runner().run(loader(Database_freed_record_number))
    runner().run(loader(Database_empty_freed_record_number))
    runner().run(loader(RecordsetCursor))
