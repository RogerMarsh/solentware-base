# test__lmdb_setUp_tearDown.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""_lmdb _database setUp and tearDown tests wiyh lmdb interface."""

import unittest
import os
import shutil

import lmdb

from .. import _lmdb
from .. import _lmdbdu
from .. import filespec
from ..segmentsize import SegmentSize
from ..wherevalues import ValuesClause

# Symas LMDB does not support memory-only databases.
LMDB_TEST_ROOT = "___test_lmdb"
HOME = os.path.expanduser(os.path.join("~", LMDB_TEST_ROOT))
# The subdir==True names.
# DATA = "data.mdb"
# LOCK = "lock.mdb"
# The subdir==False names.
DATA = LMDB_TEST_ROOT
LOCK = "-".join((LMDB_TEST_ROOT, "lock"))
HOME_DATA = os.path.join(HOME, DATA)
HOME_LOCK = os.path.join(HOME, LOCK)


class HomeNull(unittest.TestCase):
    def setUp(self):
        shutil.rmtree(HOME, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(HOME, ignore_errors=True)

    @property
    def dbe_module(self):
        return self._dbe_module()

    # Subclasses should override this method where alternative modules
    # provide the database emgine interface.
    # For example bsddb3 and berkeleydb for Berkeley DB.
    # As far as I know this is not the case for Symas LMMD.
    def _dbe_module(self):
        return lmdb


class HomeExists(HomeNull):
    def setUp(self):
        super().setUp()
        os.mkdir(HOME)


class EnvironmentExists(HomeExists):
    def setUp(self):
        super().setUp()
        self.env = self.dbe_module.open(HOME_DATA, subdir=False)

    def tearDown(self):
        self.env.close()
        super().tearDown()


class EnvironmentExistsReadOnly(EnvironmentExists):
    def setUp(self):
        super().setUp()
        self.env.close()
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, readonly=True)

    def tearDown(self):
        self.env.close()
        super().tearDown()


class EnvironmentExistsOneDb(HomeExists):
    def setUp(self):
        super().setUp()
        self.env = self.dbe_module.open(HOME_DATA, subdir=False, max_dbs=1)

    def tearDown(self):
        self.env.close()
        super().tearDown()


class _D(_lmdb.Database):
    def __init__(self, specification, folder=None, **kwargs):
        super().__init__(
            specification,
            folder=folder if folder is not None else HOME,
            **kwargs,
        )


class _Ddu(_lmdbdu.Database, _D):
    pass


class DB(HomeNull):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        super().setUp()
        self._D = _D

    def tearDown(self):
        super().tearDown()
        self._D = None

    def create_ebm(self, bmb=None):
        if bmb is None:
            bmb = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.dbtxn.transaction.put(
            int(0).to_bytes(4, byteorder="big"),
            bmb,
            db=self.database.ebm_control["file1"].ebm_table.datastore,
        )

    def create_ebm_extra(self, bmb=None):
        if bmb is None:
            bmb = b"\xff" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        with self.database.dbtxn.transaction.cursor(
            self.database.ebm_control["file1"].ebm_table.datastore
        ) as cursor:
            if cursor.last():
                key = int.from_bytes(cursor.key(), byteorder="big") + 1
            else:
                key = 0
            cursor.put(
                key.to_bytes(4, byteorder="big"),
                bmb,
                overwrite=False,
            )

    def create_record(self, record_number):
        # self.database.table["file1"].put(
        #    record_number, str(record_number).encode()
        # )
        self.database.dbtxn.transaction.put(
            record_number.to_bytes(4, byteorder="big"),
            str(record_number).encode(),
            db=self.database.table["file1"].datastore,
        )


class DBdu(HomeNull):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        super().setUp()
        self._D = _Ddu

    def tearDown(self):
        super().tearDown()
        self._D = None


class DBDir(HomeExists):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        super().setUp()
        self._D = _D

    def tearDown(self):
        super().tearDown()
        self._D = None


class DBduDir(HomeExists):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        super().setUp()
        self._D = _Ddu

    def tearDown(self):
        super().tearDown()
        self._D = None


class DBExist(DBDir):
    def setUp(self):
        super().setUp()
        dbenv = self.dbe_module.open(HOME, create=True, readonly=False)
        dbenv.close()


class DBduExist(DBduDir):
    def setUp(self):
        super().setUp()
        dbenv = self.dbe_module.open(HOME, create=True, readonly=False)
        dbenv.close()


class Database_transaction_bad_calls(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})


class Database_start_transaction(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)


class Database_backout_and_commit(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})
        self.database.dbenv = self.dbe_module.open(HOME)
        self.database.dbtxn._transaction = self.database.dbenv.begin()


class Database_database_contexts_bad_calls(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})


class DatabaseInstance(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({})


class Database_open_database_contexts(DB):
    def setUp(self):
        super().setUp()
        self.database = self._D({**{"file1": {"field1"}, "file2": {"field2"}}})
        self.database.dbenv = self.dbe_module.open(
            HOME, subdir is False, max_dbs=7
        )


class Database_do_database_task(unittest.TestCase):
    # SegmentSize.db_segment_size_bytes is not reset in this class because only
    # one pass through the test loop is done: for lmdb.  Compare with modules
    # test__sqlite and test__nosql where two passes are done.

    def setUp(self):
        class _ED(_lmdb.Database):
            def __init__(self, specification, folder=None, **kwargs):
                super().__init__(
                    specification,
                    folder=folder if folder is not None else LMDB_TEST_ROOT,
                    **kwargs,
                )

            def open_database(self, **k):
                super().open_database(self.dbe_module, **k)

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


class Database_put_replace_delete(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()

    def tearDown(self):
        self.database.commit()
        super().tearDown()


class Database_find_values(_DBOpen):
    def setUp(self):
        super().setUp()
        self.valuespec = ValuesClause()
        self.valuespec.field = "field1"


class Database_make_recordset(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
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
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\xff",
                    b"\xff\xff\x00\x00\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\xff\xff\xff\x00\x00\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\xff\xff\xff\x00\x00",
                )
            ),
            b"".join(
                (
                    b"\x00\x00\x00\x00\x00\x00\x00\x00",
                    b"\x00\x00\x00\x00\x00\xff\xff\xff",
                )
            ),
            b"\x00\x40\x00\x41",
            b"\x00\x42\x00\x43\x00\x44",
        )
        self.segments = {}
        keys = (
            "a_o",
            "aa_o",
            "ba_o",
            "bb_o",
            "c_o",
            "cep",
            "deq",
        )
        self.keyvalues = {}
        self.references = {}
        for s in segments:
            self.segments[self.database.segment_table["file1"].append(s)] = s
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        try:
            for e, k in enumerate(keys):
                self.keyvalues[k] = e + 1
                self.references[k] = b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(32 if e else 31).to_bytes(2, byteorder="big"),
                        self.keyvalues[k].to_bytes(4, byteorder="big"),
                    )
                )
                cursor.put(
                    k.encode(),
                    self.references[k],
                    self.dbe_module.db.DB_KEYLAST,
                )
            self.keyvalues["tww"] = 8
            self.references["tww"] = b"".join(
                (
                    b"\x00\x00\x00\x00",
                    int(2).to_bytes(2, byteorder="big"),
                    self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                )
            )
            cursor.put(
                "tww".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["tww"].to_bytes(4, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )
            self.keyvalues["twy"] = 9
            cursor.put(
                "twy".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "one".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(50).to_bytes(2, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "nin".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(100).to_bytes(2, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )

            # This pair of puts wrote their records to different files before
            # solentware-base-4.0, one for lists and one for bitmaps.
            # At solentware-base-4.0 the original test_55_file_records_under
            # raises a lmdb.db.DBKeyEmptyError exception when attempting to
            # delete the second record referred to by self.keyvalues['twy'].
            # The test is changed to expect the exception.
            cursor.put(
                "www".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x00",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )
            cursor.put(
                "www".encode(),
                b"".join(
                    (
                        b"\x00\x00\x00\x01",
                        int(2).to_bytes(2, byteorder="big"),
                        self.keyvalues["twy"].to_bytes(4, byteorder="big"),
                    )
                ),
                self.dbe_module.db.DB_KEYLAST,
            )

        finally:
            cursor.close()

    def tearDown(self):
        self.database.commit()
        super().tearDown()


class Database_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.start_transaction()
        self.database.ebm_control["file1"] = _lmdb.ExistenceBitmapControl(
            "file1",
            self.database,
            self.dbe_module.db,
            self.dbe_module.db.DB_CREATE,
        )
        for i in range(SegmentSize.db_segment_size * 3 - 1):
            self.database.add_record_to_ebm(
                "file1",
                self.database.table["file1"][0].append(
                    encode("value"), txn=self.database.dbtxn
                ),
            )
        self.high_record = self.database.get_high_record_number("file1")
        self.database.ebm_control["file1"].segment_count = divmod(
            self.high_record[0], SegmentSize.db_segment_size
        )[0]

    def tearDown(self):
        self.database.commit()
        super().tearDown()


class Database_empty_freed_record_number(_DBOpen):
    def setUp(self):
        super().setUp()
        self.database.ebm_control["file1"] = _lmdb.ExistenceBitmapControl(
            "file1",
            self.database,
            self.dbe_module.db,
            self.dbe_module.db.DB_CREATE,
        )
        self.high_record = self.database.get_high_record_number("file1")


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
        for i in range(380):
            self.database.table["file1"][0].append(
                encode(str(i + 1) + "Any value")
            )
        bits = b"\x7f" + b"\xff" * (SegmentSize.db_segment_size_bytes - 1)
        self.database.ebm_control["file1"].ebm_table.put(1, bits)
        bits = b"\xff" * SegmentSize.db_segment_size_bytes
        self.database.ebm_control["file1"].ebm_table.put(2, bits)
        self.database.ebm_control["file1"].ebm_table.put(3, bits)
        for s in segments:
            self.database.segment_table["file1"].append(s)
        self.database.start_transaction()
        cursor = self.database.table["file1_field1"][0].cursor(
            txn=self.database.dbtxn
        )
        for e in range(len(segments)):
            cursor.put(
                b"a_o",
                b"".join(
                    (
                        e.to_bytes(4, byteorder="big"),
                        (128 if e else 127).to_bytes(2, byteorder="big"),
                        (e + 1).to_bytes(4, byteorder="big"),
                    )
                ),
                self.database._dbe.DB_KEYLAST,
            )


def encode(value):
    return value


if __name__ == "__main__":

    class _Module:
        def _dbe_module(self):
            return lmdb

    class _HomeNull(_Module, HomeNull):
        def test_01_HomeNull(self):
            self.assertRaisesRegex(
                FileNotFoundError,
                "".join(
                    (
                        r"\[Errno 2] No such file or directory: '",
                        os.path.expanduser(os.path.join("~", HOME)),
                        r"'$",
                    )
                ),
                os.listdir,
                *(HOME,),
            )

        def test_02_HomeNull_dbe_module(self):
            self.assertIs(self.dbe_module, lmdb)

    class _HomeExists(_Module, HomeExists):
        def test_01_HomeExists(self):
            self.assertEqual(os.listdir(HOME), [])

    class _EnvironmentExists(_Module, EnvironmentExists):
        def test_01_EnvironmentExists(self):
            self.assertEqual(set(os.listdir(HOME)), set([DATA, LOCK]))

    class _EnvironmentExistsReadOnly(_Module, EnvironmentExistsReadOnly):
        def test_01_EnvironmentExistsReadOnly(self):
            self.assertEqual(set(os.listdir(HOME)), set([DATA, LOCK]))

    class _EnvironmentExistsOneDb(_Module, EnvironmentExistsOneDb):
        def test_01_EnvironmentExistsOneDb(self):
            self.assertEqual(set(os.listdir(HOME)), set([DATA, LOCK]))

    class _DB(_Module, DB):
        def test_01_DB(self):
            self.assertTrue(issubclass(self._D, _lmdb.Database))

    class _DBDir(_Module, DBDir):
        def test_01_DB(self):
            self.assertTrue(issubclass(self._D, _lmdb.Database))

    class _DBExist(_Module, DBExist):
        def test_01_DB(self):
            self.assertTrue(issubclass(self._D, _lmdb.Database))

    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(_HomeNull))
    runner().run(loader(_HomeExists))
    runner().run(loader(_EnvironmentExists))
    runner().run(loader(_EnvironmentExistsReadOnly))
    runner().run(loader(_EnvironmentExistsOneDb))
    runner().run(loader(_DB))
    runner().run(loader(_DBDir))
    runner().run(loader(_DBExist))
