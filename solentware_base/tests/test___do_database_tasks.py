# test___do_database_tasks.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test do_database_tasks method against all engines on non-memory databases.

Test behaviour for empty specification after resolution of problems exposed
when support for dbm.gnu was introduced.

Test behaviour for the simplest possible non-empty specification.
"""

import unittest
import os
from ast import literal_eval
import gc

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis = None
try:
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None
try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
try:
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None
try:
    import lmdb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdb = None
try:
    from dpt_dbms import dptapi
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptapi = None

from ..core.tests import _data_generator
from ..core.segmentsize import SegmentSize

try:
    from .. import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from .. import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
try:
    from .. import unqlite_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite_database = None
try:
    from .. import vedis_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis_database = None
try:
    from .. import sqlite3_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3_database = None
try:
    from .. import apsw_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw_database = None
try:
    from .. import lmdb_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdb_database = None
try:
    from .. import bsddb3_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3_database = None
try:
    from .. import berkeleydb_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb_database = None
try:
    from .. import dpt_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dpt_database = None
try:
    from .. import ndbm_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_database = None
try:
    from .. import gnu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_database = None


class _Database(unittest.TestCase):
    def setUp(self):
        gc.collect()
        self.__ssb = SegmentSize.db_segment_size_bytes

    def tearDown(self):
        self.database = None
        self._ED = None
        SegmentSize.db_segment_size_bytes = self.__ssb
        if os.path.exists(self._folder):
            self._delete_test_directories_and_files(self._folder)

    def _delete_test_directories_and_files(self, pathname):
        if os.path.isdir(pathname):
            for item in os.listdir(pathname):
                self._delete_test_directories_and_files(
                    os.path.join(pathname, item)
                )
            os.rmdir(pathname)
        else:
            os.remove(pathname)

    def task(self, *a, **k):
        return

    names_t01 = None
    names_t02 = None


def t01_database_names(self):
    ae = self.assertEqual
    self.database = self._ED(
        empty_filespec, folder=self._folder, segment_size_bytes=None
    )
    ae(os.path.exists(self.database.home_directory), False)
    self.database.open_database()
    self.database.close_database()
    ae(os.path.exists(self.database.home_directory), True)
    ae(os.path.basename(self.database.home_directory), self._folder)
    self.check_database_names(self.names_t01)


def t02_database_names(self):
    ae = self.assertEqual
    self.database = self._ED(
        simple_filespec, folder=self._folder, segment_size_bytes=None
    )
    ae(os.path.exists(self.database.home_directory), False)
    self.database.open_database()
    ae(os.path.exists(self.database.home_directory), True)
    ae(os.path.basename(self.database.home_directory), self._folder)
    self.check_database_names(self.names_t02)


# Overridden in dpt_dbms (imported as dptapi) tests.
def t01_do_database_task_empty_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    ae(self.database.do_database_task(self.task), None)


def t02_do_database_task_empty_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    self.database.close_database()
    ae(self.database.do_database_task(self.task), None)


def t03_do_database_task_empty_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    ae(self.database.do_database_task(self.task), None)


# Overridden in dpt_dbms (imported as dptapi) tests.
def t04_do_database_task_empty_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    ae(self.database.do_database_task(self.task), None)
    self.database.close_database()


def t05_do_database_task_empty_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    ae(self.database.do_database_task(self.task), None)
    self.database.open_database()
    self.database.close_database()


# Overridden in dpt_dbms (imported as dptapi) tests.
def t01_do_database_task_simple_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    ae(self.database.do_database_task(self.task), None)


def t02_do_database_task_simple_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    self.database.close_database()
    ae(self.database.do_database_task(self.task), None)


def t03_do_database_task_simple_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    ae(self.database.do_database_task(self.task), None)


# Overridden in dpt_dbms (imported as dptapi) tests.
def t04_do_database_task_simple_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    self.database.open_database()
    ae(self.database.do_database_task(self.task), None)
    self.database.close_database()


def t05_do_database_task_simple_spec(self):
    ae = self.assertEqual
    self.database = self._AD(folder=self._folder)
    ae(self.database.do_database_task(self.task), None)
    self.database.open_database()
    self.database.close_database()


class _DatabaseBerkeley(_Database):
    def setUp(self):
        super().setUp()
        oda = self._oda

        class _ED(self._interface.Database):
            def open_database(self, **k):
                super().open_database(*oda, **k)

        self._ED = _ED

    def tearDown(self):
        # self.database.home_directory is usually the same as self._folder
        # in BerkeleyDB tests and has likeley been deleted because
        # self._folder is deleted in a superclass tearDown().
        database_folder = self.database.home_directory
        super().tearDown()
        if os.path.exists(database_folder):
            self._delete_test_directories_and_files(database_folder)

class _DatabaseDpt(_Database):
    def setUp(self):
        super().setUp()

        class _ED(self._interface.Database):
            def open_database(self, **k):
                super().open_database(**k)

        self._ED = _ED

    def tearDown(self):
        # self.database.home_directory is an ancestor of self._folder in
        # DPT tests and cannot be deleted until the Core Services object
        # has been destroyed.
        # self._folder is deleted in a superclass tearDown().
        database_folder = self.database.home_directory
        super().tearDown()
        if os.path.exists(database_folder):
            self._delete_test_directories_and_files(database_folder)


class _DatabaseOther(_Database):
    def setUp(self):
        super().setUp()
        oda = self._oda

        class _ED(self._interface.Database):
            def open_database(self, **k):
                super().open_database(*oda, **k)

        self._ED = _ED

    def tearDown(self):
        # self.database.home_directory is usually the same as self._folder
        # in other database engine tests and has likeley been deleted because
        # self._folder is deleted in a superclass tearDown().
        database_folder = self.database.home_directory
        super().tearDown()
        if os.path.exists(database_folder):
            self._delete_test_directories_and_files(database_folder)


if unqlite:

    class _DatabaseUnqlite(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_unqlite"
            self._interface = unqlite_database._nosql
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            self._module = unqlite
            super().setUp()

    class DatabaseFilesUnqlite(_DatabaseUnqlite):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(self._folder in files, True)

    class DoDatabaseTaskUnqlite(_DatabaseUnqlite):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecUnqlite(DoDatabaseTaskUnqlite):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecUnqlite(DoDatabaseTaskUnqlite):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if vedis:

    class _DatabaseVedis(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_vedis"
            self._interface = vedis_database._nosql
            self._oda = vedis, vedis.Vedis, None
            self._module = vedis
            super().setUp()

    class DatabaseFilesVedis(_DatabaseVedis):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(self._folder in files, True)

    class DoDatabaseTaskVedis(_DatabaseVedis):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecVedis(DoDatabaseTaskVedis):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecVedis(DoDatabaseTaskVedis):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if bsddb3:

    class _DatabaseBsddb3(_DatabaseBerkeley):
        def setUp(self):
            self._folder = "___update_test_bsddb3"
            self._interface = bsddb3_database._db
            self._oda = (bsddb3.db,)
            self._module = bsddb3
            super().setUp()

    class DatabaseFilesBsddb3(_DatabaseBsddb3):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
            ae(self._folder in files, True)

    class DoDatabaseTaskBsddb3(_DatabaseBsddb3):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecBsddb3(DoDatabaseTaskBsddb3):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecBsddb3(DoDatabaseTaskBsddb3):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if berkeleydb:

    class _DatabaseBerkeleydb(_DatabaseBerkeley):
        def setUp(self):
            self._folder = "___update_test_berkeleydb"
            self._interface = berkeleydb_database._db
            self._oda = (berkeleydb.db,)
            self._module = berkeleydb
            super().setUp()

    class DatabaseFilesBerkeleydb(_DatabaseBerkeleydb):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
            ae(self._folder in files, True)

    class DoDatabaseTaskBerkeleydb(_DatabaseBerkeleydb):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecBerkeleydb(DoDatabaseTaskBerkeleydb):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecBerkeleydb(DoDatabaseTaskBerkeleydb):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if sqlite3:

    class _DatabaseSqlite3(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_sqlite3"
            self._interface = sqlite3_database._sqlite
            self._oda = (sqlite3,)
            self._module = sqlite3
            super().setUp()

    class DatabaseFilesSqlite3(_DatabaseSqlite3):
        test_01 = t01_database_names

        # On Microsoft Windows a PermissionError is reported in tearDown()
        # when deleting the file if this test is run: also the next sqlite3
        # test gets a ResourceWarning 'unclosed database' if trying to open
        # a database with the same name.
        # On other operating systems the next garbage collection cycle,
        # probably in gc.collect() call in setUp() for this module's tests,
        # gets a ResourceWarning 'unclosed database'.
        # apsw does not have this behaviour.
        if not os.name == "nt":
            test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(self._folder in files, True)

    class DoDatabaseTaskSqlite3(_DatabaseSqlite3):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecSqlite3(DoDatabaseTaskSqlite3):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        # On Microsoft Windows a PermissionError is reported in tearDown()
        # when deleting the file if this test is run: also the next sqlite3
        # test gets a ResourceWarning 'unclosed database' if trying to open
        # a database with the same name.
        # On other operating systems the next garbage collection cycle,
        # probably in gc.collect() call in setUp() for this module's tests,
        # gets a ResourceWarning 'unclosed database'.
        # apsw does not have this behaviour.
        if not os.name == "nt":
            test_01 = t01_do_database_task_empty_spec

        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecSqlite3(DoDatabaseTaskSqlite3):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        # On Microsoft Windows a PermissionError is reported in tearDown()
        # when deleting the file if this test is run: also the next sqlite3
        # test gets a ResourceWarning 'unclosed database' if trying to open
        # a database with the same name.
        # On other operating systems the next garbage collection cycle,
        # probably in gc.collect() call in setUp() for this module's tests,
        # gets a ResourceWarning 'unclosed database'.
        # apsw does not have this behaviour.
        if not os.name == "nt":
            test_01 = t01_do_database_task_simple_spec

        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if apsw:

    class _DatabaseApsw(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_apsw"
            self._interface = apsw_database._sqlite
            self._oda = (apsw,)
            self._module = apsw
            super().setUp()

    class DatabaseFilesApsw(_DatabaseApsw):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(self._folder in files, True)

    class DoDatabaseTaskApsw(_DatabaseApsw):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecApsw(DoDatabaseTaskApsw):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecApsw(DoDatabaseTaskApsw):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if lmdb:

    class _DatabaseLmdb(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_lmdb"
            self._interface = lmdb_database._lmdb
            self._oda = (lmdb,)
            self._module = lmdb
            super().setUp()

    class DatabaseFilesLmdb(_DatabaseLmdb):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(self._folder + "-lock" in files, True)
            ae(len(files), 2)
            ae(self._folder in files, True)

    class DoDatabaseTaskLmdb(_DatabaseLmdb):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecLmdb(DoDatabaseTaskLmdb):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        # On Microsoft Windows a lmbd.Error <database already open> is
        # reported for the test, followed by a PermissionError in tearDown()
        # when deleting the file.
        if not os.name == "nt":
            test_01 = t01_do_database_task_empty_spec

        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec

        # On Microsoft Windows a lmbd.Error <database already open> is
        # reported for the test, followed by a PermissionError in tearDown()
        # when deleting the file.
        if not os.name == "nt":
            test_04 = t04_do_database_task_empty_spec

        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecLmdb(DoDatabaseTaskLmdb):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        # On Microsoft Windows a lmbd.Error <database already open> is
        # reported for the test, followed by a PermissionError in tearDown()
        # when deleting the file.
        if not os.name == "nt":
            test_01 = t01_do_database_task_simple_spec

        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec

        # On Microsoft Windows a lmbd.Error <database already open> is
        # reported for the test, followed by a PermissionError in tearDown()
        # when deleting the file.
        if not os.name == "nt":
            test_04 = t04_do_database_task_simple_spec

        test_05 = t05_do_database_task_simple_spec


if dptapi:

    class _DatabaseDptapi(_DatabaseDpt):
        def setUp(self):
            self._folder = "___update_test_dpt"
            self._interface = dpt_database._dpt
            self._oda = (dptapi,)  # Not sure if this is complete.
            self._module = dptapi
            super().setUp()

    class DatabaseFilesDptapi(_DatabaseDptapi):
        test_01 = t01_database_names
        test_02 = t02_database_names
        names_t01 = set(["dptsys", "___control.dpt"])
        names_t02 = set(["dptsys", "___control.dpt", "file1.dpt"])

        def check_database_names(self, names):
            ae = self.assertEqual
            ae(self.database.database_file is None, True)
            ae(
                set(os.listdir(self.database.home_directory)),
                names,
            )

    class DoDatabaseTaskDptapi(_DatabaseDptapi):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecDptapi(DoDatabaseTaskDptapi):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        def t01_do_database_task_empty_spec(self):
            self.database = self._AD(folder=self._folder)
            self.database.open_database()
            self.assertRaisesRegex(
                RuntimeError,
                "".join(
                    (
                        r"User CoreServices initialization failed - ",
                        "second API on the same thread is not allowed$",
                    )
                ),
                self.database.do_database_task,
                *(self.task,),
            )

        def t04_do_database_task_empty_spec(self):
            self.database = self._AD(folder=self._folder)
            self.database.open_database()
            self.assertRaisesRegex(
                RuntimeError,
                "".join(
                    (
                        r"User CoreServices initialization failed - ",
                        "second API on the same thread is not allowed$",
                    )
                ),
                self.database.do_database_task,
                *(self.task,),
            )

        # The RuntimeError test succeeds but tearDown() gets PermissionError
        # when trying to delete the directories and files used in test.
        # test_01 = t01_do_database_task_empty_spec

        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec

        # The RuntimeError test succeeds but tearDown() gets PermissionError
        # when trying to delete the directories and files used in test.
        # test_04 = t04_do_database_task_empty_spec

        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecDptapi(DoDatabaseTaskDptapi):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        def t01_do_database_task_simple_spec(self):
            self.database = self._AD(folder=self._folder)
            self.database.open_database()
            self.assertRaisesRegex(
                RuntimeError,
                "".join(
                    (
                        r"User CoreServices initialization failed - ",
                        "second API on the same thread is not allowed$",
                    )
                ),
                self.database.do_database_task,
                *(self.task,),
            )

        def t04_do_database_task_simple_spec(self):
            self.database = self._AD(folder=self._folder)
            self.database.open_database()
            self.assertRaisesRegex(
                RuntimeError,
                "".join(
                    (
                        r"User CoreServices initialization failed - ",
                        "second API on the same thread is not allowed$",
                    )
                ),
                self.database.do_database_task,
                *(self.task,),
            )

        # The RuntimeError test succeeds but tearDown() gets PermissionError
        # when trying to delete the directories and files used in test.
        # test_01 = t01_do_database_task_simple_spec

        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec

        # The RuntimeError test succeeds but tearDown() gets PermissionError
        # when trying to delete the directories and files used in test.
        # test_04 = t04_do_database_task_simple_spec

        test_05 = t05_do_database_task_simple_spec


if ndbm_module:

    class _DatabaseNdbm(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_ndbm"
            self._interface = ndbm_database._nosql
            self._oda = ndbm_module, ndbm_module.Ndbm, None
            self._module = ndbm_module
            super().setUp()

    class DatabaseFilesNdbm(_DatabaseNdbm):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(".".join((self._folder, "db")) in files, True)

    class DoDatabaseTaskNdbm(_DatabaseNdbm):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecNdbm(DoDatabaseTaskNdbm):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecNdbm(DoDatabaseTaskNdbm):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


if gnu_module:

    class _DatabaseGnu(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_gnu"
            self._interface = gnu_database._nosql
            self._oda = gnu_module, gnu_module.Gnu, None
            self._module = gnu_module
            super().setUp()

    class DatabaseFilesGnu(_DatabaseGnu):
        test_01 = t01_database_names
        test_02 = t02_database_names

        def check_database_names(self, names):
            del names
            ae = self.assertEqual
            ae(
                os.path.splitext(
                    os.path.basename(self.database.database_file)
                )[0],
                self._folder,
            )
            files = os.listdir(self.database.home_directory)
            ae(len(files), 1)
            ae(self._folder in files, True)

    class DoDatabaseTaskGnu(_DatabaseGnu):
        def setUp(self):
            super().setUp()
            filespec = self._filespec

            class _AD(self._ED):
                def __init__(self, folder, **k):
                    super().__init__(filespec, folder, **k)

            self._AD = _AD

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DoDatabaseTaskEmptySpecGnu(DoDatabaseTaskGnu):
        def setUp(self):
            self._filespec = empty_filespec
            super().setUp()

        test_01 = t01_do_database_task_empty_spec
        test_02 = t02_do_database_task_empty_spec
        test_03 = t03_do_database_task_empty_spec
        test_04 = t04_do_database_task_empty_spec
        test_05 = t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecGnu(DoDatabaseTaskGnu):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = t01_do_database_task_simple_spec
        test_02 = t02_do_database_task_simple_spec
        test_03 = t03_do_database_task_simple_spec
        test_04 = t04_do_database_task_simple_spec
        test_05 = t05_do_database_task_simple_spec


empty_filespec = {}
simple_filespec = {"file1": {"field1"}}

if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if unqlite:
        runner().run(loader(DatabaseFilesUnqlite))
        runner().run(loader(DoDatabaseTaskEmptySpecUnqlite))
        runner().run(loader(DoDatabaseTaskSimpleSpecUnqlite))
    if vedis:
        runner().run(loader(DatabaseFilesVedis))
        runner().run(loader(DoDatabaseTaskEmptySpecVedis))
        runner().run(loader(DoDatabaseTaskSimpleSpecVedis))
    if bsddb3:
        runner().run(loader(DatabaseFilesBsddb3))
        runner().run(loader(DoDatabaseTaskEmptySpecBsddb3))
        runner().run(loader(DoDatabaseTaskSimpleSpecBsddb3))
    if berkeleydb:
        runner().run(loader(DatabaseFilesBerkeleydb))
        runner().run(loader(DoDatabaseTaskEmptySpecBerkeleydb))
        runner().run(loader(DoDatabaseTaskSimpleSpecBerkeleydb))
    if sqlite3:
        runner().run(loader(DatabaseFilesSqlite3))
        runner().run(loader(DoDatabaseTaskEmptySpecSqlite3))
        runner().run(loader(DoDatabaseTaskSimpleSpecSqlite3))
    if apsw:
        runner().run(loader(DatabaseFilesApsw))
        runner().run(loader(DoDatabaseTaskEmptySpecApsw))
        runner().run(loader(DoDatabaseTaskSimpleSpecApsw))
    if lmdb:
        runner().run(loader(DatabaseFilesLmdb))
        runner().run(loader(DoDatabaseTaskEmptySpecLmdb))
        runner().run(loader(DoDatabaseTaskSimpleSpecLmdb))
    if dptapi:
        runner().run(loader(DatabaseFilesDptapi))
        runner().run(loader(DoDatabaseTaskEmptySpecDptapi))
        runner().run(loader(DoDatabaseTaskSimpleSpecDptapi))
    if ndbm_module:
        runner().run(loader(DatabaseFilesNdbm))
        runner().run(loader(DoDatabaseTaskEmptySpecNdbm))
        runner().run(loader(DoDatabaseTaskSimpleSpecNdbm))
    if gnu_module:
        runner().run(loader(DatabaseFilesGnu))
        runner().run(loader(DoDatabaseTaskEmptySpecGnu))
        runner().run(loader(DoDatabaseTaskSimpleSpecGnu))
