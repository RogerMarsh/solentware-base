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
    from dptdb import dptapi
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
        self.__ssb = SegmentSize.db_segment_size_bytes
        oda = self._oda

        class _ED(self._interface.Database):
            def open_database(self, **k):
                super().open_database(*oda, **k)

        self._ED = _ED

    def tearDown(self):
        self.database = None
        self._ED = None
        SegmentSize.db_segment_size_bytes = self.__ssb
        if os.path.exists(self._folder):
            if self._folder in ("___update_test_bsddb3", "___update_test_berkeleydb"):
                logdir = os.path.join(self._folder, "___logs_" + self._folder)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        if f.startswith("log."):
                            os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            if self._folder == "___update_test_dpt":
                for dptsys in os.path.join("dptsys", "dptsys"), "dptsys":
                    logdir = os.path.join(self._folder, dptsys)
                    if os.path.exists(logdir):
                        for f in os.listdir(logdir):
                            os.remove(os.path.join(logdir, f))
                        os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)

    def task(self, *a, **k):
        return


class DatabaseFiles(_Database):
    def t01_database_names(self):
        ae = self.assertEqual
        self.database = self._ED(
            empty_filespec, folder=self._folder, segment_size_bytes=None
        )
        ae(os.path.exists(self.database.home_directory), False)
        self.database.open_database()
        ae(os.path.exists(self.database.home_directory), True)
        ae(os.path.basename(self.database.home_directory), self._folder)
        ae(
            os.path.splitext(os.path.basename(self.database.database_file))[0],
            self._folder,
        )
        files = os.listdir(self.database.home_directory)
        if self._folder in ("___update_test_bsddb3", "___update_test_berkeleydb"):
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
        elif self._oda[0] is lmdb:
            ae(self._folder + "-lock" in files, True)
            ae(len(files), 2)
        else:
            ae(len(files), 1)
        if self._oda[0] is ndbm_module:
            ae(".".join((self._folder, "db")) in files, True)
        else:
            ae(self._folder in files, True)

    def t02_database_names(self):
        ae = self.assertEqual
        self.database = self._ED(
            simple_filespec, folder=self._folder, segment_size_bytes=None
        )
        ae(os.path.exists(self.database.home_directory), False)
        self.database.open_database()
        ae(os.path.exists(self.database.home_directory), True)
        ae(os.path.basename(self.database.home_directory), self._folder)
        ae(
            os.path.splitext(os.path.basename(self.database.database_file))[0],
            self._folder,
        )
        files = os.listdir(self.database.home_directory)
        if self._folder in ("___update_test_bsddb3", "___update_test_berkeleydb"):
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
        elif self._oda[0] is lmdb:
            ae(self._folder + "-lock" in files, True)
            ae(len(files), 2)
        else:
            ae(len(files), 1)
        if self._oda[0] is ndbm_module:
            ae(".".join((self._folder, "db")) in files, True)
        else:
            ae(self._folder in files, True)


class DoDatabaseTaskEmptySpec(_Database):
    def setUp(self):
        super().setUp()

        class _AD(self._ED):
            def __init__(self, folder, **k):
                super().__init__(empty_filespec, folder, **k)

        self._AD = _AD

    def tearDown(self):
        self._AD = None
        super().tearDown()

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


class DoDatabaseTaskSimpleSpec(_Database):
    def setUp(self):
        super().setUp()

        class _AD(self._ED):
            def __init__(self, folder, **k):
                super().__init__(simple_filespec, folder, **k)

        self._AD = _AD

    def tearDown(self):
        self._AD = None
        super().tearDown()

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


if unqlite:

    class _DatabaseUnqlite(_Database):
        def setUp(self):
            self._folder = "___update_test_unqlite"
            self._interface = unqlite_database._nosql
            self._oda = unqlite, unqlite.UnQLite, unqlite.UnQLiteError
            super().setUp()

    class DatabaseFilesUnqlite(_DatabaseUnqlite):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecUnqlite(DoDatabaseTaskUnqlite):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if vedis:

    class _DatabaseVedis(_Database):
        def setUp(self):
            self._folder = "___update_test_vedis"
            self._interface = vedis_database._nosql
            self._oda = vedis, vedis.Vedis, None
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesVedis(_DatabaseVedis):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecVedis(DoDatabaseTaskVedis):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if bsddb3:

    class _DatabaseBsddb3(_Database):
        def setUp(self):
            self._folder = "___update_test_bsddb3"
            self._interface = bsddb3_database._db
            self._oda = (bsddb3.db,)
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesBsddb3(_DatabaseBsddb3):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecBsddb3(DoDatabaseTaskBsddb3):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if berkeleydb:

    class _DatabaseBerkeleydb(_Database):
        def setUp(self):
            self._folder = "___update_test_berkeleydb"
            self._interface = berkeleydb_database._db
            self._oda = (berkeleydb.db,)
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesBerkeleydb(_DatabaseBerkeleydb):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecBerkeleydb(DoDatabaseTaskBerkeleydb):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if sqlite3:

    class _DatabaseSqlite3(_Database):
        def setUp(self):
            self._folder = "___update_test_sqlite3"
            self._interface = sqlite3_database._sqlite
            self._oda = (sqlite3,)
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesSqlite3(_DatabaseSqlite3):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecSqlite3(DoDatabaseTaskSqlite3):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if apsw:

    class _DatabaseApsw(_Database):
        def setUp(self):
            self._folder = "___update_test_apsw"
            self._interface = apsw_database._sqlite
            self._oda = (apsw,)
            super().setUp()

    class DatabaseFilesApsw(_DatabaseApsw):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecApsw(DoDatabaseTaskApsw):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if lmdb:

    class _DatabaseLmdb(_Database):
        def setUp(self):
            self._folder = "___update_test_lmdb"
            self._interface = lmdb_database._lmdb
            self._oda = (lmdb,)
            super().setUp()

    class DatabaseFilesLmdb(_DatabaseLmdb):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecLmdb(DoDatabaseTaskLmdb):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if dptapi:

    class _DatabaseDptapi(_Database):
        def setUp(self):
            self._folder = "___update_test_dpt"
            self._interface = dpt_database._dpt
            self._oda = (dptapi,)  # Not sure if this is complete.
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesDptapi(_DatabaseDptapi):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecDptapi(DoDatabaseTaskDptapi):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if ndbm_module:

    class _DatabaseNdbm(_Database):
        def setUp(self):
            self._folder = "___update_test_ndbm"
            self._interface = ndbm_database._nosql
            self._oda = ndbm_module, ndbm_module.Ndbm, None
            super().setUp()

        def tearDown(self):
            self._AD = None
            super().tearDown()

    class DatabaseFilesNdbm(_DatabaseNdbm):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecNdbm(DoDatabaseTaskNdbm):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec

if gnu_module:

    class _DatabaseGnu(_Database):
        def setUp(self):
            self._folder = "___update_test_gnu"
            self._interface = gnu_database._nosql
            self._oda = gnu_module, gnu_module.Gnu, None
            super().setUp()

    class DatabaseFilesGnu(_DatabaseGnu):
        test_01 = DatabaseFiles.t01_database_names
        test_02 = DatabaseFiles.t02_database_names

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

        test_01 = DoDatabaseTaskEmptySpec.t01_do_database_task_empty_spec
        test_02 = DoDatabaseTaskEmptySpec.t02_do_database_task_empty_spec
        test_03 = DoDatabaseTaskEmptySpec.t03_do_database_task_empty_spec
        test_04 = DoDatabaseTaskEmptySpec.t04_do_database_task_empty_spec
        test_05 = DoDatabaseTaskEmptySpec.t05_do_database_task_empty_spec

    class DoDatabaseTaskSimpleSpecGnu(DoDatabaseTaskGnu):
        def setUp(self):
            self._filespec = simple_filespec
            super().setUp()

        test_01 = DoDatabaseTaskSimpleSpec.t01_do_database_task_simple_spec
        test_02 = DoDatabaseTaskSimpleSpec.t02_do_database_task_simple_spec
        test_03 = DoDatabaseTaskSimpleSpec.t03_do_database_task_simple_spec
        test_04 = DoDatabaseTaskSimpleSpec.t04_do_database_task_simple_spec
        test_05 = DoDatabaseTaskSimpleSpec.t05_do_database_task_simple_spec


empty_filespec = {}
simple_filespec = {"file1": {"field1"}}

if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if unqlite:
        runner().run(loader(_DatabaseFilesUnqlite))
        runner().run(loader(_DatabaseTaskEmptySpecUnqlite))
        runner().run(loader(_DatabaseTaskSimpleSpecUnqlite))
    if vedis:
        runner().run(loader(_DatabaseFilesVedis))
        runner().run(loader(_DatabaseTaskEmptySpecVedis))
        runner().run(loader(_DatabaseTaskSimpleSpecVedis))
    if bsddb3:
        runner().run(loader(_DatabaseFilesBsddb3))
        runner().run(loader(_DatabaseTaskEmptySpecBsddb3))
        runner().run(loader(_DatabaseTaskSimpleSpecBsddb3))
    if berkeleydb:
        runner().run(loader(_DatabaseFilesBerkeleydb))
        runner().run(loader(_DatabaseTaskEmptySpecBerkeleydb))
        runner().run(loader(_DatabaseTaskSimpleSpecBerkeleydb))
    if sqlite3:
        runner().run(loader(_DatabaseFilesSqlite3))
        runner().run(loader(_DatabaseTaskEmptySpecSqlite3))
        runner().run(loader(_DatabaseTaskSimpleSpecSqlite3))
    if apsw:
        runner().run(loader(_DatabaseFilesApsw))
        runner().run(loader(_DatabaseTaskEmptySpecApsw))
        runner().run(loader(_DatabaseTaskSimpleSpecApsw))
    if lmdb:
        runner().run(loader(_DatabaseFilesLmdb))
        runner().run(loader(_DatabaseTaskEmptySpecLmdb))
        runner().run(loader(_DatabaseTaskSimpleSpecLmdb))
    if dptapi:
        runner().run(loader(_DatabaseFilesDptapi))
        runner().run(loader(_DatabaseTaskEmptySpecDptapi))
        runner().run(loader(_DatabaseTaskSimpleSpecDptapi))
    if ndbm_module:
        runner().run(loader(_DatabaseFilesNdbm))
        runner().run(loader(_DatabaseTaskEmptySpecNdbm))
        runner().run(loader(_DatabaseTaskSimpleSpecNdbm))
    if gnu_module:
        runner().run(loader(_DatabaseFilesGnu))
        runner().run(loader(_DatabaseTaskEmptySpecGnu))
        runner().run(loader(_DatabaseTaskSimpleSpecGnu))
