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
    import sqlite3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None
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
    from .. import bsddb3_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3_database = None
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
        self._folder = "___do_database_tasks"
        if dbe_module is unqlite:
            interface = unqlite_database._nosql
            _oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
        elif dbe_module is vedis:
            interface = vedis_database._nosql
            _oda = dbe_module, dbe_module.Vedis, None
        elif dbe_module is bsddb3:
            interface = bsddb3_database._db
            _oda = (dbe_module.db,)
        elif dbe_module is sqlite3:
            interface = sqlite3_database._sqlite
            _oda = (dbe_module,)
        elif dbe_module is apsw:
            interface = apsw_database._sqlite
            _oda = (dbe_module,)
        elif dbe_module is dptapi:
            interface = dpt_database._dpt
            _oda = (dbe_module,)  # Not sure if this is complete.
        elif dbe_module is ndbm_module:
            interface = ndbm_database._nosql
            _oda = dbe_module, dbe_module.Ndbm, None
        elif dbe_module is gnu_module:
            interface = gnu_database._nosql
            _oda = dbe_module, dbe_module.Gnu, None
        self.__ssb = SegmentSize.db_segment_size_bytes

        class _ED(interface.Database):
            def open_database(self, **k):
                super().open_database(*_oda, **k)

        self._ED = _ED

    def tearDown(self):
        self.database = None
        self._ED = None
        SegmentSize.db_segment_size_bytes = self.__ssb
        if os.path.exists(self._folder):
            if dbe_module is bsddb3:
                logdir = os.path.join(self._folder, "___logs_" + self._folder)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        if f.startswith("log."):
                            os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            if dbe_module is dptapi:
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
    def test_01_database_names(self):
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
        if dbe_module is bsddb3:
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
        else:
            ae(len(files), 1)
        if dbe_module is ndbm_module:
            ae(".".join((self._folder, "db")) in files, True)
        else:
            ae(self._folder in files, True)

    def test_02_database_names(self):
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
        if dbe_module is bsddb3:
            ae("___logs_" + self._folder in files, True)
            ae(len(files), 2)
        else:
            ae(len(files), 1)
        if dbe_module is ndbm_module:
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

    def test_01_do_database_task_empty_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        ae(self.database.do_database_task(self.task), None)

    def test_02_do_database_task_empty_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        self.database.close_database()
        ae(self.database.do_database_task(self.task), None)

    def test_03_do_database_task_empty_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        ae(self.database.do_database_task(self.task), None)

    def test_04_do_database_task_empty_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        ae(self.database.do_database_task(self.task), None)
        self.database.close_database()

    def test_05_do_database_task_empty_spec(self):
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

    def test_01_do_database_task_simple_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        ae(self.database.do_database_task(self.task), None)

    def test_02_do_database_task_simple_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        self.database.close_database()
        ae(self.database.do_database_task(self.task), None)

    def test_03_do_database_task_simple_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        ae(self.database.do_database_task(self.task), None)

    def test_04_do_database_task_simple_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        self.database.open_database()
        ae(self.database.do_database_task(self.task), None)
        self.database.close_database()

    def test_05_do_database_task_simple_spec(self):
        ae = self.assertEqual
        self.database = self._AD(folder=self._folder)
        ae(self.database.do_database_task(self.task), None)
        self.database.open_database()
        self.database.close_database()


if __name__ == "__main__":
    empty_filespec = {}
    simple_filespec = {"file1": {"field1"}}
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in (
        unqlite,
        vedis,
        bsddb3,
        sqlite3,
        apsw,
        dptapi,
        ndbm_module,
        gnu_module,
    ):
        if dbe_module is None:
            continue
        runner().run(loader(DatabaseFiles))
        runner().run(loader(DoDatabaseTaskEmptySpec))
        runner().run(loader(DoDatabaseTaskSimpleSpec))
