# test___deferred_update.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Update databases in deferred update mode with sample real data.

The one tests done is a copy of the similar test in ..core.tests.test___update
except that the default segement size is used rather than the 'testing' one.

The imported modules, apswdu_database for _sqlitedu and so forth, take care of
some minor differences exposed in the lower lever tests.
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
    from .. import unqlitedu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlitedu_database = None
try:
    from .. import vedisdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedisdu_database = None
try:
    from .. import sqlite3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    sqlite3du_database = None
try:
    from .. import apswdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apswdu_database = None
try:
    from .. import bsddb3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3du_database = None
try:
    from .. import dptdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptdu_database = None
try:
    from .. import ndbmdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbmdu_database = None
try:
    from .. import gnudu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnudu_database = None


class _Database(unittest.TestCase):
    def setUp(self):
        self._folder = "___update_test"
        if dbe_module is unqlite:
            self._engine = unqlitedu_database
        elif dbe_module is vedis:
            self._engine = vedisdu_database
        elif dbe_module is bsddb3:
            self._engine = bsddb3du_database
        elif dbe_module is sqlite3:
            self._engine = sqlite3du_database
        elif dbe_module is apsw:
            self._engine = apswdu_database
        elif dbe_module is dptapi:
            self._engine = dptdu_database
        elif dbe_module is ndbm_module:
            self._engine = ndbmdu_database
        elif dbe_module is gnu_module:
            self._engine = gnudu_database
        self.__ssb = SegmentSize.db_segment_size_bytes

        class _D(self._engine.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
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

    def test_01_open_database__in_directory_no_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # No problem on OpenBSD for vedis as in .test___update module.
        # Transaction for each record.
        self.database = self._D(
            generated_filespec,
            folder=self._folder,
        )
        self.database.open_database()
        try:
            self.assertEqual(
                self.database.home_directory,
                os.path.join(os.getcwd(), self._folder),
            )
            if dbe_module is not dptapi:
                self.assertEqual(SegmentSize.db_segment_size_bytes, 4000)
                self.assertEqual(
                    self.database.database_file,
                    os.path.join(os.getcwd(), self._folder, self._folder),
                )
            self.database.set_defer_update()
            _data_generator.populate(self.database, dg, transaction=False)
            self.database.unset_defer_update()
        finally:
            self.database.close_database()


if __name__ == "__main__":
    dg = _data_generator._DataGenerator()
    generated_filespec = _data_generator.generate_filespec(dg)
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
        runner().run(loader(_Database))
