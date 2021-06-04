# test___update.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Update databases with sample real data.

The two tests done are copies of similar tests in ..core.tests.test___update
except that the default segement size is used rather than the 'testing' one.

The imported modules, apsw_database for _sqlite and so forth, take care of some
minor differences exposed in the lower lever tests.
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
        self._folder = "___update_test"
        if dbe_module is unqlite:
            self._engine = unqlite_database
        elif dbe_module is vedis:
            self._engine = vedis_database
        elif dbe_module is bsddb3:
            self._engine = bsddb3_database
        elif dbe_module is sqlite3:
            self._engine = sqlite3_database
        elif dbe_module is apsw:
            self._engine = apsw_database
        elif dbe_module is dptapi:
            self._engine = dpt_database
        elif dbe_module is ndbm_module:
            self._engine = ndbm_database
        elif dbe_module is gnu_module:
            self._engine = gnu_database
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

    def test_01_open_database__in_directory_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
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
            _data_generator.populate(self.database, dg)
        finally:
            self.database.close_database()

    # The very first run of this test for vedis gave an error parsing an ebm
    # segment data record starting at _nosql.py line 515 then line 2039: EOL
    # missing?  The repeat was fine but next two failed the same way.
    # Reducing the segment size seems to fix the problem.  Is it a memory limit
    # on OpenBSD? Could just increase the memory limit but adjusting segment
    # size highlights the problem.  Happens on FreeBSD too.
    # On Windows 10 get KeyError at _nosql.py line 582 add_record_to_field_value
    # called from _database.py line 208 put_instance.
    def test_02_open_database__in_directory_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # Transaction for all records.
        if dbe_module is vedis:
            ssb = SegmentSize.db_segment_size_bytes_minimum
        else:
            ssb = 4000
        self.database = self._D(
            generated_filespec,
            folder=self._folder,
            segment_size_bytes=ssb,
        )
        self.database.open_database()
        try:
            self.assertEqual(
                self.database.home_directory,
                os.path.join(os.getcwd(), self._folder),
            )
            if dbe_module is not dptapi:
                self.assertEqual(SegmentSize.db_segment_size_bytes, ssb)
                self.assertEqual(
                    self.database.database_file,
                    os.path.join(os.getcwd(), self._folder, self._folder),
                )
            self.database.start_transaction()
            _data_generator.populate(self.database, dg, transaction=False)
            self.database.commit()
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
