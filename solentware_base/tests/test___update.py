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

        class _D(self._engine.Database):
            pass

        self._D = _D

    def tearDown(self):
        self.database = None
        self._D = None
        SegmentSize.db_segment_size_bytes = self.__ssb

    def t01_open_database__in_directory_txn_generated_filespec(self):
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
            if self._folder != "___update_test_dpt":
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
    # On Windows 10 get KeyError at _nosql.py line 582
    # add_record_to_field_value called from _database.py line 208 put_instance.
    def t02_open_database__in_directory_txn_generated_filespec(self):
        # No cachesize problem for bsddb3 when database is not in memory.
        # Transaction for all records.
        if self._folder == "___update_test_vedis":
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
            if self._folder != "___update_test_dpt":
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


class _DatabaseBerkeley(_Database):
    def tearDown(self):
        super().tearDown()
        if os.path.exists(self._folder):
            logdir = os.path.join(self._folder, "___logs_" + self._folder)
            if os.path.exists(logdir):
                for f in os.listdir(logdir):
                    if f.startswith("log."):
                        os.remove(os.path.join(logdir, f))
                os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


class _DatabaseDpt(_Database):
    def tearDown(self):
        super().tearDown()
        if os.path.exists(self._folder):
            for dptsys in os.path.join("dptsys", "dptsys"), "dptsys":
                logdir = os.path.join(self._folder, dptsys)
                if os.path.exists(logdir):
                    for f in os.listdir(logdir):
                        os.remove(os.path.join(logdir, f))
                    os.rmdir(logdir)
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


class _DatabaseOther(_Database):
    def tearDown(self):
        super().tearDown()
        if os.path.exists(self._folder):
            for f in os.listdir(self._folder):
                os.remove(os.path.join(self._folder, f))
            os.rmdir(self._folder)


if unqlite:

    class _DatabaseUnqlite(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_unqlite"
            self._engine = unqlite_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if vedis:

    class _DatabaseVedis(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_vedis"
            self._engine = vedis_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if bsddb3:

    class _DatabaseBsddb3(_DatabaseBerkeley):
        def setUp(self):
            self._folder = "___update_test_bsddb3"
            self._engine = bsddb3_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if berkeleydb:

    class _DatabaseBerkeleydb(_DatabaseBerkeley):
        def setUp(self):
            self._folder = "___update_test_berkeleydb"
            self._engine = berkeleydb_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if sqlite3:

    class _DatabaseSqlite3(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_sqlite3"
            self._engine = sqlite3_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if apsw:

    class _DatabaseApsw(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_apsw"
            self._engine = apsw_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if lmdb:

    class _DatabaseLmdb(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_lmdb"
            self._engine = lmdb_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if dptapi:

    class _DatabaseDptapi(_DatabaseDpt):
        def setUp(self):
            self._folder = "___update_test_dpt"
            self._engine = dpt_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if ndbm_module:

    class _DatabaseNdbm(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_ndbm"
            self._engine = ndbm_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


if gnu_module:

    class _DatabaseGnu(_DatabaseOther):
        def setUp(self):
            self._folder = "___update_test_gnu"
            self._engine = gnu_database
            super().setUp()

        test_01 = (
            _Database.t01_open_database__in_directory_txn_generated_filespec
        )
        test_02 = (
            _Database.t02_open_database__in_directory_txn_generated_filespec
        )


dg = _data_generator._DataGenerator()
generated_filespec = _data_generator.generate_filespec(dg)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    if unqlite:
        runner().run(loader(_DatabaseUnqlite))
    if vedis:
        runner().run(loader(_DatabaseVedis))
    if bsddb3:
        runner().run(loader(_DatabaseBsddb3))
    if berkeleydb:
        runner().run(loader(_DatabaseBerkeleydb))
    if sqlite3:
        runner().run(loader(_DatabaseSqlite3))
    if apsw:
        runner().run(loader(_DatabaseApsw))
    if lmdb:
        runner().run(loader(_DatabaseLmdb))
    if dptapi:
        runner().run(loader(_DatabaseDptapi))
    if ndbm_module:
        runner().run(loader(_DatabaseNdbm))
    if gnu_module:
        runner().run(loader(_DatabaseGnu))
