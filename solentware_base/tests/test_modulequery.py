# test_modulequery.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""do_deferred_updates tests"""

import unittest
import sys
import os

from .. import modulequery


class Modulequery(unittest.TestCase):
    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "installed_database_modules\(\) takes 0 ",
                    "positional arguments but 1 was given",
                )
            ),
            modulequery.installed_database_modules,
            *(None,),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "installed_database_modules\(\) got an unexpected ",
                    "keyword argument 'xxxxx'",
                )
            ),
            modulequery.installed_database_modules,
            **dict(xxxxx=None),
        )
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "modules_for_existing_databases\(\) missing 2 required ",
                    "positional arguments: 'folder' and 'filespec'",
                )
            ),
            modulequery.modules_for_existing_databases,
        )

    def test_database_modules_in_default_preference_order(self):
        r = modulequery.DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER
        if sys.version_info.major < 3 or (
            sys.version_info.major == 3 and sys.version_info.minor < 6
        ):
            if sys.platform == "win32":
                self.assertEqual(
                    r,
                    (
                        "dptdb.dptapi",
                        "berkeleydb",
                        "bsddb3",
                        "vedis",
                        "unqlite",
                        "apsw",
                    ),
                )
            else:
                self.assertEqual(
                    r,
                    (
                        "berkeleydb",
                        "bsddb3",
                        "vedis",
                        "unqlite",
                        "apsw",
                        "dbm.gnu",
                        "dbm.ndbm",
                    ),
                )
        else:
            if sys.platform == "win32":
                self.assertEqual(
                    r,
                    (
                        "dptdb.dptapi",
                        "berkeleydb",
                        "bsddb3",
                        "vedis",
                        "unqlite",
                        "apsw",
                        "sqlite3",
                    ),
                )
            else:
                self.assertEqual(
                    r,
                    (
                        "berkeleydb",
                        "bsddb3",
                        "vedis",
                        "unqlite",
                        "apsw",
                        "sqlite3",
                        "dbm.gnu",
                        "dbm.ndbm",
                    ),
                )

    def test_installed_database_modules(self):
        # r depends on what's installed, and successfully imported.
        r = modulequery.installed_database_modules()
        self.assertIsInstance(r, dict)
        self.assertEqual(
            set(r).intersection(
                (
                    "apsw",
                    "berkeleydb",
                    "bsddb3",
                    "dbm.gnu",
                    "dbm.ndbm",
                    "dptdb.dptapi",
                    "sqlite3",
                    "unqlite",
                    "vedis",
                )
            ),
            set(r),
        )
        m = {
            i
            for i in (
                modulequery.apsw,
                modulequery.berkeleydb,
                modulequery.bsddb3,
                modulequery.gnu,
                modulequery.ndbm,
                modulequery.dptapi,
                modulequery.sqlite3,
                modulequery.unqlite,
                modulequery.vedis,
            )
            if i
        }
        self.assertEqual(len(m) >= len(r), True)
        if len(m) > len(r):
            if modulequery.berkeleydb in m and modulequery.bsddb3 in m:
                self.assertEqual("berkeleydb" in r, True)
                self.assertEqual("bsddb3" not in r, True)
            if modulequery.apsw in m and modulequery.sqlite3 in m:
                self.assertEqual("sqlite3" not in r, True)
                self.assertEqual("apsw" in r, True)

    def test_modules_for_existing_databases(self):
        # r depends on what's installed, and the existence of a file structure
        # which could have been created by one of these modules.
        # The assertIs means the search has successfully found no modules able
        # to access the non-existent database in os.path.dirname(__file__).
        r = modulequery.modules_for_existing_databases(
            os.path.dirname(__file__), {}
        )
        self.assertIs(r, None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Modulequery))
