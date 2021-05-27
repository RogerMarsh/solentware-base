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
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "database_modules_in_default_preference_order\(\) takes 0 ",
                "positional arguments but 1 was given",
                )),
            modulequery.database_modules_in_default_preference_order,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "supported_database_modules\(\) takes 0 positional ",
                "arguments but 1 was given",
                )),
            modulequery.supported_database_modules,
            *(None,),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "installed_database_modules\(\) takes from 0 to 2 ",
                "positional arguments but 3 were given",
                )),
            modulequery.installed_database_modules,
            *(None, None, None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "installed_database_modules\(\) got an unexpected ",
                "keyword argument 'xxxxx'",
                )),
            modulequery.installed_database_modules,
            **dict(xxxxx=None),
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "modules_for_existing_databases\(\) missing 2 required ",
                "positional arguments: 'folder' and 'filespec'",
                )),
            modulequery.modules_for_existing_databases,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "_bsddb_preference\(\) missing 2 required positional ",
                "arguments: 'mapping' and 'bsddb_before_bsddb3'",
                )),
            modulequery._bsddb_preference,
            )
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "_sqlite_preference\(\) missing 2 required positional ",
                "arguments: 'mapping' and 'apsw_before_sqlite3'",
                )),
            modulequery._sqlite_preference,
            )

    def test_database_modules_in_default_preference_order(self):
        # r depends an what's installed and operating system.
        r = modulequery.database_modules_in_default_preference_order()
        if sys.platform == 'win32':
            self.assertEqual(
                r, ('dptdb.dptapi', 'bsddb3', 'apsw', 'sqlite3', 'bsddb'))
        else:
            self.assertEqual(r, ('bsddb3', 'apsw', 'sqlite3', 'bsddb'))

    def test_supported_database_modules(self):
        # r depends an what's installed and operating system.
        r = modulequery.supported_database_modules()
        self.assertIsInstance(r, dict)
        self.assertEqual(sorted(r.keys()),
                         ['apsw',
                          'bsddb',
                          'bsddb3',
                          'dptdb.dptapi',
                          'sqlite3',
                          ])

    def test_installed_database_modules(self):
        # r depends an what's installed and operating system.
        r = modulequery.installed_database_modules()
        self.assertIsInstance(r, set)
        self.assertEqual(r.intersection(('apsw',
                                         'bsddb',
                                         'bsddb3',
                                         'dptdb.dptapi',
                                         'sqlite3',
                                         )),
                         r)

    def test_modules_for_existing_databases(self):
        # r depends an what's installed and operating system, and the
        # existence of a file structure which could have been created by
        # one of these modules.
        r = modulequery.modules_for_existing_databases(
            os.path.dirname(__file__), {})
        self.assertIsInstance(r, list)

    def test__bsddb_preference_01(self):
        map_ = {'bsddb3':True, 'bsddb':True}
        modulequery._bsddb_preference(map_, None)
        self.assertEqual(map_, {'bsddb3':True, 'bsddb':False})

    def test__bsddb_preference_02(self):
        map_ = {'bsddb3':True, 'bsddb':None}
        modulequery._bsddb_preference(map_, None)
        self.assertEqual(map_, {'bsddb3':True, 'bsddb':None})


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Modulequery))
