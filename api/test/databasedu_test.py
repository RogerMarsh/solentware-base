# databasedu_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""databasedu tests.  Only the simplest of tests are done in this module: the
put_instance tests, in particular, are done in apswduapi_test, dbduapi_test,
dptduapi_test, and sqlite3duapi_test, all in solentware_base.test.
"""

import unittest
import os

from .. import databasedu


class Database_00(unittest.TestCase):

    def test___init___(self):
        d = databasedu.Database()
        self.assertEqual(d.__dict__, {})
        self.assertEqual(
            [a for a in dir(d) if not a.startswith('__')],
            ['_get_deferable_update_files',
             'close_context',
             'open_context',
             'put_instance',
             'use_deferred_update_process',
             ])


class Database_01(unittest.TestCase):
    # Indicate the attributes expected elsewhere in the class hierarchy.

    def setUp(self):
        class C:
            def open_root(self, *a):
                pass
            def close(self):
                pass
        class D:
            def __init__(self):
                self._control = C()
                self.dbservices = None
            def close_context(self):
                pass
            def open_context(self):
                pass
        class E(databasedu.Database, D):
            pass
        self.e = E()

    def tearDown(self):
        self.e = None

    def test_close_context(self):
        self.assertEqual(self.e.close_context(), None)

    def test_open_context(self):
        self.assertEqual(self.e.open_context(), True)

    def test_use_deferred_update_process(self):
        self.assertRaisesRegex(
            databasedu.DatabaseduError,
            "Query use of du when in deferred update mode",
            self.e.use_deferred_update_process)


if __name__ == '__main__':
    unittest.main()
