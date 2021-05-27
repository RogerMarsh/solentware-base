# test_sqlite3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""sqlite3_database tests"""

import unittest

from .. import sqlite3_database


class SqliteDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            sqlite3_database.Database,
            )
        self.assertIsInstance(
            sqlite3_database.Database({}),
            sqlite3_database.Database,
            )

    def test_open_database(self):
        self.assertEqual(sqlite3_database.Database({}).open_database(), None)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(SqliteDatabase))
