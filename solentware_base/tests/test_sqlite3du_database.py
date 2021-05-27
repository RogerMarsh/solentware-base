# test_sqlite3du_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""sqlite3du_database tests"""

import unittest

from .. import sqlite3du_database


class Sqlite3duDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            sqlite3du_database.Database,
            )
        self.assertIsInstance(sqlite3du_database.Database({}),
                              sqlite3du_database.Database)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Sqlite3duDatabase))