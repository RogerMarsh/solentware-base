# test_lmdb_database.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""lmdb_database tests."""

import unittest
import os

try:
    from .. import lmdb_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdb_database = None


class LmdbDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'specification'",
                )
            ),
            lmdb_database.Database,
        )
        self.assertIsInstance(
            lmdb_database.Database({}),
            lmdb_database.Database,
        )

    def test_open_database(self):
        database = lmdb_database.Database({})
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"mkdir: path should be string, bytes or os.PathLike, ",
                    "not NoneType",
                )
            ),
            database.open_database,
        )
        self.assertEqual(database.home_directory, None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if lmdb_database is not None:
        runner().run(loader(LmdbDatabase))
