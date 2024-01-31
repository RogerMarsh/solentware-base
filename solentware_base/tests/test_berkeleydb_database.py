# test_berkeleydb_database.py
# Copyright 2021 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""berkeleydb_database tests"""

import unittest
import os

try:
    from .. import berkeleydb_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb_database = None


class BerkeleydbDatabase(unittest.TestCase):
    def tearDown(self):
        logdir = "___memlogs_memory_db"
        if os.path.exists(logdir):
            for f in os.listdir(logdir):
                if f.startswith("log."):
                    os.remove(os.path.join(logdir, f))
            os.rmdir(logdir)

    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'specification'$",
                )
            ),
            berkeleydb_database.Database,
        )
        self.assertIsInstance(
            berkeleydb_database.Database({}),
            berkeleydb_database.Database,
        )

    def test_open_database(self):
        self.assertEqual(
            berkeleydb_database.Database({}).open_database(), None
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if berkeleydb_database is not None:
        runner().run(loader(BerkeleydbDatabase))
