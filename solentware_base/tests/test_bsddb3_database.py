# test_bsddb3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""bsddb3_database tests"""

import unittest
import os

try:
    from .. import bsddb3_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3_database = None


class Bsddb3Database(unittest.TestCase):
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
                    "__init__\(\) missing 1 required positional argument: ",
                    "'specification'",
                )
            ),
            bsddb3_database.Database,
        )
        self.assertIsInstance(
            bsddb3_database.Database({}),
            bsddb3_database.Database,
        )

    def test_open_database(self):
        self.assertEqual(bsddb3_database.Database({}).open_database(), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if bsddb3_database is not None:
        runner().run(loader(Bsddb3Database))
