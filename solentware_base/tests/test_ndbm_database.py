# test_ndbm_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""ndbm_database tests"""

import unittest
import os

try:
    from .. import ndbm_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_module = None
try:
    from .. import ndbm_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm_database = None


class NdbmDatabase(unittest.TestCase):
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
            ndbm_database.Database,
        )
        self.assertIsInstance(
            ndbm_database.Database({}),
            ndbm_database.Database,
        )

    def test_open_database_01(self):
        self.assertRaisesRegex(
            ndbm_module.NdbmError,
            "Memory-only databases not supported by dbm.ndbm",
            ndbm_database.Database({}).open_database,
        )

    def test_open_database_02(self):
        path = os.path.join(os.path.dirname("__file__"), "___ndbmtest")
        self.assertEqual(
            ndbm_database.Database({}, path).open_database(), None
        )
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))
        os.rmdir(path)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    # See some kind of unit test failure if either import succeeds.
    if ndbm_module is not None or ndbm_database is not None:
        runner().run(loader(NdbmDatabase))
