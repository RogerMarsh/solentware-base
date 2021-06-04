# test_gnu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""gnu_database tests"""

import unittest
import os

try:
    from .. import gnu_module
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_module = None
try:
    from .. import gnu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu_database = None


class GnuDatabase(unittest.TestCase):
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
            gnu_database.Database,
        )
        self.assertIsInstance(
            gnu_database.Database({}),
            gnu_database.Database,
        )

    def test_open_database_01(self):
        self.assertRaisesRegex(
            gnu_module.GnuError,
            "Memory-only databases not supported by dbm.gnu",
            gnu_database.Database({}).open_database,
        )

    def test_open_database_02(self):
        path = os.path.join(os.path.dirname("__file__"), "___gnutest")
        self.assertEqual(gnu_database.Database({}, path).open_database(), None)
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))
        os.rmdir(path)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    # See some kind of unit test failure if either import succeeds.
    if gnu_module is not None or gnu_database is not None:
        runner().run(loader(GnuDatabase))
