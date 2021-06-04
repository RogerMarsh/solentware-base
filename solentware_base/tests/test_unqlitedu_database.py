# test_unqlitedu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""unqlitedu_database tests"""

import unittest

try:
    from .. import unqlitedu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlitedu_database = None


class UnqliteduDatabase(unittest.TestCase):
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
            unqlitedu_database.Database,
        )
        self.assertIsInstance(
            unqlitedu_database.Database({}), unqlitedu_database.Database
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if unqlitedu_database is not None:
        runner().run(loader(UnqliteduDatabase))
