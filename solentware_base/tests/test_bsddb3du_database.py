# test_bsddb3du_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""bsddb3du_database tests"""

import unittest

try:
    from .. import bsddb3du_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3du_database = None


class Bsddb3duDatabase(unittest.TestCase):
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
            bsddb3du_database.Database,
        )
        self.assertIsInstance(
            bsddb3du_database.Database({}), bsddb3du_database.Database
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if bsddb3du_database is not None:
        runner().run(loader(Bsddb3duDatabase))
