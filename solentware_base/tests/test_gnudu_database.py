# test_gnudu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""gnudu_database tests"""

import unittest

try:
    from .. import gnudu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnudu_database = None


class GnuduDatabase(unittest.TestCase):
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
            gnudu_database.Database,
        )
        self.assertIsInstance(
            gnudu_database.Database({}), gnudu_database.Database
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if gnudu_database is not None:
        runner().run(loader(GnuduDatabase))
