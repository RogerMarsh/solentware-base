# test_berkeleydbdu_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""berkeleydbdu_database tests"""

import unittest

try:
    from .. import berkeleydbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydbdu_database = None


if berkeleydbdu_database is not None:

    class BerkeleydbduDatabase(unittest.TestCase):
        def test__assumptions(self):
            msg = "Failure of this test invalidates all other tests"
            self.assertRaisesRegex(
                TypeError,
                "".join(
                    (
                        r"__init__\(\) missing 1 required positional ",
                        "argument: 'specification'$",
                    )
                ),
                berkeleydbdu_database.Database,
            )
            self.assertIsInstance(
                berkeleydbdu_database.Database({}),
                berkeleydbdu_database.Database,
            )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if berkeleydbdu_database is not None:
        runner().run(loader(BerkeleydbduDatabase))
