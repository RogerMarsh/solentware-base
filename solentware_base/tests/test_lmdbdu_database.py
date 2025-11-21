# test_lmdbdu_database.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""lmdbdu_database tests."""

import unittest

try:
    from .. import lmdbdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    lmdbdu_database = None


if lmdbdu_database is not None:

    class LmdbduDatabase(unittest.TestCase):
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
                lmdbdu_database.Database,
            )
            self.assertIsInstance(
                lmdbdu_database.Database({}), lmdbdu_database.Database
            )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if lmdbdu_database is not None:
        runner().run(loader(LmdbduDatabase))
