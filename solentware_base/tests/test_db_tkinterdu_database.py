# test_db_tkinterdu_database.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""db_tkinterdu_database tests."""

import unittest

try:
    from .. import db_tkinterdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    db_tkinterdu_database = None


class DbTkinterduDatabase(unittest.TestCase):
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
            db_tkinterdu_database.Database,
        )
        self.assertIsInstance(
            db_tkinterdu_database.Database({}), db_tkinterdu_database.Database
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if db_tkinterdu_database is not None:
        runner().run(loader(DbTkinterduDatabase))
