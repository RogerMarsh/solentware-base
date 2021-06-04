# test_apswdu_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""apswdu_database tests"""

import unittest

try:
    from .. import apswdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apswdu_database = None


class ApswduDatabase(unittest.TestCase):
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
            apswdu_database.Database,
        )
        self.assertIsInstance(
            apswdu_database.Database({}), apswdu_database.Database
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if apswdu_database is not None:
        runner().run(loader(ApswduDatabase))
