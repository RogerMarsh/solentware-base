# test_ndbmdu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""ndbmdu_database tests"""

import unittest

try:
    from .. import ndbmdu_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbmdu_database = None


class NdbmduDatabase(unittest.TestCase):
    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    r"__init__\(\) missing 1 required positional argument: ",
                    "'specification'",
                )
            ),
            ndbmdu_database.Database,
        )
        self.assertIsInstance(
            ndbmdu_database.Database({}), ndbmdu_database.Database
        )

    def test_01_take_backup_before_deferred_update(self):
        self.assertEqual(
            ndbmdu_database.Database({}).take_backup_before_deferred_update,
            True,
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if ndbmdu_database is not None:
        runner().run(loader(NdbmduDatabase))
