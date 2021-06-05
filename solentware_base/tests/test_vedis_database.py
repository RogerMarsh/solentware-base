# test_vedis_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""vedis_database tests"""

import unittest

try:
    from .. import vedis_database
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis_database = None


class VedisDatabase(unittest.TestCase):
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
            vedis_database.Database,
        )
        self.assertIsInstance(
            vedis_database.Database({}),
            vedis_database.Database,
        )

    def test_open_database(self):
        self.assertEqual(vedis_database.Database({}).open_database(), None)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if vedis_database is not None:
        runner().run(loader(VedisDatabase))
