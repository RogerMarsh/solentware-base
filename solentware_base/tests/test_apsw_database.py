# test_apsw_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""apsw_database tests"""

import unittest

from .. import apsw_database


class ApswDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            apsw_database.Database,
            )
        self.assertIsInstance(
            apsw_database.Database({}),
            apsw_database.Database,
            )

    def test_open_database(self):
        self.assertEqual(apsw_database.Database({}).open_database(), None)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(ApswDatabase))