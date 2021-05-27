# test_bsddb3_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""bsddb3_database tests"""

import unittest

from .. import bsddb3_database


class Bsddb3Database(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            bsddb3_database.Database,
            )
        self.assertIsInstance(
            bsddb3_database.Database({}),
            bsddb3_database.Database,
            )

    def test_open_database(self):
        self.assertEqual(bsddb3_database.Database({}).open_database(), None)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(Bsddb3Database))
