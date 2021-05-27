# test_unqlite_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""unqlite_database tests"""

import unittest

from .. import unqlite_database


class UnqliteDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            unqlite_database.Database,
            )
        self.assertIsInstance(
            unqlite_database.Database({}),
            unqlite_database.Database,
            )

    def test_open_database(self):
        self.assertEqual(unqlite_database.Database({}).open_database(), None)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(UnqliteDatabase))
