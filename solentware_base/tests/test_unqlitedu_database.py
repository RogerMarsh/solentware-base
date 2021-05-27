# test_unqlitedu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""unqlitedu_database tests"""

import unittest

from .. import unqlitedu_database


class UnqliteduDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            unqlitedu_database.Database,
            )
        self.assertIsInstance(unqlitedu_database.Database({}),
                              unqlitedu_database.Database)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(UnqliteduDatabase))
