# test_ndbmdu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""ndbmdu_database tests"""

import unittest

from .. import ndbmdu_database


class NdbmduDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            ndbmdu_database.Database,
            )
        self.assertIsInstance(ndbmdu_database.Database({}),
                              ndbmdu_database.Database)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(NdbmduDatabase))
