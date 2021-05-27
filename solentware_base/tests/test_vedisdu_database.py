# test_vedisdu_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""vedisdu_database tests"""

import unittest

from .. import vedisdu_database
try:
    from .. import vedisdu_database
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedisdu_database = None


class VedisduDatabase(unittest.TestCase):

    def test__assumptions(self):
        msg = 'Failure of this test invalidates all other tests'
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 1 required positional argument: ",
                "'specification'",
                )),
            vedisdu_database.Database,
            )
        self.assertIsInstance(vedisdu_database.Database({}),
                              vedisdu_database.Database)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    if vedisdu_database is not None:
        runner().run(loader(VedisduDatabase))
