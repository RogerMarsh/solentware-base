# filespec_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""filespec tests"""

import unittest
from copy import copy, deepcopy

from .. import filespec


class FileSpec(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__raises(self):
        """"""
        pass

    def test__copy(self):
        """"""
        pass

    def test__assumptions(self):
        """"""
        msg = 'Failure of this test invalidates all other tests'


def suite__fs():
    return unittest.TestLoader().loadTestsFromTestCase(FileSpec)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__fs())
