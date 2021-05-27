# constants_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""constants tests"""

import unittest
from copy import copy, deepcopy

from .. import constants


class ConstantsFunctions(unittest.TestCase):

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


def suite__cf():
    return unittest.TestLoader().loadTestsFromTestCase(ConstantsFunctions)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__cf())
