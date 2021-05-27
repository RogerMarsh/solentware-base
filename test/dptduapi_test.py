# dptduapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dptduapi tests"""

import unittest
from copy import copy, deepcopy

from .. import dptduapi


class DPTduapi(unittest.TestCase):

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


class DPTRecord(unittest.TestCase):

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


def suite__dpt():
    return unittest.TestLoader().loadTestsFromTestCase(DPTduapi)


def suite__dptr():
    return unittest.TestLoader().loadTestsFromTestCase(DPTRecord)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__dpt())
    unittest.TextTestRunner(verbosity=2).run(suite__dptr())
