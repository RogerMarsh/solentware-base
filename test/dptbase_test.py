# dptbase_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dptbase tests"""

import unittest
from copy import copy, deepcopy

from .. import dptbase


class DPTbase(unittest.TestCase):

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


class DPTbaseFile(unittest.TestCase):

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


class DPTbaseRecord(unittest.TestCase):

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


class CursorDPT(unittest.TestCase):

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


class _CursorDPT(unittest.TestCase):

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
    return unittest.TestLoader().loadTestsFromTestCase(DPTbase)


def suite__dptf():
    return unittest.TestLoader().loadTestsFromTestCase(DPTbaseFile)


def suite__dptr():
    return unittest.TestLoader().loadTestsFromTestCase(DPTbaseRecord)


def suite__cdpt():
    return unittest.TestLoader().loadTestsFromTestCase(CursorDPT)


def suite___cdpt():
    return unittest.TestLoader().loadTestsFromTestCase(_CursorDPT)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__dpt())
    unittest.TextTestRunner(verbosity=2).run(suite__dptf())
    unittest.TextTestRunner(verbosity=2).run(suite__dptr())
    unittest.TextTestRunner(verbosity=2).run(suite__cdpt())
    unittest.TextTestRunner(verbosity=2).run(suite___cdpt())
