# dptdumultiapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dptdumultiapi tests"""

import unittest
from copy import copy, deepcopy

from .. import dptdumultiapi


class DPTdumultiapi(unittest.TestCase):

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


class _DPTdumultiDeferBase(unittest.TestCase):

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


class _DPTdumultiNoPadNoCRLF(unittest.TestCase):

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


class _DPTdumultiNoPadNoCRLFOrdChar(unittest.TestCase):

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


class _DPTdumultiNoPadNoCRLFOrdNum(unittest.TestCase):

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


class _rmFile(unittest.TestCase):

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
    return unittest.TestLoader().loadTestsFromTestCase(DPTdumultiapi)


def suite__dptr():
    return unittest.TestLoader().loadTestsFromTestCase(DPTRecord)


def suite___dptdb():
    return unittest.TestLoader().loadTestsFromTestCase(_DPTdumultiDeferBase)


def suite___dptnpn():
    return unittest.TestLoader().loadTestsFromTestCase(_DPTdumultiNoPadNoCRLF)


def suite___dptnpnoc():
    return unittest.TestLoader().loadTestsFromTestCase(
        _DPTdumultiNoPadNoCRLFOrdChar)


def suite___dptnpnon():
    return unittest.TestLoader().loadTestsFromTestCase(
        _DPTdumultiNoPadNoCRLFOrdNum)


def suite___f():
    return unittest.TestLoader().loadTestsFromTestCase(_rmFile)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__dpt())
    unittest.TextTestRunner(verbosity=2).run(suite__dptr())
    unittest.TextTestRunner(verbosity=2).run(suite___dptdb())
    unittest.TextTestRunner(verbosity=2).run(suite___dptnpn())
    unittest.TextTestRunner(verbosity=2).run(suite___dptnpnoc())
    unittest.TextTestRunner(verbosity=2).run(suite___dptnpnon())
    unittest.TextTestRunner(verbosity=2).run(suite___f())
