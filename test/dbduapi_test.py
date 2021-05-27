# dbduapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dbduapi tests"""

import unittest
from copy import copy, deepcopy

from .. import dbduapi


class DBduapi(unittest.TestCase):

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


class _DBduapi(unittest.TestCase):

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


class _DBduSecondary(unittest.TestCase):

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


class DBduSecondary(unittest.TestCase):

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


class DBbitduapi(unittest.TestCase):

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


class DBbitduPrimary(unittest.TestCase):

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


class DBbitduSecondary(unittest.TestCase):

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


def suite__db():
    return unittest.TestLoader().loadTestsFromTestCase(DBduapi)


def suite___db():
    return unittest.TestLoader().loadTestsFromTestCase(_DBduapi)


def suite___dbs():
    return unittest.TestLoader().loadTestsFromTestCase(_DBduSecondary)


def suite__dbs():
    return unittest.TestLoader().loadTestsFromTestCase(DBduSecondary)


def suite__dbbit():
    return unittest.TestLoader().loadTestsFromTestCase(DBbitduapi)


def suite__dbbitp():
    return unittest.TestLoader().loadTestsFromTestCase(DBbitduPrimary)


def suite__dbbits():
    return unittest.TestLoader().loadTestsFromTestCase(DBbitduSecondary)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite___db())
    unittest.TextTestRunner(verbosity=2).run(suite__db())
    unittest.TextTestRunner(verbosity=2).run(suite___dbs())
    unittest.TextTestRunner(verbosity=2).run(suite__dbs())
    unittest.TextTestRunner(verbosity=2).run(suite__dbbit())
    unittest.TextTestRunner(verbosity=2).run(suite__dbbitp())
    unittest.TextTestRunner(verbosity=2).run(suite__dbbits())
