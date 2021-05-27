# dbaseapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dbaseapi tests"""

import unittest
from copy import copy, deepcopy

from .. import dbaseapi


class dBaseapi(unittest.TestCase):

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


class dBaseIII(unittest.TestCase):

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


class CursordBaseIII(unittest.TestCase):

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


class _dBaseapiRoot(unittest.TestCase):

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


class dBaseapiRoot(unittest.TestCase):

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


class Cursor(unittest.TestCase):

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


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(dBaseapi))
    runner().run(loader(dBaseIII))
    runner().run(loader(CursordBaseIII))
    runner().run(loader(_dBaseapiRoot))
    runner().run(loader(dBaseapiRoot))
    runner().run(loader(Cursor))
