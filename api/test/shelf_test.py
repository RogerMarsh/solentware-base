# shelf_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""shelf tests"""

import unittest
from copy import copy, deepcopy

from .. import shelf


class _Segment(unittest.TestCase):

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


class Shelf(unittest.TestCase):

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


class _SegmentStringKeys(unittest.TestCase):

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


class _ShelfNoCompress(unittest.TestCase):

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


class ShelfString(unittest.TestCase):

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


def suite__s():
    return unittest.TestLoader().loadTestsFromTestCase(_Segment)


def suite__sh():
    return unittest.TestLoader().loadTestsFromTestCase(Shelf)


def suite__ssk():
    return unittest.TestLoader().loadTestsFromTestCase(_SegmentStringKeys)


def suite__snc():
    return unittest.TestLoader().loadTestsFromTestCase(_ShelfNoCompress)


def suite__ss():
    return unittest.TestLoader().loadTestsFromTestCase(ShelfString)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__s())
    unittest.TextTestRunner(verbosity=2).run(suite__sh())
    unittest.TextTestRunner(verbosity=2).run(suite__ssk())
    unittest.TextTestRunner(verbosity=2).run(suite__snc())
    unittest.TextTestRunner(verbosity=2).run(suite__ss())
