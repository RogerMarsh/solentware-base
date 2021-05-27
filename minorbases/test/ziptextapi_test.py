# ziptextapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""ziptextapi tests"""

import unittest
from copy import copy, deepcopy

from .. import ziptextapi


class ZipTextapi(unittest.TestCase):

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


class ZipTextapiRoot(unittest.TestCase):

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

    runner().run(loader(ZipTextapi))
    runner().run(loader(ZipTextapiRoot))
