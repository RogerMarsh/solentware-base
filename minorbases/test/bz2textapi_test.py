# bz2textapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""bz2textapi tests"""

import unittest
from copy import copy, deepcopy

from .. import bz2textapi


class BZ2Textapi(unittest.TestCase):

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


class BZ2TextapiRoot(unittest.TestCase):

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

    runner().run(loader(BZ2Textapi))
    runner().run(loader(BZ2TextapiRoot))
