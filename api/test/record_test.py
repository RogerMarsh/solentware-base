# record_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""record tests"""

import unittest
from copy import copy, deepcopy

from .. import record


class Key(unittest.TestCase):

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


class KeyData(unittest.TestCase):

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


class KeydBaseIII(unittest.TestCase):

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


class KeyDict(unittest.TestCase):

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


class KeyList(unittest.TestCase):

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


class KeyText(unittest.TestCase):

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


class Value(unittest.TestCase):

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


class ValueData(unittest.TestCase):

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


class ValueDict(unittest.TestCase):

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


class ValueList(unittest.TestCase):

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


class ValueText(unittest.TestCase):

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


class Record(unittest.TestCase):

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


class RecorddBaseIII(unittest.TestCase):

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


class RecordText(unittest.TestCase):

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


def suite__k():
    return unittest.TestLoader().loadTestsFromTestCase(Key)


def suite__kd():
    return unittest.TestLoader().loadTestsFromTestCase(KeyData)


def suite__kb():
    return unittest.TestLoader().loadTestsFromTestCase(KeydBaseIII)


def suite__kdi():
    return unittest.TestLoader().loadTestsFromTestCase(KeyDict)


def suite__kl():
    return unittest.TestLoader().loadTestsFromTestCase(KeyList)


def suite__kt():
    return unittest.TestLoader().loadTestsFromTestCase(KeyText)


def suite__v():
    return unittest.TestLoader().loadTestsFromTestCase(Value)


def suite__vd():
    return unittest.TestLoader().loadTestsFromTestCase(ValueData)


def suite__vdi():
    return unittest.TestLoader().loadTestsFromTestCase(ValueDict)


def suite__vl():
    return unittest.TestLoader().loadTestsFromTestCase(ValueList)


def suite__vt():
    return unittest.TestLoader().loadTestsFromTestCase(ValueText)


def suite__r():
    return unittest.TestLoader().loadTestsFromTestCase(Record)


def suite__rb():
    return unittest.TestLoader().loadTestsFromTestCase(RecorddBaseIII)


def suite__rt():
    return unittest.TestLoader().loadTestsFromTestCase(RecordText)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__k())
    unittest.TextTestRunner(verbosity=2).run(suite__kd())
    unittest.TextTestRunner(verbosity=2).run(suite__kb())
    unittest.TextTestRunner(verbosity=2).run(suite__kdi())
    unittest.TextTestRunner(verbosity=2).run(suite__kl())
    unittest.TextTestRunner(verbosity=2).run(suite__kt())
    unittest.TextTestRunner(verbosity=2).run(suite__v())
    unittest.TextTestRunner(verbosity=2).run(suite__vd())
    unittest.TextTestRunner(verbosity=2).run(suite__vdi())
    unittest.TextTestRunner(verbosity=2).run(suite__vl())
    unittest.TextTestRunner(verbosity=2).run(suite__vt())
    unittest.TextTestRunner(verbosity=2).run(suite__r())
    unittest.TextTestRunner(verbosity=2).run(suite__rb())
    unittest.TextTestRunner(verbosity=2).run(suite__rt())
