# database_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""database tests"""

import unittest
from copy import copy, deepcopy

from .. import database


class Database(unittest.TestCase):

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


class DatabaseEncoders(unittest.TestCase):

    def setUp(self):
        self.de = database.DatabaseEncoders()

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

    def test_encode_record_number(self):
        """"""
        ern = self.de.encode_record_number
        for i in range(0, 2**32, 50001):
            self.assertEqual(ern(i), i.to_bytes(4, byteorder='big'))


def suite__d():
    return unittest.TestLoader().loadTestsFromTestCase(Database)


def suite__c():
    return unittest.TestLoader().loadTestsFromTestCase(Cursor)


def suite__de():
    return unittest.TestLoader().loadTestsFromTestCase(DatabaseEncoders)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite__d())
    unittest.TextTestRunner(verbosity=2).run(suite__c())
    unittest.TextTestRunner(verbosity=2).run(suite__de())
