# secondary_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""secondary tests"""

import unittest

from .. import secondary


class Secondary(unittest.TestCase):

    def setUp(self):
        self.secondary = secondary.Secondary()

    def tearDown(self):
        self.secondary = None

    def test___init__(self):
        self.assertEqual(self.secondary.__dict__,
                         {'_clientcursors': {},
                          })
        self.assertEqual(
            [a for a in dir(self.secondary) if not a.startswith('__')],
            ['_clientcursors',
             'close',
             ])

    def test_close(self):
        class C:
            def close(self):
                pass
        class A(secondary.Secondary, C):
            pass
        a = A()
        a._clientcursors = {C(): None}
        self.assertEqual(a.close(), None)
        self.assertEqual(a._clientcursors, {})


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Secondary))
