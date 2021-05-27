# controlfile_test.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""controlfile tests"""

import unittest

from .. import controlfile


class ControlFile(unittest.TestCase):

    def setUp(self):
        class CF(controlfile.ControlFile):
            def close(self):
                pass
        self.cf = CF()

    def tearDown(self):
        pass

    def test_close_01(self):
        cf = controlfile.ControlFile()
        self.assertRaisesRegex(
            controlfile.ControlFileError,
            'Method close not implemented in subclass',
            cf.close)
        print(''.join(("__del__ will call close() producing 'Exception ",
                       "ignored' in test_close_01")))

    def test_close_02(self):
        self.assertEqual(self.cf.close(), None)

    def test_open_root_01(self):
        self.assertRaisesRegex(
            controlfile.ControlFileError,
            'Method open_root not implemented in subclass',
            self.cf.open_root)

    def test_get_control_database_01(self):
        self.assertRaisesRegex(
            controlfile.ControlFileError,
            'Method get_control_database not implemented in subclass',
            self.cf.get_control_database)

    def test_control_file_01(self):
        def f():
            return self.cf.control_file
        self.assertRaisesRegex(
            controlfile.ControlFileError,
            'Read-only property control_file not implemented in subclass',
            f)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(ControlFile))
