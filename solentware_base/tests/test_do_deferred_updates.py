# test_do_deferred_updates.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""do_deferred_updates tests"""

import unittest
import os
import subprocess

from .. import do_deferred_updates


class DoDeferredUpdates(unittest.TestCase):
    def test__assumptions(self):
        msg = "Failure of this test invalidates all other tests"
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "do_deferred_updates\(\) missing 3 required positional ",
                    "arguments: 'pyscript', 'databasepath', and 'filepath'",
                )
            ),
            do_deferred_updates.do_deferred_updates,
        )

    def test_do_deferred_updates_no_pyscript(self):
        self.assertRaisesRegex(
            do_deferred_updates.DoDeferredUpdatesError,
            "".join(("'' is not an existing file",)),
            do_deferred_updates.do_deferred_updates,
            *("", "", ""),
        )

    def test_do_deferred_updates_no_databasepath(self):
        self.assertRaisesRegex(
            do_deferred_updates.DoDeferredUpdatesError,
            "".join(("'' is not an existing directory",)),
            do_deferred_updates.do_deferred_updates,
            *(__file__, "", ""),
        )
        self.assertRaisesRegex(
            do_deferred_updates.DoDeferredUpdatesError,
            "".join(
                (
                    "'",
                    __file__,
                    "' is not an existing directory",
                )
            ),
            do_deferred_updates.do_deferred_updates,
            *(__file__, __file__, ""),
        )

    def test_do_deferred_updates_no_filepath(self):
        self.assertRaisesRegex(
            do_deferred_updates.DoDeferredUpdatesError,
            "".join(("'' is not an existing file",)),
            do_deferred_updates.do_deferred_updates,
            *(__file__, os.path.dirname(__file__), ""),
        )

    def test_do_deferred_updates_str_filepath(self):
        pid = do_deferred_updates.do_deferred_updates(
            os.path.join(
                os.path.dirname(__file__), "_script_do_deferred_updates.py"
            ),
            os.path.dirname(__file__),
            __file__,
        )
        self.assertIsInstance(pid, subprocess.Popen)

    def test_do_deferred_updates_list_no_filepath(self):
        self.assertRaisesRegex(
            do_deferred_updates.DoDeferredUpdatesError,
            "".join(("'' is not an existing file",)),
            do_deferred_updates.do_deferred_updates,
            *(__file__, os.path.dirname(__file__), [""]),
        )

    def test_do_deferred_updates_list_filepath_empty(self):
        pid = do_deferred_updates.do_deferred_updates(
            os.path.join(
                os.path.dirname(__file__), "_script_do_deferred_updates.py"
            ),
            os.path.dirname(__file__),
            [],
        )
        self.assertIsInstance(pid, subprocess.Popen)

    def test_do_deferred_updates_list_filepath(self):
        pid = do_deferred_updates.do_deferred_updates(
            os.path.join(
                os.path.dirname(__file__), "_script_do_deferred_updates.py"
            ),
            os.path.dirname(__file__),
            [__file__],
        )
        self.assertIsInstance(pid, subprocess.Popen)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(DoDeferredUpdates))
