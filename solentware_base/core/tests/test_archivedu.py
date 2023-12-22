# test_archivedu.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Backup for deferred update tests."""

import unittest
import os

from .. import archivedu
from .. import _databasedu
from .. import filespec

_DBNAME = "___dbf"
_FILE1 = "file1"
_FILE2 = "file2"
_FIELD1 = "field1"
_FIELD2 = "field2"
_EBM = "ebm"
_SEGMENT = "segment"
_ZIP = "zip"
_BZ2 = "bz2"
_GRD = "grd"


class _Archivedu(unittest.TestCase):
    def setUp(self):
        os.mkdir(os.path.join(_DBNAME))

    def tearDown(self):
        for f in os.listdir(_DBNAME):
            if f.startswith(_FILE2) or f.startswith(_DBNAME):
                try:
                    os.remove(os.path.join(_DBNAME, f))
                except FileNotFoundError:
                    pass
        os.rmdir(_DBNAME)

    def test_01__get_zip_archive_names_for_name_01(self):
        self.assertEqual(
            self._D._get_zip_archive_names_for_name(_FILE2),
            [
                os.path.join(_DBNAME, "_".join((_FILE2, _FIELD2))),
                os.path.join(_DBNAME, "__".join((_FILE2, _EBM))),
                os.path.join(_DBNAME, "__".join((_FILE2, _SEGMENT))),
            ],
        )

    def test_05_delete_archive_01(self):
        self._D.delete_archive(_FILE2)
        self.assertEqual(os.listdir(_DBNAME), [])


class _D(archivedu.Archivedu, _databasedu.Database):
    specification = filespec.FileSpec(**{_FILE1: {_FIELD1}, _FILE2: {_FIELD2}})
    home_directory = _DBNAME
    database_file = os.path.join(home_directory, _DBNAME)


class Archivedu(_Archivedu):
    def setUp(self):
        super().setUp()
        self._D = _D()

    def test_02__delete_archive_bz2_01(self):
        self._D._delete_archive_bz2(_FILE2)
        self.assertEqual(os.listdir(_DBNAME), [])


class Archivedu_fpd(_Archivedu):
    def setUp(self):
        class _Df(_D):
            _file_per_database = True

        super().setUp()
        self._D = _Df()

    def test_03__delete_archive_zip_01(self):
        self._D._delete_archive_zip(_FILE2)
        self.assertEqual(os.listdir(_DBNAME), [])


class Archivedu_fpd_backup(_Archivedu):
    def setUp(self):
        class _Dfb(_D):
            _take_backup_before_deferred_update = True
            _file_per_database = True

        super().setUp()
        self._D = _Dfb()

    def test_04__delete_archive_zip_01(self):
        with open(
            os.path.join(_DBNAME, "_".join((_FILE2, _FIELD2))), "w"
        ) as f:
            f.write("a")
        with open(os.path.join(_DBNAME, "__".join((_FILE2, _EBM))), "w") as f:
            f.write("a")
        with open(
            os.path.join(_DBNAME, "__".join((_FILE2, _SEGMENT))), "w"
        ) as f:
            f.write("a")
        orig = tuple(os.listdir(_DBNAME))
        self._D._archive_zip(_FILE2)
        self.assertEqual(len(os.listdir(_DBNAME)), 5)
        self._D._delete_archive_zip(_FILE2)
        self.assertEqual(len(os.listdir(_DBNAME)), 3)
        self.assertEqual(tuple(os.listdir(_DBNAME)), orig)


class Archivedu_backup(_Archivedu):
    def setUp(self):
        class _Db(_D):
            _take_backup_before_deferred_update = True

        super().setUp()
        self._D = _Db()

    def test_02__delete_archive_bz2_02(self):
        with open(os.path.join(_DBNAME, ".".join((_DBNAME, _BZ2))), "w") as f:
            f.write("a")
        with open(os.path.join(_DBNAME, ".".join((_DBNAME, _GRD))), "w") as f:
            f.write("a")
        self.assertEqual(len(os.listdir(_DBNAME)), 2)
        self._D._delete_archive_bz2(_FILE2)
        self.assertEqual(os.listdir(_DBNAME), [])

    def test_03__archive_bz2_01(self):
        with open(os.path.join(_DBNAME, _DBNAME), "w") as f:
            f.write("a")
        self._D._archive_bz2(_FILE2)
        self.assertEqual(len(os.listdir(_DBNAME)), 3)


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(Archivedu))
    runner().run(loader(Archivedu_fpd))
    runner().run(loader(Archivedu_fpd_backup))
    runner().run(loader(Archivedu_backup))
