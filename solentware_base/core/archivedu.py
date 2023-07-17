# archivedu.py
# Copyright 2022 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""The Archivedu class for interfaces to single file databases.

The file will contain all the 'key:value' maps in the database.

Subclasses of Archivedu will handle cases where each 'key:value' map
is in a separate file.

This module is relevant to the apsw and sqlite3 interfaces to Sqlite3,
the bsddb3 (before Python 3.10) and berkeleydb (Python 3.6 and after)
interfaces to Berkeley DB, and to the gnu, ndbm, unqlite and vedis
interfaces to their respective 'key:value' databases.

"""

import os
import bz2
import zipfile

from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_SUFFIX,
)


class Archivedu:
    """Provide deferred update archive methods shared by various interfaces.

    All the supported engines put the whole database in a single file so
    can use the same methods to manage temporary backups which may exist
    while opening and checking the database.

    Supported engines which allow one file per database should extend the
    archive and delete_archive methods to allow this choice: Berkeley DB
    for example.

    The take_backup_before_deferred_update property, defined in deferred
    update Database classes, can be used to decide whether to call these
    methods.

    The file_per_database property is used to decide if these methods are
    appropriate or if a subclass must provide the implementation.
    """

    def archive(self, name=None):
        """Write a backup of database called name.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.

        Intended to be a backup in case import fails.

        """
        # home_directory will be None for a memory-only database.
        if self.home_directory is None:
            return

        self.delete_archive(name=name)
        if self.file_per_database:
            self._archive_zip(name)
            return
        self._archive_bz2(name)

    def _archive_bz2(self, name):
        """Write a bz2 backup of file containing a database.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.  The _generate_database_file_name method
        decides whether to use 'name' or self.databasefile or some
        other appropriate name.

        Intended to be a backup in case import fails.

        """
        file = self._generate_database_file_name(name)
        compressor = bz2.BZ2Compressor()
        archiveguard = ".".join((file, "grd"))
        archivename = ".".join((file, "bz2"))
        with open(file, "rb") as file_in, open(archivename, "wb") as file_out:
            inp = file_in.read(10000000)
            while inp:
                compressed = compressor.compress(inp)
                if compressed:
                    file_out.write(compressed)
                inp = file_in.read(10000000)
            compressed = compressor.flush()
            if compressed:
                file_out.write(compressed)
        with open(archiveguard, "wb"):
            pass

    def _archive_zip(self, name):
        """Write a zip backup of files containing a database.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.  The _generate_database_file_name method
        decides whether to use 'name' or self.databasefile or some
        other appropriate name.

        Intended to be a backup in case import fails.

        """
        if name not in self.specification:
            return
        file = self._generate_database_file_name(name)
        archiveguard = ".".join((file, "grd"))
        archivename = ".".join((file, "zip"))
        name_list = self._get_zip_archive_names_for_name(name)
        with zipfile.ZipFile(
            archivename,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True,
        ) as zip_archive:
            for source in name_list:
                zip_archive.write(source, arcname=os.path.basename(source))
        with open(archiveguard, "wb"):
            pass

    def delete_archive(self, name=None):
        """Delete a backup of database called name.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.

        """
        # home_directory will be None for a memory-only database.
        if self.home_directory is None:
            return

        if self.file_per_database:
            self._delete_archive_zip(name)
            return
        self._delete_archive_bz2(name)

    def _delete_archive_bz2(self, name):
        """Delete a bz2 backup of file containing a database.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.  The _generate_database_file_name method
        decides whether to use 'name' or self.databasefile or some
        other appropriate name.

        """
        file = self._generate_database_file_name(name)
        archiveguard = ".".join((file, "grd"))
        archivename = ".".join((file, "bz2"))
        try:
            os.remove(archiveguard)
        except FileNotFoundError:
            pass
        try:
            os.remove(archivename)
        except FileNotFoundError:
            pass

    def _delete_archive_zip(self, name):
        """Delete a zip backup of files containing a database.

        Argument 'name' is the key of the FileSpec dict entry which
        defines the file.  The _generate_database_file_name method
        decides whether to use 'name' or self.databasefile or some
        other appropriate name.

        """
        file = self._generate_database_file_name(name)
        archiveguard = ".".join((file, "grd"))
        archivename = ".".join((file, "zip"))
        if not os.path.exists(archivename):
            try:
                os.remove(archiveguard)
            except FileNotFoundError:
                pass
            return
        name_list = self._get_zip_archive_names_for_name(name)
        with zipfile.ZipFile(
            archivename,
            mode="r",
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True,
        ) as zip_archive:
            namelist = zip_archive.namelist()
            extract = [
                e
                for e in namelist
                if os.path.join(self.home_directory, e) in name_list
            ]
            if len(extract) != len(namelist):
                return
        try:
            os.remove(archiveguard)
        except FileNotFoundError:
            pass
        try:
            os.remove(archivename)
        except FileNotFoundError:
            pass

    def _get_zip_archive_names_for_name(self, name):
        """Return specified files and existing operating system files."""
        name_list = []
        for item in self.specification[name][SECONDARY]:
            name_list.append(
                os.path.join(
                    self.home_directory,
                    SUBFILE_DELIMITER.join((name, item)),
                )
            )
        name_list.append(
            os.path.join(
                self.home_directory,
                SUBFILE_DELIMITER.join((name, EXISTENCE_BITMAP_SUFFIX)),
            )
        )
        name_list.append(
            os.path.join(
                self.home_directory,
                SUBFILE_DELIMITER.join((name, SEGMENT_SUFFIX)),
            )
        )
        return name_list
