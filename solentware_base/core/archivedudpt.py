# archivedudpt.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""The ArchiveduDPT class for interface to DPT.

Customise the Archivedu class for DPT
"""

import os

from dptdb.dptapi import FUNLOAD_DEFAULT

from .archivedu import Archivedu


class ArchiveduDPTError(Exception):
    """Raise for calls to _archive_zip or _delete_archive_zip."""


class ArchiveduDPT(Archivedu):
    """Provide deferred update archive methods for DPT interfaces.

    The *_zip() methods are customised to handle the files used to
    support DPT fastunload and fastload.

    The *_bz2() methods raise an exception if called.
    """

    def archive(self, name=None):
        """Write a backup of database file called name."""
        if name not in self.table:
            raise ArchiveduDPTError(
                str(name).join(
                    ("Import backups for file '", "' cannot be taken")
                )
            )
        for file, table in self.table.items():
            if name != file:
                continue
            # Unload accepts positional arguments only.
            # Want 'dir' argument as '__import_backup' in self.home_directory,
            # not the default '#FASTIO' via definition of FUNLOAD_DIR.
            # So have to specify options where FUNLOAD_DEFAULT, itself defined
            # via FUNLOAD_ALLINFO (at time of writing) which is required
            # option, is the default option.
            outputdir = os.path.join(
                self.home_directory, self.import_backup_directory
            )
            table.opencontext.Unload(FUNLOAD_DEFAULT, None, None, outputdir)
            with open(".".join((outputdir, "grd")), "wb"):
                pass
            break

    def delete_archive(self, name=None):
        """Delete a backup of database file called name."""
        if name not in self.table:
            raise ArchiveduDPTError(
                str(name).join(
                    ("Import backups for file '", "' cannot be deleted")
                )
            )
        outputdir = os.path.join(
            self.home_directory, self.import_backup_directory
        )
        expected_files = set(self._get_zip_archive_names_for_name(name))
        if set(os.listdir(outputdir)) != expected_files:
            raise ArchiveduDPTError(
                str(name).join(
                    ("Import backups for file '", "' are not those expected")
                )
            )
        try:
            os.remove(".".join((outputdir, "grd")))
        except FileNotFoundError:
            pass
        for file in expected_files:
            os.remove(os.path.join(outputdir, file))
        os.rmdir(outputdir)

    def _archive_bz2(self, name):
        """Raise an ArchiveduDPTbz2 exception."""
        raise ArchiveduDPTError("bz2 compression format not supported")

    def _archive_zip(self, name):
        """Raise an ArchiveduDPTzip exception."""
        raise ArchiveduDPTError("zip compression format not supported")

    def _delete_archive_bz2(self, name):
        """Raise an ArchiveduDPTbz2 exception."""
        raise ArchiveduDPTError("bz2 compression format not supported")

    def _delete_archive_zip(self, name):
        """Raise an ArchiveduDPTzip exception."""
        raise ArchiveduDPTError("zip compression format not supported")

    # Inverted lists and index trees are all in one file for DPT.
    # Thus a bz2 backup would be expected but attempts to read the file via
    # the open(...) built-in fail with a PermissionError.
    # Try doing DPT fast dump to create the backup: a zip file is needed for
    # the multiple files created, or perhaps a directory is best.  Confirm
    # the technique will work first.
    def _get_zip_archive_names_for_name(self, name):
        """Return specified files and existing operating system files."""
        name_list = []
        for file, table in self.table.items():
            if name != file:
                continue
            filename = table.ddname
            name_list.append("".join((filename, "_TAPED", ".DAT")))
            name_list.append("".join((filename, "_TAPEF", ".DAT")))
            for field in table.fields:
                if field == table.primary:
                    continue
                name_list.append("".join((filename, "_TAPEI_", field, ".DAT")))
            break
        return name_list
