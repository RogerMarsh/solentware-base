# dptdu_database.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database deferring index updates.

Index updates are deferred.  Transactions are disabled so explicit
external backups should be used.  Use dpt_database for transactions,
but adding lots of new records will be a lot slower.

"""
import os

from .core import _dpt
from .core.constants import DPT_SYSDU_FOLDER


class DptduDatabaseError(Exception):
    """Exception for Database class."""


class Database(_dpt.Database):
    """Bulk insert to DPT database in folder using specification.

    Support DPT single-step deferred updates.

    DPT non-deferred (normal) update methods provided by the _dpt.Database
    superclass are overridden here to implement deferred update and prevent
    delete and edit of existing records.

    """

    def __init__(self, specification, folder=None, sysfolder=None, **kargs):
        """Create DPT single-step deferred update environment."""
        if folder:
            folder = os.path.abspath(folder)
            if sysfolder is None:
                sysfolder = os.path.join(folder, DPT_SYSDU_FOLDER)
        super().__init__(
            specification, folder=folder, sysfolder=sysfolder, **kargs
        )

    # Set default parameters for single-step deferred update use.
    def create_default_parms(self):
        """Create default parms.ini file for deferred update mode.

        This means transactions are disabled and a small number of DPT buffers.

        """
        if not os.path.exists(self.parms):
            with open(self.parms, "w", encoding="iso-8859-1") as parms:
                parms.write("RCVOPT=X'00' " + os.linesep)
                parms.write("MAXBUF=100 " + os.linesep)

    def deferred_update_housekeeping(self):
        """Call Commit() if a non-TBO update is in progress.

        In non-TBO mode Commit() does not commit the tranasction, but it does
        release redundant resources which would not otherwise be released and
        may lead to an insuffient memory exception.

        """
        if self.dbenv:
            if self.dbenv.UpdateIsInProgress():
                self.dbenv.Commit()

    def delete_instance(self, dbset, instance):
        """Delete an instance is not available in deferred update mode."""
        raise DptduDatabaseError(
            "delete_instance not available in deferred update mode"
        )

    def edit_instance(self, dbset, instance):
        """Edit an instance is not available in deferred update mode."""
        raise DptduDatabaseError(
            "edit_instance not available in deferred update mode"
        )

    def _dptfileclass(self):
        return DPTFile

    def set_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""

    def unset_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""

    def do_final_segment_deferred_updates(self):
        """Do nothing.  Provided for compatibility with other engines."""


class DPTFile(_dpt.DPTFile):
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    Some methods in _dpt.DPTFile are overridden to provide single-step
    deferred update mode and ban editing and deleting records on the database.

    """

    # Call dbenv.OpenContext_DUSingle by default.
    # Python is crashed if more than one 'OpenContext'-style calls are made per
    # file in a process when any of them is OpenContext_DUSingle.
    def _open_context(self, dbenv, context_specification):
        return dbenv.OpenContext_DUSingle(context_specification)

    def delete_instance(self, instance):
        """Raise DptduDatabaseError on attempt to delete instance."""
        raise DptduDatabaseError(
            "delete_instance not available in deferred update mode"
        )

    def edit_instance(self, instance):
        """Raise DptduDatabaseError on attempt to edit instance."""
        raise DptduDatabaseError(
            "edit_instance not available in deferred update mode"
        )
