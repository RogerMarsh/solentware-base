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
from .core.constants import DPT_SYSDU_FOLDER, BRECPPG


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
        """Override. Note Table B pages used at start of dferred update."""
        for table in self.table.values():
            table.set_defer_update()

    def unset_defer_update(self):
        """Override.  Add Table D pages to fit Table B pages added."""
        for table in self.table.values():
            table.unset_defer_update()

    def do_final_segment_deferred_updates(self):
        """Do nothing.  Provided for compatibility with other engines."""

    def index_instance(self, dbset, instance):
        """Apply instance index values on database dbset.

        Formerly 'dbset' was called 'file' to fit DPT terminology but
        'dbset' is a neutral term used in other database interfaces.

        """
        self.table[dbset].index_instance(instance)

    def sort_and_write(self, *args):
        """Do nothing, provides compatibility with other database engines.

        Sort and write for deferred updates is an internal function in DPT.
        """

    def merge(self, *args):
        """Do nothing, provides compatibility with other database engines.

        Merge for deferred updates is an internal function in DPT.
        """


class DPTFile(_dpt.DPTFile):
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    Some methods in _dpt.DPTFile are overridden to provide single-step
    deferred update mode and ban editing and deleting records on the database.

    """

    def __init__(self, **kwargs):
        """Extend, provide state for Table B pages added."""
        super().__init__(**kwargs)
        self.table_b_pages_used = None
        self.viewer_resetter = None

    def close_file(self, dbenv):
        """Extend, destroy viewer resetter object before delegating."""
        if self.opencontext is None:
            return
        self.viewer_resetter = None
        super().close_file(dbenv)

    def open_file(self, dbenv):
        """Extend, create viewer resetter object after delegating."""
        super().open_file(dbenv)
        self.viewer_resetter = dbenv.Core().GetViewerResetter()

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

    def put_instance(self, instance):
        """Extend, increase Table B pages available after add record.

        Allocate lots more than needed so complementary Table D (index)
        increases, which are typically larger than those for Table B (data),
        can be added in smaller chunks without increasing the number of
        extents.

        It is unwise to make the Table B increase as small as possible
        to avoid problems if the increase is fired by one of the later
        record adds (the final one in extreme case) in a deferred update.

        It is assumed deferred updates are done without recordsets or
        cursors open otherwise an attempted increase would cause an
        exception.

        The other database engines do not have an equivalent extension of
        put_instance because they do not allocate space to a particular
        table (a DPT file) in advance.

        Note the complementry Table D space is not used until the file is
        being closed at the end of the deferred update.  Thus the Table D
        increases can be tuned to the amount of Table B actually used.

        """
        view_as_int = self.viewer_resetter.ViewAsInt
        opencontext = self.opencontext
        bhighpg = view_as_int("BHIGHPG", opencontext)
        bsize = view_as_int("BSIZE", opencontext)
        super().put_instance(instance)
        # The constant 32 assumes all primary field occurrences are 255 bytes
        # (maximum size).  This is not true because dpt_primary_field_length
        # will be a lot less than 255 to allow for conversion of Python str
        # instances to bytes with utf-8 encoding.
        # The effect is to allocate lots more space to a data extent than
        # seems necessary, which is what is wanted here.
        b_pages_needed = (
            len(instance.srvalue) // self.dpt_primary_field_length
        ) // 32 + 1
        if b_pages_needed >= bsize - bhighpg:
            opencontext.Increase(
                (self.default_records * 100) // self.filedesc[BRECPPG], False
            )

    def set_defer_update(self):
        """Override. Note Table B pages used at start of deferred update.

        BHIGHPG is proxy for pages used, initialized to -1.

        """
        self.table_b_pages_used = max(
            self.viewer_resetter.ViewAsInt("BHIGHPG", self.opencontext),
            0,
        )

    def unset_defer_update(self):
        """Override.  Add Table D pages to fit Table B pages added.

        BHIGHPG is proxy for pages used, initialized to -1.

        """
        opencontext = self.opencontext
        view_as_int = self.viewer_resetter.ViewAsInt
        table_b_pages_used = max(view_as_int("BHIGHPG", opencontext), 0)
        extra_table_b_pages = table_b_pages_used - self.table_b_pages_used
        d_pages_needed = extra_table_b_pages * self.btod_factor
        dpgsused = view_as_int("DPGSUSED", opencontext)
        dsize = view_as_int("DSIZE", opencontext)
        if d_pages_needed > (dsize - dpgsused) * 10:
            opencontext.Increase(d_pages_needed, True)
        elif d_pages_needed > dsize - dpgsused:
            opencontext.Increase(d_pages_needed - dsize + dpgsused, True)
