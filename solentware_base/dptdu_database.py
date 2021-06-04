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
from .core.constants import DPT_SYS_FOLDER


class DptduDatabaseError(Exception):
    """Exception for Database class."""


class Database(_dpt.Database):
    """Bulk insert to DPT database in folder using specification.

    Support DPT single-step deferred updates.

    DPT non-deferred (normal) update methods provided by the dptbase.Database
    superclass are overridden here to prevent delete and edit of existing
    records.

    """

    def __init__(self, specification, folder=None, sysfolder=None, **kargs):
        """Create DPT single-step deferred update environment."""
        if folder:
            folder = os.path.abspath(folder)
            if sysfolder is None:
                sysfolder = os.path.join(
                    folder, DPT_SYS_FOLDER, DPT_SYS_FOLDER
                )
        super().__init__(
            specification, folder=folder, sysfolder=sysfolder, **kargs
        )

    # Set default parameters for single-step deferred update use.
    def create_default_parms(self):
        """Create default parms.ini file for deferred update mode.

        This means transactions are disabled and a small number of DPT buffers.

        """
        if not os.path.exists(self.parms):
            parms = open(self.parms, "w")
            try:
                parms.write("RCVOPT=X'00' " + os.linesep)
                parms.write("MAXBUF=100 " + os.linesep)
            finally:
                parms.close()

    def deferred_update_housekeeping(self):
        """Call Commit() if a non-TBO update is in progress.

        In non-TBO mode Commit() does not commit the tranasction, but it does
        release redundant resources which would not otherwise be released and
        may lead to an insuffient memory exception.

        """
        if self.dbenv:
            if self.dbenv.UpdateIsInProgress():
                self.dbenv.Commit()

    def delete_instance(self, file, instance):
        """Delete an instance is not available in deferred update mode."""
        raise DptduDatabaseError(
            "delete_instance not available in deferred update mode"
        )

    def edit_instance(self, file, instance):
        """Edit an instance is not available in deferred update mode."""
        raise DptduDatabaseError(
            "edit_instance not available in deferred update mode"
        )

    def _dptfileclass(self):
        return _DPTFile

    def set_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""

    def unset_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""


class _DPTFile(_dpt._DPTFile):
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    Some methods in _dpt._DPTFile are overridden to provide single-step
    deferred update mode and ban editing and deleting records on the database.

    """

    # Call dbenv.OpenContext_DUSingle by default.
    # Python is crashed if more than one 'OpenContext'-style calls are made per
    # file in a process when any of them is OpenContext_DUSingle.
    def _open_context(self, dbenv, cs):
        return dbenv.OpenContext_DUSingle(cs)

    def delete_instance(self, instance):
        raise DptduDatabaseError(
            "delete_instance not available in deferred update mode"
        )

    def edit_instance(self, instance):
        raise DptduDatabaseError(
            "edit_instance not available in deferred update mode"
        )


if __name__ == "__main__":

    import sys

    from dptdb import dptapi

    from .core import filespec
    from .core import record

    class _V(record.Value):
        def pack(self):
            packed = super().pack()
            index = packed[1]
            index["b"] = self.v.split()
            index["c"] = [str(len(index["b"]))]
            return packed

    class _R(record.Record):
        def __init__(self):
            super().__init__(valueclass=_V)

    class _W(record.Value):
        def pack(self):
            packed = super().pack()
            index = packed[1]
            index["e"] = self.v.split()
            index["f"] = [str(len(index["e"]))]
            return packed

    class _S(record.Record):
        def __init__(self):
            super().__init__(valueclass=_W)

    d = Database(
        filespec.FileSpec(**{"a": "bc", "d": "ef"}),
        folder=os.path.expanduser(
            os.path.join(
                "~",
                "".join(
                    (str(sys.version_info[0]), str(sys.version_info[1]))
                ).join(("cccc_", "_dptdu_single")),
            )
        ),
    )
    print(d.home_directory)
    d.open_database()

    # Cannot do this and later open_database() in same process.
    # Only one open context call per file is allowed in a process if one of
    # these calls is to OpenContext_DUSingle().
    # See _open_database() definition above.
    # super(Database, d).open_database()
    # d.close_database()

    # Repeat .core._dpt 'insert' sequence to verify the different 'opencontext'.
    a = record.Record()
    r = _R()
    rd = _R()
    print(d.home_directory)
    # d.open_database()
    a.load_record((repr(dict(k=None)), repr(dict(v="A record value"))))
    d.put_instance("a", a)
    r.key.recno = None
    r.value.__dict__["v"] = "Another record value"
    r.put_record(d, "a")
    d.commit()
    s = _S()
    s.key.recno = None
    s.value.__dict__["v"] = "Another record value for another file"
    s.put_record(d, "d")
    d.commit()
    print(d.get_primary_record("d", 0))
    print(d.table["d"].foundset_all_records("d").Count())
    print(d.table["d"].foundset_all_records("e").Count())
    print(d.table["d"].foundset_all_records("f").Count())
    print(d.table["d"].foundset_field_equals_value("d", "").Count())
    print(d.table["d"].foundset_field_equals_value("e", "An").Count())
    print(d.table["d"].foundset_field_equals_value("f", str(3)).Count())
    print(
        d.table["d"]
        .foundset_field_equals_value("d", dptapi.APIFieldValue(""))
        .Count()
    )
    print(
        d.table["d"]
        .foundset_field_equals_value("e", dptapi.APIFieldValue("An"))
        .Count()
    )
    print(
        d.table["d"]
        .foundset_field_equals_value("f", dptapi.APIFieldValue(str(3)))
        .Count()
    )
    print(d.table["d"].foundset_record_number(1).Count())
    print(d.table["d"].foundset_records_before_record_number(4).Count())
    print(d.table["d"].foundset_records_not_before_record_number(5).Count())
    c = d.database_cursor("d", "e")
    try:
        print(c.count_records())
    finally:
        c.close()
    d.close_database()
    # d.open_database()
    # d.close_database()
