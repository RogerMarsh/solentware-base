# dptfastload_database.py
# Copyright (c) 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database with fastload updates.

The DPT fastload interface is used to do updates.  The compressed file
format defined in Appendix 2 of the DBA Guide is used.

"""
import os

from .core import _dpt
from .core.constants import DPT_SYSFL_FOLDER


class DptfastloadDatabaseError(Exception):
    """Exception for Database class."""


# This class may end up similar enough to dptdu_database.dptdu_database to
# become a subclass, or perhaps the other way round.
class Database(_dpt.Database):
    """Bulk insert to DPT database in folder using specification.

    Support DPT fastload updates.

    DPT non-deferred (normal) update methods provided by the _dpt.Database
    superclass are overridden here to implement fastload update and prevent
    delete and edit of existing records.

    """

    # Same as dptdu_database version.
    def __init__(self, specification, folder=None, sysfolder=None, **kargs):
        """Create DPT fastload environment."""
        if folder:
            folder = os.path.abspath(folder)
            if sysfolder is None:
                sysfolder = os.path.join(folder, DPT_SYSFL_FOLDER)
        super().__init__(
            specification, folder=folder, sysfolder=sysfolder, **kargs
        )

    # Not sure about create_default_parms().  It may differ from both _dpt
    # and dptdu_database versions.  Likely the _dpt version will do, but a
    # smaller number for MAXBUF may be acceptable.  _dpt version here now.
    def create_default_parms(self):
        """Create default parms.ini file for normal mode.

        This means transactions are enabled and a large number of DPT buffers.

        """
        if not os.path.exists(self.parms):
            with open(self.parms, "w", encoding="iso-8859-1") as parms:
                parms.write("RCVOPT=X'00' " + os.linesep)
                parms.write("MAXBUF=100 " + os.linesep)

    # Same as dptdu_database and _dpt versions.
    # Probably will need to be like versions for the other database
    # engines to 'checkpoint' update at end of each segment.
    def deferred_update_housekeeping(self):
        """Call Commit() if a non-TBO update is in progress.

        In non-TBO mode Commit() does not commit the tranasction, but it does
        release redundant resources which would not otherwise be released and
        may lead to an insuffient memory exception.

        """
        print("deferred_update_housekeeping")
        self._do_dpt_fastload()
        if self.dbenv:
            if self.dbenv.UpdateIsInProgress():
                self.dbenv.Commit()

    # Same as dptdu_database version except for exception class name.
    def delete_instance(self, dbset, instance):
        """Delete an instance is not available in fastload mode."""
        raise DptfastloadDatabaseError(
            "delete_instance not available in fastload mode"
        )

    # Same as dptdu_database version except for exception class name.
    def edit_instance(self, dbset, instance):
        """Edit an instance is not available in fastload mode."""
        raise DptfastloadDatabaseError(
            "edit_instance not available in fastload mode"
        )

    # Same as dptdu_database version.
    # Must return dptfastload_database.DPTFile class.
    def _dptfileclass(self):
        return DPTFile

    # Same as dptdu_database version.
    def set_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""
        print("set_defer_update")

    # Same as dptdu_database version.
    def unset_defer_update(self):
        """Do nothing.  Provided for compatibility with other engines."""

    # Same as dptdu_database version.
    # Probably will need to be like versions for the other database
    # engines to 'checkpoint' update at end of update run.
    def do_final_segment_deferred_updates(self):
        """Do nothing.  Provided for compatibility with other engines."""
        print("do_final_segment_deferred_updates")
        self._do_dpt_fastload()

    def _do_dpt_fastload(self):
        """Do DPT fastload for each file in database opened for input."""
        print(self.deferred_update_points)
        for file in self.table.values():
            file.do_dpt_fastload()


class DPTFile(_dpt.DPTFile):
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    Some methods in _dpt.DPTFile are overridden to provide fastload
    update mode and ban editing and deleting records on the database.

    """

    def __init__(self, **kwargs):
        """Extend provide dict for field codes and list for field values."""
        super().__init__(**kwargs)
        self._field_codes = {}
        self._field_attributes = {}
        self._field_values = []
        self._record_number = 0

    # Documentation does not say 'normal' mode must not be used so
    # _open_context from dptdu_database module is not copied.

    # Same as dptdu_database version except for exception class name.
    def delete_instance(self, instance):
        """Raise DptfastloadDatabaseError on attempt to delete instance."""
        raise DptfastloadDatabaseError(
            "delete_instance not available in fastload mode"
        )

    # Same as dptdu_database version except for exception class name.
    def edit_instance(self, instance):
        """Raise DptfastloadDatabaseError on attempt to edit instance."""
        raise DptfastloadDatabaseError(
            "edit_instance not available in fastload mode"
        )

    # Definitely different from _dpt.DPTFile and _databasedu.Database
    # versions; but the former's algorithm is needed if not the detail.
    def put_instance(self, instance):
        """Override.  Put instance in buffer for fastload to database."""
        field_values = []
        field_codes = self._field_codes
        instance.set_packed_value_and_indexes()
        srv = instance.srvalue
        fieldcode = field_codes[self.dpt_field_names[self.primary]]
        safe_length = self.dpt_primary_field_length
        recnum = self._record_number.to_bytes(4, byteorder="little")
        field_values.append(recnum)
        for i in range(0, len(srv), safe_length):
            value = srv[i : i + safe_length].encode()
            field_values.extend(
                (
                    fieldcode,
                    len(value).to_bytes(1, byteorder="little"),
                    value,
                )
            )
        sri = instance.srindex
        sec = self.secondary
        pcb = instance.putcallbacks
        for indexname in sri:
            if indexname not in pcb:
                fieldcode = field_codes[self.dpt_field_names[sec[indexname]]]
                for value in sri[indexname]:
                    value = value.encode(encoding="iso-8859-1")
                    field_values.extend(
                        (
                            fieldcode,
                            len(value).to_bytes(1, byteorder="little"),
                            value,
                        )
                    )
        field_values.append(b"\xff\xff")
        instance.key.load(self._record_number)

        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.delete_instance() method.
        instance.srkey = repr(self._record_number)

        if len(pcb):
            for indexname in sri:
                if indexname in pcb:
                    pcb[indexname](instance, sri[indexname])
        self._record_number += 1

    def open_existing_file(self, *args):
        """Extend to note field codes as 2-byte little-endian values."""
        super().open_existing_file(*args)
        cursor = self.opencontext.OpenFieldAttCursor()
        try:
            while cursor.Accessible():
                self._field_codes[cursor.Name()] = cursor.FID().to_bytes(
                    2, byteorder="little"
                )
                self._field_attributes[cursor.Name()] = cursor.Atts()
                cursor.Advance()
        finally:
            self.opencontext.CloseFieldAttCursor(cursor)

    def do_dpt_fastload(self):
        """Prepare DPT fastload input and run DPT fastload.

        Files have a single visible string field which is not ordered, and
        many invisible fields, numeric or character, which are ordered.

        """
        print(len(b"".join(self._field_values)))
        self._field_values = []
