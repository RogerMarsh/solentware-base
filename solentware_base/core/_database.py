# _database.py
# Copyright 2008, 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the interface shared by python database modules.

The major components are the delete_instance, edit_instance, and put_instance,
methods.  The _dpt module's versions of these methods are too different to
justify making _dpt.Database a subclass of _database.Database.

"""
from .segmentsize import SegmentSize
from .find import Find
from .where import Where
from .findvalues import FindValues
from .wherevalues import WhereValues
from .constants import (
    SECONDARY,
)
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
)


class DatabaseError(Exception):
    """Exception for Database class."""


class DataSourceCursorError(Exception):
    """Exception for database-specific DataSourceCursor setting."""


# The Database class was added to the ignored-classes list of pylint.conf for
# solentware_base at a time when all the no-member errors (E1101) referred
# to it or one of it's subclasses with the same name in another module.
class Database:
    """Define file and record access methods; which subclasses may override.

    The set_segment_size() method is used to set the segment size to fit
    the segment size recorded on a database.  There is no segment_size_bytes
    class attribute.

    Property file_per_database returns False because all 'key:value' sets,
    and the associated inverted list indicies, are held in one file by
    default.  Berkeley DB is the known example where it is reasonable to
    return True: override the class attribute, if this case is wanted.

    """

    _file_per_database = False

    @property
    def file_per_database(self):
        """Return True if each database is in a separate file.

        Berkeley DB is the known cases where True is reasonable (see ._db
        module), and DPT is the known case where True is mandatory (see
        ._dpt module).

        """
        return self._file_per_database

    def _generate_database_file_name(self, name):
        """Return path to database file.

        By default this is a file where os.path.basename(path) is same as
        os.path.dirname(path); and path is assumed to follow this rule.

        However some database engines mangle os.path.basename(path) to
        give the file name.  Override this method if necessary to apply
        the required mangle to os.path.basename(path).

        """
        del name
        return self.database_file

    def delete_instance(self, dbset, instance):
        """Delete an existing instance on databases in dbset.

        Deletes are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.

        """
        deletekey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        high_record = self.get_high_record_number(dbset)
        self.delete(dbset, deletekey, instance.srvalue)
        instance.srkey = self.encode_record_number(deletekey)
        srindex = instance.srindex
        segment, record_number = self.remove_record_from_ebm(dbset, deletekey)
        dcb = instance.deletecallbacks
        for secondary in srindex:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for value in srindex[secondary]:
                self.remove_record_from_field_value(
                    dbset, secondary, value, segment, record_number
                )
        self.note_freed_record_number_segment(
            dbset, segment, record_number, high_record
        )

    def edit_instance(self, dbset, instance):
        """Edit an existing instance on databases in dbset.

        Edits are direct while callbacks handle subsidiary databases
        and non-standard inverted indexes.

        """
        oldkey = instance.key.pack()
        newkey = instance.newrecord.key.pack()
        instance.set_packed_value_and_indexes()
        instance.newrecord.set_packed_value_and_indexes()
        srindex = instance.srindex
        nsrindex = instance.newrecord.srindex
        dcb = instance.deletecallbacks
        npcb = instance.newrecord.putcallbacks

        # Changing oldkey to newkey should not be allowed
        # Not changed by default.  See oldkey != newkey below.

        ionly = []
        nionly = []
        iandni = []
        for field in srindex:
            if field in nsrindex:
                iandni.append(field)
            else:
                ionly.append(field)
        for field in nsrindex:
            if field not in srindex:
                nionly.append(field)

        if oldkey != newkey:
            self.delete(dbset, oldkey, instance.srvalue)
            key = self.put(dbset, newkey, instance.newrecord.srvalue)
            if key is not None:
                # put was append to record number database and
                # returned the new primary key. Adjust record key
                # for secondary updates.
                instance.newrecord.key.load(key)
                newkey = key

            old_segment, old_record_number = self.remove_record_from_ebm(
                dbset, oldkey
            )
            new_segment, new_record_number = self.add_record_to_ebm(
                dbset, newkey
            )
        elif instance.srvalue != instance.newrecord.srvalue:
            self.replace(
                dbset, oldkey, instance.srvalue, instance.newrecord.srvalue
            )
            old_segment, old_record_number = divmod(
                oldkey, SegmentSize.db_segment_size
            )
            new_segment, new_record_number = old_segment, old_record_number
        else:
            old_segment, old_record_number = divmod(
                oldkey, SegmentSize.db_segment_size
            )
            new_segment, new_record_number = old_segment, old_record_number

        instance.srkey = self.encode_record_number(oldkey)
        instance.newrecord.srkey = self.encode_record_number(newkey)

        for secondary in ionly:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                continue
            for value in srindex[secondary]:
                self.remove_record_from_field_value(
                    dbset, secondary, value, old_segment, old_record_number
                )

        for secondary in nionly:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in npcb:
                    npcb[secondary](instance.newrecord, nsrindex[secondary])
                continue
            for value in nsrindex[secondary]:
                self.add_record_to_field_value(
                    dbset, secondary, value, new_segment, new_record_number
                )

        for secondary in iandni:
            if secondary not in self.specification[dbset][SECONDARY]:
                if srindex[secondary] == nsrindex[secondary]:
                    if oldkey == newkey:
                        continue
                if secondary in dcb:
                    dcb[secondary](instance, srindex[secondary])
                if secondary in npcb:
                    npcb[secondary](instance.newrecord, nsrindex[secondary])
                continue
            srset = set(srindex[secondary])
            nsrset = set(nsrindex[secondary])
            if oldkey == newkey:
                for value in sorted(srset - nsrset):
                    self.remove_record_from_field_value(
                        dbset, secondary, value, old_segment, old_record_number
                    )
                for value in sorted(nsrset - srset):
                    self.add_record_to_field_value(
                        dbset, secondary, value, new_segment, new_record_number
                    )
            else:
                for value in srset:
                    self.remove_record_from_field_value(
                        dbset, secondary, value, old_segment, old_record_number
                    )
                for value in nsrset:
                    self.add_record_to_field_value(
                        dbset, secondary, value, new_segment, new_record_number
                    )

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.

        This method assumes all primary databases are integer primary key.

        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        if putkey is None:
            # reuse record number if possible
            putkey = self.get_lowest_freed_record_number(dbset)
            if putkey is not None:
                instance.key.load(putkey)

        key = self.put(dbset, putkey, instance.srvalue)
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            # Perhaps this key should be remembered to avoid the cursor
            # operation to find the high segment in every delete_instance call.
            instance.key.load(key)
            putkey = key

        instance.srkey = self.encode_record_number(putkey)
        srindex = instance.srindex
        segment, record_number = self.add_record_to_ebm(dbset, putkey)
        pcb = instance.putcallbacks
        for secondary in srindex:
            if secondary not in self.specification[dbset][SECONDARY]:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for value in srindex[secondary]:
                self.add_record_to_field_value(
                    dbset, secondary, value, segment, record_number
                )

    def record_finder(self, dbset, recordclass=None):
        """Return a solentware_base.core.find.Find instance."""
        return Find(self, dbset, recordclass=recordclass)

    def record_selector(self, statement):
        """Return a solentware_base.core.where.Where instance."""
        return Where(statement)

    def values_finder(self, dbset):
        """Return a solentware_base.core.findvalues.FindValues instance."""
        return FindValues(self, dbset)

    def values_selector(self, statement):
        """Return a solentware_base.core.wherevalues.WhereValues instance."""
        return WhereValues(statement)

    def make_segment(self, key, segment_number, record_count, records):
        """Return a Segment subclass instance created from arguments."""
        del key  # Looks reasonable to remove key from arguments.
        if record_count == 1:
            return RecordsetSegmentInt(
                segment_number,
                None,
                records=records.to_bytes(2, byteorder="big"),
            )
        if len(records) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                segment_number, None, records=records
            )
        return RecordsetSegmentList(segment_number, None, records=records)

    def set_segment_size(self):
        """Copy the database segment size to the SegmentSize object.

        The database segment size will be in the segment_size_bytes attribute
        of a subclass of _database.Database (this class).

        The SegmentSize object derives various constants from the database
        segment size, initially from a default value.
        """
        SegmentSize.db_segment_size_bytes = self.segment_size_bytes

    def exists(self, file, field):
        """Return True if database specification defines field in file."""
        if field == file:
            return field in self.specification
        if file not in self.specification:
            return False
        return field in self.specification[file][SECONDARY]

    def is_primary(self, file, field):
        """Return True if field in file is specified as primary database.

        The terminology is from the Berkeley DB database engine.

        """
        assert file in self.specification
        if field == file:
            return True
        assert field in self.specification[file][SECONDARY]
        return False

    def is_recno(self, file, field):
        """Return True if field in file is specified as record number.

        The terminology is from the Berkeley DB database engine where
        field would be a DB_RECNO database.

        """
        # Same answer as is_primary() by definition now.
        # Originally Berkeley DB primary databases were potentially not record
        # number, but addition of DPT and SQLite led to primary databases being
        # record number only.
        return self.is_primary(file, field)

    def repair_cursor(self, oldcursor, *a):
        """Return oldcursor for compatibility with DPT database engine.

        When using the DPT database engine an application may need to replace
        cursor with a new cursor attached to a new Recordset.  The existing
        cursor is fine when using the Berkeley DB or SQLite3 database engines.

        For example Recordset may be the records for keys which already exist.
        Adding a new key means a new Recordset is needed, which implies a new
        cursor in DPT.  The existing cursor will notice the new record in the
        Berkeley DB and SQLite3 database engines.

        *a absorbs the arguments needed by the DPT version of this method.

        """
        del a
        return oldcursor

    def allocate_and_open_contexts(self, files=None):
        """Re-open files in the database specification.

        The subset files in the database specification is assumed to have been
        closed and need to be opened again.

        The Berkeley DB DBEnv or SQLite3 Connection object is assumed closed as
        well so open all files in the specification.
        """
        del files
        self.open_database()

    def open_database_contexts(self, files=None):
        """Open files in the database specification.

        This method exists because in DPT the method open_database_contexts
        assumes the Database Services object is available while the method
        open_database creates the Database Services object before opening the
        files.
        """
        self.open_database(files=files)

    def start_read_only_transaction(self):
        """Do nothing, present for compatibility with Symas LMMD."""

    def end_read_only_transaction(self):
        """Do nothing, present for compatibility with Symas LMMD."""

    def increase_database_record_capacity(self, **kwargs):
        """Do nothing, present for compatibility with DPT."""
        del kwargs

    def get_database_table_sizes(self, **kwargs):
        """Return empty dict, present for compatibility with DPT."""
        del kwargs
        return {}

    def close_datasourcecursor_recordset(self, datasourcecursor):
        """Do nothing.  Override if necessary.

        Present for compatibility with DPT database engine.
        """
        del datasourcecursor

    def set_datasourcecursor_recordset(self, datasourcecursor, recordset):
        """Validate and set recordset as datasourcecursor's recordset.

        The recordset and datasourcecursor must be associated with the same
        database identity and be the identity of self.

        This method exists because in DPT these checks are implied when the
        recordset and cursor are created, and both objects are details of
        the implementation.
        """
        if datasourcecursor.dbhome is not self:
            raise DataSourceCursorError("DataSource is not for this database")
        if datasourcecursor.recordset:
            if (
                datasourcecursor.recordset.dbidentity
                == recordset.recordset.dbidentity
            ):
                datasourcecursor.recordset.recordset.close()
                datasourcecursor.recordset = recordset
            else:
                raise DataSourceCursorError(
                    "New and existing Recordsets are for different databases"
                )
        elif datasourcecursor.dbidentity == recordset.recordset.dbidentity:
            datasourcecursor.recordset = recordset
        else:
            raise DataSourceCursorError(
                "New Recordset and DataSource are for different databases"
            )

    def get_datasourcecursor_recordset_cursor(self, dsc):
        """Create and return cursor on dsc's recordset.

        dsc not datasourcecursor to shorten argument name.

        """
        if dsc.dbhome is not self:
            raise DataSourceCursorError("DataSource is not for this database")
        if dsc.recordset:
            if dsc.dbidentity == dsc.recordset.recordset.dbidentity:
                cursor = dsc.recordset.dbhome.create_recordset_cursor(
                    dsc.recordset.recordset
                )
            else:
                raise DataSourceCursorError(
                    "Recordset and DataSource are for different databases"
                )
        else:
            dsc.recordset = dsc.dbhome.recordlist_nil(dsc.dbset)
            cursor = dsc.recordset.dbhome.create_recordset_cursor(
                dsc.recordset.recordset
            )
        return cursor


class ExistenceBitmapControl:
    """Base class for managing existence bitmap of file in database.

    Note the primary or secondary database instance to be managed.

    Subclasses implement the management.
    """

    def __init__(self, file, database):
        """Note file whose existence bitmap record number re-use is managed."""
        super().__init__()
        self.ebm_table = None
        self.freed_record_number_pages = None
        self._segment_count = None
        self._file = file
        self.ebmkey = database.encode_record_selector("E" + file)

    @property
    def segment_count(self):
        """Return number of segments."""
        return self._segment_count

    @segment_count.setter
    def segment_count(self, segment_number):
        """Set segment count from 0-based segment_number if greater."""
        if segment_number > self._segment_count:
            self._segment_count = segment_number + 1
