# _nosql.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access databases created with the unqlite, vedis, dbm, or ndbm modules.

These database engines cannot access each others databases, but their
interfaces are sufficiently similar that one module and set of classes
can cope with all of them.

"""
import os
from ast import literal_eval
import re
from bisect import bisect_right, bisect_left

from . import filespec
from .constants import (
    PRIMARY,
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_KEY_SUFFIX,
    CONTROL_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    TABLE_REGISTER_KEY,
    FIELD_REGISTER_KEY,
    FREED_RECORD_NUMBER_SEGMENTS_SUFFIX,
    FIELDS,
    NOSQL_FIELDATTS,
    SECONDARY_FIELDATTS,
    BRANCHING_FACTOR,
    ACCESS_METHOD,
    BTREE,
    SEGMENT_VALUE_SUFFIX,
    LIST_BYTES,
    BITMAP_BYTES,
)
from . import _database
from . import tree
from .bytebit import Bitarray, SINGLEBIT
from .segmentsize import SegmentSize

# Some names are imported '* as _*' to avoid confusion with sensible
# object names within the _sqlite module.
# Did not bother about this until pylint with default settings gave
# warnings.
from . import cursor as _cursor
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordsetCursor as _RecordsetCursor,
    RecordList,
)


class DatabaseError(Exception):
    """Exception for Database class."""


class Database(_database.Database):
    """Define file and record access methods."""

    class SegmentSizeError(Exception):
        """Raise when segment size in database is not in specification."""

    def __init__(
        self,
        specification,
        folder=None,
        segment_size_bytes=DEFAULT_SEGMENT_SIZE_BYTES,
        use_specification_items=None,
        **soak
    ):
        """Initialize database structures."""
        if folder is not None:
            try:
                path = os.path.abspath(folder)
            except:
                msg = " ".join(
                    ["Database folder name", str(folder), "is not valid"]
                )
                raise DatabaseError(msg)
        else:
            path = None
        if not isinstance(specification, filespec.FileSpec):
            specification = filespec.FileSpec(
                use_specification_items=use_specification_items,
                **specification
            )
        self._use_specification_items = use_specification_items
        self._validate_segment_size_bytes(segment_size_bytes)
        if folder is not None:
            self.home_directory = path
            self.database_file = os.path.join(path, os.path.basename(path))
        else:
            self.home_directory = None
            self.database_file = None
        for filedesc in specification.values():
            for fieldname in filedesc[FIELDS]:
                if fieldname == filedesc[PRIMARY]:
                    continue
                for fieldattr in NOSQL_FIELDATTS:
                    if fieldattr not in filedesc[FIELDS][fieldname]:
                        filedesc[FIELDS][fieldname][
                            fieldattr
                        ] = SECONDARY_FIELDATTS[fieldattr]
        self.specification = specification
        self.segment_size_bytes = segment_size_bytes
        self.dbenv = None
        self.table = {}

        # self,index should not be necessary in _nosql.

        self.table_data = {}
        self.segment_table = {}
        self.segment_records = {}
        self.ebm_control = {}
        self.ebm_segment_count = {}
        self.trees = {}

        # Set to value read from database on attempting to open database if
        # different from segment_size_bytes.
        self._real_segment_size_bytes = False

        # Used to reset segment_size_bytes to initialization value after close
        # database.
        self._initial_segment_size_bytes = segment_size_bytes

    def _validate_segment_size_bytes(self, segment_size_bytes):
        if segment_size_bytes is None:
            return
        if not isinstance(segment_size_bytes, int):
            raise DatabaseError("Database segment size must be an int")
        if not segment_size_bytes > 0:
            raise DatabaseError("Database segment size must be more than 0")

    def start_transaction(self):
        """Start a transaction."""
        if self.dbenv:
            self.dbenv.begin()

    def backout(self):
        """Backout tranaction."""
        if self.dbenv:
            self.dbenv.rollback()

    def commit(self):
        """Commit tranaction."""
        if self.dbenv:
            self.dbenv.commit()

    def open_database(self, dbe, dbclass, dberror, files=None):
        """Open NoSQL connection and specified tables and indicies.

        By default all tables are opened, but just those named in files
        otherwise, along with their indicies.

        dbe must be a Python module implementing the NoSQL API of UnQLite.
        dbclass and dberror are needed because the database creation commands
        are:
        unqlite.UnQLite(..)
        vedis.Vedis(..)
        and only unqlite provides an exception class, UnQLiteError.

        A connection object is created.

        """
        self.dberror = dberror
        if self.home_directory is not None:
            try:
                os.mkdir(self.home_directory)
            except FileExistsError:
                if not os.path.isdir(self.home_directory):
                    raise

        # Need to look for control file if database already exists.
        table_register = dict()
        field_register = dict()
        high_table_number = len(table_register)
        high_field_number = dict()
        self.table[CONTROL_FILE] = str(high_table_number)
        table_register[CONTROL_FILE] = high_table_number
        specification_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], SPECIFICATION_KEY.decode())
        )
        segment_size_bytes_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], SEGMENT_SIZE_BYTES_KEY.decode())
        )
        table_register_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], TABLE_REGISTER_KEY.decode())
        )
        field_register_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], FIELD_REGISTER_KEY.decode())
        )

        # The ___control table should be present already if the file exists.
        if self.database_file is not None:
            dbenv = dbclass(self.database_file)
            if specification_key in dbenv:
                rsk = dbenv[specification_key]
            else:
                rsk = None
            if segment_size_bytes_key in dbenv:
                rssbk = dbenv[segment_size_bytes_key]
            else:
                rssbk = None
            if rsk is not None and rssbk is not None:
                spec_from_db = literal_eval(rsk.decode())
                if self._use_specification_items is not None:
                    self.specification.is_consistent_with(
                        {
                            k: v
                            for k, v in spec_from_db.items()
                            if k in self._use_specification_items
                        }
                    )
                else:
                    self.specification.is_consistent_with(spec_from_db)
                segment_size = literal_eval(rssbk.decode())
                if self._real_segment_size_bytes is not False:
                    self.segment_size_bytes = self._real_segment_size_bytes
                    self._real_segment_size_bytes = False
                if segment_size != self.segment_size_bytes:
                    self._real_segment_size_bytes = segment_size
                    raise self.SegmentSizeError(
                        "".join(
                            (
                                "Segment size recorded in database is not ",
                                "the one used attemping to open database",
                            )
                        )
                    )
                if table_register_key in dbenv:
                    table_register = literal_eval(
                        dbenv[table_register_key].decode()
                    )
                    high_table_number = max(table_register.values())
                    if high_table_number < len(spec_from_db):
                        raise DatabaseError(
                            "High table number less than specification items"
                        )
                if field_register_key in dbenv:
                    field_register = literal_eval(
                        dbenv[field_register_key].decode()
                    )
                    for k, value in field_register.items():
                        hfn = list(value.values())
                        if len(hfn):
                            high_field_number[k] = max(hfn)
                        else:
                            high_field_number[k] = 0
                        if high_field_number[k] < len(
                            spec_from_db[k][SECONDARY]
                        ):
                            raise DatabaseError(
                                "".join(
                                    (
                                        "High field number less than number ",
                                        "of specification items",
                                    )
                                )
                            )
            elif rsk is None and rssbk is not None:
                raise DatabaseError("No specification recorded in database")
            elif rsk is not None and rssbk is None:
                raise DatabaseError("No segment size recorded in database")
        else:

            # A memory database
            # Set branching factor close to minimum value, 4, assuming a value
            # less than 100 is the default, if segment_size_bytes is None.
            # Assumption is a test environment: small segment, memory database.
            if self.segment_size_bytes is None:
                for filedesc in self.specification.values():
                    for fieldname in filedesc[FIELDS]:
                        if fieldname == filedesc[PRIMARY]:
                            continue
                        for fieldattr in NOSQL_FIELDATTS:
                            if fieldattr == BRANCHING_FACTOR:
                                filedesc[FIELDS][fieldname][fieldattr] = max(
                                    SECONDARY_FIELDATTS[fieldattr] // 10, 4
                                )
            dbenv = dbclass()

        self.set_segment_size()
        self.dbenv = dbenv
        if files is None:
            files = self.specification.keys()
        if self.database_file is None:
            fspec = self.specification.keys()
        elif rsk is None:
            fspec = self.specification.keys()
        else:
            fspec = literal_eval(rsk.decode()).keys()
        self.start_transaction()

        # Sorted so each file gets the same prefix each time in a new database.
        for file in sorted(fspec):
            if file not in files:
                continue
            specification = self.specification[file]

            # Sorted so each field gets same prefix each time.
            # Use self.table values stored in a 'control file' record when an
            # existing file is opened.
            fields = sorted(specification[SECONDARY])

            if file in table_register:
                self.table[file] = [str(table_register[file])]
            else:
                high_table_number += 1
                self.table[file] = [str(high_table_number)]
                table_register[file] = high_table_number

            # Not sure what to store, if anything.  But the key should exist.
            # Maybe name and key which must agree with control file data?
            if self.table[file][0] not in dbenv:
                dbenv[self.table[file][0]] = repr({})

            # The primary field is always field number 0.
            self.ebm_control[file] = ExistenceBitmapControl(
                self.table[file][0], str(0), self
            )
            self.table_data[file] = SUBFILE_DELIMITER.join(
                (self.table[file][0], str(0))
            )
            fieldprops = specification[FIELDS]
            if file not in field_register:
                field_register[file] = dict()
            frf = field_register[file]
            for field in fields:
                if field not in frf:
                    if len(frf):
                        frf[field] = max(frf.values()) + 1
                    else:
                        frf[field] = 1
                field_number = frf[field]

                # The self.table entries for indicies, necessary in _sqlite to
                # be indexed, should not be needed in _nosql; so follow the
                # example of _db and put the self.index entries in self.table.
                fieldkey = SUBFILE_DELIMITER.join((file, field))
                self.table[fieldkey] = [
                    SUBFILE_DELIMITER.join(
                        (self.table[file][0], str(field_number))
                    )
                ]

                # Tree is needed only for ordered access to keys.
                fieldname = specification[SECONDARY][field]
                if fieldname is None:
                    fieldname = filespec.FileSpec.field_name(field)
                if ACCESS_METHOD in fieldprops[fieldname]:
                    if fieldprops[fieldname][ACCESS_METHOD] == BTREE:
                        self.trees[fieldkey] = tree.Tree(file, field, self)
                else:
                    self.trees[fieldkey] = tree.Tree(file, field, self)

                # List of segments containing records indexed by a value.
                # (Append SUBFILE_DELIMITER<value> to create database key.)
                self.segment_table[fieldkey] = SUBFILE_DELIMITER.join(
                    (
                        self.table[file][0],
                        str(field_number),
                        SEGMENT_KEY_SUFFIX,
                    )
                )

                # The records in a segment indexed by a value.
                # (SUBFILE_DELIMITER<segment Number>SUBFILE_DELIMITER<value> is
                # appended to create database key.)
                self.segment_records[fieldkey] = SUBFILE_DELIMITER.join(
                    (
                        self.table[file][0],
                        str(field_number),
                        SEGMENT_VALUE_SUFFIX,
                    )
                )

        if self.database_file is not None:
            if rsk is None and rssbk is None:
                self.dbenv[specification_key] = repr(self.specification)
                self.dbenv[segment_size_bytes_key] = repr(
                    self.segment_size_bytes
                )
                self.dbenv[table_register_key] = repr(table_register)
                self.dbenv[field_register_key] = repr(field_register)
        self.commit()

    def close_database_contexts(self, files=None):
        """Close files in database.

        Provided for compatibility with the DPT interface where there is a real
        difference between close_database_contexts() and close_database().

        In SQLite all the implementation detail is handled by the connection
        object bound to the self.dbenv object.

        The files argument is ignored because the connection object is deleted.

        """
        self.table = {}
        self.table_data = {}
        self.segment_table = {}
        self.segment_records = {}
        self.ebm_control = {}
        self.ebm_segment_count = {}
        if self.dbenv is not None:
            self.dbenv.close()
            self.dbenv = None
        self.segment_size_bytes = self._initial_segment_size_bytes

    def close_database(self):
        """Close primary and secondary databases and connection.

        That means clear all dictionaries of names of tables and indicies used
        in SQL statements executed by a self.dbenv.cursor() object, and close
        and discard the connection bound to self.dbenv.

        """
        self.close_database_contexts()

    def put(self, file, key, value):
        """Insert key, or replace key, in table for file using value."""
        # Normal source, put_instance, generates value by repr(object).
        assert file in self.specification
        if key is None:
            dbkey = self.next_record_number(file)
            self.dbenv[
                SUBFILE_DELIMITER.join((self.table_data[file], str(dbkey)))
            ] = value
            self.ebm_control[file].high_record_number = dbkey
            return dbkey
        self.dbenv[
            SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
        ] = value
        return None

    def replace(self, file, key, oldvalue, newvalue):
        """Replace key from table for file using newvalue.

        oldvalue is ignored in _sqlite version of replace() method.
        """
        # Normal source, edit_instance, generates oldvalue and newvalue by
        # repr(object).
        assert file in self.specification
        dbkey = SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
        try:
            self.dbenv[dbkey] = newvalue
        except KeyError:
            pass

    def delete(self, file, key, value):
        """Delete key from table for file.

        value is ignored in _nosql version of delete() method.
        """
        # Normal source, delete_instance, generates value by repr(object).
        assert file in self.specification
        dbkey = SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
        try:
            del self.dbenv[dbkey]
        except KeyError:
            pass

    def get_primary_record(self, file, key):
        """Return the instance given the record number in key."""
        assert file in self.specification
        if key is None:
            return None
        dbkey = SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
        if dbkey in self.dbenv:
            return key, self.dbenv[dbkey].decode()
        return None

    def encode_record_number(self, key):
        """Return repr(key) because this is sqlite3 version.

        Typically used to convert primary key to secondary index format,
        using Berkeley DB terminology.

        """
        return repr(key)

    def decode_record_number(self, skey):
        """Return literal_eval(skey) because this is sqlite3 version.

        Typically used to convert secondary index reference to primary record,
        a str(int), to a record number.

        """
        return literal_eval(skey)

    def encode_record_selector(self, key):
        """Return key because this is sqlite3 version.

        Typically used to convert a key being used to search a secondary index
        to the form held on the database.

        """
        return key

    def get_lowest_freed_record_number(self, dbset):
        """Return lowest freed record number in existence bitmap.

        The list of segments with freed record numbers is searched.
        """
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            if ebmc.ebm_freed in self.dbenv:
                ebmc.freed_record_number_pages = literal_eval(
                    self.dbenv[ebmc.ebm_freed].decode()
                )
            else:
                ebmc.freed_record_number_pages = []
        while len(ebmc.freed_record_number_pages):
            segment_number = ebmc.freed_record_number_pages[0]

            # Do not reuse record number on segment of high record number.
            if segment_number == ebmc.table_ebm_segments[-1]:
                return None

            lfrns = ebmc.read_exists_segment(segment_number, self.dbenv)
            if lfrns is None:

                # Segment does not exist now.
                ebmc.freed_record_number_pages.remove(segment_number)
                self.dbenv[ebmc.ebm_freed] = repr(
                    ebmc.freed_record_number_pages
                )
                continue

            try:
                first_zero_bit = lfrns.index(False, 0 if segment_number else 1)
            except ValueError:

                # No longer any record numbers available for re-use in segment.
                ebmc.freed_record_number_pages.remove(segment_number)
                self.dbenv[ebmc.ebm_freed] = repr(
                    ebmc.freed_record_number_pages
                )
                continue

            return (
                segment_number * SegmentSize.db_segment_size + first_zero_bit
            )
        return None

    def next_record_number(self, dbset):
        """Return high record number plus 1."""
        high_record = self.get_high_record(dbset)
        if high_record is None:
            return 0
        return high_record[0] + 1

    # high_record will become high_record_number to fit changed get_high_record.
    def note_freed_record_number_segment(
        self, dbset, segment, record_number_in_segment, high_record
    ):
        """Add existence bitmap segment to list with spare record numbers.

        Caller should check segment has unused records before calling
        note_freed_record_number_segment.  A successful record deletion
        passes this test.
        """
        try:
            high_segment = divmod(high_record[0], SegmentSize.db_segment_size)[
                0
            ]
        except TypeError:

            # Implies attempt to delete record from empty database.
            # The delete method will have raised an exception if appropriate.
            return

        if segment > high_segment:
            return
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            if ebmc.ebm_freed in self.dbenv:
                ebmc.freed_record_number_pages = literal_eval(
                    self.dbenv[ebmc.ebm_freed].decode()
                )
            else:
                ebmc.freed_record_number_pages = []
        insert = bisect_left(ebmc.freed_record_number_pages, segment)
        if ebmc.freed_record_number_pages:
            if insert < len(ebmc.freed_record_number_pages):
                if ebmc.freed_record_number_pages[insert] == segment:
                    return
        ebmc.freed_record_number_pages.insert(insert, segment)
        self.dbenv[ebmc.ebm_freed] = repr(ebmc.freed_record_number_pages)

    def remove_record_from_ebm(self, file, deletekey):
        """Remove deletekey from file's existence bitmap; return key.

        deletekey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        ebmcf = self.ebm_control[file]
        ebmb = ebmcf.get_ebm_segment(segment, self.dbenv)
        if ebmb is None:
            raise DatabaseError("Existence bit map for segment does not exist")
        ebm = Bitarray()
        ebm.frombytes(ebmb)
        ebm[record_number] = False
        self.ebm_control[file].put_ebm_segment(
            segment, ebm.tobytes(), self.dbenv
        )
        if ebmcf.high_record_number == deletekey:
            ebmcf.set_high_record_number(self.dbenv)
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        """Add putkey to file's existence bitmap; return (segment, record).

        putkey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        ebmcf = self.ebm_control[file]
        ebmb = ebmcf.get_ebm_segment(segment, self.dbenv)
        if ebmb is None:
            ebm = SegmentSize.empty_bitarray.copy()
            ebm[record_number] = True
            self.ebm_control[file].append_ebm_segment(
                ebm.tobytes(), self.dbenv
            )
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            ebm[record_number] = True
            self.ebm_control[file].put_ebm_segment(
                segment, ebm.tobytes(), self.dbenv
            )
        ebmcf.high_record_number = max(ebmcf.high_record_number, putkey)
        return segment, record_number

    # Change to return just the record number, and the name to fit.
    # Only used in one place, and it is extra work to get the data in_nosql.
    # Also actually getting the record does not prove it is the high record; in
    # _sqlite and _db asking for high record returns (key, data) where key will
    # be the high record number.
    def get_high_record(self, file):
        """Return the high existing record number in table for file."""
        high_record_number = self.ebm_control[file].high_record_number
        if high_record_number == -1:
            return None
        return (
            high_record_number,
            self.dbenv[
                SUBFILE_DELIMITER.join(
                    (self.table_data[file], str(high_record_number))
                )
            ],
        )

    def add_record_to_field_value(
        self, file, field, key, segment, record_number
    ):
        """Add record_number to set of records in segment for key.

        key is a value of index field on segment table for file.

        The representation of the set of records on the database is
        converted from integer to list to bitmap if the addition
        increases the number of records in the set above the relevant
        limit.
        """
        segment_table_key = SUBFILE_DELIMITER.join(
            (self.segment_table[SUBFILE_DELIMITER.join((file, field))], key)
        )
        db = self.dbenv
        if segment_table_key not in db:
            # Insert key into tree before creating segment_table_key record.
            if SUBFILE_DELIMITER.join((file, field)) in self.trees:
                self.trees[SUBFILE_DELIMITER.join((file, field))].insert(key)
            db[segment_table_key] = repr({segment: (record_number, 1)})
            return
        segment_table = literal_eval(db[segment_table_key].decode())
        if segment not in segment_table:
            segment_table[segment] = record_number, 1
            db[segment_table_key] = repr(segment_table)
            return
        reference = segment_table[segment][0]
        if isinstance(reference, int):
            segment_records = sorted({reference, record_number})
            if len(segment_records) == 1:
                return
            segment_records_key = SUBFILE_DELIMITER.join(
                (
                    self.segment_records[
                        SUBFILE_DELIMITER.join((file, field))
                    ],
                    str(segment),
                    key,
                )
            )
            db[segment_records_key] = repr(
                b"".join(
                    [n.to_bytes(2, byteorder="big") for n in segment_records]
                )
            )
            segment_table[segment] = LIST_BYTES, len(segment_records)
            db[segment_table_key] = repr(segment_table)
            return
        segment_records_key = SUBFILE_DELIMITER.join(
            (
                self.segment_records[SUBFILE_DELIMITER.join((file, field))],
                str(segment),
                key,
            )
        )
        if reference == LIST_BYTES:
            segment_records = RecordsetSegmentList(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()),
            )
            segment_records.insort_left_nodup(record_number)
            count = segment_records.count_records()
            if count > SegmentSize.db_upper_conversion_limit:
                db[segment_records_key] = repr(
                    segment_records.promote().tobytes()
                )
                segment_table[segment] = BITMAP_BYTES, count
                db[segment_table_key] = repr(segment_table)
                return
            db[segment_records_key] = repr(segment_records.tobytes())
            segment_table[segment] = LIST_BYTES, count
            db[segment_table_key] = repr(segment_table)
            return
        assert reference == BITMAP_BYTES
        segment_records = RecordsetSegmentBitarray(
            segment,
            key,
            records=literal_eval(db[segment_records_key].decode()),
        )
        # Cheat a little rather than say:
        # segment_records[segment * <segment size> + record_number] = True
        segment_records.bitarray[record_number] = True
        db[segment_records_key] = repr(segment_records.tobytes())
        segment_table[segment] = BITMAP_BYTES, segment_records.count_records()
        db[segment_table_key] = repr(segment_table)
        return

    def remove_record_from_field_value(
        self, file, field, key, segment, record_number
    ):
        """Remove record_number from set of records in segment for key.

        key is a value of index field on segment table for file.

        The representation of the set of records on the database is
        converted from bitmap to list to integer if the removal reduces
        the number of records in the set below the relevant limit.
        """
        segment_table_key = SUBFILE_DELIMITER.join(
            (self.segment_table[SUBFILE_DELIMITER.join((file, field))], key)
        )
        db = self.dbenv
        if segment_table_key not in db:
            return
        segment_table = literal_eval(db[segment_table_key].decode())
        if segment not in segment_table:
            return
        reference = segment_table[segment][0]
        if reference == BITMAP_BYTES:
            segment_records_key = SUBFILE_DELIMITER.join(
                (
                    self.segment_records[
                        SUBFILE_DELIMITER.join((file, field))
                    ],
                    str(segment),
                    key,
                )
            )
            segment_records = RecordsetSegmentBitarray(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()),
            )
            # Cheat a little rather than say:
            # segment_records[segment * <segment size> + record_number] = False
            segment_records.bitarray[record_number] = False
            count = segment_records.count_records()
            if count > SegmentSize.db_lower_conversion_limit:
                db[segment_records_key] = repr(segment_records.tobytes())
                segment_table[segment] = BITMAP_BYTES, count
                db[segment_table_key] = repr(segment_table)
                return
            # Cheat a little rather than say:
            # segment_records = segment_records.normalize(use_upper_limit=False)
            rsl = RecordsetSegmentList(segment, key)
            rsl.list.extend(segment_records.bitarray.search(SINGLEBIT))
            db[segment_records_key] = repr(rsl.tobytes())
            segment_table[segment] = LIST_BYTES, len(rsl.list)
            db[segment_table_key] = repr(segment_table)
            return
        if reference == LIST_BYTES:
            segment_records_key = SUBFILE_DELIMITER.join(
                (
                    self.segment_records[
                        SUBFILE_DELIMITER.join((file, field))
                    ],
                    str(segment),
                    key,
                )
            )
            segment_records = RecordsetSegmentList(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()),
            )
            # Cheating is only option!
            srl = segment_records.list
            discard = bisect_right(srl, record_number)
            if srl and srl[discard - 1] == record_number:
                del srl[discard - 1]
            count = segment_records.count_records()
            if count > 1:
                db[segment_records_key] = repr(segment_records.tobytes())
                segment_table[segment] = LIST_BYTES, count
                db[segment_table_key] = repr(segment_table)
                return
            del db[segment_records_key]
            segment_table[segment] = segment_records.list[0], 1
            db[segment_table_key] = repr(segment_table)
            return
        if reference == record_number:
            del segment_table[segment]
            if len(segment_table):
                db[segment_table_key] = repr(segment_table)
            else:
                # Delete segment_table_key record before deleting key from tree.
                del db[segment_table_key]
                if SUBFILE_DELIMITER.join((file, field)) in self.trees:
                    self.trees[SUBFILE_DELIMITER.join((file, field))].delete(
                        key
                    )

    def populate_segment(self, segment_number, segment_reference, file):
        """Return records for segment number and reference in table for file.

        A RecordsetSegmentBitarray, RecordsetSegmentList, or
        RecordsetSegmentInt, instance is returned.
        """
        if isinstance(segment_reference, int):
            return RecordsetSegmentInt(
                segment_number,
                None,
                records=segment_reference.to_bytes(2, byteorder="big"),
            )
        if len(segment_reference) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                segment_number, None, records=segment_reference
            )
        return RecordsetSegmentList(
            segment_number, None, records=segment_reference
        )

    def populate_recordset(self, recordset, db, keyprefix, segmentprefix, key):
        """Populate recordset with segments of records for key.

        Existing segments in recordset are replaced only if a segment
        with the same key and record number is found on database.

        """
        segment_records = literal_eval(
            db[SUBFILE_DELIMITER.join((segmentprefix, key))].decode()
        )
        for segment_number, record_number in segment_records.items():
            if record_number[0] == LIST_BYTES:
                segment = RecordsetSegmentList(
                    segment_number,
                    None,
                    records=literal_eval(
                        db[
                            SUBFILE_DELIMITER.join(
                                (
                                    keyprefix,
                                    SEGMENT_VALUE_SUFFIX,
                                    str(segment_number),
                                    key,
                                )
                            )
                        ].decode()
                    ),
                )
            elif record_number[0] == BITMAP_BYTES:
                segment = RecordsetSegmentBitarray(
                    segment_number,
                    None,
                    records=literal_eval(
                        db[
                            SUBFILE_DELIMITER.join(
                                (
                                    keyprefix,
                                    SEGMENT_VALUE_SUFFIX,
                                    str(segment_number),
                                    key,
                                )
                            )
                        ].decode()
                    ),
                )
            else:
                segment = RecordsetSegmentInt(
                    segment_number,
                    None,
                    records=record_number[0].to_bytes(2, byteorder="big"),
                )
            if segment_number not in recordset:
                recordset[segment_number] = segment  # .promote()
            else:
                recordset[segment_number] |= segment

    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        cursor = tree.Cursor(
            self.trees[SUBFILE_DELIMITER.join((file, valuespec.field))]
        )
        try:
            if valuespec.above_value and valuespec.below_value:
                k = cursor.nearest(valuespec.above_value)
                if k == valuespec.above_value:
                    k = cursor.next()
                while k:
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.above_value and valuespec.to_value:
                k = cursor.nearest(valuespec.above_value)
                if k == valuespec.above_value:
                    k = cursor.next()
                while k:
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.from_value and valuespec.to_value:
                k = cursor.nearest(valuespec.from_value)
                while k:
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.from_value and valuespec.below_value:
                k = cursor.nearest(valuespec.from_value)
                while k:
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.above_value:
                k = cursor.nearest(valuespec.above_value)
                if k == valuespec.above_value:
                    k = cursor.next()
                while k:
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.from_value:
                k = cursor.nearest(valuespec.from_value)
                while k:
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.to_value:
                k = cursor.first()
                while k:
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            elif valuespec.below_value:
                k = cursor.first()
                while k:
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
            else:
                k = cursor.first()
                while k:
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    k = cursor.next()
        finally:
            cursor.close()

    # The bit setting in existence bit map decides if a record is put on the
    # recordset created by the make_recordset_*() methods.

    def recordlist_record_number(self, file, key=None, cache_size=1):
        """Return RecordList on file containing records for key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return recordlist
        segment_number, record_number = divmod(
            key, SegmentSize.db_segment_size
        )
        if segment_number not in self.ebm_control[file].table_ebm_segments:
            return recordlist
        ebm_segment = self.ebm_control[file].get_ebm_segment(
            segment_number, self.dbenv
        )
        if ebm_segment and record_number in RecordsetSegmentBitarray(
            segment_number, key, records=ebm_segment
        ):
            recordlist[segment_number] = RecordsetSegmentList(
                segment_number,
                None,
                records=record_number.to_bytes(2, byteorder="big"),
            )
        return recordlist

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1
    ):
        """Return RecordList of records on file in a record number range.

        The records have record number between keystart and keyend.  Both
        default to include all records to the respective edge of segment.
        """
        if keystart is None and keyend is None:
            return self.recordlist_ebm(file, cache_size=cache_size)
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            segment_start, recnum_start = 0, 0
        else:
            segment_start, recnum_start = divmod(
                keystart, SegmentSize.db_segment_size
            )
        if keyend is not None:
            segment_end, recnum_end = divmod(
                keyend, SegmentSize.db_segment_size
            )
        else:
            segment_end, recnum_end = None, None
        ebmcf = self.ebm_control[file]
        first_segment = None
        final_segment = None
        for segment_number in ebmcf.table_ebm_segments:
            if segment_number < segment_start:
                continue
            if segment_end is not None and segment_number > segment_end:
                continue
            segment_record = literal_eval(
                self.dbenv[
                    SUBFILE_DELIMITER.join(
                        (ebmcf.ebm_table, str(segment_number))
                    )
                ].decode()
            )
            if segment_number == segment_start:
                if recnum_start:
                    first_segment, start_byte = divmod(recnum_start, 8)
                    segment_record = (
                        b"\x00" * first_segment
                        + segment_record[first_segment:]
                    )
            if keyend is not None:
                if (
                    segment_number == segment_end
                    and recnum_end < SegmentSize.db_segment_size - 1
                ):
                    final_segment, end_byte = divmod(recnum_end, 8)
                    segment_record = segment_record[
                        : final_segment + 1
                    ] + b"\x00" * (
                        SegmentSize.db_segment_size_bytes - final_segment - 1
                    )
            recordlist[segment_number] = RecordsetSegmentBitarray(
                segment_number, None, records=segment_record
            )
        if first_segment is not None:
            for i in range(first_segment * 8, first_segment * 8 + start_byte):
                recordlist[segment_start][(segment_start, i)] = False
        if final_segment is not None:
            for i in range(
                final_segment * 8 + end_byte, (final_segment + 1) * 8
            ):
                recordlist[segment_end][(segment_end, i)] = False
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList containing records on file."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        ebm_table = self.ebm_control[file].ebm_table
        for segment_number in self.ebm_control[file].table_ebm_segments:
            recordlist[segment_number] = RecordsetSegmentBitarray(
                segment_number,
                None,
                records=literal_eval(
                    self.dbenv[
                        SUBFILE_DELIMITER.join(
                            (ebm_table, str(segment_number))
                        )
                    ].decode()
                ),
            )
        return recordlist

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return RecordList containing records for field on file.

        The records are indexed by keys containing keylike.
        """
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(
                "".join(
                    ("'", field, "' field in '", file, "' file is not ordered")
                )
            )
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keylike is None:
            return recordlist
        matcher = re.compile(".*?" + keylike, flags=re.IGNORECASE | re.DOTALL)
        db = self.dbenv
        fieldtree = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(fieldtree)
        try:
            while True:
                k = cursor.next()
                if k is None:
                    break
                if not matcher.match(k):
                    continue
                self.populate_recordset(
                    recordlist,
                    db,
                    fieldtree.key_root,
                    fieldtree.key_segment,
                    k,
                )
        finally:
            cursor.close()
        return recordlist

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        key_root = self.table[SUBFILE_DELIMITER.join((file, field))][0]
        key_segment = SUBFILE_DELIMITER.join((key_root, SEGMENT_KEY_SUFFIX))
        # segment_records is not used, but populate_recordset does this
        # first without the try wrapper.  Not understood but left as it
        # is for now.
        try:
            segment_records = literal_eval(
                db[SUBFILE_DELIMITER.join((key_segment, key))].decode()
            )
        except KeyError:
            return recordlist
        except TypeError:
            if key is not None:
                raise
            return recordlist
        self.populate_recordset(recordlist, db, key_root, key_segment, key)
        return recordlist

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        The records are indexed by keys starting keystart.
        """
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(
                "".join(
                    ("'", field, "' field in '", file, "' file is not ordered")
                )
            )
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            return recordlist
        db = self.dbenv
        fieldtree = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(fieldtree)
        try:
            k = cursor.nearest(keystart)
            while k is not None:
                if not k.startswith(keystart):
                    break
                self.populate_recordset(
                    recordlist,
                    db,
                    fieldtree.key_root,
                    fieldtree.key_segment,
                    k,
                )
                k = cursor.next()
        finally:
            cursor.close()
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        Keys are in range set by combinations of ge, gt, le, and lt.
        """
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(
                "".join(
                    ("'", field, "' field in '", file, "' file is not ordered")
                )
            )
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        if le and lt:
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        fieldtree = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(fieldtree)
        try:
            if ge is None and gt is None:
                k = cursor.first()
            else:
                k = cursor.nearest(ge or gt)
            if gt:
                while k is not None:
                    if k > gt:
                        break
                    k = cursor.next()
            if le is None and lt is None:
                while k is not None:
                    self.populate_recordset(
                        recordlist,
                        db,
                        fieldtree.key_root,
                        fieldtree.key_segment,
                        k,
                    )
                    k = cursor.next()
            elif lt is None:
                while k is not None:
                    if k > le:
                        break
                    self.populate_recordset(
                        recordlist,
                        db,
                        fieldtree.key_root,
                        fieldtree.key_segment,
                        k,
                    )
                    k = cursor.next()
            else:
                while k is not None:
                    if k >= lt:
                        break
                    self.populate_recordset(
                        recordlist,
                        db,
                        fieldtree.key_root,
                        fieldtree.key_segment,
                        k,
                    )
                    k = cursor.next()
        finally:
            cursor.close()
        return recordlist

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(
                "".join(
                    ("'", field, "' field in '", file, "' file is not ordered")
                )
            )
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        fieldtree = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(fieldtree)
        try:
            while True:
                k = cursor.next()
                if k is None:
                    break
                self.populate_recordset(
                    recordlist,
                    db,
                    fieldtree.key_root,
                    fieldtree.key_segment,
                    k,
                )
        finally:
            cursor.close()
        return recordlist

    def recordlist_nil(self, file, cache_size=1):
        """Return empty RecordList on file."""
        return RecordList(dbhome=self, dbset=file, cache_size=cache_size)

    def unfile_records_under(self, file, field, key):
        """Delete the reference to records for index field[key].

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        assert file in self.table
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        table_key = SUBFILE_DELIMITER.join((self.segment_table[fieldkey], key))
        segment_key = self.segment_records[fieldkey]
        db = self.dbenv
        if table_key not in db:
            return
        for segment_number, ref in literal_eval(
            db[table_key].decode()
        ).items():
            if isinstance(ref[0], str):
                del db[
                    SUBFILE_DELIMITER.join(
                        (segment_key, str(segment_number), key)
                    )
                ]
        del self.dbenv[table_key]
        if fieldkey in self.trees:
            self.trees[fieldkey].delete(key)

    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records."""
        assert recordset.dbset == file
        assert file in self.table
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        segment_key = self.segment_records[fieldkey]

        # Delete existing segments for key
        self.unfile_records_under(file, field, key)

        recordset.normalize()

        db = self.dbenv
        segments = {}
        for segment_number, rs_segment in recordset.rs_segments.items():
            if isinstance(rs_segment, RecordsetSegmentBitarray):
                db[
                    SUBFILE_DELIMITER.join(
                        (segment_key, str(segment_number), key)
                    )
                ] = repr(rs_segment.tobytes())
                segments[segment_number] = (
                    BITMAP_BYTES,
                    rs_segment.count_records(),
                )
            elif isinstance(rs_segment, RecordsetSegmentList):
                db[
                    SUBFILE_DELIMITER.join(
                        (segment_key, str(segment_number), key)
                    )
                ] = repr(rs_segment.tobytes())
                segments[segment_number] = (
                    LIST_BYTES,
                    rs_segment.count_records(),
                )
            elif isinstance(rs_segment, RecordsetSegmentInt):
                segments[segment_number] = rs_segment.record_number, 1
        if fieldkey in self.trees:
            self.trees[fieldkey].insert(key)
        db[SUBFILE_DELIMITER.join((self.segment_table[fieldkey], key))] = repr(
            segments
        )

    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on SQLite Connection() for (file, field).

        keyrange is an addition for DPT. It may yet be removed.

        """
        assert file in self.specification
        if file == field:
            return CursorPrimary(self, file=file, keyrange=keyrange)
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        if fieldkey not in self.trees:
            raise DatabaseError(
                "".join(
                    ("'", field, "' field in '", file, "' file is not ordered")
                )
            )
        return CursorSecondary(self, file=file, field=field, keyrange=keyrange)

    def create_recordset_cursor(self, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursor(recordset, self.dbenv)

    # Comment in chess_ui for make_position_analysis_data_source method, only
    # call, suggests is_database_file_active should not be needed.
    def is_database_file_active(self, file):
        """Return True if the SQLite database connection exists.

        SQLite version of method ignores file argument.

        """
        return self.dbenv is not None

    def get_table_connection(self, file):
        """Return NoSQL database connection.  The file argument is ignored.

        The file argument is present for compatibility with versions of this
        method defined in sibling modules.

        The connection is an unqlite.UnQLite or a vedis.Vedis object.

        """
        return self.dbenv

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs=None,
        use_specification_items=None,
    ):
        """Open new connection to database, run method, then close connection.

        This method is intended for use in a separate thread from the one
        dealing with the user interface.  If the normal user interface thread
        also uses a separate thread for it's normal, quick, database actions
        there is probably no need to use this method at all.

        This method assumes usage like:

        class _ED(_sqlite.Database):
            def open_database(self, **k):
                try:
                    super().open_database(dbe_module, **k)
                except self.__class__.SegmentSizeError:
                    super().open_database(dbe_module, **k)
        class DPTcompatibility:
            def open_database(self, files=None):
                super().open_database(files=files)
                return True
        class _AD(DPTcompatibility, _ED):
            def __init__(self, folder, **k):
                super().__init__(FileSpec(**kargs), folder, **k)
        d = _AD(foldername, **k)
        d.do_database_task(method_name, **k)

        but the unittest abbreviates the class structure to:

        class _ED(_db.Database):
            def open_database(self, **k):
                super().open_database(dbe_module, **k)
        class _AD(_ED):
            def __init__(self, folder, **k):
                super().__init__({}, folder, **k)

        where dbe_module is either sqlite3 or apsw.

        """
        db = self.__class__(
            self.home_directory,
            use_specification_items=use_specification_items,
        )
        db.open_database()
        if taskmethodargs is None:
            taskmethodargs = {}
        try:
            taskmethod(db, logwidget, **taskmethodargs)
        finally:
            db.close_database()

    # Anticipate maintaining a cache of database (key, value) objects.
    def _read_key(self, key):
        return self.database.dbenv[key]

    # Anticipate maintaining a cache of database (key, value) objects.
    def _write_key(self, key, value):
        self.database.dbenv[key] = repr(value)


class Cursor(_cursor.Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - _nosql.Database object.
    table - table name of table the cursor will be applied to.
    file - file name of table in FileSpec() object for database.
    keyrange - not used.
    kargs - absorb argunents relevant to other database engines.

    A SQLite3 cursor is created which exists until this Cursor is
    deleted.

    The CursorPrimary and CursorSecondary subclasses define the
    bsddb style cursor methods peculiar to primary and secondary databases.

    Primary and secondary database, and others, should be read as the Berkeley
    DB usage.  This class emulates interaction with a Berkeley DB database via
    the Python bsddb3 module.

    Segmented should be read as the DPT database engine usage.

    The value part of (key, value) on primary or secondary databases is either:

        primary key (segment and record number)
        reference to a list of primary keys for a segment
        reference to a bit map of primary keys for a segment

    References are to rowids on the primary database's segment table.

    Each primary database rowid is mapped to a bit in the bitmap associated
    with the segment for the primary database rowid.

    """

    def __init__(self, dbset, file=None, keyrange=None, **kargs):
        """Define a cursor on the underlying database engine dbset."""
        super().__init__(dbset.dbenv)
        self._file = file
        self._current_segment = None
        self.current_segment_number = None
        self._current_record_number_in_segment = None

    def close(self):
        """Delete database cursor then extend."""
        self._file = None
        self.current_segment_number = None
        self._current_record_number_in_segment = None
        super().close()

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError("get_partial_with_wildcard not implemented")

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return self._partial

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in _nosql.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """


class CursorPrimary(Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - apsw or sqlite3 Connection() object.
    ebm - table name of existence bitmap of file cursor will be applied to.
    kargs - superclass arguments and absorb arguments for other engines.

    This class does not need a field argument, like CursorSecondary, because
    the file argument collected by super().__init__() fills that role here.

    """

    def __init__(self, dbset, **kargs):
        """Extend, note existence bitmap table name, and table name."""
        super().__init__(dbset, **kargs)
        self._table = dbset.table_data[self._file]
        self._ebm = dbset.ebm_control[self._file]

    def count_records(self):
        """Return record count or None if cursor is not usable."""
        if self._dbset is None:
            return None
        count = 0
        db = self._dbset
        ebm = self._ebm
        for segment_number in ebm.table_ebm_segments:
            count += ebm.read_exists_segment(segment_number, db).count()
        return count

    def first(self):
        """Return first record."""
        db = self._dbset
        ebm = self._ebm
        for segment_number in ebm.table_ebm_segments:
            seg = RecordsetSegmentBitarray(
                segment_number, None, ebm.get_ebm_segment(segment_number, db)
            )
            k = seg.first()
            if k:
                return self._get_record(seg, k)
        return None

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        count = 0
        db = self._dbset
        ebm = self._ebm
        tes = ebm.table_ebm_segments
        if not tes:
            return count
        segment_number, record_number = divmod(
            record[0], SegmentSize.db_segment_size
        )
        index = bisect_right(tes, segment_number)
        for i in ebm.table_ebm_segments[: index - 1]:
            count += ebm.read_exists_segment(i, db).count()
        if tes[index - 1] == segment_number:
            count += RecordsetSegmentBitarray(
                segment_number, None, ebm.get_ebm_segment(segment_number, db)
            ).get_position_of_record_number(record_number)
        elif tes[index - 1] < segment_number:
            count += ebm.read_exists_segment(tes[index - 1], db).count()
        return count

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        db = self._dbset
        ebm = self._ebm
        tes = ebm.table_ebm_segments
        if not tes:
            return None
        count = 0
        if position < 0:
            for segment_number in reversed(tes):
                bitarray = ebm.read_exists_segment(segment_number, db)
                bacount = bitarray.count()
                count -= bacount
                if count > position:
                    continue
                seg = _empty_recordset_segment_bitarray()
                seg.bitarray = bitarray
                seg.segment_number = segment_number
                seg.current_position_in_segment = position - count - bacount
                k = seg.get_record_number_at_position(
                    seg.current_position_in_segment
                )
                if k is not None:
                    return self._get_record(seg, (None, k))
                break
            else:
                return None
        else:
            for segment_number in tes:
                bitarray = ebm.read_exists_segment(segment_number, db)
                bacount = bitarray.count()
                count += bacount
                if count <= position:
                    continue
                seg = _empty_recordset_segment_bitarray()
                seg.bitarray = bitarray
                seg.segment_number = segment_number
                seg.current_position_in_segment = position - count + bacount
                k = seg.get_record_number_at_position(
                    seg.current_position_in_segment
                )
                if k is not None:
                    return self._get_record(seg, (None, k))
                break
            else:
                return None

    def last(self):
        """Return last record."""
        db = self._dbset
        ebm = self._ebm
        for segment_number in reversed(ebm.table_ebm_segments):
            seg = RecordsetSegmentBitarray(
                segment_number, None, ebm.get_ebm_segment(segment_number, db)
            )
            k = seg.last()
            if k:
                return self._get_record(seg, k)
        return None

    def nearest(self, key):
        """Return nearest record to key."""
        db = self._dbset
        ebm = self._ebm
        segment_number, record_number = divmod(
            key, SegmentSize.db_segment_size
        )
        for i in ebm.table_ebm_segments[
            bisect_left(ebm.table_ebm_segments, segment_number) :
        ]:
            seg = RecordsetSegmentBitarray(i, None, ebm.get_ebm_segment(i, db))
            if record_number in seg:
                return self._get_record(
                    seg,
                    (None, i * SegmentSize.db_segment_size + record_number),
                )
            seg.current_position_in_segment = record_number
            record_number = seg.next()
            if record_number is not None:
                return self._get_record(seg, (None, record_number[-1]))
            record_number = 0
        return None

    def next(self):
        """Return next record."""
        if self.current_segment_number is None:
            return self.first()
        db = self._dbset
        ebm = self._ebm
        segment_number = self.current_segment_number
        record_number = self._current_record_number_in_segment
        if record_number == SegmentSize.db_segment_size - 1:
            record_number = 0
            segment_number += 1
        else:
            record_number += 1
        for i in ebm.table_ebm_segments[
            bisect_left(ebm.table_ebm_segments, segment_number) :
        ]:
            seg = RecordsetSegmentBitarray(i, None, ebm.get_ebm_segment(i, db))
            seg.current_position_in_segment = record_number
            if record_number in seg:
                return self._get_record(
                    seg,
                    (None, i * SegmentSize.db_segment_size + record_number),
                )
            seg.current_position_in_segment = record_number
            record_number = seg.next()
            if record_number is not None:
                return self._get_record(seg, (None, record_number[-1]))
            record_number = 0
        return None

    def prev(self):
        """Return previous record."""
        if self.current_segment_number is None:
            return self.last()
        db = self._dbset
        ebm = self._ebm
        segment_number = self.current_segment_number
        record_number = self._current_record_number_in_segment
        if record_number == 0:
            record_number = SegmentSize.db_segment_size - 1
            segment_number -= 1
        else:
            record_number -= 1
        for i in reversed(
            ebm.table_ebm_segments[
                : bisect_left(ebm.table_ebm_segments, segment_number + 1)
            ]
        ):
            seg = RecordsetSegmentBitarray(i, None, ebm.get_ebm_segment(i, db))
            seg.current_position_in_segment = record_number
            if record_number in seg:
                return self._get_record(
                    seg,
                    (None, i * SegmentSize.db_segment_size + record_number),
                )
            seg.current_position_in_segment = record_number
            record_number = seg.prev()
            if record_number is not None:
                return self._get_record(seg, (None, record_number[-1]))
            record_number = SegmentSize.db_segment_size - 1
        return None

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        ebm = self._ebm
        tes = ebm.table_ebm_segments
        segment_number, record_number = divmod(
            record[0], SegmentSize.db_segment_size
        )
        i = bisect_right(tes, segment_number)
        if tes and tes[i - 1] == segment_number:
            seg = RecordsetSegmentBitarray(
                segment_number,
                None,
                ebm.get_ebm_segment(segment_number, self._dbset),
            )
            if record_number in seg:
                seg.current_position_in_segment = record_number
                return self._get_record(
                    seg,
                    (
                        None,
                        segment_number * SegmentSize.db_segment_size
                        + record_number,
                    ),
                )
        return None

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # raise DatabaseError('refresh_recordset not implemented')

    def _get_record(self, segment, ref):
        assert ref is not None
        data = self._dbset[
            SUBFILE_DELIMITER.join((self._table, str(ref[-1])))
        ].decode()
        (
            self.current_segment_number,
            self._current_record_number_in_segment,
        ) = (segment.segment_number, segment.current_position_in_segment)
        return (ref[-1], data)


class CursorSecondary(Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - apsw or sqlite3 Connection() object.
    field - field name of table for file in FileSpec() object for database.
    segment - name of segment table for file in FileSpec() object for database.
    kargs - superclass arguments and absorb arguments for other engines.

    The file name is collected by super().__init__() call, and is used in this
    class as the name of the column containing references to rows in the table
    named file in the FileSpec() object for the database.

    """

    def __init__(self, dbset, field=None, **kargs):
        """Extend, note field and make data structures for ordered keys."""
        super().__init__(dbset, **kargs)
        fieldkey = SUBFILE_DELIMITER.join((self._file, field))
        if fieldkey not in dbset.trees:
            raise DatabaseError(
                "".join(
                    (
                        "Cannot create cursor because '",
                        field,
                        "' field in '",
                        self._file,
                        "' file is not ordered",
                    )
                )
            )
        self._field = field
        self._table = dbset.table_data[self._file]
        self._tree = dbset.trees[fieldkey]
        self._cursor = tree.Cursor(self._tree)
        self._value_prefix = "".join(
            (
                self._tree.key_root,
                SUBFILE_DELIMITER,
                SEGMENT_VALUE_SUFFIX,
                SUBFILE_DELIMITER,
            )
        )
        self._segment_table_prefix = "".join(
            (self._tree.key_segment, SUBFILE_DELIMITER)
        )
        self._segment_table = None

    def count_records(self):
        """Return count of key references to records.

        When n keys refer to a record the count is incremented by 10, not 1.
        In a recordset built from the same keys the count would be incremented
        by 1, not 10.
        """
        db = self._dbset
        cursor = self._cursor
        count = 0
        if self.get_partial() in (None, False):
            while True:
                key = cursor.next()
                if key is None:
                    break
                count += SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, key
                ).count_records()
        else:
            key = cursor.nearest(self.get_converted_partial())
            while key is not None:
                if not key.startswith(self.get_converted_partial()):
                    break
                count += SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, key
                ).count_records()
                key = cursor.next()
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            return self._first()
        if self.get_partial() is False:
            return None
        record = self.nearest(self.get_converted_partial())
        if record is not None:
            if not record[0].startswith(self.get_partial()):
                return None
        return record

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        db = self._dbset
        key, value = record
        segment_number, record_number = divmod(
            value, SegmentSize.db_segment_size
        )

        # Define lambdas to handle presence or absence of partial key.
        low = lambda rk, recordkey: rk < recordkey
        if not self.get_partial():
            high = lambda rk, recordkey: rk > recordkey
        else:
            high = lambda rk, partial: not rk.startswith(partial)

        # Get position of record relative to start point.
        position = 0
        if not self.get_partial():
            rkey = self._cursor.first()
        else:
            rkey = self._cursor.nearest(
                self.get_converted_partial_with_wildcard()
            )
        while rkey:
            if low(rkey, key):
                position += SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, rkey
                ).count_records()
            elif high(rkey, key):
                break
            else:
                segment_records = SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, rkey
                )
                while True:
                    i = segment_records.next()
                    if i is None:
                        break
                    if i < segment_number:
                        position += (
                            segment_records.count_current_segment_records()
                        )
                    elif i == segment_number:
                        # Comment for black 21.5b1 at another example.
                        position += (
                            segment_records.get_current_segment()
                        ).get_position_of_record_number(record_number)
                        break
            rkey = self._cursor.next()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        db = self._dbset

        # Start at first or last record whichever is likely closer to position
        # and define lambdas to handle presence or absence of partial key.
        if not self.get_partial():
            get_partial = self.get_partial
        else:
            get_partial = self.get_converted_partial
        if position < 0:
            step = self._cursor.prev
            if not self.get_partial():
                start = lambda partial: self._cursor.last()
            else:
                start = lambda partial: self._last_partial(partial)
        else:
            step = self._cursor.next
            if not self.get_partial():
                start = lambda partial: self._cursor.first()
            else:
                start = lambda partial: self._first_partial(partial)

        # Get record at position relative to start point
        # r2 named for the way this is done in ._sqlite module.
        count = 0
        key = start(get_partial())
        if position < 0:
            while key:
                segment_records = SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, key
                )
                while True:
                    ssn = segment_records.prev()
                    if ssn is None:
                        break
                    i = segment_records.count_current_segment_records()
                    count -= i
                    if count > position:
                        continue
                    # At black version 21.5b1 the () not required by
                    # syntax have to be present to get the formatting
                    # seen here.  Issues 2279, 1723, 1671, and 571, on
                    # black at Github seem to cover it.
                    record_number = (
                        segment_records.get_current_segment()
                    ).get_record_number_at_position(position - count - i)
                    if record_number is not None:
                        return key, record_number
                    return None
                key = step()
        else:
            while key:
                segment_records = SegmentsetCursor(
                    db, self._segment_table_prefix, self._value_prefix, key
                )
                while True:
                    ssn = segment_records.next()
                    if ssn is None:
                        break
                    i = segment_records.count_current_segment_records()
                    count += i
                    if count <= position:
                        continue
                    # Comment for black 21.5b1 at another example.
                    record_number = (
                        segment_records.get_current_segment()
                    ).get_record_number_at_position(position - count + i)
                    if record_number is not None:
                        return key, record_number
                    return None
                key = step()

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            return self._last()
        if self.get_partial() is False:
            return None
        chars = list(self.get_partial())
        while True:
            try:
                chars[-1] = chr(ord(chars[-1]) + 1)
            except ValueError:
                chars.pop()
                if not chars:
                    try:
                        k, value = self._cursor.last()
                    except TypeError:
                        return None
                    return k, value
                continue
            self._nearest("".join(chars))
            try:
                k, value = self._prev()
            except TypeError:
                return None
            return k, value

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if self.get_partial() is False:
            return None
        try:
            k, value = self._nearest(key)
        except TypeError:
            return None
        if self.get_partial() is not None:
            if not k.startswith(self.get_converted_partial()):
                return None
        return k, value

    def next(self):
        """Return next record taking partial key into account."""
        if self._current_segment is None:
            return self.first()
        if self.get_partial() is False:
            return None
        try:
            k, value = self._next()
        except TypeError:
            return None
        return k, value

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._current_segment is None:
            return self.last()
        if self.get_partial() is False:
            return None
        try:
            k, value = self._prev()
        except TypeError:
            return None
        return k, value

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.

        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            if not record[0].startswith(self.get_partial()):
                return None
        if self._cursor.setat(record[0]) is None:
            return None
        segment_number, record_number = divmod(
            record[1], SegmentSize.db_segment_size
        )
        segment_table = SegmentsetCursor(
            self._dbset,
            self._segment_table_prefix,
            self._value_prefix,
            record[0],
        )
        ssn = segment_table.sorted_segment_numbers
        table_index = bisect_right(ssn, segment_number)
        if not (ssn and ssn[table_index - 1] == segment_number):
            return None
        segment = self.set_current_segment_table(
            record[0], segment_table, table_index - 1
        )
        if record_number not in segment:
            return None
        # The minus 1 should not be there but it prevents the problem occuring
        # until a segment with one record is met.
        # Implies problem is in this class' next, prev, etc methods and the
        # interaction with the three RecordsetSegment... classes methods used
        # in each case.
        # I think I forget the record position is based at 1 not 0 when writing
        # this code.
        segment.current_position_in_segment = (
            segment.get_position_of_record_number(record_number)
        )  # - 1)
        return record

    def set_partial_key(self, partial):
        """Set partial key and mark current segment as None."""
        self._partial = partial
        self._current_segment = None
        self.current_segment_number = None

    def set_current_segment(self, key):
        """Return the recordset segment for current segment.

        The returned item is a RecordsetSegmentBitarray, RecordsetSegmentInt,
        or RecordsetSegmentList instance, depending on the current
        representation of the segment on the database.

        key is ignored.  Argument is present for compatibility with other
        database engines.

        """
        self.current_segment_number = (
            self._segment_table.current_segment_number
        )
        self._current_segment = self._segment_table.get_current_segment()
        return self._current_segment

    def set_current_segment_table(self, key, segment_table, table_index=None):
        """Make segment_table current, and return current_segment."""
        self._segment_table = segment_table
        segment_table.current_segment_number = (
            segment_table.sorted_segment_numbers[table_index]
        )
        self.current_segment_number = segment_table.current_segment_number
        self._current_segment = segment_table.get_current_segment()
        return self._current_segment

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.

        # raise DatabaseError('refresh_recordset not implemented')

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        nkey = self._cursor.nearest(key)
        if nkey != key:
            return None
        segment_table = SegmentsetCursor(
            self._dbset, self._segment_table_prefix, self._value_prefix, key
        )
        if len(segment_table) == 1:
            segment_number = segment_table.sorted_segment_numbers[0]
            record_number = segment_table.segments[segment_number]
            if isinstance(record_number, int):
                return (
                    record_number
                    + segment_number * SegmentSize.db_segment_size
                )
        raise DatabaseError("Index must refer to unique record")

    def _first(self):
        key = self._cursor.first()
        if key is None:
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(
                self._dbset,
                self._segment_table_prefix,
                self._value_prefix,
                key,
            ),
            0,
        ).first()

    def _last(self):
        key = self._cursor.last()
        if key is None:
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(
                self._dbset,
                self._segment_table_prefix,
                self._value_prefix,
                key,
            ),
            -1,
        ).last()

    def _nearest(self, key):
        key = self._cursor.nearest(key)
        if key is None:
            self._current_segment = None
            self.current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(
                self._dbset,
                self._segment_table_prefix,
                self._value_prefix,
                key,
            ),
            0,
        ).first()

    def _next(self):
        key = self._current_segment.next()
        if key is not None:
            return key
        if self._segment_table.next() is not None:
            self.set_current_segment(self._current_segment.index_key)
            key = self._current_segment.next()
            if key is not None:
                return key
        key = self._cursor.next()
        if key is None:
            return None
        if self.get_partial() is not None:
            if not key.startswith(self.get_converted_partial()):
                return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(
                self._dbset,
                self._segment_table_prefix,
                self._value_prefix,
                key,
            ),
            0,
        ).first()

    def _prev(self):
        key = self._current_segment.prev()
        if key is not None:
            return key
        if self._segment_table.prev() is not None:
            self.set_current_segment(self._current_segment.index_key)
            key = self._current_segment.prev()
            if key is not None:
                if self.get_partial() is not None:
                    if not key[0].startswith(self.get_converted_partial()):
                        return None
                return key
        key = self._cursor.prev()
        if key is None:
            return None
        if self.get_partial() is not None:
            if not key.startswith(self.get_converted_partial()):
                return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(
                self._dbset,
                self._segment_table_prefix,
                self._value_prefix,
                key,
            ),
            -1,
        ).last()

    def _first_partial(self, partial):
        record = self._cursor.nearest(partial)
        if record is None:
            return None
        if not record.startswith(partial):
            return None
        return record

    def _last_partial(self, partial):
        record = self._first_partial(partial)
        if record is None:
            return None
        while True:
            record = self._cursor.next()
            if record is None:
                record = self._cursor.last()
                if record is None:
                    return None
                if not record.startswith(partial):
                    return None
                return record
            if not record.startswith(partial):
                record = self._cursor.prev()
                if record is None:
                    return None
                if not record.startswith(partial):
                    return None
                return record


class RecordsetCursor(_RecordsetCursor):
    """Add _get_record method to RecordsetCursor.

    RecordsetCursor is imported from recordset as _RecordsetCursor to
    avoid confusion on the class names within the _nosql module.
    """

    def __init__(self, recordset, engine, **kargs):
        """Delegate recordset to superclass and note engine.

        kargs absorbs arguments relevant to other database engines.

        """
        super().__init__(recordset)
        self.engine = engine

    # These comments were written for solentware_base version of this class
    # and need translating!
    # Hack to get round self._dbset._database being a Sqlite.Cursor which means
    # the RecordList.get_record method does not work here because it does:
    # record = self._database.get(record_number)
    # All self._dbset.get_record(..) calls replaced by self._get_record(..) in
    # this module (hope no external use for now).
    # Maybe RecordList should not have a get_record method.
    def _get_record(self, record_number, use_cache=False):
        """Return (record_number, record) using cache if requested."""
        dbset = self._dbset
        if use_cache:
            record = dbset.record_cache.get(record_number)
            if record is not None:
                return record_number, record
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return None  # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return None  # maybe raise
        dbkey = SUBFILE_DELIMITER.join(
            (dbset.dbhome.table_data[dbset.dbset], str(record_number))
        )
        try:
            record = self.engine[dbkey].decode()
        except KeyError:
            return None
        # maybe raise if record is None (if not, None should go on cache)
        if use_cache:
            dbset.record_cache[record_number] = record
            dbset.record.deque.append(record_number)
        return record_number, record


class ExistenceBitmapControl(_database.ExistenceBitmapControl):
    """Access existence bitmap for file in database.

    ebm_table is the string at start of key of an existence bitmap segment
    record, or the list of segments containing freed record numbers, for the
    file.  The third element of <file>_<field>_<third element> must not be a
    str(int()) because <file>_<field>_<str(int())> are the data record keys.

    table_ebm_segments is the set of segment numbers which exist on the file.
    It is stored on the database as a tuple because literal_eval(set()) raises
    a ValueError exception.  Fortunately we are not trying to put a set in the
    tuple, which gives a TypeError exception in repr().
    """

    def __init__(self, file, notional_field, database):
        """Note file whose existence bitmap is managed."""
        super().__init__(file, database)
        self.ebm_table = SUBFILE_DELIMITER.join(
            (self._file, notional_field, EXISTENCE_BITMAP_SUFFIX)
        )
        self.ebm_freed = SUBFILE_DELIMITER.join(
            (self._file, notional_field, FREED_RECORD_NUMBER_SEGMENTS_SUFFIX)
        )
        dbenv = database.dbenv

        # Cannot do database.get(...) because it does not return None if key
        # does not exist, and unqlite calls this method fetch(...).
        # ebm_table is supposed to be a set, but literal_eval(repr(set())) gives
        # a ValueError exception. However literal_eval(repr(set((1,)))), a non
        # empty set in other words, is fine. repr(set((1, {2,3}))) gives a
        # TypeError, unhashable type so I thought I may have to move to json to
        # do this data storage, but json.dumps(set((1, {2,3}))) gives the same
        # TypeError exception.
        # The code below assumes the idea:
        # t = ()
        # try:
        #     t.add(1)
        # except AttributeError:
        #     if not isinstance(t, tuple) or len(t):
        #         raise
        #     t = set((1,))
        # combined with:
        # t.remove(1)
        # if not len(t) and isinstance(t, set):
        #     t = ()
        # will solve the problem at the repr and literal_eval interface.
        # Otherwise it's pickle.
        if self.ebm_table not in dbenv:
            dbenv[self.ebm_table] = repr([])
        self.table_ebm_segments = literal_eval(dbenv[self.ebm_table].decode())

        self._segment_count = len(self.table_ebm_segments)
        self.set_high_record_number(dbenv)

    def read_exists_segment(self, segment_number, dbenv):
        """Return existence bitmap for segment_number in database dbenv.

        get_ebm_segment returns the record containing the existence bitmap,
        read_exists_segment calls get_ebm_segment to get the bitmap record,
        converts it to a bitmap, and then returns the bitmap.

        """
        # Return existence bit map for segment_number.
        ebm = Bitarray()
        try:
            ebm.frombytes(self.get_ebm_segment(segment_number, dbenv))
        except TypeError:
            return None
        return ebm

    def get_ebm_segment(self, key, dbenv):
        """Return existence bitmap for segment number key in database dbenv.

        get_ebm_segment returns the record containing the existence bitmap,
        use read_exists_segment to return the bitmap itself.

        """
        tes = self.table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            return literal_eval(
                dbenv[
                    SUBFILE_DELIMITER.join((self.ebm_table, str(key)))
                ].decode()
            )
        return None

    # Not used at present but defined anyway.
    def delete_ebm_segment(self, key, dbenv):
        """Delete existence bitmap for segment key from database dbenv."""
        tes = self.table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            del dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))]
            del self.table_ebm_segments[insertion_point - 1]
            dbenv[self.ebm_table] = repr(tes)
            self._segment_count = len(tes)

    def put_ebm_segment(self, key, value, dbenv):
        """Update existence bitmap value for segment key to database dbenv."""
        tes = self.table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))] = repr(
                value
            )

    def append_ebm_segment(self, value, dbenv):
        """Add existence bitmap, value, to database, dbenv."""
        segments = self.table_ebm_segments
        key = segments[-1] + 1 if len(segments) else 0
        dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))] = repr(value)
        segments.append(key)
        dbenv[self.ebm_table] = repr(segments)
        self._segment_count = len(segments)
        return key

    def set_high_record_number(self, dbenv):
        """Set high record number from existence bitmap for database dbenv."""
        for i in reversed(self.table_ebm_segments):
            high_segment = RecordsetSegmentBitarray(
                i, None, self.get_ebm_segment(i, dbenv)
            ).last()
            if high_segment is None:
                continue
            self.high_record_number = high_segment[-1]
            break
        else:
            self.high_record_number = -1


class SegmentsetCursor:
    """Provide cursor for segment headers for value of field in file.

    The current segment number can be retrieved from the database
    but it is discarded once the current pointer moves on.
    """

    def __init__(self, dbenv, segment_table_prefix, value_prefix, key):
        """Initialize data structures for segment set cursor."""
        self.current_segment_number = None
        self._dbenv = dbenv
        self._index = "".join((segment_table_prefix, key))
        self.segments = literal_eval(self._dbenv[self._index].decode())
        self.sorted_segment_numbers = sorted(self.segments)
        self._values_index = value_prefix, "".join((SUBFILE_DELIMITER, key))

    def __del__(self):
        """Delete segment set."""
        self.close()

    def close(self):
        """Close segment set making it unusable."""
        self.current_segment_number = None
        self._dbenv = None
        self._index = None
        self.segments = None
        self.sorted_segment_numbers = None
        self._values_index = None

    def __len__(self):
        """Return number of existing segments."""
        return len(self.segments)

    def __contains__(self, segment_number):
        """Return True if segment_number exists."""
        return segment_number in self.segments

    def first(self):
        """Return segment number of first segment in segment number order."""
        if not self.sorted_segment_numbers:
            return None
        self.current_segment_number = self.sorted_segment_numbers[0]
        return self.current_segment_number

    def last(self):
        """Return segment number of last segment in segment number order."""
        if not self.sorted_segment_numbers:
            return None
        self.current_segment_number = self.sorted_segment_numbers[-1]
        return self.current_segment_number

    def next(self):
        """Return segment number of next segment in segment number order."""
        if self.current_segment_number is None:
            return self.first()
        point = bisect_right(
            self.sorted_segment_numbers, self.current_segment_number
        )
        if point == len(self.sorted_segment_numbers):
            return None
        self.current_segment_number = self.sorted_segment_numbers[point]
        return self.current_segment_number

    def prev(self):
        """Return segment number of previous segment in segment number order."""
        if self.current_segment_number is None:
            return self.last()
        point = bisect_left(
            self.sorted_segment_numbers, self.current_segment_number
        )
        if point == 0:
            return None
        self.current_segment_number = self.sorted_segment_numbers[point - 1]
        return self.current_segment_number

    def setat(self, segment_number):
        """Set current segment number and return segment."""
        if segment_number not in self.segments:
            return None
        self.current_segment_number = segment_number
        return self.get_current_segment()

    def get_current_segment(self):
        """Return segment for current segment number in segment number order."""
        segment_number = self.current_segment_number
        segment_type = self.segments[segment_number][0]
        key = self._values_index[-1][len(SUBFILE_DELIMITER) :]
        if segment_type == BITMAP_BYTES:
            return RecordsetSegmentBitarray(
                segment_number,
                key,
                records=literal_eval(
                    self._dbenv[
                        str(segment_number).join(self._values_index)
                    ].decode()
                ),
            )
        if segment_type == LIST_BYTES:
            return RecordsetSegmentList(
                segment_number,
                key,
                records=literal_eval(
                    self._dbenv[
                        str(segment_number).join(self._values_index)
                    ].decode()
                ),
            )
        return RecordsetSegmentInt(
            segment_number,
            key,
            records=segment_type.to_bytes(2, byteorder="big"),
        )

    def count_records(self):
        """Return count of record references in segment table."""
        count = 0
        while True:
            k = self.next()
            if k is None:
                break
            count += self.count_current_segment_records()
        return count

    def count_current_segment_records(self):
        """Return count of record references in current segment."""
        return self.segments[self.current_segment_number][1]
        # segment_number = self.current_segment_number
        # r = self.segments[segment_number]
        # if r == LIST_BYTES:
        #    return RecordsetSegmentList(
        #        segment_number,
        #        None,
        #        records=literal_eval(
        #            self._dbenv[str(segment_number).join(self._values_index)
        #                        ].decode())).count_records()
        # elif r == BITMAP_BYTES:
        #    bitarray = Bitarray()
        #    bitarray.frombytes(literal_eval(
        #        self._dbenv[str(segment_number).join(self._values_index)
        #                    ].decode()))
        #    return bitarray.count()
        # else:
        #    return 1


# Defining and using this function implies RecordsetSegmentBitarray is not
# quite correct any more.
# Motivation is passing a Bitarray, not the bytes to create a Bitarray, as an
# argument to RecordsetSegmentBitarray() call.
def _empty_recordset_segment_bitarray():
    """Create and return an empty instance of RecordsetSegmentBitarray."""

    class _E(RecordsetSegmentBitarray):
        def __init__(self):
            """Do nothing."""

    k = _E()
    k.__class__ = RecordsetSegmentBitarray
    return k
