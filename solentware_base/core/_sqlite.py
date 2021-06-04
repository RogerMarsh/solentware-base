# _sqlite.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a SQLite3 database with either the apsw or sqlite3 modules.

When using sqlite3 the Python version must be 3.6 or later.

"""
import os
from ast import literal_eval
import re
import bisect

from . import filespec
from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_SUFFIX,
    CONTROL_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    SQLITE_VALUE_COLUMN,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_RECORDS_COLUMN,
    INDEXPREFIX,
)
from . import _database
from .bytebit import Bitarray
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
        self.specification = specification
        self.segment_size_bytes = segment_size_bytes
        self.dbenv = None
        self.table = {}
        self.index = {}
        self.segment_table = {}
        self.ebm_control = {}
        self.ebm_segment_count = {}

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
            cursor = self.dbenv.cursor()
            try:
                cursor.execute("begin")
            finally:
                cursor.close()

    def backout(self):
        """Backout tranaction."""
        if self.dbenv:
            cursor = self.dbenv.cursor()
            try:
                cursor.execute("rollback")
            finally:
                cursor.close()

    def commit(self):
        """Commit tranaction."""
        if self.dbenv:
            cursor = self.dbenv.cursor()
            try:
                cursor.execute("commit")
            finally:
                cursor.close()

    def open_database(self, dbe, files=None):
        """Open SQLite connection and specified tables and indicies.

        By default all tables are opened, but just those named in files
        otherwise, along with their indicies.

        dbe must be a Python module implementing the SQLite API.

        A connection object is created.  If dbe is sqlite3 it can be used only
        in the thread where it was created, but the restriction does not apply
        when dbe is apsw.

        """
        if self.home_directory is not None:
            try:
                os.mkdir(self.home_directory)
            except FileExistsError:
                if not os.path.isdir(self.home_directory):
                    raise

        # The ___control table should be present already if the file exists.
        if self.database_file is not None:
            dbenv = dbe.Connection(self.database_file)
            cursor = dbenv.cursor()
            statement = " ".join(
                (
                    "select",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    CONTROL_FILE,
                    "where",
                    CONTROL_FILE,
                    "== ?",
                )
            )
            try:
                cursor.execute(statement, (SPECIFICATION_KEY,))
                rsk = cursor.fetchall()
            except Exception:
                rsk = None
            try:
                cursor.execute(statement, (SEGMENT_SIZE_BYTES_KEY,))
                rssbk = cursor.fetchall()
            except Exception as exception:
                rssbk = None
            if rsk is not None and rssbk is not None:
                spec_from_db = literal_eval(rsk[0][0])
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
                segment_size = literal_eval(rssbk[0][0])
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
            elif rsk is None and rssbk is not None:
                raise DatabaseError("No specification recorded in database")
            elif rsk is not None and rssbk is None:
                raise DatabaseError("No segment size recorded in database")
        else:
            dbenv = dbe.Connection(":memory:")
            cursor = dbenv.cursor()

        self.set_segment_size()
        create_table = "create table if not exists"
        db_key = "integer primary key ,"
        db_create_index = "create unique index if not exists"
        self.dbenv = dbenv
        if files is None:
            files = self.specification.keys()
        self.start_transaction()
        cursor = self.dbenv.cursor()
        self.table[CONTROL_FILE] = CONTROL_FILE
        statement = " ".join(
            (
                create_table,
                CONTROL_FILE,
                "(",
                CONTROL_FILE,
                ",",
                SQLITE_VALUE_COLUMN,
                ",",
                "primary key",
                "(",
                CONTROL_FILE,
                ",",
                SQLITE_VALUE_COLUMN,
                ") )",
            )
        )
        cursor.execute(statement)
        for file, specification in self.specification.items():
            if file not in files:
                continue
            fields = specification[SECONDARY]
            self.table[file] = [file]
            statement = " ".join(
                (
                    create_table,
                    self.table[file][0],
                    "(",
                    file,
                    db_key,
                    SQLITE_VALUE_COLUMN,
                    ")",
                )
            )
            cursor.execute(statement)
            self.ebm_control[file] = ExistenceBitmapControl(file, self)
            segmentfile = SUBFILE_DELIMITER.join((file, SEGMENT_SUFFIX))
            self.segment_table[file] = segmentfile
            statement = " ".join(
                (
                    create_table,
                    segmentfile,
                    "(",
                    SQLITE_RECORDS_COLUMN,
                    ")",
                )
            )
            cursor.execute(statement)
            for field in fields:
                secondary = SUBFILE_DELIMITER.join((file, field))
                self.table[secondary] = [secondary]
                statement = " ".join(
                    (
                        create_table,
                        secondary,
                        "(",
                        field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        file,
                        ")",
                    )
                )
                cursor.execute(statement)
                indexname = "".join(
                    (INDEXPREFIX, SUBFILE_DELIMITER.join((file, field)))
                )
                self.index[secondary] = [indexname]
                statement = " ".join(
                    (
                        db_create_index,
                        indexname,
                        "on",
                        secondary,
                        "(",
                        field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ")",
                    )
                )
                cursor.execute(statement)
        if self.database_file is not None:
            if rsk is None and rssbk is None:
                statement = " ".join(
                    (
                        "insert into",
                        CONTROL_FILE,
                        "(",
                        CONTROL_FILE,
                        ",",
                        SQLITE_VALUE_COLUMN,
                        ")",
                        "values ( ? , ? )",
                    )
                )
                cursor.execute(
                    statement, (SPECIFICATION_KEY, repr(self.specification))
                )
                cursor.execute(
                    statement,
                    (SEGMENT_SIZE_BYTES_KEY, repr(self.segment_size_bytes)),
                )
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
        self.segment_table = {}
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
        # _database.Database.put_instance() decides if a deleted record number
        # is reused before calling the put() method of a subclass.
        # So _sqlite.Database.put() does what it is told to do.  Deleted records
        # have to exist as stubs to be indexed as available for reuse: hence
        # 'insert or replace' rather than 'insert'.  The replace option allows
        # possibility of overwriting existing records by ignoring put_instance.
        assert file in self.specification
        cursor = self.dbenv.cursor()
        try:
            if key is None:
                statement = " ".join(
                    (
                        "insert into",
                        self.table[file][0],
                        "(",
                        SQLITE_VALUE_COLUMN,
                        ")",
                        "values ( ? )",
                    )
                )
                cursor.execute(statement, (value,))
                return cursor.execute(
                    " ".join(("select last_insert_rowid() from", file))
                ).fetchone()[0]

            # The original 'update' version is probably correct!
            # Especially if it succeeds only if SQLITE_VALUE_COLUMN has been
            # set to 'null' by a previous delete (to indicate which rowids
            # may be re-used).  The original's where clause is wrong and
            # should check 'SQLITE_VALUE_COLUMN = null' too.
            statement = " ".join(
                (
                    "insert or replace into",
                    self.table[file][0],
                    "(",
                    SQLITE_VALUE_COLUMN,
                    ",",
                    file,
                    ")",
                    "values ( ? , ? )",
                )
            )
            # statement = ' '.join((
            #    'update',
            #    self.table[file][0],
            #    'set',
            #    SQLITE_VALUE_COLUMN, '= ?',
            #    'where',
            #    file, '== ?',
            #    ))
            cursor.execute(statement, (value, key))
            return None
        finally:
            cursor.close()

    def replace(self, file, key, oldvalue, newvalue):
        """Replace key from table for file using newvalue.

        oldvalue is ignored in _sqlite version of replace() method.
        """
        assert file in self.specification
        cursor = self.dbenv.cursor()
        try:
            statement = " ".join(
                (
                    "update",
                    self.table[file][0],
                    "set",
                    SQLITE_VALUE_COLUMN,
                    "= ?",
                    "where",
                    file,
                    "== ?",
                )
            )
            cursor.execute(statement, (newvalue, key))
        finally:
            cursor.close()

    def delete(self, file, key, value):
        """Delete key from table for file.

        value is ignored in _sqlite version of delete() method.
        """
        assert file in self.specification
        cursor = self.dbenv.cursor()
        try:

            # The update version is original and may be correct if comment in
            # put has correct assessment of situation.  The original's where
            # clause is wrong and should be same as new version.
            statement = " ".join(
                (
                    "delete from",
                    self.table[file][0],
                    "where",
                    file,
                    "== ?",
                )
            )
            # statement = ' '.join((
            #    'update',
            #    self.table[file][0],
            #    'set',
            #    SQLITE_VALUE_COLUMN, '= null',
            #    'where',
            #    file, '== ?',
            #    ))
            cursor.execute(statement, (key,))
        finally:
            cursor.close()

    def get_primary_record(self, file, key):
        """Return the instance given the record number in key."""
        assert file in self.specification
        if key is None:
            return None
        statement = " ".join(
            (
                "select * from",
                self.table[file][0],
                "where",
                file,
                "== ?",
                # Assume there is a maximum of one record (unlike original query).
                #'order by',
                # file,
                #'limit 1',
            )
        )
        cursor = self.dbenv.cursor()
        try:
            return cursor.execute(statement, (key,)).fetchone()
        finally:
            cursor.close()

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
            ebmc.freed_record_number_pages = []
            statement = " ".join(
                (
                    "select",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    self.table[CONTROL_FILE],
                    "where",
                    self.table[CONTROL_FILE],
                    "== ?",
                    "order by",
                    SQLITE_VALUE_COLUMN,
                )
            )
            values = (ebmc.ebmkey,)
            cursor = self.dbenv.cursor()
            try:
                for record in cursor.execute(statement, values):
                    ebmc.freed_record_number_pages.append(record[0])
            finally:
                cursor.close()
        while len(ebmc.freed_record_number_pages):
            segment_number = ebmc.freed_record_number_pages[0]

            # Do not reuse record number on segment of high record number.
            if segment_number == ebmc.segment_count - 1:
                return None

            lfrns = ebmc.read_exists_segment(segment_number, self.dbenv)
            if lfrns is None:

                # Segment does not exist now.
                ebmc.freed_record_number_pages.remove(segment_number)
                continue

            try:
                first_zero_bit = lfrns.index(False, 0 if segment_number else 1)
            except ValueError:

                # No longer any record numbers available for re-use in segment.
                statement = " ".join(
                    (
                        "delete from",
                        self.table[CONTROL_FILE],
                        "where",
                        self.table[CONTROL_FILE],
                        "== ? and",
                        SQLITE_VALUE_COLUMN,
                        "== ?",
                    )
                )
                values = (ebmc.ebmkey, segment_number)
                cursor = self.dbenv.cursor()
                try:
                    cursor.execute(statement, values)
                finally:
                    cursor.close()
                del ebmc.freed_record_number_pages[0]
                continue
            return (
                segment_number * SegmentSize.db_segment_size + first_zero_bit
            )
        return None

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
            ebmc.freed_record_number_pages = []
            statement = " ".join(
                (
                    "select",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    self.table[CONTROL_FILE],
                    "where",
                    self.table[CONTROL_FILE],
                    "== ?",
                    "order by",
                    SQLITE_VALUE_COLUMN,
                )
            )
            values = (ebmc.ebmkey,)
            cursor = self.dbenv.cursor()
            try:
                for record in cursor.execute(statement, values):
                    ebmc.freed_record_number_pages.append(record[0])
            finally:
                cursor.close()
        insert = bisect.bisect_left(ebmc.freed_record_number_pages, segment)
        if ebmc.freed_record_number_pages:
            if insert < len(ebmc.freed_record_number_pages):
                if ebmc.freed_record_number_pages[insert] == segment:
                    return
        ebmc.freed_record_number_pages.insert(insert, segment)
        statement = " ".join(
            (
                "insert into",
                self.table[CONTROL_FILE],
                "(",
                self.table[CONTROL_FILE],
                ",",
                SQLITE_VALUE_COLUMN,
                ")",
                "values ( ? , ? )",
            )
        )
        values = (ebmc.ebmkey, segment)
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def remove_record_from_ebm(self, file, deletekey):
        """Remove deletekey from file's existence bitmap; return key.

        deletekey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        ebmb = self.ebm_control[file].get_ebm_segment(segment + 1, self.dbenv)
        if ebmb is None:
            raise DatabaseError("Existence bit map for segment does not exist")
        ebm = Bitarray()
        ebm.frombytes(ebmb)
        ebm[record_number] = False
        self.ebm_control[file].put_ebm_segment(
            segment + 1, ebm.tobytes(), self.dbenv
        )
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        """Add putkey to file's existence bitmap; return (segment, record).

        putkey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        ebmb = self.ebm_control[file].get_ebm_segment(segment + 1, self.dbenv)
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
                segment + 1, ebm.tobytes(), self.dbenv
            )
        return segment, record_number

    # Change to return just the record number, and the name to fit.
    # Only used in one place, and it is extra work to get the data in _nosql.
    def get_high_record(self, file):
        """Return the high existing rowid in table for file."""
        statement = " ".join(
            (
                "select",
                file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self.table[file][0],
                "order by",
                file,
                "desc",
                "limit 1",
            )
        )
        values = ()
        cursor = self.dbenv.cursor()
        try:
            return cursor.execute(statement, values).fetchone()
        finally:
            cursor.close()

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
        secondary = self.table[SUBFILE_DELIMITER.join((file, field))][0]
        select_existing_segment = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                secondary,
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        update_record_count = " ".join(
            (
                "update",
                secondary,
                "set",
                SQLITE_COUNT_COLUMN,
                "= ?",
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        update_count_and_reference = " ".join(
            (
                "update",
                secondary,
                "set",
                SQLITE_COUNT_COLUMN,
                "= ? ,",
                file,
                "= ?",
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        insert_new_segment = " ".join(
            (
                "insert into",
                secondary,
                "(",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            segment_reference = cursor.execute(
                select_existing_segment, (key, segment)
            ).fetchone()
        finally:
            cursor.close()
        if segment_reference is None:
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(
                    insert_new_segment, (key, segment, 1, record_number)
                )
            finally:
                cursor.close()
            return
        existing_segment = self.populate_segment(segment_reference, file)
        seg = (
            RecordsetSegmentInt(
                segment,
                None,
                records=record_number.to_bytes(2, byteorder="big"),
            )
            | existing_segment
        )
        count = seg.count_records()
        if count == existing_segment.count_records():
            return
        if not isinstance(existing_segment, RecordsetSegmentBitarray):
            seg = seg.normalize()
        if segment_reference[2] > 1:
            self.set_segment_records(
                (seg.tobytes(), segment_reference[3]), file
            )
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(
                    update_record_count,
                    (segment_reference[2] + 1, key, segment),
                )
            finally:
                cursor.close()
        else:
            rowid = self.insert_segment_records((seg.tobytes(),), file)
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(
                    update_count_and_reference,
                    (
                        segment_reference[2] + 1,
                        rowid,
                        key,
                        segment_reference[1],
                    ),
                )
            finally:
                cursor.close()

    def remove_record_from_field_value(
        self, file, field, key, segment, record_number
    ):
        """Remove record_number from set of records in segment for key.

        key is a value of index field on segment table for file.

        The representation of the set of records on the database is
        converted from bitmap to list to integer if the removal reduces
        the number of records in the set below the relevant limit.
        """
        secondary = self.table[SUBFILE_DELIMITER.join((file, field))][0]
        select_existing_segment = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                secondary,
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        update_record_count = " ".join(
            (
                "update",
                secondary,
                "set",
                SQLITE_COUNT_COLUMN,
                "= ?",
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        update_count_and_reference = " ".join(
            (
                "update",
                secondary,
                "set",
                SQLITE_COUNT_COLUMN,
                "= ? ,",
                file,
                "= ?",
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        delete_existing_segment = " ".join(
            (
                "delete from",
                secondary,
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            segment_reference = cursor.execute(
                select_existing_segment, (key, segment)
            ).fetchone()
        finally:
            cursor.close()
        if segment_reference is None:
            return
        seg = RecordsetSegmentInt(
            segment, None, records=record_number.to_bytes(2, byteorder="big")
        )
        existing_segment = self.populate_segment(segment_reference, file)
        seg = (seg & existing_segment) ^ existing_segment
        count = seg.count_records()
        if count == existing_segment.count_records():
            return
        if not isinstance(existing_segment, RecordsetSegmentBitarray):
            seg = seg.normalize()
        else:
            seg = seg.normalize(use_upper_limit=False)
        if count > 1:
            self.set_segment_records(
                (seg.tobytes(), segment_reference[3]), file
            )
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(update_record_count, (count, key, segment))
            finally:
                cursor.close()
            return
        if count == 1:
            self.delete_segment_records((segment_reference[3],), file)
            record_number = seg.get_record_number_at_position(0)
            cursor = self.dbenv.cursor()
            try:
                cursor.execute(
                    update_count_and_reference,
                    (
                        count,
                        record_number % (segment * SegmentSize.db_segment_size)
                        if segment
                        else record_number,
                        key,
                        segment,
                    ),
                )
            finally:
                cursor.close()
            return
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(delete_existing_segment, (key, segment))
        finally:
            cursor.close()
        return

    def populate_segment(self, segment_reference, file):
        """Return records for segment_reference in segment table for file.

        A RecordsetSegmentBitarray, RecordsetSegmentList, or
        RecordsetSegmentInt, instance is returned.

        segment_reference has a rowid, or the record number if there is
        exactly one record in the segment.

        get_segment_records is called to get the segment record if there
        is more than one record in the segment.
        """
        if segment_reference[2] == 1:
            return RecordsetSegmentInt(
                segment_reference[1],
                None,
                records=segment_reference[3].to_bytes(2, byteorder="big"),
            )
        segment_record = self.get_segment_records(segment_reference[3], file)
        if len(segment_record) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                segment_reference[1], None, records=segment_record
            )
        return RecordsetSegmentList(
            segment_reference[1], None, records=segment_record
        )

    def get_segment_records(self, rownumber, file):
        """Return segment for rownumber in segment table for file.

        The returned object has not been converted to the list, bitarray,
        or integer, representing a set of records.
        """
        statement = " ".join(
            (
                "select",
                SQLITE_RECORDS_COLUMN,
                "from",
                self.segment_table[file],
                "where rowid == ?",
            )
        )
        values = (rownumber,)
        cursor = self.dbenv.cursor()
        try:
            return cursor.execute(statement, values).fetchone()[0]
        except TypeError:
            raise DatabaseError(
                "".join(
                    (
                        "Segment record ",
                        str(rownumber),
                        " missing in '",
                        file,
                        "'",
                    )
                )
            )
        finally:
            cursor.close()

    def set_segment_records(self, values, file):
        """Update a segment in segment table for file.

        values is a tuple(segment, rowid)

        segment is a list of integers, a bitmap representing a set of
        integers, or an integer; which represent a set of records in
        the segment.

        rowid is the row in segment table for file to be updated.
        """
        statement = " ".join(
            (
                "update",
                self.segment_table[file],
                "set",
                SQLITE_RECORDS_COLUMN,
                "= ?",
                "where rowid == ?",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def delete_segment_records(self, values, file):
        """Delete a segment from segment table for file.

        values is the rowid for the segment to be deleted.
        """
        statement = " ".join(
            (
                "delete from",
                self.segment_table[file],
                "where rowid == ?",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def insert_segment_records(self, values, file):
        """Insert a segment into segment table for file and return rowid.

        values is a list of integers, a bitmap representing a set of
        integers, or an integer; which represent a set of records in
        the segment.
        """
        statement = " ".join(
            (
                "insert into",
                self.segment_table[file],
                "(",
                SQLITE_RECORDS_COLUMN,
                ")",
                "values ( ? )",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            cursor.execute(statement, values)
            return cursor.execute(
                " ".join(
                    (
                        "select last_insert_rowid() from",
                        self.segment_table[file],
                    )
                )
            ).fetchone()[0]
        finally:
            cursor.close()

    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        field = valuespec.field
        if valuespec.above_value and valuespec.below_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    "> ? and",
                    field,
                    "< ?",
                )
            )
            values = valuespec.above_value, valuespec.below_value
        elif valuespec.above_value and valuespec.to_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    "> ? and",
                    field,
                    "<= ?",
                )
            )
            values = valuespec.above_value, valuespec.to_value
        elif valuespec.from_value and valuespec.to_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    ">= ? and",
                    field,
                    "<= ?",
                )
            )
            values = valuespec.from_value, valuespec.to_value
        elif valuespec.from_value and valuespec.below_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    ">= ? and",
                    field,
                    "< ?",
                )
            )
            values = valuespec.from_value, valuespec.below_value
        elif valuespec.above_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    "> ?",
                )
            )
            values = (valuespec.above_value,)
        elif valuespec.from_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    ">= ?",
                )
            )
            values = (valuespec.from_value,)
        elif valuespec.to_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    "<= ?",
                )
            )
            values = (valuespec.to_value,)
        elif valuespec.below_value:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    "< ?",
                )
            )
            values = (valuespec.below_value,)
        else:
            statement = " ".join(
                (
                    "select distinct",
                    field,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                )
            )
            values = ()
        apply_to_value = valuespec.apply_pattern_and_set_filters_to_value
        cursor = self.dbenv.cursor()
        try:
            for row in cursor.execute(statement, values):
                if apply_to_value(row[0]):
                    yield row[0]
        finally:
            cursor.close()

    # The bit setting in existence bit map decides if a record is put on the
    # recordset created by the make_recordset_*() methods.

    # Look at ebm_control.ebm_table even though the additional 'rn in row'
    # clause when populating the recordset makes table access cheaper.
    def recordlist_record_number(self, file, key=None, cache_size=1):
        """Return RecordList on file containing records for key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return recordlist
        statement = " ".join(
            (
                "select",
                self.ebm_control[file].ebm_table,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self.ebm_control[file].ebm_table,
                "where",
                self.ebm_control[file].ebm_table,
                "= ?",
            )
        )
        segment_number, record_number = divmod(
            key, SegmentSize.db_segment_size
        )
        values = (segment_number + 1,)
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record_number in RecordsetSegmentBitarray(
                    segment_number, key, records=record[1]
                ):
                    recordlist[segment_number] = RecordsetSegmentList(
                        segment_number,
                        None,
                        records=record_number.to_bytes(2, byteorder="big"),
                    )
        finally:
            cursor.close()
        return recordlist

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1
    ):
        """Return RecordList of records on file in a record number range.

        The records have record number between keystart and keyend.  Both
        default to include all records to the respective edge of segment.
        """
        # The keys in self.ebm_control.ebm_table are always 'segment + 1',
        # see note in recordlist_ebm method.
        if keystart is None and keyend is None:
            return self.recordlist_ebm(file, cache_size=cache_size)
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(
                keystart, SegmentSize.db_segment_size
            )
        if keyend is not None:
            segment_end, recnum_end = divmod(
                keyend, SegmentSize.db_segment_size
            )
        if keyend is None:
            statement = " ".join(
                (
                    "select",
                    self.ebm_control[file].ebm_table,
                    ",",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    self.ebm_control[file].ebm_table,
                    "where",
                    self.ebm_control[file].ebm_table,
                    ">= ?",
                )
            )
            values = (segment_start + 1,)
        elif keystart is None:
            statement = " ".join(
                (
                    "select",
                    self.ebm_control[file].ebm_table,
                    ",",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    self.ebm_control[file].ebm_table,
                    "where",
                    self.ebm_control[file].ebm_table,
                    "<= ?",
                )
            )
            values = (segment_end + 1,)
        else:
            statement = " ".join(
                (
                    "select",
                    self.ebm_control[file].ebm_table,
                    ",",
                    SQLITE_VALUE_COLUMN,
                    "from",
                    self.ebm_control[file].ebm_table,
                    "where",
                    self.ebm_control[file].ebm_table,
                    ">= ? and",
                    self.ebm_control[file].ebm_table,
                    "<= ?",
                )
            )
            values = (segment_start + 1, segment_end + 1)
        cursor = self.dbenv.cursor()
        try:
            first_segment = None
            final_segment = None
            for row in cursor.execute(statement, values):
                segment_number, segment_record = row
                segment_number -= 1
                if segment_number == segment_start:
                    if (segment_number and recnum_start) or recnum_start > 1:
                        first_segment, start_byte = divmod(recnum_start, 8)
                        segment_record = (
                            b"\x00" * first_segment
                            + segment_record[first_segment:]
                        )
                if keyend is not None:
                    if (
                        segment_number == segment_end
                        and recnum_start < SegmentSize.db_segment_size - 1
                    ):
                        final_segment, end_byte = divmod(recnum_end, 8)
                        segment_record = segment_record[
                            : final_segment + 1
                        ] + b"\x00" * (
                            SegmentSize.db_segment_size_bytes
                            - final_segment
                            - 1
                        )
                recordlist[segment_number] = RecordsetSegmentBitarray(
                    segment_number, None, records=segment_record
                )
            if first_segment is not None:
                for i in range(
                    first_segment * 8, first_segment * 8 + start_byte
                ):
                    recordlist[segment_start][(segment_start, i)] = False
            if final_segment is not None:
                for i in range(
                    final_segment * 8 + end_byte, (final_segment + 1) * 8
                ):
                    recordlist[segment_end][(segment_end, i)] = False
        finally:
            cursor.close()
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList containing records on file."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        statement = " ".join(
            (
                "select",
                self.ebm_control[file].ebm_table,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self.ebm_control[file].ebm_table,
            )
        )
        values = ()
        cursor = self.dbenv.cursor()
        try:
            for row in cursor.execute(statement, values):

                # The keys in self.ebm_control[file].ebm_table are always
                # 'segment + 1' because automatically allocated
                # 'integer primary key's start at 1 in an empty table and the
                # first segment is segment 0.
                # Maybe this should change to use the actual segment number.
                recordlist[row[0] - 1] = RecordsetSegmentBitarray(
                    row[0] - 1, None, records=row[1]
                )

        finally:
            cursor.close()
        return recordlist

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return RecordList containing records for field on file.

        The records are indexed by keys containing keylike.
        """
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keylike is None:
            return recordlist
        statement = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                self.table[SUBFILE_DELIMITER.join((file, field))][0],
            )
        )
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        matcher = re.compile(".*?" + keylike, flags=re.IGNORECASE | re.DOTALL)
        get_segment_records = self.get_segment_records
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement):
                if not matcher.match(record[0]):
                    continue
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = get_segment_records(record[3], file)
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=segment_record
                        )
                if record[1] not in recordlist:
                    recordlist[record[1]] = segment  # .promote()
                else:
                    recordlist[record[1]] |= segment
        finally:
            cursor.close()
        return recordlist

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        statement = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                self.table[SUBFILE_DELIMITER.join((file, field))][0],
                "where",
                field,
                "== ?",
            )
        )
        values = (key,)
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_segment_records
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    recordlist[record[1]] = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = get_segment_records(record[3], file)
                    if len(segment_record) == db_segment_size_bytes:
                        recordlist[record[1]] = RecordsetSegmentBitarray(
                            record[1], None, records=segment_record
                        )
                    else:
                        recordlist[record[1]] = RecordsetSegmentList(
                            record[1], None, records=segment_record
                        )
        finally:
            cursor.close()
        return recordlist

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        The records are indexed by keys starting keystart.
        """
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            return recordlist
        statement = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                self.table[SUBFILE_DELIMITER.join((file, field))][0],
                "where",
                field,
                "glob ?",
            )
        )
        values = (
            b"".join(
                (
                    keystart.encode()
                    if isinstance(keystart, str)
                    else keystart,
                    b"*",
                )
            ),
        )
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_segment_records
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = get_segment_records(record[3], file)
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=segment_record
                        )
                if record[1] not in recordlist:
                    recordlist[record[1]] = segment  # .promote()
                else:
                    recordlist[record[1]] |= segment
        finally:
            cursor.close()
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        Keys are in range set by combinations of ge, gt, le, and lt.
        """
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        if le and lt:
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        if ge is None and gt is None and le is None and lt is None:
            return self.recordlist_all(file, field, cache_size=cache_size)
        highop = "<" if lt else "<=" if le else None
        lowop = ">" if gt else ">=" if ge else None
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if highop is None:
            statement = " ".join(
                (
                    "select",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    lowop,
                    "?",
                )
            )
            values = (ge or gt,)
        elif lowop is None:
            statement = " ".join(
                (
                    "select",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    highop,
                    "?",
                )
            )
            values = (le or lt,)
        else:
            statement = " ".join(
                (
                    "select",
                    field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    file,
                    "from",
                    self.table[SUBFILE_DELIMITER.join((file, field))][0],
                    "where",
                    field,
                    lowop,
                    "? and",
                    field,
                    highop,
                    "?",
                )
            )
            values = ge or gt, le or lt
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_segment_records
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = get_segment_records(record[3], file)
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=segment_record
                        )
                if record[1] not in recordlist:
                    recordlist[record[1]] = segment  # .promote()
                else:
                    recordlist[record[1]] |= segment
        finally:
            cursor.close()
        return recordlist

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        statement = " ".join(
            (
                "select",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                self.table[SUBFILE_DELIMITER.join((file, field))][0],
            )
        )
        values = ()
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        get_segment_records = self.get_segment_records
        cursor = self.dbenv.cursor()
        try:
            for record in cursor.execute(statement, values):
                if record[2] == 1:
                    segment = RecordsetSegmentInt(
                        record[1],
                        None,
                        records=record[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = get_segment_records(record[3], file)
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            record[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            record[1], None, records=segment_record
                        )
                if record[1] not in recordlist:
                    recordlist[record[1]] = segment  # .promote()
                else:
                    recordlist[record[1]] |= segment
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
        secondary = SUBFILE_DELIMITER.join((file, field))
        select_existing_segments = " ".join(
            (
                "select",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                "from",
                self.table[secondary][0],
                "indexed by",
                self.index[secondary][0],
                "where",
                field,
                "== ?",
            )
        )
        delete_existing_segment = " ".join(
            (
                "delete from",
                self.table[secondary][0],
                "where",
                field,
                "== ? and",
                SQLITE_SEGMENT_COLUMN,
                "== ?",
            )
        )
        cursor = self.dbenv.cursor()
        try:
            rows = cursor.execute(select_existing_segments, (key,)).fetchall()
        finally:
            cursor.close()
        old_rows = [s[1] for s in sorted((r[1], r) for r in rows if r[1] > 1)]
        rows = {r[0] for r in rows}
        cursor = self.dbenv.cursor()
        try:
            for reuse in rows:
                cursor.execute(delete_existing_segment, (key, reuse))
        finally:
            cursor.close()
        for segment_key in old_rows:
            self.delete_segment_records((segment_key[2],), file)

    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records."""
        assert recordset.dbset == file
        assert file == self.table[file][0]
        secondary = SUBFILE_DELIMITER.join((file, field))

        # insert or replace (index value, segment number, record count,
        # key reference) statement used when a new segment is created or an
        # existing one replaced.  Key reference is the segment reference if
        # more than one record is in the segment for the index value, or the
        # record key when one record is referenced.
        insert_new_segment = " ".join(
            (
                "insert or replace into",
                self.table[secondary][0],
                "(",
                field,
                ",",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                file,
                ")",
                "values ( ? , ? , ? , ? )",
            )
        )

        # Delete existing segments for key
        self.unfile_records_under(file, field, key)

        recordset.normalize()

        # Process the segments in segment number order.
        cursor = self.dbenv.cursor()
        try:
            for segment_number in recordset.sorted_segnums:
                rs_segment = recordset.rs_segments[segment_number]
                if isinstance(rs_segment, RecordsetSegmentBitarray):
                    segment_key = self.insert_segment_records(
                        (rs_segment.bitarray.tobytes(),), file
                    )
                    cursor.execute(
                        insert_new_segment,
                        (
                            key,
                            segment_number,
                            rs_segment.count_records(),
                            segment_key,
                        ),
                    )
                elif isinstance(rs_segment, RecordsetSegmentList):
                    rnlist = b"".join(
                        [
                            rn.to_bytes(2, byteorder="big")
                            for rn in rs_segment.list
                        ]
                    )
                    segment_key = self.insert_segment_records((rnlist,), file)
                    cursor.execute(
                        insert_new_segment,
                        (
                            key,
                            segment_number,
                            rs_segment.count_records(),
                            segment_key,
                        ),
                    )
                elif isinstance(rs_segment, RecordsetSegmentInt):
                    cursor.execute(
                        insert_new_segment,
                        (
                            key,
                            segment_number,
                            rs_segment.count_records(),
                            int.from_bytes(rs_segment.tobytes(), "big"),
                        ),
                    )
        finally:
            cursor.close()

    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on SQLite Connection() for (file, field).

        keyrange is an addition for DPT. It may yet be removed.

        """
        assert file in self.specification
        if file == field:
            return CursorPrimary(
                self.dbenv,
                table=self.table[file][0],
                ebm=self.ebm_control[file].ebm_table,
                file=file,
                keyrange=keyrange,
            )
        return CursorSecondary(
            self.dbenv,
            table=self.table[SUBFILE_DELIMITER.join((file, field))][0],
            file=file,
            field=field,
            keyrange=keyrange,
            segment=self.segment_table[file],
        )

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
        """Return SQLite database connection.  The file argument is ignored.

        The file argument is present for compatibility with versions of this
        method defined in sibling modules.

        The connection's database is assumed to contain a table for file, and
        appropriate file names, and so forth, will be used in SQL statements
        passed to the execute method of cursors created on the returned
        connection object.

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


class Cursor(_cursor.Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - apsw or sqlite3 Connection() object.
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

    def __init__(self, dbset, table=None, file=None, keyrange=None, **kargs):
        """Define a cursor on the underlying database engine dbset."""
        super().__init__(dbset)
        self._cursor = dbset.cursor()
        self._table = table
        self._file = file
        self._current_segment = None
        self.current_segment_number = None
        self._current_record_number_in_segment = None

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError("get_partial_with_wildcard not implemented")

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return "".join((self._partial, "*"))

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
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

    def __init__(self, dbset, ebm=None, **kargs):
        """Extend, note existence bitmap table name, initialize row read."""
        super().__init__(dbset, **kargs)
        self._most_recent_row_read = False
        self._ebm = ebm

    def close(self):
        """Delete database cursor then extend."""
        self._most_recent_row_read = False
        super().close()

    def count_records(self):
        """Return record count or None if cursor is not usable."""
        # Quicker than executing 'select count ( * ) ...' for many records.
        statement = " ".join(
            (
                "select",
                SQLITE_VALUE_COLUMN,
                "from",
                self._ebm,
            )
        )
        return sum(
            RecordsetSegmentBitarray(0, None, records=r[0]).count_records()
            for r in self._cursor.execute(statement)
        )

    def first(self):
        """Return first record taking partial key into account."""
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "order by",
                self._file,
                "limit 1",
            )
        )
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0

        # Quicker than executing 'select count ( * ) ...' for many records.
        statement = " ".join(
            (
                "select",
                "rowid",
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._ebm,
                "order by rowid",
            )
        )
        dss = SegmentSize.db_segment_size
        position = 0
        rowid = record[0]
        for row in self._cursor.execute(statement):
            segment = RecordsetSegmentBitarray(
                row[0] - 1, None, records=row[1]
            )
            if row[0] * dss <= rowid:
                position += segment.count_records()
                continue
            position += segment.get_position_of_record_number(rowid % dss)
            break
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        if position < 0:
            statement = " ".join(
                (
                    "select * from",
                    self._table,
                    "order by",
                    self._file,
                    "desc",
                    "limit 1",
                    "offset ?",
                )
            )
            values = (str(-1 - position),)
        else:
            statement = " ".join(
                (
                    "select * from",
                    self._table,
                    "order by",
                    self._file,
                    "limit 1",
                    "offset ?",
                )
            )
            values = (str(position - 1),)
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def last(self):
        """Return last record taking partial key into account."""
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "order by",
                self._file,
                "desc",
                "limit 1",
            )
        )
        values = ()
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "where",
                self._file,
                ">= ?",
                "order by",
                self._file,
                "limit 1",
            )
        )
        values = (key,)
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def next(self):
        """Return next record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.first()
        if self._most_recent_row_read is None:
            return None
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "where",
                self._file,
                "> ?",
                "order by",
                self._file,
                "limit 1",
            )
        )
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._most_recent_row_read is False:
            return self.last()
        if self._most_recent_row_read is None:
            return None
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "where",
                self._file,
                "< ?",
                "order by",
                self._file,
                "desc",
                "limit 1",
            )
        )
        values = (self._most_recent_row_read[0],)
        self._most_recent_row_read = self._cursor.execute(
            statement, values
        ).fetchone()
        return self._most_recent_row_read

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.

        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        statement = " ".join(
            (
                "select",
                self._file,
                ",",
                SQLITE_VALUE_COLUMN,
                "from",
                self._table,
                "where",
                self._file,
                "== ?",
                "order by",
                self._file,
                "limit 1",
            )
        )
        values = (record[0],)
        row = self._cursor.execute(statement, values).fetchone()
        if row:
            self._most_recent_row_read = row
        return row

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # raise DatabaseError('refresh_recordset not implemented')


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

    def __init__(self, dbset, field=None, segment=None, **kargs):
        """Extend, note field and segment table names."""
        super().__init__(dbset, **kargs)
        self._field = field
        self._segment = segment

    @property
    def rowids_in_primary(self):
        """Return name of table definition in application's FileSpec."""
        return self._file

    def get_segment_records(self, rownumber):
        """Return the segment record for rownumber."""
        statement = " ".join(
            (
                "select",
                SQLITE_RECORDS_COLUMN,
                "from",
                self._segment,
                "where rowid == ?",
            )
        )
        values = (rownumber,)
        cursor = self._dbset.cursor()
        try:
            return cursor.execute(statement, values).fetchone()[0]
        except TypeError:
            raise DatabaseError(
                "".join(
                    (
                        "Segment record ",
                        str(rownumber),
                        " missing in '",
                        self._segment,
                        "'",
                    )
                )
            )
        finally:
            cursor.close()

    def count_records(self):
        """Return record count."""
        if self.get_partial() in (None, False):
            statement = " ".join(
                (
                    "select",
                    SQLITE_COUNT_COLUMN,
                    "from",
                    self._table,
                )
            )
            values = ()
        else:
            statement = " ".join(
                (
                    "select",
                    SQLITE_COUNT_COLUMN,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ?",
                )
            )
            values = (self.get_converted_partial_with_wildcard(),)
        count = 0
        for row in self._cursor.execute(statement, values):
            count += row[0]
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (self.get_converted_partial_with_wildcard(),)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        return self.set_current_segment(row).first()

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(
            value, SegmentSize.db_segment_size
        )

        # Get position of record relative to start point
        if not self.get_partial():
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                )
            )
            values = ()
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                )
            )
            values = (self.get_converted_partial_with_wildcard(),)
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        position = 0
        for row in self._cursor.execute(statement, values):
            if row[0] < key:
                position += row[2]
            elif row[0] > key:
                break
            elif row[1] < segment_number:
                position += row[2]
            elif row[1] > segment_number:
                break
            else:
                if row[2] == 1:
                    segment = RecordsetSegmentInt(
                        segment_number,
                        None,
                        records=row[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = self.get_segment_records(row[3])
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            segment_number, None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            segment_number, None, records=segment_record
                        )
                position += segment.get_position_of_record_number(
                    record_number
                )
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None

        # Start at first or last record whichever is likely closer to position
        if position < 0:
            if not self.get_partial():
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "order by",
                        self._field,
                        "desc",
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "desc",
                    )
                )
                values = ()
            else:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "glob ?",
                        "order by",
                        self._field,
                        "desc",
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "desc",
                    )
                )
                values = (self.get_converted_partial_with_wildcard(),)
        else:
            if not self.get_partial():
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "order by",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                    )
                )
                values = ()
            else:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "glob ?",
                        "order by",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                    )
                )
                values = (self.get_converted_partial_with_wildcard(),)

        # Get record at position relative to start point
        db_segment_size_bytes = SegmentSize.db_segment_size_bytes
        count = 0
        if position < 0:
            for row in self._cursor.execute(statement, values):
                count -= row[2]
                if count > position:
                    continue
                if row[2] == 1:
                    segment = RecordsetSegmentInt(
                        row[1],
                        None,
                        records=row[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = self.get_segment_records(row[3])
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            row[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            row[1], None, records=segment_record
                        )
                record_number = segment.get_record_number_at_position(
                    position - count - row[2]
                )
                if record_number is not None:
                    return row[0], record_number
                break
        else:
            for row in self._cursor.execute(statement, values):
                count += row[2]
                if count <= position:
                    continue
                if row[2] == 1:
                    segment = RecordsetSegmentInt(
                        row[1],
                        None,
                        records=row[3].to_bytes(2, byteorder="big"),
                    )
                else:
                    segment_record = self.get_segment_records(row[3])
                    if len(segment_record) == db_segment_size_bytes:
                        segment = RecordsetSegmentBitarray(
                            row[1], None, records=segment_record
                        )
                    else:
                        segment = RecordsetSegmentList(
                            row[1], None, records=segment_record
                        )
                record_number = segment.get_record_number_at_position(
                    position - count + row[2]
                )
                if record_number is not None:
                    return row[0], record_number
                break
        return None

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "order by",
                    self._field,
                    "desc",
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "desc",
                    "limit 1",
                )
            )
            values = ()
        elif self.get_partial() is False:
            return None
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ?",
                    "order by",
                    self._field,
                    "desc",
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "desc",
                    "limit 1",
                )
            )
            values = (self.get_converted_partial_with_wildcard(),)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        return self.set_current_segment(row).last()

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if self.get_partial() is None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    ">= ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (key,)
        elif self.get_partial() is False:
            return None
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ? and",
                    self._field,
                    ">= ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (self.get_converted_partial_with_wildcard(), key)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        return self.set_current_segment(row).first()

    def next(self):
        """Return next record taking partial key into account."""
        if self._current_segment is None:
            return self.first()
        if self.get_partial() is False:
            return None
        record = self._current_segment.next()
        if record is not None:
            return record
        if self.get_partial() is None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "> ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (
                self._current_segment.index_key,
                self.current_segment_number,
            )
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "> ?",
                        "order by",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "limit 1",
                    )
                )
                values = (self._current_segment.index_key,)
                row = self._cursor.execute(statement, values).fetchone()
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ? and",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "> ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (
                self.get_converted_partial_with_wildcard(),
                self._current_segment.index_key,
                self.current_segment_number,
            )
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "glob ? and",
                        self._field,
                        "> ?",
                        "order by",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "limit 1",
                    )
                )
                values = (
                    self.get_converted_partial_with_wildcard(),
                    self._current_segment.index_key,
                )
                row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        return self.set_current_segment(row).first()

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._current_segment is None:
            return self.last()
        if self.get_partial() is False:
            return None
        record = self._current_segment.prev()
        if record is not None:
            return record
        if self.get_partial() is None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "< ?",
                    "order by",
                    self._field,
                    "desc",
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "desc",
                    "limit 1",
                )
            )
            values = (
                self._current_segment.index_key,
                self.current_segment_number,
            )
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "< ?",
                        "order by",
                        self._field,
                        "desc",
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "desc",
                        "limit 1",
                    )
                )
                values = (self._current_segment.index_key,)
                row = self._cursor.execute(statement, values).fetchone()
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ? and",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "< ?",
                    "order by",
                    self._field,
                    "desc",
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "desc",
                    "limit 1",
                )
            )
            values = (
                self.get_converted_partial_with_wildcard(),
                self._current_segment.index_key,
                self.current_segment_number,
            )
            row = self._cursor.execute(statement, values).fetchone()
            if row is None:
                statement = " ".join(
                    (
                        "select",
                        self._field,
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        ",",
                        SQLITE_COUNT_COLUMN,
                        ",",
                        self.rowids_in_primary,
                        "from",
                        self._table,
                        "where",
                        self._field,
                        "glob ? and",
                        self._field,
                        "< ?",
                        "order by",
                        self._field,
                        "desc",
                        ",",
                        SQLITE_SEGMENT_COLUMN,
                        "desc",
                        "limit 1",
                    )
                )
                values = (
                    self.get_converted_partial_with_wildcard(),
                    self._current_segment.index_key,
                )
                row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        return self.set_current_segment(row).last()

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
        segment_number, record_number = divmod(
            record[1], SegmentSize.db_segment_size
        )
        if self.get_partial() is not None:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "glob ? and",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "== ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (
                self.get_converted_partial_with_wildcard(),
                record[0],
                segment_number,
            )
        else:
            statement = " ".join(
                (
                    "select",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    ",",
                    SQLITE_COUNT_COLUMN,
                    ",",
                    self.rowids_in_primary,
                    "from",
                    self._table,
                    "where",
                    self._field,
                    "== ? and",
                    SQLITE_SEGMENT_COLUMN,
                    "== ?",
                    "order by",
                    self._field,
                    ",",
                    SQLITE_SEGMENT_COLUMN,
                    "limit 1",
                )
            )
            values = (record[0], segment_number)
        row = self._cursor.execute(statement, values).fetchone()
        if row is None:
            return None
        segment = self._get_segment(*row)
        if record_number not in segment:
            return None
        self._current_segment = segment
        self.current_segment_number = row[1]
        return segment.setat(record[1])

    def set_partial_key(self, partial):
        """Set partial key and mark current segment as None."""
        self._partial = partial
        self._current_segment = None
        self.current_segment_number = None

    def _get_segment(self, key, segment_number, count, record_number):
        if count == 1:
            return RecordsetSegmentInt(
                segment_number,
                key,
                records=record_number.to_bytes(2, byteorder="big"),
            )
        if self.current_segment_number == segment_number:
            if key == self._current_segment.index_key:
                return self._current_segment
        records = self.get_segment_records(record_number)
        if len(records) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                segment_number, key, records=records
            )
        return RecordsetSegmentList(segment_number, key, records=records)

    def set_current_segment(self, segment_reference):
        """Return the recordset segment for segment_reference.

        The returned item is a RecordsetSegmentBitarray, RecordsetSegmentInt,
        or RecordsetSegmentList instance, depending on the current
        representation of the segment on the database.

        Argument is the 4-tuple segment reference returned by fetchone().

        """
        self._current_segment = self._get_segment(*segment_reference)
        self.current_segment_number = segment_reference[1]
        return self._current_segment

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.

        # raise DatabaseError('refresh_recordset not implemented')

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        statement = " ".join(
            (
                "select",
                SQLITE_SEGMENT_COLUMN,
                ",",
                SQLITE_COUNT_COLUMN,
                ",",
                self.rowids_in_primary,
                "from",
                self._table,
                "where",
                self._field,
                "== ?",
                "order by",
                self.rowids_in_primary,
                "limit 1",
            )
        )
        values = (key,)
        rows = self._cursor.execute(statement, values).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise DatabaseError("More than one segment for index value")
        segment_number, reference_count, record_number = rows[0]
        if reference_count != 1:
            raise DatabaseError("Index must refer to unique record")
        return segment_number * SegmentSize.db_segment_size + record_number


class RecordsetCursor(_RecordsetCursor):
    """Add _get_record method to _RecordsetCursor.

    RecordsetCursor is imported from recordset as _RecordsetCursor to
    avoid confusion on the class names within the _sqlite module.
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
                return record  # maybe (record_number, record)
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return None  # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return None  # maybe raise
        statement = " ".join(
            (
                "select",
                SQLITE_VALUE_COLUMN,
                "from",
                dbset.dbset,
                "where",
                dbset.dbset,
                "== ?",
                "limit 1",
            )
        )
        values = (record_number,)
        database_cursor = self.engine.cursor()
        try:
            record = database_cursor.execute(statement, values).fetchone()[0]
        finally:
            database_cursor.close()
        # maybe raise if record is None (if not, None should go on cache)
        if use_cache:
            dbset.record_cache[record_number] = record
            dbset.record.deque.append(record_number)
        return (record_number, record)


class ExistenceBitmapControl(_database.ExistenceBitmapControl):
    """Access existence bit map for file in database."""

    def __init__(self, file, database):
        """Note file whose existence bitmap is managed."""
        super().__init__(file, database)
        self.ebm_table = SUBFILE_DELIMITER.join(
            (self._file, EXISTENCE_BITMAP_SUFFIX)
        )
        create_statement = " ".join(
            (
                "create table if not exists",
                self.ebm_table,
                "(",
                self.ebm_table,
                "integer primary key",
                ",",
                SQLITE_VALUE_COLUMN,
                ")",
            )
        )
        count_statement = " ".join(
            (
                "select count ( rowid ) from",
                self.ebm_table,
            )
        )
        cursor = database.dbenv.cursor()
        try:
            cursor.execute(create_statement)
            self._segment_count = cursor.execute(count_statement).fetchone()[0]
        finally:
            cursor.close()

    def read_exists_segment(self, segment_number, dbenv):
        """Return existence bitmap for segment_number in database dbenv.

        get_ebm_segment returns the record containing the existence bitmap,
        read_exists_segment calls get_ebm_segment to get the bitmap record,
        converts it to a bitmap, and then returns the bitmap.

        """
        # Return existence bit map for segment_number.
        # record keys are 1-based but segment_numbers are 0-based.
        ebm = Bitarray()
        try:
            ebm.frombytes(self.get_ebm_segment(segment_number + 1, dbenv))
        except TypeError:
            return None
        return ebm

    def get_ebm_segment(self, key, dbenv):
        """Return existence bitmap for segment number key in database dbenv.

        get_ebm_segment returns the record containing the existence bitmap,
        use read_exists_segment to return the bitmap itself.

        """
        statement = " ".join(
            (
                "select",
                SQLITE_VALUE_COLUMN,
                "from",
                self.ebm_table,
                "where",
                self.ebm_table,
                "== ?",
                "limit 1",
            )
        )
        values = (key,)
        cursor = dbenv.cursor()
        try:
            return cursor.execute(statement, values).fetchone()[0]
        except TypeError:
            return None
        finally:
            cursor.close()

    # Not used at present but defined anyway.
    def delete_ebm_segment(self, key, dbenv):
        """Delete existence bitmap for segment key from database dbenv."""
        statement = " ".join(
            (
                "delete from",
                self.ebm_table,
                "where",
                self.ebm_table,
                "== ?",
            )
        )
        values = (key,)
        cursor = dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def put_ebm_segment(self, key, value, dbenv):
        """Update existence bitmap value for segment key to database dbenv."""
        statement = " ".join(
            (
                "update",
                self.ebm_table,
                "set",
                SQLITE_VALUE_COLUMN,
                "= ?",
                "where",
                self.ebm_table,
                "== ?",
            )
        )
        values = (value, key)
        cursor = dbenv.cursor()
        try:
            cursor.execute(statement, values)
        finally:
            cursor.close()

    def append_ebm_segment(self, value, dbenv):
        """Add existence bitmap, value, to database, dbenv."""
        statement = " ".join(
            (
                "insert into",
                self.ebm_table,
                "(",
                SQLITE_VALUE_COLUMN,
                ")",
                "values ( ? )",
            )
        )
        values = (value,)
        cursor = dbenv.cursor()
        try:
            return (
                cursor.execute(statement, values)
                .execute(
                    " ".join(
                        ("select last_insert_rowid() from", self.ebm_table)
                    )
                )
                .fetchone()[0]
            )
        finally:
            cursor.close()
