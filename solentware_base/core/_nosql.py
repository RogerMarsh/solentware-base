# _nosql.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a NoSQL database created from a FileSpec() definition with either
the unqlite or vedis modules.

"""
import os
from ast import literal_eval
import re
from bisect import bisect_right, bisect_left

import sys
_platform_win32 = sys.platform == 'win32'
_python_version = '.'.join(
    (str(sys.version_info[0]),
     str(sys.version_info[1])))
del sys

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
from . import cursor
from .bytebit import Bitarray, SINGLEBIT
from .segmentsize import SegmentSize
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordsetCursor,
    RecordList,
    )


class DatabaseError(Exception):
    pass


class Database(_database.Database):
    
    """Define file and record access methods which subclasses may override if
    necessary.
    """


    class SegmentSizeError(Exception):
        pass


    def __init__(self,
                 specification,
                 folder=None,
                 segment_size_bytes=DEFAULT_SEGMENT_SIZE_BYTES,
                 use_specification_items=None,
                 **soak):
        if folder is not None:
            try:
                path = os.path.abspath(folder)
            except:
                msg = ' '.join(['Database folder name', str(folder),
                                'is not valid'])
                raise DatabaseError(msg)
        else:
            path = None
        if not isinstance(specification, filespec.FileSpec):
            specification = filespec.FileSpec(
                use_specification_items=use_specification_items,
                **specification)
        self._use_specification_items = use_specification_items
        self._validate_segment_size_bytes(segment_size_bytes)
        if folder is not None:
            self.home_directory = path
            self.database_file = os.path.join(path, os.path.basename(path))
        else:
            self.home_directory = None
            self.database_file = None
        for fl in specification.values():
            for fd in fl[FIELDS]:
                if fd == fl[PRIMARY]:
                    continue
                for fa in NOSQL_FIELDATTS:
                    if fa not in fl[FIELDS][fd]:
                        fl[FIELDS][fd][fa] = SECONDARY_FIELDATTS[fa]
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
            raise DatabaseError('Database segment size must be an int')
        if not segment_size_bytes > 0:
            raise DatabaseError('Database segment size must be more than 0')

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
            (self.table[CONTROL_FILE], SPECIFICATION_KEY.decode()))
        segment_size_bytes_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], SEGMENT_SIZE_BYTES_KEY.decode()))
        table_register_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], TABLE_REGISTER_KEY.decode()))
        field_register_key = SUBFILE_DELIMITER.join(
            (self.table[CONTROL_FILE], FIELD_REGISTER_KEY.decode()))

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
                        {k:v for k, v in spec_from_db.items()
                         if k in self._use_specification_items})
                else:
                    self.specification.is_consistent_with(spec_from_db)
                segment_size = literal_eval(rssbk.decode())
                if self._real_segment_size_bytes is not False:
                    self.segment_size_bytes = self._real_segment_size_bytes
                    self._real_segment_size_bytes = False
                if segment_size != self.segment_size_bytes:
                    self._real_segment_size_bytes = segment_size
                    raise self.SegmentSizeError(
                        ''.join(('Segment size recorded in database is not ',
                                 'the one used attemping to open database')))
                if table_register_key in dbenv:
                    table_register = literal_eval(
                        dbenv[table_register_key].decode())
                    high_table_number = max(table_register.values())
                    if high_table_number < len(spec_from_db):
                        raise DatabaseError(
                            'High table number less than specification items')
                if field_register_key in dbenv:
                    field_register = literal_eval(
                        dbenv[field_register_key].decode())
                    for k, v in field_register.items():
                        hfn = [n for n in v.values()]
                        if len(hfn):
                            high_field_number[k] = max(hfn)
                        else:
                            high_field_number[k] = 0
                        if high_field_number[k] < len(
                            spec_from_db[k][SECONDARY]):
                            raise DatabaseError(
                                ''.join((
                                    'High field number less than number of ',
                                    'specification items')))
            elif rsk is None and rssbk is not None:
                raise DatabaseError('No specification recorded in database')
            elif rsk is not None and rssbk is None:
                raise DatabaseError('No segment size recorded in database')
        else:
            
            # A memory database
            # Set branching factor close to minimum value, 4, assuming a value
            # less than 100 is the default, if segment_size_bytes is None.
            # Assumption is a test environment: small segment, memory database.
            if self.segment_size_bytes is None:
                for fl in self.specification.values():
                    for fd in fl[FIELDS]:
                        if fd == fl[PRIMARY]:
                            continue
                        for fa in NOSQL_FIELDATTS:
                            if fa == BRANCHING_FACTOR:
                                fl[FIELDS][fd][fa] = max(
                                    SECONDARY_FIELDATTS[fa] // 10, 4)
            dbenv = dbclass()
            
        self.set_segment_size()
        self.dbenv = dbenv
        if files is None:
            files = self.specification.keys()
        if self.database_file is None:
            fs = self.specification.keys()
        elif rsk is None:
            fs = self.specification.keys()
        else:
            fs = literal_eval(rsk.decode()).keys()
        self.start_transaction()

        # Sorted so each file gets the same prefix each time in a new database.
        for e, file in enumerate(sorted(fs)):
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
                self.table[file][0], str(0), self)
            self.table_data[file] = SUBFILE_DELIMITER.join(
                (self.table[file][0], str(0)))
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
                        (self.table[file][0], str(field_number)))]

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
                    (self.table[file][0],
                     str(field_number),
                     SEGMENT_KEY_SUFFIX))

                # The records in a segment indexed by a value.
                # (SUBFILE_DELIMITER<segment Number>SUBFILE_DELIMITER<value> is
                # appended to create database key.)
                self.segment_records[fieldkey] = SUBFILE_DELIMITER.join(
                    (self.table[file][0],
                     str(field_number),
                     SEGMENT_VALUE_SUFFIX))

        if self.database_file is not None:
            if rsk is None and rssbk is None:
                self.dbenv[specification_key] = repr(self.specification)
                self.dbenv[segment_size_bytes_key
                           ] = repr(self.segment_size_bytes)
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
        # Normal source, put_instance, generates value by repr(object).
        assert file in self.specification
        if key is None:
            dbkey = self.next_record_number(file)
            self.dbenv[SUBFILE_DELIMITER.join(
                (self.table_data[file], str(dbkey)))] = value
            self.ebm_control[file]._high_record_number = dbkey
            return dbkey
        else:
            self.dbenv[SUBFILE_DELIMITER.join((self.table_data[file],
                                               str(key)))] = value
            return None

    def replace(self, file, key, oldvalue, newvalue):
        # Normal source, edit_instance, generates oldvalue and newvalue by
        # repr(object).
        assert file in self.specification
        self.dbenv[SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
                   ] = newvalue

    def delete(self, file, key, value):
        # Normal source, delete_instance, generates value by repr(object).
        assert file in self.specification
        del self.dbenv[
            SUBFILE_DELIMITER.join((self.table_data[file], str(key)))]
    
    def get_primary_record(self, file, key):
        """Return the instance given the record number in key."""
        assert file in self.specification
        if key is None:
            return None
        dbkey = SUBFILE_DELIMITER.join((self.table_data[file], str(key)))
        if dbkey in self.dbenv:
            return key, self.dbenv[dbkey].decode()

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
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            if ebmc.ebm_freed in self.dbenv:
                ebmc.freed_record_number_pages = literal_eval(
                    self.dbenv[ebmc.ebm_freed].decode())
            else:
                ebmc.freed_record_number_pages = []
        while len(ebmc.freed_record_number_pages):
            s = ebmc.freed_record_number_pages[0]

            # Do not reuse record number on segment of high record number.
            if s == ebmc._table_ebm_segments[-1]:
                return None

            lfrns = ebmc.read_exists_segment(s, self.dbenv)
            if lfrns is None:

                # Segment does not exist now.
                ebmc.freed_record_number_pages.remove(s)
                self.dbenv[ebmc.ebm_freed
                           ] = repr(ebmc.freed_record_number_pages)
                continue

            try:
                first_zero_bit = lfrns.index(False, 0 if s else 1)
            except ValueError:

                # No longer any record numbers available for re-use in segment.
                ebmc.freed_record_number_pages.remove(s)
                self.dbenv[ebmc.ebm_freed
                           ] = repr(ebmc.freed_record_number_pages)
                continue

            return s * SegmentSize.db_segment_size + first_zero_bit
        else:
            return None

    def next_record_number(self, dbset):
        ebmc = self.ebm_control[dbset]
        hr = self.get_high_record(dbset)
        if hr is None:
            return 0
        return hr[0] + 1

    # high_record will become high_record_number to fit changed get_high_record.
    def note_freed_record_number_segment(
        self, dbset, segment, record_number_in_segment, high_record):
        try:
            high_segment = divmod(high_record[0],
                                  SegmentSize.db_segment_size)[0]
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
                    self.dbenv[ebmc.ebm_freed].decode())
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
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        ebmcf = self.ebm_control[file]
        ebmb = ebmcf.get_ebm_segment(segment, self.dbenv)
        if ebmb is None:
            raise DatabaseError(
                'Existence bit map for segment does not exist')
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            ebm[record_number] = False
            self.ebm_control[file].put_ebm_segment(
                segment, ebm.tobytes(), self.dbenv)
            if ebmcf._high_record_number == deletekey:
                ebmcf.set_high_record_number(self.dbenv)
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        ebmcf = self.ebm_control[file]
        ebmb = ebmcf.get_ebm_segment(segment, self.dbenv)
        if ebmb is None:
            ebm = SegmentSize.empty_bitarray.copy()
            ebm[record_number] = True
            self.ebm_control[file].append_ebm_segment(ebm.tobytes(), self.dbenv)
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            ebm[record_number] = True
            self.ebm_control[file].put_ebm_segment(
                segment, ebm.tobytes(), self.dbenv)
        if ebmcf._high_record_number < putkey:
            ebmcf._high_record_number = putkey
        return segment, record_number

    # Change to return just the record number, and the name to fit.
    # Only used in one place, and it is extra work to get the data in_nosql.
    # Also actually getting the record does not prove it is the high record; in
    # _sqlite and _db asking for high record returns (key, data) where key will
    # be the high record number.
    def get_high_record(self, file):
        high_record_number = self.ebm_control[file]._high_record_number
        if high_record_number == -1:
            return None
        return high_record_number, self.dbenv[SUBFILE_DELIMITER.join(
            (self.table_data[file], str(high_record_number)))]
    
    def add_record_to_field_value(
        self, file, field, key, segment, record_number):
        segment_table_key = SUBFILE_DELIMITER.join(
            (self.segment_table[SUBFILE_DELIMITER.join((file, field))], key))
        db = self.dbenv
        if segment_table_key not in db:
            # Insert key into tree before creating segment_table_key record.
            if SUBFILE_DELIMITER.join((file, field)) in self.trees:
                self.trees[SUBFILE_DELIMITER.join((file, field))
                           ].insert(key)
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
                (self.segment_records[SUBFILE_DELIMITER.join((file, field))],
                 str(segment),
                 key))
            db[segment_records_key] = repr(
                b''.join([n.to_bytes(2, byteorder='big')
                          for n in segment_records]))
            segment_table[segment] = LIST_BYTES, len(segment_records)
            db[segment_table_key] = repr(segment_table)
            return
        segment_records_key = SUBFILE_DELIMITER.join(
            (self.segment_records[SUBFILE_DELIMITER.join((file, field))],
             str(segment),
             key))
        if reference == LIST_BYTES:
            segment_records = RecordsetSegmentList(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()))
            segment_records.insort_left_nodup(record_number)
            count = segment_records.count_records()
            if count > SegmentSize.db_upper_conversion_limit:
                db[segment_records_key
                   ] = repr(segment_records.promote().tobytes())
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
            records=literal_eval(db[segment_records_key].decode()))
        # Cheat a little rather than say:
        # segment_records[segment * <segment size> + record_number] = True
        segment_records._bitarray[record_number] = True
        db[segment_records_key] = repr(segment_records.tobytes())
        segment_table[segment] = BITMAP_BYTES, segment_records.count_records()
        db[segment_table_key] = repr(segment_table)
        return
    
    def remove_record_from_field_value(
        self, file, field, key, segment, record_number):
        segment_table_key = SUBFILE_DELIMITER.join(
            (self.segment_table[SUBFILE_DELIMITER.join((file, field))], key))
        db = self.dbenv
        if segment_table_key not in db:
            return
        segment_table = literal_eval(db[segment_table_key].decode())
        if segment not in segment_table:
            return
        reference = segment_table[segment][0]
        if reference == BITMAP_BYTES:
            segment_records_key = SUBFILE_DELIMITER.join(
                (self.segment_records[SUBFILE_DELIMITER.join((file, field))],
                 str(segment),
                 key))
            segment_records = RecordsetSegmentBitarray(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()))
            # Cheat a little rather than say:
            # segment_records[segment * <segment size> + record_number] = False
            segment_records._bitarray[record_number] = False
            count = segment_records.count_records()
            if count > SegmentSize.db_lower_conversion_limit:
                db[segment_records_key] = repr(segment_records.tobytes())
                segment_table[segment] = BITMAP_BYTES, count
                db[segment_table_key] = repr(segment_table)
                return
            # Cheat a little rather than say:
            #segment_records = segment_records.normalize(use_upper_limit=False)
            rsl = RecordsetSegmentList(segment, key)
            rsl._list.extend(segment_records._bitarray.search(SINGLEBIT))
            db[segment_records_key] = repr(rsl.tobytes())
            segment_table[segment] = LIST_BYTES, len(rsl._list)
            db[segment_table_key] = repr(segment_table)
            return
        if reference == LIST_BYTES:
            segment_records_key = SUBFILE_DELIMITER.join(
                (self.segment_records[SUBFILE_DELIMITER.join((file, field))],
                 str(segment),
                 key))
            segment_records = RecordsetSegmentList(
                segment,
                key,
                records=literal_eval(db[segment_records_key].decode()))
            # Cheating is only option!
            srl = segment_records._list
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
            segment_table[segment] = segment_records._list[0], 1
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
                    self.trees[SUBFILE_DELIMITER.join((file, field))
                               ].delete(key)

    def populate_segment(self, segment_number, segment_reference, file):
        if isinstance(segment_reference, int):
            return RecordsetSegmentInt(
                segment_number,
                None,
                records=segment_reference.to_bytes(2, byteorder='big'))
        else:
            if len(segment_reference) == SegmentSize.db_segment_size_bytes:
                return RecordsetSegmentBitarray(
                    segment_number, None, records=segment_reference)
            else:
                return RecordsetSegmentList(
                    segment_number, None, records=segment_reference)

    def populate_recordset(self, recordset, db, keyprefix, segmentprefix, key):
        segment_records = literal_eval(
            db[SUBFILE_DELIMITER.join((segmentprefix, key))].decode())
        for s, r in segment_records.items():
            if r[0] == LIST_BYTES:
                segment = RecordsetSegmentList(
                    s,
                    None,
                    records=literal_eval(db[
                        SUBFILE_DELIMITER.join(
                            (keyprefix,
                             SEGMENT_VALUE_SUFFIX,
                             str(s),
                             key))].decode()))
            elif r[0] == BITMAP_BYTES:
                segment = RecordsetSegmentBitarray(
                    s,
                    None,
                    records=literal_eval(db[
                        SUBFILE_DELIMITER.join(
                            (keyprefix,
                             SEGMENT_VALUE_SUFFIX,
                             str(s),
                             key))].decode()))
            else:
                segment = RecordsetSegmentInt(
                    s,
                    None,
                    records=r[0].to_bytes(2, byteorder='big'))
            if s not in recordset:
                recordset[s] = segment#.promote()
            else:
                recordset[s] |= segment

    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        cursor = tree.Cursor(self.trees[
            SUBFILE_DELIMITER.join((file, valuespec.field))])
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
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return rs
        s, rn = divmod(key, SegmentSize.db_segment_size)
        if s not in self.ebm_control[file]._table_ebm_segments:
            return rs
        r = self.ebm_control[file].get_ebm_segment(s, self.dbenv)
        if r and rn in RecordsetSegmentBitarray(s, key, records=r):
            rs[s] = RecordsetSegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))
        return rs

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1):
        """Return RecordList on file containing record numbers whose record
        exists in record number range."""
        if keystart is None and keyend is None:
            return self.recordlist_ebm(file, cache_size=cache_size)
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            segment_start, recnum_start = 0, 0
        else:
            segment_start, recnum_start = divmod(keystart,
                                                 SegmentSize.db_segment_size)
        if keyend is not None:
            segment_end, recnum_end = divmod(keyend,
                                             SegmentSize.db_segment_size)
        else:
            segment_end, recnum_end = None, None
        ebmcf = self.ebm_control[file]
        so = None
        eo = None
        for s in ebmcf._table_ebm_segments:
            if s < segment_start:
                continue
            if segment_end is not None and s > segment_end:
                continue
            b = literal_eval(self.dbenv[
                SUBFILE_DELIMITER.join((ebmcf.ebm_table, str(s)))].decode())
            if s == segment_start:
                if recnum_start:
                    so, sb = divmod(recnum_start, 8)
                    b = b'\x00' * so + b[so:]
            if keyend is not None:
                if (s == segment_end and
                    recnum_end < SegmentSize.db_segment_size - 1):
                    eo, eb = divmod(recnum_end, 8)
                    b = (b[:eo+1] +
                         b'\x00' * (
                             SegmentSize.db_segment_size_bytes - eo - 1))
            rs[s] = RecordsetSegmentBitarray(s, None, records=b)
        if so is not None:
            for i in range(so * 8, so * 8 + sb):
                rs[segment_start][(segment_start, i)] = False
        if eo is not None:
            for i in range(eo * 8 + eb, (eo + 1) * 8):
                rs[segment_end][(segment_end, i)] = False
        return rs
    
    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList on file containing record numbers whose record
        exists."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        ebm_table = self.ebm_control[file].ebm_table
        for r in self.ebm_control[file]._table_ebm_segments:
            rs[r] = RecordsetSegmentBitarray(
                r,
                None,
                records=literal_eval(self.dbenv[
                    SUBFILE_DELIMITER.join((ebm_table, str(r)))].decode()))
        return rs

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return RecordList on file containing database records for field
        with keys like key."""
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(''.join(
                ("'", field, "' field in '", file, "' file is not ordered")))
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keylike is None:
            return rs
        matcher = re.compile('.*?' + keylike, flags=re.IGNORECASE|re.DOTALL)
        db = self.dbenv
        t = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(t)
        try:
            while True:
                k = cursor.next()
                if k is None:
                    break
                if not matcher.match(k):
                    continue
                self.populate_recordset(rs, db, t.key_root, t.key_segment, k)
        finally:
            cursor.close()
        return rs

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        key_root = self.table[SUBFILE_DELIMITER.join((file, field))][0]
        key_segment = SUBFILE_DELIMITER.join((key_root, SEGMENT_KEY_SUFFIX))
        try:
            segment_records = literal_eval(
                db[SUBFILE_DELIMITER.join((key_segment, key))].decode())
        except KeyError:
            return rs
        except TypeError:
            if key is not None:
                raise
            return rs
        self.populate_recordset(rs, db, key_root, key_segment, key)
        return rs

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1):
        """Return RecordList on file containing records for field with
        keys starting key.
        """
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(''.join(
                ("'", field, "' field in '", file, "' file is not ordered")))
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            return rs
        db = self.dbenv
        t = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(t)
        try:
            k = cursor.nearest(keystart)
            while k is not None:
                if not k.startswith(keystart):
                    break
                self.populate_recordset(rs, db, t.key_root, t.key_segment, k)
                k = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1):
        """Return RecordList on file containing records for field with
        keys in range set by combinations of ge, gt, le, and lt.
        """
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(''.join(
                ("'", field, "' field in '", file, "' file is not ordered")))
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        elif le and lt:
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        t = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(t)
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
                        rs, db, t.key_root, t.key_segment, k)
                    k = cursor.next()
            elif lt is None:
                while k is not None:
                    if k > le:
                        break
                    segment_records = literal_eval(
                        db[SUBFILE_DELIMITER.join((t.key_segment, k))].decode())
                    self.populate_recordset(
                        rs, db, t.key_root, t.key_segment, k)
                    k = cursor.next()
            else:
                while k is not None:
                    if k >= lt:
                        break
                    self.populate_recordset(
                        rs, db, t.key_root, t.key_segment, k)
                    k = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        if SUBFILE_DELIMITER.join((file, field)) not in self.trees:
            raise DatabaseError(''.join(
                ("'", field, "' field in '", file, "' file is not ordered")))
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        db = self.dbenv
        t = self.trees[SUBFILE_DELIMITER.join((file, field))]
        cursor = tree.Cursor(t)
        try:
            while True:
                k = cursor.next()
                if k is None:
                    break
                self.populate_recordset(rs, db, t.key_root, t.key_segment, k)
        finally:
            cursor.close()
        return rs

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
        for sn, ref in literal_eval(db[table_key].decode()).items():
            if isinstance(ref[0], str):
                del db[SUBFILE_DELIMITER.join((segment_key, str(sn), key))]
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
        for sn, rs_segment in recordset.rs_segments.items():
            if isinstance(rs_segment, RecordsetSegmentBitarray):
                db[SUBFILE_DELIMITER.join((segment_key, str(sn), key))
                   ] = repr(rs_segment.tobytes())
                segments[sn] = BITMAP_BYTES, rs_segment.count_records()
            elif isinstance(rs_segment, RecordsetSegmentList):
                db[SUBFILE_DELIMITER.join((segment_key, str(sn), key))
                   ] = repr(rs_segment.tobytes())
                segments[sn] = LIST_BYTES, rs_segment.count_records()
            elif isinstance(rs_segment, RecordsetSegmentInt):
                segments[sn] = rs_segment._record_number, 1
        if fieldkey in self.trees:
            self.trees[fieldkey].insert(key)
        db[SUBFILE_DELIMITER.join((self.segment_table[fieldkey], key))
           ] = repr(segments)

    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on SQLite Connection() for (file, field).
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        assert file in self.specification
        if file == field:
            return CursorPrimary(self,
                                 file=file,
                                 keyrange=keyrange)
        fieldkey = SUBFILE_DELIMITER.join((file, field))
        if fieldkey not in self.trees:
            raise DatabaseError(''.join(
                ("'", field, "' field in '", file, "' file is not ordered")))
        return CursorSecondary(self,
                               file=file,
                               field=field,
                               keyrange=keyrange)

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
        taskmethodargs={},
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
            use_specification_items=use_specification_items)
        db.open_database()
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


class Cursor(cursor.Cursor):
    
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
        self._current_segment_number = None
        self._current_record_number_in_segment = None

    def close(self):
        """Delete database cursor then delegate to superclass close() method."""
        self._file = None
        self._current_segment_number = None
        self._current_record_number_in_segment = None
        super().close()

    def get_converted_partial(self):
        """return self._partial as it would be held on database."""
        return self._partial

    def get_partial_with_wildcard(self):
        """return self._partial with wildcard suffix appended."""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """return converted self._partial with wildcard suffix appended."""
        return self._partial

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in sqlite3.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass


class CursorPrimary(Cursor):
    
    """Define a cursor on the underlying database engine dbset.

    dbset - apsw or sqlite3 Connection() object.
    ebm - table name of existence bitmap of file cursor will be applied to.
    kargs - superclass arguments and absorb arguments for other engines.

    This class does not need a field argument, like CursorSecondary, because
    the file argument collected by super().__init__() fills that role here.
    
    """

    def __init__(self, dbset, **kargs):
        super().__init__(dbset, **kargs)
        self._table = dbset.table_data[self._file]
        self._ebm = dbset.ebm_control[self._file]

    def count_records(self):
        """return record count or None if cursor is not usable."""
        if self._dbset is None:
            return None
        count = 0
        db = self._dbset
        e = self._ebm
        for s in e._table_ebm_segments:
            count += e.read_exists_segment(s, db).count()
        return count

    def first(self):
        """Return first record."""
        db = self._dbset
        e = self._ebm
        for s in e._table_ebm_segments:
            seg = RecordsetSegmentBitarray(
                s, None, e.get_ebm_segment(s, db))
            k = seg.first()
            if k:
                return self._get_record(seg, k)

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        count = 0
        db = self._dbset
        e = self._ebm
        tes = e._table_ebm_segments
        if not tes:
            return count
        s, r = divmod(record[0], SegmentSize.db_segment_size)
        si = bisect_right(tes, s)
        for i in e._table_ebm_segments[:si-1]:
            count += e.read_exists_segment(i, db).count()
        if tes[si-1] == s:
            count += RecordsetSegmentBitarray(
                s, None, e.get_ebm_segment(s, db
                                           )).get_position_of_record_number(r)
        elif tes[si-1] < s:
            count += e.read_exists_segment(tes[si-1], db).count()
        return count

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None."""
        if position is None:
            return None
        db = self._dbset
        e = self._ebm
        tes = e._table_ebm_segments
        if not tes:
            return None
        count = 0
        if position < 0:
            for s in reversed(tes):
                ba = e.read_exists_segment(s, db)
                bacount = ba.count()
                count -= bacount
                if count > position:
                    continue
                seg = _empty_recordset_segment_bitarray()
                seg._bitarray = ba
                seg._segment_number = s
                seg._current_position_in_segment = position - count - bacount
                k = seg.get_record_number_at_position(
                    seg._current_position_in_segment)
                if k is not None:
                    return self._get_record(seg, (None, k))
                break
            else:
                return None
        else:
            for s in tes:
                ba = e.read_exists_segment(s, db)
                bacount = ba.count()
                count += bacount
                if count <= position:
                    continue
                seg = _empty_recordset_segment_bitarray()
                seg._bitarray = ba
                seg._segment_number = s
                seg._current_position_in_segment = position - count + bacount
                k = seg.get_record_number_at_position(
                    seg._current_position_in_segment)
                if k is not None:
                    return self._get_record(seg, (None, k))
                break
            else:
                return None

    def last(self):
        """Return last record."""
        db = self._dbset
        e = self._ebm
        for s in reversed(e._table_ebm_segments):
            seg = RecordsetSegmentBitarray(
                s, None, e.get_ebm_segment(s, db))
            k = seg.last()
            if k:
                return self._get_record(seg, k)

    def nearest(self, key):
        """Return nearest record to key."""
        db = self._dbset
        e = self._ebm
        s, r = divmod(key, SegmentSize.db_segment_size)
        for i in e._table_ebm_segments[bisect_left(e._table_ebm_segments, s):]:
            seg = RecordsetSegmentBitarray(
                i, None, e.get_ebm_segment(i, db))
            if r in seg:
                return self._get_record(
                    seg, (None, i * SegmentSize.db_segment_size + r))
            else:
                seg._current_position_in_segment = r
                r = seg.next()
                if r is not None:
                    return self._get_record(seg, (None, r[-1]))
                r = 0

    def next(self):
        """Return next record."""
        if self._current_segment_number is None:
            return self.first()
        db = self._dbset
        e = self._ebm
        s = self._current_segment_number
        r = self._current_record_number_in_segment
        if r == SegmentSize.db_segment_size - 1:
            r = 0
            s += 1
        else:
            r += 1
        for i in e._table_ebm_segments[bisect_left(e._table_ebm_segments, s):]:
            seg = RecordsetSegmentBitarray(
                i, None, e.get_ebm_segment(i, db))
            seg._current_position_in_segment = r
            if r in seg:
                return self._get_record(
                    seg, (None, i * SegmentSize.db_segment_size + r))
            else:
                seg._current_position_in_segment = r
                r = seg.next()
                if r is not None:
                    return self._get_record(seg, (None, r[-1]))
                r = 0

    def prev(self):
        """Return previous record."""
        if self._current_segment_number is None:
            return self.last()
        db = self._dbset
        e = self._ebm
        s = self._current_segment_number
        r = self._current_record_number_in_segment
        if r == 0:
            r = SegmentSize.db_segment_size - 1
            s -= 1
        else:
            r -= 1
        for i in reversed(
            e._table_ebm_segments[:bisect_left(e._table_ebm_segments, s + 1)]):
            seg = RecordsetSegmentBitarray(
                i, None, e.get_ebm_segment(i, db))
            seg._current_position_in_segment = r
            if r in seg:
                return self._get_record(
                    seg, (None, i * SegmentSize.db_segment_size + r))
            else:
                seg._current_position_in_segment = r
                r = seg.prev()
                if r is not None:
                    return self._get_record(seg, (None, r[-1]))
                r = SegmentSize.db_segment_size - 1

    def setat(self, record):
        """Return current record after positioning cursor at record.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        e = self._ebm
        tes = e._table_ebm_segments
        s, r = divmod(record[0], SegmentSize.db_segment_size)
        i = bisect_right(tes, s)
        if tes and tes[i - 1] == s:
            seg = RecordsetSegmentBitarray(
                s, None, e.get_ebm_segment(s, self._dbset))
            if r in seg:
                seg._current_position_in_segment = r
                return self._get_record(
                    seg, (None, s * SegmentSize.db_segment_size + r))

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        #raise DatabaseError('refresh_recordset not implemented')

    def _get_record(self, segment, ref):
        assert ref is not None
        d = self._dbset[SUBFILE_DELIMITER.join((self._table, str(ref[-1])))
                        ].decode()
        (self._current_segment_number,
         self._current_record_number_in_segment) = (
             segment.segment_number,
             segment._current_position_in_segment)
        return (ref[-1], d)


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
        super().__init__(dbset, **kargs)
        fieldkey = SUBFILE_DELIMITER.join((self._file, field))
        if fieldkey not in dbset.trees:
            raise DatabaseError(
                ''.join(("Cannot create cursor because '",
                         field, "' field in '",
                         self._file, "' file is not ordered")))
        self._field = field
        self._table = dbset.table_data[self._file]
        self._tree = dbset.trees[fieldkey]
        self._cursor = tree.Cursor(self._tree)
        self._value_prefix = ''.join(
            (self._tree.key_root,
             SUBFILE_DELIMITER,
             SEGMENT_VALUE_SUFFIX,
             SUBFILE_DELIMITER))
        self._segment_table_prefix = ''.join(
            (self._tree.key_segment, SUBFILE_DELIMITER))
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
                count += SegmentsetCursor(db,
                                          self._segment_table_prefix,
                                          self._value_prefix,
                                          key).count_records()
        else:
            key = cursor.nearest(self.get_converted_partial())
            while key is not None:
                if not key.startswith(self.get_converted_partial()):
                    break
                count += SegmentsetCursor(db,
                                          self._segment_table_prefix,
                                          self._value_prefix,
                                          key).count_records()
                key = cursor.next()
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            return self._first()
        elif self.get_partial() is False:
            return None
        else:
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
        segment_number, record_number = divmod(value,
                                               SegmentSize.db_segment_size)

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
                self.get_converted_partial_with_wildcard())
        while rkey:
            if low(rkey, key):
                position += SegmentsetCursor(db,
                                             self._segment_table_prefix,
                                             self._value_prefix,
                                             rkey).count_records()
            elif high(rkey, key):
                break
            else:
                segment_records = SegmentsetCursor(db,
                                                   self._segment_table_prefix,
                                                   self._value_prefix,
                                                   rkey)
                while True:
                    s = segment_records.next()
                    if s is None:
                        break
                    if s < segment_number:
                        position += (
                            segment_records.count_current_segment_records())
                    elif s == segment_number:
                        position += segment_records.get_current_segment(
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
                segment_records = SegmentsetCursor(db,
                                                   self._segment_table_prefix,
                                                   self._value_prefix,
                                                   key)
                while True:
                    ssn = segment_records.prev()
                    if ssn is None:
                        break
                    r2 = segment_records.count_current_segment_records()
                    count -= r2
                    if count > position:
                        continue
                    record_number = segment_records.get_current_segment(
                        ).get_record_number_at_position(position - count - r2)
                    if record_number is not None:
                        return key, record_number
                    return
                key = step()
        else:
            while key:
                segment_records = SegmentsetCursor(db,
                                                   self._segment_table_prefix,
                                                   self._value_prefix,
                                                   key)
                while True:
                    ssn = segment_records.next()
                    if ssn is None:
                        break
                    r2 = segment_records.count_current_segment_records()
                    count += r2
                    if count <= position:
                        continue
                    record_number = segment_records.get_current_segment(
                        ).get_record_number_at_position(position - count + r2)
                    if record_number is not None:
                        return key, record_number
                    return
                key = step()

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            return self._last()
        elif self.get_partial() is False:
            return None
        else:
            c = list(self.get_partial())
            while True:
                try:
                    c[-1] = chr(ord(c[-1]) + 1)
                except ValueError:
                    c.pop()
                    if not len(c):
                        try:
                            k, v = self._cursor.last()
                        except TypeError:
                            return None
                        return k, v
                    continue
                self._nearest(''.join(c))
                try:
                    k, v = self._prev()
                except TypeError:
                    return None
                return k, v

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if  self.get_partial() is False:
            return None
        try:
            k, v = self._nearest(key)
        except TypeError:
            return None
        if self.get_partial() is not None:
            if not k.startswith(self.get_converted_partial()):
                return None
        return k, v

    def next(self):
        """Return next record taking partial key into account."""
        if self._current_segment is None:
            return self.first()
        if self.get_partial() is False:
            return None
        try:
            k, v = self._next()
        except TypeError:
            return None
        return k, v

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._current_segment is None:
            return self.last()
        if self.get_partial() is False:
            return None
        try:
            k, v = self._prev()
        except TypeError:
            return None
        return k, v

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
        segment_number, record_number = divmod(record[1],
                                               SegmentSize.db_segment_size)
        segment_table = SegmentsetCursor(self._dbset,
                                         self._segment_table_prefix,
                                         self._value_prefix,
                                         record[0])
        ssn = segment_table._sorted_segment_numbers
        table_index = bisect_right(ssn, segment_number)
        if not (ssn and ssn[table_index - 1] == segment_number):
            return None
        segment = self.set_current_segment_table(
            record[0], segment_table, table_index - 1)
        if record_number not in segment:
            return None
        # The minus 1 should not be there but it prevents the problem occuring
        # until a segment with one record is met.
        # Implies problem is in this class' next, prev, etc methods and the
        # interaction with the three RecordsetSegment... classes methods used
        # in each case.
        # I think I forget the record position is based at 1 not 0 when writing
        # this code.
        segment._current_position_in_segment = (
            segment.get_position_of_record_number(record_number))# - 1)
        return record

    def set_partial_key(self, partial):
        """Set partial key and mark current segment as None."""
        self._partial = partial
        self._current_segment = None
        self._current_segment_number = None

    def set_current_segment(self, key):
        self._current_segment_number = (
            self._segment_table._current_segment_number)
        self._current_segment = self._segment_table.get_current_segment()
        return self._current_segment

    def set_current_segment_table(self, key, segment_table, table_index=None):
        self._segment_table = segment_table
        segment_table._current_segment_number = (
            segment_table._sorted_segment_numbers[table_index])
        self._current_segment_number = segment_table._current_segment_number
        self._current_segment = segment_table.get_current_segment()
        return self._current_segment

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.
        
        #raise DatabaseError('refresh_recordset not implemented')

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        nkey = self._cursor.nearest(key)
        if nkey != key:
            return None
        segment_table = SegmentsetCursor(self._dbset,
                                         self._segment_table_prefix,
                                         self._value_prefix,
                                         key)
        if len(segment_table) == 1:
            s = segment_table._sorted_segment_numbers[0]
            record_number = segment_table._segments[s]
            if isinstance(record_number, int):
                return record_number + s * SegmentSize.db_segment_size
        raise DatabaseError('Index must refer to unique record')

    def _first(self):
        key = self._cursor.first()
        if key is None:
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(self._dbset,
                             self._segment_table_prefix,
                             self._value_prefix,
                             key),
            0).first()

    def _last(self):
        key = self._cursor.last()
        if key is None:
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(self._dbset,
                             self._segment_table_prefix,
                             self._value_prefix,
                             key),
            -1).last()

    def _nearest(self, key):
        key = self._cursor.nearest(key)
        if key is None:
            self._current_segment = None
            self._current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        return self.set_current_segment_table(
            key,
            SegmentsetCursor(self._dbset,
                             self._segment_table_prefix,
                             self._value_prefix,
                             key),
            0).first()

    def _next(self):
        key = self._current_segment.next()
        if key is not None:
            return key
        if self._segment_table.next() is not None:
            self.set_current_segment(self._current_segment._key)
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
            SegmentsetCursor(self._dbset,
                             self._segment_table_prefix,
                             self._value_prefix,
                             key),
            0).first()

    def _prev(self):
        key = self._current_segment.prev()
        if key is not None:
            return key
        if self._segment_table.prev() is not None:
            self.set_current_segment(self._current_segment._key)
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
            SegmentsetCursor(self._dbset,
                             self._segment_table_prefix,
                             self._value_prefix,
                             key),
            -1).last()

    def _first_partial(self, partial):
        r = self._cursor.nearest(partial)
        if r is None:
            return None
        if not r.startswith(partial):
            return None
        return r

    def _last_partial(self, partial):
        r = self._first_partial(partial)
        if r is None:
            return None
        while True:
            r = self._cursor.next()
            if r is None:
                r = self._cursor.last()
                if r is None:
                    return None
                if not r.startswith(partial):
                    return None
                return r
            if not r.startswith(partial):
                r = self._cursor.prev()
                if r is None:
                    return None
                if not r.startswith(partial):
                    return None
                return r


class RecordsetCursor(RecordsetCursor):
    
    """Add _get_record method to RecordsetCursor."""

    def __init__(self, recordset, engine, **kargs):
        """Delegate recordset to superclass.

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
            return # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return # maybe raise
        dbkey = SUBFILE_DELIMITER.join(
            (dbset.dbhome.table_data[dbset.dbset],
             str(record_number)))
        try:
            record = self.engine[dbkey].decode()
        except KeyError:
            return
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

    _table_ebm_segments is the set of segment numbers which exist on the file.
    It is stored on the database as a tuple because literal_eval(set()) raises
    a ValueError exception.  Fortunately we are not trying to put a set in the
    tuple, which gives a TypeError exception in repr().
    """

    def __init__(self, file, notional_field, database):
        """Note file whose existence bitmap record number re-use is managed.
        """
        super().__init__(file, database)
        self.ebm_table = SUBFILE_DELIMITER.join(
            (self._file, notional_field, EXISTENCE_BITMAP_SUFFIX))
        self.ebm_freed = SUBFILE_DELIMITER.join(
            (self._file, notional_field, FREED_RECORD_NUMBER_SEGMENTS_SUFFIX))
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
        self._table_ebm_segments = literal_eval(dbenv[self.ebm_table].decode())

        self._segment_count = len(self._table_ebm_segments)
        self.set_high_record_number(dbenv)

    def read_exists_segment(self, segment_number, dbenv):
        # Return existence bit map for segment_number.
        ebm = Bitarray()
        try:
            ebm.frombytes(self.get_ebm_segment(segment_number, dbenv))
        except TypeError:
            return None
        return ebm

    def get_ebm_segment(self, key, dbenv):
        tes = self._table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            return literal_eval(dbenv[
                SUBFILE_DELIMITER.join((self.ebm_table, str(key)))].decode())

    # Not used at present but defined anyway.
    def delete_ebm_segment(self, key, dbenv):
        tes = self._table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            del dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))]
            del self._table_ebm_segments[insertion_point - 1]
            dbenv[self.ebm_table] = repr(tes)
            self._segment_count = len(tes)

    def put_ebm_segment(self, key, value, dbenv):
        tes = self._table_ebm_segments
        insertion_point = bisect_right(tes, key)
        if tes and tes[insertion_point - 1] == key:
            dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))
                  ] = repr(value)

    def append_ebm_segment(self, value, dbenv):
        segments = self._table_ebm_segments
        key = segments[-1] + 1 if len(segments) else 0
        dbenv[SUBFILE_DELIMITER.join((self.ebm_table, str(key)))] = repr(value)
        segments.append(key)
        dbenv[self.ebm_table] = repr(segments)
        self._segment_count = len(segments)
        return key

    def set_high_record_number(self, dbenv):
        for s in reversed(self._table_ebm_segments):
            hr = RecordsetSegmentBitarray(
                s, None, self.get_ebm_segment(s, dbenv)).last()
            if hr is None:
                continue
            self._high_record_number = hr[-1]
            break
        else:
            self._high_record_number = -1


class SegmentsetCursor:
    
    """Provide cursor for segment headers associated with a value of a field
    in a file.  The current segment number can be retrieved from the database
    but it is discarded once the current pointer moves on.
    """

    def __init__(self, dbenv, segment_table_prefix, value_prefix, key):
        #""""""
        self._current_segment_number = None
        self._dbenv = dbenv
        self._index = ''.join((segment_table_prefix, key))
        self._segments = literal_eval(self._dbenv[self._index].decode())
        self._sorted_segment_numbers = sorted(self._segments)
        self._values_index = value_prefix, ''.join((SUBFILE_DELIMITER, key))

    def __del__(self):
        #"""Delete segment set."""
        self.close()

    def close(self):
        #"""Close segment set making it unusable."""
        self._current_segment_number = None
        self._dbenv = None
        self._index = None
        self._segments = None
        self._sorted_segment_numbers = None
        self._values_index = None

    def __len__(self):
        return len(self._segments)

    def __contains__(self, segment_number):
        return segment_number in self._segments

    def first(self):
        #"""Return segment number of first segment in segment number order."""
        if not len(self._sorted_segment_numbers):
            return None
        self._current_segment_number = self._sorted_segment_numbers[0]
        return self._current_segment_number

    def last(self):
        #"""Return segment number of last segment in segment number order."""
        if not len(self._sorted_segment_numbers):
            return None
        self._current_segment_number = self._sorted_segment_numbers[-1]
        return self._current_segment_number

    def next(self):
        #"""Return segment number of next segment in segment number order."""
        if self._current_segment_number is None:
            return self.first()
        point = bisect_right(self._sorted_segment_numbers,
                             self._current_segment_number)
        if point == len(self._sorted_segment_numbers):
            return None
        self._current_segment_number = self._sorted_segment_numbers[point]
        return self._current_segment_number

    def prev(self):
        #"""Return segment number of previous segment in segment number order."""
        if self._current_segment_number is None:
            return self.last()
        point = bisect_left(self._sorted_segment_numbers,
                            self._current_segment_number)
        if point == 0:
            return None
        self._current_segment_number = self._sorted_segment_numbers[point - 1]
        return self._current_segment_number

    def setat(self, segment_number):
        #"""Set current segment number and return segment."""
        if segment_number not in self._segments:
            return None
        self._current_segment_number = segment_number
        return self.get_current_segment()

    def get_current_segment(self):
        #"""Return segment for current segment number in segment number order."""
        segment_number = self._current_segment_number
        segment_type = self._segments[segment_number][0]
        key = self._values_index[-1][len(SUBFILE_DELIMITER):]
        if segment_type == BITMAP_BYTES:
            return RecordsetSegmentBitarray(
                segment_number,
                key,
                records=literal_eval(
                    self._dbenv[str(segment_number
                                    ).join(self._values_index)].decode()))
        elif segment_type == LIST_BYTES:
            return RecordsetSegmentList(
                segment_number,
                key,
                records=literal_eval(
                    self._dbenv[str(segment_number
                                    ).join(self._values_index)].decode()))
        else:
            return RecordsetSegmentInt(
                segment_number,
                key,
                records=segment_type.to_bytes(2, byteorder='big'))

    def count_records(self):
        # Return count of record references in segment table.
        count = 0
        while True:
            s = self.next()
            if s is None:
                break
            count += self.count_current_segment_records()
        return count

    def count_current_segment_records(self):
        # Return count of record references in current segment.
        return self._segments[self._current_segment_number][1]
        #segment_number = self._current_segment_number
        #r = self._segments[segment_number]
        #if r == LIST_BYTES:
        #    return RecordsetSegmentList(
        #        segment_number,
        #        None,
        #        records=literal_eval(
        #            self._dbenv[str(segment_number).join(self._values_index)
        #                        ].decode())).count_records()
        #elif r == BITMAP_BYTES:
        #    ba = Bitarray()
        #    ba.frombytes(literal_eval(
        #        self._dbenv[str(segment_number).join(self._values_index)
        #                    ].decode()))
        #    return ba.count()
        #else:
        #    return 1


# Defining and using this function implies RecordsetSegmentBitarray is not
# quite correct any more.
# Motivation is passing a Bitarray, not the bytes to create a Bitarray, as an
# argument to RecordsetSegmentBitarray() call.
def _empty_recordset_segment_bitarray():
    class E(RecordsetSegmentBitarray):
        def __init__(self):
            pass
    e = E()
    e.__class__ = RecordsetSegmentBitarray
    return e
