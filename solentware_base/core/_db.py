# _db.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley database created from a FileSpec() definition with the
bsddb3 module.

"""
import os
from ast import literal_eval
import bisect
import re

import sys
_openbsd_platform = sys.platform.startswith('openbsd')
del sys

from . import filespec
from .constants import (
    PRIMARY,
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_SUFFIX,
    CONTROL_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    SEGMENT_HEADER_LENGTH,
    FIELDS,
    ACCESS_METHOD,
    HASH,
    BTREE,
    )
from . import _database
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
                 environment=None,
                 segment_size_bytes=DEFAULT_SEGMENT_SIZE_BYTES,
                 use_specification_items=None,
                 file_per_database=False,
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
        if environment is None:
            environment = {}
        if not isinstance(environment, dict):
            raise DatabaseError('Database environment must be a dictionary')
        self._validate_segment_size_bytes(segment_size_bytes)
        if folder is not None:
            self.home_directory = path
            self.database_file = os.path.join(path, os.path.basename(path))
        else:
            self.home_directory = None
            self.database_file = None
        self.specification = specification
        self.environment = environment
        self.segment_size_bytes = segment_size_bytes
        self.dbenv = None
        self.table = {}
        self.dbtxn = None
        self._dbe = None
        self.segment_table = {}
        self.ebm_control = {}
        self.ebm_segment_count = {}

        # Set to value read from database on attempting to open database if
        # different from segment_size_bytes.
        self._real_segment_size_bytes = False

        # Used to reset segment_size_bytes to initialization value after close
        # database.
        self._initial_segment_size_bytes = segment_size_bytes

        # All databases can be put in a single file in home_directory, or each
        # database can be put in a separate file in home_directory.  The single
        # file has same name as home_directory and the separate files have the
        # same name as the database.
        # The databases created in an empty home_directory from a FileSpec are
        # placed in separate files if file_per_database is True.
        self._file_per_database = file_per_database
        self._initial_file_per_database = file_per_database

    def _validate_segment_size_bytes(self, segment_size_bytes):
        if segment_size_bytes is None:
            return
        if not isinstance(segment_size_bytes, int):
            raise DatabaseError('Database segment size must be an int')
        if not segment_size_bytes > 0:
            raise DatabaseError('Database segment size must be more than 0')

    def start_transaction(self):
        """Start transaction if none and bind txn object to self._dbtxn."""
        if self.dbtxn is None:
            self.dbtxn = self.dbenv.txn_begin()

    def backout(self):
        """Abort the active transaction and remove binding to txn object."""
        if self.dbtxn is not None:
            self.dbtxn.abort()
            self.dbtxn = None
            self.dbenv.txn_checkpoint(5)
            
    def commit(self):
        """Commit the active transaction and remove binding to txn object."""
        if self.dbtxn is not None:
            self.dbtxn.commit()
            self.dbtxn = None
            self.dbenv.txn_checkpoint(5)

    def file_name_for_database(self, database):
        if not self._file_per_database:
            return self.database_file
        if self.home_directory is not None:
            return os.path.join(self.home_directory, database)
        return database

    def open_database(self, dbe, files=None):
        """Open DB environment and specified primary and secondary databases.

        By default all primary databases are opened, but just those named in
        files otherwise, along with their associated secondaries.

        dbe must be db from a Python module implementing the Berkeley DB API.

        """
        if self.home_directory is not None:
            try:
                os.mkdir(self.home_directory)
            except FileExistsError:
                if not os.path.isdir(self.home_directory):
                    raise
            all_in_one = set()
            one_per_database = set()
            for f in self.specification:
                db = dbe.DB()
                try:
                    db.open(self.database_file,
                            dbname=f,
                            flags=dbe.DB_RDONLY)
                    all_in_one.add(f)
                except:
                    pass
                finally:
                    db.close()
                db = dbe.DB()
                try:
                    db.open(os.path.join(self.home_directory, f),
                            dbname=f,
                            flags=dbe.DB_RDONLY)
                    one_per_database.add(f)
                except:
                    pass
                finally:
                    db.close()
            if all_in_one and one_per_database:
                raise DatabaseError(
                    ''.join(("Specified databases exist in both 'one per ",
                             "file' and 'all in one file' style")))
            elif all_in_one:
                self._file_per_database = False
            elif one_per_database:
                self._file_per_database = True
            else:
                self._file_per_database = self._initial_file_per_database
        else:
            self._file_per_database = self._initial_file_per_database
        control = dbe.DB()
        control.set_flags(dbe.DB_DUPSORT)
        try:
            control.open(
                self.file_name_for_database(CONTROL_FILE),
                dbname=CONTROL_FILE,
                dbtype=dbe.DB_BTREE,
                flags=dbe.DB_RDONLY,
                txn=self.dbtxn)
            try:
                spec_from_db = literal_eval(control.get(SPECIFICATION_KEY
                                            ).decode())
                if self._use_specification_items is not None:
                    self.specification.is_consistent_with(
                        {k:v for k, v in spec_from_db.items()
                         if k in self._use_specification_items})
                else:
                    self.specification.is_consistent_with(spec_from_db)
                segment_size = literal_eval(
                    control.get(SEGMENT_SIZE_BYTES_KEY).decode())
                if self._real_segment_size_bytes is not False:
                    self.segment_size_bytes = self._real_segment_size_bytes
                    self._real_segment_size_bytes = False
                if segment_size != self.segment_size_bytes:
                    self._real_segment_size_bytes = segment_size
                    raise self.SegmentSizeError(
                        ''.join(('Segment size recorded in database is not ',
                                 'the one used attemping to open database')))
            finally:
                control.close()
            db_create = 0
        except dbe.DBNoSuchFileError:
            db_create = dbe.DB_CREATE
        finally:
            del control
        self.set_segment_size()
        gbytes = self.environment.get('gbytes', 0)
        bytes_ = self.environment.get('bytes', 0)
        self.dbenv = dbe.DBEnv()
        if gbytes or bytes_:
            self.dbenv.set_cachesize(gbytes, bytes_)
        if self.home_directory is not None:
            logdir = os.path.join(
                self.home_directory,
                ''.join((
                    SUBFILE_DELIMITER * 3,
                    'logs',
                    SUBFILE_DELIMITER,
                    os.path.basename(self.home_directory))))
            if not os.path.exists(logdir):
                os.mkdir(logdir)
            self.dbenv.set_lg_dir(logdir)

        # To cope with log files created for in-memory databases, mainly when
        # running tests.  Deleted in tearDown() method of 'unittest' classes.
        else:
            logdir = ''.join((SUBFILE_DELIMITER * 3,
                              'memlogs',
                              SUBFILE_DELIMITER,
                              'memory_db'))
            if not os.path.exists(logdir):
                os.mkdir(logdir)
            self.dbenv.set_lg_dir(logdir)

        # Dated 26 April 2020.
        # Not sure what has changed, if anything, but if the maxlocks parameter
        # is not set self.dbenv.lock_stat()['maxlocks'] returns 0 on FreeBSD
        # and 1000 on OpenBSD.  For ChessTab databases keys can be deleted
        # within transactions on FreeBSD, but not necessarily on OpenBSD where
        # it is possible for a game record to be too big to be deleted.  A Game
        # record has around 4500 index references depending on the number of
        # moves played.
        # A game with no moves was deleted successfully on OpenBSD, but typical
        # games are too big.
        # Setting maxlocks to 0 (zero) leaves FreeBSD behaviour unchanged, but
        # stops OpenBSD opening any databases at all.
        # Setting maxlocks to 10000 on OpenBSD allows at least one Game record
        # to be deleted within a transaction.  However making the same setting
        # on FreeBSD might be applying an unnecessary upper bound on locks
        # assuming 0 (zero) means no limit.  A game with 138 moves was deleted,
        # which is about 3 times the average number of moves in a game.  The
        # FIDE longest possible game, at nearly 6000 moves, might be a problem.
        # maxlocks is added to the things taken from self.environment but is
        # only applied if non-zero and OS is OpenBSD.
        if _openbsd_platform:
            maxlocks = self.environment.get('maxlocks', 0)
            if maxlocks:
                self.dbenv.set_lk_max_locks(maxlocks)

        self.dbenv.open(self.home_directory, self.environment_flags(dbe))
        if files is None:
            files = self.specification.keys()
        self.start_transaction()
        self.table[CONTROL_FILE] = dbe.DB(self.dbenv)
        try:
            self.table[CONTROL_FILE].set_flags(dbe.DB_DUPSORT)
            self.table[CONTROL_FILE].open(
                self.file_name_for_database(CONTROL_FILE),
                dbname=CONTROL_FILE,
                dbtype=dbe.DB_BTREE,
                flags=db_create,
                txn=self.dbtxn)
        except:
            self.table[CONTROL_FILE] = None
            raise
        for file, specification in self.specification.items():
            if file not in files:
                continue
            fields = specification[SECONDARY]
            self.table[file] = [dbe.DB(self.dbenv)]
            try:
                self.table[file][0].open(
                    self.file_name_for_database(file),
                    dbname=file,
                    dbtype=dbe.DB_RECNO,
                    flags=db_create,
                    txn=self.dbtxn)
            except:
                self.table[file] = None
                raise
            self.ebm_control[file] = ExistenceBitmapControl(
                file, self, dbe, db_create)
            segmentfile = SUBFILE_DELIMITER.join((file, SEGMENT_SUFFIX))
            self.segment_table[file] = dbe.DB(self.dbenv)
            try:
                self.segment_table[file].open(
                    self.file_name_for_database(segmentfile),
                    dbname=segmentfile,
                    dbtype=dbe.DB_RECNO,
                    flags=db_create,
                    txn=self.dbtxn)
            except:
                self.segment_table[file] = None
                raise
            fieldprops = specification[FIELDS]
            for field, fieldname in fields.items():
                if fieldname is None:
                    fieldname = filespec.FileSpec.field_name(field)
                if fieldprops[fieldname] is None:
                    access_method = dbe.DB_BTREE
                elif ACCESS_METHOD in fieldprops[fieldname]:
                    if fieldprops[fieldname][ACCESS_METHOD] == HASH:
                        access_method = dbe.DB_HASH
                    else:
                        access_method = dbe.DB_BTREE
                else:
                    access_method = dbe.DB_BTREE
                secondary = SUBFILE_DELIMITER.join((file, field))
                self.table[secondary] = [dbe.DB(self.dbenv)]
                try:
                    self.table[secondary][0].set_flags(dbe.DB_DUPSORT)
                    self.table[secondary][0].open(
                        self.file_name_for_database(secondary),
                        dbname=secondary,
                        dbtype=access_method,
                        flags=db_create,
                        txn=self.dbtxn)
                except dbe.DBInvalidArgError as exc:
                    if not str(exc) == secondary.join(
                        ("(22, 'Invalid argument -- ",
                         ": unexpected file type or format')")):
                        raise
                    if access_method is not dbe.DB_HASH:
                        raise

                    # Accept existing DB_BTREE database if DB_HASH was in the
                    # supplied specification for database.
                    self.table[secondary] = [dbe.DB(self.dbenv)]
                    try:
                        self.table[secondary][0].set_flags(dbe.DB_DUPSORT)
                        self.table[secondary][0].open(
                            self.file_name_for_database(secondary),
                            dbname=secondary,
                            dbtype=dbe.DB_BTREE,
                            #flags=db_create,
                            txn=self.dbtxn)
                    except:
                        self.table[secondary] = None
                        raise

                except:
                    self.table[secondary] = None
                    raise
        if db_create:# and files:
            self.table[CONTROL_FILE].put(SPECIFICATION_KEY,
                                         repr(self.specification).encode(),
                                         txn=self.dbtxn)
            self.table[CONTROL_FILE].put(SEGMENT_SIZE_BYTES_KEY,
                                         repr(self.segment_size_bytes).encode(),
                                         txn=self.dbtxn)
        self.commit()
        self._dbe = dbe

    def environment_flags(self, dbe):
        return (dbe.DB_CREATE |
                dbe.DB_RECOVER |
                dbe.DB_INIT_MPOOL |
                dbe.DB_INIT_LOCK |
                dbe.DB_INIT_LOG |
                dbe.DB_INIT_TXN |
                dbe.DB_PRIVATE)

    def checkpoint_before_close_dbenv(self):

        # Rely on environment_flags() call for transaction state.
        self.dbenv.txn_checkpoint()

    def close_database_contexts(self, files=None):
        """Close files in database.

        Provided for compatibility with the DPT interface where there is a real
        difference between close_database_contexts() and close_database().

        In Berkeley DB most of the implementation detail is handled by cursors
        created on DB objects bound to elements of dictionaries such as table.

        The files argument is ignored because the DBEnv object is deleted and
        all the DB objects were created in the context of the DBEnv object.

        """
        self._dbe = None
        for file, specification in self.specification.items():
            if file in self.table:
                if self.table[file] is not None:
                    for f in self.table[file]:
                        f.close()
                    self.table[file] = None
            if file in self.segment_table:
                if self.segment_table[file] is not None:
                    self.segment_table[file].close()
                    self.segment_table[file] = None
            if file in self.ebm_control:
                if self.ebm_control[file] is not None:
                    self.ebm_control[file].close()
                    self.ebm_control[file] = None
                self.ebm_control[file] = None
            for field in specification[SECONDARY]:
                secondary = SUBFILE_DELIMITER.join((file, field))
                if secondary in self.table:
                    for f in self.table[secondary]:
                        f.close()
                    self.table[secondary] = None
        for k, v in self.table.items():
            if v is not None:
                v.close()
                self.table[k] = None
        if self.dbenv is not None:
            self.checkpoint_before_close_dbenv()
            self.dbenv.close()
            self.dbenv = None
            self.table = {}
            self.segment_table = {}
            self.ebm_control = {}
            self.ebm_segment_count = {}
        self.segment_size_bytes = self._initial_segment_size_bytes

    def close_database(self):
        """Close primary and secondary databases and environment.

        That means close all DB objects for tables and indicies and the DBEnv
        object defining the environment, and clear all dictionaries and the
        environment references.

        """
        self.close_database_contexts()

    def put(self, file, key, value):
        assert file in self.specification
        if key is None:
            return self.table[file][0].append(value.encode(), txn=self.dbtxn)
        else:
            self.table[file][0].put(key, value.encode(), txn=self.dbtxn)
            return None

    def replace(self, file, key, oldvalue, newvalue):
        assert file in self.specification
        self.table[file][0].put(key, newvalue.encode(), txn=self.dbtxn)

    def delete(self, file, key, value):
        assert file in self.specification
        try:
            self.table[file][0].delete(key, txn=self.dbtxn)
        except:
            pass

    def get_primary_record(self, file, key):
        """Return primary record (key, value) given primary key on dbset."""
        assert file in self.specification
        if key is None:
            return None
        record = self.table[file][0].get(key, txn=self.dbtxn)
        if record is None:
            return None
        return key, record.decode()

    def encode_record_number(self, key):
        """Return repr(key).encode() because this is bsddb(3) version.

        Typically used to convert primary key, a record number, to secondary
        index format.
        
        """
        return repr(key).encode()

    def decode_record_number(self, skey):
        """Return literal_eval(skey.decode()) because this is bsddb(3) version.

        Typically used to convert secondary index reference to primary record,
        a str(int), to a record number.

        """
        return literal_eval(skey.decode())

    def encode_record_selector(self, key):
        """Return key.encode() because this is bsddb(3) version.

        Typically used to convert a key being used to search a secondary index
        to the form held on the database.
        
        """
        return key.encode()

    def get_lowest_freed_record_number(self, dbset):
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            ebmc.freed_record_number_pages = []
            cursor = self.table[CONTROL_FILE].cursor(txn=self.dbtxn)
            try:
                record = cursor.set(ebmc.ebmkey)
                while record:
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder='big'))
                    record = cursor.next_dup()
            finally:
                cursor.close()
        while len(ebmc.freed_record_number_pages):
            s = ebmc.freed_record_number_pages[0]

            # Do not reuse record number on segment of high record number.
            if s == ebmc.segment_count - 1:
                return None

            lfrns = ebmc.read_exists_segment(s, self.dbtxn)
            if lfrns is None:

                # Segment does not exist now.
                ebmc.freed_record_number_pages.remove(s)
                continue

            try:
                first_zero_bit = lfrns.index(False, 0 if s else 1)
            except ValueError:
                cursor = self.table[CONTROL_FILE].cursor(txn=self.dbtxn)
                try:
                    if cursor.set_both(
                        ebmc.ebmkey,
                        s.to_bytes(1 + s.bit_length() // 8, byteorder='big')):
                        cursor.delete()
                    else:
                        raise
                finally:
                    cursor.close()
                del ebmc.freed_record_number_pages[0]
                continue
            return s * SegmentSize.db_segment_size + first_zero_bit
        else:
            return None

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
            ebmc.freed_record_number_pages = []
            cursor = self.table[CONTROL_FILE].cursor(txn=self.dbtxn)
            try:
                record = cursor.set(ebmc.ebmkey)
                while record:
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder='big'))
                    record = cursor.next_dup()
            finally:
                cursor.close()
        insert = bisect.bisect_left(ebmc.freed_record_number_pages, segment)

        # Should be:
        # if insert <= len(ebmc.freed_record_number_pages):
        # Leave as it is until dbapi tests give same results as sqlite3 tests,
        # which have the same problem.
        if ebmc.freed_record_number_pages:
            if insert < len(ebmc.freed_record_number_pages):
                if ebmc.freed_record_number_pages[insert] == segment:
                    return

        ebmc.freed_record_number_pages.insert(insert, segment)
        self.table[CONTROL_FILE].put(
            ebmc.ebmkey,
            segment.to_bytes(1 + segment.bit_length() // 8, byteorder='big'),
            flags=self._dbe.DB_NODUPDATA,
            txn=self.dbtxn)

    def remove_record_from_ebm(self, file, deletekey):
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        ebmb = self.ebm_control[file].ebm_table.get(segment + 1, txn=self.dbtxn)
        if ebmb is None:
            raise DatabaseError('Existence bit map for segment does not exist')
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
            ebm[record_number] = False
            self.ebm_control[file].ebm_table.put(
                segment + 1, ebm.tobytes(), txn=self.dbtxn)
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        ebmb = self.ebm_control[file].ebm_table.get(segment + 1, txn=self.dbtxn)
        if ebmb is None:
            ebm = SegmentSize.empty_bitarray.copy()
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
        ebm[record_number] = True
        self.ebm_control[file].ebm_table.put(
            segment + 1, ebm.tobytes(), txn=self.dbtxn)
        return segment, record_number

    # Change to return just the record number, and the name to fit.
    # Only used in one place, and it is extra work to get the data in_nosql.
    def get_high_record(self, file):
        c = self.table[file][0].cursor(txn=self.dbtxn)
        try:
            return c.last()
        finally:
            c.close()
    
    def _get_segment_record_numbers(self, file, reference):
        bs = self.segment_table[file].get(reference, txn=self.dbtxn)
        if len(bs) < SegmentSize.db_segment_size_bytes:
            return [int.from_bytes(bs[i:i+2], byteorder='big')
                    for i in range(0, len(bs), 2)]
        recnums = Bitarray()
        recnums.frombytes(bs)
        return recnums
    
    def add_record_to_field_value(
        self, file, field, key, segment, record_number):
        key = self.encode_record_selector(key)
        secondary = SUBFILE_DELIMITER.join((file, field))
        cursor = self.table[secondary][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.set_range(key)
            while r:
                k, v = r
                if k != key:

                    # No index entry for key.
                    cursor.put(
                        key,
                        b''.join(
                            (segment.to_bytes(4, byteorder='big'),
                             record_number.to_bytes(2, byteorder='big'))),
                        self._dbe.DB_KEYLAST)
                    return

                sr = int.from_bytes(v[:4], byteorder='big')
                if sr < segment:
                    r = cursor.next_dup()
                    continue
                elif sr > segment:

                    # No index entry for key in this segment.
                    cursor.put(
                        k,
                        b''.join(
                            (segment.to_bytes(4, byteorder='big'),
                             record_number.to_bytes(2, byteorder='big'))),
                        self._dbe.DB_KEYLAST)
                    return

                if len(v) == SEGMENT_HEADER_LENGTH:
                    rn = int.from_bytes(v[4:], byteorder='big')
                    if rn != record_number:
                        segnum = self.segment_table[file].append(
                            b''.join(
                                sorted([
                                    record_number.to_bytes(
                                        length=2, byteorder='big'),
                                    rn.to_bytes(length=2, byteorder='big')])),
                            txn=self.dbtxn)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 b'\x00\x02',
                                 segnum.to_bytes(4, byteorder='big'),
                                 )),
                            self._dbe.DB_KEYLAST)
                    return
                segnum = int.from_bytes(v[SEGMENT_HEADER_LENGTH:],
                                        byteorder='big')
                recnums = self._get_segment_record_numbers(file, segnum)
                if isinstance(recnums, list):
                    ip = bisect.bisect_left(recnums, record_number)
                    if ip < len(recnums):
                        if recnums[ip] != record_number:
                            recnums.insert(ip, record_number)
                    else:
                        recnums.append(record_number)
                    count = len(recnums)
                    if count > SegmentSize.db_upper_conversion_limit:
                        seg = SegmentSize.empty_bitarray.copy()
                        for rn in recnums:
                            seg[rn] = True
                        self.segment_table[file].put(
                            segnum, seg.tobytes(), txn=self.dbtxn)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(2, byteorder='big'),
                                 v[SEGMENT_HEADER_LENGTH:])),
                            self._dbe.DB_KEYLAST)
                    else:
                        self.segment_table[file].put(
                            segnum,
                            b''.join((
                                rn.to_bytes(length=2, byteorder='big')
                                for rn in recnums)),
                            txn=self.dbtxn)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(2, byteorder='big'),
                                 v[SEGMENT_HEADER_LENGTH:])),
                            self._dbe.DB_KEYLAST)
                    return

                # ignore possibility record_number already present
                recnums[record_number] = True
                self.segment_table[file].put(
                    segnum,
                    recnums.tobytes(),
                    txn=self.dbtxn)
                cursor.delete()
                cursor.put(
                    k,
                    b''.join(
                        (v[:4],
                         recnums.count().to_bytes(2, byteorder='big'),
                         v[SEGMENT_HEADER_LENGTH:])),
                    self._dbe.DB_KEYLAST)
                return

            else:

                # No index entry for key because database is empty.
                cursor.put(
                    key,
                    b''.join(
                        (segment.to_bytes(4, byteorder='big'),
                         record_number.to_bytes(2, byteorder='big'))),
                    self._dbe.DB_KEYLAST)

        finally:
            cursor.close()

    def remove_record_from_field_value(
        self, file, field, key, segment, record_number):
        key = self.encode_record_selector(key)
        secondary = SUBFILE_DELIMITER.join((file, field))
        cursor = self.table[secondary][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.set_range(key)
            while r:
                k, v = r
                if k != key:

                    # Assume that multiple requests to delete an index value
                    # have been made for a record.  The segment_put method uses
                    # sets to avoid adding multiple entries.  Consider using
                    # set rather than list in the pack method of the subclass
                    # of Value if this will happen a lot.
                    return

                sr = int.from_bytes(v[:4], byteorder='big')
                if sr < segment:
                    r = cursor.next_dup()
                    continue
                elif sr > segment:
                    return
                if len(v) == SEGMENT_HEADER_LENGTH:
                    if record_number == int.from_bytes(v[4:], byteorder='big'):
                        cursor.delete()
                    return
                segnum = int.from_bytes(v[SEGMENT_HEADER_LENGTH:],
                                        byteorder='big')
                recnums = self._get_segment_record_numbers(file, segnum)
                if isinstance(recnums, list):
                    discard = bisect.bisect_left(recnums, record_number)
                    if recnums[discard] == record_number:
                        del recnums[discard]
                    count = len(recnums)
                    if count < 2:
                        for rn in recnums:
                            ref = b''.join(
                                (segment.to_bytes(4, byteorder='big'),
                                 rn.to_bytes(2, byteorder='big')))
                        self.segment_table[file].delete(segnum, txn=self.dbtxn)
                        cursor.delete()
                        if count:
                            cursor.put(k, ref, self._dbe.DB_KEYLAST)
                    else:
                        self.segment_table[file].put(
                            segnum,
                            b''.join((
                                rn.to_bytes(length=2, byteorder='big')
                                for rn in sorted(recnums))),
                            txn=self.dbtxn)
                        cursor.delete()
                        cursor.put(
                            k,
                            b''.join(
                                (v[:4],
                                 count.to_bytes(2, byteorder='big'),
                                 v[SEGMENT_HEADER_LENGTH:])),
                            self._dbe.DB_KEYLAST)
                    return

                # ignore possibility record_number already absent
                recnums[record_number] = False

                count = recnums.count()
                if count > SegmentSize.db_lower_conversion_limit:
                    self.segment_table[file].put(
                        segnum, recnums.tobytes(), txn=self.dbtxn)
                    cursor.delete()
                    cursor.put(
                        k,
                        b''.join(
                            (v[:4],
                             recnums.count().to_bytes(2, byteorder='big'),
                             v[SEGMENT_HEADER_LENGTH:])),
                        self._dbe.DB_KEYLAST)
                else:
                    recnums = {rn for rn in recnums.search(SINGLEBIT)}
                    self.segment_table[file].put(
                        segnum,
                        b''.join((
                            rn.to_bytes(length=2, byteorder='big')
                            for rn in sorted(recnums))),
                        txn=self.dbtxn)
                    cursor.delete()
                    cursor.put(
                        k,
                        b''.join(
                            (v[:4],
                             len(recnums).to_bytes(2, byteorder='big'),
                             v[SEGMENT_HEADER_LENGTH:])),
                        self._dbe.DB_KEYLAST)
                return
        finally:
            cursor.close()

    def populate_segment(self, segment_reference, file):
        if len(segment_reference) == SEGMENT_HEADER_LENGTH:
            return RecordsetSegmentInt(
                int.from_bytes(segment_reference[:4], byteorder='big'),
                None,
                records=segment_reference[4:])
        bs = self.segment_table[file].get(
            int.from_bytes(segment_reference[SEGMENT_HEADER_LENGTH:],
                           byteorder='big'))
        if bs is None:
            raise DatabaseError('Segment record missing')
        if len(bs) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                int.from_bytes(segment_reference[:4], byteorder='big'),
                None,
                records=bs)
        return RecordsetSegmentList(
            int.from_bytes(segment_reference[:4], byteorder='big'),
            None,
            records=bs)

    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        cursor = self.table[SUBFILE_DELIMITER.join((file, valuespec.field))][
            0].cursor(txn=self.dbtxn)
        try:
            if valuespec.above_value and valuespec.below_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.above_value and valuespec.to_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value and valuespec.to_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value and valuespec.below_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.above_value:
                r = cursor.set_range(valuespec.above_value.encode())
                if r:
                    if r[0] == valuespec.above_value.encode():
                        r = cursor.next_nodup()
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.from_value:
                r = cursor.set_range(valuespec.from_value.encode())
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.to_value:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if k > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            elif valuespec.below_value:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if k >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
            else:
                r = cursor.first()
                while r:
                    k = r[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(k):
                        yield k
                    r = cursor.next_nodup()
        finally:
            cursor.close()

    # The bit setting in existence bit map decides if a record is put on the
    # recordset created by the make_recordset_*() methods.

    # Look at ebm_control.ebm_table even though the additional 'rn in r'
    # clause when populating the recordset makes table access cheaper.
    def recordlist_record_number(self, file, key=None, cache_size=1):
        """Return RecordList on file containing records for key."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return rs
        s, rn = divmod(key, SegmentSize.db_segment_size)
        r = self.ebm_control[file].ebm_table.get(s + 1, txn=self.dbtxn)
        if r and rn in RecordsetSegmentBitarray(s, key, records=r):
            rs[s] = RecordsetSegmentList(
                s, None, records=rn.to_bytes(2, byteorder='big'))
        return rs

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1):
        """Return RecordList on file containing record numbers whose record
        exists in record number range."""

        # The keys in self.ebm_control[file].ebm_table are always
        # 'segment + 1', see note in recordlist_ebm method.
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            segment_start, recnum_start = 0, 1
        else:
            segment_start, recnum_start = divmod(keystart,
                                                 SegmentSize.db_segment_size)
        if keyend is not None:
            segment_end, recnum_end = divmod(keyend,
                                             SegmentSize.db_segment_size)
        else:
            segment_end, recnum_end = None, None
        cursor = self.ebm_control[file].ebm_table.cursor(txn=self.dbtxn)
        try:
            so = None
            eo = None
            r = cursor.set(segment_start + 1)
            while r:
                s, b = r
                s -= 1
                if s < segment_start:
                    r = cursor.next()
                    continue
                if segment_end is not None and s > segment_end:
                    r = cursor.next()
                    continue
                if s == segment_start:
                    if (s and recnum_start) or recnum_start > 1:
                        so, sb = divmod(recnum_start, 8)
                        b = b'\x00' * so + b[so:]
                if keyend is not None:
                    if s > segment_end:
                        break
                    if (s == segment_end and
                        recnum_start < SegmentSize.db_segment_size - 1):
                        eo, eb = divmod(recnum_end, 8)
                        b = (b[:eo+1] +
                             b'\x00' * (
                                 SegmentSize.db_segment_size_bytes - eo - 1))
                rs[s] = RecordsetSegmentBitarray(s, None, records=b)
                r = cursor.next()
            if so is not None:
                for i in range(so * 8, so * 8 + sb):
                    rs[segment_start][(segment_start, i)] = False
            if eo is not None:
                for i in range(eo * 8 + eb, (eo + 1) * 8):
                    rs[segment_end][(segment_end, i)] = False
        finally:
            cursor.close()
        return rs
    
    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList on file containing record numbers whose record
        exists."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        cursor = self.ebm_control[file].ebm_table.cursor(txn=self.dbtxn)
        try:
            r = cursor.first()
            while r:

                # The keys in self.ebm_control[file].ebm_table are always
                # 'segment + 1' because automatically allocated RECNO keys
                # start at 1 in an empty table and segment numbers start at 0.
                # It is not possible to use the actual segment number because
                # 0 is not allowed as a RECNO key.
                rs[r[0] - 1] = RecordsetSegmentBitarray(
                    r[0] - 1, None, records=r[1])
                r = cursor.next()

        finally:
            cursor.close()
        return rs

    def populate_recordset_segment(self, recordset, reference):
        s = int.from_bytes(reference[:4], byteorder='big')
        if len(reference) == SEGMENT_HEADER_LENGTH:
            segment = RecordsetSegmentInt(
                s,
                None,
                records=reference[4:])
        else:
            bs = self.segment_table[recordset.dbset].get(
                int.from_bytes(reference[SEGMENT_HEADER_LENGTH:],
                               byteorder='big'))
            if len(bs) == SegmentSize.db_segment_size_bytes:
                segment = RecordsetSegmentBitarray(
                    s, None, records=bs)
            else:
                segment = RecordsetSegmentList(
                    s, None, records=bs)
        if s not in recordset:
            recordset[s] = segment
        else:
            recordset[s] |= segment

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return RecordList on file containing database records for field
        with keys like key."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keylike is None:
            return rs
        pattern = b'.*?' + keylike
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.first()
            while r:
                k, v = r
                if re.match(pattern, k, flags=re.IGNORECASE|re.DOTALL):
                    self.populate_recordset_segment(rs, v)
                r = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.set_range(key)
            while r:
                k, v = r
                if k != key:
                    break
                self.populate_recordset_segment(rs, v)
                r = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1):
        """Return RecordList on file containing records for field with
        keys starting key.
        """
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keystart is None:
            return rs
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.set_range(keystart)
            while r:
                if not r[0].startswith(keystart):
                    break
                self.populate_recordset_segment(rs, r[1])
                r = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1):
        """Return RecordList on file containing records for field with
        keys in range set by combinations of ge, gt, le, and lt.
        """
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        elif le and lt:
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            if ge is None and gt is None:
                r = cursor.first()
            else:
                r = cursor.set_range(ge or gt)
            if gt:
                while r:
                    if r[0] > gt:
                        break
                    r = cursor.next()
            if le is None and lt is None:
                while r:
                    self.populate_recordset_segment(rs, r[1])
                    r = cursor.next()
            elif lt is None:
                while r:
                    if r[0] > le:
                        break
                    self.populate_recordset_segment(rs, r[1])
                    r = cursor.next()
            else:
                while r:
                    if r[0] >= lt:
                        break
                    self.populate_recordset_segment(rs, r[1])
                    r = cursor.next()
        finally:
            cursor.close()
        return rs

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        rs = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            r = cursor.first()
            while r:
                self.populate_recordset_segment(rs, r[1])
                r = cursor.next()
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
        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:

            # Delete segment records.
            r = cursor.set_range(key)
            while r:
                k, v = r
                if k != key:
                    break
                if len(v) > SEGMENT_HEADER_LENGTH:
                    self.segment_table[file].delete(
                        int.from_bytes(v[SEGMENT_HEADER_LENGTH:],
                                       byteorder='big'),
                        txn=self.dbtxn)

                # Delete segment references.
                #cursor.delete()

                r = cursor.next()

            # Delete segment references.
            #try:
            #    self.table[SUBFILE_DELIMITER.join((file, field))
            #               ][0].delete(key, txn=self.dbtxn)
            #except self._dbe.DBNotFoundError:
            #    pass

        finally:
            cursor.close()

        # Delete segment references.
        # The commented delete methods, cursor and database, within preceding
        # try ... finally ... attract exceptions when deleting a partial
        # position query from the database while that query is displayed
        # (by F11 for example).
        # cursor gets 'BDB0097 Transaction not specified for a transactional
        # database'.
        # database gets 'BDB0087 DB_RUNRECOVERY: Fatal error, run database
        # recovery -- BDB0060 PANIC: fatal region error detected; run recovery'
        # In both cases recovery run when starting application normally seems
        # to leave things in good order, with the record deleted.
        # In neither case is an exception generated when the deleted record is
        # not displayed, and displaying a different record does not result in
        # an exception.
        #
        try:
            self.table[SUBFILE_DELIMITER.join((file, field))
                       ][0].delete(key, txn=self.dbtxn)
        except self._dbe.DBNotFoundError:
            pass
    
    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records."""
        assert recordset.dbset == file
        assert file in self.table

        # Delete existing segments for key
        self.unfile_records_under(file, field, key)

        cursor = self.table[SUBFILE_DELIMITER.join((file, field))
                            ][0].cursor(txn=self.dbtxn)
        try:
            recordset.normalize()
            for sn in recordset.sorted_segnums:
                if isinstance(recordset.rs_segments[sn], RecordsetSegmentInt):
                    cursor.put(
                        key,
                        b''.join(
                            (sn.to_bytes(4, byteorder='big'),
                             recordset.rs_segments[sn].tobytes(),
                             )),
                        self._dbe.DB_KEYLAST)
                else:
                    count = recordset.rs_segments[sn].count_records()
                    sk = self.segment_table[file].append(
                        recordset.rs_segments[sn].tobytes(), txn=self.dbtxn)
                    cursor.put(
                        key,
                        b''.join(
                            (sn.to_bytes(4, byteorder='big'),
                             count.to_bytes(2, byteorder='big'),
                             sk.to_bytes(4, byteorder='big'))),
                        self._dbe.DB_KEYLAST)
        finally:
            cursor.close()

    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on DB() for (file, field).
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        assert file in self.specification
        if file == field:
            return CursorPrimary(self.table[file][0],
                                 keyrange=keyrange,
                                 transaction=self.dbtxn,
                                 ebm=self.ebm_control[file].ebm_table,
                                 engine=self._dbe)
        return CursorSecondary(
            self.table[SUBFILE_DELIMITER.join((file, field))][0],
            keyrange=keyrange,
            transaction=self.dbtxn,
            segment=self.segment_table[file])

    def create_recordset_cursor(self, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursor(recordset,
                               transaction=self.dbtxn,
                               database=self.table[recordset.dbset][0])

    # Comment in chess_ui for make_position_analysis_data_source method, only
    # call, suggests is_database_file_active should not be needed.
    def is_database_file_active(self, file):
        """Return True if the DB object for file exists."""
        return self.table[file][0] is not None
    
    def get_table_connection(self, file):
        """Return main DB object for file."""
        if self.dbenv:
            return self.table[file][0]

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs={},
        use_specification_items=None,
        ):
        """Run taskmethod to perform database task.

        This method is structured to be compatible with the requirements of
        the sqlite3 version which is intended for use in a separate thread and
        must open a separate connection to the database.  Such action seems to
        be unnecessary in Berkeley DB so far.

        This method assumes usage like:

        class _ED(_db.Database):
            def open_database(self, **k):
                try:
                    super().open_database(bsddb3.db, **k)
                except self.__class__.SegmentSizeError:
                    super().open_database(bsddb3.db, **k)
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
                super().open_database(bsddb3.db, **k)
        class _AD(_ED):
            def __init__(self, folder, **k):
                super().__init__({}, folder, **k)

        """
        #taskmethod(self, logwidget, **taskmethodargs)
        db = self.__class__(
            self.home_directory,
            use_specification_items=use_specification_items)
        db.open_database()
        try:
            taskmethod(db, logwidget, **taskmethodargs)
        finally:
            db.close_database()


class Cursor(cursor.Cursor):
    
    """Define a cursor on the underlying database engine dbset.

    dbset - bsddb3 DB() object.
    keyrange - not used.
    transaction - the current transaction.
    kargs - absorb argunents relevant to other database engines.

    The wrapped cursor is created on the Berkeley DB database in a File
    instance.

    The transaction argument in the Cursor() call should be a function
    which returns current tranasction active on the Berkeley DB environment, or
    None if there isn't one.  If supplied it's return value is used in all calls
    to methods of the wrapped cursor which have the 'txn' parameter.  By default
    the calls are not within a transaction.

    The CursorPrimary and CursorSecondary subclasses define the bsddb3
    style cursor methods peculiar to primary and secondary databases.
    """

    def __init__(self, dbset, keyrange=None, transaction=None, **kargs):
        """Define a cursor on the underlying database engine dbset."""
        super().__init__(dbset)
        self._transaction = transaction
        self._cursor = dbset.cursor(txn=transaction)
        self._current_segment = None
        self._current_segment_number = None
        self._current_record_number_in_segment = None

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial.encode()

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return self._partial.encode()

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in Berkeley DB.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """
        pass


class CursorPrimary(Cursor):
    
    """Define a cursor on the underlying database engine dbset.

    dbset - bsddb3 DB() object.
    ebm - bsddb3 DB() object for existence bitmap.
    engine - bsddb3.db module.  Only the DB_FAST_STAT flag is used at present.
    kargs - superclass arguments and absorb arguments for other engines.
    
    """

    def __init__(self, dbset, ebm=None, engine=None, **kargs):
        super().__init__(dbset, **kargs)
        self._ebm = ebm
        self._engine = engine

    def count_records(self):
        """Return record count."""
        return self._dbset.stat(flags=self._engine.DB_FAST_STAT,
                                txn=self._transaction)['ndata']

    def first(self):
        """Return first record taking partial key into account."""
        return self._decode_record(self._cursor.first())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        segment_number, record_number = divmod(record[0],
                                        SegmentSize.db_segment_size)
        segment = self._ebm.get(segment_number + 1, txn=self._transaction)
        if segment is None:
            return 0
        position = 0
        for i in range(segment_number):
            sebm = Bitarray()
            sebm.frombytes(self._ebm.get(i + 1, txn=self._transaction))
            position += sebm.count()
        sebm = Bitarray()
        sebm.frombytes(segment)
        try:
            position += sebm.search(SINGLEBIT).index(record_number)
        except ValueError:
            position += bisect.bisect_left(
                record_number, sebm.search(SINGLEBIT))
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None
        count = 0
        abspos = abs(position)
        ebdc = self._ebm.cursor(txn=self._transaction)
        try:
            if position < 0:
                r = ebdc.last()
                while r:
                    sebm = Bitarray()
                    sebm.frombytes(r[1])
                    sc = sebm.count()
                    if count + sc < abspos:
                        count += sc
                        r = ebdc.prev()
                        continue
                    recno = sebm.search(SINGLEBIT)[position + count] + (
                        (r[0] - 1) * SegmentSize.db_segment_size)
                    ebdc.close()
                    return self._decode_record(self._cursor.set(recno))
            else:
                r = ebdc.first()
                while r:
                    sebm = Bitarray()
                    sebm.frombytes(r[1])
                    sc = sebm.count()
                    if count + sc < abspos:
                        count += sc
                        r = ebdc.next()
                        continue
                    recno = sebm.search(SINGLEBIT)[position - count] + (
                        (r[0] - 1) * SegmentSize.db_segment_size)
                    ebdc.close()
                    return self._decode_record(self._cursor.set(recno))
        finally:
            ebdc.close()
        return None

    def last(self):
        """Return last record taking partial key into account."""
        return self._decode_record(self._cursor.last())

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        return self._decode_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account."""
        return self._decode_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account."""
        return self._decode_record(self._cursor.prev())

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.
        
        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        return self._decode_record(self._cursor.set(record[0]))

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            k, v = record
            return k, v.decode()
        except:
            if record is None:
                return record
            raise

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DatabaseError('_get_record not implemented')

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        #raise DatabaseError('refresh_recordset not implemented')


class CursorSecondary(Cursor):
    
    """Define a cursor on the underlying database engine dbset.

    dbset - bsddb3 DB() object.
    segment - bsddb3 DB() object for segment, list of record numbers or bitmap.
    kargs - superclass arguments and absorb arguments for other engines.
    
    """

    def __init__(self, dbset, segment=None, **kargs):
        super().__init__(dbset, **kargs)
        self._segment = segment

    def count_records(self):
        """Return record count."""
        if self.get_partial() in (None, False):
            count = 0
            r = self._cursor.first()
            while r:
                if len(r[1]) > SEGMENT_HEADER_LENGTH:
                    count += int.from_bytes(r[1][4:6], byteorder='big')
                else:
                    count += 1
                r = self._cursor.next()
            return count
        else:
            count = 0
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
            while r:
                if not r[0].startswith(self.get_converted_partial()):
                    break
                if len(r[1]) > SEGMENT_HEADER_LENGTH:
                    count += int.from_bytes(r[1][4:6], byteorder='big')
                else:
                    count += 1
                r = self._cursor.next()
            return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            try:
                k, v = self._first()
            except TypeError:
                return None
            return k.decode(), v
        elif self.get_partial() is False:
            return None
        else:
            record = self.nearest(self.get_converted_partial())
            if record is not None:
                if not record[0].startswith(self.get_partial()):
                    return None
            return record

    def _get_segment(self, key, segment_number, reference):
        if len(reference) == SEGMENT_HEADER_LENGTH:
            return RecordsetSegmentInt(
                segment_number,
                key,
                records=reference[4:])
        if self._current_segment_number == segment_number:
            if key == self._current_segment._key:
                return self._current_segment
        bs = self._segment.get(
            int.from_bytes(reference[SEGMENT_HEADER_LENGTH:],
                           byteorder='big'),
            txn=self._transaction)
        if len(bs) < SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentList(
                segment_number,
                key,
                records=bs)
        return RecordsetSegmentBitarray(
            segment_number,
            key,
            records=bs)

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(value,
                                               SegmentSize.db_segment_size)

        # Define lambdas to handle presence or absence of partial key
        low = lambda rk, recordkey: rk < recordkey
        if not self.get_partial():
            high = lambda rk, recordkey: rk > recordkey
        else:
            high = lambda rk, partial: not rk.startswith(partial)

        # Get position of record relative to start point
        position = 0
        if not self.get_partial():
            r = self._cursor.first()
        else:
            r = self._cursor.set_range(
                self.get_converted_partial_with_wildcard())
        while r:
            if low(r[0].decode(), key):
                if len(r[1]) > SEGMENT_HEADER_LENGTH:
                    position += int.from_bytes(r[1][4:6], byteorder='big')
                else:
                    position += 1
            elif high(r[0].decode(), key):
                break
            else:
                sr = int.from_bytes(r[1][:4], byteorder='big')
                if sr < segment_number:
                    if len(r[1]) > SEGMENT_HEADER_LENGTH:
                        position += int.from_bytes(r[1][4:6], byteorder='big')
                    else:
                        position += 1
                elif sr > segment_number:
                    break
                else:
                    position += self._get_segment(
                        key,
                        segment_number,
                        r[1]).get_position_of_record_number(record_number)
                    break
            r = self._cursor.next()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None

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
        r = start(get_partial())
        if position < 0:
            while r:
                if len(r[1]) > SEGMENT_HEADER_LENGTH:
                    r2 = int.from_bytes(r[1][4:6], byteorder='big')
                else:
                    r2 = 1
                count -= r2
                if count > position:
                    r = step()
                    continue
                record_number = self._get_segment(
                    r[0],
                    int.from_bytes(r[1][:4], byteorder='big'),
                    r[1]).get_record_number_at_position(position - count - r2)
                if record_number is not None:
                    return r[0].decode(), record_number
                break
        else:
            while r:
                if len(r[1]) > SEGMENT_HEADER_LENGTH:
                    r2 = int.from_bytes(r[1][4:6], byteorder='big')
                else:
                    r2 = 1
                count += r2
                if count <= position:
                    r = step()
                    continue
                record_number = self._get_segment(
                    r[0],
                    int.from_bytes(r[1][:4], byteorder='big'),
                    r[1]).get_record_number_at_position(position - count + r2)
                if record_number is not None:
                    return r[0].decode(), record_number
                break

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            try:
                k, v = self._last()
            except TypeError:
                return None
            return k.decode(), v
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
                        return k.decode(), v
                    continue
                self._set_range(''.join(c).encode())
                try:
                    k, v = self._prev()
                except TypeError:
                    return None
                return k.decode(), v

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if self.get_partial() is False:
            return None
        try:
            k, v = self._set_range(key)
        except TypeError:
            return None
        if self.get_partial() is not None:
            if not k.startswith(self.get_converted_partial()):
                return None
        return k.decode(), v

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
        return k.decode(), v

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
        return k.decode(), v

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
        key, value = record
        if self.get_partial() is not None:
            if not key.startswith(self.get_partial()):
                return None
        try:
            k, v = self._set_both(key.encode(), value)
        except TypeError:
            return None
        return k.decode(), v

    def set_partial_key(self, partial):
        """Set partial key and mark current segment as None."""
        self._partial = partial
        self._current_segment = None
        self._current_segment_number = None

    def _get_record(self, record):
        raise DatabaseError('_get_record not implemented')

    def set_current_segment(self, key, reference):
        """Return a RecordsetSegmentBitarray, RecordsetSegmentInt, or
        RecordsetSegmentList instance, depending on the current representation
        of the segment on the database."""
        segment_number = int.from_bytes(reference[:4], byteorder='big')
        if self._current_segment_number == segment_number:
            if key == self._current_segment._key:
                return self._current_segment
        segment = self._get_segment(key, segment_number, reference)
        self._current_segment = segment
        self._current_segment_number = segment_number
        return segment

    def _first(self):
        record = self._cursor.first()
        if record is None:
            return None
        return self.set_current_segment(*record).first()

    def _last(self):
        record = self._cursor.last()
        if record is None:
            return None
        return self.set_current_segment(*record).last()

    def _next(self):
        record = self._current_segment.next()
        if record is None:
            record = self._cursor.next()
            if record is None:
                return None
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).first()
        else:
            return record

    def _prev(self):
        record = self._current_segment.prev()
        if record is None:
            record = self._cursor.prev()
            if record is None:
                return None
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).last()
        else:
            return record

    def _set_both(self, key, value):
        segment, record_number = divmod(value, SegmentSize.db_segment_size)
        cursor = self._dbset.cursor(txn=self._transaction)
        try:
            record = cursor.set_range(key)
            while record:
                if record[0] != key:
                    return None
                segment_number = int.from_bytes(record[1][:4], byteorder='big')
                if segment_number > segment:
                    return None
                if segment_number == segment:
                    break
                record = cursor.next()
            else:
                return None
        finally:
            cursor.close()
        segment = self._get_segment(
            key,
            int.from_bytes(record[1][:4], byteorder='big'),
            record[1])
        if segment.setat(value) is None:
            return None
        record = self._cursor.set_both(key, record[1])
        if record is None:
            return None
        self._current_segment = segment
        self._current_segment_number = segment_number
        return key, value

    def _set_range(self, key):
        record = self._cursor.set_range(key)
        if record is None:
            self._current_segment = None
            self._current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        segment_number = int.from_bytes(record[1][:4], byteorder='big')
        segment = self._get_segment(record[0], segment_number, record[1])
        self._current_segment = segment
        self._current_segment_number = segment_number
        return segment.first()

    def _first_partial(self, partial):
        r = self._cursor.set_range(partial)
        if r is None:
            return None
        if not r[0].startswith(partial):
            return None
        return r

    def _last_partial(self, partial):
        k = partial.encode()
        record = self._cursor.set_range(k)
        while record is not None:
            if not record[0].startswith(k):
                break
            record = self._cursor.next_nodup()
        record = self._cursor.prev()
        if record is None:
            return None
        if record[0].startswith(k):
            return self.set_current_segment(*record).last()

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.
        # It seems not needed by this class.
        
        #raise DatabaseError('refresh_recordset not implemented')

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        r = self.nearest(key)
        if not r:
            return None
        if r[0].encode() != key:
            return None
        nr = self.next()
        if nr:
            if r[0] == nr[0]:
                raise DatabaseError('Index must refer to unique record')
        return r[1]


class RecordsetCursor(RecordsetCursor):
    
    """Add _get_record method and tranasction support to RecordsetCursor."""

    def __init__(self, recordset, transaction=None, database=None, **kargs):
        """Note database and transaction identity and delegate recordset to
        superclass.

        kargs absorbs arguments relevant to other database engines.

        """
        super().__init__(recordset)
        self._transaction = transaction
        self._database = database

    # The _get_record hack in sqlite3bitdatasource.py becomes the correct way
    # to do this because the record has bsddb-specific decoding needs.
    def _get_record(self, record_number, use_cache=False):
        """Return (record_number, record) using cache if requested."""
        dbset = self._dbset
        if use_cache:
            record = dbset.record_cache.get(record_number)
            if record is not None:
                return record # maybe (record_number, record)
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return # maybe raise
        try:
            record = self._database.get(record_number,
                                        txn=self._transaction).decode()
        except AttributeError:
            # Assume get() returned None.
            record = None
        # maybe raise if record is None (if not, None should go on cache)
        if use_cache:
            dbset.record_cache[record_number] = record
            dbset.record.deque.append(record_number)
        return (record_number, record)


class ExistenceBitmapControl(_database.ExistenceBitmapControl):
    
    """Access existence bit map for file in database.

    The dbe and create arguments are needed to open the Berkeley DB database
    which holds the existence bit map segment records.
    """

    def __init__(self, file, database, dbe, db_create):
        """Note file whose existence bitmap is managed.
        """
        super().__init__(file, database)
        self.ebm_table = dbe.DB(database.dbenv)
        try:
            self.ebm_table.set_re_pad(0)
            self.ebm_table.set_re_len(
                SegmentSize.db_segment_size_bytes)
            dbname = SUBFILE_DELIMITER.join((file, EXISTENCE_BITMAP_SUFFIX))
            self.ebm_table.open(
                database.file_name_for_database(dbname),
                dbname=dbname,
                dbtype=dbe.DB_RECNO,
                flags=db_create,
                txn=database.dbtxn)
            self._segment_count = self.ebm_table.stat(
                flags=dbe.DB_FAST_STAT, txn=database.dbtxn)['ndata']
        except:
            self.ebm_table = None
            raise

    def read_exists_segment(self, segment_number, dbtxn):
        # Return existence bit map for segment_number.
        # record keys are 1-based but segment_numbers are 0-based.
        ebm = Bitarray()
        ebm.frombytes(self.ebm_table.get(segment_number + 1, txn=dbtxn))
        return ebm

    def close(self):
        if self.ebm_table is not None:
            self.ebm_table.close()
            self.ebm_table = None
