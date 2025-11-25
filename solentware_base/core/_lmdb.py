# _lmdb.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Symas Lightning Memory-Mapped Database (LMMD) database."""
import os
from ast import literal_eval
import bisect
import re

from . import filespec
from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_SUFFIX,
    CONTROL_FILE,
    DESIGN_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    APPLICATION_CONTROL_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    SEGMENT_HEADER_LENGTH,
    DEFAULT_MAP_SIZE,
    DEFAULT_MAP_BLOCKS,
    DEFAULT_MAP_PAGES,
)
from . import _database
from .bytebit import Bitarray, SINGLEBIT
from .segmentsize import SegmentSize
from . import cursor as _cursor
from . import recordsetcursor
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordList,
    FoundSet,
)


class DatabaseError(_database.DatabaseError):
    """Exception for Database class."""


class Database(_database.Database):
    """Define file and record access methods."""

    class SegmentSizeError(Exception):
        """Raise when segment size in database is not in specification."""

    def __init__(
        self,
        specification,
        folder=None,
        environment=None,
        map_blocks=DEFAULT_MAP_BLOCKS,
        segment_size_bytes=DEFAULT_SEGMENT_SIZE_BYTES,
        use_specification_items=None,
        **soak
    ):
        """Initialize data structures."""
        del soak
        if folder is not None:
            try:
                path = os.path.abspath(folder)
            except Exception as exc:
                msg = " ".join(
                    ["Database folder name", str(folder), "is not valid"]
                )
                raise DatabaseError(msg) from exc
        else:
            path = None
        if not isinstance(specification, filespec.FileSpec):
            specification = filespec.FileSpec(
                use_specification_items=use_specification_items,
                **specification,
            )
        self._use_specification_items = use_specification_items
        if environment is None:
            environment = {}
        if not isinstance(environment, dict):
            raise DatabaseError("Database environment must be a dictionary")
        self._validate_segment_size_bytes(segment_size_bytes)
        # self.database_file is interpreted using the subdir argument in
        # lmdb.open() calls.
        if folder is not None:
            self.home_directory = path
            # Setting for subdir==True
            # self.database_file = path
            # Setting for subdir==False
            self.database_file = os.path.join(path, os.path.basename(path))
        else:
            self.home_directory = None
            self.database_file = None
        self.specification = specification
        self.environment = environment
        self.segment_size_bytes = segment_size_bytes
        self.map_blocks = map_blocks
        self.dbenv = None
        self.table = {}
        self.dbtxn = _DBtxn()
        self._dbe = None
        self.segment_table = {}
        self.ebm_control = {}

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
        """Start transaction if none and bind txn object to self._dbtxn.

        Start a transaction with both read and write allowed.  This action
        is equivalent to starting a transaction in other database engine
        interfaces such as berkeleydb and apsw.

        The arguments to the 'mdb_txn_begin' method with their
        Symas documentation equivalents appear to be:
        db
        parent
        write             MDB_RDONLY
        buffers

        """
        self.dbtxn.start_transaction(self.dbenv, True)

    def start_read_only_transaction(self):
        """Start transaction if none and bind txn object to self._dbtxn.

        Start a transaction with just read allowed.  This action is taken as
        equivalent to not starting a transaction in other database engine
        interfaces such as berkeleydb and apsw.  It is defined to allow a
        'do nothing' version to exist for those engines.

        """
        self.dbtxn.start_transaction(self.dbenv, False)

    def end_read_only_transaction(self):
        """Abort the active transaction and remove binding to txn object.

        This method is defined to allow a 'do nothing' version to exist for
        database engines which do not have explicit read-only transactions.

        """
        self.backout()

    def backout(self):
        """Abort the active transaction and remove binding to txn object."""
        txn = self.dbtxn
        # Some optimizations seem possible for read-only transactions which
        # do not involve destroying the self.dbtxn._transaction instance.
        # Symas LMMD documentation suggests this is useful if another
        # read-only transaction will be done soon.
        if txn.transaction is not None:
            txn.transaction.abort()
            txn.end_transaction()

    def commit(self):
        """Commit the active transaction and remove binding to txn object."""
        txn = self.dbtxn
        if txn.transaction is not None:
            txn.transaction.commit()
            txn.end_transaction()

    # ._db has file_name_for_database() returning a file, not database, name.
    def _encoded_database_name(self, database):
        """Return encoded database name."""
        return database.encode()

    def open_database(self, dbe, files=None):
        """Open DB environment and specified primary and secondary databases.

        By default all primary databases are opened, but just those named in
        files otherwise, along with their associated secondaries.

        dbe must be a Python module implementing the Symas LMMD API.

        The arguments to the 'mdb_dbi_open' method with their
        Symas documentation equivalents appear to be:
        key               Database name
        txn               Tranasaction object
        reverse_key       MDB_REVERSEKEY
        dupsort           MDB_DUPSORT
        create            MDB_CREATE
        integerkey        MDB_INTEGERKEY
        integerdup        MDB_INTEGERDUP MDB_DUPFIXED MDB_DUPSORT
        dupfixed          MDB_DUPFIXED MDB_DUPSORT

        It is not clear where MDB_REVERSEDUP fits.
        Nothing obvious in Issues at github.com/jnwatson/py-lmdb 2023-01-17.
        Assumed not supported.

        The main files, which are RECNO with keys encoded byteorder=="big"
        as values in secondary files on Berkeley DB, have integerkey=False
        with keys encoded byteorder=="big" in both main and secondary files
        on Symas LMMD.

        """
        # Memory databases are not supported by Symas LMMD: there must be a
        # database to be memory mapped.
        # Thus self.home_directory==None is allowed to generate an exception
        # unlike in the sibling _db, _sqlite, and _nosql, modules where it
        # actually or conventionally means memory-only database.
        try:
            os.mkdir(self.home_directory)
        except FileExistsError:
            if not os.path.isdir(self.home_directory):
                raise
        # The ___design table should be present already if the file exists.
        # Other database engines use the ___control table for this task.
        # The specification record datasize will usually exceed the default
        # compile-time limit for data in Symas LMMD dupsort databases.
        dbenv = None
        db_create = False
        # The create argument to lmdb.open does not correspond to any of
        # the Environment flags in Symas LMMD documentation.  Reading the
        # py-lmdb/cpython.c source code at github implies the create argument
        # controls whether a mkdir call is done if readonly==False and
        # subdir==True.  So accept the default create argument value because
        # subdir==False, which means only two argument cominations have to be
        # tried: readonly==True and readonly==False.
        # The OpenBSD port, as published on ports@openbsd.org mailing list
        # on 28 January 2023, does not set LMDB_FORCE_CFFI and gives the
        # first str(exc) below.
        # The FreeBSD port of py-lmdb is built with 'LMDB_FORCE_CFFI=1' and
        # this emerges as the second str(exc) below.
        # The OpenBSD development environment does not, but the FreeBSD
        # development environment must, have py-cffi installed.
        try:
            dbenv = dbe.Environment(
                path=self.database_file,
                map_size=DEFAULT_MAP_SIZE * self.map_blocks,
                readonly=True,
                max_dbs=self._calculate_max_dbs(files=files),
                **self.environment_flags(dbe),
            )
        except dbe.Error as exc:
            if str(exc) != ": ".join(
                (self.database_file, "No such file or directory")
            ) and str(exc) != "".join(
                (
                    "b'",
                    self.database_file,
                    "': b'No such file or directory'",
                )
            ):
                raise DatabaseError(str(exc)) from exc
            db_create = True
        if dbenv is None:
            dbenv = dbe.Environment(
                path=self.database_file,
                map_size=DEFAULT_MAP_SIZE * self.map_blocks,
                readonly=False,
                max_dbs=self._calculate_max_dbs(files=files),
                **self.environment_flags(dbe),
            )
        try:
            self.table[DESIGN_FILE] = _Datastore(
                self._encoded_database_name(DESIGN_FILE),
                dupsort=False,
                create=db_create,
            )
        except:
            self.table[DESIGN_FILE] = None
            raise
        try:
            self.table[CONTROL_FILE] = _Datastore(
                self._encoded_database_name(CONTROL_FILE),
                dupsort=True,
                create=db_create,
            )
        except:
            self.table[CONTROL_FILE] = None
            raise
        if files is None:
            files = self.specification.keys()
        for file, specification in self.specification.items():
            if file not in files:
                continue
            fields = specification[SECONDARY]
            self.table[file] = [None]
            self.table[file] = _Datastore(
                self._encoded_database_name(file),
                integerkey=False,
                create=db_create,
            )
            self.ebm_control[file] = ExistenceBitmapControl(
                file, self, dbe, db_create
            )
            segmentfile = SUBFILE_DELIMITER.join((file, SEGMENT_SUFFIX))
            try:
                self.segment_table[file] = _Datastore(
                    self._encoded_database_name(segmentfile),
                    integerkey=False,
                    create=db_create,
                )
            except:
                self.segment_table[file] = None
                raise
            for field, fieldname in fields.items():
                if fieldname is None:
                    fieldname = filespec.FileSpec.field_name(field)
                secondary = SUBFILE_DELIMITER.join((file, field))
                self.table[secondary] = [None]
                self.table[secondary] = _Datastore(
                    self._encoded_database_name(secondary),
                    dupsort=True,
                    create=db_create,
                )
        # If db_create==True there is no database version of the specification
        # to check the supplied specification against, so write it to the
        # database.
        self.dbenv = dbenv
        if db_create:  # and files:
            self.table[DESIGN_FILE].open_datastore(self.dbenv)
            self.start_transaction()
            txn = self.dbtxn.transaction
            # Using txn.put(<key>, <value>, db=<instance>) rather than a
            # cursor leads to a segmentation fault on the txn.put for
            # SEGMENT_SIZE_BYTES_KEY after success with SPECIFICATION_KEY.
            # The problem does not occur if the env.begin() and env.open_db()
            # calls are arranged so the open_db() call uses the txn argument.
            # It is usually not convenient to open and close the databases
            # for each transaction.
            cursor = txn.cursor(self.table[DESIGN_FILE].datastore)
            cursor.put(
                SPECIFICATION_KEY,
                repr(self.specification).encode(),
            )
            cursor.put(
                SEGMENT_SIZE_BYTES_KEY,
                repr(self.segment_size_bytes).encode(),
            )
            cursor.put(APPLICATION_CONTROL_KEY, repr({}).encode())
            cursor.close()
            self.table[DESIGN_FILE].close_datastore()
            self.open_database_contexts()
            self.close_database_context_files()
            self.commit()
            self.dbenv = None
        dbenv.close()
        dbenv = None
        # This open must not be readonly so both read-only and read-write
        # tranasctions can be done in the environment.
        self.dbenv = dbe.Environment(
            path=self.database_file,
            map_size=DEFAULT_MAP_SIZE * self.map_blocks,
            readonly=False,
            max_dbs=self._calculate_max_dbs(files=files),
            **self.environment_flags(dbe),
        )
        self.table[DESIGN_FILE].open_datastore(self.dbenv)
        self.start_read_only_transaction()
        cursor = self.dbtxn.transaction.cursor(
            self.table[DESIGN_FILE].datastore
        )
        spec_from_db = cursor.get(SPECIFICATION_KEY)
        segment_size = cursor.get(SEGMENT_SIZE_BYTES_KEY)
        cursor.close()
        self.end_read_only_transaction()
        self.table[DESIGN_FILE].close_datastore()
        spec_from_db = literal_eval(spec_from_db.decode())
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
        segment_size = literal_eval(segment_size.decode())
        if self._real_segment_size_bytes is not False:
            self.segment_size_bytes = self._real_segment_size_bytes
            self._real_segment_size_bytes = False
        if segment_size != self.segment_size_bytes:
            self._real_segment_size_bytes = segment_size
            self.dbenv.close()
            self.dbenv = None
            raise self.SegmentSizeError(
                " ".join(
                    (
                        "Segment size recorded in database is not",
                        "the one used attemping to open database",
                    )
                )
            )
        self.set_segment_size()
        self.open_database_contexts()
        self._dbe = dbe

    def _calculate_max_dbs(self, files=None):
        """Return the number of databases that will be opened."""
        if files is None:
            files = self.specification.keys()
        # The control and design databases.
        db_count = 2
        for file, specification in self.specification.items():
            if file not in files:
                continue
            # Data, existence bitmap, and index bitmap or list, databases.
            db_count += 3
            # Index databases.
            db_count += len(specification[SECONDARY])
        return db_count

    def environment_flags(self, dbe):
        """Return environment flags for transaction update.

        The flag arguments to the 'mdb_env_open' method with their
        Symas documentation equivalents appear to be:
        subdir            MDB_NOSUBDIR
        readonly          MDB_RDONLY
        metasync          MDB_NOMETASYNC
        sync              MDB_NOSYNC
        map_async         MDB_MAPASYNC
        mode
        create
        readahead         MDB_NORDAHEAD
        writemap          MDB_WRITEMAP
        meminit           MDB_NOMEMINIT
        max_readers
        max_dbs
        max_spare_txns
        lock              MDB_NOLOCK

        It is not clear where MDB_FIXEDMAP and MDB_NOTLS fit.

        In python lmdb code:
        MDB_NOTLS seems to be permanently set,
        MDB_MIXEDMAP seems to be permanently unset,
        and cannot be changed by users of python package lmdb.

        What about mode, create, max_readers, max_dbs, and max_spare_txns?

        Python source code suggests mode is file permissions, and maybe
        create controls creation of non-existent directory before call to
        Symas LMMD mdb_env_open() method.

        Not yet sure what the max_*s are counting precisely.
        max_dbs must be equal to or greater than the number of open_db
        calls implied by the FileSpec instance for the database.
        (Could, but do not, put this in the return from this method which
        is equivalent to the evironment properties in Berkeley DB).

        """
        # subdir and readahead seem to default to True (1).
        # subdir==True means create two files, data.mdb and lock.mdb, in the
        #       directory named in path argument to lmdb.open() call.
        # subdir==False means create two files, <path> and <path>-lock,
        #       in the directory named os.path.dirname(<path>) in path
        #       argument to lmdb.open() call.
        # The default leads to self.home_directory and self.database_file
        # being the same in this module's version of Database.
        # subdir==False leads to self.home_directory and self.database_file
        # having the same releationship as in the sibling modules for the
        # Berkeley DB (_db), Sqlite3 (_sqlite), Unqlite, Vedis, Gnu, and
        # Ndbm, (all _nosql), interfaces.
        #
        # Adding 'lock=False' cures the symptoms of a lmdb.BadRslotError hit
        # by, well first seen in, LiteResults application by tab navigation
        # Events | Performances; Cancel (when done); Players.  Task in hand
        # was fitting explicit read-only transactions to converted Berkeley
        # DB code.
        # See stackoverflow.com/questions/56905502/lmdb... and referenced
        # openldap.org/lists/openldap-devel/201409/... thread for informed
        # comment.
        del dbe
        return {"subdir": False, "readahead": False}

    def checkpoint_before_close_dbenv(self):
        """Do nothing.  Present for compatibility with _db module."""

    def open_database_contexts(self, files=None):
        """Open files in the transaction, if any, in self.dbtxn.

        Method named from the DPT interface and given a new purpose.  In
        Berkeley DB interfaces, bsddb3 and berkeleydb, this method has no
        purpose separate from open_database.

        Assume the self.dbtxn object notes the correct transaction type:
        read-only or read-write.

        """
        if files is None:
            files = self.specification.keys()
        else:
            files = files or {}
        dbenv = self.dbenv
        txn = self.dbtxn.transaction
        self.table[CONTROL_FILE].open_datastore(dbenv, txn=txn)
        for file, specification in self.specification.items():
            if file not in files:
                continue
            if file in self.table:
                if self.table[file] is not None:
                    self.table[file].open_datastore(dbenv, txn=txn)
            if file in self.segment_table:
                if self.segment_table[file] is not None:
                    self.segment_table[file].open_datastore(dbenv, txn=txn)
            if file in self.ebm_control:
                if self.ebm_control[file] is not None:
                    self.ebm_control[file].ebm_table.open_datastore(
                        dbenv, txn=txn
                    )
                    # Counting this seems to be available via a transaction
                    # only.  If not in one, avoid setting self.dbenv if at
                    # all possible.
                    if txn:
                        self.ebm_control[file].set_segment_count(txn)
                    else:
                        localtxn = dbenv.begin(write=False)
                        self.ebm_control[file].set_segment_count(localtxn)
                        localtxn.commit()
            for field in specification[SECONDARY]:
                secondary = SUBFILE_DELIMITER.join((file, field))
                if secondary in self.table:
                    self.table[secondary].open_datastore(dbenv, txn=txn)

    def close_database_context_files(self, files=None):
        """Close datastores implementing files in database.

        Method name based on close_database_contexts provided for DPT
        compatibility.

        The files argument is ignored because all current database handles
        will be invalid in the next transaction, read-only or read-write.
        All database handle bindings are released.

        File close action is delegated to the close_datastore() method of
        the key:value datastore managers.

        """
        del files
        if CONTROL_FILE in self.table:
            if self.table[CONTROL_FILE] is not None:
                self.table[CONTROL_FILE].close_datastore()
        for file, specification in self.specification.items():
            if file in self.table:
                if self.table[file] is not None:
                    self.table[file].close_datastore()
            if file in self.segment_table:
                if self.segment_table[file] is not None:
                    self.segment_table[file].close_datastore()
            if file in self.ebm_control:
                if self.ebm_control[file] is not None:
                    self.ebm_control[file].ebm_table.close_datastore()
            for field in specification[SECONDARY]:
                secondary = SUBFILE_DELIMITER.join((file, field))
                if secondary in self.table:
                    self.table[secondary].close_datastore()

    def close_database_contexts(self, files=None):
        """Commit or backout transaction then delete the database handles.

        Provided for compatibility with the DPT interface where there is a
        real difference between close_database_contexts() and
        close_database().

        In Symas LMMD interface, lmdb, this method has no purpose separate
        from close_database.

        In Symas LMMD the files are closed in close_database_context_files()
        because open_database() may need to close the files without closing
        the environment.

        """
        self._dbe = None
        self.close_database_context_files(files=files)
        if self.dbenv is not None:
            self.checkpoint_before_close_dbenv()
            self.dbenv.close()
            self.dbenv = None
            self.table = {}
            self.segment_table = {}
            self.ebm_control = {}
        self.segment_size_bytes = self._initial_segment_size_bytes

    def close_database(self):
        """Close primary and secondary databases and environment.

        That means close all _Database objects for tables and indicies and
        the Environment object defining the environment, and clear all
        dictionaries and the environment references.

        """
        self.close_database_contexts(files=None)

    def put(self, file, key, value):
        """Insert key, or replace key, in table for file using value."""
        assert file in self.specification
        if key is None:
            with self.dbtxn.transaction.cursor(
                self.table[file].datastore
            ) as cursor:
                if cursor.last():
                    key = int.from_bytes(cursor.key(), byteorder="big") + 1
                else:
                    key = 0
                cursor.put(
                    key.to_bytes(4, byteorder="big"),
                    value.encode(),
                    overwrite=False,
                )
            return key
        self.dbtxn.transaction.put(
            key.to_bytes(4, byteorder="big"),
            value.encode(),
            db=self.table[file].datastore,
        )
        return None

    def replace(self, file, key, oldvalue, newvalue):
        """Replace key from table for file using newvalue.

        oldvalue is ignored in _lmdb version of replace() method.
        """
        del oldvalue
        assert file in self.specification
        self.dbtxn.transaction.put(
            key.to_bytes(4, byteorder="big"),
            newvalue.encode(),
            db=self.table[file].datastore,
        )

    def delete(self, file, key, value):
        """Delete key from table for file.

        value is ignored in _lmdb version of delete() method.

        The return value is ignored: True meaning deleted and False meaning
        key not in database.

        """
        del value
        assert file in self.specification
        self.dbtxn.transaction.delete(
            key.to_bytes(4, byteorder="big"),
            db=self.table[file].datastore,
        )

    def get_primary_record(self, file, key):
        """Return primary record (key, value) given primary key on dbset."""
        assert file in self.specification
        if key is None:
            return None
        record = self.dbtxn.transaction.get(
            key.to_bytes(4, byteorder="big"),
            db=self.table[file].datastore,
        )
        if record is None:
            return None
        return key, record.decode()

    def encode_record_number(self, key):
        """Return repr(key).encode() because this is Symas LMMB version.

        Typically used to convert primary key, a record number, to secondary
        index format.

        """
        return repr(key).encode()

    def decode_record_number(self, skey):
        """Return literal_eval(skey.decode()) for Symas LMMB version.

        Typically used to convert secondary index reference to primary record,
        a str(int), to a record number.

        """
        return literal_eval(skey.decode())

    def encode_record_selector(self, key):
        """Return key.encode() because this is Symas LMMB version.

        Typically used to convert a key being used to search a secondary index
        to the form held on the database.

        """
        return key.encode()

    def get_lowest_freed_record_number(self, dbset):
        """Return lowest freed record number in existence bitmap.

        The list of segments with freed record numbers is searched.
        """
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            ebmc.freed_record_number_pages = []
            with self.dbtxn.transaction.cursor(
                self.table[CONTROL_FILE].datastore
            ) as cursor:
                record = cursor.set_key(ebmc.ebmkey)
                while record:
                    record = cursor.item()
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder="big")
                    )
                    record = cursor.next_dup()
        while len(ebmc.freed_record_number_pages):
            segment_number = ebmc.freed_record_number_pages[0]

            # Do not reuse record number on segment of high record number.
            if segment_number == ebmc.segment_count - 1:
                return None

            lfrns = ebmc.read_exists_segment(segment_number, self.dbtxn)
            if lfrns is None:
                # Segment does not exist now.
                ebmc.freed_record_number_pages.remove(segment_number)
                continue

            try:
                first_zero_bit = lfrns.index(False, 0)
            except ValueError:
                with self.dbtxn.transaction.cursor(
                    self.table[CONTROL_FILE].datastore
                ) as cursor:
                    if cursor.set_key_dup(
                        ebmc.ebmkey,
                        segment_number.to_bytes(
                            1 + segment_number.bit_length() // 8,
                            byteorder="big",
                        ),
                    ):
                        cursor.delete()
                    else:
                        raise
                del ebmc.freed_record_number_pages[0]
                continue
            return (
                segment_number * SegmentSize.db_segment_size + first_zero_bit
            )
        return None

    def note_freed_record_number_segment(
        self, dbset, segment, record_number_in_segment, high_record_number
    ):
        """Add existence bitmap segment to list with spare record numbers.

        Caller should check segment has unused records before calling
        note_freed_record_number_segment.  A successful record deletion
        passes this test.
        """
        del record_number_in_segment
        try:
            high_segment = divmod(
                high_record_number, SegmentSize.db_segment_size
            )[0]
        except TypeError:
            # Implies attempt to delete record from empty database.
            # The delete method will have raised an exception if appropriate.
            return

        if segment > high_segment:
            return
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            ebmc.freed_record_number_pages = []
            with self.dbtxn.transaction.cursor(
                self.table[CONTROL_FILE].datastore
            ) as cursor:
                record = cursor.set_key(ebmc.ebmkey)
                while record:
                    record = cursor.item()
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder="big")
                    )
                    record = cursor.next_dup()
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
        self.dbtxn.transaction.put(
            ebmc.ebmkey,
            segment.to_bytes(1 + segment.bit_length() // 8, byteorder="big"),
            db=self.table[CONTROL_FILE].datastore,
        )

    def remove_record_from_ebm(self, file, deletekey):
        """Remove deletekey from file's existence bitmap; return key.

        deletekey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        key = segment.to_bytes(4, byteorder="big")
        ebmb = self.dbtxn.transaction.get(
            key,
            db=self.ebm_control[file].ebm_table.datastore,
        )
        if ebmb is None:
            raise DatabaseError("Existence bit map for segment does not exist")
        ebm = Bitarray()
        ebm.frombytes(ebmb)
        ebm[record_number] = False
        self.dbtxn.transaction.put(
            key,
            ebm.tobytes(),
            db=self.ebm_control[file].ebm_table.datastore,
        )
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        """Add putkey to file's existence bitmap; return (segment, record).

        putkey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        key = segment.to_bytes(4, byteorder="big")
        ebmb = self.dbtxn.transaction.get(
            key,
            db=self.ebm_control[file].ebm_table.datastore,
        )
        if ebmb is None:
            ebm = SegmentSize.empty_bitarray.copy()
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb)
        ebm[record_number] = True
        self.dbtxn.transaction.put(
            key,
            ebm.tobytes(),
            db=self.ebm_control[file].ebm_table.datastore,
        )
        return segment, record_number

    def get_high_record_number(self, file):
        """Return the high existing record number in table for file."""
        with self.dbtxn.transaction.cursor(
            self.table[file].datastore
        ) as cursor:
            if cursor.last():
                return int.from_bytes(cursor.key(), byteorder="big")
            return None

    def _get_segment_record_numbers(self, file, reference):
        segment_record = self.dbtxn.transaction.get(
            reference.to_bytes(4, byteorder="big"),
            db=self.segment_table[file].datastore,
        )
        if len(segment_record) < SegmentSize.db_segment_size_bytes:
            return [
                int.from_bytes(segment_record[i : i + 2], byteorder="big")
                for i in range(0, len(segment_record), 2)
            ]
        recnums = Bitarray()
        recnums.frombytes(segment_record)
        return recnums

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
        key = self.encode_record_selector(key)
        secondary = SUBFILE_DELIMITER.join((file, field))
        with self.dbtxn.transaction.cursor(
            self.table[secondary].datastore
        ) as cursor:
            segment_bytes = segment.to_bytes(4, byteorder="big")
            record = cursor.set_range_dup(key, segment_bytes)
            while record:
                record_key, value = cursor.item()
                if record_key != key:
                    # Not reachable given py-lmdb set_range_dup behaviour.
                    # Code copied for Berkeley DB set_range behaviour.
                    # No index entry for key.
                    cursor.put(
                        key,
                        b"".join(
                            (
                                segment_bytes,
                                record_number.to_bytes(2, byteorder="big"),
                            )
                        ),
                    )
                    return

                segment_number = int.from_bytes(value[:4], byteorder="big")
                if segment_number > segment:
                    # Not reachable given py-lmdb set_range_dup behaviour.
                    # Code copied for Berkeley DB set_range behaviour.
                    # No index entry for key in this segment.
                    cursor.put(
                        key,
                        b"".join(
                            (
                                segment_bytes,
                                record_number.to_bytes(2, byteorder="big"),
                            )
                        ),
                    )
                    return

                if len(value) == SEGMENT_HEADER_LENGTH:
                    existing_record_number = int.from_bytes(
                        value[4:], byteorder="big"
                    )
                    if existing_record_number != record_number:
                        with self.dbtxn.transaction.cursor(
                            self.segment_table[file].datastore
                        ) as seg_cursor:
                            if seg_cursor.last():
                                segment_key = (
                                    int.from_bytes(
                                        seg_cursor.key(), byteorder="big"
                                    )
                                    + 1
                                )
                            else:
                                # Should not be reached.
                                # Sibling _db module for Berkeley DB uses
                                # append() method and does not care if any of
                                # these records exist.
                                segment_key = 0
                            segment_key = segment_key.to_bytes(
                                4, byteorder="big"
                            )
                            seg_cursor.put(
                                segment_key,
                                b"".join(
                                    sorted(
                                        [
                                            record_number.to_bytes(
                                                length=2, byteorder="big"
                                            ),
                                            existing_record_number.to_bytes(
                                                length=2, byteorder="big"
                                            ),
                                        ]
                                    )
                                ),
                                overwrite=False,
                            )
                            cursor.delete()
                            cursor.put(
                                key,
                                b"".join(
                                    (
                                        value[:4],
                                        b"\x00\x02",
                                        segment_key,
                                    )
                                ),
                            )
                    return
                segment_key = int.from_bytes(
                    value[SEGMENT_HEADER_LENGTH:], byteorder="big"
                )
                recnums = self._get_segment_record_numbers(file, segment_key)
                if isinstance(recnums, list):
                    i = bisect.bisect_left(recnums, record_number)
                    if i < len(recnums):
                        if recnums[i] != record_number:
                            recnums.insert(i, record_number)
                    else:
                        recnums.append(record_number)
                    count = len(recnums)
                    if count > SegmentSize.db_upper_conversion_limit:
                        seg = SegmentSize.empty_bitarray.copy()
                        for i in recnums:
                            seg[i] = True
                        self.dbtxn.transaction.put(
                            value[SEGMENT_HEADER_LENGTH:],
                            seg.tobytes(),
                            db=self.segment_table[file].datastore,
                        )
                        cursor.delete()
                        cursor.put(
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                    else:
                        self.dbtxn.transaction.put(
                            value[SEGMENT_HEADER_LENGTH:],
                            b"".join(
                                (
                                    rn.to_bytes(length=2, byteorder="big")
                                    for rn in recnums
                                )
                            ),
                            db=self.segment_table[file].datastore,
                        )
                        cursor.delete()
                        cursor.put(
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                    return

                # ignore possibility record_number already present
                recnums[record_number] = True
                self.dbtxn.transaction.put(
                    value[SEGMENT_HEADER_LENGTH:],
                    recnums.tobytes(),
                    db=self.segment_table[file].datastore,
                )
                cursor.delete()
                cursor.put(
                    key,
                    b"".join(
                        (
                            value[:4],
                            recnums.count().to_bytes(2, byteorder="big"),
                            value[SEGMENT_HEADER_LENGTH:],
                        )
                    ),
                )
                return

            # No index entry for key because database is empty.
            cursor.put(
                key,
                b"".join(
                    (
                        segment.to_bytes(4, byteorder="big"),
                        record_number.to_bytes(2, byteorder="big"),
                    )
                ),
            )

    def remove_record_from_field_value(
        self, file, field, key, segment, record_number
    ):
        """Remove record_number from set of records in segment for key.

        key is a value of index field on segment table for file.

        The representation of the set of records on the database is
        converted from bitmap to list to integer if the removal reduces
        the number of records in the set below the relevant limit.
        """
        key = self.encode_record_selector(key)
        secondary = SUBFILE_DELIMITER.join((file, field))
        with self.dbtxn.transaction.cursor(
            self.table[secondary].datastore
        ) as cursor:
            segment_bytes = segment.to_bytes(4, byteorder="big")
            record = cursor.set_range_dup(key, segment_bytes)
            while record:
                record_key, value = cursor.item()
                if record_key != key:
                    # Assume that multiple requests to delete an index value
                    # have been made for a record.  The segment_put method uses
                    # sets to avoid adding multiple entries.  Consider using
                    # set rather than list in the pack method of the subclass
                    # of Value if this will happen a lot.
                    return

                segment_number = int.from_bytes(value[:4], byteorder="big")
                if segment_number < segment:
                    record = cursor.next_dup()
                    continue
                if segment_number > segment:
                    return
                if len(value) == SEGMENT_HEADER_LENGTH:
                    if record_number == int.from_bytes(
                        value[4:], byteorder="big"
                    ):
                        cursor.delete()
                    return
                segment_key = int.from_bytes(
                    value[SEGMENT_HEADER_LENGTH:], byteorder="big"
                )
                recnums = self._get_segment_record_numbers(file, segment_key)
                if isinstance(recnums, list):
                    discard = bisect.bisect_left(recnums, record_number)
                    if recnums[discard] == record_number:
                        del recnums[discard]
                    count = len(recnums)
                    if count < 2:
                        for i in recnums:
                            ref = b"".join(
                                (
                                    segment.to_bytes(4, byteorder="big"),
                                    i.to_bytes(2, byteorder="big"),
                                )
                            )
                        self.dbtxn.transaction.delete(
                            value[SEGMENT_HEADER_LENGTH:],
                            db=self.segment_table[file].datastore,
                        )
                        cursor.delete()
                        if count:
                            cursor.put(key, ref)
                    else:
                        self.dbtxn.transaction.put(
                            value[SEGMENT_HEADER_LENGTH:],
                            b"".join(
                                (
                                    i.to_bytes(length=2, byteorder="big")
                                    for i in sorted(recnums)
                                )
                            ),
                            db=self.segment_table[file].datastore,
                        )
                        cursor.delete()
                        cursor.put(
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                    return

                # ignore possibility record_number already absent
                recnums[record_number] = False

                count = recnums.count()
                if count > SegmentSize.db_lower_conversion_limit:
                    self.dbtxn.transaction.put(
                        value[SEGMENT_HEADER_LENGTH:],
                        recnums.tobytes(),
                        db=self.segment_table[file].datastore,
                    )
                    cursor.delete()
                    cursor.put(
                        key,
                        b"".join(
                            (
                                value[:4],
                                recnums.count().to_bytes(2, byteorder="big"),
                                value[SEGMENT_HEADER_LENGTH:],
                            )
                        ),
                    )
                else:
                    recnums = set(recnums.search(SINGLEBIT))
                    self.dbtxn.transaction.put(
                        value[SEGMENT_HEADER_LENGTH:],
                        b"".join(
                            (
                                i.to_bytes(length=2, byteorder="big")
                                for i in sorted(recnums)
                            )
                        ),
                        db=self.segment_table[file].datastore,
                    )
                    cursor.delete()
                    cursor.put(
                        key,
                        b"".join(
                            (
                                value[:4],
                                len(recnums).to_bytes(2, byteorder="big"),
                                value[SEGMENT_HEADER_LENGTH:],
                            )
                        ),
                    )
                return

    def populate_segment(self, segment_reference, file):
        """Return records for segment_reference in segment table for file.

        A RecordsetSegmentBitarray, RecordsetSegmentList, or
        RecordsetSegmentInt, instance is returned.

        segment_reference has a rowid, or the record number if there is
        exactly one record in the segment.
        """
        if len(segment_reference) == SEGMENT_HEADER_LENGTH:
            return RecordsetSegmentInt(
                int.from_bytes(segment_reference[:4], byteorder="big"),
                None,
                records=segment_reference[4:],
            )
        segment_record = self.dbtxn.transaction.get(
            segment_reference[SEGMENT_HEADER_LENGTH:],
            db=self.segment_table[file].datastore,
        )
        if segment_record is None:
            raise DatabaseError("Segment record missing")
        if len(segment_record) == SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentBitarray(
                int.from_bytes(segment_reference[:4], byteorder="big"),
                None,
                records=segment_record,
            )
        return RecordsetSegmentList(
            int.from_bytes(segment_reference[:4], byteorder="big"),
            None,
            records=segment_record,
        )

    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        with self.dbtxn.transaction.cursor(
            self.table[
                SUBFILE_DELIMITER.join((file, valuespec.field))
            ].datastore
        ) as cursor:
            if valuespec.above_value and valuespec.below_value:
                record = cursor.set_range(valuespec.above_value.encode())
                if record:
                    if cursor.item()[0] == valuespec.above_value.encode():
                        record = cursor.next_nodup()
                while record:
                    key = cursor.item()[0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.above_value and valuespec.to_value:
                record = cursor.set_range(valuespec.above_value.encode())
                if record:
                    if cursor.item()[0] == valuespec.above_value.encode():
                        record = cursor.next_nodup()
                while record:
                    key = cursor.item()[0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.from_value and valuespec.to_value:
                record = cursor.set_range(valuespec.from_value.encode())
                while record:
                    key = cursor.item()[0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.from_value and valuespec.below_value:
                record = cursor.set_range(valuespec.from_value.encode())
                while record:
                    key = cursor.item()[0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.above_value:
                record = cursor.set_range(valuespec.above_value.encode())
                if record:
                    if cursor.item()[0] == valuespec.above_value.encode():
                        record = cursor.next_nodup()
                while record:
                    key = cursor.item()[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.from_value:
                record = cursor.set_range(valuespec.from_value.encode())
                while record:
                    key = cursor.item()[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.to_value:
                record = cursor.first()
                while record:
                    key = cursor.item()[0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            elif valuespec.below_value:
                record = cursor.first()
                while record:
                    key = cursor.item()[0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()
            else:
                record = cursor.first()
                while record:
                    key = cursor.item()[0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = cursor.next_nodup()

    # The bit setting in existence bit map decides if a record is put on the
    # recordset created by the make_recordset_*() methods.

    # Look at ebm_control.ebm_table even though the additional 'rn in record'
    # clause when populating the recordset makes table access cheaper.
    def recordlist_record_number(self, file, key=None, cache_size=1):
        """Return RecordList on file containing records for key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return recordlist
        segment_number, record_number = divmod(
            key, SegmentSize.db_segment_size
        )
        record = self.dbtxn.transaction.get(
            segment_number.to_bytes(4, byteorder="big"),
            db=self.ebm_control[file].ebm_table.datastore,
        )
        if record and record_number in RecordsetSegmentBitarray(
            segment_number, key, records=record
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
        else:
            segment_end, recnum_end = None, None
        with self.dbtxn.transaction.cursor(
            self.ebm_control[file].ebm_table.datastore
        ) as cursor:
            first_segment = None
            final_segment = None
            record = cursor.first()
            while record:
                segment_number, segment_record = cursor.item()
                segment_number = int.from_bytes(
                    segment_number, byteorder="big"
                )
                if segment_number < segment_start:
                    record = cursor.next()
                    continue
                if segment_end is not None and segment_number > segment_end:
                    record = cursor.next()
                    continue
                if segment_number == segment_start:
                    if (segment_number and recnum_start) or recnum_start > 1:
                        first_segment, start_byte = divmod(recnum_start, 8)
                        segment_record = (
                            b"\x00" * first_segment
                            + segment_record[first_segment:]
                        )
                if keyend is not None:
                    if segment_number > segment_end:
                        break
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
                record = cursor.next()
            if first_segment is not None:
                for i in range(
                    first_segment * 8, first_segment * 8 + start_byte
                ):
                    recordlist[segment_start][(segment_start, i)] = False
            if final_segment is not None:
                for i in range(
                    final_segment * 8 + end_byte + 1, (final_segment + 1) * 8
                ):
                    recordlist[segment_end][(segment_end, i)] = False
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList containing records on file."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        with self.dbtxn.transaction.cursor(
            self.ebm_control[file].ebm_table.datastore
        ) as cursor:
            record = cursor.first()
            while record:
                segment_number, segment_record = cursor.item()
                segment_number = int.from_bytes(
                    segment_number, byteorder="big"
                )
                recordlist[segment_number] = RecordsetSegmentBitarray(
                    segment_number, None, records=segment_record
                )
                record = cursor.next()
        return recordlist

    def populate_recordset_segment(self, recordset, reference):
        """Populate recordset with segment in reference.

        The segment from reference is added to an existing segment in
        recordset if there is one, or becomes recordset's segment for
        that segment number if not.
        """
        segment_number = int.from_bytes(reference[:4], byteorder="big")
        if len(reference) == SEGMENT_HEADER_LENGTH:
            segment = RecordsetSegmentInt(
                segment_number, None, records=reference[4:]
            )
        else:
            segment_record = self.dbtxn.transaction.get(
                reference[SEGMENT_HEADER_LENGTH:],
                db=self.segment_table[recordset.dbset].datastore,
            )
            # 'assert segment_record is not None' is reasonable at this point.
            # Working on version 5.0.1 much time was lost looking for a
            # transaction design problem which turned out to be not writing
            # the record this assert statement tests.
            if len(segment_record) == SegmentSize.db_segment_size_bytes:
                segment = RecordsetSegmentBitarray(
                    segment_number, None, records=segment_record
                )
            else:
                segment = RecordsetSegmentList(
                    segment_number, None, records=segment_record
                )
        if segment_number not in recordset:
            recordset[segment_number] = segment
        else:
            recordset[segment_number] |= segment

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return RecordList containing records for field on file.

        The records are indexed by keys containing keylike.
        """
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if keylike is None:
            return recordlist
        matcher = re.compile(keylike)
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            record = cursor.first()
            while record:
                key, value = cursor.item()
                if matcher.search(key):
                    self.populate_recordset_segment(recordlist, value)
                record = cursor.next()
        return recordlist

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return recordlist
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            record = cursor.set_range(key)
            while record:
                record_key, value = cursor.item()
                if record_key != key:
                    break
                self.populate_recordset_segment(recordlist, value)
                record = cursor.next()
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
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            record = cursor.set_range(keystart)
            while record:
                record = cursor.item()
                if not record[0].startswith(keystart):
                    break
                self.populate_recordset_segment(recordlist, record[1])
                record = cursor.next()
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        Keys are in range set by combinations of ge, gt, le, and lt.
        """
        if isinstance(ge, bytes) and isinstance(gt, bytes):
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        if isinstance(le, bytes) and isinstance(lt, bytes):
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            if ge is None and gt is None:
                record = cursor.first()
            else:
                record = cursor.set_range(ge or gt or b"")
            if gt:
                while record:
                    record = cursor.item()
                    if record[0] > gt:
                        break
                    record = cursor.next()
            if le is None and lt is None:
                while record:
                    record = cursor.item()
                    self.populate_recordset_segment(recordlist, record[1])
                    record = cursor.next()
            elif lt is None:
                while record:
                    record = cursor.item()
                    if record[0] > le:
                        break
                    self.populate_recordset_segment(recordlist, record[1])
                    record = cursor.next()
            else:
                while record:
                    record = cursor.item()
                    if record[0] >= lt:
                        break
                    self.populate_recordset_segment(recordlist, record[1])
                    record = cursor.next()
        return recordlist

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            record = cursor.first()
            while record:
                record = cursor.item()
                self.populate_recordset_segment(recordlist, record[1])
                record = cursor.next()
        return recordlist

    def recordlist_nil(self, file, cache_size=1):
        """Return empty RecordList on file."""
        return RecordList(dbhome=self, dbset=file, cache_size=cache_size)

    def unfile_records_under(self, file, field, key):
        """Delete the reference to records for index field[key].

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            # Delete segment records.
            record = cursor.set_range(key)
            while record:
                record = cursor.item()
                record_key, value = record
                if record_key != key:
                    break
                if len(value) > SEGMENT_HEADER_LENGTH:
                    self.dbtxn.transaction.delete(
                        value[SEGMENT_HEADER_LENGTH:],
                        db=self.segment_table[file].datastore,
                    )
                record = cursor.next()

        self.dbtxn.transaction.delete(
            key,
            db=self.table[SUBFILE_DELIMITER.join((file, field))].datastore,
        )

    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records."""
        assert recordset.dbset == file
        assert file in self.table

        # Delete existing segments for key
        self.unfile_records_under(file, field, key)

        with self.dbtxn.transaction.cursor(
            self.table[SUBFILE_DELIMITER.join((file, field))].datastore
        ) as cursor:
            recordset.normalize()
            for segment_number in recordset.sorted_segnums:
                rs_segment = recordset.rs_segments[segment_number]
                if isinstance(rs_segment, RecordsetSegmentInt):
                    cursor.put(
                        key,
                        b"".join(
                            (
                                segment_number.to_bytes(4, byteorder="big"),
                                rs_segment.tobytes(),
                            )
                        ),
                    )
                else:
                    count = rs_segment.count_records()
                    with self.dbtxn.transaction.cursor(
                        self.segment_table[file].datastore
                    ) as seg_cursor:
                        if seg_cursor.last():
                            segment_key = (
                                int.from_bytes(
                                    seg_cursor.key(), byteorder="big"
                                )
                                + 1
                            ).to_bytes(4, byteorder="big")
                        else:
                            # Should not be reached.
                            # Sibling _db module for Berkeley DB uses
                            # append() method and does not care if any of
                            # these records exist.
                            segment_key = int(0).to_bytes(4, byteorder="big")

                    # This write was missing at version 5.0 and the problem
                    # was initially thought to be transaction design related
                    # to explicit read-only transactions in lmdb.
                    self.dbtxn.transaction.put(
                        segment_key,
                        rs_segment.tobytes(),
                        db=self.segment_table[file].datastore,
                    )

                    cursor.put(
                        key,
                        b"".join(
                            (
                                segment_number.to_bytes(4, byteorder="big"),
                                count.to_bytes(2, byteorder="big"),
                                segment_key,
                            )
                        ),
                    )

    def database_cursor(self, file, field, keyrange=None, recordset=None):
        """Return a cursor on Symas LMMD sub-database for (file, field).

        keyrange is an addition for DPT. It may yet be removed.
        recordset must be an instance of RecordList or FoundSet, or None.

        """
        assert file in self.specification
        if recordset is not None:
            assert isinstance(recordset, (RecordList, FoundSet))
            return recordset.create_recordsetbase_cursor(internalcursor=True)
        if file == field:
            return CursorPrimary(
                self.table[file],
                keyrange=keyrange,
                transaction=self.dbtxn,
                ebm=self.ebm_control[file].ebm_table,
                engine=self._dbe,
            )
        return CursorSecondary(
            self.table[SUBFILE_DELIMITER.join((file, field))],
            keyrange=keyrange,
            transaction=self.dbtxn,
            segment=self.segment_table[file],
        )

    def create_recordset_cursor(self, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursor(
            recordset,
            transaction=self.dbtxn,
            database=self.table[recordset.dbset],
        )

    # Comment in chess_ui for make_position_analysis_data_source method, only
    # call, suggests is_database_file_active should not be needed.
    def is_database_file_active(self, file):
        """Return True if Symas LMMD sub-database object for file exists."""
        return self.table[file] is not None

    def get_table_connection(self, file):
        """Return main Symas LMMD sub-database object for file."""
        if self.dbenv:
            return self.table[file].datastore
        return None

    def _datastoreclass(self):
        return _Datastore

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs=None,
        use_specification_items=None,
    ):
        """Run taskmethod to perform database task."""
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

    def database_stats(self):
        """Return dict of dicts of database stats."""
        stats = {}
        for file, table in self.table.items():
            if file in {DESIGN_FILE, CONTROL_FILE}:
                stats[file] = table.datastore_stats(self.dbtxn)
            elif table is not None:
                for count, dbo in enumerate(table):
                    stats[(file, count)] = dbo.datastore_stats(self.dbtxn)
            if file in self.segment_table:
                if self.segment_table[file] is not None:
                    stats[(file, "segment")] = self.segment_table[
                        file
                    ].datastore_stats(self.dbtxn)
            if file in self.ebm_control:
                if self.ebm_control[file] is not None:
                    stats[(file, "ebm")] = self.ebm_control[
                        file
                    ].ebm_table.datastore_stats(self.dbtxn)
        return stats

    def database_stats_summary(self):
        """Return tuple of environment size and usage, and database stats.

        database_stats_summary should be called outside any transaction.

        """
        env_info = self.dbenv.info()
        env_stat = self.dbenv.stat()
        self.start_read_only_transaction()
        stats = self.database_stats()
        self.end_read_only_transaction()
        used_page_count = sum(
            env_stat[pages]
            for pages in ("branch_pages", "leaf_pages", "overflow_pages")
        )
        used_byte_count = used_page_count * env_stat["psize"]
        for value in stats.values():
            psize = value.get("psize", 0)
            branch_pages = value.get("branch_pages", 0)
            leaf_pages = value.get("leaf_pages", 0)
            overflow_pages = value.get("overflow_pages", 0)
            pages = branch_pages + leaf_pages + overflow_pages
            used_page_count += pages
            used_byte_count += pages * psize
        return (
            env_info["map_size"],
            env_info["last_pgno"] + 1,  # page numbers start at 0.
            used_byte_count,
            used_page_count,
            stats,  # for application specific purposes.
        )

    def _set_map_blocks_above_used_pages(self, increment):
        """Set map_blocks to increase enviroment size by increment blocks.

        Approximately DEFAULT_MAP_PAGES * increment pages are set to be
        added to the environment size when the database is next opened.

        The figure is approximate because blocks = pages // block size.

        This method assumes the database and environment are closed.

        """
        # self.open_database(), and self.close_database(), are used because
        # the environment is opened inside open_database; and extracting it
        # has to be done in way compatible with the open_database() methods
        # for other database engines.
        self.open_database()
        map_pages = self.dbenv.info()["last_pgno"] + 1  # numbered from 0.
        self.close_database()
        self.map_blocks = (
            map_pages + DEFAULT_MAP_PAGES * increment
        ) // DEFAULT_MAP_PAGES

    def _set_map_size_above_used_pages_between_transactions(self, increment):
        """Set enviroment size to increment blocks above used size.

        Approximately DEFAULT_MAP_PAGES * increment pages are added to the
        environment size and map_blocks is set to fit this size.

        The figure is approximate because blocks = pages // block size.

        This method assumes the database and environment are open.

        """
        map_pages = self.dbenv.info()["last_pgno"] + 1  # numbered from 0.
        self.map_blocks = (
            map_pages + DEFAULT_MAP_PAGES * increment
        ) // DEFAULT_MAP_PAGES
        self.dbenv.set_mapsize(self.map_blocks * DEFAULT_MAP_SIZE)

    def get_application_control(self):
        """Return dict of application control items."""
        value = self.dbtxn.transaction.get(
            APPLICATION_CONTROL_KEY,
            db=self.table[DESIGN_FILE].datastore,
        )
        if value is not None:
            return literal_eval(value.decode())
        return {}

    def set_application_control(self, appcontrol):
        """Set dict of application control items."""
        self.dbtxn.transaction.put(
            APPLICATION_CONTROL_KEY,
            repr(appcontrol).encode(),
            db=self.table[DESIGN_FILE].datastore,
        )


class _DBtxn:
    """Provide a Symas LMMD read-only or read-write transaction instance.

    This class is intended to implement transactions for Symas LMMD with
    the existing open_database_contexts, close_database_contexts, commit,
    backout, and start_transaction, methods in the Database classes for
    each database engine.  The other engines do not need their own version
    of this class because 'read transactions' are implicit.

    """

    def __init__(self):
        self._write_requested = False
        self._transaction = None

    @property
    def transaction(self):
        """Return self._transaction."""
        return self._transaction

    def start_transaction(self, dbenv, write):
        """Begin a read-only or read-write transaction in dbenv environment.

        bool(write)==True   A read-write transaction.
        bool(write)==False  A read-only transaction.

        """
        if self._transaction is not None:
            return
        self._write_requested = write
        self._transaction = dbenv.begin(write=self._write_requested)

    def end_transaction(self):
        """Discard transaction and set self._read_write_requested False.

        Assume the caller has arranged for transaction commit or backout,
        and destroying the self._transaction instance, before calling this
        method.

        This is likely called by Database.close_database_contexts().

        """
        if self._transaction is None:
            return
        self._transaction = None
        self._write_requested = False


class _Datastore:
    """This class is used to access datastores in a Symas LMMD database.

    Instances are created as necessary by a Database.open_database() call.

    The definition of the datastores, in Database.open_database, needs to
    be separated from opening and accessing the datastore.

    The sibling modules for Berkeley DB and SQLite3 do not have classes like
    _Datastore.  (They used to have such but it seems simpler there
    without.  The _dpt module has a _DPTFile class for similar reasons.)

    """

    def __init__(self, datastorename, **keywords):
        """Set arguments for py-lmdb.open_db (Symas LMMD mdb_dbi_open) call.

        datastorename is the name argument.
        **keywords is a dict used by open_db to set flags for mdb_dbi_open.

        **kewwords does not include the txn argument which must be supplied
        in open_* calls.

        """
        self._name = datastorename
        self._flags = keywords
        self._datastore = None

    @property
    def datastore(self):
        """Return the self._datastore lmdb.open_db instance."""
        return self._datastore

    def open_datastore(self, dbenv, txn=None):
        """Open lmdb datastore in dbenv, and optionally in a transaction."""
        # It is possible to pass a transaction to the open_db() call.
        # This implies the database is opened and closed in each transaction.
        # By not passing a transaction opening and closing databases is done
        # similar to the style used in the _db, _sqlite, and _nosql modules.
        # The datastore must be created in a transaction so that commit or
        # backout action can be done.
        assert self._datastore is None
        self._datastore = dbenv.open_db(self._name, txn=txn, **self._flags)

    def close_datastore(self):
        """Set datastore handle to None.

        Intended for use just after commit or backout of a transaction.

        """
        # py-lmdb does not expose the mdb_dbi_close method.
        # It is assumed the only time mdb_dbi_close action might occur is
        # when the reference count of the object bound to self._datastore
        # in the open_datastore method reaches zero.
        # Symas LMMD documentation states closing a handle allows it to be
        # reused in a future mdb_dbi_open() call; but also states this action
        # is unnecessary and 'use with care'.
        # But this method throws the handle away.
        self._datastore = None

    def datastore_stats(self, txn):
        """Return dict of database stats.

        An empty dict is returned if self._datastore is None on catching
        the consequential AttributeError.

        """
        # The exception is expected on the first call only, when the database
        # has not yet been opened.
        try:
            return txn.transaction.stat(self._datastore)
        except AttributeError:
            if self._datastore is None:
                return {}
            raise


class Cursor(_cursor.Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - Symas LMMD sub-database object.
    keyrange - not used.
    transaction - the current transaction.
    kargs - absorb argunents relevant to other database engines.

    The wrapped cursor is created on the Symas LMMD sub-database in a File
    instance.

    The transaction argument in the Cursor() call should be a function
    which returns current tranasction active on the Symas LMMD environment,
    or None if there isn't one.  If supplied it's return value is used in all
    calls to methods of the wrapped cursor which have the 'txn' parameter.
    By default the calls are not within a transaction.

    The CursorPrimary and CursorSecondary subclasses define the bsddb3
    style cursor methods peculiar to primary and secondary databases.
    """

    def __init__(self, dbset, keyrange=None, transaction=None, **kargs):
        """Define a cursor on the underlying database engine dbset."""
        del keyrange, kargs
        super().__init__(dbset)
        self._transaction = transaction
        self._cursor = transaction.transaction.cursor(dbset.datastore)
        self._current_segment = None
        self.current_segment_number = None
        self._current_record_number_in_segment = None

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial.encode()

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError("get_partial_with_wildcard not implemented")

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return self._partial.encode()

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Do nothing in Symas LMMD.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """


class CursorPrimary(Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - Symas LMMD sub-database object.
    ebm - Symas LMMD sub-database object for existence bitmap.
    engine - lmdb module.  Only the DB_FAST_STAT flag is used at present.
    kargs - superclass arguments and absorb arguments for other engines.

    """

    def __init__(self, dbset, ebm=None, engine=None, **kargs):
        """Extend, note existence bitmap table and engine."""
        super().__init__(dbset, **kargs)
        self._ebm = ebm
        self._engine = engine

    def count_records(self):
        """Return record count."""
        return self._transaction.transaction.stat(self._dbset.datastore)[
            "entries"
        ]

    def first(self):
        """Return first record taking partial key into account."""
        if not self._cursor.first():
            return None
        return self._decode_record(self._cursor.item())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        # record keys are 0-based converted to bytes.
        # segment_numbers are 0-based.
        if record is None:
            return 0
        segment_number, record_number = divmod(
            record[0], SegmentSize.db_segment_size
        )
        segment = self._transaction.transaction.get(
            segment_number.to_bytes(4, byteorder="big"), db=self._ebm.datastore
        )
        if segment is None:
            return 0
        position = 0
        for i in range(segment_number):
            segment_ebm = Bitarray()
            segment_ebm.frombytes(
                self._transaction.transaction.get(
                    i.to_bytes(4, byteorder="big"),
                    db=self._ebm.datastore,
                )
            )
            position += segment_ebm.count()
        segment_ebm = Bitarray()
        segment_ebm.frombytes(segment)
        try:
            position += segment_ebm.search(SINGLEBIT).index(record_number) + 1
        except ValueError:
            position += (
                bisect.bisect_left(
                    segment_ebm.search(SINGLEBIT), record_number
                )
                + 1
            )
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if not position:  # Include position 0 in this case.
            return None
        count = 0
        abspos = abs(position)
        ebm_cursor = self._transaction.transaction.cursor(
            db=self._ebm.datastore
        )
        try:
            if position < 0:
                record = ebm_cursor.last()
                while record:
                    record = ebm_cursor.item()
                    segment_ebm = Bitarray()
                    segment_ebm.frombytes(record[1])
                    ebm_count = segment_ebm.count()
                    if count + ebm_count < abspos:
                        count += ebm_count
                        record = ebm_cursor.prev()
                        continue
                    recno = segment_ebm.search(SINGLEBIT)[position + count] + (
                        (int.from_bytes(record[0], byteorder="big"))
                        * SegmentSize.db_segment_size
                    )
                    if recno < 0:
                        return None
                    self._cursor.set_key(recno.to_bytes(4, byteorder="big"))
                    return self._decode_record(self._cursor.item())
            else:
                record = ebm_cursor.first()
                while record:
                    record = ebm_cursor.item()
                    segment_ebm = Bitarray()
                    segment_ebm.frombytes(record[1])
                    ebm_count = segment_ebm.count()
                    if count + ebm_count < abspos:
                        count += ebm_count
                        record = ebm_cursor.next()
                        continue
                    recno = segment_ebm.search(SINGLEBIT)[
                        position - count - 1
                    ] + (
                        (int.from_bytes(record[0], byteorder="big"))
                        * SegmentSize.db_segment_size
                    )
                    if recno < 0:
                        return None
                    self._cursor.set_key(recno.to_bytes(4, byteorder="big"))
                    return self._decode_record(self._cursor.item())
        finally:
            ebm_cursor.close()
        return None

    def last(self):
        """Return last record taking partial key into account."""
        if not self._cursor.last():
            return None
        return self._decode_record(self._cursor.item())

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if not self._cursor.set_range(key.to_bytes(4, byteorder="big")):
            return None
        return self._decode_record(self._cursor.item())

    def next(self):
        """Return next record taking partial key into account."""
        if not self._cursor.next():
            return None
        return self._decode_record(self._cursor.item())

    def prev(self):
        """Return previous record taking partial key into account."""
        if not self._cursor.prev():
            return None
        return self._decode_record(self._cursor.item())

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.

        """
        # Should this be 'set_key_dup' and, or, return None if the 'set_*'
        # call returns False?
        self._cursor.set_key(record[0].to_bytes(4, byteorder="big"))
        return self._decode_record(self._cursor.item())

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            key, value = record
            return int.from_bytes(key, byteorder="big"), value.decode()
        except:
            if record is None:
                return record
            raise

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        raise DatabaseError("_get_record not implemented")

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # raise DatabaseError('refresh_recordset not implemented')


class CursorSecondary(Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - Symas LMMD sub-database object.
    segment - Symas LMMD sub-database object for segment, list of record
            numbers or bitmap.
    kargs - superclass arguments and absorb arguments for other engines.

    """

    def __init__(self, dbset, segment=None, **kargs):
        """Extend, note segment table name."""
        super().__init__(dbset, **kargs)
        self._segment = segment

    def count_records(self):
        """Return record count."""
        if self.get_partial() in (None, False):
            count = 0
            record = self._cursor.first()
            while record:
                record = self._cursor.item()
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    count += int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    count += 1
                record = self._cursor.next()
            return count
        count = 0
        record = self._cursor.set_range(
            self.get_converted_partial_with_wildcard()
        )
        while record:
            record = self._cursor.item()
            if not record[0].startswith(self.get_converted_partial()):
                break
            if len(record[1]) > SEGMENT_HEADER_LENGTH:
                count += int.from_bytes(record[1][4:6], byteorder="big")
            else:
                count += 1
            record = self._cursor.next()
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is None:
            try:
                key, value = self._first()
            except TypeError:
                return None
            return key.decode(), value
        if self.get_partial() is False:
            return None
        record = self.nearest(self.get_converted_partial())
        if record is not None:
            if not record[0].startswith(self.get_partial()):
                return None
        return record

    def _get_segment(self, key, segment_number, reference):
        if len(reference) == SEGMENT_HEADER_LENGTH:
            return RecordsetSegmentInt(
                segment_number, key, records=reference[4:]
            )
        if self.current_segment_number == segment_number:
            if key == self._current_segment.index_key:
                return self._current_segment
        records = self._transaction.transaction.get(
            reference[SEGMENT_HEADER_LENGTH:],
            db=self._segment.datastore,
        )
        if len(records) < SegmentSize.db_segment_size_bytes:
            return RecordsetSegmentList(segment_number, key, records=records)
        return RecordsetSegmentBitarray(segment_number, key, records=records)

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        key, value = record
        segment_number, record_number = divmod(
            value, SegmentSize.db_segment_size
        )

        # Define functions to handle presence or absence of partial key.

        def low(jkey, recordkey):
            return jkey < recordkey

        if not self.get_partial():

            def high(jkey, recordkey):
                return jkey > recordkey

        else:

            def high(jkey, partial):
                return not jkey.startswith(partial)

        # Get position of record relative to start point.
        position = 0
        if not self.get_partial():
            j = self._cursor.first()
        else:
            j = self._cursor.set_range(
                self.get_converted_partial_with_wildcard()
            )
        while j:
            j = self._cursor.item()
            if low(j[0].decode(), key):
                if len(j[1]) > SEGMENT_HEADER_LENGTH:
                    position += int.from_bytes(j[1][4:6], byteorder="big")
                else:
                    position += 1
            elif high(j[0].decode(), key):
                break
            else:
                i = int.from_bytes(j[1][:4], byteorder="big")
                if i < segment_number:
                    if len(j[1]) > SEGMENT_HEADER_LENGTH:
                        position += int.from_bytes(j[1][4:6], byteorder="big")
                    else:
                        position += 1
                elif i > segment_number:
                    break
                else:
                    position += self._get_segment(
                        key, segment_number, j[1]
                    ).get_position_of_record_number(record_number)
                    break
            j = self._cursor.next()
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None

        # Start at first or last record whichever is likely closer to position
        # and define functions to handle presence or absence of partial key.
        if not self.get_partial():
            get_partial = self.get_partial
        else:
            get_partial = self.get_converted_partial
        if position < 0:
            step = self._cursor.prev
            if not self.get_partial():

                def start(partial):
                    del partial
                    return self._cursor.last()

            else:

                def start(partial):
                    return self._last_partial(partial)

        else:
            step = self._cursor.next
            if not self.get_partial():

                def start(partial):
                    del partial
                    return self._cursor.first()

            else:

                def start(partial):
                    return self._first_partial(partial)

        # Get record at position relative to start point.
        count = 0
        record = start(get_partial())
        if position < 0:
            while record:
                record = self._cursor.item()
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    offset = int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    offset = 1
                count -= offset
                if count > position:
                    record = step()
                    continue
                record_number = self._get_segment(
                    record[0],
                    int.from_bytes(record[1][:4], byteorder="big"),
                    record[1],
                ).get_record_number_at_position(position - count - offset)
                if record_number is not None:
                    return record[0].decode(), record_number
                break
        else:
            while record:
                record = self._cursor.item()
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    offset = int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    offset = 1
                count += offset
                if count <= position:
                    record = step()
                    continue
                record_number = self._get_segment(
                    record[0],
                    int.from_bytes(record[1][:4], byteorder="big"),
                    record[1],
                ).get_record_number_at_position(position - count + offset)
                if record_number is not None:
                    return record[0].decode(), record_number
                break
        return None

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is None:
            try:
                key, value = self._last()
            except TypeError:
                return None
            return key.decode(), value
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
                        key, value = self._cursor.last()
                    except TypeError:
                        return None
                    return key.decode(), value
                continue
            self._set_range("".join(chars).encode())
            try:
                key, value = self._prev()
            except TypeError:
                return None
            return key.decode(), value

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        if self.get_partial() is False:
            return None
        try:
            nearestkey, nearestvalue = self._set_range(key)
        except TypeError:
            return None
        if self.get_partial() is not None:
            if not nearestkey.startswith(self.get_converted_partial()):
                return None
        return nearestkey.decode(), nearestvalue

    def next(self):
        """Return next record taking partial key into account."""
        if self._current_segment is None:
            return self.first()
        if self.get_partial() is False:
            return None
        try:
            key, value = self._next()
        except TypeError:
            return None
        return key.decode(), value

    def prev(self):
        """Return previous record taking partial key into account."""
        if self._current_segment is None:
            return self.last()
        if self.get_partial() is False:
            return None
        try:
            key, value = self._prev()
        except TypeError:
            return None
        return key.decode(), value

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.

        """
        if self.get_partial() is False:
            return None
        setkey, setvalue = record
        if self.get_partial() is not None:
            if not setkey.startswith(self.get_partial()):
                return None
        try:
            key, value = self._set_both(setkey.encode(), setvalue)
        except TypeError:
            return None
        return key.decode(), value

    def set_partial_key(self, partial):
        """Set partial key and mark current segment as None."""
        self._partial = partial
        self._current_segment = None
        self.current_segment_number = None

    def _get_record(self, record):
        raise DatabaseError("_get_record not implemented")

    def set_current_segment(self, key, reference):
        """Return the recordset segment for key and reference.

        The returned item is a RecordsetSegmentBitarray, RecordsetSegmentInt,
        or RecordsetSegmentList instance, depending on the current
        representation of the segment on the database.

        reference is bytes containing segment number, count of records
        in segment, and a key to access the segment record.

        """
        segment_number = int.from_bytes(reference[:4], byteorder="big")
        if self.current_segment_number == segment_number:
            if key == self._current_segment.index_key:
                return self._current_segment
        segment = self._get_segment(key, segment_number, reference)
        self._current_segment = segment
        self.current_segment_number = segment_number
        return segment

    def _first(self):
        if not self._cursor.first():
            return None
        return self.set_current_segment(*self._cursor.item()).first()

    def _last(self):
        if not self._cursor.last():
            return None
        return self.set_current_segment(*self._cursor.item()).last()

    def _next(self):
        record = self._current_segment.next()
        if record is None:
            if not self._cursor.next():
                return None
            record = self._cursor.item()
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).first()
        return record

    def _prev(self):
        record = self._current_segment.prev()
        if record is None:
            if not self._cursor.prev():
                return None
            record = self._cursor.item()
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).last()
        return record

    def _set_both(self, key, value):
        # segment, record_number = divmod(value, SegmentSize.db_segment_size)
        segment = divmod(value, SegmentSize.db_segment_size)[0]
        cursor = self._transaction.transaction.cursor(db=self._dbset.datastore)
        try:
            record = cursor.set_range(key)
            while record:
                record = cursor.item()
                if record[0] != key:
                    return None
                segment_number = int.from_bytes(record[1][:4], byteorder="big")
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
            key, int.from_bytes(record[1][:4], byteorder="big"), record[1]
        )
        if segment.setat(value) is None:
            return None
        record = self._cursor.set_key_dup(key, record[1])
        if record is None:
            return None
        self._current_segment = segment
        self.current_segment_number = segment_number
        return key, value

    def _set_range(self, key):
        if not self._cursor.set_range(key):
            self._current_segment = None
            self.current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        record = self._cursor.item()
        segment_number = int.from_bytes(record[1][:4], byteorder="big")
        segment = self._get_segment(record[0], segment_number, record[1])
        self._current_segment = segment
        self.current_segment_number = segment_number
        return segment.first()

    def _first_partial(self, partial):
        record = self._cursor.set_range(partial)
        if not record:
            return None
        record = self._cursor.item()
        if not record[0].startswith(partial):
            return None
        return record

    def _last_partial(self, partial):
        record = self._cursor.set_range(partial)
        while record:
            record = self._cursor.item()
            if not record[0].startswith(partial):
                break
            record = self._cursor.next_nodup()
        if not self._cursor.prev():
            return None
        record = self._cursor.item()
        if record[0].startswith(partial):
            return self.set_current_segment(*record).last()
        return None

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        The bitmap for the record set may not match the existence bitmap.

        """
        # See set_selection() hack in chesstab subclasses of DataGrid.
        # It seems not needed by this class.

        # raise DatabaseError('refresh_recordset not implemented')

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        record = self.nearest(key)
        if not record:
            return None
        if record[0].encode() != key:
            return None
        next_record = self.next()
        if next_record:
            if record[0] == next_record[0]:
                raise DatabaseError("Index must refer to unique record")
        return record[1]


class RecordsetCursor(recordsetcursor.RecordsetCursor):
    """Add _get_record method and tranasction support to RecordsetCursor."""

    def __init__(self, recordset, transaction=None, database=None, **kargs):
        """Delegate recordset to superclass.

        Note database and transaction identity.

        kargs absorbs arguments relevant to other database engines.

        """
        del kargs
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
                return record  # maybe (record_number, record)
        segment, recnum = divmod(record_number, SegmentSize.db_segment_size)
        if segment not in dbset.rs_segments:
            return None  # maybe raise
        if recnum not in dbset.rs_segments[segment]:
            return None  # maybe raise
        try:
            record = self._transaction.transaction.get(
                record_number.to_bytes(4, byteorder="big"),
                db=self._database.datastore,
            ).decode()
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

    The database and create arguments are needed to open the Symas LMMD
    database which holds the existence bit map segment records.

    Unresolved is whether, and if so how, the re_pad(..) and set_re_len(..)
    actions need to be replicated.

    """

    def __init__(self, file, database, dbe, db_create):
        """Note file whose existence bitmap is managed."""
        del dbe
        super().__init__(file, database)
        try:
            dbname = SUBFILE_DELIMITER.join((file, EXISTENCE_BITMAP_SUFFIX))
            self.ebm_table = _Datastore(
                database._encoded_database_name(dbname),
                integerkey=False,
                create=db_create,
            )
        except:
            self.ebm_table = None
            raise

    def set_segment_count(self, txn):
        """Set _segment_count to number of entries in datastore."""
        self._segment_count = txn.stat(self.ebm_table.datastore)["entries"]

    def read_exists_segment(self, segment_number, dbtxn):
        """Return existence bitmap for segment_number in database dbenv."""
        # record keys are 0-based converted to bytes.
        # segment_numbers are 0-based.
        ebm = Bitarray()
        ebm.frombytes(
            dbtxn.transaction.get(
                segment_number.to_bytes(4, byteorder="big"),
                db=self.ebm_table.datastore,
            )
        )
        return ebm

    def close(self):
        """Close the table."""
        if self.ebm_table is not None:
            self.ebm_table.close_datastore()
