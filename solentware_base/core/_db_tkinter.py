# _db_tkinter.py
# Copyright 2022 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database with the tkinter module.

There are combinations of Berkeley DB, Python, bsddb3, and berkeleydb,
versions which do not work.  See jcea.es/programacion/pybsddb.htm for
details.

If Berkeley DB has been built with Tcl support it is possible to use the
Tcl interface via tkinter.

In particular on OpenBSD 7.3 where the default Python is 3.10 and 4.6.21
is the version of Berkeley DB.

"""
import os
from ast import literal_eval
import bisect
import re
import subprocess

import sys

from ..db_tcl import tcl_tk_call, TclError
from . import filespec
from .constants import (
    SECONDARY,
    SUBFILE_DELIMITER,
    EXISTENCE_BITMAP_SUFFIX,
    SEGMENT_SUFFIX,
    CONTROL_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    APPLICATION_CONTROL_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    SEGMENT_HEADER_LENGTH,
    FIELDS,
    ACCESS_METHOD,
    HASH,
)
from . import _database
from .bytebit import Bitarray, SINGLEBIT
from .segmentsize import SegmentSize

# Some names are imported '* as _*' to avoid confusion with sensible
# object names within the _db_tkinter module.
# Did not bother about this until pylint with default settings gave
# warnings.
from . import cursor as _cursor
from . import recordsetcursor
from .recordset import (
    RecordsetSegmentBitarray,
    RecordsetSegmentInt,
    RecordsetSegmentList,
    RecordList,
    FoundSet,
)

# DBenv parameter maxlocks may need setting on OpenBSD.
_openbsd_platform = sys.platform.startswith("openbsd")
del sys


class DatabaseError(_database.DatabaseError):
    """Exception for Database class."""


class Database(_database.Database):
    """Define file and record access methods."""

    # Default checkpoint interval after commits.
    _MINIMUM_CHECKPOINT_INTERVAL = 5

    class SegmentSizeError(Exception):
        """Raise when segment size in database is not in specification."""

    def __init__(
        self,
        specification,
        folder=None,
        environment=None,
        segment_size_bytes=DEFAULT_SEGMENT_SIZE_BYTES,
        use_specification_items=None,
        file_per_database=False,
        **soak,
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
        self._file_per_database = bool(file_per_database)
        self._initial_file_per_database = bool(file_per_database)

    def _validate_segment_size_bytes(self, segment_size_bytes):
        if segment_size_bytes is None:
            return
        if not isinstance(segment_size_bytes, int):
            raise DatabaseError("Database segment size must be an int")
        if not segment_size_bytes > 0:
            raise DatabaseError("Database segment size must be more than 0")

    def start_transaction(self):
        """Start transaction if none and bind txn object to self._dbtxn."""
        if self.dbtxn is None:
            # Raise an exception because the emulated _db module method does
            # this (AttributeError).  The _sqlite and _nosql module methods
            # just keep going without starting a transaction.  Perhaps _db
            # should too.
            if self.dbenv is None:
                raise DatabaseError("No environment for start transaction")
            self.dbtxn = tcl_tk_call((self.dbenv, "txn"))

    def backout(self):
        """Abort the active transaction and remove binding to txn object."""
        if self.dbtxn is not None:
            tcl_tk_call((self.dbtxn, "abort"))
            self.dbtxn = None
            tcl_tk_call(
                (
                    self.dbenv,
                    "txn_checkpoint",
                    "-min",
                    str(self._MINIMUM_CHECKPOINT_INTERVAL),
                )
            )

    def commit(self):
        """Commit the active transaction and remove binding to txn object."""
        if self.dbtxn is not None:
            tcl_tk_call((self.dbtxn, "commit"))
            self.dbtxn = None
            tcl_tk_call(
                (
                    self.dbenv,
                    "txn_checkpoint",
                    "-min",
                    str(self._MINIMUM_CHECKPOINT_INTERVAL),
                )
            )

    def file_name_for_database(self, database):
        """Return filename for database.

        Berkeley DB supports one database per file or all databases in
        one file.

        The _db module version returns None if self.database_file is None
        meaning memory-only database.
        """
        if not self._file_per_database:
            if self.database_file is None:
                return ""
            return self.database_file
        if self.home_directory is not None:
            return os.path.join(self.home_directory, database)
        return database

    def _get_log_dir_name(self):
        """Return the log directory name.

        This is needed because the Tcl API does not support the get_lg_dir()
        method.

        Use to generate the "-logdir" parameter when opening environment,
        and when getting list of files to delete when deleting database.

        """
        if self.home_directory is not None:
            return os.path.join(
                self.home_directory,
                "".join(
                    (
                        SUBFILE_DELIMITER * 3,
                        "logs",
                        SUBFILE_DELIMITER,
                        os.path.basename(self.home_directory),
                    )
                ),
            )

        # To cope with log files created for in-memory databases, mainly when
        # running tests.  Deleted in tearDown() method of 'unittest' classes.
        return "".join(
            (
                SUBFILE_DELIMITER * 3,
                "memlogs",
                SUBFILE_DELIMITER,
                "memory_db",
            )
        )

    def open_database(self, dbe, files=None):
        """Open DB environment and specified primary and secondary databases.

        By default all primary databases are opened, but just those named in
        files otherwise, along with their associated secondaries.

        dbe must be the command created by the "package require Db_tcl"
        command executed via the Python tkinter module.

        """
        if self.home_directory is not None:
            try:
                os.mkdir(self.home_directory)
            except FileExistsError:
                if not os.path.isdir(self.home_directory):
                    raise
            all_in_one = set()
            one_per_database = set()
            for name in self.specification:
                dbo = None
                try:
                    dbo = tcl_tk_call(
                        (
                            "berkdb open",
                            "-rdonly",
                            "--",
                            self.database_file,
                            name,
                        )
                    )
                    all_in_one.add(name)
                except TclError:
                    pass
                finally:
                    if dbo:
                        tcl_tk_call((dbo, "close"))
                dbo = None
                try:
                    dbo = tcl_tk_call(
                        (
                            "berkdb open",
                            "-rdonly",
                            "--",
                            os.path.join(self.home_directory, name),
                            name,
                        )
                    )
                    one_per_database.add(name)
                except TclError:
                    pass
                finally:
                    if dbo:
                        tcl_tk_call((dbo, "close"))
            if all_in_one and one_per_database:
                raise DatabaseError(
                    "".join(
                        (
                            "Specified databases exist in both 'one per ",
                            "file' and 'all in one file' style",
                        )
                    )
                )
            if all_in_one:
                self._file_per_database = False
            elif one_per_database:
                self._file_per_database = True
            else:
                self._file_per_database = self._initial_file_per_database
        else:
            self._file_per_database = self._initial_file_per_database
        control = None
        command = [
            "berkdb",
            "open",
            "-dupsort",
            "-btree",
            "-rdonly",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append("--")
        fnfd = self.file_name_for_database(CONTROL_FILE)
        command.append(fnfd)
        command.append(CONTROL_FILE)
        try:
            control = tcl_tk_call(tuple(command))
            try:
                spec_from_db = literal_eval(
                    tcl_tk_call((control, "get", SPECIFICATION_KEY))[0][
                        1
                    ].decode()
                )
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
                segment_size = literal_eval(
                    tcl_tk_call((control, "get", SEGMENT_SIZE_BYTES_KEY))[0][
                        1
                    ].decode()
                )
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
            finally:
                tcl_tk_call((control, "close"))
            db_create = False
            options = []
        except TclError:
            # Assume equivalent to not a DBNoSuchFileError exception if path
            # exists.
            if fnfd and os.path.exists(fnfd):
                raise
            db_create = True
            options = ["-create"]
        finally:
            del control
        self.set_segment_size()
        gbytes = self.environment.get("gbytes", 0)
        bytes_ = self.environment.get("bytes", 0)
        if gbytes or bytes_:
            cachesize = (
                str(self.environment.get("gbytes", 0)),
                str(self.environment.get("bytes", 0)),
                "0",  # "0" or "1" is contiguous memory, "n" > "1" is n chunks.
            )
            options.append("-cachesize")
            options.append(" ".join(cachesize))
        logdir = self._get_log_dir_name()
        if not os.path.exists(logdir):
            os.mkdir(logdir)
        options.append("-log_dir")
        options.append(logdir)

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
        # Dated 28 December 2022.
        # The Tcl interface to Berkeley DB does not support the maxlocks
        # parameter.  Put the argument in <home>/DB_CONFIG file and delete
        # the file immediately after opening the environment.
        # maxobjects needed too on doing deferred updates in a transaction.
        # Both maxobjects and maxlocks had to be set to 120000 to allow a
        # full segment, plus some, to be imported into an empty database.
        # Delete the file first too avoiding mis-use.
        if self.home_directory is not None:
            try:
                os.remove(os.path.join(self.home_directory, "DB_CONFIG"))
            except FileNotFoundError:
                pass
        if _openbsd_platform:
            maxlocks = self.environment.get("maxlocks", 0)
            maxobjects = self.environment.get("maxobjects", 0)

            # encoding added in response to pylint W1514.  It is assumed
            # iso-8859-1 is best here: do not see instruction in Berkeley
            # DB docs.
            if maxlocks:
                with open(
                    os.path.join(self.home_directory, "DB_CONFIG"),
                    mode="w",
                    encoding="iso-8859-1",
                ) as file:
                    file.write(" ".join(("set_lk_max_locks", str(maxlocks))))
                    file.write("\n")
                    file.write(
                        " ".join(("set_lk_max_objects", str(maxobjects)))
                    )

        self._run_db_archive()
        options.append("-home")
        if self.home_directory is not None:
            options.append(self.home_directory)
        else:
            options.append("")
        options.extend(self.environment_flags(dbe))
        self.dbenv = tcl_tk_call(tuple(["berkdb", "env"] + options))
        # log_set_config method not documented in Tcl interface, and indeed
        # is not a supported command, so use the db_archive utility just
        # before opening and after closing environment.
        if self.home_directory is not None:
            try:
                os.remove(os.path.join(self.home_directory, "DB_CONFIG"))
            except FileNotFoundError:
                pass
        if files is None:
            files = self.specification.keys()
        # Set self._dbe earlier than in the bsddb3 and berkeleydb version of
        # this module because start_transaction() needs the dbe attribute,
        # actually a Tcl interpreter, and it seems too disruptive to change
        # the start_transaction arguments in all the other modules too.
        # The apsw and sqlite3 version of this module uses the self.dbenv
        # attribute for this, but in this module self.dbenv has it's natural
        # Berkeley DB meaning.
        self._dbe = dbe
        self.start_transaction()
        command = [
            "berkdb",
            "open",
            "-env",
            self.dbenv,
            "-dupsort",
            "-btree",
        ]
        if db_create:
            command.append("-create")
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend(
            [
                "--",
                self.file_name_for_database(CONTROL_FILE),
                CONTROL_FILE,
            ]
        )
        try:
            self.table[CONTROL_FILE] = tcl_tk_call(tuple(command))
        except TclError:
            self.table[CONTROL_FILE] = None
            raise
        for file, specification in self.specification.items():
            if file not in files:
                continue
            fields = specification[SECONDARY]
            command = [
                "berkdb",
                "open",
                "-env",
                self.dbenv,
                "-recno",
            ]
            if db_create:
                command.append("-create")
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.extend(
                [
                    "--",
                    self.file_name_for_database(file),
                    file,
                ]
            )
            self.table[file] = [None]
            try:
                self.table[file] = tcl_tk_call(tuple(command))
            except TclError:
                self.table[file] = None
                raise
            self.ebm_control[file] = ExistenceBitmapControl(
                file, self, dbe, db_create
            )
            segmentfile = SUBFILE_DELIMITER.join((file, SEGMENT_SUFFIX))
            command = [
                "berkdb",
                "open",
                "-env",
                self.dbenv,
                "-recno",
            ]
            if db_create:
                command.append("-create")
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.extend(
                [
                    "--",
                    self.file_name_for_database(segmentfile),
                    segmentfile,
                ]
            )
            try:
                self.segment_table[file] = tcl_tk_call(tuple(command))
            except TclError:
                self.segment_table[file] = None
                raise
            fieldprops = specification[FIELDS]
            for field, fieldname in fields.items():
                if fieldname is None:
                    fieldname = filespec.FileSpec.field_name(field)
                if fieldprops[fieldname] is None:
                    access_method = "-btree"
                elif ACCESS_METHOD in fieldprops[fieldname]:
                    if fieldprops[fieldname][ACCESS_METHOD] == HASH:
                        access_method = "-hash"
                    else:
                        access_method = "-btree"
                else:
                    access_method = "-btree"
                secondary = SUBFILE_DELIMITER.join((file, field))
                command = [
                    "berkdb",
                    "open",
                    "-env",
                    self.dbenv,
                    "-dupsort",
                    access_method,
                ]
                if db_create:
                    command.append("-create")
                if self.dbtxn:
                    command.extend(["-txn", self.dbtxn])
                command.extend(
                    [
                        "--",
                        self.file_name_for_database(secondary),
                        secondary,
                    ]
                )
                self.table[secondary] = [None]
                try:
                    self.table[secondary] = tcl_tk_call(tuple(command))
                except TclError as exc:
                    if not str(exc).endswith(
                        "".join(
                            (
                                secondary,
                                ": unexpected file type or format",
                            )
                        )
                    ):
                        raise
                    if access_method != "-hash":
                        raise

                    # Accept existing DB_BTREE database if DB_HASH was in the
                    # supplied specification for database.
                    command = [
                        "berkdb open",
                        "-env",
                        self.dbenv,
                        "-dupsort",
                        "-btree",
                        "-txn",
                        self.dbtxn,
                        "--",
                        self.file_name_for_database(secondary),
                        secondary,
                    ]
                    self.table[secondary] = [None]
                    try:
                        self.table[secondary] = tcl_tk_call(tuple(command))
                    except:
                        self.table[secondary] = None
                        raise

                except:
                    self.table[secondary] = None
                    raise
        if db_create:  # and files:
            command = [self.table[CONTROL_FILE], "put"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.extend(
                [SPECIFICATION_KEY, repr(self.specification).encode()]
            )
            tcl_tk_call(tuple(command))
            command = [self.table[CONTROL_FILE], "put"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.extend(
                [
                    SEGMENT_SIZE_BYTES_KEY,
                    repr(self.segment_size_bytes).encode(),
                ]
            )
            tcl_tk_call(tuple(command))
            command = [self.table[CONTROL_FILE], "put"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.extend([APPLICATION_CONTROL_KEY, repr({}).encode()])
            tcl_tk_call(tuple(command))
        self.commit()

    def _run_db_archive(self):
        """Run the db_archive utility to remove redundant log files."""
        if self.home_directory is None:
            return
        # This module is intended for OpenBSD 7.3 where Python 3.10 has
        # become the default making access to Berkeley DB 4.6 impossible
        # via Python packages bsddb3 or berkeleydb.
        # In other cases the archive utility should be run separately as
        # required to keep space taken by log files low.  The name will
        # indicate the Berkeley DB version somehow.
        db_archive_utility = "db4_archive"
        try:
            subprocess.run(
                [
                    os.path.join(
                        os.path.sep, "usr", "local", "bin", db_archive_utility
                    ),
                    "-d",
                ],
                cwd=self._get_log_dir_name(),
                check=False,  # pylint message W1510: maybe should be True?
            )
        except FileNotFoundError as exc:
            if db_archive_utility not in str(exc):
                raise

    def environment_flags(self, dbe):
        """Return environment flags for transaction update."""
        del dbe
        return list(
            self.environment.get("flags", ("-create", "-recover", "-txn"))
        )

    def checkpoint_before_close_dbenv(self):
        """Do a checkpoint call."""
        # Rely on environment_flags() call for transaction state.
        if self.dbtxn is not None:
            tcl_tk_call((self.dbenv, "txn_checkpoint"))

    def close_database_contexts(self, files=None):
        """Close files in database.

        Provided for compatibility with the DPT interface where there is a real
        difference between close_database_contexts() and close_database().

        In Berkeley DB most of the implementation detail is handled by cursors
        created on DB objects bound to elements of dictionaries such as table.

        The files argument is ignored because the DBEnv object is deleted and
        all the DB objects were created in the context of the DBEnv object.

        """
        del files
        for file, specification in self.specification.items():
            if file in self.table:
                if self.table[file] is not None:
                    tcl_tk_call((self.table[file], "close"))
                    self.table[file] = None
            if file in self.segment_table:
                if self.segment_table[file] is not None:
                    tcl_tk_call((self.segment_table[file], "close"))
                    self.segment_table[file] = None
            if file in self.ebm_control:
                if self.ebm_control[file] is not None:
                    tcl_tk_call((self.ebm_control[file].ebm_table, "close"))
                    self.ebm_control[file] = None
            for field in specification[SECONDARY]:
                secondary = SUBFILE_DELIMITER.join((file, field))
                if secondary in self.table:
                    # The bsddb3 and berkeleydb version of this module is
                    # careless about deleting closed secondary database DB
                    # objects in deferred update mode.  Exceptions occur
                    # here if that is allowed to happen with the Tcl API.
                    try:
                        tcl_tk_call((self.table[secondary], "close"))
                    except AttributeError:
                        pass
                    self.table[secondary] = None
        for k, dbo in self.table.items():
            if dbo is not None:
                tcl_tk_call((dbo, "close"))
                self.table[k] = None
        if self.dbenv is not None:
            self.checkpoint_before_close_dbenv()
            # The bsddb3 and berkeleydb version of close environment seems
            # to do a silent close transaction: not sure if it is commit
            # or abort.
            self.backout()
            tcl_tk_call((self.dbenv, "close"))
            self.dbenv = None
            self._run_db_archive()
            self.table = {}
            self.segment_table = {}
            self.ebm_control = {}
        self.segment_size_bytes = self._initial_segment_size_bytes

    def close_database(self):
        """Close primary and secondary databases and environment.

        That means close all DB objects for tables and indicies and the DBEnv
        object defining the environment, and clear all dictionaries and the
        environment references.

        """
        self.close_database_contexts()

    def put(self, file, key, value):
        """Insert key, or replace key, in table for file using value."""
        assert file in self.specification
        command = [self.table[file], "put"]
        if key is None:
            command.append("-append")
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        if key is not None:
            command.append(key)
        command.append(value.encode())
        if key is None:
            return tcl_tk_call(tuple(command))
        tcl_tk_call(tuple(command))
        return None

    def replace(self, file, key, oldvalue, newvalue):
        """Replace key from table for file using newvalue.

        oldvalue is ignored in _sqlite version of replace() method.
        """
        del oldvalue
        assert file in self.specification
        command = [self.table[file], "put"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend([key, newvalue.encode()])
        tcl_tk_call(tuple(command))

    def delete(self, file, key, value):
        """Delete key from table for file.

        value is ignored in _db_tkinter version of delete() method.
        """
        assert file in self.specification
        del value
        command = [self.table[file], "del"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(key)
        try:
            tcl_tk_call(tuple(command))
        except TclError:
            pass

    def get_primary_record(self, file, key):
        """Return primary record (key, value) given primary key on dbset."""
        assert file in self.specification
        if key is None:
            return None
        command = [self.table[file], "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(key)
        record = tcl_tk_call(tuple(command))
        if not record:
            return None
        return key, record[0][1].decode()

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
        """Return lowest freed record number in existence bitmap.

        The list of segments with freed record numbers is searched.
        """
        ebmc = self.ebm_control[dbset]
        if ebmc.freed_record_number_pages is None:
            ebmc.freed_record_number_pages = []
            command = [self.table[CONTROL_FILE], "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            cursor = tcl_tk_call(tuple(command))
            try:
                record = tcl_tk_call((cursor, "get", "-set", ebmc.ebmkey))
                while record:
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[1], byteorder="big")
                    )
                    record = tcl_tk_call((cursor, "get", "-nextdup"))
            finally:
                tcl_tk_call((cursor, "close"))
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
                first_zero_bit = lfrns.index(False, 0 if segment_number else 1)
            except ValueError:
                command = [self.table[CONTROL_FILE], "cursor"]
                if self.dbtxn:
                    command.extend(["-txn", self.dbtxn])
                cursor = tcl_tk_call(tuple(command))
                try:
                    if tcl_tk_call(
                        (
                            cursor,
                            "get",
                            "-get_both",
                            ebmc.ebmkey,
                            segment_number.to_bytes(
                                1 + segment_number.bit_length() // 8,
                                byteorder="big",
                            ),
                        )
                    ):
                        tcl_tk_call((cursor, "del"))
                    else:
                        raise
                finally:
                    tcl_tk_call((cursor, "close"))
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
            command = [self.table[CONTROL_FILE], "cursor"]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            cursor = tcl_tk_call(tuple(command))
            try:
                record = tcl_tk_call((cursor, "get", "-set", ebmc.ebmkey))
                while record:
                    ebmc.freed_record_number_pages.append(
                        int.from_bytes(record[0][1], byteorder="big")
                    )
                    record = tcl_tk_call((cursor, "get", "-nextdup"))
            finally:
                tcl_tk_call((cursor, "close"))
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
        command = [self.table[CONTROL_FILE], "put"]
        # The Tcl API has no equivalent to self.DB_NODUPDATA flag for put.
        # It seems this flag should not be used in bsddb3 put, but is
        # ignored anyway.
        # The "-nooverwrite" option constrains each key to one value.
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend(
            [
                ebmc.ebmkey,
                segment.to_bytes(
                    1 + segment.bit_length() // 8, byteorder="big"
                ),
            ]
        )
        tcl_tk_call(tuple(command))

    def remove_record_from_ebm(self, file, deletekey):
        """Remove deletekey from file's existence bitmap; return key.

        deletekey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(deletekey, SegmentSize.db_segment_size)
        command = [self.ebm_control[file].ebm_table, "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(segment + 1)
        ebmb = tcl_tk_call(tuple(command))
        if not ebmb:
            raise DatabaseError("Existence bit map for segment does not exist")
        ebm = Bitarray()
        ebm.frombytes(ebmb[0][1])
        ebm[record_number] = False
        command = [self.ebm_control[file].ebm_table, "put"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend([segment + 1, ebm.tobytes()])
        tcl_tk_call(tuple(command))
        return segment, record_number

    def add_record_to_ebm(self, file, putkey):
        """Add putkey to file's existence bitmap; return (segment, record).

        putkey is split into segment number and record number within
        segment to form the returned value.
        """
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        command = [self.ebm_control[file].ebm_table, "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(segment + 1)
        ebmb = tcl_tk_call(tuple(command))
        if not ebmb:
            ebm = SegmentSize.empty_bitarray.copy()
        else:
            ebm = Bitarray()
            ebm.frombytes(ebmb[0][1])
        ebm[record_number] = True
        command = [self.ebm_control[file].ebm_table, "put"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend([segment + 1, ebm.tobytes()])
        tcl_tk_call(tuple(command))
        return segment, record_number

    def get_high_record_number(self, file):
        """Return the high existing record number in table for file."""
        command = [self.table[file], "cursor"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            last = tcl_tk_call((cursor, "get", "-last")) or None
            if last is None:
                return None
            return last[0]
        finally:
            tcl_tk_call((cursor, "close"))

    def _get_segment_record_numbers(self, file, reference):
        command = [self.segment_table[file], "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(reference)
        segment_record = tcl_tk_call(tuple(command))
        if not segment_record:
            segment_record = None
        else:
            segment_record = segment_record[0][1]
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
        command = [self.table[secondary], "cursor"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-set_range", key))
            while record:
                record_key, value = record[0]
                if record_key != key:
                    # No index entry for key.
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                segment.to_bytes(4, byteorder="big"),
                                record_number.to_bytes(2, byteorder="big"),
                            )
                        ),
                    )
                    tcl_tk_call(command)
                    return

                segment_number = int.from_bytes(value[:4], byteorder="big")
                if segment_number < segment:
                    record = tcl_tk_call((cursor, "get", "-nextdup"))
                    continue
                if segment_number > segment:
                    # No index entry for key in this segment.
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                segment.to_bytes(4, byteorder="big"),
                                record_number.to_bytes(2, byteorder="big"),
                            )
                        ),
                    )
                    tcl_tk_call(command)
                    return

                if len(value) == SEGMENT_HEADER_LENGTH:
                    existing_record_number = int.from_bytes(
                        value[4:], byteorder="big"
                    )
                    if existing_record_number != record_number:
                        command = [self.segment_table[file], "put", "-append"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.append(
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
                            )
                        )
                        segment_key = tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor, "del"))
                        command = (
                            cursor,
                            "put",
                            "-keylast",
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    b"\x00\x02",
                                    segment_key.to_bytes(4, byteorder="big"),
                                )
                            ),
                        )
                        tcl_tk_call(command)
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
                        command = [
                            self.segment_table[file],
                            "put",
                        ]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend([segment_key, seg.tobytes()])
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor, "del"))
                        command = (
                            cursor,
                            "put",
                            "-keylast",
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                        tcl_tk_call(command)
                    else:
                        command = [
                            self.segment_table[file],
                            "put",
                        ]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend(
                            [
                                segment_key,
                                b"".join(
                                    (
                                        rn.to_bytes(length=2, byteorder="big")
                                        for rn in recnums
                                    )
                                ),
                            ]
                        )
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor, "del"))
                        command = (
                            cursor,
                            "put",
                            "-keylast",
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                        tcl_tk_call(command)
                    return

                # ignore possibility record_number already present
                recnums[record_number] = True
                command = [
                    self.segment_table[file],
                    "put",
                ]
                if self.dbtxn:
                    command.extend(["-txn", self.dbtxn])
                command.extend(
                    [
                        segment_key,
                        recnums.tobytes(),
                    ]
                )
                tcl_tk_call(tuple(command))
                tcl_tk_call((cursor, "del"))
                command = (
                    cursor,
                    "put",
                    "-keylast",
                    key,
                    b"".join(
                        (
                            value[:4],
                            recnums.count().to_bytes(2, byteorder="big"),
                            value[SEGMENT_HEADER_LENGTH:],
                        )
                    ),
                )
                tcl_tk_call(command)
                return

            # No index entry for key because database is empty.
            command = (
                cursor,
                "put",
                "-keylast",
                key,
                b"".join(
                    (
                        segment.to_bytes(4, byteorder="big"),
                        record_number.to_bytes(2, byteorder="big"),
                    )
                ),
            )
            tcl_tk_call(command)

        finally:
            tcl_tk_call((cursor, "close"))

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
        command = [self.table[secondary], "cursor"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-set_range", key))
            while record:
                record_key, value = record[0]
                if record_key != key:
                    # Assume that multiple requests to delete an index value
                    # have been made for a record.  The segment_put method uses
                    # sets to avoid adding multiple entries.  Consider using
                    # set rather than list in the pack method of the subclass
                    # of Value if this will happen a lot.
                    return

                segment_number = int.from_bytes(value[:4], byteorder="big")
                if segment_number < segment:
                    record = tcl_tk_call((cursor, "get", "-nextdup"))
                    continue
                if segment_number > segment:
                    return
                if len(value) == SEGMENT_HEADER_LENGTH:
                    if record_number == int.from_bytes(
                        value[4:], byteorder="big"
                    ):
                        tcl_tk_call((cursor, "del"))
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
                        command = [self.segment_table[file], "del"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.append(segment_key)
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor, "del"))
                        if count:
                            tcl_tk_call((cursor, "put", "-keylast", key, ref))
                    else:
                        command = [self.segment_table[file], "put"]
                        if self.dbtxn:
                            command.extend(["-txn", self.dbtxn])
                        command.extend(
                            [
                                segment_key,
                                b"".join(
                                    (
                                        i.to_bytes(length=2, byteorder="big")
                                        for i in sorted(recnums)
                                    )
                                ),
                            ]
                        )
                        tcl_tk_call(tuple(command))
                        tcl_tk_call((cursor, "del"))
                        command = (
                            cursor,
                            "put",
                            "-keylast",
                            key,
                            b"".join(
                                (
                                    value[:4],
                                    count.to_bytes(2, byteorder="big"),
                                    value[SEGMENT_HEADER_LENGTH:],
                                )
                            ),
                        )
                        tcl_tk_call(command)
                    return

                # ignore possibility record_number already absent
                recnums[record_number] = False

                count = recnums.count()
                if count > SegmentSize.db_lower_conversion_limit:
                    command = [self.segment_table[file], "put"]
                    if self.dbtxn:
                        command.extend(["-txn", self.dbtxn])
                    command.extend([segment_key, recnums.tobytes()])
                    tcl_tk_call(tuple(command))
                    tcl_tk_call((cursor, "del"))
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                value[:4],
                                recnums.count().to_bytes(2, byteorder="big"),
                                value[SEGMENT_HEADER_LENGTH:],
                            )
                        ),
                    )
                    tcl_tk_call(command)
                else:
                    recnums = set(recnums.search(SINGLEBIT))
                    command = [self.segment_table[file], "put"]
                    if self.dbtxn:
                        command.extend(["-txn", self.dbtxn])
                    command.extend(
                        [
                            segment_key,
                            b"".join(
                                (
                                    i.to_bytes(length=2, byteorder="big")
                                    for i in sorted(recnums)
                                )
                            ),
                        ]
                    )
                    tcl_tk_call(tuple(command))
                    tcl_tk_call((cursor, "del"))
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                value[:4],
                                len(recnums).to_bytes(2, byteorder="big"),
                                value[SEGMENT_HEADER_LENGTH:],
                            )
                        ),
                    )
                    tcl_tk_call(command)
                return
        finally:
            tcl_tk_call((cursor, "close"))

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
        command = [self.segment_table[file], "get"]
        # The replaced get() call does not use the txn argument.
        # But surely it should?
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(
            int.from_bytes(
                segment_reference[SEGMENT_HEADER_LENGTH:], byteorder="big"
            )
        )
        segment_record = tcl_tk_call(tuple(command))
        if not segment_record:
            raise DatabaseError("Segment record missing")
        segment_record = segment_record[0][1]
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
        command = [
            self.table[SUBFILE_DELIMITER.join((file, valuespec.field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            if valuespec.above_value and valuespec.below_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.above_value.encode(),
                    )
                )
                if record:
                    if record[0][0] == valuespec.above_value.encode():
                        record = tcl_tk_call((cursor, "get", "-nextnodup"))
                while record:
                    key = record[0][0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.above_value and valuespec.to_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.above_value.encode(),
                    )
                )
                if record:
                    if record[0][0] == valuespec.above_value.encode():
                        record = tcl_tk_call((cursor, "get", "-nextnodup"))
                while record:
                    key = record[0][0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.from_value and valuespec.to_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.from_value.encode(),
                    )
                )
                while record:
                    key = record[0][0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.from_value and valuespec.below_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.from_value.encode(),
                    )
                )
                while record:
                    key = record[0][0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.above_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.above_value.encode(),
                    )
                )
                if record:
                    if record[0][0] == valuespec.above_value.encode():
                        record = tcl_tk_call((cursor, "get", "-nextnodup"))
                while record:
                    key = record[0][0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.from_value:
                record = tcl_tk_call(
                    (
                        cursor,
                        "get",
                        "-set_range",
                        valuespec.from_value.encode(),
                    )
                )
                while record:
                    key = record[0][0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.to_value:
                record = tcl_tk_call((cursor, "get", "-first"))
                while record:
                    key = record[0][0].decode()
                    if key > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            elif valuespec.below_value:
                record = tcl_tk_call((cursor, "get", "-first"))
                while record:
                    key = record[0][0].decode()
                    if key >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
            else:
                record = tcl_tk_call((cursor, "get", "-first"))
                while record:
                    key = record[0][0].decode()
                    if valuespec.apply_pattern_and_set_filters_to_value(key):
                        yield key
                    record = tcl_tk_call((cursor, "get", "-nextnodup"))
        finally:
            tcl_tk_call((cursor, "close"))

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
        command = [
            self.ebm_control[file].ebm_table,
            "get",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(segment_number + 1)
        record = tcl_tk_call(tuple(command))
        if record and record_number in RecordsetSegmentBitarray(
            segment_number, key, records=record[0][1]
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
        # The keys in self.ebm_control[file].ebm_table are always
        # 'segment + 1', see note in recordlist_ebm method.
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
        command = [self.ebm_control[file].ebm_table, "cursor"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            first_segment = None
            final_segment = None
            record = tcl_tk_call(
                tuple((cursor, "get", "-set", segment_start + 1))
            )
            while record:
                segment_number, segment_record = record[0]
                segment_number -= 1
                if segment_number < segment_start:
                    record = tcl_tk_call((cursor, "get", "-next"))
                    continue
                if segment_end is not None and segment_number > segment_end:
                    record = tcl_tk_call((cursor, "get", "-next"))
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
                record = tcl_tk_call((cursor, "get", "-next"))
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
        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return RecordList containing records on file."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        command = [self.ebm_control[file].ebm_table, "cursor"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-first"))
            while record:
                record = record[0]

                # The keys in self.ebm_control[file].ebm_table are always
                # 'segment + 1' because automatically allocated RECNO keys
                # start at 1 in an empty table and segment numbers start at 0.
                # It is not possible to use the actual segment number because
                # 0 is not allowed as a RECNO key.
                recordlist[record[0] - 1] = RecordsetSegmentBitarray(
                    record[0] - 1, None, records=record[1]
                )
                record = tcl_tk_call((cursor, "get", "-next"))

        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def populate_recordset_segment(self, recordset, reference):
        """Populate recordset with segment in reference.

        The segement from reference is added to an existing segment in
        recordset if there is one, or becomes recordset's segment for
        that segment number if not.
        """
        segment_number = int.from_bytes(reference[:4], byteorder="big")
        if len(reference) == SEGMENT_HEADER_LENGTH:
            segment = RecordsetSegmentInt(
                segment_number, None, records=reference[4:]
            )
        else:
            command = [
                self.segment_table[recordset.dbset],
                "get",
            ]
            if self.dbtxn:
                command.extend(["-txn", self.dbtxn])
            command.append(
                int.from_bytes(
                    reference[SEGMENT_HEADER_LENGTH:], byteorder="big"
                )
            )
            segment_record = tcl_tk_call(tuple(command))[0][1]
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
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-first"))
            while record:
                key, value = record[0]
                if matcher.search(key):
                    self.populate_recordset_segment(recordlist, value)
                record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return RecordList on file containing records for field with key."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        if key is None:
            return recordlist
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-set_range", key))
            while record:
                record_key, value = record[0]
                if record_key != key:
                    break
                self.populate_recordset_segment(recordlist, value)
                record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))
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
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-set_range", keystart))
            while record:
                record = record[0]
                if not record[0].startswith(keystart):
                    break
                self.populate_recordset_segment(recordlist, record[1])
                record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1
    ):
        """Return RecordList containing records for field on file.

        Keys are in range set by combinations of ge, gt, le, and lt.
        """
        if isinstance(ge, str) and isinstance(gt, str):
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        if isinstance(le, str) and isinstance(lt, str):
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            if ge is None and gt is None:
                record = tcl_tk_call((cursor, "get", "-first"))
            else:
                record = tcl_tk_call(
                    (cursor, "get", "-set_range", ge or gt or "")
                )
            if gt:
                while record:
                    record0 = record[0]
                    if record0[0] > gt:
                        break
                    record = tcl_tk_call((cursor, "get", "-next"))
            if le is None and lt is None:
                while record:
                    record0 = record[0]
                    self.populate_recordset_segment(recordlist, record0[1])
                    record = tcl_tk_call((cursor, "get", "-next"))
            elif lt is None:
                while record:
                    record0 = record[0]
                    if record0[0] > le:
                        break
                    self.populate_recordset_segment(recordlist, record0[1])
                    record = tcl_tk_call((cursor, "get", "-next"))
            else:
                while record:
                    record0 = record[0]
                    if record0[0] >= lt:
                        break
                    self.populate_recordset_segment(recordlist, record0[1])
                    record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def recordlist_all(self, file, field, cache_size=1):
        """Return RecordList on file containing records for field."""
        recordlist = RecordList(dbhome=self, dbset=file, cache_size=cache_size)
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-first"))
            while record:
                record = record[0]
                self.populate_recordset_segment(recordlist, record[1])
                record = tcl_tk_call((cursor, "get", "-next"))
        finally:
            tcl_tk_call((cursor, "close"))
        return recordlist

    def recordlist_nil(self, file, cache_size=1):
        """Return empty RecordList on file."""
        return RecordList(dbhome=self, dbset=file, cache_size=cache_size)

    def unfile_records_under(self, file, field, key):
        """Delete the reference to records for index field[key].

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            # Delete segment records.
            record = tcl_tk_call((cursor, "get", "-set_range", key))
            while record:
                record = record[0]
                record_key, value = record
                if record_key != key:
                    break
                if len(value) > SEGMENT_HEADER_LENGTH:
                    command = [self.segment_table[file], "del"]
                    if self.dbtxn:
                        command.extend(["-txn", self.dbtxn])
                    command.append(
                        int.from_bytes(
                            value[SEGMENT_HEADER_LENGTH:], byteorder="big"
                        )
                    )
                    tcl_tk_call(tuple(command))

                # Kept so block comment after finally clause makes sense.
                # Not converted to Tcl API.
                # Delete segment references.
                # cursor.delete()

                record = tcl_tk_call((cursor, "get", "-next"))

            # Kept so block comment after finally clause makes sense.
            # Not converted to Tcl API.
            # Delete segment references.
            # try:
            #    self.table[SUBFILE_DELIMITER.join((file, field))
            #               ].delete(key, txn=self.dbtxn)
            # except self._dbe.DBNotFoundError:
            #    pass

        finally:
            tcl_tk_call((cursor, "close"))

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
        command = [self.table[SUBFILE_DELIMITER.join((file, field))], "del"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.append(key)
        tcl_tk_call(tuple((command)))

    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records."""
        assert recordset.dbset == file
        assert file in self.table

        # Delete existing segments for key
        self.unfile_records_under(file, field, key)

        command = [
            self.table[SUBFILE_DELIMITER.join((file, field))],
            "cursor",
        ]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        cursor = tcl_tk_call(tuple(command))
        try:
            recordset.normalize()
            for segment_number in recordset.sorted_segnums:
                if isinstance(
                    recordset.rs_segments[segment_number], RecordsetSegmentInt
                ):
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                segment_number.to_bytes(4, byteorder="big"),
                                recordset.rs_segments[
                                    segment_number
                                ].tobytes(),
                            )
                        ),
                    )
                    tcl_tk_call(command)
                else:
                    count = recordset.rs_segments[
                        segment_number
                    ].count_records()
                    command = [self.segment_table[file], "put", "-append"]
                    if self.dbtxn:
                        command.extend(["-txn", self.dbtxn])
                    command.append(
                        recordset.rs_segments[segment_number].tobytes()
                    )
                    segment_key = tcl_tk_call(tuple(command))
                    command = (
                        cursor,
                        "put",
                        "-keylast",
                        key,
                        b"".join(
                            (
                                segment_number.to_bytes(4, byteorder="big"),
                                count.to_bytes(2, byteorder="big"),
                                segment_key.to_bytes(4, byteorder="big"),
                            )
                        ),
                    )
                    tcl_tk_call(command)
        finally:
            tcl_tk_call((cursor, "close"))

    def database_cursor(self, file, field, keyrange=None, recordset=None):
        """Create and return a cursor on DB() for (file, field).

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
            engine=self._dbe,
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
        """Return True if the DB object for file exists."""
        return self.table[file] is not None

    def get_table_connection(self, file):
        """Return main DB object for file."""
        if self.dbenv:
            return self.table[file]
        return None

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs=None,
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
        # taskmethod(self, logwidget, **taskmethodargs)
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

    def _generate_database_file_name(self, name):
        """Extend, return path to Berkeley DB file for name.

        Delegate to superclass if all databases are in one file.

        Otherwise return stem of name for databases associated with name.

        """
        if not self._file_per_database:
            return super()._generate_database_file_name(name)
        return os.path.join(self.home_directory, name)

    def get_application_control(self):
        """Return dict of application control items."""
        command = [self.table[CONTROL_FILE], "get"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend(
            [
                APPLICATION_CONTROL_KEY,
            ]
        )
        value = tcl_tk_call(tuple(command))
        if value is not None:
            return literal_eval(value.decode())
        return {}

    def set_application_control(self, appcontrol):
        """Set dict of application control items."""
        command = [self.table[CONTROL_FILE], "delete"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend([APPLICATION_CONTROL_KEY])
        tcl_tk_call(tuple(command))
        command = [self.table[CONTROL_FILE], "put"]
        if self.dbtxn:
            command.extend(["-txn", self.dbtxn])
        command.extend([APPLICATION_CONTROL_KEY, repr(appcontrol).encode()])
        tcl_tk_call(tuple(command))


class Cursor(_cursor.Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - bsddb3 DB() object.
    keyrange - not used.
    transaction - the current transaction.
    kargs - absorb argunents relevant to other database engines.

    The wrapped cursor is created on the Berkeley DB database in a File
    instance.

    The transaction argument in the Cursor() call should be a function
    which returns current tranasction active on the Berkeley DB environment,
    or None if there isn't one.  If supplied it's return value is used in all
    calls to methods of the wrapped cursor which have the 'txn' parameter.
    By default the calls are not within a transaction.

    The CursorPrimary and CursorSecondary subclasses define the bsddb3
    style cursor methods peculiar to primary and secondary databases.
    """

    def __init__(
        self,
        dbset,
        keyrange=None,
        transaction=None,
        engine=None,
        **kargs,
    ):
        """Define a cursor on the underlying database engine dbset.

        engine - tkinter module.  Use Tcl interface to Berkeley DB.
        """
        del keyrange, kargs
        super().__init__(dbset)
        self._transaction = transaction
        self._engine = engine
        command = [self._dbset, "cursor"]
        if transaction:
            command.extend(["-txn", transaction])
        # Define a subclass of str with close() method?  See close() below.
        self._cursor = tcl_tk_call(tuple(command))
        self._current_segment = None
        self.current_segment_number = None
        self._current_record_number_in_segment = None

    def close(self):
        """Close database cursor via tkinter then delegate to tidy up.

        Superclass method expects self._cursor to have a close() method
        but here self._cursor is a str naming the tcl command for the
        cursor.

        """
        tcl_tk_call((self._cursor, "close"))
        super().close()

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

        Do nothing in Berkeley DB.  The cursor (for the datagrid) accesses
        database directly.  There are no intervening data structures which
        could be inconsistent.

        """


class CursorPrimary(Cursor):
    """Define a cursor on the underlying database engine dbset.

    dbset - bsddb3 DB() object.
    ebm - bsddb3 DB() object for existence bitmap.
    kargs - superclass arguments and absorb arguments for other engines.

    """

    def __init__(self, dbset, ebm=None, **kargs):
        """Extend, note existence bitmap table and engine."""
        super().__init__(dbset, **kargs)
        self._ebm = ebm

    def count_records(self):
        """Return record count."""
        # No "-txn" option in Tcl API.
        # Not giving "-txn" option gets a _tkinter.TclError exception.
        # Giving the "-txn" option seems to work anyway.
        command = [self._dbset, "stat", "-faststat"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        return ndata(tcl_tk_call(tuple(command)))

    def first(self):
        """Return first record taking partial key into account."""
        return self._decode_record(
            tcl_tk_call((self._cursor, "get", "-first"))
        )

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        # record keys are 1-based but segment_numbers are 0-based.
        if record is None:
            return 0
        segment_number, record_number = divmod(
            record[0], SegmentSize.db_segment_size
        )
        command = [self._ebm, "get"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        segment = tcl_tk_call(tuple(command + [segment_number + 1]))
        if not segment:
            return 0
        position = 0
        for i in range(segment_number):
            segment_ebm = Bitarray()
            segment_ebm.frombytes(tcl_tk_call(tuple(command + [i + 1]))[0][1])
            position += segment_ebm.count()
        segment_ebm = Bitarray()
        segment_ebm.frombytes(segment[0][1])
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
        command = [self._ebm, "cursor"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        ebm_cursor = tcl_tk_call(tuple(command))
        try:
            if position < 0:
                record = tcl_tk_call((ebm_cursor, "get", "-last"))
                while record:
                    segment_ebm = Bitarray()
                    segment_ebm.frombytes(record[0][1])
                    ebm_count = segment_ebm.count()
                    if count + ebm_count < abspos:
                        count += ebm_count
                        record = tcl_tk_call((ebm_cursor, "get", "-prev"))
                        continue
                    recno = segment_ebm.search(SINGLEBIT)[position + count] + (
                        (record[0][0] - 1) * SegmentSize.db_segment_size
                    )
                    return self._decode_record(
                        tcl_tk_call((self._cursor, "get", "-set", recno))
                    )
            else:
                record = tcl_tk_call((ebm_cursor, "get", "-first"))
                while record:
                    segment_ebm = Bitarray()
                    segment_ebm.frombytes(record[0][1])
                    ebm_count = segment_ebm.count()
                    if count + ebm_count < abspos:
                        count += ebm_count
                        record = tcl_tk_call((ebm_cursor, "get", "-next"))
                        continue
                    recno = segment_ebm.search(SINGLEBIT)[
                        position - count - 1
                    ] + ((record[0][0] - 1) * SegmentSize.db_segment_size)
                    return self._decode_record(
                        tcl_tk_call((self._cursor, "get", "-set", recno))
                    )
        finally:
            tcl_tk_call((ebm_cursor, "close"))
        return None

    def last(self):
        """Return last record taking partial key into account."""
        return self._decode_record(tcl_tk_call((self._cursor, "get", "-last")))

    def nearest(self, key):
        """Return nearest record to key taking partial key into account."""
        return self._decode_record(
            tcl_tk_call((self._cursor, "get", "-set_range", key))
        )

    def next(self):
        """Return next record taking partial key into account."""
        return self._decode_record(tcl_tk_call((self._cursor, "get", "-next")))

    def prev(self):
        """Return previous record taking partial key into account."""
        return self._decode_record(tcl_tk_call((self._cursor, "get", "-prev")))

    def setat(self, record):
        """Return current record after positioning cursor at record.

        Take partial key into account.

        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        There seems to be a difference from the Tcl interface: cannot use
        _decode_record() method simply so return (key, value.decode()) or
        None directly.

        """
        key = record[0]
        value = tcl_tk_call((self._cursor, "get", "-set", key))
        if not value:
            return None
        return (key, value[0][1].decode())

    def _decode_record(self, record):
        """Return decoded (key, value) of record."""
        try:
            key, value = record[0]
            return key, value.decode()
        except:
            if not record:
                return None
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

    dbset - bsddb3 DB() object.
    segment - bsddb3 DB() object for segment, list of record numbers or bitmap.
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
            record = tcl_tk_call((self._cursor, "get", "-first"))
            while record:
                record = record[0]
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    count += int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    count += 1
                record = tcl_tk_call((self._cursor, "get", "-next"))
            return count
        count = 0
        record = tcl_tk_call(
            (
                self._cursor,
                "get",
                "-set_range",
                self.get_converted_partial_with_wildcard(),
            )
        )
        while record:
            record = record[0]
            if not record[0].startswith(self.get_converted_partial()):
                break
            if len(record[1]) > SEGMENT_HEADER_LENGTH:
                count += int.from_bytes(record[1][4:6], byteorder="big")
            else:
                count += 1
            record = tcl_tk_call((self._cursor, "get", "-next"))
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
        command = [self._segment, "get"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        command.append(
            int.from_bytes(reference[SEGMENT_HEADER_LENGTH:], byteorder="big")
        )
        records = tcl_tk_call(tuple(command))[0][1]
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
            j = tcl_tk_call((self._cursor, "get", "-first"))
        else:
            j = tcl_tk_call(
                (
                    self._cursor,
                    "get",
                    "-set_range",
                    self.get_converted_partial_with_wildcard(),
                )
            )
        while j:
            j = j[0]
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
            j = tcl_tk_call((self._cursor, "get", "-next"))
        return position

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None

        # Get record at position relative to start point.
        count = 0
        if position < 0:
            if not self.get_partial():  # Replace start() part 1 of 2.
                record = tcl_tk_call((self._cursor, "get", "-last"))
            else:
                record = self._last_partial(self.get_converted_partial())
            step = (self._cursor, "get", "-prev")
            while record:
                record = record[0]
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    offset = int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    offset = 1
                count -= offset
                if count > position:
                    record = tcl_tk_call(step)
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
            if not self.get_partial():  # Replace start() part 2 of 2.
                record = tcl_tk_call((self._cursor, "get", "-first"))
            else:
                record = self._first_partial(self.get_converted_partial())
            step = (self._cursor, "get", "-next")
            while record:
                record = record[0]
                if len(record[1]) > SEGMENT_HEADER_LENGTH:
                    offset = int.from_bytes(record[1][4:6], byteorder="big")
                else:
                    offset = 1
                count += offset
                if count <= position:
                    record = tcl_tk_call(step)
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
                        key, value = tcl_tk_call(
                            (self._cursor, "get", "-last")
                        )
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

        Words used in bsddb3 (Python) to describe set and set_both say
        (key, value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

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
        record = tcl_tk_call((self._cursor, "get", "-first"))
        if not record:
            return None
        return self.set_current_segment(*record[0]).first()

    def _last(self):
        record = tcl_tk_call((self._cursor, "get", "-last"))
        if not record:
            return None
        return self.set_current_segment(*record[0]).last()

    def _next(self):
        record = self._current_segment.next()
        if record is None:
            record = tcl_tk_call((self._cursor, "get", "-next"))
            if not record:
                return None
            record = record[0]
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).first()
        return record

    def _prev(self):
        record = self._current_segment.prev()
        if record is None:
            record = tcl_tk_call((self._cursor, "get", "-prev"))
            if not record:
                return None
            record = record[0]
            if self.get_partial() is not None:
                if not record[0].startswith(self.get_converted_partial()):
                    return None
            return self.set_current_segment(*record).last()
        return record

    def _set_both(self, key, value):
        # segment, record_number = divmod(value, SegmentSize.db_segment_size)
        segment = divmod(value, SegmentSize.db_segment_size)[0]
        command = [self._dbset, "cursor"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        cursor = tcl_tk_call(tuple(command))
        try:
            record = tcl_tk_call((cursor, "get", "-set_range", key))
            while record:
                record = record[0]
                if record[0] != key:
                    return None
                segment_number = int.from_bytes(record[1][:4], byteorder="big")
                if segment_number > segment:
                    return None
                if segment_number == segment:
                    break
                record = tcl_tk_call((cursor, "get", "-next"))
            else:
                return None
        finally:
            tcl_tk_call((cursor, "close"))
        segment = self._get_segment(
            key, int.from_bytes(record[1][:4], byteorder="big"), record[1]
        )
        if segment.setat(value) is None:
            return None
        record = tcl_tk_call(
            (self._cursor, "get", "-get_both", key, record[1])
        )
        if not record:
            return None
        self._current_segment = segment
        self.current_segment_number = segment_number
        return key, value

    def _set_range(self, key):
        record = tcl_tk_call((self._cursor, "get", "-set_range", key))
        if not record:
            self._current_segment = None
            self.current_segment_number = None
            self._current_record_number_in_segment = None
            return None
        segment_number = int.from_bytes(record[0][1][:4], byteorder="big")
        segment = self._get_segment(record[0][0], segment_number, record[0][1])
        self._current_segment = segment
        self.current_segment_number = segment_number
        return segment.first()

    def _first_partial(self, partial):
        record = tcl_tk_call((self._cursor, "get", "-set_range", partial))
        if record is None:
            return None
        if not record[0].startswith(partial):
            return None
        return record

    def _last_partial(self, partial):
        partial_key = partial.encode()
        record = tcl_tk_call((self._cursor, "get", "-set_range", partial_key))
        while record is not None:
            if not record[0].startswith(partial_key):
                break
            record = tcl_tk_call((self._cursor, "get", "-nextnodup"))
        record = tcl_tk_call((self._cursor, "get", "-prev"))
        if record is None:
            return None
        if record[0].startswith(partial_key):
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
        command = [self._database, "get"]
        if self._transaction:
            command.extend(["-txn", self._transaction])
        command.append(record_number)
        try:
            record = tcl_tk_call(tuple(command))[0][1].decode()
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
        """Note file whose existence bitmap is managed."""
        super().__init__(file, database)
        self.ebm_table = None
        dbname = SUBFILE_DELIMITER.join((file, EXISTENCE_BITMAP_SUFFIX))
        command = [
            "berkdb",
            "open",
            "-env",
            database.dbenv,
            "-pad",
            "0",
            "-len",
            str(SegmentSize.db_segment_size_bytes),
            "-recno",
        ]
        if db_create:
            command.append("-create")
        if database.dbtxn:
            command.extend(["-txn", database.dbtxn])
        command.extend(
            [
                "--",
                database.file_name_for_database(dbname),
                dbname,
            ]
        )
        try:
            self.ebm_table = tcl_tk_call(tuple(command))
            # No "-txn" option in Tcl API.
            # Not giving "-txn" option gets a _tkinter.TclError exception.
            # Giving the "-txn" option seems to work anyway.
            command = [self.ebm_table, "stat", "-faststat"]
            if database.dbtxn:
                command.extend(["-txn", database.dbtxn])
            self._segment_count = ndata(tcl_tk_call(tuple(command)))
        except TclError:
            self.ebm_table = None
            raise
        self._dbe = dbe

    def read_exists_segment(self, segment_number, dbtxn):
        """Return existence bitmap for segment_number in database dbenv."""
        # record keys are 1-based but segment_numbers are 0-based.
        command = [self.ebm_table, "get"]
        if dbtxn:
            command.extend(["-txn", dbtxn])
        command.append(segment_number + 1)
        ebm = Bitarray()
        ebm.frombytes(tcl_tk_call(tuple(command))[0][1])
        return ebm

    def close(self):
        """Close the table."""
        if self.ebm_table is not None:
            self.ebm_table.close()
            self.ebm_table = None


def ndata(faststat_str):
    """Return the number of records."""
    return dict(faststat_str)[b"Number of records"]
