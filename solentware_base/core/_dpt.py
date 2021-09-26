# _dpt.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database with the dptdb module.

The database will have been created from the application's customization
of the filespec.FileSpec class.

"""
import os
from ast import literal_eval
import re

from dptdb import dptapi

from . import filespec
from . import cursor
from .constants import (
    SECONDARY,
    DEFAULT_SEGMENT_SIZE_BYTES,
    DPT_SYS_FOLDER,
    FILEDESC,
    DEFAULT_RECORDS,
    BSIZE,
    BRECPPG,
    BTOD_FACTOR,
    BTOD_CONSTANT,
    DSIZE,
    SAFE_DPT_FIELD_LENGTH,
    FILEATTS,
    PRIMARY_FIELDATTS,
    SECONDARY_FIELDATTS,
    DPT_FIELDATTS,
    ONM,
    ORD,
    BRESERVE,
    BREUSE,
    DRESERVE,
    DPGSRES,
    FILEORG,
    FLT,
    INV,
    UAE,
    SPT,
    DPT_PATTERN_CHARS,
)

# from .find_dpt import Find
# from .where_dpt import Where
from .find import Find
from .where import Where
from .findvalues import FindValues
from .wherevalues import WhereValues

FILE_PARAMETER_LIST = (
    "BHIGHPG",
    "BSIZE",
    "DPGSRES",
    "DPGSUSED",
    "DRESERVE",
    "DSIZE",
    "FIFLAGS",
)


class DatabaseError(Exception):
    """Raise when an exceptional case is encountered in Database class."""


class Database:
    """Access a DPT database with transactions enabled by default.

    Direct use of this class is not intended: rather use the Database class
    in the dpt_database or dptdu_database modules which customize this class.

    """

    # Not used by _dpt: segment size follows page size defined by DPT.
    # Present to be compatible with _db and _sqlite modules, where segment size
    # is independent from the page size defined by Berkeley DB or SQLite3.  For
    # these database engines a segment size is assumed when opening existing
    # databases: this exception says 'try again with the segment size extracted
    # from the relevant control record on the database'.  Mostly this will be
    # done without user intervention.
    class SegmentSizeError(Exception):
        """Raise when segment size in database is not in specification.

        Not used by _dpt because the DPT database engine defines it's own
        segment size.  SegmentSizeError is defined for compatibility with
        sibling modules for other database engines which do not use the
        idea of segment size.

        """

    def __init__(
        self,
        specification,
        folder=None,
        segment_size_bytes=None,
        sysfolder=None,
        sysprint=None,
        parms=None,
        msgctl=None,
        audit=None,
        username=None,
        **soak
    ):
        """Create definition of database in folder from specification."""
        if folder is None:
            raise DatabaseError(
                "A directory must be given: DPT does not do memory databases"
            )
        try:
            path = os.path.abspath(folder)
        except:
            msg = " ".join(
                ["Database folder name", str(folder), "is not valid"]
            )
            raise DatabaseError(msg)
        if segment_size_bytes is None:
            segment_size_bytes = DEFAULT_SEGMENT_SIZE_BYTES
        if not isinstance(specification, filespec.FileSpec):
            specification = filespec.FileSpec(**specification)
        self._validate_segment_size_bytes(segment_size_bytes)
        self.home_directory = path
        self.database_file = None
        self.specification = specification
        self.segment_size_bytes = segment_size_bytes
        self.dbenv = None
        self.table = {}
        self.dbtxn = None
        self.index = {}

        # APISequentialFileServices object
        self.sfserv = None

        # The database system parameters. DPT assumes reasonable defaults
        # for any values sought in arguments.
        if sysfolder is None:
            sysfolder = os.path.join(self.home_directory, DPT_SYS_FOLDER)
        if sysprint is None:
            sysprint = os.path.join(sysfolder, "sysprint.txt")
        if parms is None:
            parms = os.path.join(sysfolder, "parms.ini")
        if msgctl is None:
            msgctl = os.path.join(sysfolder, "msgctl.ini")
        if audit is None:
            audit = os.path.join(sysfolder, "audit.txt")
        if username is None:
            username = "dptapi"
        self.sysfolder = sysfolder
        self.sysprint = sysprint
        self.parms = parms
        self.msgctl = msgctl
        self.audit = audit
        self.username = username

    def __del__(self):
        """Close files and destroy dptapi.APIDatabaseServices object."""
        if self.dbenv is None:
            return
        self.close_database()

    def _validate_segment_size_bytes(self, segment_size_bytes):
        if not isinstance(segment_size_bytes, int):
            raise DatabaseError("Database segment size must be an int")
        if not segment_size_bytes > 0:
            raise DatabaseError("Database segment size must be more than 0")

    def start_transaction(self):
        """Start a transaction.

        Do nothing, DPT transactions are started implicitly.

        """

    def backout(self):
        """Backout tranaction."""
        if self.dbenv:
            if self.dbenv.UpdateIsInProgress():
                self.dbenv.Backout()

    def commit(self):
        """Commit tranaction."""
        if self.dbenv:
            if self.dbenv.UpdateIsInProgress():
                self.dbenv.Commit()

    def open_database(self, files=None):
        """Open DPT database.  Just files named in files or all by default."""
        for specification in self.specification.values():
            filedesc = specification[FILEDESC]
            if filedesc[BSIZE] is None:
                records = specification[DEFAULT_RECORDS]
                bsize = int(round(records / filedesc[BRECPPG]))
                if bsize * filedesc[BRECPPG] < records:
                    bsize += 1
                filedesc[BSIZE] = bsize
            if filedesc[DSIZE] is None:
                dsize = int(
                    round(filedesc[BSIZE] * specification[BTOD_FACTOR])
                    + specification[BTOD_CONSTANT]
                )
                filedesc[DSIZE] = dsize
        try:
            os.mkdir(self.home_directory)
        except FileExistsError:
            if not os.path.isdir(self.home_directory):
                raise
        try:
            os.makedirs(self.sysfolder, exist_ok=True)
        except FileExistsError:
            if not os.path.isdir(self.sysfolder):
                raise
        self.create_default_parms()

        # Create #SEQTEMP and checkpoint.ckp in self.sysfolder.
        if self.dbenv is None:
            cwd = os.getcwd()
            os.chdir(self.sysfolder)
            self.dbenv = dptapi.APIDatabaseServices(
                self.sysprint,
                self.username,
                self.parms,
                self.msgctl,
                self.audit,
            )
            os.chdir(cwd)

        if files is None:
            files = self.specification.keys()
        for i, (file, specification) in enumerate(self.specification.items()):
            if file not in files:
                continue
            self.table[file] = self._dptfileclass()(
                dbset=file,
                default_dataset_folder=self.home_directory,
                sfi=i,
                **specification
            )
        for table in self.table.values():
            table.open_file(self.dbenv, dptapi)

    def open_database_contexts(self, files=None):
        """Open all files in normal mode.

        Intended use is to open files to examine file status, or perhaps the
        equivalent of DPT command VIEW TABLES, when the database is closed as
        far as the application subclass of dptbase.Database is concerned.

        The Database Services object, bound to self.dbenv, is assumed to exist.

        """
        if files is None:
            files = self.specification.keys()
        for file in self.specification:
            if file not in files or file not in self.table:
                continue
            self.table[file].open_existing_file(self.dbenv)

    def close_database_contexts(self, files=None):
        """Close files in database.

        The Database Services object, bound to self.dbenv, is assumed to exist.

        The files, by default all those in self.specification, are closed but
        the Database Services object is left open and usable.

        """
        if files is None:
            files = self.specification.keys()
        for file in self.specification:
            if file not in files or file not in self.table:
                continue
            self.table[file].close_file(self.dbenv)

    # Set default parameters for normal use.
    def create_default_parms(self):
        """Create default parms.ini file for normal mode.

        This means transactions are enabled and a large number of DPT buffers.

        """
        if not os.path.exists(self.parms):
            parms = open(self.parms, "w")
            try:
                parms.write("MAXBUF=10000 " + os.linesep)
            finally:
                parms.close()

    def close_database(self):
        """Close DPT database and destroy dptapi.APIDatabaseServices object."""
        if self.dbenv is None:
            return
        self.close_database_contexts()

        # Delete #SEQTEMP and checkpoint.ckp from self.sysfolder.
        cwd = os.getcwd()
        try:
            os.chdir(self.sysfolder)
            self.dbenv.Destroy()
            self.dbenv = None
        finally:
            os.chdir(cwd)

    def get_primary_record(self, file, key):
        """Get instance from file given record number in key.

        The return value is intended to populate an instance of a subclass
        of Record().

        """
        return self.table[file].get_primary_record(key)

    def encode_record_number(self, key):
        """Return repr(key) because this is dptdb version.

        Typically used to convert primary key, a record number, to secondary
        index format.

        """
        return repr(key)

    def decode_record_number(self, skey):
        """Return literal_eval(skey) because this is dptdb version.

        Typically used to convert secondary index reference to primary record,
        a str(int), to a record number.

        """
        return literal_eval(skey)

    def encode_record_selector(self, key):
        """Return key because this is dptdb version.

        Typically used to convert a key being used to search a secondary index
        to the form held on the database.

        """
        return key

    def increase_database_size(self, files=None):
        """Increase file sizes if files nearly full.

        files = {'name':(table_b_count, table_d_count), ...}.

        Method increase_file_size will treat the two numbers as record counts
        and increase Table B and Table D, if necessary, to hold these numbers
        of extra records using the sizing parameters in the FileSpec instance
        for the database.  The value None for a file, "{..., 'name':None, ...}"
        means apply the default increase from the file specification.

        """
        if files is None:
            files = dict()
        for k, table in self.table.items():
            if files and k not in files:
                continue
            table.increase_file_size(
                self.dbenv, sizing_record_counts=files.get(k)
            )

    def initial_database_size(self):
        """Set initial file sizes as specified in file descriptions."""
        for table in self.table.values():
            table.initial_file_size()
        return True

    def get_database_parameters(self, files=None):
        """Return file parameters infomation for file names in files."""
        if files is None:
            files = ()
        sizes = {}
        for file_ in files:
            if file_ in self.table:
                sizes[file_] = self.table[file_].get_file_parameters(
                    self.dbenv
                )
        return sizes

    def get_database_increase(self, files=None):
        """Return required file increases for file names in files."""
        if files is None:
            files = ()
        increases = {}
        dptfiles = self.table
        for file_ in files:
            if file_ in dptfiles:
                increases[file_] = dptfiles[file_].get_tables_increase(
                    self.dbenv, sizing_record_counts=files[file_]
                )
        return increases

    def delete_instance(self, file, instance):
        """Delete instance from file."""
        self.table[file].delete_instance(instance)

    def edit_instance(self, file, instance):
        """Edit an existing instance on file."""
        self.table[file].edit_instance(instance)

    def put_instance(self, file, instance):
        """Add a new instance to file."""
        self.table[file].put_instance(instance)

    # def find_values(self, valuespec, file):
    #    yield self.table[file].find_values(valuespec)

    # Until sure how to make definition above work.
    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""
        # DPT provides two ways of doing this.  The FindValues construct which
        # returns the selected values accessed by a Value Set Cursor, and the
        # direct b-tree cursor construct which walks the database b-tree.
        # This method uses the direct b-tree cursor approach.
        dvcursor = self.table[file].opencontext.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[valuespec.field]
            )
        )
        try:
            dvcursor.SetOptions(dptapi.CURSOR_POSFAIL_NEXT)
            if valuespec.above_value and valuespec.below_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.above_value)
                )
                if dvcursor.Accessible():
                    if (
                        dvcursor.GetCurrentValue().ExtractString()
                        == valuespec.above_value
                    ):
                        dvcursor.Advance()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.above_value and valuespec.to_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.above_value)
                )
                if dvcursor.Accessible():
                    if (
                        dvcursor.GetCurrentValue().ExtractString()
                        == valuespec.above_value
                    ):
                        dvcursor.Advance()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.from_value and valuespec.to_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.from_value)
                )
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.from_value and valuespec.below_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.from_value)
                )
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.above_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.above_value)
                )
                if dvcursor.Accessible():
                    if (
                        dvcursor.GetCurrentValue().ExtractString()
                        == valuespec.above_value
                    ):
                        dvcursor.Advance()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.from_value:
                dvcursor.SetPosition(
                    dptapi.APIFieldValue(valuespec.from_value)
                )
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.to_value:
                dvcursor.GotoFirst()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            elif valuespec.below_value:
                dvcursor.GotoFirst()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if value >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
            else:
                dvcursor.GotoFirst()
                while dvcursor.Accessible():
                    value = dvcursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(value):
                        yield value
                    dvcursor.Advance()
        finally:
            self.table[file].opencontext.CloseDirectValueCursor(dvcursor)

    def allocate_and_open_contexts(self, files=None):
        """Open contexts which had been closed and possibly freed.

        This method is intended for use only when re-opening a file after
        closing it temporarily to ask another thread to increase the size
        of the file.

        """
        # One thread may close contexts temporarily to allow another thread to
        # use the file.  For example a UI thread might delegate a long data
        # import task.  Increasing the file size is an example.
        # The DPT Increase() method can be used only if the file is open in
        # exactly one thread. meaning the file is at points like 'x' but not
        # 'y' in the sequence 'x OpenContext() y CloseContext() x' in all other
        # threads.  A typical file size increase will go 'prepare OpenContext()
        # calculate_increase Increase() work CloseContext() tidy'.  The file is
        # freed by the CloseContext() call because it is the only open context.
        # If the thread, or possibly threads, which closed contexts open them
        # again while the increaser is in it's work phase the file will not be
        # freed by the increaser's CloseContext() call.
        # Note the file will not be freed until the increaser thread finishes:
        # which is why a single thread does not have to allocate the file again
        # every time it closes it's last context on a file.
        for name in files:
            self.table[name].open_existing_file(self.dbenv)

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

    # Cursor instance is created here because there are no other calls to that
    # method.
    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on APIOpenContext() for (file, field).

        keyrange is an addition in DPT. It may yet be removed.

        """
        return Cursor(self.table[file], fieldname=field, keyrange=keyrange)

    def repair_cursor(self, oldcursor, file, field):
        """Return new cursor based on oldcursor with fresh recordset."""
        oldcursor.close()
        return self.database_cursor(file, field)

    # Design flaw:
    # Implication of _CursorDPT definition is create_recordset_cursor and
    # create_recordsetlist_cursor can use dbname to pick recordset or direct
    # value cursor.  When called from get_cursor this fails when dbname is a
    # secondary field: self.table[dbname].primary raised KeyError exception.
    # Changed to self.table[dbset].primary because the secondary versions did
    # not work anyway.
    # Flaw exposed when merging ChessTab modules dptanalysis and analysis;
    # where dptanalysis used create_recordset_cursor and analysis used
    # get_cursor, while both modules failed using the other's method.
    # Also the create_recordset_cursor signatures differ.

    def create_recordset_cursor(self, dbset, dbname, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetCursorDPT(
            self.table[dbset], self.table[dbset].primary, recordset=recordset
        )

    # Only active references in appsuites are in dptdatasourceset module; the
    # version in core.database raises an exception if called.
    def create_recordsetlist_cursor(self, dbset, dbname, keyrange, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetListCursorDPT(
            self.table[dbset], self.table[dbset].primary, recordset=recordset
        )

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs=None,
        use_specification_items=None,
    ):
        """Open new connection to database, run method, then close database.

        This method is intended for use in a separate thread from the one
        dealing with the user interface.  If the normal user interface thread
        also uses a separate thread for it's normal, quick, database actions
        there is probably no need to use this method at all.

        """
        # Works only if sysprint='CONSOLE' as +SYSPRNT is already allocated
        db = self.__class__(
            self.home_directory,
            sysprint="CONSOLE",
            use_specification_items=use_specification_items,
        )
        if db.open_database() is not True:
            return
        if taskmethodargs is None:
            taskmethodargs = {}
        try:
            taskmethod(db, logwidget, **taskmethodargs)
        finally:
            # close_database() invoked by __del__ for db
            # db.close_context()
            pass

    # Comment in chess_ui for make_position_analysis_data_source method, only
    # call, suggests is_database_file_active should not be needed.
    def is_database_file_active(self, file):
        """Return True if the SQLite database connection exists.

        SQLite version of method ignores file argument.

        """
        return bool(self.table[file].opencontext)

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

    def get_table_connection(self, file):
        """Return OpenContext object for file."""
        return self.table[file].opencontext

    def _dptfileclass(self):
        return _DPTFile

    # The make_recordset_* methods should first take a FD recordset to lock
    # records while evaluating.  Perhaps _DPTFile or _CursorDPT foundset_*
    # methods usable.

    def recordlist_record_number(self, file, key=None, cache_size=1):
        """Return _DPTRecordList on file containing records for key.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if key is None:
            return recordlist
        foundset = _DPTFoundSet(
            dptfile,
            dptfile.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_SINGLEREC, key)
            ),
        )
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1
    ):
        """Return _DPTRecordList on file for records which exist in key range.

        keystart and keyend define the range of record numbers to be
        searched.

        cache_size is not relevant to DPT.
        """
        if keystart is None and keyend is None:
            return self.recordlist_ebm(file, cache_size=cache_size)
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if keystart is None:
            keystart = 0
        spec = dptapi.APIFindSpecification(dptapi.FD_POINT, keystart)
        if keyend is not None:
            spec &= dptapi.APIFindSpecification(dptapi.FD_NOT_POINT, keyend)
        foundset = _DPTFoundSet(dptfile, dptfile.FindRecords(spec))
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return _DPTRecordList on file for records which exist.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        foundset = _DPTFoundSet(
            dptfile,
            dptfile.FindRecords(
                dptapi.APIFindSpecification(
                    self.table[file].dpt_field_names[self.table[file].primary],
                    dptapi.FD_ALLRECS,
                    dptapi.APIFieldValue(""),
                )
            ),
        )
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return _DPTRecordList on file containing database records for field.

        The chosen keys contain keylike.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if keylike is None:
            return recordlist
        foundset = dptfile.FindRecords(
            dptapi.APIFindSpecification(
                self.table[file].dpt_field_names[self.table[file].primary],
                dptapi.FD_ALLRECS,
                dptapi.APIFieldValue(""),
            )
        )
        matcher = re.compile(".*?" + keylike, flags=re.IGNORECASE | re.DOTALL)
        dvcursor = dptfile.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[field]
            )
        )
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            value = dvcursor.GetCurrentValue()
            if matcher.match(value.ExtractString()):
                vfs = _DPTFoundSet(
                    dptfile,
                    dptfile.FindRecords(
                        dptapi.APIFindSpecification(
                            self.table[file].secondary[field],
                            dptapi.FD_EQ,
                            dptapi.APIFieldValue(value),
                        ),
                        foundset,
                    ),
                )
                # recordlist.Place(vfs)
                recordlist |= vfs
                # dptfile.DestroyRecordSet(vfs)
            dvcursor.Advance(1)
        dptfile.CloseDirectValueCursor(dvcursor)
        dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key(self, file, field, key=None, cache_size=1):
        """Return _DPTRecordList on file containing records for field with key.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if key is None:
            return recordlist
        foundset = _DPTFoundSet(
            dptfile,
            dptfile.FindRecords(
                dptapi.APIFindSpecification(
                    self.table[file].secondary[field],
                    dptapi.FD_EQ,
                    dptapi.APIFieldValue(self.encode_record_selector(key)),
                ),
                dptapi.FD_LOCK_SHR,
            ),
        )
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1
    ):
        """Return _DPTRecordList on file containing records for field.

        The selected records have keys starting keystart.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if keystart is None:
            return recordlist
        foundset = dptfile.FindRecords(
            dptapi.APIFindSpecification(
                self.table[file].dpt_field_names[self.table[file].primary],
                dptapi.FD_ALLRECS,
                dptapi.APIFieldValue(""),
            )
        )
        dvcursor = dptfile.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[field]
            )
        )
        dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        dvcursor.SetRestriction_LoLimit(dptapi.APIFieldValue(keystart), True)
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            value = dvcursor.GetCurrentValue()
            if not value.ExtractString().startswith(keystart):
                break
            vfs = _DPTFoundSet(
                dptfile,
                dptfile.FindRecords(
                    dptapi.APIFindSpecification(
                        self.table[file].secondary[field],
                        dptapi.FD_EQ,
                        dptapi.APIFieldValue(value),
                    ),
                    foundset,
                ),
            )
            # recordlist.Place(vfs)
            recordlist |= vfs
            # dptfile.DestroyRecordSet(vfs)
            dvcursor.Advance(1)
        dptfile.CloseDirectValueCursor(dvcursor)
        dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1
    ):
        """Return _DPTRecordList on file containing records for field.

        The keys in range are set by combinations of ge, gt, le, and lt.

        cache_size is not relevant to DPT.
        """
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        if le and lt:
            raise DatabaseError("Both 'le' and 'lt' given in key range")
        if ge is None and gt is None and le is None and lt is None:
            return self.recordlist_all(file, field, cache_size=cache_size)
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        if le is None and lt is None:
            foundset = _DPTFoundSet(
                dptfile,
                dptfile.FindRecords(
                    dptapi.APIFindSpecification(
                        self.table[file].secondary[field],
                        dptapi.FD_GE if ge else dptapi.FD_GT,
                        dptapi.APIFieldValue(ge or gt),
                    )
                ),
            )
        elif ge is None and gt is None:
            foundset = _DPTFoundSet(
                dptfile,
                dptfile.FindRecords(
                    dptapi.APIFindSpecification(
                        self.table[file].secondary[field],
                        dptapi.FD_LE if le else dptapi.FD_LT,
                        dptapi.APIFieldValue(le or lt),
                    )
                ),
            )
        else:
            if ge:
                range_ = dptapi.FD_RANGE_GE_LE if le else dptapi.FD_RANGE_GE_LT
            else:
                range_ = dptapi.FD_RANGE_GT_LE if le else dptapi.FD_RANGE_GT_LT
            foundset = _DPTFoundSet(
                dptfile,
                dptfile.FindRecords(
                    dptapi.APIFindSpecification(
                        self.table[file].secondary[field],
                        range_,
                        dptapi.APIFieldValue(ge or gt),
                        dptapi.APIFieldValue(le or lt),
                    )
                ),
            )
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_all(self, file, field, cache_size=1):
        """Return _DPTRecordList on file containing records for field.

        cache_size is not relevant to DPT.
        """
        dptfile = self.get_table_connection(file)
        recordlist = _DPTRecordList(dptfile)
        foundset = _DPTFoundSet(
            dptfile,
            dptfile.FindRecords(
                dptapi.APIFindSpecification(
                    self.table[file].secondary[field],
                    dptapi.FD_GE,
                    dptapi.APIFieldValue(self.encode_record_selector("")),
                ),
                dptapi.FD_LOCK_SHR,
            ),
        )
        # recordlist.Place(foundset)
        recordlist |= foundset
        # dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_nil(self, file, cache_size=1):
        """Return empty _DPTRecordList on file.

        cache_size is not relevant to DPT.
        """
        return _DPTRecordList(self.get_table_connection(file))

    def unfile_records_under(self, file, field, key):
        """Delete the reference to records for index field[key].

        The existing reference by key, usually created by file_records_under,
        is deleted.

        """
        dptfile = self.get_table_connection(file)
        value = dptapi.APIFieldValue(self.encode_record_selector(key))
        sfield = self.table[file].secondary[field]
        foundset = dptfile.FindRecords(
            dptapi.APIFindSpecification(sfield, dptapi.FD_EQ, value),
            dptapi.FD_LOCK_EXCL,
        )
        rscursor = foundset.OpenCursor()
        rscursor.GotoFirst()
        while rscursor.Accessible():
            rscursor.AccessCurrentRecordForReadWrite().DeleteFieldByValue(
                sfield, value
            )
            rscursor.Advance(1)
        foundset.CloseCursor(rscursor)
        dptfile.DestroyRecordSet(foundset)

    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records.

        recordset must be a _DPTRecordSet, or subclass, instance.

        """
        dptfile = self.get_table_connection(file)
        dptfile.FileRecordsUnder(
            recordset.recordset,
            self.table[file].secondary[field],
            dptapi.APIFieldValue(self.encode_record_selector(key)),
        )


class _DPTFile:
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    There is too much 'per file' state to conveniently manage DPT files in the
    Database class.

    The sibling modules for Berkeley DB and SQLite3 do not have classes like
    _DPTFile.  (They used to have such but it seems simpler there without.)

    """

    def __init__(
        self,
        primary=None,
        ddname=None,
        file=None,
        secondary=None,
        fields=None,
        default_records=None,
        filedesc=None,
        btod_factor=None,
        btod_constant=None,
        dpt_primary_field_length=None,
        folder=None,
        default_increase_factor=None,
        dbset=None,
        default_dataset_folder=None,
        sfi=None,
    ):
        self._dbe = None
        self.fieldvalue = None
        self._putrecordcopy = None
        self.opencontext = None
        self.primary = primary
        self.ddname = ddname
        self.default_records = default_records
        self.btod_factor = btod_factor
        self.btod_constant = btod_constant
        if dpt_primary_field_length is None:
            self.dpt_primary_field_length = SAFE_DPT_FIELD_LENGTH
        else:
            self.dpt_primary_field_length = dpt_primary_field_length
        if folder is None:
            folder = default_dataset_folder
        self.file = os.path.join(folder, file)
        self.default_increase_factor = default_increase_factor
        self.dbset = dbset
        self.default_dataset_folder = default_dataset_folder
        self.sfi = sfi
        self.secondary = {}
        if secondary is not None:
            for k, dptname in secondary.items():
                self.secondary[k] = dptname if dptname is not None else k
        if filedesc is not None:
            self.filedesc = FILEATTS.copy()
            for attr in filedesc:
                self.filedesc[attr] = filedesc[attr]
        else:
            self.filedesc = None
        self.fields = {}
        self.dpt_field_names = {}
        self.pyappend = dict()
        if fields is not None:
            for fieldname in fields:
                if primary == fieldname:
                    fieldatts = PRIMARY_FIELDATTS
                else:
                    fieldatts = SECONDARY_FIELDATTS
                self.fields[fieldname] = dict()
                for attr in DPT_FIELDATTS:
                    self.fields[fieldname][attr] = fieldatts[attr]
                description = fields[fieldname]
                if description is None:
                    description = dict()
                for attr in description:
                    if attr in DPT_FIELDATTS:
                        self.fields[fieldname][attr] = description[attr]

        # Conversion of specification fieldname to DPT field name is
        # not consistent throughout these modules when calling
        # foundset_field_equals_value() or foundset_all_records().
        # Patch problem by including identity map for DPT field name.
        if secondary is not None:
            for k, dptname in secondary.items():
                if dptname is None:
                    self.dpt_field_names[k] = k[:1].upper() + k[1:]
                else:
                    self.dpt_field_names[k] = dptname
        if fields is not None:
            for fieldname in fields:
                if fieldname not in self.dpt_field_names:
                    self.dpt_field_names[fieldname] = fieldname

    def close_file(self, dbenv):
        """Close file if open.

        Any recordsets on the file are destroyed, the context is closed,
        and the file's dataset is 'free'd.
        """
        if self.opencontext is None:
            return
        self.opencontext.DestroyAllRecordSets()
        dbenv.CloseContext(self.opencontext)
        self.opencontext = None
        dbenv.Free(self.ddname)

    def open_file(self, dbenv, dbe):
        """Open file, after creation if file's dataset does not exist.

        os.path.exists() determines if file's dataset exists.

        If the dataset does not exist, the file is allocated in a mode
        which allows it to be created and then freed and allocated
        again in a mode for normal use.
        """
        # Create the file if it does not exist.
        foldername, filename = os.path.split(self.file)
        if os.path.exists(foldername):
            if not os.path.isdir(foldername):
                msg = " ".join([foldername, "exists but is not a folder"])
                raise DatabaseError(msg)
        else:
            os.makedirs(foldername)
        if not os.path.exists(self.file):
            dbenv.Allocate(self.ddname, self.file, dbe.FILEDISP_COND)
            dbenv.Create(
                self.ddname,
                self.filedesc[BSIZE],
                self.filedesc[BRECPPG],
                self.filedesc[BRESERVE],
                self.filedesc[BREUSE],
                self.filedesc[DSIZE],
                self.filedesc[DRESERVE],
                self.filedesc[DPGSRES],
                self.filedesc[FILEORG],
            )
            context_specification = dbe.APIContextSpecification(self.ddname)
            open_context = dbenv.OpenContext(context_specification)
            open_context.Initialize()
            for field in self.fields:
                attributes = dbe.APIFieldAttributes()
                fld = self.fields[field]
                if fld[FLT]:
                    attributes.SetFloatFlag()
                if fld[INV]:
                    attributes.SetInvisibleFlag()
                if fld[UAE]:
                    attributes.SetUpdateAtEndFlag()
                if fld[ORD]:
                    attributes.SetOrderedFlag()
                if fld[ONM]:
                    attributes.SetOrdNumFlag()
                attributes.SetSplitPct(fld[SPT])
                open_context.DefineField(
                    self.dpt_field_names[field], attributes
                )
            dbenv.CloseContext(open_context)
            dbenv.Free(self.ddname)
        if not os.path.isfile(self.file):
            msg = " ".join([self.file, "exists but is not a file"])
            raise DatabaseError(msg)

        for field in self.fields:
            fld = self.fields[field]
            if fld[ONM]:
                self.pyappend[field] = dbe.pyAppendDouble
            elif fld[ORD]:
                self.pyappend[field] = dbe.pyAppendStdString

        # Open the file for normal use.
        self._dbe = dbe
        self.open_existing_file(dbenv)

        # Permanent instances for efficient file updates.
        self.fieldvalue = dbe.APIFieldValue()
        self._putrecordcopy = dbe.APIStoreRecordTemplate()

    def open_existing_file(self, dbenv):
        """Allocate file and open a context if the file exists."""
        dbenv.Allocate(self.ddname, self.file, self._dbe.FILEDISP_OLD)
        context_specification = self._dbe.APIContextSpecification(self.ddname)
        self.opencontext = self._open_context(dbenv, context_specification)

    def initial_file_size(self):
        """Set the initial table B and table D sizes.

        Defaults are used if the specification says nothing.

        Defaults are likely to be too small to be usable, or wasteful
        of space if not.
        """
        if not os.path.exists(self.file):
            filedesc = self.filedesc
            if filedesc[BSIZE] is None:
                records = self.default_records
                bsize = int(round(records / filedesc[BRECPPG]))
                if bsize * filedesc[BRECPPG] < records:
                    bsize += 1
                dsize = int(
                    round(bsize * self.btod_factor) + self.btod_constant
                )
                filedesc[BSIZE] = bsize
                filedesc[DSIZE] = dsize
        return True

    def increase_file_size(self, dbserv, sizing_record_counts=None):
        """Increase file size using sizing_record_counts.

        sizing_record_counts gives two numbers, record counts for
        table B and table D respectively, which determine the size of
        the two tables after the increase in size.
        """
        if self.opencontext is not None:
            table_b_needed, table_d_needed = self.get_tables_increase(
                dbserv, sizing_record_counts=sizing_record_counts
            )
            if len(self.get_extents()) % 2:
                if table_b_needed:
                    self.opencontext.Increase(table_b_needed, False)
                if table_d_needed:
                    self.opencontext.Increase(table_d_needed, True)
            elif table_d_needed:
                self.opencontext.Increase(table_d_needed, True)
                if table_b_needed:
                    self.opencontext.Increase(table_b_needed, False)
            elif table_b_needed:
                self.opencontext.Increase(table_b_needed, False)

    def increase_size_of_full_file(self, dbserv, size_before, size_filled):
        """Increase file size taking file full into account.

        Intended for use when the required size to do a deferred update has
        been estimated and the update fills a file.  Make Table B and, or,
        Table D free space at least 20% bigger before trying again.

        It is the caller's responsibility to manage the backups needed, and
        the collection of 'view tables' information, to enable effective use
        of this method.

        """
        b_diff_imp = size_filled["BSIZE"] - size_before["BSIZE"]
        d_diff_imp = size_filled["DSIZE"] - size_before["DSIZE"]
        b_spare = size_before["BSIZE"] - max((0, size_before["BHIGHPG"]))
        d_spare = size_before["DSIZE"] - size_before["DPGSUSED"]
        b_filled = size_filled["FIFLAGS"] & dptapi.FIFLAGS_FULL_TABLEB
        d_filled = size_filled["FIFLAGS"] & dptapi.FIFLAGS_FULL_TABLED
        deferred = size_filled["FISTAT"][0] & dptapi.FISTAT_DEFERRED_UPDATES
        broken = size_filled["FISTAT"][0] & dptapi.FISTAT_PHYS_BROKEN
        if b_filled:
            b_increase = ((b_diff_imp + b_spare) * 6) // 5
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) // 5)),
                int(b_increase * self.btod_factor - d_spare + 1),
            )
        elif d_filled:
            b_increase = b_diff_imp
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) // 5)),
                int(b_increase * self.btod_factor - d_spare + 1),
            )
        elif deferred:
            if broken:
                b_increase = 0
                d_increase = max(
                    ((((d_diff_imp + d_spare) * 6) // 5)),
                    int(b_increase * self.btod_factor - d_spare + 1),
                )
            else:
                b_increase = b_diff_imp
                d_increase = d_diff_imp
        else:
            b_increase = 0
            d_increase = 0
        if b_increase > 0 and d_increase > 0:
            if len(self.get_extents()) % 2:
                self.opencontext.Increase(b_increase, False)
                self.opencontext.Increase(d_increase, True)
            else:
                self.opencontext.Increase(d_increase, True)
                self.opencontext.Increase(b_increase, False)
        elif b_increase > 0:
            self.opencontext.Increase(b_increase, False)
        elif d_increase > 0:
            self.opencontext.Increase(d_increase, True)

    def calculate_table_b_increase(
        self,
        unused=None,
        increase=None,
    ):
        """Return the number of pages to add to DPT file data area.

        unused - current spare pages in Table B or None
        increase - number of extra records or None
        """
        if unused is not None:
            unused = unused * self.filedesc[BRECPPG]
        if unused is None:
            if increase is not None:
                return increase
        elif increase is not None:
            if increase > unused:
                return increase
        increase = int(
            (1 + self.default_records) * self.default_increase_factor
        )
        if unused is None:
            return increase
        if increase > unused:
            return increase - unused
        return 0

    def calculate_table_d_increase(
        self,
        unused=None,
        increase=None,
        table_b_increase=None,
    ):
        """Return the number of pages to add to DPT file index area.

        unused - current spare pages in Table D or None
        increase - number of extra records or None
        table_b_increase - increase index to match extra data pages if not
                           None.
        """
        if unused is not None:
            unused = (unused * self.filedesc[BRECPPG]) // self.btod_factor
        if table_b_increase is None:
            if unused is None:
                if increase is not None:
                    return increase
            elif increase is not None:
                if increase > unused:
                    return increase
            increase = int(
                (1 + self.default_records) * self.default_increase_factor
            )
            if unused is not None:
                if increase > unused:
                    return increase - unused
        else:
            increase = int(table_b_increase * self.filedesc[BRECPPG])
            if unused is not None:
                if increase > unused:
                    return increase
        if unused is None:
            return increase
        return 0

    def get_tables_increase(self, dbserv, sizing_record_counts=None):
        """Return tuple (Table B, Table D) increase needed or None."""
        if self.opencontext is not None:
            parameter = self.get_file_parameters(dbserv)
            b_size, b_used, d_size, d_used = (
                parameter["BSIZE"],
                max(0, parameter["BHIGHPG"]),
                parameter["DSIZE"],
                parameter["DPGSUSED"],
            )
            if sizing_record_counts is None:
                increase_record_counts = (
                    self.calculate_table_b_increase(unused=(b_size - b_used)),
                    self.calculate_table_d_increase(unused=(d_size - d_used)),
                )
            else:
                increase_record_counts = (
                    self.calculate_table_b_increase(
                        unused=(b_size - b_used),
                        increase=sizing_record_counts[0],
                    ),
                    self.calculate_table_d_increase(
                        unused=(d_size - d_used),
                        increase=sizing_record_counts[1],
                    ),
                )
            return (
                increase_record_counts[0] // self.filedesc[BRECPPG],
                int(
                    (increase_record_counts[1] * self.btod_factor)
                    // self.filedesc[BRECPPG]
                ),
            )
        return None

    def get_file_parameters(self, dbserv):
        """Get current values of selected file parameters."""
        viewer_resetter = dbserv.Core().GetViewerResetter()
        parameter = dict()
        parameter["FISTAT"] = (
            viewer_resetter.ViewAsInt("FISTAT", self.opencontext),
            viewer_resetter.View("FISTAT", self.opencontext),
        )
        for name in FILE_PARAMETER_LIST:
            parameter[name] = viewer_resetter.ViewAsInt(name, self.opencontext)
        for name in (dptapi.FIFLAGS_FULL_TABLEB, dptapi.FIFLAGS_FULL_TABLED):
            parameter[name] = bool(parameter["FIFLAGS"] & name)
        return parameter

    def get_extents(self):
        """Get current extents for file."""
        extents = dptapi.IntVector()
        self.opencontext.ShowTableExtents(extents)
        return extents

    def _open_context(self, dbenv, context_specification):
        return dbenv.OpenContext(context_specification)

    def get_primary_record(self, key):
        """Return (key, value) or None given the record number in key."""
        if key is None:
            return None
        foundset = self.foundset_record_number(key)
        rscursor = foundset.recordset.OpenCursor()
        try:
            if rscursor.Accessible():
                record = (
                    key,
                    self.join_primary_field_occurrences(
                        rscursor.AccessCurrentRecordForRead()
                    ),
                )
            else:
                record = None
        finally:
            foundset.recordset.CloseCursor(rscursor)
            # self.opencontext.DestroyRecordSet(foundset)
        return record

    def join_primary_field_occurrences(self, record):
        """Return concatenated occurrences of field holding record value."""
        advance = record.AdvanceToNextFVPair
        fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        primary = self.dpt_field_names[self.primary]
        value = []
        while advance():
            if fieldocc() == primary:
                value.append(valueocc().ExtractString())
        return "".join(value)

    def delete_instance(self, instance):
        """Delete an existing instance from database."""
        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.delete_instance() method.
        instance.srkey = repr(instance.key.pack())

        instance.set_packed_value_and_indexes()
        sri = instance.srindex
        sec = self.secondary
        dcb = instance.deletecallbacks
        fieldvalue = self.fieldvalue
        assign = fieldvalue.Assign
        foundset = self.foundset_record_number(instance.key.pack())
        rscursor = foundset.recordset.OpenCursor()
        while rscursor.Accessible():
            current = rscursor.AccessCurrentRecordForReadWrite()
            for indexname in sri:
                if indexname in dcb:
                    dcb[indexname](instance, sri[indexname])
                else:
                    fieldname = self.dpt_field_names[sec[indexname]]
                    for value in sri[indexname]:
                        assign(value)
                        current.DeleteFieldByValue(fieldname, fieldvalue)
            current.Delete()
            rscursor.Advance(1)
        foundset.recordset.CloseCursor(rscursor)
        # self.opencontext.DestroyRecordSet(foundset)

    def edit_instance(self, instance):
        """Edit an existing instance on database."""
        if instance.key != instance.newrecord.key:
            self.delete_instance(instance)
            self.put_instance(instance.newrecord)
            return

        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.edit_instance() method.
        instance.srkey = repr(instance.key.pack())
        instance.newrecord.srkey = repr(instance.newrecord.key.pack())

        instance.set_packed_value_and_indexes()
        instance.newrecord.set_packed_value_and_indexes()
        nsrv = instance.newrecord.srvalue
        sri = instance.srindex
        nsri = instance.newrecord.srindex
        dcb = instance.deletecallbacks
        ndcb = instance.newrecord.deletecallbacks
        pcb = instance.putcallbacks
        npcb = instance.newrecord.putcallbacks
        ionly = []
        nionly = []
        iandni = []
        for indexname in sri:
            if indexname in nsri:
                iandni.append(indexname)
            else:
                ionly.append(indexname)
        for indexname in nsri:
            if indexname not in sri:
                nionly.append(indexname)
        sec = self.secondary
        fieldvalue = self.fieldvalue
        assign = fieldvalue.Assign
        foundset = self.foundset_record_number(instance.key.pack())
        rscursor = foundset.recordset.OpenCursor()
        safe_length = self.dpt_primary_field_length
        while rscursor.Accessible():
            current = rscursor.AccessCurrentRecordForReadWrite()
            fieldname = self.dpt_field_names[self.primary]
            current.DeleteEachOccurrence(fieldname)
            for i in range(0, len(nsrv), safe_length):
                assign(nsrv[i : i + safe_length])
                current.AddField(fieldname, fieldvalue)
            for indexname in ionly:
                if indexname in dcb:
                    dcb[indexname](instance, sri[indexname])
                else:
                    fieldname = self.dpt_field_names[sec[indexname]]
                    for value in sri[indexname]:
                        assign(value)
                        current.DeleteFieldByValue(fieldname, fieldvalue)
            for indexname in nionly:
                if indexname in npcb:
                    npcb[indexname](instance, sri[indexname])
                else:
                    fieldname = self.dpt_field_names[sec[indexname]]
                    for value in nsri[indexname]:
                        assign(value)
                        current.AddField(fieldname, fieldvalue)
            for indexname in iandni:
                if indexname in dcb:
                    dcb[indexname](instance, sri[indexname])
                    npcb[indexname](instance.newrecord, nsri[indexname])
                else:
                    fieldname = self.dpt_field_names[sec[indexname]]
                    for value in sri[indexname]:
                        assign(value)
                        current.DeleteFieldByValue(fieldname, fieldvalue)
                    for value in nsri[indexname]:
                        assign(value)
                        current.AddField(fieldname, fieldvalue)
            rscursor.Advance(1)
        foundset.recordset.CloseCursor(rscursor)
        # self.opencontext.DestroyRecordSet(foundset)

    def put_instance(self, instance):
        """Put new instance on database."""
        instance.set_packed_value_and_indexes()
        recordcopy = self._putrecordcopy
        pyappendstdstring = self._dbe.pyAppendStdString
        fieldvalue = self.fieldvalue
        srv = instance.srvalue
        fieldname = self.dpt_field_names[self.primary]
        safe_length = self.dpt_primary_field_length
        for i in range(0, len(srv), safe_length):
            pyappendstdstring(
                recordcopy, fieldname, fieldvalue, srv[i : i + safe_length]
            )
        sri = instance.srindex
        sec = self.secondary
        pcb = instance.putcallbacks
        for indexname in sri:
            if indexname not in pcb:
                fieldname = self.dpt_field_names[sec[indexname]]
                pyappend = self.pyappend[fieldname]
                for value in sri[indexname]:
                    pyappend(recordcopy, fieldname, fieldvalue, value)
        recnum = self.opencontext.StoreRecord(recordcopy)
        recordcopy.Clear()
        instance.key.load(recnum)

        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.delete_instance() method.
        instance.srkey = repr(recnum)

        if len(pcb):
            for indexname in sri:
                if indexname in pcb:
                    pcb[indexname](instance, sri[indexname])

    def foundset_all_records(self, fieldname):
        """Return APIFoundset containing all records on DPT file."""
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self.dpt_field_names[fieldname],
                    self._dbe.FD_ALLRECS,
                    self._dbe.APIFieldValue(""),
                )
            ),
        )

    def foundset_field_equals_value(self, fieldname, value):
        """Return APIFoundset with records where fieldname contains value."""
        if isinstance(value, self._dbe.APIFieldValue):
            return _DPTFoundSet(
                self.opencontext,
                self.opencontext.FindRecords(
                    self._dbe.APIFindSpecification(
                        self.dpt_field_names[fieldname], self._dbe.FD_EQ, value
                    )
                ),
            )
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self.dpt_field_names[fieldname],
                    self._dbe.FD_EQ,
                    self._dbe.APIFieldValue(value),
                )
            ),
        )

    def foundset_record_number(self, recnum):
        """Return APIFoundset containing record whose record number is recnum."""
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(self._dbe.FD_SINGLEREC, recnum)
            ),
        )

    def foundset_records_before_record_number(self, recnum):
        """Return APIFoundset containing records before recnum in file."""
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(self._dbe.FD_NOT_POINT, recnum)
            ),
        )

    def foundset_records_not_before_record_number(self, recnum):
        """Return APIFoundset containing records at and after recnum in file."""
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(self._dbe.FD_POINT, recnum)
            ),
        )

    def foundset_recordset_before_record_number(self, recnum, recordset):
        """Return APIFoundset containing records before recnum in recordset."""
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(self._dbe.FD_NOT_POINT, recnum),
                recordset,
            ),
        )


class Cursor(cursor.Cursor):
    """Define bsddb3 style cursor methods on a DPT file.

    Primary and secondary database, and others, should be read as the Berkeley
    DB usage.  This class emulates interaction with a Berkeley DB database via
    the Python bsddb3 module.

    APIRecordSetCursor is used to emulate Berkeley DB primary database access.

    APIDirectValueCursor is used to emulate Berkeley DB secondary database
    access, with the help of sn APIRecordSetCursor created for each key of the
    secondary database as required.

    The _CursorDPT class handles the details.

    """

    def __init__(self, dptdb, fieldname=None, keyrange=None, recordset=None):
        """Create an APIRecordSetCursor or an APIDirectValueCursor.

        An APIRecordSetCursor is created if fieldname is an unordered field.
        An APIDirectValueCursor is created if fieldname is an ordered field.
        keyrange is ignored at present
        recordset is a found set or list used as a starting point instead of
        the default all records on file.

        """
        super().__init__(dptdb)

        # Delay cursor creation till first use so next() and prev() can default
        # to first() and last() if cursor not initialized.
        # The c++ code supporting dptdb.dptapi OpenCursor() calls assumes the
        # new cursor should be positioned at the first record.
        # self._cursor == False means not yet created.
        # self._cursor == None means closed and not usable.
        self._fieldname = fieldname
        self._keyrange = keyrange
        self.recordset = recordset
        self._cursor = False

    def close(self):
        """Close the cursors implementing ordered access to records."""
        # Allow for False meaning not yet used.
        if self._cursor is False:
            self._cursor = None
            self.set_partial_key(None)
        elif self._cursor is not None:
            super().close()
        self._fieldname = None
        self._keyrange = None
        self.recordset = None

    # Allow for False meaning not yet used.
    def _create_cursor(self):
        """Create cursor if not yet created."""
        if self._cursor is False:
            self._cursor = _CursorDPT(
                self._dbset.opencontext,
                self._fieldname,
                self._dbset.dpt_field_names.get(
                    self._fieldname, self._dbset.primary
                ),
                self._dbset.primary,
                keyrange=self._keyrange,
                recordset=self.recordset,
            )

    def count_records(self):
        """Return record count or None if cursor is not usable."""
        if self.get_partial() is False:
            return 0

        # Allow for False meaning not yet used.
        self._create_cursor()

        _cursor = self._cursor
        fieldname = _cursor.dptfieldname
        context = _cursor.dptdb
        if _cursor.nonorderedfield:
            foundset = _cursor.foundset_all_records()
            count = foundset.recordset.Count()
            # context.DestroyRecordSet(foundset)
        else:
            dvcursor = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname)
            )
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            if self.get_partial() is not None:
                dvcursor.SetRestriction_Pattern(
                    self.get_converted_partial_with_wildcard()
                )
            games = context.CreateRecordList()
            dvcursor.GotoFirst()
            while dvcursor.Accessible():
                foundset = _cursor.foundset_field_equals_value(
                    dvcursor.GetCurrentValue()
                )
                games.Place(foundset.recordset)
                # context.DestroyRecordSet(foundset)
                dvcursor.Advance(1)
            context.CloseDirectValueCursor(dvcursor)
            count = games.Count()
            context.DestroyRecordSet(games)
        return count

    def first(self):
        """Return first record taking partial key into account."""
        if self.get_partial() is False:
            return None

        # Allow for False meaning not yet used.
        self._create_cursor()

        if self.get_partial() is None:
            return self._get_record(self._cursor.first())
        return self.nearest(self.get_partial())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0

        # Allow for False meaning not yet used.
        self._create_cursor()

        _cursor = self._cursor
        fieldname = _cursor.dptfieldname
        context = _cursor.dptdb
        if _cursor.nonorderedfield:
            foundset = _cursor.foundset_records_before_record_number(record[0])
            count = foundset.recordset.Count()
            # context.DestroyRecordSet(foundset)
            return count
        index_key, record_number = record
        dvcursor = context.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(fieldname)
        )
        dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        if self.get_partial():
            dvcursor.SetRestriction_Pattern(
                self.get_converted_partial_with_wildcard()
            )
        games = context.CreateRecordList()
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            value = dvcursor.GetCurrentValue()
            if value.ExtractString() >= index_key:
                if value.ExtractString() == index_key:
                    foundset = _cursor.foundset_recordset_before_record_number(
                        record_number, foundset
                    )
                    games.Place(foundset.recordset)
                    # context.DestroyRecordSet(fs)
                # context.DestroyRecordSet(foundset)
                break
            foundset = _cursor.foundset_field_equals_value(value)
            games.Place(foundset.recordset)
            # context.DestroyRecordSet(foundset)
            dvcursor.Advance(1)
        context.CloseDirectValueCursor(dvcursor)
        count = games.Count()
        context.DestroyRecordSet(games)
        return count

    def get_record_at_position(self, position=None):
        """Return record for positionth record in file or None."""
        if position is None:
            return None

        # Allow for False meaning not yet used.
        self._create_cursor()

        backwardscan = bool(position < 0)
        _cursor = self._cursor
        fieldname = _cursor.dptfieldname
        context = _cursor.dptdb
        if self._cursor.nonorderedfield:
            # it is simpler, and just as efficient, to do forward scans always
            foundset = _cursor.foundset_all_records()
            recordcount = foundset.recordset.Count()
            if backwardscan:
                position = recordcount + position
            rscursor = foundset.recordset.OpenCursor()
            if position > recordcount:
                if backwardscan:
                    rscursor.GotoFirst()
                else:
                    rscursor.GotoLast()
                if not rscursor.Accessible():
                    foundset.recordset.CloseCursor(rscursor)
                    # context.DestroyRecordSet(foundset)
                    return None
                current = rscursor.AccessCurrentRecordForRead()
                record = (
                    current.RecNum(),
                    _cursor.join_primary_field_occs(current),
                )
                foundset.recordset.CloseCursor(rscursor)
                # context.DestroyRecordSet(foundset)
                return record
            rscursor.GotoLast()
            if not rscursor.Accessible():
                foundset.recordset.CloseCursor(rscursor)
                # context.DestroyRecordSet(foundset)
                return None
            highrecnum = rscursor.LastAdvancedRecNum()
            foundset.recordset.CloseCursor(rscursor)
            # context.DestroyRecordSet(foundset)
            foundset = _cursor.foundset_records_before_record_number(position)
            recordcount = foundset.recordset.Count()
            if recordcount > position:
                rscursor = foundset.recordset.OpenCursor()
                rscursor.GotoLast()
                if not rscursor.Accessible():
                    foundset.recordset.CloseCursor(rscursor)
                    # context.DestroyRecordSet(foundset)
                    return None
                current = rscursor.AccessCurrentRecordForRead()
                record = (
                    current.RecNum(),
                    _cursor.join_primary_field_occs(current),
                )
                foundset.recordset.CloseCursor(rscursor)
                # context.DestroyRecordSet(foundset)
                return record
            # context.DestroyRecordSet(foundset)
            foundset = _cursor.foundset_records_not_before_record_number(
                position
            )
            rscursor = foundset.recordset.OpenCursor()
            rscursor.GotoFirst()
            while recordcount < position:
                if not rscursor.Accessible():
                    foundset.recordset.CloseCursor(rscursor)
                    # context.DestroyRecordSet(foundset)
                    return None
                rscursor.Advance(1)
                recordcount += 1
            current = rscursor.AccessCurrentRecordForRead()
            record = (
                current.RecNum(),
                _cursor.join_primary_field_occs(current),
            )
            foundset.recordset.CloseCursor(rscursor)
            # context.DestroyRecordSet(foundset)
            return record
        # it is more efficient to scan from the nearest edge of the file
        dvcursor = context.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(fieldname)
        )
        if backwardscan:
            dvcursor.SetDirection(dptapi.CURSOR_DESCENDING)
            position = -1 - position
        else:
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        if self.get_partial():
            dvcursor.SetRestriction_Pattern(
                self.get_converted_partial_with_wildcard()
            )
        count = 0
        record = None
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            value = dvcursor.GetCurrentValue()
            foundset = _cursor.foundset_field_equals_value(value)
            recordcount = foundset.recordset.Count()
            count += recordcount
            if count > position:
                rscursor = foundset.recordset.OpenCursor()
                rscursor.GotoFirst()
                if not rscursor.Accessible():
                    foundset.recordset.CloseCursor(rscursor)
                    # context.DestroyRecordSet(foundset)
                    record = None
                    break
                rscursor.Advance(position - count + recordcount)
                if not rscursor.Accessible():
                    foundset.recordset.CloseCursor(rscursor)
                    # context.DestroyRecordSet(foundset)
                    record = None
                    break
                record = (
                    value.ExtractString(),
                    rscursor.AccessCurrentRecordForRead().RecNum(),
                )
                foundset.recordset.CloseCursor(rscursor)
                # context.DestroyRecordSet(foundset)
                break
            # context.DestroyRecordSet(foundset)
            dvcursor.Advance(1)
        context.CloseDirectValueCursor(dvcursor)
        return record

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is False:
            return None

        # Allow for False meaning not yet used.
        self._create_cursor()

        if self.get_partial() is None:
            return self._get_record(self._cursor.last())
        chars = list(self.get_partial())
        while True:
            try:
                chars[-1] = chr(ord(chars[-1]) + 1)
            except ValueError:
                chars.pop()
                if not chars:
                    return self._get_record(self._cursor.last())
                continue
            self._cursor.dptdb.fieldvalue.Assign("".join(chars))
            self._cursor.dvcursor.SetOptions(dptapi.CURSOR_POSFAIL_NEXT)
            self._cursor.dvcursor.SetPosition(self._cursor.dptdb.fieldvalue)
            self._cursor.dvcursor.SetOptions(dptapi.CURSOR_DEFOPTS)
            if self._cursor.dvcursor.Accessible():
                return self.prev()
            return self._get_record(self._cursor.last())

    def set_partial_key(self, partial):
        """Set partial key to constrain range of key values returned."""
        self._partial = partial

    def _get_record(self, record):
        # Return record matching key or partial key or None if no match.
        if self.get_partial() is False:
            return None
        if self.get_partial() is not None:
            try:
                key, value = record
                if not key.startswith(self.get_converted_partial()):
                    return None
            except:
                return None
        return record

    def nearest(self, key):
        """Return nearest record taking partial key into account."""
        # Allow for False meaning not yet used.
        self._create_cursor()

        return self._get_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account."""
        # Allow for False meaning not yet used.
        if self._cursor is False:
            return self.first()

        return self._get_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account."""
        # Allow for False meaning not yet used.
        if self._cursor is False:
            return self.last()

        return self._get_record(self._cursor.prev())

    def refresh_recordset(self, instance=None):
        """Refresh records for datagrid access after database update.

        Ignore instance because DPT always rebuilds the entire record set.

        It is possible to distinguish between Lists, which could be modified
        here, and Record Sets which are immutable and must be discarded and
        recalculated.

        """
        # Allow for False meaning not yet used.
        self._create_cursor()

        if self._cursor:
            self._cursor.refresh_recordset_keep_position()

    def setat(self, record):
        """Position cursor at record. Then return current record (or None).

        Words used in bsddb3 (Python) to describe set and set_both say
        (key,value) is returned while Berkeley DB description seems to
        say that value is returned by the corresponding C functions.
        Do not know if there is a difference to go with the words but
        bsddb3 works as specified.

        """
        if self.get_partial() is False:
            return None
        key, value = record
        if self.get_partial() is not None:
            if not key.startswith(self.get_converted_partial()):
                return None

        # Allow for False meaning not yet used.
        self._create_cursor()

        if self._cursor.nonorderedfield:
            return self._get_record(self._cursor.set(key))
        return self._get_record(self._cursor.set_both(key, value))

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError("get_partial_with_wildcard not implemented")

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return "".join(
            (
                "".join([DPT_PATTERN_CHARS.get(c, c) for c in self._partial]),
                "*",
            )
        )

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        self._create_cursor()
        foundset = self._cursor.foundset_field_equals_value(key)
        rscursor = foundset.recordset.OpenCursor()
        try:
            if rscursor.Accessible():
                recno = rscursor.LastAdvancedRecNum()
            else:
                recno = None
        finally:
            foundset.recordset.CloseCursor(rscursor)
            # self._cursor.dptdb.DestroyRecordSet(foundset)
        return recno


# This is the DPT version of the cursor used in other database interfaces
# when emulating the recordset idea, not a cursor returned by the OpenCursor()
# method of a DPT recordset.
# Only reference in appsuites is in create_recordset_cursor() in dptbase, the
# source of this module.
class RecordsetCursorDPT(Cursor):
    """Provide a bsddb3 style cursor for a recordset of arbitrary records.

    The cursor does not support partial keys because the records in the
    recordset do not have an implied order (apart from the accidential order
    of existence on the database).

    """

    def set_partial_key(self, partial):
        """Set partial key to None.  Always.

        Always set to None because the record set or list should be trimmed
        to the required records before passing to the cursor.

        """
        # See comments in _CursorDPT class definition for reasons why _partial
        # is now constrained to be None always. Originally a design choice.
        super().set_partial_key(None)


# Only reference in appsuites is in create_recordsetlist_cursor() in dptbase,
# the source of this module.
class RecordsetListCursorDPT(Cursor):
    """A Cursor cursor with partial keys disabled.

    If a subset of the records on self.recordset is needed do more Finds
    to get the subset and pass this to the cursor.

    Likely to become an independent cursor since the direct value set
    option of Cursor is irrelevant.

    """

    def __init__(self, dptdb, fieldname, keyrange=None, recordset=None):
        """Create a Cursor cursor with partial keys disabled.

        Detail of managing cursors on all the record sets in recordset are
        to be determiined.

        """
        super().__init__(
            dptdb, fieldname, keyrange=keyrange, recordset=recordset[None]
        )

    def set_partial_key(self, partial):
        """Set partial key to None.  Always.

        Always set to None because the record set or list should be trimmed
        to the required records before passing to the cursor.

        """
        super().set_partial_key(None)


# Attempt to cope with dptdb being an APIRecordSet rather than a DPTRecord.
# DPT field names are not readily available from APIRecordSet, primary name in
# particular.
# Replacing _CursorDPT by _CursorRS and _CursorDV introduces problems scrolling
# at edge of record set and positing slider id scrollbars for sorted lists.
# Add filename to arguments passed to _CursorDPT
class _CursorDPT:
    """An APIRecordSetCursor or APIDirectValueCursor on a record set.

    A cursor implemented using either a DPT record set cursor for access in
    record number order or one of these managed by a DPT direct value cursor
    for access on an ordered index field.

    This class and its methods support the Cursor class in this module and may
    not be appropriate in other contexts.

    """

    def __init__(
        self,
        dptdb,
        fieldname,
        dptfieldname,
        dptprimaryfieldname,
        keyrange=None,
        recordset=None,
    ):

        # Introduction of DataClient.refresh_cursor method in solentware_grid
        # package may force _foundset to be implementaed as a list to avoid
        # time problems positioning cursor somewhere in a large foundset.
        self.dvcursor = None
        self._rscursor = None
        self._foundset = None
        self._delete_foundset_on_close_cursor = True
        # self.dptdb = None
        # self._fieldname = None
        # self.nonorderedfield = None

        if not isinstance(dptdb, dptapi.APIDatabaseFileContext):
            msg = " ".join(
                [
                    "The dptdb argument must be a",
                    "".join([dptapi.APIDatabaseFileContext.__name__, ","]),
                    "or a subclass, instance.",
                ]
            )
            raise DatabaseError(msg)

        self.dptdb = dptdb
        self._fieldname = fieldname
        self.dptfieldname = dptfieldname

        # Assume only visible field contains the stored Python object.
        # Move this to validation on opening database?
        # nonorderedfield = None
        # fac = dptdb.OpenFieldAttCursor()
        # name = dptapi.StdStringPtr()
        # while fac.Accessible():
        #    name.Assign(fac.Name())
        #    fn = name.value()
        #    atts = fac.Atts()
        #    if atts.IsVisible():
        #        if self.nonorderedfield:
        #            msg = 'More than one visible field defined on file'
        #            raise DatabaseError(msg)
        #        nonorderedfield = fn
        #    fac.Advance(1)
        # dptdb.CloseFieldAttCursor(fac)

        self.fieldvalue = dptapi.APIFieldValue()
        self.nonorderedfield = dptprimaryfieldname == dptfieldname

        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant. Safe because
        # self._partial in RecordsetCursorDPT instances is None always.
        # self._foundset must be this instance's scratch set and a separate
        # permanent reference for recordset, if not None, kept for use by
        # foundset_all_records and similar methods.
        if self.nonorderedfield:

            # A record set cursor.
            if recordset:
                self._foundset = recordset
                self._delete_foundset_on_close_cursor = False
            else:
                self._foundset = self.foundset_all_records()
            self._rscursor = self._foundset.recordset.OpenCursor()
            return

        # A record set cursor managed by a direct value cursor.
        self.dvcursor = self.dptdb.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(dptfieldname)
        )
        self.dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        self._first_by_value()

    def __del__(self):
        self.close()

    def close(self):
        """Close the cursor."""
        if self.dvcursor:
            self.dptdb.CloseDirectValueCursor(self.dvcursor)
        if self._foundset:
            if self._rscursor:
                self._foundset.recordset.CloseCursor(self._rscursor)
            # if self._delete_foundset_on_close_cursor:
            #    self.dptdb.DestroyRecordSet(self._foundset)
        self.dvcursor = None
        self._rscursor = None
        self._foundset = None
        self.dptdb = None

    def first(self):
        """Position cursor at first record and return it."""
        if self.dvcursor is not None:
            self._new_value_context()
            self._first_by_value()
            record = self._rscursor.AccessCurrentRecordForRead()
            return (
                self.dvcursor.GetCurrentValue().ExtractString(),
                record.RecNum(),
            )
        try:
            self._rscursor.GotoFirst()
            if not self._rscursor.Accessible():
                return None
            record = self._rscursor.AccessCurrentRecordForRead()
            return (record.RecNum(), self.join_primary_field_occs(record))
        except AttributeError:
            if self._rscursor is None:
                return None
            raise

    def last(self):
        """Position cursor at last record and return it."""
        if self.dvcursor is not None:
            self._new_value_context()
            self._last_by_value()
            record = self._rscursor.AccessCurrentRecordForRead()
            return (
                self.dvcursor.GetCurrentValue().ExtractString(),
                record.RecNum(),
            )
        try:
            self._rscursor.GotoLast()
            if not self._rscursor.Accessible():
                return None
            record = self._rscursor.AccessCurrentRecordForRead()
            return (record.RecNum(), self.join_primary_field_occs(record))
        except AttributeError:
            if self._rscursor is None:
                return None
            raise

    def next(self):
        """Position cursor at next record and return it."""
        rscursor = self._rscursor
        try:
            rscursor.Advance(1)
            if rscursor.Accessible():
                record = self._rscursor.AccessCurrentRecordForRead()
                if self.dvcursor is None:
                    return (
                        record.RecNum(),
                        self.join_primary_field_occs(record),
                    )
                return (
                    self.dvcursor.GetCurrentValue().ExtractString(),
                    record.RecNum(),
                )
        except AttributeError:
            if rscursor is None:
                return None
            raise

        if self.dvcursor is not None:
            # context = self.dptdb
            while not self._rscursor.Accessible():
                self.dvcursor.Advance(1)
                if self.dvcursor.Accessible():
                    self._foundset.recordset.CloseCursor(self._rscursor)
                    # context.DestroyRecordSet(self._foundset)
                    self._foundset = self.foundset_field_equals_value(
                        self.dvcursor.GetCurrentValue()
                    )
                    self._rscursor = self._foundset.recordset.OpenCursor()
                    if self._rscursor.Accessible():
                        record = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self.dvcursor.GetCurrentValue().ExtractString(),
                            record.RecNum(),
                        )
                else:
                    break

            # No more records for current position of direct value cursor
            self._new_value_context()
            self._last_by_value()
        else:

            # No more records on record set cursor.
            self._last()

        return None

    def prev(self):
        """Position cursor at previous record and return it."""
        rscursor = self._rscursor
        try:
            rscursor.Advance(-1)
            if rscursor.Accessible():
                record = self._rscursor.AccessCurrentRecordForRead()
                if self.dvcursor is None:
                    return (
                        record.RecNum(),
                        self.join_primary_field_occs(record),
                    )
                return (
                    self.dvcursor.GetCurrentValue().ExtractString(),
                    record.RecNum(),
                )
        except AttributeError:
            if rscursor is None:
                return None
            raise

        if self.dvcursor is not None:
            # context = self.dptdb
            while not self._rscursor.Accessible():
                self.dvcursor.Advance(-1)
                if self.dvcursor.Accessible():
                    self._foundset.recordset.CloseCursor(self._rscursor)
                    # context.DestroyRecordSet(self._foundset)
                    self._foundset = self.foundset_field_equals_value(
                        self.dvcursor.GetCurrentValue()
                    )
                    self._rscursor = self._foundset.recordset.OpenCursor()
                    self._rscursor.GotoLast()
                    if self._rscursor.Accessible():
                        record = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self.dvcursor.GetCurrentValue().ExtractString(),
                            record.RecNum(),
                        )
                else:
                    break

            # No more records for current position of direct value cursor
            self._new_value_context()
            self._first_by_value()
        else:

            # No more records on record set cursor.
            self._first()

        return None

    def refresh_recordset_keep_position(self):
        """Rebuild recordset and retain current cursor position.

        Record insertion, deletion, or amendment, may have changed the
        set of records which should be on the foundset.  Reset the
        cursor as close as possible to it's current position in the
        new record population.
        """
        if self._foundset:
            key = self._rscursor.LastAdvancedRecNum()
            self._foundset.recordset.CloseCursor(self._rscursor)
            # self.dptdb.DestroyRecordSet(self._foundset)
        else:
            key = -1  # (first + last) < key * 2
        if self.nonorderedfield:
            self._foundset = self.dptdb.foundset_all_records(self._fieldname)
        elif self.dvcursor is not None:
            self._foundset = self.foundset_field_equals_value(
                self.dvcursor.GetCurrentValue()
            )
        else:
            self.dvcursor = self.dptdb.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(self.dptfieldname)
            )
            self.dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            self._first_by_value()
            if self._foundset is None:
                return
        self._rscursor = self._foundset.recordset.OpenCursor()
        rscursor = self._rscursor
        rscursor.GotoLast()
        last = rscursor.LastAdvancedRecNum()
        rscursor.GotoFirst()
        first = rscursor.LastAdvancedRecNum()
        if (first + last) < key * 2:
            rscursor.GotoLast()
            adv = -1
            while rscursor.Accessible():
                if key <= rscursor.LastAdvancedRecNum():
                    return
                rscursor.Advance(adv)
            self._foundset.recordset.CloseCursor(rscursor)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoFirst()
        else:
            adv = 1
            while rscursor.Accessible():
                if key >= rscursor.LastAdvancedRecNum():
                    return
                rscursor.Advance(adv)
            self._foundset.recordset.CloseCursor(rscursor)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoLast()

    def set(self, key):
        """Set cursor position at key and return record.

        key is a record number.
        """
        rscursor = self._rscursor
        try:
            pos = rscursor.LastAdvancedRecNum()
            if pos > key:
                adv = -1
            elif pos < key:
                adv = 1
            while rscursor.Accessible():
                if key == rscursor.LastAdvancedRecNum():
                    record = self._rscursor.AccessCurrentRecordForRead()
                    return (
                        record.RecNum(),
                        self.join_primary_field_occs(record),
                    )
                rscursor.Advance(adv)
        except AttributeError:
            if rscursor is None:
                return None
            raise

        return None

    def set_range(self, key):
        """Restrict cursor to key range and return first matching record.

        Subsequent cursor operations are limited to field occurrences
        whose value starts with key.
        """
        if self.dvcursor is None:
            return self.set(key)

        dvcursor = self.dvcursor
        try:
            self.fieldvalue.Assign(key)
            dvcursor.SetRestriction_LoLimit(self.fieldvalue, True)
            dvcursor.GotoFirst()
        except AttributeError:
            if dvcursor is None:
                return None
            raise

        # context = self.dptdb
        while dvcursor.Accessible():
            self._foundset.recordset.CloseCursor(self._rscursor)
            # context.DestroyRecordSet(self._foundset)
            self._foundset = self.foundset_field_equals_value(
                dvcursor.GetCurrentValue()
            )
            self._rscursor = self._foundset.recordset.OpenCursor()
            if self._rscursor.Accessible():
                record = self._rscursor.AccessCurrentRecordForRead()
                return (
                    self.dvcursor.GetCurrentValue().ExtractString(),
                    record.RecNum(),
                )
            dvcursor.Advance(1)

        # Run off end available records.
        self._new_value_context()
        self._last_by_value()

        return None

    def set_both(self, key, value):
        """Position cursor at key and value and return record.

        key is the value of the field which fits the search.
        value is the record number of the record which fits the search.
        """
        # Need to take account of the direction cursor moves to get from
        # current position to (key, value).  dvcursor component is fine but
        # always stepping forward through rscursor component is wrong.
        # set does it right.
        dvcursor = self.dvcursor
        try:
            cpos = dvcursor.GetCurrentValue().ExtractString()
            if cpos == key:
                if self._rscursor.LastAdvancedRecNum() <= value:
                    advance = 1
                else:
                    advance = -1
                npos = cpos
            else:
                if cpos <= key:
                    advance = 1
                else:
                    advance = -1
                self.fieldvalue.Assign(key)
                dvcursor.SetPosition(self.fieldvalue)
                pos = dvcursor.GetCurrentValue().ExtractString()
                if pos == key:
                    npos = pos
                else:
                    npos = None
        except AttributeError:
            if dvcursor is None:
                return None
            raise

        if dvcursor.Accessible():
            if key != npos:
                return None
            if key != cpos:
                # context = self.dptdb
                self._foundset.recordset.CloseCursor(self._rscursor)
                # context.DestroyRecordSet(self._foundset)
                self._foundset = self.foundset_field_equals_value(
                    dvcursor.GetCurrentValue()
                )
                self._rscursor = self._foundset.recordset.OpenCursor()
                if advance > 0:
                    self._rscursor.GotoFirst()
                else:
                    self._rscursor.GotoLast()
            rscursor = self._rscursor
            while rscursor.Accessible():
                if value == rscursor.LastAdvancedRecNum():
                    record = self._rscursor.AccessCurrentRecordForRead()
                    return (
                        self.dvcursor.GetCurrentValue().ExtractString(),
                        record.RecNum(),
                    )
                rscursor.Advance(advance)

        # Set by key and value failed.
        self._new_value_context()
        self._first_by_value()

        return None

    def _foundset_all_records(self):
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self.dptfieldname,
                    dptapi.FD_ALLRECS,
                    dptapi.APIFieldValue(""),
                )
            ),
        )

    def _foundset_field_equals_value(self, value):
        if isinstance(value, dptapi.APIFieldValue):
            return _DPTFoundSet(
                self.dptdb,
                self.dptdb.FindRecords(
                    dptapi.APIFindSpecification(
                        self.dptfieldname, dptapi.FD_EQ, value
                    )
                ),
            )
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self.dptfieldname,
                    dptapi.FD_EQ,
                    dptapi.APIFieldValue(value),
                )
            ),
        )

    def _foundset_record_number(self, recnum):
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_SINGLEREC, recnum)
            ),
        )

    def _foundset_records_before_record_number(self, recnum):
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_NOT_POINT, recnum)
            ),
        )

    def _foundset_records_not_before_record_number(self, recnum):
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_POINT, recnum)
            ),
        )

    def _foundset_recordset_before_record_number(self, recnum, recordset):
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_NOT_POINT, recnum),
                recordset.recordset,
            ),
        )

    def foundset_all_records(self):
        """Return _DPTFoundSet containg all records."""
        if self._delete_foundset_on_close_cursor:
            return self._foundset_all_records()
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self.dptfieldname,
                    dptapi.FD_ALLRECS,
                    dptapi.APIFieldValue(""),
                ),
                self._foundset.recordset,
            ),
        )

    def foundset_field_equals_value(self, value):
        """Return _DPTFoundSet of records with field occurrence value.

        The field name is in self.dptfieldname and the field should be
        an ordered invisible field.
        """
        if self._delete_foundset_on_close_cursor:
            return self._foundset_field_equals_value(value)
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self.dptfieldname,
                    dptapi.FD_EQ,
                    dptapi.APIFieldValue(value),
                ),
                self._foundset.recordset,
            ),
        )

    def foundset_record_number(self, recnum):
        """Return _DPTFoundSet of record whose record number is recnum."""
        if self._delete_foundset_on_close_cursor:
            return self._foundset_record_number(recnum)
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_SINGLEREC, recnum),
                self._foundset.recordset,
            ),
        )

    def foundset_records_before_record_number(self, recnum):
        """Return _DPTFoundSet of records before recnum."""
        if self._delete_foundset_on_close_cursor:
            return self._foundset_records_before_record_number(recnum)
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_NOT_POINT, recnum),
                self._foundset.recordset,
            ),
        )

    def foundset_records_not_before_record_number(self, recnum):
        """Return _DPTFoundSet of records not before recnum."""
        if self._delete_foundset_on_close_cursor:
            return self._foundset_records_not_before_record_number(recnum)
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_POINT, recnum),
                self._foundset.recordset,
            ),
        )

    def foundset_recordset_before_record_number(self, recnum, recordset):
        """Return _DPTFoundSet of records in recordset before recnum."""
        if self._delete_foundset_on_close_cursor:
            return self._foundset_recordset_before_record_number(
                recnum, recordset
            )
        return _DPTFoundSet(
            self.dptdb,
            self.dptdb.FindRecords(
                dptapi.APIFindSpecification(dptapi.FD_NOT_POINT, recnum),
                recordset.recordset,
            ),
        )

    def _first(self):
        self._foundset.recordset.CloseCursor(self._rscursor)
        rscursor = self._foundset.recordset.OpenCursor()
        if rscursor.Accessible():
            self._rscursor = rscursor
            return
        self._foundset.recordset.CloseCursor(rscursor)
        self._rscursor = None

    def _first_by_value(self):
        context = self.dptdb
        dvcursor = self.dvcursor
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            foundset = self._foundset_field_equals_value(
                dvcursor.GetCurrentValue()
            )
            rscursor = foundset.recordset.OpenCursor()
            if rscursor.Accessible():
                self._rscursor = rscursor
                self._foundset = foundset
                return
            foundset.recordset.CloseCursor(rscursor)
            # context.DestroyRecordSet(foundset)
            dvcursor.Advance(1)
        context.CloseDirectValueCursor(dvcursor)
        self.dvcursor = None
        self._rscursor = None
        self._foundset = None

    def join_primary_field_occs(self, record):
        """Return concatenated occurrences of primary field.

        It is assumed the primary field is the only visible field, so
        there is no need to check the field name before including the
        value in the concatenation.
        """
        advance = record.AdvanceToNextFVPair
        # fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        value = []
        while advance():
            value.append(valueocc().ExtractString())
        return "".join(value)

    def _last(self):
        self._foundset.recordset.CloseCursor(self._rscursor)
        rscursor = self._foundset.recordset.OpenCursor()
        if rscursor.Accessible():
            rscursor.GotoLast()
            self._rscursor = rscursor
            return
        self._foundset.recordset.CloseCursor(rscursor)
        self._rscursor = None

    def _last_by_value(self):
        context = self.dptdb
        dvcursor = self.dvcursor
        dvcursor.GotoLast()
        while dvcursor.Accessible():
            foundset = self.foundset_field_equals_value(
                dvcursor.GetCurrentValue()
            )
            rscursor = foundset.recordset.OpenCursor()
            if rscursor.Accessible():
                rscursor.GotoLast()
                self._rscursor = rscursor
                self._foundset = foundset
                return
            foundset.recordset.CloseCursor(rscursor)
            # context.DestroyRecordSet(foundset)
            dvcursor.Advance(-1)
        context.CloseDirectValueCursor(dvcursor)
        self.dvcursor = None
        self._rscursor = None
        self._foundset = None

    def _new_value_context(self):
        context = self.dptdb
        context.CloseDirectValueCursor(self.dvcursor)
        self._foundset.recordset.CloseCursor(self._rscursor)
        # context.DestroyRecordSet(self._foundset)
        self.dvcursor = context.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(self.dptfieldname)
        )
        self.dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)


# Attempt to cope with absence of & ^ | &= ^= |= operators in dptapi interface
# to DPT recordlist objects via APIRecordList class.
class _DPTRecordSet:
    """Methods common to _DPTRecordList and _DPTFoundSet."""

    def __del__(self):
        """Destroy APIRecordList instance if not done by explicit close()."""
        if self.recordset:
            self.close()

    def __or__(self, other):
        """Return _DPTRecordList of records in self or other."""
        recordlist = _DPTRecordList(self._context)
        recordlist.recordset.Place(self.recordset)
        recordlist.recordset.Place(other.recordset)
        return recordlist

    def __and__(self, other):
        """Return _DPTRecordList of records in self and other."""
        recordlist = _DPTRecordList(self._context)
        recordlist.recordset.Place(self.recordset)
        recordlist &= other
        return recordlist

    def __xor__(self, other):
        """Return _DPTRecordList of records in self or other, but not both."""
        recordlist = _DPTRecordList(self._context)
        recordlist.recordset.Place(self.recordset)
        recordlist ^= other
        return recordlist

    def close(self):
        """Destroy the APIRecordSet instance.

        If close() is called more than once for an instance an AttributeError
        will be raised.  This follows DPT where DestroyRecordSet() calls, after
        the first, for an APIRecordSet instance raise a RuntimeError.

        """
        self._context.DestroyRecordSet(self.recordset)

        # This should cause deletion of the APIRecordSet object, but may not
        # cause deletion of this _DPTRecordSet instance.
        self.recordset = None

        # This probably does not cause deletion of the APIDatabaseContext but
        # does mean subsequent close() calls will raise an AttributeError.
        self._context = None

    def count_records(self):
        """Return count of records in the record set."""
        return self.recordset.Count()


# Attempt to cope with absence of & ^ | &= ^= |= operators in dptapi interface
# to DPT recordlist objects via APIRecordList class.
class _DPTRecordList(_DPTRecordSet):
    """Wrapper for dptapi.APIRecordList to implement & ^ | &= ^= |= actions.

    The recordset.Recordset class implements the recordset idea in the _db and
    _sqlite interfaces to Berkeley DB and SQLite3.  Recordset provides the
    __and__, __xor__, __or__, __iand__, __ixor__, and  __ior__, methods to do
    the & ^ | &= ^= |= actions.  _DPTRecordList implements these methods with
    the Place and Remove methods exposed in APIRecordList.

    The context (DB instance in Berkeley DB or a table in SQLite3) owning a
    recordset.Recordset is held in the _database attribute of that class.
    Garbage collection is sufficient to destroy recordset.Recordset instances.

    dptapi.APIRecordList instances are created by a context's CreateRecordList
    method, and must be destroyed explicitly by the context's DestroyRecordSet
    method.  The context is a dptapi.APIDatabaseFileContext instance.

    _DPTRecordList is roughly equivalent to recordset.Recordset and
    dptapi.APIRecordList is roughly equivalent to recordset._Recordset.

    """

    def __init__(self, context):
        """Note context and create a _DPTRecordList in context.

        context: a dptapi.APIDatabaseFileContext

        _DPTRecordList wraps an empty dptapi.APIRecordList belonging
        to a dptapi.APIDatabaseFileContext.

        """
        self._context = context
        self.recordset = context.CreateRecordList()

    def __ior__(self, other):
        """Return self with records in self or other."""
        self.recordset.Place(other.recordset)
        return self

    def __iand__(self, other):
        """Return self with records in self and other."""
        recordlist = _DPTRecordList(self._context)
        recordlist.recordset.Place(self.recordset)
        recordlist.recordset.Remove(other.recordset)
        self.recordset.Remove(recordlist.recordset)
        return self

    def __ixor__(self, other):
        """Return self with records in self or other but not both."""
        recordlist = _DPTRecordList(self._context)
        recordlist.recordset.Place(other.recordset)
        recordlist.recordset.Remove(self.recordset)
        self.recordset.Remove(recordlist.recordset)
        return self

    def clear_recordset(self):
        """Remove all records from self.recordset."""
        self.recordset.Clear()

    def place_record_number(self, record_number):
        """Place record record_number on self, a _DPTRecordList."""
        self.recordset.Place(record_number)

    def remove_record_number(self, record_number):
        """Remove record record_number on self, a _DPTRecordList."""
        self.recordset.Remove(record_number)

    def remove_recordset(self, recordset):
        """Remove other's records from recordset using Remove method.

        Equivalent to '|=' and '^=' sequence in _database version of method.
        """
        self.recordset.Remove(recordset.recordset)

    def replace_records(self, newrecords):
        """Replace records in recordset with newrecords.

        This method exists for compatibility with DPT where simply binding an
        attribute to newrecords may not be correct.

        """
        self.recordset.Clear()
        self.recordset.Place(newrecords.recordset)


# Attempt to cope with absence of & ^ | &= ^= |= operators in dptapi interface
# to DPT recordlist objects via APIRecordList class.
class _DPTFoundSet(_DPTRecordSet):
    """Wrapper for dptapi.APIFoundSet to implement & ^ | &= ^= |= actions.

    The recordset.Recordset class implements the recordset idea in the _db and
    _sqlite interfaces to Berkeley DB and SQLite3.  Recordset provides the
    __and__, __xor__, __or__, __iand__, __ixor__, and  __ior__, methods to do
    the & ^ | &= ^= |= actions.  _DPTRecordList implements these methods with
    the Place and Remove methods exposed in APIRecordList.

    The context (DB instance in Berkeley DB or a table in SQLite3) owning a
    recordset.Recordset is held in the _database attribute of that class.
    Garbage collection is sufficient to destroy recordset.Recordset instances.

    _DPTFoundSet holds the foundset such that _DPTRecordList & ^ | &= ^= |=
    operators can accept a foundset as the other argument.

    """

    def __init__(self, context, foundset):
        """Note foundset and it's owning context.

        context: a dptapi.APIDatabaseFileContext
        foundset: a dptapi.APIFoundSet belonging to context

        """
        self._context = context
        self.recordset = foundset
