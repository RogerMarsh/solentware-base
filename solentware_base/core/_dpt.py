# _dpt.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a DPT database created from a FileSpec() definition with the dptdb
module.

"""
import os
from ast import literal_eval
import re

from dptdb import dptapi

from . import filespec
from . import cursor
from .constants import (
    PRIMARY,
    SECONDARY,
    SUBFILE_DELIMITER,
    CONTROL_FILE,
    DEFAULT_SEGMENT_SIZE_BYTES,
    SPECIFICATION_KEY,
    SEGMENT_SIZE_BYTES_KEY,
    SQLITE_VALUE_COLUMN,
    SQLITE_SEGMENT_COLUMN,
    SQLITE_COUNT_COLUMN,
    SQLITE_RECORDS_COLUMN,
    INDEXPREFIX,
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
#from .find_dpt import Find
#from .where_dpt import Where
from .find import Find
from .where import Where
from .findvalues import FindValues
from .wherevalues import WhereValues

FILE_PARAMETER_LIST = (
    'BHIGHPG', 'BSIZE', 'DPGSRES', 'DPGSUSED', 'DRESERVE', 'DSIZE', 'FIFLAGS')


class DatabaseError(Exception):
    pass


class Database:
    
    """Access a DPT database, by default with transactions enabled (normal
    mode).

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
        pass


    def __init__(self,
                 specification,
                 folder=None,
                 segment_size_bytes=None,
                 sysfolder=None,
                 sysprint=None,
                 parms=None,
                 msgctl=None,
                 audit=None,
                 username=None,
                 **soak):
        if folder is None:
            raise DatabaseError(
                'A directory must be given: DPT does not do memory databases')
        try:
            path = os.path.abspath(folder)
        except:
            msg = ' '.join(['Database folder name', str(folder),
                            'is not valid'])
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
            sysprint = os.path.join(sysfolder, 'sysprint.txt')
        if parms is None:
            parms = os.path.join(sysfolder, 'parms.ini')
        if msgctl is None:
            msgctl = os.path.join(sysfolder, 'msgctl.ini')
        if audit is None:
            audit = os.path.join(sysfolder, 'audit.txt')
        if username is None:
            username = 'dptapi'
        self.sysfolder = sysfolder
        self.sysprint = sysprint
        self.parms = parms
        self.msgctl = msgctl
        self.audit = audit
        self.username = username

    def __del__(self):
        # Close files and destroy dptapi.APIDatabaseServices object.

        if self.dbenv is None:
            return
        self.close_database()

    def _validate_segment_size_bytes(self, segment_size_bytes):
        if not isinstance(segment_size_bytes, int):
            raise DatabaseError('Database segment size must be an int')
        if not segment_size_bytes > 0:
            raise DatabaseError('Database segment size must be more than 0')

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
        for v in self.specification.values():
            filedesc = v[FILEDESC]
            if filedesc[BSIZE] is None:
                records = v[DEFAULT_RECORDS]
                bsize = int(round(records / filedesc[BRECPPG]))
                if bsize * filedesc[BRECPPG] < records:
                    bsize += 1
                filedesc[BSIZE] = bsize
            if filedesc[DSIZE] is None:
                dsize = int(round(filedesc[BSIZE] * v[BTOD_FACTOR]) +
                            v[BTOD_CONSTANT])
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
                self.audit)
            os.chdir(cwd)

        if files is None:
            files = self.specification.keys()
        for e, (file, specification) in enumerate(self.specification.items()):
            if file not in files:
                continue
            self.table[file] = self._dptfileclass()(
                dbset=file,
                default_dataset_folder=self.home_directory,
                sfi=e,
                **specification)
        for t in self.table.values():
            t.open_file(self.dbenv, dptapi)

    def open_database_contexts(self, files=None):
        """Open all files in normal mode.

        Intended use is to open files to examine file status, or perhaps the
        equivalent of DPT command VIEW TABLES, when the database is closed as
        far as the application subclass of dptbase.Database is concerned.

        The Database Services object, bound to self.dbenv, is assumed to exist.

        """
        if files is None:
            files = self.specification.keys()
        for e, (file, specification) in enumerate(self.specification.items()):
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
        for e, (file, specification) in enumerate(self.specification.items()):
            if file not in files or file not in self.table:
                continue
            self.table[file].close_file(self.dbenv)

    # Set default parameters for normal use.
    def create_default_parms(self):
        """Create default parms.ini file for normal mode.

        This means transactions are enabled and a large number of DPT buffers.

        """
        if not os.path.exists(self.parms):
            pf = open(self.parms, 'w')
            try:
                pf.write("MAXBUF=10000 " + os.linesep)
            finally:
                pf.close()

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
        """Increase file sizes if files nearly full

        files = {'name':(table_b_count, table_d_count), ...}.
        
        Method increase_file_size will treat the two numbers as record counts
        and increase Table B and Table D, if necessary, to hold these numbers
        of extra records using the sizing parameters in the FileSpec instance
        for the database.  The value None for a file, "{..., 'name':None, ...}"
        means apply the default increase from the file specification.

        """
        if files is None:
            files = dict()
        for k, v in self.table.items():
            if files and k not in files:
                continue
            v.increase_file_size(
                self.dbenv,
                sizing_record_counts=files.get(k))

    def initial_database_size(self):
        """Set initial file sizes as specified in file descriptions"""
        for v in self.table.values():
            v.initial_file_size()
        return True

    def get_database_parameters(self, files=None):
        """Return file parameters infomation for file names in files."""
        if files is None:
            files = ()
        sizes = {}
        for file_ in files:
            if file_ in self.table:
                sizes[file_] = self.table[file_].get_file_parameters(
                    self.dbenv)
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
                    self.dbenv,
                    sizing_record_counts=files[file_])
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

    #def find_values(self, valuespec, file):
    #    yield self.table[file].find_values(valuespec)

    # Until sure how to make definition above work.
    def find_values(self, valuespec, file):
        """Yield values in range defined in valuespec in index named file."""

        # DPT provides two ways of doing this.  The FindValues construct which
        # returns the selected values accessed by a Value Set Cursor, and the
        # direct b-tree cursor construct which walks the database b-tree.
        # This method uses the direct b-tree cursor approach.
        cursor = self.table[file].opencontext.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[valuespec.field]))
        try:
            cursor.SetOptions(dptapi.CURSOR_POSFAIL_NEXT)
            if valuespec.above_value and valuespec.below_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.above_value))
                if cursor.Accessible():
                    if cursor.GetCurrentValue().ExtractString(
                        ) == valuespec.above_value:
                        cursor.Advance()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.above_value and valuespec.to_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.above_value))
                if cursor.Accessible():
                    if cursor.GetCurrentValue().ExtractString(
                        ) == valuespec.above_value:
                        cursor.Advance()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.from_value and valuespec.to_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.from_value))
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.from_value and valuespec.below_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.from_value))
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.above_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.above_value))
                if cursor.Accessible():
                    if cursor.GetCurrentValue().ExtractString(
                        ) == valuespec.above_value:
                        cursor.Advance()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.from_value:
                cursor.SetPosition(dptapi.APIFieldValue(valuespec.from_value))
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.to_value:
                cursor.GotoFirst()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v > valuespec.to_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            elif valuespec.below_value:
                cursor.GotoFirst()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if v >= valuespec.below_value:
                        break
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
            else:
                cursor.GotoFirst()
                while cursor.Accessible():
                    v = cursor.GetCurrentValue().ExtractString()
                    if valuespec.apply_pattern_and_set_filters_to_value(v):
                        yield v
                    cursor.Advance()
        finally:
            self.table[file].opencontext.CloseDirectValueCursor(cursor)

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
        for c in files:
            self.table[c].open_existing_file(self.dbenv)

    def record_finder(self, dbset, recordclass=None):
        """Return an instance of solentware_base.core.find.Find class."""
        return Find(self, dbset, recordclass=recordclass)

    def record_selector(self, statement):
        """Return an instance of solentware_base.core.where.Where class."""
        return Where(statement)

    def values_finder(self, dbset):
        """Return an instance of solentware_base.core.findvalues.FindValues
        class."""
        return FindValues(self, dbset)

    def values_selector(self, statement):
        """Return an instance of solentware_base.core.wherevalues.WhereValues
        class."""
        return WhereValues(statement)

    # Cursor instance is created here because there are no other calls to that
    # method.
    def database_cursor(self, file, field, keyrange=None):
        """Create and return a cursor on APIOpenContext() for (file, field).

        keyrange is an addition in DPT. It may yet be removed.

        """
        c = Cursor(
            self.table[file],
            fieldname=field,
            keyrange=keyrange)
        return c

    def repair_cursor(self, cursor, file, field):
        """Return new cursor based on argument cursor with fresh recordset."""
        cursor.close()
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
            self.table[dbset],
            self.table[dbset].primary,
            recordset=recordset)

    # Only active references in appsuites are in dptdatasourceset module; the
    # version in api.database raises an exception if called.
    def create_recordsetlist_cursor(self, dbset, dbname, keyrange, recordset):
        """Create and return a cursor for this recordset."""
        return RecordsetListCursorDPT(
            self.table[dbset],
            self.table[dbset].primary,
            recordset=recordset)

    def do_database_task(
        self,
        taskmethod,
        logwidget=None,
        taskmethodargs={},
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
            sysprint='CONSOLE',
            use_specification_items=use_specification_items)
        if db.open_database() is not True:
            return
        try:
            taskmethod(db, logwidget, **taskmethodargs)
        finally:
            # close_database() invoked by __del__ for db
            #db.close_context()
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
        """Return True if database specification defines field as primary
        database (Berkeley DB terminology) in file."""
        assert file in self.specification
        if field == file:
            return True
        assert field in self.specification[file][SECONDARY]
        return False

    def is_recno(self, file, field):
        """Return True if database specification defines field in file as
        record number (Berkeley DB terminology)."""

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
                dptapi.APIFindSpecification(dptapi.FD_SINGLEREC, key)))
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_record_number_range(
        self, file, keystart=None, keyend=None, cache_size=1):
        """Return _DPTRecordList on file containing record numbers whose record
        exists in record number range.

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
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_ebm(self, file, cache_size=1):
        """Return _DPTRecordList on file containing record numbers whose record
        exists.

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
                    dptapi.APIFieldValue(''))))
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_like(self, file, field, keylike=None, cache_size=1):
        """Return _DPTRecordList on file containing database records for field
        with keys like key.

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
                dptapi.APIFieldValue('')))
        matcher = re.compile('.*?' + keylike, flags=re.IGNORECASE|re.DOTALL)
        dvcursor = dptfile.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[field]))
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            v = dvcursor.GetCurrentValue()
            if matcher.match(v.ExtractString()):
                vfs = _DPTFoundSet(
                    dptfile,
                    dptfile.FindRecords(
                        dptapi.APIFindSpecification(
                            self.table[file].secondary[field],
                            dptapi.FD_EQ,
                            dptapi.APIFieldValue(v)),
                        foundset))
                #recordlist.Place(vfs)
                recordlist |= vfs
                #dptfile.DestroyRecordSet(vfs)
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
                    dptapi.APIFieldValue(self.encode_record_selector(key))),
                dptapi.FD_LOCK_SHR))
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_startswith(
        self, file, field, keystart=None, cache_size=1):
        """Return _DPTRecordList on file containing records for field with
        keys starting key.

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
                dptapi.APIFieldValue('')))
        dvcursor = dptfile.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(
                self.table[file].secondary[field]))
        dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        dvcursor.SetRestriction_LoLimit(dptapi.APIFieldValue(keystart), True)
        dvcursor.GotoFirst()
        while dvcursor.Accessible():
            v = dvcursor.GetCurrentValue()
            if not v.ExtractString().startswith(keystart):
                break
            vfs = _DPTFoundSet(
                    dptfile,
                    dptfile.FindRecords(
                        dptapi.APIFindSpecification(
                            self.table[file].secondary[field],
                            dptapi.FD_EQ,
                            dptapi.APIFieldValue(v)),
                        foundset))
            #recordlist.Place(vfs)
            recordlist |= vfs
            #dptfile.DestroyRecordSet(vfs)
            dvcursor.Advance(1)
        dptfile.CloseDirectValueCursor(dvcursor)
        dptfile.DestroyRecordSet(foundset)
        return recordlist

    def recordlist_key_range(
        self, file, field, ge=None, gt=None, le=None, lt=None, cache_size=1):
        """Return _DPTRecordList on file containing records for field with
        keys in range set by combinations of ge, gt, le, and lt.

        cache_size is not relevant to DPT.
        """
        if ge and gt:
            raise DatabaseError("Both 'ge' and 'gt' given in key range")
        elif le and lt:
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
                        dptapi.APIFieldValue(ge or gt))))
        elif ge is None and gt is None:
            foundset = _DPTFoundSet(
                dptfile,
                dptfile.FindRecords(
                    dptapi.APIFindSpecification(
                        self.table[file].secondary[field],
                        dptapi.FD_LE if le else dptapi.FD_LT,
                        dptapi.APIFieldValue(le or lt))))
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
                        dptapi.APIFieldValue(le or lt))))
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
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
                    dptapi.APIFieldValue(self.encode_record_selector(''))),
                dptapi.FD_LOCK_SHR))
        #recordlist.Place(foundset)
        recordlist |= foundset
        #dptfile.DestroyRecordSet(foundset)
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
            dptapi.FD_LOCK_EXCL)
        rscursor = foundset.OpenCursor()
        rscursor.GotoFirst()
        while rscursor.Accessible():
            rscursor.AccessCurrentRecordForReadWrite().DeleteFieldByValue(
                sfield, value)
            rscursor.Advance(1)
        foundset.CloseCursor(rscursor)
        dptfile.DestroyRecordSet(foundset)
    
    def file_records_under(self, file, field, recordset, key):
        """Replace records for index field[key] with recordset records.

        recordset must be a _DPTRecordSet, or subclass, instance.

        """
        dptfile = self.get_table_connection(file)
        dptfile.FileRecordsUnder(
            recordset._recordset,
            self.table[file].secondary[field],
            dptapi.APIFieldValue(self.encode_record_selector(key)))


class _DPTFile:
    """This class is used to access files in a DPT database.

    Instances are created as necessary by a Database.open_database() call.

    There is too much 'per file' state to conveniently manage DPT files in the
    Database class.

    The sibling modules for Berkeley DB and SQLite3 do not have classes like
    _DPTFile.  (They used to have such but it seems simpler there without.)

    """

    def __init__(self,
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
                 sfi=None):
        self._dbe = None
        self._fieldvalue = None
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
        else:
            folder = folder
        self.file = os.path.join(folder, file)
        self.default_increase_factor = default_increase_factor
        self.dbset = dbset
        self.default_dataset_folder = default_dataset_folder
        self.sfi = sfi
        self.secondary = {}
        if secondary is not None:
            for k, v in secondary.items():
                self.secondary[k] = v if v is not None else k
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
            for k, v in secondary.items():
                if v is None:
                    self.dpt_field_names[k] = k[:1].upper() + k[1:]
                else:
                    self.dpt_field_names[k] = v
        if fields is not None:
            for fieldname in fields:
                if fieldname not in self.dpt_field_names:
                    self.dpt_field_names[fieldname] = fieldname

    def close_file(self, dbenv):
        if self.opencontext is None:
            return
        self.opencontext.DestroyAllRecordSets()
        dbenv.CloseContext(self.opencontext)
        self.opencontext = None
        dbenv.Free(self.ddname)

    def open_file(self, dbenv, dbe):

        # Create the file if it does not exist.
        foldername, filename = os.path.split(self.file)
        if os.path.exists(foldername):
            if not os.path.isdir(foldername):
                msg = ' '.join([foldername, 'exists but is not a folder'])
                raise DatabaseError(msg)
        else:
            os.makedirs(foldername)
        if not os.path.exists(self.file):
            dbenv.Allocate(
                self.ddname,
                self.file,
                dbe.FILEDISP_COND)
            dbenv.Create(
                self.ddname,
                self.filedesc[BSIZE],
                self.filedesc[BRECPPG],
                self.filedesc[BRESERVE],
                self.filedesc[BREUSE],
                self.filedesc[DSIZE],
                self.filedesc[DRESERVE],
                self.filedesc[DPGSRES],
                self.filedesc[FILEORG])
            cs = dbe.APIContextSpecification(self.ddname)
            oc = dbenv.OpenContext(cs)
            oc.Initialize()
            for field in self.fields:
                fa = dbe.APIFieldAttributes()
                fld = self.fields[field]
                if fld[FLT]: fa.SetFloatFlag()
                if fld[INV]: fa.SetInvisibleFlag()
                if fld[UAE]: fa.SetUpdateAtEndFlag()
                if fld[ORD]: fa.SetOrderedFlag()
                if fld[ONM]: fa.SetOrdNumFlag()
                fa.SetSplitPct(fld[SPT])
                oc.DefineField(self.dpt_field_names[field], fa)
            dbenv.CloseContext(oc)
            dbenv.Free(self.ddname)
        if not os.path.isfile(self.file):
            msg = ' '.join([self.file, 'exists but is not a file'])
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
        self._fieldvalue = dbe.APIFieldValue()
        self._putrecordcopy = dbe.APIStoreRecordTemplate()

    def open_existing_file(self, dbenv):
        dbenv.Allocate(
            self.ddname,
            self.file,
            self._dbe.FILEDISP_OLD)
        cs = self._dbe.APIContextSpecification(self.ddname)
        self.opencontext = self._open_context(dbenv, cs)
            
    def initial_file_size(self):
        if not os.path.exists(self.file):
            f = self.filedesc
            if f[BSIZE] is None:
                records = self.default_records
                bsize = int(round(records / f[BRECPPG]))
                if bsize * f[BRECPPG] < records:
                    bsize += 1
                dsize = int(round(bsize * self.btod_factor) +
                            self.btod_constant)
                f[BSIZE] = bsize
                f[DSIZE] = dsize
        return True

    def increase_file_size(self, dbserv, sizing_record_counts=None):
        if self.opencontext is not None:
            table_B_needed, table_D_needed = self.get_tables_increase(
                dbserv, sizing_record_counts=sizing_record_counts)
            if len(self.get_extents()) % 2:
                if table_B_needed:
                    self.opencontext.Increase(table_B_needed, False)
                if table_D_needed:
                    self.opencontext.Increase(table_D_needed, True)
            elif table_D_needed:
                self.opencontext.Increase(table_D_needed, True)
                if table_B_needed:
                    self.opencontext.Increase(table_B_needed, False)
            elif table_B_needed:
                self.opencontext.Increase(table_B_needed, False)

    def increase_size_of_full_file(self, dbserv, size_before, size_filled):
        """Increase file size taking file full into account.

        Intended for use when the required size to do a deferred update has
        been estimated and the update fills a file.  Make Table B and, or,
        Table D free space at least 20% bigger before trying again.

        It is the caller's responsibility to manage the backups needed, and
        the collection of 'view tables' information, to enable effective use
        of this method.

        """
        b_diff_imp = size_filled['BSIZE'] - size_before['BSIZE']
        d_diff_imp = size_filled['DSIZE'] - size_before['DSIZE']
        b_spare = size_before['BSIZE'] - max((0, size_before['BHIGHPG']))
        d_spare = size_before['DSIZE'] - size_before['DPGSUSED']
        b_filled = size_filled['FIFLAGS'] & dptapi.FIFLAGS_FULL_TABLEB
        d_filled = size_filled['FIFLAGS'] & dptapi.FIFLAGS_FULL_TABLED
        deferred = size_filled['FISTAT'][0] & dptapi.FISTAT_DEFERRED_UPDATES
        broken = size_filled['FISTAT'][0] & dptapi.FISTAT_PHYS_BROKEN
        if b_filled:
            b_increase = ((((b_diff_imp + b_spare) * 6) // 5))
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) // 5)),
                int(b_increase * self.btod_factor - d_spare + 1))
        elif d_filled:
            b_increase = b_diff_imp
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) // 5)),
                int(b_increase * self.btod_factor - d_spare + 1))
        elif deferred:
            if broken:
                b_increase = 0
                d_increase = max(
                    ((((d_diff_imp + d_spare) * 6) // 5)),
                    int(b_increase * self.btod_factor - d_spare + 1))
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
        return

    def calculate_table_b_increase(
        self,
        unused=None,
        increase=None,
        ):
        # Return the number of pages to add to DPT file data area.

        # unused - current spare pages in Table B or None
        # increase - number of extra records or None

        if unused is not None:
            unused = unused * self.filedesc[BRECPPG]
        if unused is None:
            if increase is not None:
                return increase
        elif increase is not None:
            if increase > unused:
                return increase
        increase =  int((1 + self.default_records) *
                        self.default_increase_factor)
        if unused is None:
            return increase
        elif increase > unused:
            return increase - unused
        return 0

    def calculate_table_d_increase(
        self,
        unused=None,
        increase=None,
        table_b_increase=None,
        ):
        # Return the number of pages to add to DPT file index area.

        # unused - current spare pages in Table D or None
        # increase - number of extra records or None
        # table_b_increase - increase index to match extra data pages if not
        #                    None.

        if unused is not None:
            unused = (unused * self.filedesc[BRECPPG]) // self.btod_factor
        if table_b_increase is None:
            if unused is None:
                if increase is not None:
                    return increase
            elif increase is not None:
                if increase > unused:
                    return increase
            increase =  int((1 + self.default_records) *
                            self.default_increase_factor)
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
        # Return tuple (Table B, Table D) increase needed or None.
        if self.opencontext is not None:
            fp = self.get_file_parameters(dbserv)
            b_size, b_used, d_size, d_used = (
                fp['BSIZE'],
                max(0, fp['BHIGHPG']),
                fp['DSIZE'],
                fp['DPGSUSED'])
            if sizing_record_counts is None:
                increase_record_counts = (
                    self.calculate_table_b_increase(unused=(b_size - b_used)),
                    self.calculate_table_d_increase(unused=(d_size - d_used)),
                    )
            else:
                increase_record_counts = (
                    self.calculate_table_b_increase(
                        unused=(b_size - b_used),
                        increase=sizing_record_counts[0]),
                    self.calculate_table_d_increase(
                        unused=(d_size - d_used),
                        increase=sizing_record_counts[1]),
                    )
            return (
                increase_record_counts[0] // self.filedesc[BRECPPG],
                int((increase_record_counts[1] * self.btod_factor)
                 // self.filedesc[BRECPPG]),
                )

    def get_file_parameters(self, dbserv):
        """Get current values of selected file parameters."""
        vr = dbserv.Core().GetViewerResetter()
        fp = dict()
        fp['FISTAT'] = (
            vr.ViewAsInt('FISTAT', self.opencontext),
            vr.View('FISTAT', self.opencontext),
            )
        for p in FILE_PARAMETER_LIST:
            fp[p] = vr.ViewAsInt(p, self.opencontext)
        for p in (dptapi.FIFLAGS_FULL_TABLEB, dptapi.FIFLAGS_FULL_TABLED):
            fp[p] = bool(fp['FIFLAGS'] & p)
        return fp
            
    def get_extents(self):
        """Get current extents for file."""
        extents = dptapi.IntVector()
        self.opencontext.ShowTableExtents(extents)
        return extents
    
    def _open_context(self, dbenv, cs):
        return dbenv.OpenContext(cs)
    
    def get_primary_record(self, key):
        # Return (key, value) or None given the record number in key.
        if key is None:
            return None
        fs = self.foundset_record_number(key)
        rsc = fs._recordset.OpenCursor()
        try:
            if rsc.Accessible():
                r = (
                    key,
                    self.join_primary_field_occurrences(
                        rsc.AccessCurrentRecordForRead()))
            else:
                r = None
        finally:
            fs._recordset.CloseCursor(rsc)
            #self.opencontext.DestroyRecordSet(fs)
        return r

    def join_primary_field_occurrences(self, record):
        # Return concatenated occurrences of field holding record value.
        advance = record.AdvanceToNextFVPair
        fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        primary = self.dpt_field_names[self.primary]
        v = []
        while advance():
            if fieldocc() == primary:
                v.append(valueocc().ExtractString())
        return ''.join(v)
        
    def foundset_record_number(self, recnum):
        # Return APIFoundset containing record whose record number is recnum.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self._dbe.FD_SINGLEREC,
                    recnum)))
        
    def delete_instance(self, instance):
        
        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.delete_instance() method.
        instance.srkey = repr(instance.key.pack())

        instance.set_packed_value_and_indexes()
        sri = instance.srindex
        sec = self.secondary
        dcb = instance._deletecallbacks
        fieldvalue = self._fieldvalue
        Assign = fieldvalue.Assign
        fd = self.foundset_record_number(instance.key.pack())
        rsc = fd._recordset.OpenCursor()
        while rsc.Accessible():
            r = rsc.AccessCurrentRecordForReadWrite()
            for s in sri:
                if s in dcb:
                    dcb[s](instance, sri[s])
                else:
                    f = self.dpt_field_names[sec[s]]
                    for v in sri[s]:
                        Assign(v)
                        r.DeleteFieldByValue(
                            f,
                            fieldvalue)
            r.Delete()
            rsc.Advance(1)
        fd._recordset.CloseCursor(rsc)
        #self.opencontext.DestroyRecordSet(fd)

    def edit_instance(self, instance):
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
        dcb = instance._deletecallbacks
        ndcb = instance.newrecord._deletecallbacks
        pcb = instance._putcallbacks
        npcb = instance.newrecord._putcallbacks
        ionly = []
        nionly = []
        iandni = []
        for f in sri:
            if f in nsri:
                iandni.append(f)
            else:
                ionly.append(f)
        for f in nsri:
            if f not in sri:
                nionly.append(f)
        sec = self.secondary
        fieldvalue = self._fieldvalue
        Assign = fieldvalue.Assign
        fd = self.foundset_record_number(instance.key.pack())
        rsc = fd._recordset.OpenCursor()
        safe_length = self.dpt_primary_field_length
        while rsc.Accessible():
            r = rsc.AccessCurrentRecordForReadWrite()
            f = self.dpt_field_names[self.primary]
            r.DeleteEachOccurrence(f)
            for i in range(0, len(nsrv), safe_length):
                Assign(nsrv[i:i+safe_length])
                r.AddField(
                    f,
                    fieldvalue)
            for s in ionly:
                if s in dcb:
                    dcb[s](instance, sri[s])
                else:
                    f = self.dpt_field_names[sec[s]]
                    for v in sri[s]:
                        Assign(v)
                        r.DeleteFieldByValue(
                            f,
                            fieldvalue)
            for s in nionly:
                if s in npcb:
                    npcb[s](instance, sri[s])
                else:
                    f = self.dpt_field_names[sec[s]]
                    for nv in nsri[s]:
                        Assign(nv)
                        r.AddField(
                            f,
                            fieldvalue)
            for s in iandni:
                if s in dcb:
                    dcb[s](instance, sri[s])
                    npcb[s](instance.newrecord, nsri[s])
                else:
                    f = self.dpt_field_names[sec[s]]
                    for v in sri[s]:
                        Assign(v)
                        r.DeleteFieldByValue(
                            f,
                            fieldvalue)
                    for nv in nsri[s]:
                        Assign(nv)
                        r.AddField(
                            f,
                            fieldvalue)
            rsc.Advance(1)
        fd._recordset.CloseCursor(rsc)
        #self.opencontext.DestroyRecordSet(fd)

    def put_instance(self, instance):
        instance.set_packed_value_and_indexes()
        recordcopy = self._putrecordcopy
        pyAppend = self._dbe.pyAppendStdString
        fieldvalue = self._fieldvalue
        srv = instance.srvalue
        f = self.dpt_field_names[self.primary]
        safe_length = self.dpt_primary_field_length
        for i in range(0, len(srv), safe_length):
            pyAppend(recordcopy, f, fieldvalue, srv[i:i+safe_length])
        sri = instance.srindex
        sec = self.secondary
        pcb = instance._putcallbacks
        for s in sri:
            if s not in pcb:
                f = self.dpt_field_names[sec[s]]
                pyAppend = self.pyappend[f]
                for v in sri[s]:
                    pyAppend(recordcopy, f, fieldvalue, v)
        recnum = self.opencontext.StoreRecord(recordcopy)
        recordcopy.Clear()
        instance.key.load(recnum)

        # Copy ._dpt.Database.encode_record_number() implementation to mimic
        # ._database.Database.delete_instance() method.
        instance.srkey = repr(recnum)

        if len(pcb):
            for s in sri:
                if s in pcb:
                    pcb[s](instance, sri[s])

    def foundset_all_records(self, fieldname):
        # Return APIFoundset containing all records on DPT file.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self.dpt_field_names[fieldname],
                    self._dbe.FD_ALLRECS,
                    self._dbe.APIFieldValue(''))))

    def foundset_field_equals_value(self, fieldname, value):
        # Return APIFoundset with records where fieldname contains value.
        if isinstance(value, self._dbe.APIFieldValue):
            return _DPTFoundSet(
                self.opencontext,
                self.opencontext.FindRecords(
                    self._dbe.APIFindSpecification(
                        self.dpt_field_names[fieldname],
                        self._dbe.FD_EQ, value)))
        else:
            return _DPTFoundSet(
                self.opencontext,
                self.opencontext.FindRecords(
                    self._dbe.APIFindSpecification(
                        self.dpt_field_names[fieldname],
                        self._dbe.FD_EQ,
                        self._dbe.APIFieldValue(value))))
        
    def foundset_record_number(self, recnum):
        # Return APIFoundset containing record whose record number is recnum.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self._dbe.FD_SINGLEREC,
                    recnum)))

    def foundset_records_before_record_number(self, recnum):
        # Return APIFoundset containing records before recnum in file.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self._dbe.FD_NOT_POINT,
                    recnum)))

    def foundset_records_not_before_record_number(self, recnum):
        # Return APIFoundset containing records at and after recnum in file.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self._dbe.FD_POINT,
                    recnum)))

    def foundset_recordset_before_record_number(self, recnum, recordset):
        # Return APIFoundset containing records before recnum in recordset.
        return _DPTFoundSet(
            self.opencontext,
            self.opencontext.FindRecords(
                self._dbe.APIFindSpecification(
                    self._dbe.FD_NOT_POINT,
                    recnum),
                recordset))


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
        self._recordset = recordset
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
        self._recordset = None

    # Allow for False meaning not yet used.
    def _create_cursor(self):
        """Create cursor if not yet created."""
        if self._cursor is False:
            self._cursor = _CursorDPT(
                self._dbset.opencontext,
                self._fieldname,
                self._dbset.dpt_field_names.get(self._fieldname,
                                                self._dbset.primary),
                self._dbset.primary,
                keyrange=self._keyrange,
                recordset=self._recordset)

    def count_records(self):
        """Return record count or None if cursor is not usable."""
        if self.get_partial() is False:
            return 0

        # Allow for False meaning not yet used.
        self._create_cursor()

        cursor = self._cursor
        fieldname = cursor._dptfieldname
        context = cursor._dptdb
        if cursor._nonorderedfield:
            foundset = cursor.foundset_all_records()
            count = foundset._recordset.Count()
            #context.DestroyRecordSet(foundset)
        else:
            dvcursor = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname))
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            if self.get_partial() is not None:
                dvcursor.SetRestriction_Pattern(
                    self.get_converted_partial_with_wildcard())
            games = context.CreateRecordList()
            dvcursor.GotoFirst()
            while dvcursor.Accessible():
                foundset = cursor.foundset_field_equals_value(
                    dvcursor.GetCurrentValue())
                games.Place(foundset._recordset)
                #context.DestroyRecordSet(foundset)
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
        else:
            return self.nearest(self.get_partial())

    def get_position_of_record(self, record=None):
        """Return position of record in file or 0 (zero)."""
        if record is None:
            return 0

        # Allow for False meaning not yet used.
        self._create_cursor()

        cursor = self._cursor
        fieldname = cursor._dptfieldname
        context = cursor._dptdb
        if cursor._nonorderedfield:
            foundset = cursor.foundset_records_before_record_number(record[0])
            count = foundset._recordset.Count()
            #context.DestroyRecordSet(foundset)
            return count
        else:
            sk, rn = record
            dvcursor = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname))
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            if self.get_partial():
                dvcursor.SetRestriction_Pattern(
                    self.get_converted_partial_with_wildcard())
            games = context.CreateRecordList()
            dvcursor.GotoFirst()
            while dvcursor.Accessible():
                cv = dvcursor.GetCurrentValue()
                foundset = cursor.foundset_field_equals_value(cv)
                if cv.ExtractString() >= sk:
                    if cv.ExtractString() == sk:
                        fs = cursor.foundset_recordset_before_record_number(
                            rn, foundset)
                        games.Place(fs._recordset)
                        #context.DestroyRecordSet(fs)
                    #context.DestroyRecordSet(foundset)
                    break
                games.Place(foundset._recordset)
                #context.DestroyRecordSet(foundset)
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
        cursor = self._cursor
        fieldname = cursor._dptfieldname
        context = cursor._dptdb
        if self._cursor._nonorderedfield:
            # it is simpler, and just as efficient, to do forward scans always
            fs = cursor.foundset_all_records()
            c = fs._recordset.Count()
            if backwardscan:
                position = c + position
            rsc = fs._recordset.OpenCursor()
            if position > c:
                if backwardscan:
                    rsc.GotoFirst()
                else:
                    rsc.GotoLast()
                if not rsc.Accessible():
                    fs._recordset.CloseCursor(rsc)
                    #context.DestroyRecordSet(fs)
                    return None
                r = rsc.AccessCurrentRecordForRead()
                record = (
                    r.RecNum(),
                    cursor._join_primary_field_occs(r))
                fs._recordset.CloseCursor(rsc)
                #context.DestroyRecordSet(fs)
                return record
            rsc.GotoLast()
            if not rsc.Accessible():
                fs._recordset.CloseCursor(rsc)
                #context.DestroyRecordSet(fs)
                return None
            highrecnum = rsc.LastAdvancedRecNum()
            fs._recordset.CloseCursor(rsc)
            #context.DestroyRecordSet(fs)
            fs = cursor.foundset_records_before_record_number(position)
            c = fs._recordset.Count()
            if c > position:
                rsc = fs._recordset.OpenCursor()
                rsc.GotoLast()
                if not rsc.Accessible():
                    fs._recordset.CloseCursor(rsc)
                    #context.DestroyRecordSet(fs)
                    return None
                r = rsc.AccessCurrentRecordForRead()
                record = (
                    r.RecNum(),
                    cursor._join_primary_field_occs(r))
                fs._recordset.CloseCursor(rsc)
                #context.DestroyRecordSet(fs)
                return record
            #context.DestroyRecordSet(fs)
            fs = cursor.foundset_records_not_before_record_number(position)
            rsc = fs._recordset.OpenCursor()
            rsc.GotoFirst()
            while c < position:
                if not rsc.Accessible():
                    fs._recordset.CloseCursor(rsc)
                    #context.DestroyRecordSet(fs)
                    return None
                rsc.Advance(1)
                c += 1
            r = rsc.AccessCurrentRecordForRead()
            record = (
                r.RecNum(),
                cursor._join_primary_field_occs(r))
            fs._recordset.CloseCursor(rsc)
            #context.DestroyRecordSet(fs)
            return record
        else:
            # it is more efficient to scan from the nearest edge of the file
            dvc = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname))
            if backwardscan:
                dvc.SetDirection(dptapi.CURSOR_DESCENDING)
                position = -1 - position
            else:
                dvc.SetDirection(dptapi.CURSOR_ASCENDING)
            if self.get_partial():
                dvc.SetRestriction_Pattern(
                    self.get_converted_partial_with_wildcard())
            count = 0
            record = None
            dvc.GotoFirst()
            while dvc.Accessible():
                cv = dvc.GetCurrentValue()
                fs = cursor.foundset_field_equals_value(cv)
                c = fs._recordset.Count()
                count += c
                if count > position:
                    rsc = fs._recordset.OpenCursor()
                    rsc.GotoFirst()
                    if not rsc.Accessible():
                        fs._recordset.CloseCursor(rsc)
                        #context.DestroyRecordSet(fs)
                        record = None
                        break
                    rsc.Advance(position - count + c)
                    if not rsc.Accessible():
                        fs._recordset.CloseCursor(rsc)
                        #context.DestroyRecordSet(fs)
                        record = None
                        break
                    r = rsc.AccessCurrentRecordForRead()
                    record = (cv.ExtractString(), r.RecNum())
                    fs._recordset.CloseCursor(rsc)
                    #context.DestroyRecordSet(fs)
                    break
                #context.DestroyRecordSet(fs)
                dvc.Advance(1)
            context.CloseDirectValueCursor(dvc)
            return record

    def last(self):
        """Return last record taking partial key into account."""
        if self.get_partial() is False:
            return None

        # Allow for False meaning not yet used.
        self._create_cursor()

        if self.get_partial() is None:
            return self._get_record(self._cursor.last())
        else:
            k = list(self.get_partial())
            while True:
                try:
                    k[-1] = chr(ord(k[-1]) + 1)
                except ValueError:
                    k.pop()
                    if not len(k):
                        return self._get_record(self._cursor.last())
                    continue
                self._cursor._dptdb._fieldvalue.Assign(''.join(k))
                self._cursor._dvcursor.SetOptions(dptapi.CURSOR_POSFAIL_NEXT)
                self._cursor._dvcursor.SetPosition(
                    self._cursor._dptdb._fieldvalue)
                self._cursor._dvcursor.SetOptions(dptapi.CURSOR_DEFOPTS)
                if self._cursor._dvcursor.Accessible():
                    return self.prev()
                else:
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

        if self._cursor._nonorderedfield:
            return self._get_record(self._cursor.set(key))
        else:
            return self._get_record(self._cursor.set_both(key, value))

    def get_converted_partial(self):
        """Return self._partial as it would be held on database."""
        return self._partial

    def get_partial_with_wildcard(self):
        """Return self._partial with wildcard suffix appended."""
        raise DatabaseError('get_partial_with_wildcard not implemented')

    def get_converted_partial_with_wildcard(self):
        """Return converted self._partial with wildcard suffix appended."""
        return ''.join(
            (''.join([DPT_PATTERN_CHARS.get(c, c) for c in self._partial]),
             '*'))

    def get_unique_primary_for_index_key(self, key):
        """Return the record number on primary table given key on index."""
        self._create_cursor()
        fs = self._cursor.foundset_field_equals_value(key)
        rsc = fs._recordset.OpenCursor()
        try:
            if rsc.Accessible():
                recno = rsc.LastAdvancedRecNum()
            else:
                recno = None
        finally:
            fs._recordset.CloseCursor(rsc)
            #self._cursor._dptdb.DestroyRecordSet(fs)
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
        """A Cursor cursor with partial keys disabled.

        Detail of managing cursors on all the record sets in recordset are
        to be determiined.

        """
        super().__init__(
            dptdb, fieldname, keyrange=keyrange, recordset=recordset[None])

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

    def __init__(self,
                 dptdb,
                 fieldname,
                 dptfieldname,
                 dptprimaryfieldname,
                 keyrange=None,
                 recordset=None):

        # Introduction of DataClient.refresh_cursor method in solentware_grid
        # package may force _foundset to be implementaed as a list to avoid
        # time problems positioning cursor somewhere in a large foundset.
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None
        self._delete_foundset_on_close_cursor = True
        #self._dptdb = None
        #self._fieldname = None
        #self._nonorderedfield = None

        if not isinstance(dptdb,
                          dptapi.APIDatabaseFileContext):
            msg = ' '.join(['The dptdb argument must be a',
                            ''.join([dptapi.APIDatabaseFileContext.__name__,
                                     ',']),
                            'or a subclass, instance.'])
            raise DatabaseError(msg)
        
        self._dptdb = dptdb
        self._fieldname = fieldname
        self._dptfieldname = dptfieldname

        # Assume only visible field contains the stored Python object.
        # Move this to validation on opening database?
        #nonorderedfield = None
        #fac = dptdb.OpenFieldAttCursor()
        #name = dptapi.StdStringPtr()
        #while fac.Accessible():
        #    name.assign(fac.Name())
        #    fn = name.value()
        #    atts = fac.Atts()
        #    if atts.IsVisible():
        #        if self._nonorderedfield:
        #            msg = 'More than one visible field defined on file'
        #            raise DatabaseError(msg)
        #        nonorderedfield = fn
        #    fac.Advance(1)
        #dptdb.CloseFieldAttCursor(fac)

        self._fieldvalue = dptapi.APIFieldValue()
        self._nonorderedfield = dptprimaryfieldname == dptfieldname

        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant. Safe because
        # self._partial in RecordsetCursorDPT instances is None always.
        # self._foundset must be this instance's scratch set and a separate
        # permanent reference for recordset, if not None, kept for use by
        # foundset_all_records and similar methods.
        if self._nonorderedfield:

            # A record set cursor.
            if recordset:
                self._foundset = recordset
                self._delete_foundset_on_close_cursor = False
            else:
                self._foundset = self.foundset_all_records()
            self._rscursor = self._foundset._recordset.OpenCursor()
            return

        # A record set cursor managed by a direct value cursor.
        self._dvcursor = self._dptdb.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(dptfieldname))
        self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        self._first_by_value()

    def __del__(self):
        self.close()

    def close(self):
        if self._dvcursor:
            self._dptdb.CloseDirectValueCursor(self._dvcursor)
        if self._foundset:
            if self._rscursor:
                self._foundset._recordset.CloseCursor(self._rscursor)
            #if self._delete_foundset_on_close_cursor:
            #    self._dptdb.DestroyRecordSet(self._foundset)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None
        self._dptdb = None

    def first(self):
        if self._dvcursor is not None:
            self._new_value_context()
            self._first_by_value()
            r = self._rscursor.AccessCurrentRecordForRead()
            return (
                self._dvcursor.GetCurrentValue().ExtractString(),
                r.RecNum())
        else:
            try:
                self._rscursor.GotoFirst()
                if not self._rscursor.Accessible():
                    return None
                r = self._rscursor.AccessCurrentRecordForRead()
                return (
                    r.RecNum(),
                    self._join_primary_field_occs(r))
            except AttributeError:
                if self._rscursor is None:
                    return None
                else:
                    raise

    def last(self):
        if self._dvcursor is not None:
            self._new_value_context()
            self._last_by_value()
            r = self._rscursor.AccessCurrentRecordForRead()
            return (
                self._dvcursor.GetCurrentValue().ExtractString(),
                r.RecNum())
        else:
            try:
                self._rscursor.GotoLast()
                if not self._rscursor.Accessible():
                    return None
                r = self._rscursor.AccessCurrentRecordForRead()
                return (
                    r.RecNum(),
                    self._join_primary_field_occs(r))
            except AttributeError:
                if self._rscursor is None:
                    return None
                else:
                    raise

    def next(self):
        rsc = self._rscursor
        try:
            rsc.Advance(1)
            if rsc.Accessible():
                r = self._rscursor.AccessCurrentRecordForRead()
                if self._dvcursor is None:
                    return (
                        r.RecNum(),
                        self._join_primary_field_occs(r))
                else:
                    return (
                        self._dvcursor.GetCurrentValue().ExtractString(),
                        r.RecNum())
        except AttributeError:
            if rsc is None:
                return None
            else:
                raise

        if self._dvcursor is not None:
            context = self._dptdb
            while not self._rscursor.Accessible():
                self._dvcursor.Advance(1)
                if self._dvcursor.Accessible():
                    self._foundset._recordset.CloseCursor(self._rscursor)
                    #context.DestroyRecordSet(self._foundset)
                    self._foundset = self.foundset_field_equals_value(
                        self._dvcursor.GetCurrentValue())
                    self._rscursor = self._foundset._recordset.OpenCursor()
                    if self._rscursor.Accessible():
                        r = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self._dvcursor.GetCurrentValue().ExtractString(),
                            r.RecNum())
                else:
                    break

            # No more records for current position of direct value cursor 
            self._new_value_context()
            self._last_by_value()
        else:

            # No more records on record set cursor. 
            self._last()

    def prev(self):
        rsc = self._rscursor
        try:
            rsc.Advance(-1)
            if rsc.Accessible():
                r = self._rscursor.AccessCurrentRecordForRead()
                if self._dvcursor is None:
                    return (
                        r.RecNum(),
                        self._join_primary_field_occs(r))
                else:
                    return (
                        self._dvcursor.GetCurrentValue().ExtractString(),
                        r.RecNum())
        except AttributeError:
            if rsc is None:
                return None
            else:
                raise

        if self._dvcursor is not None:
            context = self._dptdb
            while not self._rscursor.Accessible():
                self._dvcursor.Advance(-1)
                if self._dvcursor.Accessible():
                    self._foundset._recordset.CloseCursor(self._rscursor)
                    #context.DestroyRecordSet(self._foundset)
                    self._foundset = self.foundset_field_equals_value(
                        self._dvcursor.GetCurrentValue())
                    self._rscursor = self._foundset._recordset.OpenCursor()
                    self._rscursor.GotoLast()
                    if self._rscursor.Accessible():
                        r = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self._dvcursor.GetCurrentValue().ExtractString(),
                            r.RecNum())
                else:
                    break

            # No more records for current position of direct value cursor 
            self._new_value_context()
            self._first_by_value()
        else:

            # No more records on record set cursor. 
            self._first()

    def refresh_recordset_keep_position(self):
        if self._foundset:
            key = self._rscursor.LastAdvancedRecNum()
            self._foundset._recordset.CloseCursor(self._rscursor)
            #self._dptdb.DestroyRecordSet(self._foundset)
        else:
            key = -1 # (first + last) < key * 2
        if self._nonorderedfield:
            self._foundset = self._dptdb.foundset_all_records(self._fieldname)
        elif self._dvcursor is not None:
            self._foundset = self.foundset_field_equals_value(
                self._dvcursor.GetCurrentValue())
        else:
            self._dvcursor = self._dptdb.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(self._dptfieldname))
            self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            self._first_by_value()
            if self._foundset is None:
                return
        self._rscursor = self._foundset._recordset.OpenCursor()
        rsc = self._rscursor
        rsc.GotoLast()
        last = rsc.LastAdvancedRecNum()
        rsc.GotoFirst()
        first = rsc.LastAdvancedRecNum()
        if (first + last) < key * 2:
            rsc.GotoLast()
            adv = -1
            while rsc.Accessible():
                if key <= rsc.LastAdvancedRecNum():
                    return
                rsc.Advance(adv)
            self._foundset._recordset.CloseCursor(rsc)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoFirst()
        else:
            adv = 1
            while rsc.Accessible():
                if key >= rsc.LastAdvancedRecNum():
                    return
                rsc.Advance(adv)
            self._foundset._recordset.CloseCursor(rsc)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoLast()

    def set(self, key):
        rsc = self._rscursor
        try:
            pos = rsc.LastAdvancedRecNum()
            if pos > key:
                adv = -1
            elif pos < key:
                adv = 1
            while rsc.Accessible():
                if key == rsc.LastAdvancedRecNum():
                    r = self._rscursor.AccessCurrentRecordForRead()
                    return (
                        r.RecNum(),
                        self._join_primary_field_occs(r))
                rsc.Advance(adv)
        except AttributeError:
            if rsc is None:
                return None
            else:
                raise

        return None

    def set_range(self, key):
        if self._dvcursor is None:
            return self.set(key)

        dvc = self._dvcursor
        try:
            self._fieldvalue.Assign(key)
            dvc.SetRestriction_LoLimit(self._fieldvalue, True)
            dvc.GotoFirst()
        except AttributeError:
            if dvc is None:
                return None
            else:
                raise

        context = self._dptdb
        while dvc.Accessible():
            self._foundset._recordset.CloseCursor(self._rscursor)
            #context.DestroyRecordSet(self._foundset)
            self._foundset = self.foundset_field_equals_value(
                dvc.GetCurrentValue())
            self._rscursor = self._foundset._recordset.OpenCursor()
            if self._rscursor.Accessible():
                r = self._rscursor.AccessCurrentRecordForRead()
                return (
                    self._dvcursor.GetCurrentValue().ExtractString(),
                    r.RecNum())
            dvc.Advance(1)

        # Run off end available records. 
        self._new_value_context()
        self._last_by_value()

        return None

    def set_both(self, key, value):

        # Need to take account of the direction cursor moves to get from
        # current position to (key, value).  dvc component is fine but always
        # stepping forward through rsc component is wrong.  set does it right.
        dvc = self._dvcursor
        try:
            cpos = dvc.GetCurrentValue().ExtractString()
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
                self._fieldvalue.Assign(key)
                dvc.SetPosition(self._fieldvalue)
                pos = dvc.GetCurrentValue().ExtractString()
                if pos == key:
                    npos = pos
                else:
                    npos = None
        except AttributeError:
            if dvc is None:
                return None
            else:
                raise

        if dvc.Accessible():
            if key != npos:
                return None
            if key != cpos:
                context = self._dptdb
                self._foundset._recordset.CloseCursor(self._rscursor)
                #context.DestroyRecordSet(self._foundset)
                self._foundset = self.foundset_field_equals_value(
                    dvc.GetCurrentValue())
                self._rscursor = self._foundset._recordset.OpenCursor()
                if advance > 0:
                    self._rscursor.GotoFirst()
                else:
                    self._rscursor.GotoLast()
            rsc = self._rscursor
            while rsc.Accessible():
                if value == rsc.LastAdvancedRecNum():
                    r = self._rscursor.AccessCurrentRecordForRead()
                    return (
                        self._dvcursor.GetCurrentValue().ExtractString(),
                        r.RecNum())
                rsc.Advance(advance)

        # Set by key and value failed. 
        self._new_value_context()
        self._first_by_value()

        return None

    def _foundset_all_records(self):
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self._dptfieldname,
                    dptapi.FD_ALLRECS,
                    dptapi.APIFieldValue(''))))

    def _foundset_field_equals_value(self, value):
        if isinstance(value, dptapi.APIFieldValue):
            return _DPTFoundSet(
                self._dptdb,
                self._dptdb.FindRecords(
                    dptapi.APIFindSpecification(
                        self._dptfieldname, dptapi.FD_EQ, value)))
        else:
            return _DPTFoundSet(
                self._dptdb,
                self._dptdb.FindRecords(
                    dptapi.APIFindSpecification(
                        self._dptfieldname,
                        dptapi.FD_EQ,
                        dptapi.APIFieldValue(value))))
        
    def _foundset_record_number(self, recnum):
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_SINGLEREC,
                    recnum)))

    def _foundset_records_before_record_number(self, recnum):
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_NOT_POINT,
                    recnum)))

    def _foundset_records_not_before_record_number(self, recnum):
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_POINT,
                    recnum)))

    def _foundset_recordset_before_record_number(self, recnum, recordset):
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_NOT_POINT,
                    recnum),
                recordset._recordset))

    def foundset_all_records(self):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_all_records()
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self._dptfieldname,
                    dptapi.FD_ALLRECS,
                    dptapi.APIFieldValue('')),
                self._foundset._recordset))

    def foundset_field_equals_value(self, value):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_field_equals_value(value)
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    self._dptfieldname,
                    dptapi.FD_EQ,
                    dptapi.APIFieldValue(value)),
                self._foundset._recordset))

    def foundset_record_number(self, recnum):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_record_number(recnum)
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_SINGLEREC,
                    recnum),
                self._foundset._recordset))

    def foundset_records_before_record_number(self, recnum):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_records_before_record_number(recnum)
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_NOT_POINT,
                    recnum),
                self._foundset._recordset))

    def foundset_records_not_before_record_number(self, recnum):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_records_not_before_record_number(recnum)
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_POINT,
                    recnum),
                self._foundset._recordset))

    def foundset_recordset_before_record_number(self, recnum, recordset):
        if self._delete_foundset_on_close_cursor:
            return self._foundset_recordset_before_record_number(
                recnum, recordset)
        return _DPTFoundSet(
            self._dptdb,
            self._dptdb.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_NOT_POINT,
                    recnum),
                recordset._recordset))

    def _first(self):
        self._foundset._recordset.CloseCursor(self._rscursor)
        rsc = self._foundset._recordset.OpenCursor()
        if rsc.Accessible():
            self._rscursor = rsc
            return
        self._foundset._recordset.CloseCursor(rsc)
        self._rscursor = None

    def _first_by_value(self):
        context = self._dptdb
        dvc = self._dvcursor
        dvc.GotoFirst()
        while dvc.Accessible():
            fs = self._foundset_field_equals_value(
                dvc.GetCurrentValue())
            rsc = fs._recordset.OpenCursor()
            if rsc.Accessible():
                self._rscursor = rsc
                self._foundset = fs
                return
            fs._recordset.CloseCursor(rsc)
            #context.DestroyRecordSet(fs)
            dvc.Advance(1)
        context.CloseDirectValueCursor(dvc)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None

    def _join_primary_field_occs(self, record):
        advance = record.AdvanceToNextFVPair
        fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        v = []
        while advance():
            v.append(valueocc().ExtractString())
        return ''.join(v)

    def _last(self):
        self._foundset._recordset.CloseCursor(self._rscursor)
        rsc = self._foundset._recordset.OpenCursor()
        if rsc.Accessible():
            rsc.GotoLast()
            self._rscursor = rsc
            return
        self._foundset._recordset.CloseCursor(rsc)
        self._rscursor = None

    def _last_by_value(self):
        context = self._dptdb
        dvc = self._dvcursor
        dvc.GotoLast()
        while dvc.Accessible():
            fs = self.foundset_field_equals_value(
                dvc.GetCurrentValue())
            rsc = fs._recordset.OpenCursor()
            if rsc.Accessible():
                rsc.GotoLast()
                self._rscursor = rsc
                self._foundset = fs
                return
            fs._recordset.CloseCursor(rsc)
            #context.DestroyRecordSet(fs)
            dvc.Advance(-1)
        context.CloseDirectValueCursor(dvc)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None

    def _new_value_context(self):
        context = self._dptdb
        context.CloseDirectValueCursor(self._dvcursor)
        self._foundset._recordset.CloseCursor(self._rscursor)
        #context.DestroyRecordSet(self._foundset)
        self._dvcursor = context.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(self._dptfieldname))
        self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)


# Attempt to cope with absence of & ^ | &= ^= |= operators in dptapi interface
# to DPT recordlist objects via APIRecordList class.
class _DPTRecordSet:

    """Methods common to _DPTRecordList and _DPTFoundSet."""

    def __del__(self):
        """Destroy APIRecordList instance if not done by explicit close()."""
        if self._recordset:
            self.close()

    def __or__(self, other):
        """Return _DPTRecordList of records in self or other."""
        r = _DPTRecordList(self._context)
        r._recordset.Place(self._recordset)
        r._recordset.Place(other._recordset)
        return r

    def __and__(self, other):
        """Return _DPTRecordList of records in self and other."""
        r = _DPTRecordList(self._context)
        r._recordset.Place(self._recordset)
        r &= other
        return r

    def __xor__(self, other):
        """Return _DPTRecordList of records in self or other, but not both."""
        r = _DPTRecordList(self._context)
        r._recordset.Place(self._recordset)
        r ^= other
        return r

    def close(self):
        """Destroy the APIRecordSet instance.

        If close() is called more than once for an instance an AttributeError
        will be raised.  This follows DPT where DestroyRecordSet() calls, after
        the first, for an APIRecordSet instance raise a RuntimeError.

        """
        self._context.DestroyRecordSet(self._recordset)

        # This should cause deletion of the APIRecordSet object, but may not
        # cause deletion of this _DPTRecordSet instance.
        self._recordset = None

        # This probably does not cause deletion of the APIDatabaseContext but
        # does mean subsequent close() calls will raise an AttributeError.
        self._context = None

    def count_records(self):
        """Return count of records in the record set."""
        return self._recordset.Count()


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
        """Create _DPTRecordList wrapping an empty dptapi.APIRecordList
        belonging to a dptapi.APIDatabaseFileContext."""
        self._context = context
        self._recordset = context.CreateRecordList()

    def __ior__(self, other):
        """Return self, a _DPTRecordList, with records in self or other."""
        self._recordset.Place(other._recordset)
        return self

    def __iand__(self, other):
        """Return self, a _DPTRecordList, with records in self and other."""
        r = _DPTRecordList(self._context)
        r._recordset.Place(self._recordset)
        r._recordset.Remove(other._recordset)
        self._recordset.Remove(r._recordset)
        return self

    def __ixor__(self, other):
        """Return self, a _DPTRecordList, with records in self or other, but
        not both.
        """
        r = _DPTRecordList(self._context)
        r._recordset.Place(other._recordset)
        r._recordset.Remove(self._recordset)
        self._recordset.Remove(r._recordset)
        return self

    def clear_recordset(self):
        self._recordset.Clear()

    def place_record_number(self, record_number):
        """Place record record_number on self, a _DPTRecordList."""
        self._recordset.Place(record_number)

    def remove_record_number(self, record_number):
        """Remove record record_number on self, a _DPTRecordList."""
        self._recordset.Remove(record_number)

    def remove_recordset(self, recordset):
        """Remove other's records from recordset using Remove method.

        Equivalent to '|=' and '^=' sequence in _database version of method.
        """
        self._recordset.Remove(recordset._recordset)

    def replace_records(self, newrecords):
        """Replace records in recordset with newrecords.

        This method exists for compatibility with DPT where simply binding an
        attribute to newrecords may not be correct.

        """
        self._recordset.Clear()
        self._recordset.Place(newrecords._recordset)


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
        """Create _DPTFoundSet wrapping a dptapi.APIFoundSet belonging to
        a dptapi.APIDatabaseFileContext."""
        self._context = context
        self._recordset = foundset
