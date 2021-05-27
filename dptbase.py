# dptbase.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide DPT file access using bsddb style methods.

The original interface used bsddb.  For compatibility DPT file access is
done using the same method names where possible.  DPT provides more complex
index manipulation.  Use the DPT API directly for this at the expense of
compatibility with bsddb.

The DPTbaseRecord.open_root method creates a file but does not leave it open
for record level access.  Subclasses in this package extend the open_root
method to open files for deferred update or non-deferred update.  Non-deferred
updates are recoverable and can be backed out.  Deferred updates are not but
take much less time (for large volume file loads).

This module on Windows and Wine only.
On freebsd6 invoke python in deferred update subprocess but pythonw on
win32 to avoid getting a CMD window.

See www.dptoolkit.com for details of DPT

List of classes

DPTbaseError - Exceptions
DPTbase - DPT database definition and API
DPTbaseFile - DPT file definition and file level access
DPTbaseRecord - DPT record level access
CursorDPT - Define cursor on file and access methods
_CursorDPT - DPT record set and direct value set access

"""

from api.database import DatabaseError

import sys
_platform_win32 = sys.platform == 'win32'
del sys

if not _platform_win32:
    raise DatabaseError, 'Platform is not "win32"'

import os
import os.path
import subprocess

from dptdb import dptapi

from api.database import (
    Database, Cursor, decode_record_number, encode_record_number,
    )
from api.constants import (
    FLT, INV, UAE, ORD, ONM, SPT,
    BSIZE, BRECPPG, BRESERVE, BREUSE,
    DSIZE, DRESERVE, DPGSRES,
    FILEORG, DEFAULT, EO, RRN, SUPPORTED_FILEORGS,
    MANDATORY_FILEATTS, FILEATTS,
    PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, DPT_FIELDATTS,
    DDNAME, FILE, FILEDESC, FOLDER, FIELDS,
    PRIMARY, SECONDARY, DEFER,
    BTOD_FACTOR, BTOD_CONSTANT, DEFAULT_RECORDS, DEFAULT_INCREASE_FACTOR,
    DPT_DEFER_FOLDER, DPT_DU_SEQNUM, DPT_SYS_FOLDER,
    )
from basesup.gui.dptfilesize import get_sizes_for_new_files

file_parameter_list = (
    'BHIGHPG', 'BSIZE', 'DPGSRES', 'DPGSUSED', 'DRESERVE', 'DSIZE', 'FIFLAGS')


class DPTbaseError(DatabaseError):
    pass


class DPTbase(Database):
    
    """Implement Database API using DPT database.

    DPT databases consist of one or more files each of which has zero or
    more fields defined. File names are unique and field names are unique
    within a file.  Each file contains zero or more records where each
    record contains zero or more occurrences of each field defined on the
    file.  Records are identified by a record number that is unique within
    a file.  The lowest possible record number is zero.
    Applications are expected to store instances of at least one class on
    a file; using multiple occurrences of one field to store a pickled
    instance of a class and multiple occurrences of other fields to
    implement indexes.  Field names can be shared between classes without
    restriction; but usually such sharing will turn out to be rare.

    Methods added:

    create_default_parms
    do_deferred_updates
    get_database_instance
    open_context_allocated
    set_defer_update
    unset_defer_update
    __del__
    make_root

    Methods overridden:

    __init__
    backout
    close_context
    close_database
    commit
    close_internal_cursors
    db_compatibility_hack
    delete_instance
    edit_instance
    exists
    files_exist
    make_cursor
    get_database_folder
    get_database
    get_database_increase
    get_database_parameters
    get_first_primary_key_for_index_key
    get_primary_record
    make_internal_cursors
    increase_database_size
    initial_database_size
    is_primary
    is_primary_recno
    is_recno
    open_context
    get_packed_key
    decode_as_primary_key
    encode_primary_key
    put_instance

    Methods extended:

    None
    
    """

    def __init__(self, DPTfiles, DPTfolder, **kargs):
        """Define DPT database.

        DPTfiles = {name:{ddname:name,
                          folder:name,
                          file:name,
                          filedesc:{property:value, ...},
                          fields:{name:{property:value, ...}, ...},
                          }, ...}
        DPTfolder = folder for files unless overridden in DPTfiles
        **kargs = DPT database system parameters

        """
        #The database definition from DPTfiles after validation.
        #Note that folder is removed and file becomes the absolute path.
        self._dptfiles = None
        
        #The folder from DPTfolder after validation
        self._dptfolder = None

        #APIDatabaseServices object
        self._dbserv = None

        #APISequentialFileServices object
        self._sfserv = None

        try:
            dptfolder = os.path.abspath(DPTfolder)
        except:
            msg = ' '.join(['Main folder name', str(DPTfolder),
                            'is not valid'])
            raise DPTbaseError, msg
        
        #The database system parameters. DPT assumes reasonable defaults
        #for any values sought in self._dptkargs.

        #At Python26+ need to convert unicode to str for DPT
        self._dptsysfolder = str(kargs.get(
            DPT_SYS_FOLDER, os.path.join(dptfolder, DPT_SYS_FOLDER)))
        self._sysprint = str(kargs.get(
            'sysprint', os.path.join(self._dptsysfolder, 'sysprint.txt')))
        self._parms = str(kargs.get(
            'parms', os.path.join(self._dptsysfolder, 'parms.ini')))
        self._msgctl = str(kargs.get(
            'msgctl', os.path.join(self._dptsysfolder, 'msgctl.ini')))
        self._audit = str(kargs.get(
            'audit', os.path.join(self._dptsysfolder, 'audit.txt')))
        self._username = str(kargs.get('username', 'dptapi'))
        
        #DPTfiles processing

        dptfiles = dict()
        pathnames = dict()
        sfi = 0

        if not isinstance(DPTfiles, dict):
            raise DPTbaseError, 'File definitions must be a dictionary'
        
        for dd in DPTfiles:
            if not isinstance(DPTfiles[dd], dict):
                msg = ' '.join(
                    ['DPT file definition for', repr(dd),
                     'must be a dictionary'])
                raise DPTbaseError, msg
            
            ddname = DPTfiles[dd][DDNAME]
            if len(ddname) == 0:
                raise DPTbaseError, 'Zero length DD name'
            
            elif len(ddname) > 8:
                raise DPTbaseError, 'DD name length over 8 characters'
            
            elif not ddname.isalnum():
                msg = ' '.join(['DD name', ddname, 'must be upper case',
                                'alphanum starting with alpha'])
                raise DPTbaseError, msg
                
            elif not ddname.isupper():
                msg = ' '.join(['DD name', ddname, 'must be upper case',
                                'alphanum starting with alpha'])
                raise DPTbaseError, msg
                
            elif not ddname[0].isupper():
                msg = ' '.join(['DD name', ddname, 'must be upper case',
                                'alphanum starting with alpha'])
                raise DPTbaseError, msg
                
            else:
                folder = DPTfiles[dd].get(FOLDER, None)
                filename = DPTfiles[dd].get(FILE, None)
                if folder is None:
                    folder = dptfolder
                try:
                    folder = os.path.abspath(folder)
                    #At Python26+ need to convert unicode to str for DPT
                    fname = str(os.path.join(folder, filename))
                except:
                    msg = ' '.join(
                        ['Full path name from file description', dd,
                         'is invalid'])
                    raise DPTbaseError, msg
                
                if fname in pathnames:
                    msg = ' '.join(['File name', fname,
                                    'linked to', pathnames[fname],
                                    'cannot link to', dd])
                    raise DPTbaseError, msg

                pathnames[fname] = dd
                dptfiles[dd] = self.make_root(
                    dd,
                    fname,
                    DPTfiles[dd],
                    sfi)
                sfi += 1

        self._dptfiles = dptfiles
        self._dptfolder = DPTfolder

    def backout(self):
        """Backout updates on all DPT files."""
        if self._dbserv:
            if self._dbserv.UpdateIsInProgress():
                self._dbserv.Backout()
            
    def close_context(self):
        """Close all DPT files."""
        if self._dbserv is None:
            return
        for dd in self._dptfiles:
            self._dptfiles[dd].close(self._dbserv)

    def close_database(self):
        """Close all DPT files and shut down database services."""
        if self._dbserv is None:
            return
        self.close_context()
        try:
            # #SEQTEMP and checkpoint.ckp are in self._dptsysfolder
            cwd = os.getcwd()
            os.chdir(os.path.abspath(self._dptsysfolder))
            self._dbserv.Destroy()
            os.chdir(cwd)
        except:
            pass

    def close_internal_cursors(self, dbnames=None):
        """Return True for compatibility with Berkeley DB subclass."""
        return True
            
    def commit(self):
        """Commit updates on all DPT files."""
        if self._dbserv:
            if self._dbserv.UpdateIsInProgress():
                self._dbserv.Commit()
            
    def db_compatibility_hack(self, record, srkey):
        """Convert to (key, value) format returned by Berkeley DB access.

        DPT is compatible with the conventions for Berkeley DB RECNO databases
        except for a Berkeley DB index where the primary key is not held as
        the value on an index record (maybe the primary key is embedded in the
        secondary key). Here the Berkeley DB index record is (key, None)
        rather than (key, primary key). The correponding DPT structure is
        always (ordered index field value, record number).
        DataClient works to Berkeley DB conventions.
        The user code side of DataClient adopts the appropriate Berkeley DB
        format because it defines the format used. The incompatibility that
        comes from mapping a (key, None) to DPT while using the same user code
        is dealt with in this method.

        """
        key, value = record
        if value is None:
            return (key, decode_record_number(srkey))
        else:
            return record

    def create_default_parms(self):
        """Create default parms.ini file."""
        if not os.path.exists(self._parms):
            pf = file(self._parms, 'w')
            try:
                pf.write("MAXBUF=10000 " + os.linesep)
            finally:
                pf.close()
                
    def delete_instance(self, filename, instance):
        """Delete an existing instance on file filename."""
        self._dptfiles[filename].delete_instance(instance)

    def do_deferred_updates(self, pyscript, filepath):
        """Invoke a deferred update process and wait for it to finish.

        pyscript is the script to do the deferred update.
        filepath is a file or a sequence of files containing updates.

        """
        if _platform_win32:
            args = ['pythonw']
        else:
            args = ['python']
        
        if not os.path.isfile(pyscript):
            msg = ' '.join([repr(pyscript),
                            'is not an existing file'])
            raise DPTbaseError, msg

        args.append(pyscript)
        
        try:
            if os.path.exists(filepath):
                paths = (filepath,)
            else:
                msg = ' '.join([repr(filepath),
                                'is not an existing file'])
                raise DPTbaseError, msg
        except:
            paths = tuple(filepath)
            for fp in paths:
                if not os.path.isfile(fp):
                    msg = ' '.join([repr(fp),
                                    'is not an existing file'])
                    raise DPTbaseError, msg

        args.append(os.path.abspath(self._dptfolder))
        args.extend(paths)

        return subprocess.Popen(args)

    def edit_instance(self, filename, instance):
        """Edit an existing instance on file filename."""
        self._dptfiles[filename].edit_instance(instance)

    def exists(self, dbname, dbfield):
        """Return True if dbname is named in dptfiles otherwise False.

        dbfield is relevant to Berkeley DB and retained for compatibility.

        """
        return dbname in self._dptfiles

    def files_exist(self):
        """Return True if all defined files exist in self._dptfolder folder."""
        fileset = set()
        for dd in self._dptfiles:
            fileset.add(self._dptfiles[dd]._file)
        filecount = len(fileset)
        for dd in self._dptfiles:
            if os.path.isfile(self._dptfiles[dd]._file):
                fileset.remove(self._dptfiles[dd]._file)
        if len(fileset) == filecount:
            return None
        return len(fileset) == 0

    def make_cursor(self, dbname, fieldname, keyrange=None):
        """Create a cursor on the DB.

        keyrange is an addition in DPT. It may yet be removed.

        """
        return self._dptfiles[dbname].make_cursor(fieldname, keyrange)
        
    def get_database_folder(self):
        """return database folder name"""
        return self._dptfolder
    
    def get_database(self, dbname, dbfield):
        """Return the APIDatabaseContext for dbname.

        dbfield is relevant to Berkeley DB and retained for compatibility.

        """
        return self._dptfiles[dbname].get_database()

    def get_database_instance(self, dbname, dbfield):
        """Return DPT instance for dbname in dbset."""
        return self._dptfiles[dbname]

    def get_first_primary_key_for_index_key(self, dbname, dbfield, key):
        """Get the record number on dbname given key for dbfield.

        This method should be used only on index fields whose keys each
        reference a single record. The intended use is where a key for a
        dbfield in dbname has been derived from a record in some other
        dbname.

        """
        return self._dptfiles[dbname].get_first_primary_key_for_index_key(
            dbfield, key)
    
    def get_primary_record(self, dbname, key):
        """Get the instance given the record number."""
        return self._dptfiles[dbname].get_primary_record(key)

    def make_internal_cursors(self, dbnames=None):
        """Return True for compatibility with Berkeley DB subclass."""
        return True
    
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
        for file_ in files:
            if file_ in self._dptfiles:
                self._dptfiles[file_].increase_file_size(
                    self._dbserv,
                    sizing_record_counts=files[file_])

    def initial_database_size(self):
        """Set initial file sizes as specified in file descriptions"""
        for v in self._dptfiles.itervalues():
            v.initial_file_size()
        return True

    def is_primary(self, dbname, dbfield):
        """Return True if dbfield is primary field (or not secondary)."""
        return self._dptfiles[dbname].is_field_primary(dbfield)

    def is_primary_recno(self, dbname):
        """Return True for compatibility with Berkeley DB.

        DPT record number is equivalent to Berkeley DB primary key.

        """
        return True

    def is_recno(self, dbname, dbfield):
        """Return True if dbfield is primary field (or not secondary). """
        return self._dptfiles[dbname].is_field_primary(dbfield)

    def open_context(self):
        """Open all files after creating them if necessary."""

        if not self.initial_database_size():
            return
        
        dptfolder = os.path.abspath(self._dptfolder)
        if not os.path.exists(dptfolder):
            os.mkdir(dptfolder)

        dptsysfolder = os.path.abspath(self._dptsysfolder)
        if not os.path.exists(dptsysfolder):
            os.mkdir(dptsysfolder)

        self.create_default_parms()
                
        if self._dbserv is None:
            # #SEQTEMP and checkpoint.ckp placed in self._dptsysfolder'
            cwd = os.getcwd()
            os.chdir(os.path.abspath(self._dptsysfolder))
            self._dbserv = dptapi.APIDatabaseServices(
                self._sysprint,
                self._username,
                self._parms,
                self._msgctl,
                self._audit)
            os.chdir(cwd)
            
        for dd in self._dptfiles:
            self._dptfiles[dd].open_root(self)
        return True

    def get_packed_key(self, dbname, instance):
        """Convert instance.key for use as database value.

        For DPT just return instance.key.pack().
        dbname is relevant to Berkeley DB and retained for compatibility.

        """
        return instance.key.pack()

    def decode_as_primary_key(self, dbname, pkey):
        """Convert pkey for use as database key.

        For DPT just return integer form of pkey.

        """
        #KEYCHANGE
        # Avoid isinstance test?
        if isinstance(pkey, int):
            return pkey
        else:
            return decode_record_number(pkey)

    def encode_primary_key(self, dbname, instance):
        """Convert instance.key for use as database value.

        For DPT just return self.get_packed_key() converted to string.

        """
        return encode_record_number(self.get_packed_key(dbname, instance))

    def put_instance(self, filename, instance):
        """Add a new instance to filename."""
        self._dptfiles[filename].put_instance(instance)
            
    def set_defer_update(self, db=None, duallowed=False):
        raise DPTbaseError, 'set_defer_update not implemented'

    def unset_defer_update(self, db=None):
        raise DPTbaseError, 'unset_defer_update not implemented'

    def __del__(self):
        """Close files and destroy APIDatabaseServices object."""

        if self._dbserv is None:
            return

        self.close_database()

    def make_root(self, name, fname, dptfile, sfi):

        return DPTbaseRecord(name, fname, dptfile, sfi)

    def get_database_parameters(self, files=None):
        """Return file parameters infomation for file names in files."""
        if files is None:
            files = ()
        sizes = {}
        for f in files:
            if f in self._dptfiles:
                sizes[f] = self._dptfiles[f].get_file_parameters(self._dbserv)
        return sizes

    def get_database_increase(self, files=None):
        """Return required file increases for file names in files."""
        if files is None:
            files = ()
        increases = {}
        for file_ in files:
            if file_ in self._dptfiles:
                increases[file_] = self._dptfiles[file_].get_tables_increase(
                    self._dbserv,
                    sizing_record_counts=files[file_])
        return increases

    def open_context_normal(self, files=()):
        """Open all files in normal mode.

        Intended use is to open files to examine file status, or perhaps the
        equivalent of DPT command VIEW TABLES, when the database is closed as
        far as the application subclass of DPTbase is concerned.

        It is assumed that the Database Services object exists.

        """
        for dd in files:
            if dd in self._dptfiles:
                root = self._dptfiles[dd]
                self._dbserv.Allocate(
                    root._ddname,
                    root._file,
                    FILEDISP_OLD)
                cs = APIContextSpecification(root._ddname)
                root._opencontext = self._dbserv.OpenContext(cs)

    def open_context_allocated(self, files=()):
        """Open all files in normal mode.

        Intended use is to open files to examine file status, or perhaps the
        equivalent of DPT command VIEW TABLES, when the database is closed as
        far as the application subclass of DPTbase is concerned.

        It is assumed that the Database Services object exists.

        """
        self.open_context_normal(self, files=files)


class DPTbaseFile(object):

    """Provide file level access to a DPT file.

    This class implements methods to create files and fields; and open and
    close files.
    Record level access is the responsibility of subclasses.

    The _primary and _secondary attribute names come from the original
    Berkeley DB classes and are retained to remind of the equivalences
    used:
    primary database (recno)
    key = DPT recnum;  value = TableB field value
    secondary database (btree)
    key = Invisible field value;  value = str(DPT recnum)

    The apparent crippling of DPT file versatility disappears if the TableB
    field value is a pickled class instance and the invisible field values
    are derived from it.  The result is like a persistent Python dictionary
    with multiple key domains and efficient selection of dictionary values
    using complex search criteria involving many key domains.

    Methods added:

    calculate_table_b_increase
    calculate_table_d_increase
    close
    create_files
    get_extents
    get_file_parameters
    get_tables_increase
    increase_file_size
    increase_size_of_full_file  -  right place?
    initial_file_size
    is_field_primary
    open_root
    open_folders

    Methods overridden:

    __init__

    Methods extended:

    None
    
    """

    def __init__(self, name, fname, dptdesc):
        """Define a DPT file.

        name = file description name
        fname = path to data file (.dpt) for ddname
        dptdesc = field description for data file

        """
        super(DPTbaseFile, self).__init__()

        primary = dptdesc.get(PRIMARY, name[0].upper() + name[1:])
        
        if primary not in dptdesc[FIELDS]:
            msg = ' '.join(['Primary field name', str(primary),
                            'for', name,
                            'does not have a field description'])
            raise DPTbaseError, msg
        
        self._name = name
        self._ddname = dptdesc[DDNAME]
        self._fields = dict()
        self._file = fname
        self._filedesc = dict()
        self._primary = primary
        self._secondary = dict()
        self._defer = dict()
        self._opencontext = None
        self._pyappend = dict()
        self._btod_factor = dptdesc[BTOD_FACTOR]
        self._btod_constant = dptdesc[BTOD_CONSTANT]
        self._default_records = dptdesc[DEFAULT_RECORDS]
        self._default_increase_factor = dptdesc[DEFAULT_INCREASE_FACTOR]

        # Functions to convert numeric keys to string representation.
        # By default base 256 with the least significant digit at the right.
        # least_significant_digit = string_value[-1] (lsd = sv[-1])
        # most_significant_digit = string_value[0]
        # This conversion makes string sort equivalent to numeric sort.
        # These functions introduced to allow dbapi.py and dptapi.py to be
        # interchangeable for user classes.
        # DPT (www.dptoolkit.com) does not allow CR or LF characters in data
        # in the default, and at one time only, deferred update mode.
        # decode_record_number and encode_record_number allow a base 128
        # conversion to be used with the top bit set thus providing one way
        # of avoiding the problem characters.  There are ways to avoid the
        # restriction allowing CR and LF in data.
        # DPT uses a big endian conversion (lsd = sv[0]).

        if SECONDARY in dptdesc:
            for s in dptdesc[SECONDARY]:
                if not isinstance(s, str):
                    msg = ' '.join(['Secondary field name', str(s),
                                    'for', self._ddname,
                                    'must be a string'])
                    raise DPTbaseError, msg

                secondary = dptdesc[SECONDARY][s]
                if secondary is None:
                    secondary = s[0].upper() + s[1:]

                if secondary == primary:
                    msg = ' '.join(['Secondary field name', str(s),
                                    'for', self._ddname,
                                    'cannot be same as primary'])
                    raise DPTbaseError, msg

                if secondary not in dptdesc[FIELDS]:
                    msg = ' '.join(['Secondary field name',
                                    str(secondary),
                                    'for', self._ddname, 'does not have',
                                    'a field description'])
                    raise DPTbaseError, msg

                self._secondary[s] = secondary
            
        filedesc = dptdesc.get(FILEDESC, None)
        if filedesc is not None:
            if not isinstance(filedesc, dict):
                msg = ' '.join(['Description of file', repr(self._ddname),
                                'must be a dictionary or "None"'])
                raise DPTbaseError, msg

            for attr in MANDATORY_FILEATTS:
                if attr not in filedesc:
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', self._ddname,
                                    'must be present'])
                    raise DPTbaseError, msg

            self._filedesc = FILEATTS.copy()
            for attr in filedesc:
                if attr not in FILEATTS:
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', self._ddname,
                                    'is not allowed'])
                    raise DPTbaseError, msg

                if attr not in MANDATORY_FILEATTS:
                    if not isinstance(filedesc[attr], int):
                        msg = ' '.join(['Attribute', repr(attr),
                                        'for file', self._ddname,
                                        'must be a number'])
                        raise DPTbaseError, msg
                elif not isinstance(filedesc[attr], MANDATORY_FILEATTS[attr]):
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for file', self._ddname,
                                    'is not correct type'])
                    raise DPTbaseError, msg

                self._filedesc[attr] = filedesc[attr]
            if filedesc.get(FILEORG, None) not in SUPPORTED_FILEORGS:
                msg = ' '.join(
                    ['File', self._ddname,
                     'must be "Entry Order" or',
                     '"Unordered and Reuse Record Number"'])
                raise DPTbaseError, msg

        else:
            self._filedesc = None

        fields = dptdesc.get(FIELDS, dict())
        if not isinstance(fields, dict):
            msg = ' '.join(['Field description of file', repr(self._ddname),
                            'must be a dictionary'])
            raise DPTbaseError, msg
        
        for fieldname in fields:
            if not isinstance(fieldname, str):
                msg = ' '.join(['Field name', repr(fieldname),
                                'in file', self._ddname, 'is invalid'])
                raise DPTbaseError, msg
            
            if not fieldname.isalnum():
                msg = ' '.join(['Field name', fieldname,
                                'in file', self._ddname, 'is invalid'])
                raise DPTbaseError, msg
            
            if not fieldname[0].isupper():
                msg = ' '.join(['Field name', fieldname, 'in file',
                                self._ddname, 'must start with upper case'])
                raise DPTbaseError, msg
            
            if self._primary == fieldname:
                fieldatts = PRIMARY_FIELDATTS
            else:
                fieldatts = SECONDARY_FIELDATTS
            self._fields[fieldname] = fieldatts.copy()
            description = fields[fieldname]
            if description is None:
                description = dict()
            if not isinstance(description, dict):
                msg = ' '.join(['Attributes for field', fieldname,
                                'in file', repr(self._ddname),
                                'must be a dictionary or "None"'])
                raise DPTbaseError, msg
            
            for attr in description:
                if attr not in fieldatts:
                    msg = ' '.join(['Attribute', repr(attr),
                                    'for field', fieldname,
                                    'in file', self._ddname,
                                    'is not allowed'])
                    raise DPTbaseError, msg
                
                if type(description[attr]) != type(fieldatts[attr]):
                    msg = ' '.join([attr, 'for field', fieldname,
                                    'in file', self._ddname, 'is wrong type'])
                    raise DPTbaseError, msg
                
                if attr == SPT:
                    if (description[attr] < 0 or
                        description[attr] > 100):
                        msg = ' '.join(['Split percentage for field',
                                        fieldname, 'in file', self._ddname,
                                        'is invalid'])
                        raise DPTbaseError, msg
                    
                if attr in DPT_FIELDATTS:
                    self._fields[fieldname][attr] = description[attr]

            if self._fields[fieldname][ONM]:
                self._pyappend[fieldname] = dptapi.pyAppendDouble
            elif self._fields[fieldname][ORD]:
                self._pyappend[fieldname] = dptapi.pyAppendStdString

        defer = dptdesc.get(DEFER, dict())
        if not isinstance(defer, dict):
            msg = ' '.join(
                ['Deferred update parameters of file', repr(self._ddname),
                 'must be a dictionary'])
            raise DPTbaseError, msg
        
        for fieldname in defer:
            if fieldname not in self._secondary:
                msg = ' '.join(['Field name', repr(fieldname),
                                'in file', self._ddname,
                                'is not a secondary field'])
                raise DPTbaseError, msg

            if not isinstance(defer[fieldname], int):
                msg = ' '.join(['Deferred update limit for field name',
                                repr(fieldname), 'in file', self._ddname,
                                'must be an integer'])
                raise DPTbaseError, msg

            self._defer[fieldname] = defer[fieldname]
        
    def close(self, dbserv):
        """Close DPT file."""
        try:
            self._opencontext.DestroyAllRecordSets()
            dbserv.CloseContext(self._opencontext)
            dbserv.Free(self._ddname)
        except:
            pass
        self._opencontext = None

    def create_files(self, dbserv):
        """Create and initialize DPT file and define fields."""
        dbserv.Allocate(
            self._ddname,
            self._file,
            dptapi.FILEDISP_COND)
        dbserv.Create(
            self._ddname,
            self._filedesc[BSIZE],
            self._filedesc[BRECPPG],
            self._filedesc[BRESERVE],
            self._filedesc[BREUSE],
            self._filedesc[DSIZE],
            self._filedesc[DRESERVE],
            self._filedesc[DPGSRES],
            self._filedesc[FILEORG])
        cs = dptapi.APIContextSpecification(self._ddname)
        oc = dbserv.OpenContext(cs)
        oc.Initialize()
        for field in self._fields:
            fa = dptapi.APIFieldAttributes()
            fld = self._fields[field]
            if fld[FLT]: fa.SetFloatFlag()
            if fld[INV]: fa.SetInvisibleFlag()
            if fld[UAE]: fa.SetUpdateAtEndFlag()
            if fld[ORD]: fa.SetOrderedFlag()
            if fld[ONM]: fa.SetOrdNumFlag()
            fa.SetSplitPct(fld[SPT])
            oc.DefineField(field, fa)
        dbserv.CloseContext(oc)
        dbserv.Free(self._ddname)
            
    def get_extents(self):
        """Get current extents for file."""
        extents = dptapi.IntVector()
        self._opencontext.ShowTableExtents(extents)
        return extents

    def get_file_parameters(self, dbserv):
        """Get current values of selected file parameters."""
        vr = dbserv.Core().GetViewerResetter()
        fp = dict()
        fp['FISTAT'] = (
            vr.ViewAsInt('FISTAT', self._opencontext),
            vr.View('FISTAT', self._opencontext),
            )
        for p in file_parameter_list:
            fp[p] = vr.ViewAsInt(p, self._opencontext)
        for p in (dptapi.FIFLAGS_FULL_TABLEB, dptapi.FIFLAGS_FULL_TABLED):
            fp[p] = bool(fp['FIFLAGS'] & p)
        return fp

    def is_field_primary(self, dbfield):
        """Return true if field is primary (not secondary test used)."""
        return dbfield not in self._secondary

    def open_root(self, db):
        """Open file after creating it if necessary."""
        self.open_folders()
        if not os.path.exists(self._file):
            self.create_files(db._dbserv)
            
    def open_folders(self):
        """Create folder hierarchy to file location if necessary."""
        pathname = self._file
        foldername, filename = os.path.split(pathname)
        if os.path.exists(foldername):
            if not os.path.isdir(foldername):
                msg = ' '.join([foldername, 'exists but is not a folder'])
                raise DPTbaseError, msg
        else:
            os.makedirs(foldername)
        if os.path.exists(pathname):
            if not os.path.isfile(pathname):
                msg = ' '.join([pathname, 'exists but is not a file'])
                raise DPTbaseError, msg
            
    def initial_file_size(self):
        """Set initial file size as specified in file description"""
        if not os.path.exists(self._file):
            f = self._filedesc
            if f[BSIZE] is None:
                records = self._default_records
                bsize = int(round(records / f[BRECPPG]))
                if bsize * f[BRECPPG] < records:
                    bsize += 1
                dsize = int(round(bsize * self._btod_factor) +
                            self._btod_constant)
                f[BSIZE] = bsize
                f[DSIZE] = dsize
        return True

    def increase_file_size(self, dbserv, sizing_record_counts=None):
        """Increase file size if file nearly full"""
        if self._opencontext is not None:
            table_B_needed, table_D_needed = self.get_tables_increase(
                dbserv, sizing_record_counts=sizing_record_counts)
            if len(self.get_extents()) % 2:
                if table_B_needed:
                    self._opencontext.Increase(table_B_needed, False)
                if table_D_needed:
                    self._opencontext.Increase(table_D_needed, True)
            elif table_D_needed:
                self._opencontext.Increase(table_D_needed, True)
                if table_B_needed:
                    self._opencontext.Increase(table_B_needed, False)
            elif table_B_needed:
                self._opencontext.Increase(table_B_needed, False)

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
        deferred = size_filled['FISTAT'][0] == dptapi.FISTAT_DEFERRED_UPDATES
        if b_filled:
            b_increase = ((((b_diff_imp + b_spare) * 6) / 5))
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) / 5)),
                b_increase * self._btod_factor - d_spare)
        elif d_filled:
            b_increase = b_diff_imp
            d_increase = max(
                ((((d_diff_imp + d_spare) * 6) / 5)),
                b_increase * self._btod_factor - d_spare)
        elif deferred:
            b_increase = b_diff_imp
            d_increase = d_diff_imp
        else:
            b_increase = 0
            d_increase = 0
        if b_increase > 0 and d_increase > 0:
            if len(self.get_extents()) % 2:
                self._opencontext.Increase(b_increase, False)
                self._opencontext.Increase(d_increase, True)
            else:
                self._opencontext.Increase(d_increase, True)
                self._opencontext.Increase(b_increase, False)
        elif b_increase > 0:
            self._opencontext.Increase(b_increase, False)
        elif d_increase > 0:
            self._opencontext.Increase(d_increase, True)
        return

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
            unused = unused * self._filedesc[BRECPPG]
        if unused is None:
            if increase is not None:
                return increase
        elif increase is not None:
            if increase > unused:
                return increase
        increase =  int((1 + self._default_records) *
                        self._default_increase_factor)
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
        """Return the number of pages to add to DPT file index area.

        unused - current spare pages in Table D or None
        increase - number of extra records or None
        table_b_increase - increase index to match extra data pages if not None

        """
        if unused is not None:
            unused = (unused * self._filedesc[BRECPPG]) / self._btod_factor
        if table_b_increase is None:
            if unused is None:
                if increase is not None:
                    return increase
            elif increase is not None:
                if increase > unused:
                    return increase
            increase =  int((1 + self._default_records) *
                            self._default_increase_factor)
            if unused is not None:
                if increase > unused:
                    return increase - unused
        else:
            increase = int(table_b_increase * self._filedesc[BRECPPG])
            if unused is not None:
                if increase > unused:
                    return increase
        if unused is None:
            return increase
        return 0

    def get_tables_increase(self, dbserv, sizing_record_counts=None):
        """Return tuple (Table B, Table D) increase needed or None"""
        if self._opencontext is not None:
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
                increase_record_counts[0] / self._filedesc[BRECPPG],
                ((increase_record_counts[1] * self._btod_factor)
                 / self._filedesc[BRECPPG]),
                )


class DPTbaseRecord(DPTbaseFile):

    """Provide record level access to a DPT file.

    This class implements methods to create delete and edit records.

    make_cursor provides access to records via record set or direct value
    cursors.  The close method deletes these objects when the file is closed.

    Subclasses, or their users, should use the DPT API in more complex
    cases.  These are the cases where DPT should be superior to the
    alternatives.

    Record definition is the responsibility of the Python classes whose
    instances are stored on the file.

    Methods added:

    delete_instance
    edit_instance
    get_database
    make_cursor
    get_first_primary_key_for_index_key
    get_primary_record
    join_primary_field_occurrences
    make_identity_getter - not implemented
    put_instance
    foundset_all_records
    foundset_field_equals_value
    foundset_record_number
    foundset_records_before_record_number
    foundset_records_not_before_record_number
    foundset_recordset_before_record_number

    Methods overridden:

    None

    Methods extended:

    __init__
    close
    
    """

    def __init__(self, name, fname, dptdesc, sfi):
        """Define a DPT file.

        See base class for argument descriptions.

        """
        super(DPTbaseRecord, self).__init__(name, fname, dptdesc)
        
        #Permanent instances for efficient file updates
        self._fieldvalue = dptapi.APIFieldValue()
        self._putrecordcopy = dptapi.APIStoreRecordTemplate()

        #All active CursorDPT objects opened by make_cursor
        self._cursors = dict()

        #All active DPTDataSource objects
        self._sources = dict()

        #Not implemented. Intended to provide record number independent
        #record identities for cross-file reference.
        #Without it file reorgs are dangerous at least.
        self._GetNextIdentity = self.make_identity_getter(None)

    def close(self, dbserv):
        """Extend close to close active CursorDPT and DPTDataSource objects."""
        for c in self._cursors.keys():
            c.close()
        self._cursors.clear()
        for d in self._sources.keys():
            d.close()
        self._sources.clear()
        super(DPTbaseRecord, self).close(dbserv)
        
    def delete_instance(self, instance):
        """Delete the record containing the instance.

        srindex is a dictionary by field name of lists of values to be
        deleted from the index. Callbacks are used to delete instances
        from subsidiary files where the field name is in _deletecallbacks.
        Callbacks are used to take the record number into account for the
        index value if the field name is in _valuecallbacks. Other fields
        are dealt with directly.

        """
        instance.srkey = encode_record_number(instance.key.pack())
        instance.set_packed_value_and_indexes()
        sri = instance.srindex
        sec = self._secondary
        dcb = instance._deletecallbacks
        fieldvalue = self._fieldvalue
        Assign = fieldvalue.Assign
        fd = self.foundset_record_number(instance.key.pack())
        rsc = fd.OpenCursor()
        while rsc.Accessible():
            r = rsc.AccessCurrentRecordForReadWrite()
            for s in sri:
                if s in dcb:
                    dcb[s](instance, sri[s])
                else:
                    f = sec[s]
                    for v in sri[s]:
                        Assign(v)
                        r.DeleteFieldByValue(
                            f,
                            fieldvalue)
            r.Delete()
            rsc.Advance(1)
        fd.CloseCursor(rsc)
        self._opencontext.DestroyRecordSet(fd)

    def edit_instance(self, instance):
        """Edit the record containing the instance.

        The data as read from file is in instance and instance.newrecord
        contains the new values.
        If the record numbers are different use delete_instance to delete
        the current values and put_instance to add a new record containing
        the new values.
        Otherwise all the fields referred to in instance are deleted from
        the record and all the field referred to in instance.newrecord are
        added to the record.
        It is possible for index fields to escape deletion if they were
        added after the read of instance. A check that the primary field
        has not changed before allowing the edit is not done because
        inequality of pickled value does not guarantee inequality of class
        instance because the order of dictionary adds and deletes to get to
        the same contents affects the pickled value.

        """
        if instance.key != instance.newrecord.key:
            self.delete_instance(instance)
            self.put_instance(instance.newrecord)
            return
        
        instance.srkey = encode_record_number(instance.key.pack())
        instance.newrecord.srkey = encode_record_number(
            instance.newrecord.key.pack())
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
        sec = self._secondary
        fieldvalue = self._fieldvalue
        Assign = fieldvalue.Assign
        fd = self.foundset_record_number(instance.key.pack())
        rsc = fd.OpenCursor()
        while rsc.Accessible():
            r = rsc.AccessCurrentRecordForReadWrite()
            f = self._primary
            r.DeleteEachOccurrence(f)
            for i in range(0, len(nsrv), 255):
                Assign(nsrv[i:i+255])
                r.AddField(
                    f,
                    fieldvalue)
            for s in ionly:
                if s in dcb:
                    dcb[s](instance, sri[s])
                else:
                    f = sec[s]
                    for v in sri[s]:
                        Assign(v)
                        r.DeleteFieldByValue(
                            f,
                            fieldvalue)
            for s in nionly:
                if s in npcb:
                    npcb[s](instance, sri[s])
                else:
                    f = sec[s]
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
                    f = sec[s]
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
        fd.CloseCursor(rsc)
        self._opencontext.DestroyRecordSet(fd)

    def get_database(self):
        """Return the APIDatabaseContext."""
        return self._opencontext

    def make_cursor(self, fieldname, keyrange=None):
        """Return a CursorDPT cursor on DPT file for fieldname."""
        c = CursorDPT(
            self,
            self._secondary.get(
                fieldname,
                self._primary),
            keyrange)
        if c:
            self._cursors[c] = True
        return c

    def get_first_primary_key_for_index_key(self, dbfield, key):
        """Return the record number on DPT file given key for dbfield.

        This method should be used only on index fields whose keys each
        reference a single record. The intended use is where a key for a
        dbfield in dbname has been derived from a record in some other
        dbname.

        """
        fs = self.foundset_field_equals_value(
            self._secondary[dbfield], key)
        rsc = fs.OpenCursor()
        try:
            if rsc.Accessible():
                recno = rsc.LastAdvancedRecNum()
            else:
                recno = None
        finally:
            fs.CloseCursor(rsc)
            self._opencontext.DestroyRecordSet(fs)
        return recno
    
    def get_primary_record(self, key):
        """Return the instance given the record number in key."""
        if key is None:
            return None
        fs = self.foundset_record_number(key)
        rsc = fs.OpenCursor()
        try:
            if rsc.Accessible():
                r = (
                    key,
                    self.join_primary_field_occurrences(
                        rsc.AccessCurrentRecordForRead()))
            else:
                r = None
        finally:
            fs.CloseCursor(rsc)
            self._opencontext.DestroyRecordSet(fs)
        return r

    def join_primary_field_occurrences(self, record):
        """Return concatenated occurrences of field holding record value.

        A record has one visible field containing a pickled class instance.
        The value may be spread over several, or many, occurrences of this
        field.

        """
        advance = record.AdvanceToNextFVPair
        fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        primary = self._primary
        v = []
        while advance():
            if fieldocc() == primary:
                v.append(valueocc().ExtractString())
        return ''.join(v)

    def make_identity_getter(self, method):
        """Make the identity getter method for use by put_instance."""

        if callable(method):

            def identity():

                return method(self._ddname)

        else:
            
            def identity():

                msg = ' '.join(
                    ('Record identity assignment for', str(self._ddname),
                     'has not been set up'))
                raise DPTbaseError, msg
        
        return identity

    def put_instance(self, instance):
        """Store instance as a record on DPT file."""
        instance.set_packed_value_and_indexes()
        recordcopy = self._putrecordcopy
        pyAppend = dptapi.pyAppendStdString
        fieldvalue = self._fieldvalue
        srv = instance.srvalue
        f = self._primary
        for i in range(0, len(srv), 255):
            pyAppend(recordcopy, f, fieldvalue, srv[i:i+255])
        sri = instance.srindex
        sec = self._secondary
        pcb = instance._putcallbacks
        for s in sri:
            if s not in pcb:
                secs = sec[s]
                pyAppend = self._pyappend[secs]
                for v in sri[s]:
                    pyAppend(recordcopy, secs, fieldvalue, v)
        recnum = self._opencontext.StoreRecord(recordcopy)
        recordcopy.Clear()
        instance.key.load(recnum)
        instance.srkey = encode_record_number(recnum)
        if len(pcb):
            for s in sri:
                if s in pcb:
                    pcb[s](instance, sri[s])

    def foundset_all_records(self, fieldname):
        """Return APIFoundset containing all records on DPT file.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                fieldname,
                dptapi.FD_ALLRECS,
                dptapi.APIFieldValue('')))

    def foundset_field_equals_value(self, fieldname, value):
        """Return APIFoundset with records where fieldname contains value.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                fieldname,
                dptapi.FD_EQ,
                dptapi.APIFieldValue(value)))

    def foundset_record_number(self, recnum):
        """Return APIFoundset containing record whose record number is recnum.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_SINGLEREC,
                recnum))

    def foundset_records_before_record_number(self, recnum):
        """Return APIFoundset containing records before recnum in file.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_NOT_POINT,
                recnum))

    def foundset_records_not_before_record_number(self, recnum):
        """Return APIFoundset containing records at and after recnum in file.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_POINT,
                recnum))

    def foundset_recordset_before_record_number(self, recnum, recordset):
        """Return APIFoundset containing records before recnum in recordset.

        Provided for convenience of CursorDPT class.

        """
        return self._opencontext.FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_NOT_POINT,
                recnum),
            recordset)


class CursorDPT(Cursor):

    """Define cursor implemented using the Berkeley DB cursor methods.

    Methods added:

    __del__
    _get_record

    Methods overridden:

    __init__
    close
    count_records
    database_cursor_exists
    first
    get_position_of_record
    get_record_at_position
    last
    set_partial_key
    nearest
    next
    prev
    refresh_recordset
    setat

    Methods extended:

    None
    
    """

    def __init__(self, dptdb, fieldname, keyrange=None, recordset=None):
        """Create an APIRecordSetCursor or an APIDirectValueCursor.

        An APIRecordSetCursor is created if fieldname is an unordered field.
        An APIDirectValueCursor is created if fieldname is an ordered field.
        keyrange is ignored at present
        recordset is a found set or list used as a starting point instead of
        the default all records on file.

        """
        self._cursor = None
        self._partial = None

        self._cursor = _CursorDPT(
            dptdb, fieldname, keyrange=keyrange, recordset=recordset)

    def __del__(self):
        self.close()

    def close(self):
        if self._cursor is not None:
            try:
                del self._cursor._dptdb._cursors[self]
            except:
                pass
            self._cursor.close()
            self._cursor = None
        self._partial = None

    def count_records(self):
        """return record count or None if cursor is not usable"""
        cursor = self._cursor
        fieldname = cursor._fieldname
        dptdb = cursor._dptdb
        context = dptdb.get_database()
        if cursor._nonorderedfield:
            foundset = cursor.foundset_all_records()
            count = foundset.Count()
            context.DestroyRecordSet(foundset)
        else:
            dvcursor = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname))
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            if self._partial is not None:
                dvcursor.SetRestriction_Pattern(''.join((self._partial, '*')))
            games = context.CreateRecordList()
            dvcursor.GotoFirst()
            while dvcursor.Accessible():
                foundset = cursor.foundset_field_equals_value(
                    dvcursor.GetCurrentValue())
                games.Place(foundset)
                context.DestroyRecordSet(foundset)
                dvcursor.Advance(1)
            context.CloseDirectValueCursor(dvcursor)
            count = games.Count()
            context.DestroyRecordSet(games)
        return count

    def database_cursor_exists(self):
        """Return True if database cursor exists and False otherwise"""
        return bool(self._cursor)

    def first(self):
        """Return first record taking partial key into account."""
        if self._partial is None:
            return self._get_record(self._cursor.first())
        else:
            return self.nearest(self._partial)

    def get_position_of_record(self, key=None):
        """return position of record in file or 0 (zero)"""
        if key is None:
            return 0
        cursor = self._cursor
        fieldname = cursor._fieldname
        dptdb = cursor._dptdb
        context = dptdb.get_database()
        if cursor._nonorderedfield:
            foundset = cursor.foundset_records_before_record_number(key[0])
            count = foundset.Count()
            context.DestroyRecordSet(foundset)
            return count
        else:
            sk, rn = key
            dvcursor = context.OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(fieldname))
            dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            if self._partial is not None:
                dvcursor.SetRestriction_Pattern(''.join((self._partial, '*')))
            games = context.CreateRecordList()
            dvcursor.GotoFirst()
            while dvcursor.Accessible():
                cv = dvcursor.GetCurrentValue()
                foundset = cursor.foundset_field_equals_value(cv)
                if cv.ExtractString() >= sk:
                    if cv.ExtractString() == sk:
                        fs = cursor.foundset_recordset_before_record_number(
                            rn, foundset)
                        games.Place(fs)
                        context.DestroyRecordSet(fs)
                    context.DestroyRecordSet(foundset)
                    break
                games.Place(foundset)
                context.DestroyRecordSet(foundset)
                dvcursor.Advance(1)
            context.CloseDirectValueCursor(dvcursor)
            count = games.Count()
            context.DestroyRecordSet(games)
            return count

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None"""
        if position is None:
            return None
        backwardscan = bool(position < 0)
        cursor = self._cursor
        fieldname = cursor._fieldname
        dptdb = cursor._dptdb
        context = dptdb.get_database()
        if self._cursor._nonorderedfield:
            # it is simpler, and just as efficient, to do forward scans always
            fs = cursor.foundset_all_records()
            c = fs.Count()
            if backwardscan:
                position = c + position
            rsc = fs.OpenCursor()
            if position > c:
                if backwardscan:
                    rsc.GotoFirst()
                else:
                    rsc.GotoLast()
                if not rsc.Accessible():
                    fs.CloseCursor(rsc)
                    context.DestroyRecordSet(fs)
                    return None
                r = rsc.AccessCurrentRecordForRead()
                record = (
                    r.RecNum(),
                    cursor._join_primary_field_occs(r))
                fs.CloseCursor(rsc)
                context.DestroyRecordSet(fs)
                return record
            rsc.GotoLast()
            if not rsc.Accessible():
                fs.CloseCursor(rsc)
                context.DestroyRecordSet(fs)
                return None
            highrecnum = rsc.LastAdvancedRecNum()
            fs.CloseCursor(rsc)
            context.DestroyRecordSet(fs)
            fs = cursor.foundset_records_before_record_number(position)
            c = fs.Count()
            if c > position:
                rsc = fs.OpenCursor()
                rsc.GotoLast()
                if not rsc.Accessible():
                    fs.CloseCursor(rsc)
                    context.DestroyRecordSet(fs)
                    return None
                r = rsc.AccessCurrentRecordForRead()
                record = (
                    r.RecNum(),
                    cursor._join_primary_field_occs(r))
                fs.CloseCursor(rsc)
                context.DestroyRecordSet(fs)
                return record
            context.DestroyRecordSet(fs)
            fs = cursor.foundset_records_not_before_record_number(position)
            rsc = fs.OpenCursor()
            rsc.GotoFirst()
            while c < position:
                if not rsc.Accessible():
                    fs.CloseCursor(rsc)
                    context.DestroyRecordSet(fs)
                    return None
                rsc.Advance(1)
                c += 1
            r = rsc.AccessCurrentRecordForRead()
            record = (
                r.RecNum(),
                cursor._join_primary_field_occs(r))
            fs.CloseCursor(rsc)
            context.DestroyRecordSet(fs)
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
            if self._partial is not None:
                dvc.SetRestriction_Pattern(''.join((self._partial, '*')))
            count = 0
            record = None
            dvc.GotoFirst()
            while dvc.Accessible():
                cv = dvc.GetCurrentValue()
                fs = cursor.foundset_field_equals_value(cv)
                c = fs.Count()
                count += c
                if count > position:
                    rsc = fs.OpenCursor()
                    if backwardscan:
                        step = count - position - c - 1
                        rsc.GotoLast()
                    else:
                        step = position - count + c
                        rsc.GotoFirst()
                    if not rsc.Accessible():
                        fs.CloseCursor(rsc)
                        context.DestroyRecordSet(fs)
                        record = None
                        break
                    rsc.Advance(step)
                    if not rsc.Accessible():
                        fs.CloseCursor(rsc)
                        context.DestroyRecordSet(fs)
                        record = None
                        break
                    r = rsc.AccessCurrentRecordForRead()
                    record = (cv.ExtractString(), r.RecNum())
                    fs.CloseCursor(rsc)
                    context.DestroyRecordSet(fs)
                    break
                context.DestroyRecordSet(fs)
                dvc.Advance(1)
            context.CloseDirectValueCursor(dvc)
            return record

    def last(self):
        """Return last record taking partial key into account."""
        if self._partial is None:
            return self._get_record(self._cursor.last())
        else:
            k = list(self._partial)
            while ord(k[-1]) == 255:
                k.pop()
            if not len(k):
                return self._get_record(self._cursor.last())
            k[-1] = chr(ord(k[-1]) + 1)
            self._cursor._dptdb._fieldvalue.Assign(''.join(k))
            self._cursor._dvcursor.SetOptions(dptapi.CURSOR_POSFAIL_NEXT)
            self._cursor._dvcursor.SetPosition(self._cursor._dptdb._fieldvalue)
            self._cursor._dvcursor.SetOptions(dptapi.CURSOR_DEFOPTS)
            if self._cursor._dvcursor.Accessible():
                return self.prev()
            else:
                return self._get_record(self._cursor.last())

    def set_partial_key(self, partial):
        """Set partial key to constrain range of key values returned."""
        self._partial = partial

    def _get_record(self, record):
        """Return record matching key or partial key or None if no match."""
        if self._partial is not None:
            try:
                key, value = record
                if not key.startswith(self._partial):
                    return None
            except:
                return None
        return record

    def nearest(self, key):
        """Return nearest record taking partial key into account."""
        return self._get_record(self._cursor.set_range(key))

    def next(self):
        """Return next record taking partial key into account."""
        return self._get_record(self._cursor.next())

    def prev(self):
        """Return previous record taking partial key into account."""
        return self._get_record(self._cursor.prev())

    def refresh_recordset(self):
        """Refresh records for datagrid access after database update."""
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
        key, value = record
        if self._partial is not None:
            if not key.startswith(self._partial):
                return None
        if self._cursor._nonorderedfield:
            return self._get_record(self._cursor.set(key))
        else:
            return self._get_record(self._cursor.set_both(key, value))


class _CursorDPT(object):

    """An APIRecordSetCursor or one managed by an APIDirectValueCursor.

    A cursor implemented using either a DPT record set cursor for
    access in record number order or one of these managed by a DPT
    direct value cursor for access on an ordered index field.  This
    class and its methods support the api.dataclient.DataClient class and
    may not be appropriate in other contexts. In particular the first
    last next prev set set_range set_both methods return a (key, value)
    tuple imitating the Berkeley DB cursor methods.

    Methods added:

    __del__
    close
    first
    foundset_all_records
    foundset_field_equals_value
    foundset_record_number
    foundset_records_before_record_number
    foundset_records_not_before_record_number
    foundset_recordset_before_record_number
    last
    next
    prev
    refresh_recordset_keep_position
    set
    set_range
    set_both
    _first
    _first_by_value
    _join_primary_field_occs
    _last
    _last_by_value
    _new_value_context

    Methods overridden:

    __init__

    Methods extended:

    None
    
    """

    def __init__(self, dptdb, fieldname, keyrange=None, recordset=None):
        """Create an APIRecordSetCursor or an APIDirectValueCursor.

        An APIRecordSetCursor is created if fieldname is an unordered field.
        An APIDirectValueCursor is created if fieldname is an ordered field.
        keyrange is ignored at present
        recordset is a found set or list used as a starting point instead of
        the default all records on file.

        """
        # The introduction of DataClient.refresh_cursor method in gridsup
        # package may force _foundset to be implementaed as a list to avoid
        # time problems positioning cursor somewhere in a large foundset.
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None
        self._delete_foundset_on_close_cursor = True
        self._dptdb = None
        self._ddname = None
        self._fieldname = None
        self._nonorderedfield = None

        if not isinstance(dptdb, DPTbaseRecord):
            msg = ' '.join(['The database object must be a',
                            ''.join([DPTbaseRecord.__name__, ',']),
                            'or a subclass, instance.'])
            raise DPTbaseError, msg
        if fieldname not in dptdb._fields:
            msg = ' '.join(['The field', str(fieldname),
                            'is not defined in the',
                            str(dptdb._ddname), 'file of the',
                            dptdb.__class__.__name__,
                            'instance.'])
            raise DPTbaseError, msg
        if not isinstance(dptdb.get_database(),
                          dptapi.APIDatabaseFileContext):
            msg = ' '.join(['The opencontext attribute for the',
                            str(dptdb._ddname), 'file must be a',
                            ''.join([dptapi.APIDatabaseFileContext.__name__,
                                     ',']),
                            'or a subclass, instance.'])
            raise DPTbaseError, msg
        
        self._dptdb = dptdb
        self._ddname = dptdb._ddname
        self._fieldname = fieldname
        self._nonorderedfield = (fieldname == dptdb._primary)

        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant. Safe because
        # self._partial in CursorRS instances is None always.
        # self._foundset must be this instance's scratch set and a separate
        # permanent reference for recordset, if not None, kept for use by
        # foundset_all_records and similar methods.
        if self._nonorderedfield:
            #A record set cursor.
            if recordset:
                self._foundset = recordset
                self._delete_foundset_on_close_cursor = False
            else:
                self._foundset = self._dptdb.foundset_all_records(fieldname)
            self._rscursor = self._foundset.OpenCursor()
            return

        #A record set cursor managed by a direct value cursor.
        self._dvcursor = self._dptdb.get_database().OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(fieldname))
        self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
        self._first_by_value()

    def __del__(self):
        self.close()

    def close(self):
        """Close cursor"""
        if self._dvcursor:
            self._dptdb.get_database().CloseDirectValueCursor(self._dvcursor)
        if self._foundset:
            if self._rscursor:
                self._foundset.CloseCursor(self._rscursor)
            if self._delete_foundset_on_close_cursor:
                self._dptdb.get_database().DestroyRecordSet(self._foundset)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None
        self._dptdb = None

    def first(self):
        """Return (key, value) or None."""
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
        """Return (key, value) or None."""
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
        """Return (key, value) or None."""
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
            context = self._dptdb.get_database()
            while not self._rscursor.Accessible():
                self._dvcursor.Advance(1)
                if self._dvcursor.Accessible():
                    self._foundset.CloseCursor(self._rscursor)
                    context.DestroyRecordSet(self._foundset)
                    self._foundset = self._dptdb.foundset_field_equals_value(
                        self._fieldname,
                        self._dvcursor.GetCurrentValue())
                    self._rscursor = self._foundset.OpenCursor()
                    if self._rscursor.Accessible():
                        r = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self._dvcursor.GetCurrentValue().ExtractString(),
                            r.RecNum())
                else:
                    break
            #No more records for current position of direct value cursor 
            self._new_value_context()
            self._last_by_value()
        else:
            #No more records on record set cursor. 
            self._last()

    def prev(self):
        """Return (key, value) or None."""
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
            context = self._dptdb.get_database()
            while not self._rscursor.Accessible():
                self._dvcursor.Advance(-1)
                if self._dvcursor.Accessible():
                    self._foundset.CloseCursor(self._rscursor)
                    context.DestroyRecordSet(self._foundset)
                    self._foundset = self._dptdb.foundset_field_equals_value(
                        self._fieldname,
                        self._dvcursor.GetCurrentValue())
                    self._rscursor = self._foundset.OpenCursor()
                    self._rscursor.GotoLast()
                    if self._rscursor.Accessible():
                        r = self._rscursor.AccessCurrentRecordForRead()
                        return (
                            self._dvcursor.GetCurrentValue().ExtractString(),
                            r.RecNum())
                else:
                    break
            #No more records for current position of direct value cursor 
            self._new_value_context()
            self._first_by_value()
        else:
            #No more records on record set cursor. 
            self._first()

    def refresh_recordset_keep_position(self):
        """Refresh records for datagrid access after database update."""
        if self._foundset:
            key = self._rscursor.LastAdvancedRecNum()
            self._foundset.CloseCursor(self._rscursor)
            self._dptdb.get_database().DestroyRecordSet(self._foundset)
        else:
            key = -1 # (first + last) < key * 2
        if self._nonorderedfield:
            self._foundset = self._dptdb.foundset_all_records(self._fieldname)
        elif self._dvcursor is not None:
            self._foundset = self._dptdb.foundset_field_equals_value(
                self._fieldname,
                self._dvcursor.GetCurrentValue())
        else:
            self._dvcursor = self._dptdb.get_database().OpenDirectValueCursor(
                dptapi.APIFindValuesSpecification(self._fieldname))
            self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)
            self._first_by_value()
            if self._foundset is None:
                return
        self._rscursor = self._foundset.OpenCursor()
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
            self._foundset.CloseCursor(rsc)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoFirst()
        else:
            adv = 1
            while rsc.Accessible():
                if key >= rsc.LastAdvancedRecNum():
                    return
                rsc.Advance(adv)
            self._foundset.CloseCursor(rsc)
            self._rscursor = self._foundset.OpenCursor()
            self._rscursor.GotoLast()

    def set(self, key):
        """Position cursor at key. Then return (key, value) or None.

        Provided to support method api.dataclient.DataClient.setat which uses
        the Berkeley DB cursor method "set" to position on a RECNO database.
        The DPT equivalent is a record set so direct value cursors are not
        supported.

        """
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
        """Position cursor nearest key. Then return (key, value) or None.

        Provided to support method api.dataclient.DataClient.nearest which
        uses the Berkeley DB cursor method "set_range" to position on any
        database. "set_range" is identical to "set" except that on a BTREE
        database the cursor is positioned at the smallest key greater than
        or equal to the specified key.
        In DPT if a direct value cursor is available use it to position at
        the nearest key and position the record cursor at the first record.
        Where only a record cursor is available attempt to position exactly
        at the key.
        Range end is not supported in this method for compatibility.

        """
        if self._dvcursor is None:
            return self.set(key)

        dvc = self._dvcursor
        try:
            self._dptdb._fieldvalue.Assign(key)
            dvc.SetRestriction_LoLimit(self._dptdb._fieldvalue, True)
            dvc.GotoFirst()
        except AttributeError:
            if dvc is None:
                return None
            else:
                raise

        context = self._dptdb.get_database()
        while dvc.Accessible():
            self._foundset.CloseCursor(self._rscursor)
            context.DestroyRecordSet(self._foundset)
            self._foundset = self._dptdb.foundset_field_equals_value(
                self._fieldname,
                dvc.GetCurrentValue())
            self._rscursor = self._foundset.OpenCursor()
            if self._rscursor.Accessible():
                r = self._rscursor.AccessCurrentRecordForRead()
                return (
                    self._dvcursor.GetCurrentValue().ExtractString(),
                    r.RecNum())
            dvc.Advance(1)

        #Run off end available records. 
        self._new_value_context()
        self._last_by_value()

        return None

    def set_both(self, key, value):
        """Position cursor at (key, value). Then return (key, value) or None.

        Provided to support method api.dataclient.DataClient.setat which uses
        the Berkeley DB cursor method "set_both" to position on BTREE and
        HASH databases. The DPT equivalent is a direct value set so record
        cursors are not supported.

        """
        '''
        Need to take account of the direction cursor moves to get from
        current position to (key, value).  dvc component is fine but always
        stepping forward through rsc component is wrong.  set does it right.
        '''
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
                self._dptdb._fieldvalue.Assign(key)
                dvc.SetPosition(self._dptdb._fieldvalue)
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
                context = self._dptdb.get_database()
                self._foundset.CloseCursor(self._rscursor)
                context.DestroyRecordSet(self._foundset)
                self._foundset = self._dptdb.foundset_field_equals_value(
                    self._fieldname,
                    dvc.GetCurrentValue())
                self._rscursor = self._foundset.OpenCursor()
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

        #Set by key and value failed. 
        self._new_value_context()
        self._first_by_value()

        return None

    def foundset_all_records(self):
        """Return APIFoundset containing all records on DPT file."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_all_records(self._fieldname)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                self._fieldname,
                dptapi.FD_ALLRECS,
                dptapi.APIFieldValue('')),
            self._foundset)

    def foundset_field_equals_value(self, value):
        """Return APIFoundset with records where fieldname contains value."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_field_equals_value(
                self._fieldname, value)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                self._fieldname,
                dptapi.FD_EQ,
                dptapi.APIFieldValue(value)),
            self._foundset)

    def foundset_record_number(self, recnum):
        """Return APIFoundset containing record with record number recnum."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_record_number(recnum)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_SINGLEREC,
                recnum),
            self._foundset)

    def foundset_records_before_record_number(self, recnum):
        """Return APIFoundset containing records before recnum in file."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_records_before_record_number(recnum)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_NOT_POINT,
                recnum),
            self._foundset)

    def foundset_records_not_before_record_number(self, recnum):
        """Return APIFoundset containing records at and after recnum in file."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_records_not_before_record_number(recnum)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_POINT,
                recnum),
            self._foundset)

    def foundset_recordset_before_record_number(self, recnum, recordset):
        """Return APIFoundset containing records before recnum in recordset."""
        # self._foundset is over-used but currently safe and resolving this
        # makes _delete_foundset_on_close_cursor redundant
        if self._delete_foundset_on_close_cursor:
            return self._dptdb.foundset_recordset_before_record_number(
                recnum, recordset)
        return self._dptdb.get_database().FindRecords(
            dptapi.APIFindSpecification(
                dptapi.FD_NOT_POINT,
                recnum),
            recordset)

    def _first(self):
        """Position record set cursor at first available record.

        Provided to emulates behaviour of Berkeley DB cursor that attempts
        to move before first record.

        """
        self._foundset.CloseCursor(self._rscursor)
        rsc = self._foundset.OpenCursor()
        if rsc.Accessible():
            self._rscursor = rsc
            return
        self._foundset.CloseCursor(rsc)
        self._rscursor = None

    def _first_by_value(self):
        """Position direct value cursor at first available record.

        Provided to emulates behaviour of Berkeley DB cursor that attempts
        to move before first record.

        """
        context = self._dptdb.get_database()
        dvc = self._dvcursor
        dvc.GotoFirst()
        while dvc.Accessible():
            fs = self._dptdb.foundset_field_equals_value(
                self._fieldname,
                dvc.GetCurrentValue())
            rsc = fs.OpenCursor()
            if rsc.Accessible():
                self._rscursor = rsc
                self._foundset = fs
                return
            fs.CloseCursor(rsc)
            context.DestroyRecordSet(fs)
            dvc.Advance(1)
        context.CloseDirectValueCursor(dvc)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None

    def _join_primary_field_occs(self, record):
        """Concatenate occurrences of field holding primary value."""
        advance = record.AdvanceToNextFVPair
        fieldocc = record.LastAdvancedFieldName
        valueocc = record.LastAdvancedFieldValue
        primary = self._dptdb._primary
        v = []
        while advance():
            if fieldocc() == primary:
                v.append(valueocc().ExtractString())
        return ''.join(v)

    def _last(self):
        """Position record set cursor at last available record.

        Provided to emulates behaviour of Berkeley DB cursor that attempts
        to move after last record.

        """
        self._foundset.CloseCursor(self._rscursor)
        rsc = self._foundset.OpenCursor()
        if rsc.Accessible():
            rsc.GotoLast()
            self._rscursor = rsc
            return
        self._foundset.CloseCursor(rsc)
        self._rscursor = None

    def _last_by_value(self):
        """Position direct value cursor at last available record.

        Provided to emulates behaviour of Berkeley DB cursor that attempts
        to move after last record.

        """
        context = self._dptdb.get_database()
        dvc = self._dvcursor
        dvc.GotoLast()
        while dvc.Accessible():
            fs = self._dptdb.foundset_field_equals_value(
                self._fieldname,
                dvc.GetCurrentValue())
            rsc = fs.OpenCursor()
            if rsc.Accessible():
                rsc.GotoLast()
                self._rscursor = rsc
                self._foundset = fs
                return
            fs.CloseCursor(rsc)
            context.DestroyRecordSet(fs)
            dvc.Advance(-1)
        context.CloseDirectValueCursor(dvc)
        self._dvcursor = None
        self._rscursor = None
        self._foundset = None

    def _new_value_context(self):
        """Create new direct value cursor.

        Provided to replace one broken by running off end of direct value set.

        """
        context = self._dptdb.get_database()
        context.CloseDirectValueCursor(self._dvcursor)
        self._foundset.CloseCursor(self._rscursor)
        context.DestroyRecordSet(self._foundset)
        self._dvcursor = context.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(self._fieldname))
        self._dvcursor.SetDirection(dptapi.CURSOR_ASCENDING)

