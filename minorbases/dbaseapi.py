# dbaseapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide read access to dBaseIII files using the database interface defined
in the api.database.Database and api.cursor.Cursor classes.

<Reference to code copied to be inserted if ever found again>

Access is read only and provided to support existing data import processes.

"""

import os
import os.path
import io
import threading

from ..api.database import DatabaseError, Database
from ..api import cursor
from ..api.constants import PRIMARY, SECONDARY, FILE, FOLDER, FIELDS

# dBaseIII specific items are not yet worth putting in api.constants
# because the definition is provided to support data import only
START = 'start'
LENGTH = 'length'
TYPE = 'type'
DBASE_FIELDATTS = {
    START: int,
    LENGTH: int,
    TYPE: bytes,
    }
_VERSIONMAP = {'\x03':'dBase III'}
C, N, L, D, F = b'C', b'N', b'L', b'D', b'F'
_FIELDTYPE = {
    C: 'Character',
    N: 'Numeric',
    L: 'Boolean',
    D: 'Date',
    F: 'Float',
    }
_DELETED = 42 # compared with data[n] where type(data) is bytes
_EXISTS = 32 # as _DELETED
_PRESENT = {_DELETED:None, _EXISTS:None}


class dBaseapiError(DatabaseError):
    pass


class dBaseapi(Database):
    
    """Define a dBaseIII database structure.
    
    The database is read only.
    dBaseIII databases consist of one or more files each of which has zero
    or more fields defined. File names are unique and field names are
    unique within a file. Each file contains zero or more records where
    each record contains one occurrence of each field defined on the file.
    Records are identified by a record number that is unique within
    a file. The lowest possible record number is zero.
    Applications are expected to store instances of one class on a file.
    Each instance is a dictionary containing a subset of the fields
    defined for the file.
    
    """

    def __init__(self, dBasefiles, dBasefolder):
        """Define database structure.
        
        dBasefiles = {
            file:{
                folder:name,
                fields:{
                    name:{start:value, length:value, type:value}, ...
                    }
                }, ...
            }
        Field names and properties specified are constraints that must
        be true of the file

        dBasefolder = folder for files unless overridden in dBasefiles

        """
        # The database definition from dBasefiles after validation
        self.dBasefiles = None
        
        # The folder from dBasefolder after validation
        self.dBasefolder = None

        files = dict()
        pathnames = dict()
        sfi = 0

        if dBasefolder is not False:
            try:
                dBasefolder = os.path.abspath(dBasefolder)
            except:
                msg = ' '.join(['Main folder name', str(dBasefolder),
                                'is not valid'])
                raise dBaseapiError(msg)
        
        for dd in dBasefiles:
            try:
                folder = dBasefiles[dd].get(FOLDER, None)
            except:
                msg = ' '.join(['dBase file definition for', repr(dd),
                                'must be a dictionary'])
                raise dBaseapiError(msg)
            
            if folder == None:
                folder = dBasefolder
            if dBasefolder is not False:
                try:
                    folder = os.path.abspath(folder)
                    fname = os.path.join(folder,
                                         dBasefiles[dd].get(FILE, None))
                except:
                    msg = ' '.join(['File name for', dd, 'is invalid'])
                    raise dBaseapiError(msg)
            else:
                fname = dBasefiles[dd].get(FILE, None)
            
            if fname in pathnames:
                msg = ' '.join(['File name', str(fname),
                                'linked to', pathnames[fname],
                                'cannot link to', dd])
                raise dBaseapiError(msg)
            
            pathnames[fname] = dd
            files[dd] = self.make_root(
                dd,
                fname,
                dBasefiles[dd],
                sfi)
            sfi += 1

        self.dBasefiles = files
        self.dBasefolder = dBasefolder

    def close_context(self):
        """Close files."""
        for n in self.dBasefiles:
            self.dBasefiles[n].close()

    def exists(self, dbset, dbname):
        """Return True if dbname is one of the defined files.

        dbset is ignored.  It is present for compatibility with bsddb.

        """
        return dbname in self.dBasefiles

    def database_cursor(self, dbname, indexname, keyrange=None):
        """Create a cursor on indexname in dbname.
        
        keyrange is an addition for DPT. It may yet be removed.
        
        """
        return self.dBasefiles[dbname].make_cursor(
            indexname,
            keyrange)

    def get_database(self, dbset, dbname):
        """Return file for dbname.

        dbset is ignored.  It is present for compatibility with bsddb.

        """
        return self.dBasefiles[dbname]._dbaseobject

    def get_primary_record(self, dbname, key):
        """Return primary record (key, value) given primary key on dbname."""
        try:
            return self.get_database(None, dbname).setat(key)
        except:
            return None

    def is_primary(self, dbset, dbname):
        """Return True.

        dbset and dbname are ignored.  They are present for compatibility
        with bsddb.

        """
        return True

    def is_primary_recno(self, dbname):
        """Return True.

        dbname is ignored.  It is present for compatibility with bsddb.

        """
        return True

    def is_recno(self, dbset, dbname):
        """Return True.

        dbset and dbname are ignored.  They are present for compatibility
        with bsddb.

        """
        return True

    def open_context(self):
        """Open all files."""
        for n in self.dBasefiles:
            try:
                self.dBasefiles[n].open_root()
            except:
                for m in self.dBasefiles:
                    self.dBasefiles[n].close()
                raise

    def decode_as_primary_key(self, dbname, srkey):
        """Return srkey.

        dbname is ignored.  It is present for compatibility with bsddb.

        """
        return srkey

    def make_root(self, dd, fname, dptdesc, sfi):

        return dBaseapiRoot(dd, fname, dptdesc, sfi)


class dBaseIII:
    
    """Emulate Berkeley DB file and record structure for dBase III files.
    
    The first, last, nearest, next, prior, and Set methods return the
    pickled value for compatibility with the bsddb and DPT interfaces.
    This is despite the data already being available as a dictionary
    of values keyed by field name.
    
    """

    def __init__(self, filename):
        
        self._localdata = threading.local()
        self._lock_dBaseIII = threading.Lock()
        self._lock_dBaseIII.acquire()
        try:
            self.filename = filename
            self._set_closed_state()
        finally:
            self._lock_dBaseIII.release()

    def __del__(self):
        
        self.close()

    def close(self):

        self._lock_dBaseIII.acquire()
        try:
            try:
                try:
                    self._table_link.close()
                except:
                    pass
            finally:
                self._set_closed_state()
        finally:
            self._lock_dBaseIII.release()

    def encode_number(self, number):
        """Convert integer to base 256 string length 4 and return.

        Least significant digit at left as in dbaseIII record count.
        """
        s = []
        while number:
            number, r = divmod(number, 256)
            s.append(chr(r))
        ls = 4 - len(s)
        if ls > 0:
            s.extend([chr(0)] * ls)
        elif  ls < 0:
            return ''.join(s[:4])
        return ''.join(s)

    def make_cursor(self):
        """Create and return a record cursor on the dBaseIII file."""
        self._lock_dBaseIII.acquire()
        try:
            if self._table_link is None:
                return
            return CursordBaseIII(self)
        finally:
            self._lock_dBaseIII.release()

    def first(self):
        """Return first record not marked as deleted."""
        value = self._first_record()
        while value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))
            elif self._localdata.record_control not in _PRESENT:
                return None
            value = self._next_record()

    def last(self):
        """Return last record not marked as deleted."""
        value = self._last_record()
        while value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))
            elif self._localdata.record_control not in _PRESENT:
                return None
            value = self._prior_record()

    def nearest(self, current):
        """Return nearest record not marked as deleted."""
        self._set_record_number(current)
        value = self._get_record()
        while value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))
            elif self._localdata.record_control not in _PRESENT:
                return None
            value = self._next_record()

    def next(self, current):
        """Return next record not marked as deleted."""
        self._set_record_number(current)
        value = self._next_record()
        while value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))
            elif self._localdata.record_control not in _PRESENT:
                return None
            value = self._next_record()

    def open_dbf(self):
        
        self._lock_dBaseIII.acquire()
        try:
            try:
                # file header consists of 32 bytes
                if isinstance(self.filename, io.BytesIO):
                    self._table_link = self.filename
                else:
                    self._table_link = open(self.filename, 'rb')
                header = self._table_link.read(32)
                self.file_header.append(header)
                self.version = header[0]
                self.record_count = self._decode_number(header[4:8])
                self.first_record_seek = self._decode_number(header[8:10])
                self.record_length = self._decode_number(header[10:12])
                #field definitions are 32 bytes
                #field definition trailer is 1 byte \r
                fieldnames = []
                self.fields = {}
                fieldstart = 1
                fielddef = self._table_link.read(32)
                terminator = fielddef[0]
                while terminator != b'\r'[0]:
                    if len(fielddef) != 32:
                        self._table_link = self.close()
                        break
                    self.file_header.append(fielddef)
                    nullbyte = fielddef.find(b'\x00', 0)
                    if nullbyte == -1:
                        nullbyte = 11
                    elif nullbyte > 10:
                        nullbyte = 11
                    # callers work with fieldnames as class instance attributes
                    #fieldname = fielddef[:nullbyte]
                    fieldname = fielddef[:nullbyte].decode('iso-8859-1')
                    ftype = fielddef[11:12] # fielddef[11] gives int not bytes
                    fieldlength = fielddef[16]
                    if ftype in _FIELDTYPE:
                        fieldnames.append(fieldname)
                        self.fields[fieldname] = {}
                        self.fields[fieldname][LENGTH] = fieldlength
                        self.fields[fieldname][START] = fieldstart
                        self.fields[fieldname][TYPE] = ftype
                    fieldstart += fieldlength
                    fielddef = self._table_link.read(32)
                    terminator = fielddef[0]
                self._localdata.record_number = None
                self._localdata.record_select = None
                self._localdata.record_control = None
                self.fieldnames = tuple(fieldnames)
                fieldnames.sort()
                self.sortedfieldnames = tuple(fieldnames)
            except:
                self._table_link = None
        finally:
            self._lock_dBaseIII.release()

    def prior(self, current):
        """Return prior record not marked as deleted."""
        self._set_record_number(current)
        value = self._prior_record()
        while value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))
            elif self._localdata.record_control not in _PRESENT:
                return None
            value = self._prior_record()

    def setat(self, current):
        """Return current record.  Return None if deleted."""
        self._set_record_number(current)
        value = self._get_record()
        if value:
            if self._localdata.record_control == _EXISTS:
                return (self._localdata.record_select, repr(value))

    def _set_closed_state(self):
        
        self._table_link = None
        self.version = None
        self.record_count = None
        self.first_record_seek = None
        self.record_length = None
        self.fields = dict()
        self._localdata.record_number = None
        self._localdata.record_select = None
        self._localdata.record_control = None
        self._localdata.record_data = None # most recent _get_record() return
        self.file_header = [] # 1 header + n field definitions each 32 bytes
        self.fieldnames = None
        self.sortedfieldnames = None
        
    def _first_record(self):
        """Position at and return first record."""
        self._select_first()
        return self._get_record()

    def _get_record(self):
        """Return selected record.
        
        Copy record deleted/exists marker to self.record_control.
        
        """
        self._lock_dBaseIII.acquire()
        try:
            if self._table_link == None:
                return None
            if self._localdata.record_select < 0:
                self._localdata.record_select = -1
                return None
            elif self._localdata.record_select >= self.record_count:
                self._localdata.record_select = self.record_count
                return None
            self._localdata.record_number = self._localdata.record_select
            seek = (
                self.first_record_seek +
                self._localdata.record_number *
                self.record_length)
            tell = self._table_link.tell()
            if seek != tell:
                self._table_link.seek(seek - tell, 1)
            self._localdata.record_data = self._table_link.read(
                self.record_length)
            self._localdata.record_control = self._localdata.record_data[0]
            if self._localdata.record_control in _PRESENT:
                result = {}
                for fieldname in self.fieldnames:
                    s = self.fields[fieldname][START]
                    f = (self.fields[fieldname][START] +
                         self.fields[fieldname][LENGTH])
                    # Do not decode bytes because caller knows codec to use
                    result[fieldname] = self._localdata.record_data[s:f].strip()
                return result
            else:
                return None
        finally:
            self._lock_dBaseIII.release()

    def _last_record(self):
        """Position at and return last record."""
        self._select_last()
        return self._get_record()

    def _next_record(self):
        """Position at and return next record."""
        self._select_next()
        return self._get_record()

    def _prior_record(self):
        """Position at and return prior record."""
        self._select_prior()
        return self._get_record()

    def _select_first(self):
        """Set record selection cursor at first record."""
        self._localdata.record_select = 0
        return self._localdata.record_select

    def _select_last(self):
        """Set record selection cursor at last record."""
        self._localdata.record_select = self.record_count - 1
        return self._localdata.record_select

    def _select_next(self):
        """Set record selection cursor at next record."""
        self._localdata.record_select = self._localdata.record_number + 1
        return self._localdata.record_select

    def _select_prior(self):
        """Set record selection cursor at prior record."""
        self._localdata.record_select = self._localdata.record_number - 1
        return self._localdata.record_select

    def _set_record_number(self, number):
        """Set record selection cursor at the specified record."""
        if not isinstance(number, int):
            self._localdata.record_select = -1
        elif number > self.record_count:
            self._localdata.record_select = self.record_count
        elif number < 0:
            self._localdata.record_select = -1
        else:
            self._localdata.record_select = number

    def _decode_number(self, number):
        """Return base 256 string converted to integer.

        Least significant digit at left as in dbaseIII record count.

        """
        result = 0
        for i in range(len(number), 0, -1):
            result = 256 * result + number[i - 1]
        return result


class CursordBaseIII:
    
    """Define a dBase III file cursor.

    Wrap the dBaseIII methods in corresponding cursor method names.
    
    """

    def __init__(self, dbobject):
        super().__init__(dbobject)
        if isinstance(dbobject, dBaseIII):
            self._dbobject = dbobject
            self._current = -1
        else:
            self._dbobject = None
            self._current = None

    def __del__(self):
        
        self.close()

    def close(self):

        self._dbobject = None
        self._current = None

    def first(self):
        """Return first record not marked as deleted."""
        r = self._dbobject.first()
        if r:
            self._current = r[0]
            return r

    def is_cursor_open(self):
        """Return True if cursor available for use and False otherwise."""
        return self._dbobject is not None

    def last(self):
        """Return last record not marked as deleted."""
        r = self._dbobject.last()
        if r:
            self._current = r[0]
            return r

    def next(self):
        """Return next record not marked as deleted."""
        r = self._dbobject.next(self._current)
        if r:
            self._current = r[0]
            return r

    def prev(self):
        """Return prior record not marked as deleted."""
        r = self._dbobject.prior(self._current)
        if r:
            self._current = r[0]
            return r

    def setat(self, record):
        """Return current record.  Return None if deleted."""
        k, v = record
        r = self._dbobject.setat(k)
        if r:
            self._current = r[0]
            return r

    def cursor_count(self):
        """Return count of records on file."""
        return self._dbobject.record_count


class _dBaseapiRoot:
    
    """Provide file level access to a dBaseIII file.

    This class containing methods to open and close dBase files.
    Record level access is the responsibility of subclasses.
    
    """

    def __init__(self, dd, fname, dptdesc):
        """Define a dBaseIII file.
        
        dd = file description name
        fname = path to data file (.dbf) for dd
        dptdesc = field description for data file
        
        """
        self._ddname = dd
        self._fields = None
        self._file = fname
        self._primary = None
        self._secondary = None
        self._dbaseobject = None

        # Functions to convert numeric keys to string representation.
        # By default base 256 with the least significant digit at the right.
        # least_significant_digit = string_value[-1] (lsd = sv[-1])
        # most_significant_digit = string_value[0]
        # This conversion makes string sort equivalent to numeric sort.
        # These functions introduced to allow dbapi.py and dptapi.py to be
        # interchangeable for user classes. Another use found.

        fields = dptdesc.get(FIELDS, dict())
        if not isinstance(fields, dict):
            msg = ' '.join(['Field description of file', repr(dd),
                            'must be a dictionary'])
            raise dBaseapiError(msg)

        sequence = dict()
        for fieldname in fields:
            if not isinstance(fieldname, str):
                msg = ' '.join(['Field name must be string not',
                                repr(fieldname),
                                'in file', dd,])
                raise dBaseapiError(msg)
            
            if not fieldname.isupper():
                msg = ' '.join(['Field name', fieldname,
                                'in file', dd,
                                'must be upper case'])
                raise dBaseapiError(msg)

            attributes = fields[fieldname]
            if attributes == None:
                attributes = dict()
                fields[fieldname] = attributes
            if not isinstance(attributes, dict):
                msg = ' '.join(['Attributes for field', fieldname,
                                'in file', repr(dd),
                                'must be a dictionary or "None"'])
                raise dBaseapiError(msg)
            
            for a in attributes:
                if a not in DBASE_FIELDATTS:
                    msg = ' '.join(['Attribute', repr(a),
                                    'for field', fieldname,
                                    'in file', dd,
                                    'is not allowed'])
                    raise dBaseapiError(msg)
                
                if not isinstance(attributes[a], DBASE_FIELDATTS[a]):
                    msg = ' '.join([a,
                                    'for field', fieldname,
                                    'in file', dd,
                                    'is wrong type'])
                    raise dBaseapiError(msg)

                if a == TYPE:
                    if attributes[a] not in _FIELDTYPE:
                        msg = ' '.join(['Type for field', fieldname,
                                        'in file', dd,
                                        'must be one of',
                                        str(list(_FIELDTYPE.keys()))])
                        raise dBaseapiError(msg)

            if START in attributes:
                if attributes[START] in sequence:
                    msg = ' '.join(['Field', fieldname,
                                    'in file', dd,
                                    'starts at', str(attributes[START]),
                                    'duplicating field',
                                    sequence[attibutes[start]],
                                    'start'])
                    raise dBaseapiError(msg)

                sequence[attributes[START]] = fieldname

        sequence = sorted(list(sequence.items()))
        while len(sequence):
            s, f = sequence.pop()
            if len(sequence):
                sp, fp = sequence[-1]
                if LENGTH in fields[fp]:
                    if sp + fields[fp][LENGTH] > s:
                        msg = ' '.join(['Field', fp,
                                        'starting at', str(sp),
                                        'length', str(fields[fp][LENGTH]),
                                        'overlaps field', f,
                                        'starting at', str(s),
                                        'in file', dd])
                        raise dBaseapiError(msg)
                
        primary = dptdesc.get(PRIMARY, dict())
        if not isinstance(primary, dict):
            msg = ' '.join(['Field mapping of file', repr(dd),
                            'must be a dictionary'])
            raise dBaseapiError(msg)

        for p in primary:
            if not isinstance(p, str):
                msg = ' '.join(['Primary field name', str(p),
                                'for', dd,
                                'must be a string'])
                raise dBaseapiError(msg)

            f = primary[p]
            if f == None:
                f = p.upper()
                primary[p] = f
            elif not isinstance(f, str):
                msg = ' '.join(['Field', str(f),
                                'for primary field name', p,
                                'in file', dd,
                                'must be a string'])
                raise dBaseapiError(msg)

            if f not in fields:
                msg = ' '.join(['Field', f,
                                'for primary field name', p,
                                'in file', dd,
                                'must have a field description'])
                raise dBaseapiError(msg)

        secondary = dptdesc.get(SECONDARY, dict())
        if not isinstance(secondary, dict):
            msg = ' '.join(['Index definition of file', repr(dd),
                            'must be a dictionary'])
            raise dBaseapiError(msg)
        
        for s in secondary:
            if not isinstance(s, str):
                msg = ' '.join(['Index name', str(s),
                                'for', dd,
                                'must be a string'])
                raise dBaseapiError(msg)

            i = secondary[s]
            if i == None:
                i = (s.upper(),)
                secondary[s] = i
            elif not isinstance(i, tuple):
                msg = ' '.join(['Index definition', str(i),
                                'in field', s,
                                'in file', dd,
                                'must be a tuple of strings'])
                raise dBaseapiError(msg)

            for f in i:
                if not isinstance(f, str):
                    msg = ' '.join(['Field name', str(f),
                                    'in index definition for', s,
                                    'in file', dd,
                                    'must be a string'])
                    raise dBaseapiError(msg)

                if f not in fields:
                    msg = ' '.join(['Field', f,
                                    'for index definition', s,
                                    'in file', dd,
                                    'must have a field description'])
                    raise dBaseapiError(msg)

        self._fields = fields
        self._primary = primary
        self._secondary = secondary

    def close(self):
        """Close file."""
        try:
            self._dbaseobject.close()
        except:
            pass
        self._dbaseobject = None

    def is_field_primary(self, dbfield):
        """Return true if field is primary (not secondary test used)."""
        return dbfield not in self._secondary

    def open_root(self):
        """Open DBaseIII file."""
        if self._dbaseobject == True:
            opendb = dBaseIII(self._file)
            opendb.open_dbf()
            for f in self._fields:
                if f not in opendb.fields:
                    raise dBaseapiError(' '.join((
                        'Field', f, 'not in file', self._ddname)))
                else:
                    for a in self._fields[f]:
                        if self._fields[f][a] != opendb.fields[f][a]:
                            raise dBaseapiError(' '.join((
                                'Declared field attribute',
                                a,
                                'for field',
                                f,
                                'does not match value on file',
                                self._ddname)))
            for f in opendb.fields:
                if f not in self._fields:
                    self._primary[f] = f
                    self._fields[f] = dict()
                for a in opendb.fields[f]:
                    if a not in self._fields[f]:
                        self._fields[f][a] = opendb.fields[f][a]
            self._dbaseobject = opendb
        elif self._dbaseobject == False:
            raise dBaseapiError('Create dBase file not supported')
            
            
class dBaseapiRoot(_dBaseapiRoot):

    """Provide record level access to a dBaseIII file.
    
    """

    def __init__(self, dd, fname, dptdesc, sfi):
        """Define a dBaseIII file.
        
        See base class for argument descriptions.
        sfi - for compatibility with bsddb
        
        """
        super().__init__(dd, fname, dptdesc)
        
        # All active Cursor objects opened by database_cursor
        self._clientcursors = dict()

    def close(self):
        """Close file and cursors."""
        for c in self._clientcursors:
            c.close()
        self._clientcursors.clear()

        super().close()
        

    def get_database(self):
        """Return the open file."""
        return self._dbaseobject

    def make_cursor(self, indexname, keyrange=None):
        """Create a cursor on the dBaseIII file."""
        if indexname not in self._secondary:
            #c = self._dbaseobject.Cursor()
            c = Cursor(self._dbaseobject, keyrange=keyrange)
            if c:
                self._clientcursors[c] = True
            return c
        else:
            raise dBaseapiError('Indexes not supported')

    def _get_deferable_update_files(self, defer, dd):
        """Return a dictionary of empty lists for the dBaseIII files.

        Provided for compatibility with DPT

        """
        defer[dd] = {self._ddname : []} # _file rather than _ddname?
            
            
    def get_first_primary_key_for_index_key(self, dbfield, key):
        """Return None.  Deny existence of primary key.

        Provided for compatibility with DPT.

        """
        return None

    def get_primary_record(self, dbname, key):
        """Return None.  Deny existence of primary record.

        Provided for compatibility with DPT.

        """
        return None # return the pickled dictionary of field values


    def open_root(self):
        """Open dBaseIII file."""
        if isinstance(self._file, io.BytesIO):
            self._dbaseobject = True
        else:
            pathname = self._file
            foldername, filename = os.path.split(pathname)
            if os.path.exists(foldername):
                if not os.path.isdir(foldername):
                    msg = ' '.join([foldername, 'exists but is not a folder'])
                    raise dBaseapiError(msg)
                
            else:
                os.makedirs(foldername)
            if os.path.exists(pathname):
                if not os.path.isfile(pathname):
                    msg = ' '.join([pathname, 'exists but is not a file'])
                    raise dBaseapiError(msg)

                if self._dbaseobject == None:
                    self._dbaseobject = True
            elif self._dbaseobject == None:
                self._dbaseobject = False
            
        super().open_root()
            

class Cursor(CursordBaseIII, cursor.Cursor):
    
    """Define a dBaseIII cursor.
    
    Clearly not finished.  So notes left as found.  Changed just enough to
    get scrollbar working (Jan 2012).

    A cursor implemented using a CursordBaseIII cursor for access in
    record number order. Index access is not supported.
    This class and its methods support the api.dataclient.DataClient class
    and may not be appropriate in other contexts.
    Cursor is a subclass of CursordBaseIII at present. The methods
    of CursordBaseIII are named to support DataClient directly but
    set_partial_key is absent. May be better to follow dbapi.Cursor and
    dptbase.Cursor classes and make the CursordBaseIII instance an
    attibute of this Cursor class. dBaseIII.Cursor() supports this.
    
    """

    def __init__(self, dbasedb, keyrange=None, **kargs):
        """Cursor emulates parts of a bsddb3 cursor.

        dbasedb - A dBaseIII instance.
        keyrange - Not used.
        kargs - absorb argunents relevant to other database engines.

        """
        super().__init__(dbobject=dbasedb)

    def count_records(self):
        """return record count or None if cursor is not usable."""
        if not self.is_cursor_open():
            return None
        return self.cursor_count()

    def get_partial(self):
        """Return None.  Partial key not relevant.

        The _partial_key attribute is ignored.

        """
        return None

    def set_partial_key(self, partial):
        """Do nothing.  Partial key not relevant.

        The _partial_key attribute is ignored.

        """
        pass

    def get_position_of_record(self, record=None):
        """return position of record in file or 0 (zero)."""
        if record is None:
            return 0
        start = self.first
        step = self.next
        keycount = self.count_records()
        position = 0
        k = record[0]
        r = start()
        while r:
            if r[0] >= k:
                break
            position += 1
            r = step()
        return position

    def get_record_at_position(self, position=None):
        """return record for positionth record in file or None."""
        if position is None:
            return None
        if position < 0:
            start = self.last
            step = self.prev
            position = -1 - position
        else:
            start = self.first
            step = self.next
        keycount = self.count_records()
        count = 0
        r = start()
        while r:
            count += 1
            if count > position:
                break
            r = step()
        if r is not None:
            return r
