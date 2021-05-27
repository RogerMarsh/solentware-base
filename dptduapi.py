# dptduapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide DPT file access in single-step deferred update mode.

This module on Windows and Wine only.
On freebsd6 invoke python in deferred update subprocess but pythonw on
win32 to avoid getting a CMD window.

See www.dptoolkit.com for details of DPT

List of classes

DPTduapiError - Exceptions
DPTduapi - DPT database definition and single-step deferred update API
DPTduapiRecord - DPT record level access in single-step deferred update mode

"""

from .api.database import DatabaseError

import sys
_platform_win32 = sys.platform == 'win32'
del sys

if not _platform_win32:
    raise DatabaseError('Platform is not "win32"')

import os

from dptdb import dptapi

from .dptbase import DPTbase, DPTbaseRecord, DPTbaseError
from .api.constants import (
    FLT, INV, UAE, ORD, ONM, SPT,
    BSIZE, BRECPPG, BRESERVE, BREUSE,
    DSIZE, DRESERVE, DPGSRES,
    FILEORG,
    DPT_SYS_FOLDER, DPT_SYSDU_FOLDER,
    )


class DPTduapiError(DPTbaseError):
    pass


class DPTduapi(DPTbase):
    
    """Support single-step deferred updates on DPT database.

    Extend and override DPTbase methods for multi-step deferred update.
    __init__ is extended to provide control structures for deferred update.
    DPT does not support edit and delete operations in deferred update so
    edit_instance and delete_instance methods raise exceptions.
    This class is not intended for general processing so the make_cursor
    method raises an exception.  Subclasses should use the underlying DPT
    record and value cursors if recordset and value processing is needed.

    Typical use is:
    du = SubclassOfDPTduapi(...)
    du.open_context()
    du.StoreRecordLoop(...)   #example subclass method

    Methods added:

    None

    Methods overridden:

    create_default_parms - create parms.ini files
    delete_instance - raise exception
    do_deferred_updates - raise exception
    edit_instance - raise exception
    make_cursor - raise exception
    make_root - use DPTduapiRecord to open file
    open_context_allocated
    use_deferred_update_process - raise exception

    Methods extended:

    __init__ - use DPT_SYSDU_FOLDER for audit file
    
    """
    
    def __init__(self, DPTfiles, DPTfolder, **kargs):
        """Extend DPT database definition with deferred update.

        DPTfiles = {name:{ddname:name,
                          folder:name,
                          file:name,
                          filedesc:{property:value, ...},
                          fields:{name:{property:value, ...}, ...},
                          }, ...}
        DPTfolder = folder for files unless overridden in DPTfiles
        **kargs = DPT database system parameters

        """
        try:
            dptfolder = os.path.abspath(DPTfolder)
        except:
            msg = ' '.join(['Main folder name', str(DPTfolder),
                            'is not valid'])
            raise DPTduapiError(msg)
        
        #The database system parameters. DPT assumes reasonable defaults
        #for any values sought in self._dptkargs.
        #At Python26+ need to convert unicode to str for DPT
        dptsys = str(kargs.get(
            DPT_SYSDU_FOLDER,
            os.path.join(dptfolder, DPT_SYS_FOLDER, DPT_SYS_FOLDER)))
        username = str(kargs.get('username', 'dptapi'))

        super(DPTduapi, self).__init__(
            DPTfiles,
            DPTfolder,
            dptsys=dptsys,
            username=username,
            **kargs)

    def create_default_parms(self):
        """Create default parms.ini file."""
        if not os.path.exists(self.get_parms()):
            pf = open(self.get_parms(), 'w')
            try:
                pf.write("RCVOPT=X'00' " + os.linesep)
                pf.write("MAXBUF=100 " + os.linesep)
            finally:
                pf.close()
                
    def delete_instance(self, dbname, instance):
        raise DPTduapiError('delete_instance not implemented')

    def do_deferred_updates(self, pyscript, filepath):
        raise DPTduapiError('do_deferred_updates not implemented')

    def edit_instance(self, dbname, instance):
        raise DPTduapiError('edit_instance not implemented')

    def make_cursor(self, dbname):
        raise DPTduapiError('make_cursor not implemented')

    def use_deferred_update_process(self):
        raise DPTduapiError('Query use of du when in deferred update mode')

    def make_root(self, name, fname, dptfile, fieldnamefn, sfi):
        """DPT file interface customised for single-step deferred update"""
        return DPTduapiRecord(name, fname, dptfile, fieldnamefn, sfi)

    def open_context_allocated(self, files=()):
        """Open all files in single-step deferred update mode.

        Intended use is to open files to examine file status, or perhaps the
        equivalent of DPT command VIEW TABLES, when the database is closed as
        far as the application subclass of DPTbase is concerned.

        It is assumed that the Database Services object exists.

        """
        for dd in files:
            if dd in self.get_dptfiles():
                root = self.get_dptfiles()[dd]
                self.get_dbserv().Allocate(
                    root._ddname,
                    root._file,
                    dptapi.FILEDISP_OLD)
                cs = dptapi.APIContextSpecification(root._ddname)
                root._opencontext = self.get_dbserv().OpenContext_DUSingle(cs)

    def deferred_update_housekeeping(self):
        """Call Commit() if a non-TBO update is in progress.

        In non-TBO mode Commit() does not commit the tranasction, but it does
        release redundant resources which would not otherwise be released and
        may lead to an insuffient memory exception.

        """
        if self.get_dbserv():
            if self.get_dbserv().UpdateIsInProgress():
                self.get_dbserv().Commit()


class DPTduapiRecord(DPTbaseRecord):

    """Provide single-step deferred update sort processing for DPT file.

    This class disables methods not appropriate to deferred update.

    Methods added:

    None

    Methods overridden:

    delete_instance - not implemented in DPT for deferred updates.
    edit_instance - not implemented in DPT for deferred updates.
    make_cursor - not supported by this class.

    Methods extended:

    open_root - open DPT file in single-step mode.
    
    """

    def delete_instance(self, dbname, instance):
        raise DPTduapiError('delete_instance not implemented')

    def edit_instance(self, dbname, instance):
        raise DPTduapiError('edit_instance not implemented')

    def make_cursor(self, dbname):
        raise DPTduapiError('make_cursor not implemented')

    def open_root(self, db):
        """Extend to open file in single-step mode."""
        super(DPTduapiRecord, self).open_root(db)
        db.get_dbserv().Allocate(
            self._ddname,
            self._file,
            dptapi.FILEDISP_COND)
        cs = dptapi.APIContextSpecification(self._ddname)
        self._opencontext = db.get_dbserv().OpenContext_DUSingle(cs)
            
