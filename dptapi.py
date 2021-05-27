# dptapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide DPT file access in non-deferred update mode.

This module on Windows and Wine only.
On freebsd6 invoke python in deferred update subprocess but pythonw on
win32 to avoid getting a CMD window.

See www.dptoolkit.com for details of DPT

List of classes

DPTapiError - Exceptions
DPTapi - DPT database definition and non-deferred update API
DPTapiRoot - DPT record level access in non-deferred update mode

"""

from .api.database import DatabaseError

import sys
_platform_win32 = sys.platform == 'win32'
del sys

if not _platform_win32:
    raise DatabaseError('Platform is not "win32"')

from dptdb import dptapi

from .dptbase import DPTbase, DPTbaseRecord, DPTbaseError
from .api.constants import FLT, INV, UAE, ORD, ONM, SPT
from .api.constants import BSIZE, BRECPPG, BRESERVE, BREUSE
from .api.constants import DSIZE, DRESERVE, DPGSRES
from .api.constants import FILEORG


class DPTapiError(DPTbaseError):
    pass


class DPTapi(DPTbase):
    
    """Provide access to a DPT database in non-deferred update mode.

    Updates can be backed out.
    Checkpointing is enabled so transactions are rolled back on recovery
    from errors.
    This is achieved by using the DPTapiRoot.open_root method to open files.

    Methods added:

    use_deferred_update_process - Get user confirmation for deferred update

    Methods overridden:

    set_defer_update - Prepare for deferred update
    unset_defer_update - Tidy up after deferred update
    make_root - use DPTapiRoot to open file

    Methods extended:

    None
    
    """

    def set_defer_update(self, db=None, duallowed=False):
        """Close files before doing deferred updates.

        This method is provided for compatibility with the interface for
        bsddb in dbapi.py.
        
        """
        self.close_context()
        return duallowed

    def unset_defer_update(self, db=None):
        """Reopen files after doing deferred updates.

        This method is provided for compatibility with the interface for
        bsddb in dbapi.py.
        
        """
        return self.open_context()

    def use_deferred_update_process(self, **kargs):
        """Return module name or None

        **kargs - soak up any arguments other database engines need.

        """
        raise DPTapiError('use_deferred_update_process not implemented')

    def make_root(self, name, fname, dptfile, fieldnamefn, sfi):

        return DPTapiRoot(name, fname, dptfile, fieldnamefn, sfi)


class DPTapiRoot(DPTbaseRecord):

    """Provide record level access to a DPT file in non-deferred update mode.

    This is achieved by extending the open_root method to open the file, or
    re-open the file after creating it.

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    open_root - open DPT file in non-deferred update mode.
    
    """

    def open_root(self, db):
        """Open file and return True if it is in non-deferred update mode.

        The superclass open_root method creates the file if it does not exist
        but leaves it closed.
        
        """
        super(DPTapiRoot, self).open_root(db)
        try:
            db.get_dbserv().Allocate(
                self._ddname,
                self._file,
                dptapi.FILEDISP_COND)
        except:
            pass
        cs = dptapi.APIContextSpecification(self._ddname)
        self._opencontext = db.get_dbserv().OpenContext(cs)
            
