# modulequery.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Module queries to support run-time choice of database module

List of functions:

database_modules_in_default_preference_order
existing_databases
installed_database_modules
supported_database_modules
_bsddb_preference

"""

import imp
import sys
import os.path

from basesup.api.constants import FILE, PRIMARY


def database_modules_in_default_preference_order():
    """Return tuple of database modules in preference order for use

    Callers are expected to use the first module in the returned tuple that is
    available according to the return value from installed_database_modules.

    The default assumes that the third party modules dpt and bsddb3 are to be
    used, preferring dpt, if available.  These modules have to be obtained and
    installed separately from Python and each other.
    
    The bsddb module is not includes in Python 3.  The sqlite3 module is
    included in Python 2.5 and later. Subject to these conditions sqlite3 is
    preferred to bsddb.  Often installing these two modules is optional.

    """
    if sys.platform == 'win32':
        return ('dptdb', 'bsddb3', 'sqlite3', 'bsddb')
    else:
        return ('bsddb3', 'sqlite3', 'bsddb')


def supported_database_modules():
    """Return dictionary of database modules supported

    For each module name in dictionary value is None if database module not
    supported by basesup, True if database module supported on Windows only,
    and False otherwise.

    """
    return dict(
        bsddb=False,
        bsddb3=False,
        sqlite3=False,
        dptdb=True,
        )


def installed_database_modules(bsddb_before_bsddb3=True):
    """Return dictionary of database modules supported and installed

    bsddb_before_bsddb3 determines which of bsddb and bsddb3 to set False if
    both are True.  So a database is attached to bsddb rather than bsddb3 by
    default if both are available.

    For each module name in dictionary value is None if database module not
    installed or supported and is the tuple returned by imp.find_module()
    otherwise.

    """
    dbm = supported_database_modules()
    windows = sys.platform == 'win32'
    for d in dbm:
        if dbm[d] is None:
            continue
        elif dbm[d] and not windows:
            dbm[d] = None
            continue
        try:
            dbm[d] = imp.find_module(d)
        except ImportError:
            dbm[d] = None
        except:
            raise
    _bsddb_preference(dbm, bsddb_before_bsddb3)
    return dbm


def existing_databases(folder, filespec, bsddb_before_bsddb3=True):
    """Return dictionary of filespec defined databases in folder for modules

    bsddb_before_bsddb3 determines which of bsddb and bsddb3 to set False if
    both are True.  So a database is attached to bsddb rather than bsddb3 by
    default if both are available.

    For each module name in dictionary value is None if database module not
    installed or supported, False if no part of the database defined in
    filespec exists, and True otherwise.

    """
    dbm = supported_database_modules()
    for d in dbm:
        if dbm[d] is None:
            continue
        if d == 'sqlite3':
            f, b = os.path.split(folder)
            dbm[d] = os.path.isfile(os.path.join(folder, b))
        elif d == 'dptdb':
            for f in filespec:
                if os.path.isfile(os.path.join(folder, filespec[f][FILE])):
                    dbm[d] = True
                    break
            else:
                dbm[d] = False
        else:
            for f in filespec:
                if os.path.isfile(
                    os.path.join(
                        folder,
                        filespec[f].get(PRIMARY, f))):
                    dbm[d] = True
                    break
            else:
                dbm[d] = False
    _bsddb_preference(dbm, bsddb_before_bsddb3)
    return dbm


def _bsddb_preference(mapping, bsddb_before_bsddb3):
    """Adjust mapping to honour bsddb_before_bsddb3 preference"""
    if mapping['bsddb'] and mapping['bsddb3']:
        mapping['bsddb'] = False
