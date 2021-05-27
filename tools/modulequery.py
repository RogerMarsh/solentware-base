# modulequery.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Module queries to support run-time choice of database module

List of functions:

database_modules_in_default_preference_order
modules_for_existing_databases
installed_database_modules
supported_database_modules
_bsddb_preference

"""

import importlib
import sys
import os.path

from ..api.constants import (
    FILE,
    PRIMARY,
    BSDDB_MODULE,
    BSDDB3_MODULE,
    SQLITE3_MODULE,
    APSW_MODULE,
    DPT_MODULE,
    )


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
        return (
            DPT_MODULE,
            BSDDB3_MODULE,
            APSW_MODULE,
            SQLITE3_MODULE,
            BSDDB_MODULE,
            )
    else:
        return (BSDDB3_MODULE, APSW_MODULE, SQLITE3_MODULE, BSDDB_MODULE)


def supported_database_modules():
    """Return dictionary of database modules supported

    For each module name in dictionary value is None if database module not
    supported by basesup, True if database module supported on Windows only,
    and False otherwise.

    """
    return {
        BSDDB_MODULE: False,
        BSDDB3_MODULE: False,
        SQLITE3_MODULE: False,
        APSW_MODULE: False,
        DPT_MODULE: True,
        }


def installed_database_modules(
    bsddb_before_bsddb3=True, apsw_before_sqlite3=True):
    """Return set of preferred database modules supported and installed

    bsddb_before_bsddb3 determines which of bsddb and bsddb3 to set False if
    both are True.  So a database is attached to bsddb rather than bsddb3 by
    default if both are available.

    apsw_before_sqlite3 determines which of apsw and sqlite3 to set False if
    both are True.  So a database is attached to apsw rather than sqlite3 by
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
            if bool(importlib.import_module(d)):
                dbm[d] = True
            else:
                dbm[d] = None
        except ImportError:
            dbm[d] = None
        except:
            raise
    _bsddb_preference(dbm, bsddb_before_bsddb3)
    _sqlite_preference(dbm, apsw_before_sqlite3)
    return {d for d in dbm if dbm[d]}


def modules_for_existing_databases(folder, filespec):
    """Return [set(modulename, ...), ...] for filespec databases in folder.

    For each module in supported_database_modules() status is None if database
    module not installed or supported, False if no part of the database defined
    in filespec exists, and True otherwise.

    """
    dbm = supported_database_modules()
    for d in dbm:
        if dbm[d] is None:
            continue
        if d in (SQLITE3_MODULE, APSW_MODULE):
            f, b = os.path.split(folder)
            dbm[d] = os.path.isfile(os.path.join(folder, b))
        elif d == DPT_MODULE:
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
    cm = {
        (SQLITE3_MODULE, APSW_MODULE): set(),
        (DPT_MODULE,): set(),
        (BSDDB_MODULE, BSDDB3_MODULE): set(),
        }
    for m in [d for d in dbm if dbm[d]]:
        for c in cm:
            if m in c:
                cm[c].add(m)
    return [v for v in cm.values() if len(v)]


def _bsddb_preference(mapping, bsddb_before_bsddb3):
    """Adjust mapping to honour bsddb_before_bsddb3 preference"""
    if mapping[BSDDB_MODULE] and mapping[BSDDB3_MODULE]:
        mapping[BSDDB_MODULE] = False


def _sqlite_preference(mapping, apsw_before_sqlite3):
    """Adjust mapping to honour apsw_before_sqlite3 preference"""
    if mapping[APSW_MODULE] and mapping[SQLITE3_MODULE]:
        mapping[SQLITE3_MODULE] = False
