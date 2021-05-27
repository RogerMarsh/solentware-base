# modulequery.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Module queries to support run-time choice of database module.

The apsw, bsddb3, dptapi, and sqlite3, modules are available if installed.

apsw and sqlite3 are different interfaces to SQLite3.  apsw is preferred if
both are available because it provides the SQLite3 API rather than Python's
DB API.

Although dptapi is always available, databases built on it will require more
database administration skills than apsw, bsddb3, or sqlite3.

The dbm.gnu, dbm.ndbm, unqlite, and vedis, modules are not made available for
use unless requested in command line options when starting the application.
Attempting to use any of these as alternatives to the ones always available
may be pushing them beyond their intended uses, so they are imported only if
requested.

The command line options are:

'allow_all', '--a'
dbm.gnu, dbm.ndbm, unqlite, and vedis, are made available if installed.

'allow_gnu', '-g'
dbm.gnu is made available if installed.

'allow_ndbm', '-n'
dbm.ndbm is made available if installed.

'allow_unqlite', '-u'
unqlite is made available if installed.

'allow_vedis', '-v'
vedis is made available if installed.

"""
import sys
import os

_deny_sqlite3 = bool(
    sys.version_info.major < 3 or
    (sys.version_info.major == 3 and sys.version_info.minor < 6))


def _allow(option):
    argv1 = sys.argv[1:]
    if 'allow_all' in argv1 or '--a' in argv1:
        return True
    return option in argv1


if _allow('allow_unqlite') or _allow('-u'):
    try:
        import unqlite
    except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
        unqlite = None
else:
    unqlite = None
if _allow('allow_vedis') or _allow('-v'):
    try:
        import vedis
    except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
        vedis = None
else:
    vedis = None
if _deny_sqlite3:
    sqlite3 = None
else:
    try:
        import sqlite3
    except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
        sqlite3 = None
try:
    import apsw
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None
try:
    import bsddb
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb = None
try:
    import bsddb3
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None
try:
    from dptdb import dptapi
except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptapi = None
if _allow('allow_ndbm') or _allow('-n'):
    try:
        from dbm import ndbm
    except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
        ndbm = None
else:
    ndbm = None
if _allow('allow_gnu') or _allow('-g'):
    try:
        from dbm import gnu
    except ImportError: # Not ModuleNotFoundError for Pythons earlier than 3.6
        gnu = None
else:
    gnu = None

from .core.constants import (
    FILE,
    PRIMARY,
    BSDDB_MODULE,
    BSDDB3_MODULE,
    SQLITE3_MODULE,
    APSW_MODULE,
    DPT_MODULE,
    UNQLITE_MODULE,
    VEDIS_MODULE,
    GNU_MODULE,
    NDBM_MODULE,
    )

if _deny_sqlite3:
    if sys.platform == 'win32':
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            DPT_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            BSDDB_MODULE,
            )
    else:
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            BSDDB_MODULE,
            GNU_MODULE,
            NDBM_MODULE,
            )
else:
    if sys.platform == 'win32':
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            DPT_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            SQLITE3_MODULE,
            BSDDB_MODULE,
            )
    else:
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            SQLITE3_MODULE,
            BSDDB_MODULE,
            GNU_MODULE,
            NDBM_MODULE,
            )

del sys
del _allow
del _deny_sqlite3

def installed_database_modules():
    """Return dict of preferred database modules supported and installed.

    For each module name in dictionary value is None if database module not
    installed, or False if available but a sibling is used instead, or the
    module if available for use.

    """
    dbm = {d:None for d in DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER}
    for m in unqlite, vedis, sqlite3, apsw, bsddb, bsddb3, dptapi, ndbm, gnu,:
        if m:
            dbm[m.__name__] = m
    if dbm[BSDDB_MODULE] and dbm[BSDDB3_MODULE]:
        dbm[BSDDB_MODULE] = False
    if dbm[APSW_MODULE] and dbm[SQLITE3_MODULE]:
        dbm[SQLITE3_MODULE] = False
    return {d:m for d, m in dbm.items() if m}


def modules_for_existing_databases(folder, filespec):
    """Return [set(modulename, ...), ...] for filespec databases in folder.

    For each module in supported_database_modules() status is None if database
    module not installed or supported, False if no part of the database defined
    in filespec exists, and True otherwise.

    """
    if not os.path.exists(folder):
        return []
    if not os.listdir(folder):
        return []
    dbm = {d:None for d in DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER}
    for d, module in installed_database_modules().items():
        if d in (SQLITE3_MODULE, APSW_MODULE):
            sf = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(sf):
                c = module.Connection(sf)
                cur = c.cursor()
                try:

                    # Various websites quote this pragma as a practical
                    # way to determine if a file is a sqlite3 database.
                    cur.execute('pragma schema_version')

                    dbm[d] = module
                except:
                    dbm[d] = False
                finally:
                    cur.close()
                    c.close()
        elif d == DPT_MODULE:
            for f in filespec:
                if os.path.isfile(os.path.join(folder, filespec[f][FILE])):
                    dbm[d] = module
                    break
            else:
                dbm[d] = False
        elif d in (BSDDB_MODULE, BSDDB3_MODULE):
            sf = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(sf):
                for f in filespec:
                    try:
                        db = module.db.DB()
                        try:
                            db.open(sf, dbname=f, flags=module.db.DB_RDONLY)

                        # Catch cases where 'f' is not a database in 'sf'
                        except module.db.DBNoSuchFileError:
                            continue

                        finally:
                            db.close()
                        dbm[d] = module

                    # Catch cases where 'sf' is not a Berkeley DB database.
                    except module.db.DBInvalidArgError:
                        dbm[d] = False
                        break

            else:
                for f in filespec:
                    df = os.path.join(folder, f)
                    try:
                        db = module.db.DB()
                        try:
                            db.open(df, flags=module.db.DB_RDONLY)

                        # Catch cases where 'df' does not exist.
                        except module.db.DBNoSuchFileError:
                            continue

                        finally:
                            db.close()
                        dbm[d] = module

                    # Catch cases where df is not a Berkeley DB database.
                    except module.db.DBInvalidArgError:
                        dbm[d] = False
                        break

        elif d == VEDIS_MODULE:
            sf = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(sf):
                db = module.Vedis(sf)
                try:
                    db['something']
                    dbm[d] = module
                except OSError as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the db['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find('Malformed database image') < 0:
                        raise

                    dbm[d] = False
                except KeyError:
                    dbm[d] = module
                finally:
                    db.close()
        elif d == UNQLITE_MODULE:
            sf = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(sf):
                db = module.UnQLite(sf)
                try:
                    db['something']
                    dbm[d] = module
                except module.UnQLiteError as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the db['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find('Malformed database image') < 0:
                        raise

                    dbm[d] = False
                except KeyError:
                    dbm[d] = module
                finally:
                    db.close()
        elif d == GNU_MODULE:
            sf = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(sf):
                try:
                    db = module.open(sf)
                    try:
                        db['something']
                        dbm[d] = module
                    except module.error as exc:

                        # At 0.7.1 should not do exact match because repeating
                        # the db['key'] gives repeated error text.
                        # Or perhaps it should since a repeat is not expected!
                        if str(exc).find('Malformed database image') < 0:
                            raise

                        dbm[d] = False
                    except KeyError:
                        dbm[d] = module
                    finally:
                        db.close()
                except module.error as exc:
                    if str(exc).find('Bad magic number') < 0:
                        raise
                    dbm[d] = False
        elif d == NDBM_MODULE:
            sf = os.path.join(
                folder, '.'.join((os.path.split(folder)[1], 'db')))
            if os.path.isfile(sf):
                db = module.open(os.path.splitext(sf)[0])
                try:
                    db['something']
                    dbm[d] = module
                except module.error as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the db['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find('Malformed database image') < 0:
                        raise

                    dbm[d] = False
                except KeyError:
                    dbm[d] = module
                finally:
                    db.close()
        else:
            dbm[d] = False
    cm = {
        (SQLITE3_MODULE, APSW_MODULE): set(),
        (DPT_MODULE,): set(),
        (BSDDB_MODULE, BSDDB3_MODULE): set(),
        (UNQLITE_MODULE,): set(),
        (VEDIS_MODULE,): set(),
        (GNU_MODULE,): set(),
        (NDBM_MODULE,): set(),
        }
    for d, module in dbm.items():
        if module:
            for c in cm:
                if d in c:
                    cm[c].add(module)
    modules = [v for v in cm.values() if len(v)]
    if modules:
        return modules
    return None
