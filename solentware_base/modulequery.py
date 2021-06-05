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
    sys.version_info.major < 3
    or (sys.version_info.major == 3 and sys.version_info.minor < 6)
)


def _allow(option):
    argv1 = sys.argv[1:]
    if "allow_all" in argv1 or "--a" in argv1:
        return True
    return option in argv1


# An import name is bound to None if 'import <name>' gives an exception
# or the import is not allowed.
if _allow("allow_unqlite") or _allow("-u"):
    try:
        import unqlite
    except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
        unqlite = None
else:
    unqlite = None
if _allow("allow_vedis") or _allow("-v"):
    try:
        import vedis
    except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
        vedis = None
else:
    vedis = None
if _deny_sqlite3:
    sqlite3 = None
else:
    try:
        import sqlite3
    except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
        sqlite3 = None
try:
    import apsw
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    apsw = None
try:
    import berkeleydb
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    berkeleydb = None
try:
    import bsddb3
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    bsddb3 = None
try:
    from dptdb import dptapi
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    dptapi = None
if _allow("allow_ndbm") or _allow("-n"):
    try:
        from dbm import ndbm
    except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
        ndbm = None
else:
    ndbm = None
if _allow("allow_gnu") or _allow("-g"):
    try:
        from dbm import gnu
    except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
        gnu = None
else:
    gnu = None

# Linters may say this import is in the wrong place.
# This appears to follow from the option to not allow some imports.
from .core.constants import (
    FILE,
    BERKELEYDB_MODULE,
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
    if sys.platform == "win32":
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            DPT_MODULE,
            BERKELEYDB_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
        )
    else:
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            BERKELEYDB_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            GNU_MODULE,
            NDBM_MODULE,
        )
else:
    if sys.platform == "win32":
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            DPT_MODULE,
            BERKELEYDB_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            SQLITE3_MODULE,
        )
    else:
        DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER = (
            BERKELEYDB_MODULE,
            BSDDB3_MODULE,
            VEDIS_MODULE,
            UNQLITE_MODULE,
            APSW_MODULE,
            SQLITE3_MODULE,
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
    dbm = {d: None for d in DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER}
    for module in (
        unqlite,
        vedis,
        sqlite3,
        apsw,
        berkeleydb,
        bsddb3,
        dptapi,
        ndbm,
        gnu,
    ):
        if module:
            dbm[module.__name__] = module
    if dbm[BERKELEYDB_MODULE] and dbm[BSDDB3_MODULE]:
        dbm[BSDDB3_MODULE] = False
    if dbm[APSW_MODULE] and dbm[SQLITE3_MODULE]:
        dbm[SQLITE3_MODULE] = False
    return {d: m for d, m in dbm.items() if m}


def modules_for_existing_databases(folder, filespec):
    """Return [set(modulename, ...), ...] for filespec databases in folder.

    For each module in supported_database_modules() status is None if
    database module not installed or supported, False if no part of the
    database defined in filespec exists, and True otherwise.

    """
    if not os.path.exists(folder):
        return []
    if not os.listdir(folder):
        return []
    dbm = {d: None for d in DATABASE_MODULES_IN_DEFAULT_PREFERENCE_ORDER}
    for name, module in installed_database_modules().items():
        if name in (SQLITE3_MODULE, APSW_MODULE):
            filepath = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(filepath):
                connection = module.Connection(filepath)
                cursor = connection.cursor()
                try:

                    # Various websites quote this pragma as a practical
                    # way to determine if a file is a sqlite3 database.
                    cursor.execute("pragma schema_version")

                    dbm[name] = module
                except:
                    dbm[name] = False
                finally:
                    cursor.close()
                    connection.close()
        elif name == DPT_MODULE:
            for filename in filespec:
                if os.path.isfile(
                    os.path.join(folder, filespec[filename][FILE])
                ):
                    dbm[name] = module
                    break
            else:
                dbm[name] = False
        elif name in (BERKELEYDB_MODULE, BSDDB3_MODULE):
            filepath = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(filepath):
                for filename in filespec:
                    try:
                        dbo = module.db.DB()
                        try:
                            dbo.open(
                                filepath,
                                dbname=filename,
                                flags=module.db.DB_RDONLY,
                            )

                        # Catch cases where 'filename' is not a database
                        # in 'filepath'
                        except module.db.DBNoSuchFileError:
                            continue

                        finally:
                            dbo.close()
                        dbm[name] = module

                    # Catch cases where 'filepath' is not a Berkeley DB
                    # database.
                    except module.db.DBInvalidArgError:
                        dbm[name] = False
                        break

            else:
                for filename in filespec:
                    filepath = os.path.join(folder, filename)
                    try:
                        dbo = module.db.DB()
                        try:
                            dbo.open(filepath, flags=module.db.DB_RDONLY)

                        # Catch cases where 'filepath' does not exist.
                        except module.db.DBNoSuchFileError:
                            continue

                        finally:
                            dbo.close()
                        dbm[name] = module

                    # Catch cases where filepath is not a Berkeley DB database.
                    except module.db.DBInvalidArgError:
                        dbm[name] = False
                        break

        elif name == VEDIS_MODULE:
            filepath = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(filepath):
                dbo = module.Vedis(filepath)
                try:
                    # KeyError or no exception if dbo is vedis database.
                    dbo["something"]
                    dbm[name] = module
                except OSError as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the dbo['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find("Malformed database image") < 0:
                        raise

                    dbm[name] = False
                except KeyError:
                    dbm[name] = module
                finally:
                    dbo.close()
        elif name == UNQLITE_MODULE:
            filepath = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(filepath):
                dbo = module.UnQLite(filepath)
                try:
                    # KeyError or no exception if dbo is unqlite database.
                    dbo["something"]
                    dbm[name] = module
                except module.UnQLiteError as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the dbo['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find("Malformed database image") < 0:
                        raise

                    dbm[name] = False
                except KeyError:
                    dbm[name] = module
                finally:
                    dbo.close()
        elif name == GNU_MODULE:
            filepath = os.path.join(folder, os.path.split(folder)[1])
            if os.path.isfile(filepath):
                try:
                    dbo = module.open(filepath)
                    try:
                        # KeyError or no exception if dbo is dbm.gnu database.
                        dbo["something"]
                        dbm[name] = module
                    except module.error as exc:

                        # At 0.7.1 should not do exact match because repeating
                        # the dbo['key'] gives repeated error text.
                        # Or perhaps it should since a repeat is not expected!
                        if str(exc).find("Malformed database image") < 0:
                            raise

                        dbm[name] = False
                    except KeyError:
                        dbm[name] = module
                    finally:
                        dbo.close()
                except module.error as exc:
                    if str(exc).find("Bad magic number") < 0:
                        raise
                    dbm[name] = False
        elif name == NDBM_MODULE:
            filepath = os.path.join(
                folder, ".".join((os.path.split(folder)[1], "db"))
            )
            if os.path.isfile(filepath):
                dbo = module.open(os.path.splitext(filepath)[0])
                try:
                    # KeyError or no exception if dbo is dbm.ndbm database.
                    dbo["something"]
                    dbm[name] = module
                except module.error as exc:

                    # At 0.7.1 should not do exact match because repeating
                    # the dbo['key'] gives repeated error text.
                    # Or perhaps it should since a repeat is not expected!
                    if str(exc).find("Malformed database image") < 0:
                        raise

                    dbm[name] = False
                except KeyError:
                    dbm[name] = module
                finally:
                    dbo.close()
        else:
            dbm[name] = False
    module_sets = {
        (SQLITE3_MODULE, APSW_MODULE): set(),
        (DPT_MODULE,): set(),
        (BERKELEYDB_MODULE, BSDDB3_MODULE): set(),
        (UNQLITE_MODULE,): set(),
        (VEDIS_MODULE,): set(),
        (GNU_MODULE,): set(),
        (NDBM_MODULE,): set(),
    }
    for name, module in dbm.items():
        if module:
            for module_names in module_sets:
                if name in module_names:
                    module_sets[module_names].add(module)
    modules = [v for v in module_sets.values() if len(v)]
    if modules:
        return modules
    return None
