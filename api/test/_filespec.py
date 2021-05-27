# _filespec.py
# Copyright 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Sample file specification for use in tests"""

import shutil
import os

try:
    from bsddb3.db import (
        DB_BTREE, DB_HASH, DB_RECNO, DB_DUPSORT, DB_DUP,
        DB_CREATE, DB_INIT_MPOOL, DB_PRIVATE,
        DB_RECOVER, DB_INIT_LOCK, DB_INIT_LOG, DB_INIT_TXN,
        )
except ImportError:
    pass

from ..constants import (
    PRIMARY,
    SECONDARY,
    DDNAME,
    FILE,
    FIELDS,
    FILEDESC,
    INV,
    ORD,
    RRN, 
    BRECPPG,
    FILEORG,
    BTOD_FACTOR,
    BTOD_CONSTANT,
    DEFAULT_RECORDS,
    DEFAULT_INCREASE_FACTOR,
    DPT_PRIMARY_FIELD_LENGTH,
    )
from .. import filespec
from ..record import KeyData, Value, Record
from ..database import DatabaseError
from ...sqlite3api import Sqlite3api, Sqlite3apiError
try:
    from ...apswapi import Sqlite3api as Sqlite3apswapi
    from ...apswapi import Sqlite3apiError as Sqlite3apswapiError
except ImportError:
    pass
try:
    from ...dbapi import DBapi, DBapiError
except ImportError:
    pass
try:
    from ...dptapi import DPTapi, DPTapiError
except ImportError:
    pass
except DatabaseError as exc:
    if str(exc) != 'Platform is not "win32"':
        raise

GAMES_FILE_DEF = 'games'
GAME_FIELD_DEF = 'Game'
EVENT_FIELD_DEF = 'Event'
SITE_FIELD_DEF = 'Site'
DATE_FIELD_DEF = 'Date'
ROUND_FIELD_DEF = 'Round'
WHITE_FIELD_DEF = 'White'
BLACK_FIELD_DEF = 'Black'
RESULT_FIELD_DEF = 'Result'
SEVEN_TAG_ROSTER = (EVENT_FIELD_DEF,
                    SITE_FIELD_DEF,
                    DATE_FIELD_DEF,
                    ROUND_FIELD_DEF,
                    WHITE_FIELD_DEF,
                    BLACK_FIELD_DEF,
                    RESULT_FIELD_DEF,
                    )
NAME_FIELD_DEF = 'Name'

# Usually data[index] = function(data[GAME_FIELD_DEF]) where index is
# one of the *_FIELD_DEF items.
# For testing just set some arbitrary plausible values.
TESTDATA = (
    {GAME_FIELD_DEF:'gamedata1',
     EVENT_FIELD_DEF:'eventdata2',
     SITE_FIELD_DEF:'sitedata3',
     DATE_FIELD_DEF:'datedata1',
     ROUND_FIELD_DEF:'rounddata2',
     WHITE_FIELD_DEF:'whitedata3',
     BLACK_FIELD_DEF:'blackdata0',
     RESULT_FIELD_DEF:'resultdata0',
     },
    {GAME_FIELD_DEF:'gamedata2',
     EVENT_FIELD_DEF:'eventdata3',
     SITE_FIELD_DEF:'sitedata1',
     DATE_FIELD_DEF:'datedata3',
     ROUND_FIELD_DEF:'rounddata1',
     WHITE_FIELD_DEF:'whitedata2',
     BLACK_FIELD_DEF:'blackdata0',
     RESULT_FIELD_DEF:'resultdata0',
     },
    {GAME_FIELD_DEF:'gamedata3',
     EVENT_FIELD_DEF:'eventdata1',
     SITE_FIELD_DEF:'sitedata2',
     DATE_FIELD_DEF:'datedata1',
     ROUND_FIELD_DEF:'rounddata2',
     WHITE_FIELD_DEF:'whitedata3',
     BLACK_FIELD_DEF:'blackdata0',
     RESULT_FIELD_DEF:'resultdata0',
     },
    )
TESTINDEX = {
    GAME_FIELD_DEF:'game',
    BLACK_FIELD_DEF:'black',
    RESULT_FIELD_DEF:'result',
    }


def samplefilespec(brecppg=10,
                   fileorg=RRN,
                   btod_factor=8,
                   btod_constant=100,
                   default_records=100,
                   default_increase_factor=0.01,
                   dpt_primary_field_length=200,
                   ):
    """Return a FileSpec for a sample database.

    All the arguments are for DPT, and safish values would be chosen if not
    given in the file specification.  The arguments allow for configuration
    of files, especially their size, for testing.

    """
    class FileSpec(filespec.FileSpec):
        def __init__(self):
            dptdsn = FileSpec.dpt_dsn
            fn = FileSpec.field_name
            super().__init__(**{
                GAMES_FILE_DEF: {
                    DDNAME: 'GAMES',
                    FILE: dptdsn(GAMES_FILE_DEF),
                    FILEDESC: {
                        BRECPPG: brecppg,
                        FILEORG: fileorg,
                        },
                    BTOD_FACTOR: btod_factor,
                    BTOD_CONSTANT: btod_constant,
                    DEFAULT_RECORDS: default_records,
                    DEFAULT_INCREASE_FACTOR: default_increase_factor,
                    PRIMARY: fn(GAME_FIELD_DEF),
                    DPT_PRIMARY_FIELD_LENGTH: dpt_primary_field_length,
                    SECONDARY : {
                        EVENT_FIELD_DEF: None,
                        SITE_FIELD_DEF: None,
                        DATE_FIELD_DEF: None,
                        ROUND_FIELD_DEF: None,
                        WHITE_FIELD_DEF: None,
                        BLACK_FIELD_DEF: None,
                        RESULT_FIELD_DEF: None,
                        NAME_FIELD_DEF: None,
                        },
                    FIELDS: {
                        GAME_FIELD_DEF: None,
                        WHITE_FIELD_DEF: None,#{INV:True, ORD:True},
                        BLACK_FIELD_DEF: None,#{INV:True, ORD:True},
                        EVENT_FIELD_DEF: None,#{INV:True, ORD:True},
                        ROUND_FIELD_DEF: None,#{INV:True, ORD:True},
                        DATE_FIELD_DEF: None,#{INV:True, ORD:True},
                        RESULT_FIELD_DEF: None,#{INV:True, ORD:True},
                        SITE_FIELD_DEF: None,#{INV:True, ORD:True},
                        NAME_FIELD_DEF: None,#{INV:True, ORD:True},
                        },
                    },
                })
    return FileSpec()


def createsampledatabase(database):
    """Populate a sample database with records."""
    record = SampleNameRecord()
    for r in TESTDATA:
        database.start_transaction()
        record.load_record((None, repr(r)))
        record.put_record(database, GAMES_FILE_DEF)
        database.commit()
    rd = {k:''.join((v, str(4).zfill(4)))
          for k, v in TESTINDEX.items()}
    database.start_transaction()
    for r in range(10):
        record.load_record((None, repr(rd)))
        record.put_record(database, GAMES_FILE_DEF)
    database.commit()
        

class SampleValue(Value):

    def pack(self):
        v = super().pack()
        index = v[1]
        for field in SEVEN_TAG_ROSTER:
            if hasattr(self, field):
                index[field] = [getattr(self, field)]
        return v


class SampleRecord(Record):

    def __init__(self):
        super().__init__(KeyData, SampleValue)
        
    def get_keys(self, datasource=None, partial=None):
        dbname = datasource.dbname
        if dbname == GAMES_FILE_DEF:
            return [(self.key.recno, self.srvalue)]
        elif dbname in SEVEN_TAG_ROSTER:
            if hasattr(self.value, field):
                return [(getattr(self.value, dbname), self.key.pack())]
        return []
        

class SampleNameValue(SampleValue):

    def pack(self):
        v = super().pack()
        name = []
        for field in (WHITE_FIELD_DEF, BLACK_FIELD_DEF):
            if hasattr(self, field):
                name.append(getattr(self, field))
        if len(name):
            v[1][NAME_FIELD_DEF] = name
        return v


class SampleNameRecord(Record):

    def __init__(self):
        super().__init__(KeyData, SampleNameValue)
        
    def get_keys(self, datasource=None, partial=None):
        dbname = datasource.dbname
        if dbname == GAMES_FILE_DEF:
            return [(self.key.recno, self.srvalue)]
        elif dbname in SEVEN_TAG_ROSTER:
            if hasattr(self.value, field):
                return [(getattr(self.value, dbname), self.key.pack())]
        return []


class Sqlite3Database(Sqlite3api):

    def __init__(self, sqlite3file):
        names = samplefilespec()
        try:
            super().__init__(
                names,
                sqlite3file,
                )
        except Sqlite3apiError:
            if __name__ == '__main__':
                raise
            else:
                raise Sqlite3apiError('sqlite3 description invalid')

    def delete_database(self):
        names = {self.get_database_home()}
        basenames = set()
        listnames = []
        self.close_database()
        for n in names:
            if os.path.isdir(n):
                shutil.rmtree(n, ignore_errors=True)
            else:
                os.remove(n)
        try:
            d, f = os.path.split(self.get_database_home())
            if f == os.path.basename(d):
                os.rmdir(d)
        except:
            pass
        return True


try:


    class Sqlite3apswDatabase(Sqlite3apswapi):

        def __init__(self, sqlite3file):
            names = samplefilespec()
            try:
                super().__init__(
                    names,
                    sqlite3file,
                    )
            except Sqlite3apswapiError:
                if __name__ == '__main__':
                    raise
                else:
                    raise Sqlite3apiError('sqlite3 description invalid')

        def delete_database(self):
            names = {self.get_database_home()}
            basenames = set()
            listnames = []
            self.close_database()
            for n in names:
                if os.path.isdir(n):
                    shutil.rmtree(n, ignore_errors=True)
                else:
                    os.remove(n)
            try:
                d, f = os.path.split(self.get_database_home())
                if f == os.path.basename(d):
                    os.rmdir(d)
            except:
                pass
            return True


except NameError:
    # Assume apsw is not installed
    pass

try:


    class Bsddb3Database(DBapi):

        def __init__(self, DBfile):
            names = samplefilespec()
            try:
                super().__init__(
                    names,
                    DBfile,
                    DBenvironment={'flags': (DB_CREATE |
                                             DB_RECOVER |
                                             DB_INIT_MPOOL |
                                             DB_INIT_LOCK |
                                             DB_INIT_LOG |
                                             DB_INIT_TXN |
                                             DB_PRIVATE)},
                    )
            except DBapiError:
                if __name__ == '__main__':
                    raise
                else:
                    raise DBapiError('sqlite3 description invalid')

        def delete_database(self):
            df = self.get_database_folder()
            files = [os.path.join(df, f) for f in self.get_database_filenames()]
            folders = [df]
            self.close_database()
            for f in files:
                if os.path.isfile(f):
                    try:
                        os.remove(f)
                    except:
                        pass
            for f in folders:
                if os.path.isdir(f):
                    try:
                        os.rmdir(f)
                    except:
                        pass
            return True


except NameError:
    # Assume bsddb3 is not installed
    pass


try:


    class DPTDatabase(DPTapi):

        def __init__(self, DPTfile):
            names = samplefilespec()
            try:
                super().__init__(
                    names,
                    DPTfile,
                    )
            except DpTapiError:
                if __name__ == '__main__':
                    raise
                else:
                    raise DPTapiError('sqlite3 description invalid')

        def delete_database(self):
            names = set()
            basenames = set()
            names.add(self.get_dptsysfolder())
            basenames.add(os.path.basename(self.get_dptsysfolder()))
            for k, v in self.database_definition.items():
                names.add(v._file)
                basenames.add(os.path.basename(v._file))
            listnames = [n for n in os.listdir(self.get_database_folder())
                         if n not in basenames]
            self.close_database()
            for n in names:
                if os.path.isdir(n):
                    shutil.rmtree(n, ignore_errors=True)
                else:
                    os.remove(n)
            try:
                os.rmdir(self.get_database_folder())
            except:
                pass
            return True


except NameError:
    # Assume dptapi is not installed
    pass
