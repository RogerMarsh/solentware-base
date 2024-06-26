# berkeleydb_du_splice_fix.py
# Copyright 2023 Roger Marsh
# Licence: See LICENSE.txt (BSD licence)

"""Delete index references kept, not replaced, in deferred update runs."""
import os
import sys

try:
    import berkeleydb as bdb
except ModuleNotFoundError:
    try:
        import bsddb3 as bdb
    except ModuleNotFoundError:
        bdb = None

from ..core import filespec
from ..core import constants

if bdb:
    _ENVIRONMENT = (
        bdb.db.DB_CREATE
        | bdb.db.DB_INIT_TXN
        | bdb.db.DB_INIT_MPOOL
        | bdb.db.DB_INIT_LOCK
        | bdb.db.DB_INIT_LOG
        | bdb.db.DB_PRIVATE
    )
else:
    _ENVIRONMENT = None


def berkeleydb_du_splice_fix(home=None, specification=None):
    """Delete index references which seem not deleted in deferred update.

    home is name of a directory or file containing instances of the
    database defined in specification.

    specification is the FileSpec instance for the database or a dict of
    primary database names naming their secondary databases.

    """
    if home is None:
        if len(sys.argv) > 1:
            home = sys.argv[-1]
        else:
            home = os.getcwd()
    try:
        if not os.path.exists(os.path.expanduser(home)):
            print(str(home), "does not exist")
            return
    except TypeError as exc:
        print("Invalid value in 'home' argument")
        print(str(exc))
        return
    path = os.path.expanduser(home)
    isdir = os.path.isdir(path)
    isfile = os.path.isfile(path)
    if isdir:
        print("_process_database_in_file_in_directory")
    if isfile:
        print("_process_database_in_file")
    if isinstance(specification, filespec.FileSpec):
        specification = {
            key: {constants.SECONDARY: value[constants.SECONDARY]}
            for key, value in specification.items()
        }
    else:
        for key, value in specification.items():
            if isinstance(value, dict):
                if constants.SECONDARY in value:
                    value = {constants.SECONDARY: value[constants.SECONDARY]}
                else:
                    value = {}
            else:
                value = {
                    constants.SECONDARY: {
                        secondary: None for secondary in value
                    }
                }
            specification[key] = value
    # The index name derivation is based on available database examples.
    for primary, definition in specification.items():
        # for secondary, field in definition[constants.SECONDARY].items():
        for secondary in definition[constants.SECONDARY]:
            if isdir:
                index = "_".join((primary, secondary))
                if not os.path.isfile(os.path.join(path, index)):
                    print(index, "file does not exist")
                else:
                    try:
                        _process_database_in_file_in_directory(path, index)
                    except bdb.db.DBNoSuchFileError:
                        print(
                            index,
                            " ".join(
                                (
                                    "database does not exist",
                                    "(Check naming convention)",
                                )
                            ),
                        )
            elif isfile:
                index = "_".join((primary, secondary))
                try:
                    _process_database_in_file(os.path.dirname(path), index)
                except bdb.db.DBNoSuchFileError:
                    print(
                        index,
                        "database does not exist (Check naming convention)",
                    )
            else:
                print(index, "is neither file nor directory")


def _process_database_in_file_in_directory(envdir, index):
    dbenv = bdb.db.DBEnv()
    # _set_log_file(dbenv, envdir)
    dbenv.open(envdir, _ENVIRONMENT)
    # dbtxn = dbenv.txn_begin()
    try:
        table = bdb.db.DB()  # dbenv)
        table.set_flags(bdb.db.DB_DUPSORT)
        table.open(
            os.path.join(envdir, index),
            dbname=index,
            dbtype=bdb.db.DB_BTREE,  # Hash is allowed but fix would not work.
        )
        try:
            _fix_database(table)
        finally:
            table.close()
    finally:
        # dbtxn.commit()
        dbenv.close()


def _process_database_in_file(envdir, index):
    dbenv = bdb.db.DBEnv()
    # _set_log_file(dbenv, envdir)
    dbenv.open(envdir, _ENVIRONMENT)
    # dbtxn = dbenv.txn_begin()
    try:
        table = bdb.db.DB()  # dbenv)
        table.set_flags(bdb.db.DB_DUPSORT)
        table.open(
            os.path.join(envdir, os.path.basename(envdir)),
            dbname=index,
            dbtype=bdb.db.DB_BTREE,  # Hash is allowed but fix would not work.
        )
        try:
            _fix_database(table)
        finally:
            table.close()
    finally:
        # dbtxn.commit()
        dbenv.close()


def _set_log_file(dbenv, envdir):
    logdir = os.path.join(envdir, "___logs")
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    dbenv.set_lg_dir(logdir)


def _fix_database(table, txn=None):
    cursor = table.cursor(txn=txn)
    try:
        fixed = 0
        total = 0
        unfixable = 0
        ok = 0
        others = 0
        deleted = 0
        prev_key = None
        prev_value = None
        delete = []
        while True:
            record = cursor.next()
            if not record:
                print(
                    total,
                    "records,",
                    others,
                    "others,",
                    unfixable,
                    "unfixable,",
                    ok,
                    "ok,",
                    fixed,
                    "fixed, in",
                    table.get_dbname()[-1],
                )
                break
            total += 1
            key, value = record
            if key != prev_key:
                prev_key = key
                prev_value = value
                ok += 1
                continue
            if value[:4] != prev_value[:4]:
                prev_key = key
                prev_value = value
                ok += 1
                continue
            if len(value) != 6 and len(prev_value) != 6:
                unfixable += 1
                continue
            if len(value) == 6 and len(prev_value) == 6:
                unfixable += 1
                continue
            if len(value) == 6:
                delete.append((key, value))
                fixed += 1
                continue
            if len(prev_value) == 6:
                delete.append((prev_key, prev_value))
                fixed += 1
                prev_key = key
                prev_value = value
                continue
            others += 1
            prev_key = key
            prev_value = value
        if unfixable + others == 0:
            for item in delete:
                record = cursor.get_both(*item)
                if record == item:
                    cursor.delete()
                    deleted += 1
            if fixed != 0:
                table.sync()  # txns do not work yet, despite cribbing.
                print(
                    deleted,
                    "deleted",
                    deleted == fixed,
                    "'deleted == fixed'",
                )
    finally:
        cursor.close()


if __name__ == "__main__":
    berkeleydb_du_splice_fix(
        os.path.join(
            "~",
            "sliderproblem",
            # "bsddb9698twoimports16",
            # "bsddb9698twoimports16",
            "oldversion",
        ),
        # This style works with an old version of ChessTab's FileSpec where
        # each database is in it's own file.  It is wrong for concurrent
        # version if tried with 'file_per_database=True' in arguments to
        # _db.Database().
        # Some of the fields, such as partialposition, force the dict
        # representation in the concurrent version of ChessTab:
        # specification=dict(
        #     games={
        #         "secondary": {
        #             "Black": games_Black,
        #             "partialposition": "games_partialposition",
        #         }
        # )
        # is a trimmed example expressing the idea.
        specification={
            "games": (
                "Black",
                "Date",
                "Event",
                "Result",
                "Round",
                "Site",
                "White",
                "PartialPosition",
                "PGNdate",
                "PieceMove",
                "PieceSquareMove",
                "Positions",
                "Source",
                "SquareMove",
            )
        },
    )
