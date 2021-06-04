# do_deferred_updates.py
# Copyright 2011, 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide the function do_deferred_updates to run a Python script.

do_deferred_updates() updates a database from a sequence of data files.

The script is expected to add records to the database in entry order but do
index updates in sorted chunks at conveniently large intervals.

"""

import os
import sys
import subprocess


class DoDeferredUpdatesError(Exception):
    """Exception for do_deferred_updates function."""


def do_deferred_updates(pyscript, databasepath, filepath):
    """Invoke a deferred update process and wait for it to finish.

    pyscript is the script to do the deferred update.
    databasepath is a directory containing a database file or files.
    filepath is a file or a sequence of files containing updates.

    """
    if sys.platform == "win32":
        args = ["pythonw"]
    else:
        args = [
            "".join(
                (
                    "python",
                    ".".join(
                        (str(sys.version_info[0]), str(sys.version_info[1]))
                    ),
                )
            )
        ]
    if not os.path.isfile(pyscript):
        raise DoDeferredUpdatesError(
            " ".join([repr(pyscript), "is not an existing file"])
        )
    args.append(pyscript)
    if not os.path.isdir(databasepath):
        raise DoDeferredUpdatesError(
            " ".join([repr(databasepath), "is not an existing directory"])
        )
    if isinstance(filepath, str):
        filepath = (filepath,)
    for path in filepath:
        if not os.path.isfile(path):
            raise DoDeferredUpdatesError(
                " ".join([repr(path), "is not an existing file"])
            )
    args.append(os.path.abspath(os.path.expanduser(databasepath)))
    args.extend(filepath)
    return subprocess.Popen(args)
