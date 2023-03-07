# db_tcl.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Access a Berkeley DB database with the Tcl API via tkinter.

Environment variable SWB_DB_TCL_LIB_PATH can be used to specify the
location of the Tcl packeage index if it is necessary to add this to
'auto_path'.  It defaults to '/usr/local/lib/db4' the OpenBSD location
where finding the package index matters most.
If this is needed environment variable LD_PRELOAD probably needs setting
as '/usr/local/lib/libtcl86.so.1.13' too (the libtcl name may vary in
version numbers used).

See ports@openbsd.org thread 'Python access to Berkeley DB' December 2022.

"""

import tkinter
import os

# The commands provided by the Db_tcl package in the tcl interpreter created
# below is used to meet external references the way done in apsw_database,
# sqlite3_database, berkeleydb_database, bsddb3_database, unqlite_database,
# and vedis_database.
tcl = tkinter.Tk(useTk=False)

TclError = tkinter.TclError
del tkinter
try:
    # For consistency throughout interface tcl.tk.call version is better.
    tcl.eval("package require Db_tcl")
    tcl_tk_call = tcl.tk.call
except TclError as exc:
    if str(exc) != "can't find package Db_tcl":
        raise TclError(str(exc)) from exc
    try:
        tcl.tk.call(
            (
                "lappend",
                "auto_path",
                os.environ.get("SWB_DB_TCL_LIB_PATH", "/usr/local/lib/db4"),
            )
        )
        # For consistency throughout interface tcl.tk.call version is better.
        tcl.eval("package require Db_tcl")
        tcl_tk_call = tcl.tk.call
    except TclError:
        tcl = None
        tcl_tk_call = None
del os
