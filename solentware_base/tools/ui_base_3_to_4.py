# ui_base_3_to_4.py
# Copyright 2019 Roger Marsh
# Licence: See LICENSE.txt (BSD licence)

"""Upgrade a database defined in a FileSpec instance."""

import tkinter
import tkinter.ttk
import tkinter.filedialog
import tkinter.messagebox
import os
import sys

# Use apsw if it is available.
try:
    import apsw as sqlite_dbe
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    try:
        import sqlite3 as sqlite_dbe
    except ImportError:
        sqlite_dbe = None

# Use berkeleydb if it is available.
try:
    from berkeleydb import db as bsddb_dbe
except ImportError:
    try:
        from bsddb3 import db as bsddb_dbe
    except ImportError:
        bsddb_dbe = None

from ..core.filespec import FileSpec
from ..core.segmentsize import SegmentSize
from .base_3_to_4_sqlite import Base_3_to_4_sqlite
from .base_3_to_4_db import Base_3_to_4_db
from .base_3_to_4 import Base_3_to_4Error

_START_TEXT = "Right-click for menu."
_HELP = "help.txt"

_deny_sqlite3 = bool(
    sys.version_info.major < 3
    or (sys.version_info.major == 3 and sys.version_info.minor < 6)
)
_PYTHON_VERSION = ".".join(
    (
        str(sys.version_info.major),
        str(sys.version_info.minor),
        str(sys.version_info.micro),
    )
)

del sys


class UIBase_3_to_4Error(Exception):
    """Exception class for UIBase_3_to_4."""


class UIBase_3_to_4:
    """Select database and convert table names to solentware_base 3 format."""

    def __init__(self, filespec):
        """Build the user interface."""
        root = tkinter.Tk()
        root.wm_title("Upgrade database")
        root.wm_resizable(width=tkinter.FALSE, height=tkinter.FALSE)
        frame = tkinter.ttk.Frame(master=root)
        frame.pack()
        entry = tkinter.ttk.Entry(master=frame)
        entry.pack(fill=tkinter.X)
        contents = tkinter.StringVar()
        entry["textvariable"] = contents
        panedwindow = tkinter.ttk.Panedwindow(
            master=frame, orient=tkinter.VERTICAL, width=600, height=300
        )
        panedwindow.pack(fill=tkinter.BOTH)
        frame = tkinter.ttk.Frame(master=panedwindow)
        panedwindow.add(frame, weight=1)
        text = tkinter.Text(master=frame, wrap=tkinter.WORD)
        scrollbar = tkinter.ttk.Scrollbar(
            master=frame, orient=tkinter.VERTICAL, command=text.yview
        )
        text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
        text.pack(fill=tkinter.BOTH)
        self.menu = tkinter.Menu(master=frame, tearoff=False)
        self.__menu = self.menu
        self.__xy = None
        self.menu_file_dbf = tkinter.Menu(master=frame, tearoff=False)
        self.menu_file_other = tkinter.Menu(master=frame, tearoff=False)
        self.menu_field = tkinter.Menu(master=frame, tearoff=False)
        self.entry = entry
        self.text = text
        self.set_menu_and_entry_events_for_open_url(True)
        entry.bind("<ButtonPress-3>", self.show_menu)
        text.bind("<ButtonPress-3>", self.show_menu)
        self.insert_text(_START_TEXT)
        entry.focus_set()
        self.root = root
        self.contents = contents
        self.__help = None
        self.filespec = filespec
        if not isinstance(filespec, FileSpec):
            msg = "The database specification is not valid."
            tkinter.messagebox.showerror(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text("Nothing can, or will, be done.")
            self.insert_text("\n")
            self.insert_text(
                "".join(
                    (
                        "A FileSpec is expected but a ",
                        self.filespec.__class__.__name__,
                        " is given.",
                    )
                )
            )
            self.set_menu_and_entry_events_for_open_url(False)
            return
        if bsddb_dbe is None and sqlite_dbe is None:
            msg = "Unable to process SQLite or Berkeley DB databases."
            tkinter.messagebox.showinfo(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text(
                "".join(
                    (
                        "This Python has none of apsw, bsddb3, and sqlite3, ",
                        "installed.  Perhaps another Python has some or all of ",
                        "these installed.",
                    )
                )
            )
            self.insert_text("\n")
            self.insert_text("Berkeley DB databases need bsddb3.")
            self.insert_text("\n")
            self.insert_text("SQLite databases need apsw or sqlite3.")
            self.insert_text("\n")
            self.insert_text("On Pythons earlier than version 3.6 use apsw.")
            self.insert_text("\n")
        elif bsddb_dbe is None:
            msg = "Unable to process Berkeley DB databases."
            tkinter.messagebox.showinfo(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text(
                "".join(
                    (
                        "This Python does not have bsddb3 installed.  Perhaps ",
                        "another Python has this installed.",
                    )
                )
            )
            self.insert_text("\n")
            self.insert_text("Berkeley DB databases need bsddb3.")
            self.insert_text("\n\n")
            self.insert_text("SQLite databases can be upgraded.")
        elif sqlite_dbe is None:
            msg = "Unable to process SQLite databases."
            tkinter.messagebox.showinfo(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text(
                "".join(
                    (
                        "This Python has none of apsw, and sqlite3, installed.  ",
                        "Perhaps another Python has some or all of these installed.",
                    )
                )
            )
            self.insert_text("\n")
            self.insert_text("SQLite databases need apsw or sqlite3.")
            self.insert_text("\n")
            self.insert_text("On Pythons earlier than version 3.6 use apsw.")
            self.insert_text("\n\n")
            self.insert_text("Berkeley DB databases can be upgraded.")
        else:
            self.insert_text("\n\n")
            self.insert_text(
                "SQLite and Berkeley DB databases can be upgraded."
            )
        self.insert_text("\n")
        self.insert_text("DPT databases do not need upgrading.")
        if sqlite_dbe is not None:
            self.insert_text("\n\n")
            self.insert_text(
                "".join(
                    (
                        "SQLite upgrades are very quick because table and column ",
                        "name changes is all that happens.",
                    )
                )
            )
        if bsddb_dbe is not None:
            self.insert_text("\n\n")
            self.insert_text(
                "".join(
                    (
                        "Berkeley DB upgrades will take a long time, depending on ",
                        "the size of the database, because all data is moved to a ",
                        "new location.",
                    )
                )
            )
        if sqlite_dbe is not None:
            if sqlite_dbe.__name__ == "sqlite3":
                if _deny_sqlite3:
                    msg = "".join(
                        (
                            "Unable to process SQLite databases on this ",
                            "version of Python.",
                        )
                    )
                    tkinter.messagebox.showinfo(
                        title="Upgrade Database",
                        message=msg,
                    )
                    self.insert_text("\n\n")
                    self.insert_text(msg)
                    self.insert_text("\n")
                    self.insert_text(
                        "".join(
                            (
                                "Cannot use sqlite3 at Python ",
                                _PYTHON_VERSION,
                                " because definition of 'transaction' is ",
                                "not compatible with database being upgraded.",
                            )
                        )
                    )
                    self.insert_text("\n")
                    self.insert_text(
                        "".join(
                            (
                                "Use apsw, or Python 3.6 or later where sqlite3 ",
                                "definition of 'transaction' is compatible.",
                            )
                        )
                    )

    def insert_text(self, text):
        """Wrap Text widget insert with Enable and Disable state configure."""
        self.text.insert(tkinter.END, text)

    def show_menu(self, event=None):
        """Show the popup menu for widget."""
        self.__menu.tk_popup(*event.widget.winfo_pointerxy())
        self.__xy = event.x, event.y
        self.__menu = self.menu

    def close_url(self, event=None):
        """Close the URL."""
        if not tkinter.messagebox.askokcancel(
            title="Close URL", message="Please confirm close URL"
        ):
            return
        self.text.delete("1.0", tkinter.END)
        self.text.insert(tkinter.END, _START_TEXT)
        self.set_menu_and_entry_events_for_open_url(True)

    def browse_localhost_file(self, event=None):
        """Select a directory on localhost."""
        localfilename = tkinter.filedialog.askdirectory(
            parent=self.text,
            title="Select directory containing database to upgrade",
            initialdir="~",
        )
        if localfilename:
            self.contents.set(localfilename)

    def upgrade_database(self, event=None):
        """Upgrade the database in selected directory."""
        if self.contents.get().strip() == "":
            tkinter.messagebox.showerror(
                title="Upgrade Database",
                message="Please choose a database directory to upgrade",
            )
            return
        try:
            b34db = Base_3_to_4_db(
                self.filespec, self.contents.get(), bsddb_dbe
            )
            b34sqlite = Base_3_to_4_sqlite(
                self.filespec, self.contents.get(), sqlite_dbe
            )
        except Base_3_to_4Error as exc:
            tkinter.messagebox.showerror(
                title="Upgrade Database",
                message=str(exc),
            )
            self.insert_text("\n\n")
            self.insert_text("Attempt to upgrade database fails")
            self.insert_text("\n")
            self.insert_text(exc)
            return
        actual_files = set(os.listdir(b34db.database))
        existing_files = b34db.v3files.intersection(actual_files)
        if len(existing_files):
            if os.path.exists(b34db.database_path_v4):
                msg = "".join(
                    (
                        "Database directory contains pre-upgrade and ",
                        "post-upgrade database files.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n\n")
                self.insert_text("The pre-upgrade files are:")
                for efname in sorted(existing_files):
                    self.insert_text("\n")
                    self.insert_text(efname)
                self.insert_text("\n\n")
                self.insert_text("The post-upgrade files are:")
                self.insert_text("\n")
                self.insert_text(b34db.database_path_v4)
                if b34db.database_path_v4 != b34sqlite.database_path:
                    if os.path.exists(b34sqlite.database_path):
                        self.insert_text("\n")
                        self.insert_text(b34db.database_path_v4)
                self.insert_text("\n\n")
                self.insert_text(
                    "".join(
                        (
                            "Either the pre-upgrade or the post-upgrade files ",
                            "must be moved out of the directory.",
                        )
                    )
                )
                return
            if os.path.exists(b34sqlite.database_path):
                msg = "".join(
                    (
                        "Database directory contains SQLite and pre-upgrade ",
                        "Berkeley DB database files.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n\n")
                self.insert_text("The pre-upgrade files are:")
                for efname in sorted(existing_files):
                    self.insert_text("\n")
                    self.insert_text(efname)
                self.insert_text("\n\n")
                self.insert_text("The SQLite file is:")
                self.insert_text("\n")
                self.insert_text(b34sqlite.database_path)
                self.insert_text("\n\n")
                self.insert_text(
                    "".join(
                        (
                            "Either the SQLite or the pre-upgrade files ",
                            "must be moved out of the directory.",
                        )
                    )
                )
                return
            try:
                missing_tables = b34db.get_missing_v3_tables()
            except AttributeError as exc:
                msg = str(exc)
                if msg == "'NoneType' object has no attribute 'DB'":
                    tkinter.messagebox.showerror(
                        title="Upgrade Database",
                        message=msg,
                    )
                    self.insert_text("\n\n")
                    self.insert_text("Attempt to upgrade database fails.")
                    self.insert_text("\n")
                    self.insert_text("The reported exception is: ")
                    self.insert_text(msg)
                    self.insert_text("\n")
                    self.insert_text(
                        "".join(
                            (
                                "bsddb3 is not available to ",
                                "open a Berkeley DB database.",
                            )
                        )
                    )
                    return
                raise
            if missing_tables:
                msg = "".join(
                    (
                        "Database directory is missing some expected ",
                        "pre-upgrade database files.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n\n")
                self.insert_text("The missing pre-upgrade files are:")
                for mtname in sorted(missing_tables):
                    self.insert_text("\n")
                    self.insert_text(mtname)
                return
            b34db.get_v3_segment_size()
            if b34db.segment_size is None:
                msg = "".join(
                    (
                        "Database not upgraded because segment size cannot be ",
                        "determined.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text("Probably the database is empty.")
                self.insert_text("\n")
                self.insert_text(b34db.database_path_v3)
                return
            if b34db.segment_size > SegmentSize.db_segment_size_bytes_maximum:
                msg = "".join(
                    (
                        "Database not upgraded because segment size is bigger ",
                        "than maximum allowed on upgraded database.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text(
                    "".join(
                        (
                            "Segment size is ",
                            str(b34db.segment_size),
                            " and mamimum allowed is ",
                            str(SegmentSize.db_segment_size_bytes_maximum),
                        )
                    )
                )
                return
            if b34db.segment_size < SegmentSize.db_segment_size_bytes_minimum:
                msg = "".join(
                    (
                        "Database not upgraded because segment size is smaller ",
                        "than minimum allowed on upgraded database.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text(
                    "".join(
                        (
                            "Segment size is ",
                            str(b34db.segment_size),
                            " and minimum allowed is ",
                            str(SegmentSize.db_segment_size_bytes_minimum),
                        )
                    )
                )
                return

            # Do Berkeley DB upgrade.
            b34db.convert_v3_to_v4()

            msg = "Database upgraded."
            tkinter.messagebox.showinfo(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text("Upgraded database is:")
            self.insert_text("\n")
            self.insert_text(b34db.database_path_v4)
            return
        if os.path.exists(b34sqlite.database_path):
            if not os.path.isfile(b34sqlite.database_path):
                msg = "".join((b34sqlite.database_path, " is not a file."))
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text("Attempt to upgrade database fails.")
                self.insert_text("\n")
                self.insert_text(msg)
                return
            try:
                missing_tables = b34sqlite.get_missing_v3_tables()
            except AttributeError as exc:
                msg = str(exc)
                if msg == "'NoneType' object has no attribute 'Connection'":
                    tkinter.messagebox.showerror(
                        title="Upgrade Database",
                        message=msg,
                    )
                    self.insert_text("\n\n")
                    self.insert_text("Attempt to upgrade database fails.")
                    self.insert_text("\n")
                    self.insert_text("The reported exception is: ")
                    self.insert_text(msg)
                    self.insert_text("\n")
                    self.insert_text(
                        "".join(
                            (
                                "Neither apsw nor sqlite3 is available to ",
                                "open a SQLite3 database.",
                            )
                        )
                    )
                    return
                raise
            except Exception as exc:
                msg = str(exc)
                if (
                    msg.startswith("file ")
                    or msg.startswith("NotADBError: file ")
                ) and msg.endswith(" is not a database"):
                    tkinter.messagebox.showerror(
                        title="Upgrade Database",
                        message=msg,
                    )
                    self.insert_text("\n\n")
                    self.insert_text("Attempt to upgrade database fails.")
                    self.insert_text("\n")
                    self.insert_text("The reported exception is: ")
                    self.insert_text(msg)
                    self.insert_text("\n")
                    self.insert_text(
                        "".join(
                            (
                                b34sqlite.database_path,
                                " may not be a SQLite database.",
                            )
                        )
                    )
                    return
                raise
            existing_tables = b34sqlite.get_existing_v4_tables()
            if missing_tables and existing_tables:
                msg = "".join(
                    (
                        "Some pre-upgrade tables are missing from, and some ",
                        "post-upgrade tables already exist in, the database ",
                        "file.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text("Has the database already been upgraded?")
                self.insert_text("\n\n")
                self.insert_text("The missing pre-upgrade tables are:")
                for mtname in sorted(missing_tables):
                    self.insert_text("\n")
                    self.insert_text(mtname)
                self.insert_text("\n\n")
                self.insert_text("The existing post-upgrade tables are:")
                for etname in sorted(existing_tables):
                    self.insert_text("\n")
                    self.insert_text(etname)
                return
            if missing_tables:
                msg = "".join(
                    (
                        "Some pre-upgrade tables are missing from the database ",
                        "file.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text("Is database one that should be upgraded?")
                self.insert_text("\n\n")
                self.insert_text("The missing pre-upgrade tables are:")
                for mtname in sorted(missing_tables):
                    self.insert_text("\n")
                    self.insert_text(mtname)
                return
            if existing_tables:
                msg = "".join(
                    (
                        "Some post-upgrade tables already exist in the database ",
                        "file, plus all the pre-upgrade tables which should ",
                        "exist.",
                    )
                )
                tkinter.messagebox.showerror(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n\n")
                self.insert_text("The existing post-upgrade tables are:")
                for etname in sorted(existing_tables):
                    self.insert_text("\n")
                    self.insert_text(etname)
                return
            b34sqlite.get_v3_segment_size()
            if b34sqlite.segment_size is None:
                msg = "".join(
                    (
                        "Database not upgraded because segment size cannot be ",
                        "determined.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text("Probably the database is empty.")
                self.insert_text("\n")
                self.insert_text(b34sqlite.database_path)
                return
            if (
                b34sqlite.segment_size
                > SegmentSize.db_segment_size_bytes_maximum
            ):
                msg = "".join(
                    (
                        "Database not upgraded because segment size is bigger ",
                        "than maximum allowed on upgraded database.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text(
                    "".join(
                        (
                            "Segment size is ",
                            str(b34sqlite.segment_size),
                            " and mamimum allowed is ",
                            str(SegmentSize.db_segment_size_bytes_maximum),
                        )
                    )
                )
                return
            if (
                b34sqlite.segment_size
                < SegmentSize.db_segment_size_bytes_minimum
            ):
                msg = "".join(
                    (
                        "Database not upgraded because segment size is smaller ",
                        "than minimum allowed on upgraded database.",
                    )
                )
                tkinter.messagebox.showinfo(
                    title="Upgrade Database",
                    message=msg,
                )
                self.insert_text("\n\n")
                self.insert_text(msg)
                self.insert_text("\n")
                self.insert_text(
                    "".join(
                        (
                            "Segment size is ",
                            str(b34sqlite.segment_size),
                            " and minimum allowed is ",
                            str(SegmentSize.db_segment_size_bytes_minimum),
                        )
                    )
                )
                return

            # Do SQLite upgrade.
            sql = b34sqlite.compose_sql_to_convert_v3_to_v4()
            b34sqlite.convert_v3_to_v4(sql)

            msg = "Database upgraded."
            tkinter.messagebox.showinfo(
                title="Upgrade Database",
                message=msg,
            )
            self.insert_text("\n\n")
            self.insert_text(msg)
            self.insert_text("\n")
            self.insert_text("Upgraded database is:")
            self.insert_text("\n")
            self.insert_text(b34sqlite.database_path)
            return
        msg = "Selected directory does not contain an upgradable database."
        tkinter.messagebox.showerror(
            title="Upgrade Database",
            message=msg,
        )
        self.insert_text("\n\n")
        self.insert_text("Attempt to upgrade database fails.")
        self.insert_text("\n")
        self.insert_text(msg)
        self.insert_text("\n")
        self.insert_text(self.contents.get())
        return

    def set_menu_and_entry_events_for_open_url(self, active):
        """Turn database upgrade events on if active is True otherwise off."""
        menu = self.menu
        if active:
            menu.add_command(
                label="Upgrade Database",
                command=self.upgrade_database,
                accelerator="Alt F4",
            )
            menu.add_command(
                label="Select Database Directory",
                command=self.browse_localhost_file,
                accelerator="Alt F5",
            )
            menu.add_separator()
            menu.add_command(
                label="Help", command=self.show_help, accelerator="F1"
            )
            menu.add_separator()
        else:
            menu.delete(0, tkinter.END)
        for entry in (self.text,):
            self._bind_for_scrolling_only(entry)
        for entry in self.entry, self.text:
            entry.bind("<KeyPress-F1>", "" if not active else self.show_help)
            entry.bind(
                "<Alt-KeyPress-F5>",
                "" if not active else self.browse_localhost_file,
            )
            entry.bind(
                "<Alt-KeyPress-F4>",
                "" if not active else self.upgrade_database,
            )
            entry.bind(
                "<KeyPress-Return>",
                "" if not active else self.upgrade_database,
            )

    def show_help(self, event=None):
        """Show database table name upgrade help."""
        if self.__help:
            return

        def clear_help(event=None):
            self.__help = None

        self.__help = tkinter.Toplevel(master=self.root)
        self.__help.wm_title("Help - Table Name Upgrade")
        self.__help.wm_resizable(width=tkinter.FALSE, height=tkinter.FALSE)
        self.__help.bind("<Destroy>", clear_help)
        text = tkinter.Text(master=self.__help, wrap=tkinter.WORD)
        scrollbar = tkinter.ttk.Scrollbar(
            master=self.__help, orient=tkinter.VERTICAL, command=text.yview
        )
        text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
        text.pack(fill=tkinter.BOTH)
        try:
            help_text = open(
                os.path.join(os.path.dirname(__file__), _HELP)
            ).read()
        except:
            help_text = "Unable to read help file"
        self._bind_for_scrolling_only(text)
        text.insert(tkinter.END, help_text)

    def _bind_for_scrolling_only(self, widget):
        widget.bind("<KeyPress>", "break")
        widget.bind("<Home>", "return")
        widget.bind("<Left>", "return")
        widget.bind("<Up>", "return")
        widget.bind("<Right>", "return")
        widget.bind("<Down>", "return")
        widget.bind("<Prior>", "return")
        widget.bind("<Next>", "return")
        widget.bind("<End>", "return")
