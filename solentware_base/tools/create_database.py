# create_database.py
# Copyright 2020 Roger Marsh
# Licence: See LICENSE.txt (BSD licence)

"""Create empty database with chosen database engine and segment size."""

import tkinter
import tkinter.ttk
import tkinter.filedialog
import tkinter.messagebox
import os
import sys

_deny_sqlite3 = bool(
    sys.version_info.major < 3
    or (sys.version_info.major == 3 and sys.version_info.minor < 6)
)
_ttk_spinbox_available = bool(
    sys.version_info.major > 3
    or (sys.version_info.major == 3 and sys.version_info.minor > 6)
)

del sys

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
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
try:
    from dbm import ndbm
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    ndbm = None
try:
    from dbm import gnu
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    gnu = None


class CreateDatabaseError(Exception):
    """Exception class for CreateDatabase."""


class CreateDatabase:
    """Select database location, segment size, and location, then create it."""

    _START_TEXT = "".join(
        (
            "By default a new database would be created with the top-left ",
            "engine, and segment size 4000.",
        )
    )

    def __init__(self, title=None, engines=None):
        """Build the user interface."""
        root = tkinter.Tk()
        root.wm_title(title if title else "Create Database")
        root.wm_resizable(width=tkinter.FALSE, height=tkinter.FALSE)
        root.columnconfigure(1, uniform="a")
        root.columnconfigure(2, uniform="a")
        root.columnconfigure(3, uniform="a")
        tkinter.ttk.Label(master=root, text="Directory").grid(row=0, column=0)
        tkinter.ttk.Label(master=root, text="Segment Size (bytes)").grid(
            row=1, column=0
        )
        tkinter.ttk.Label(
            master=root,
            text="Between 500 and 8192, or 16 (intended for tests)",
        ).grid(row=1, column=2, columnspan=2)
        tkinter.ttk.Label(master=root, text="Database engines").grid(
            row=2, column=0, rowspan=3
        )
        tkinter.ttk.Label(master=root, text="Log").grid(
            row=6, column=1, pady=5
        )
        tkinter.ttk.Label(master=root, text=self._START_TEXT).grid(
            row=5, column=0, pady=5, columnspan=4
        )
        tkinter.ttk.Label(master=root, text="Right-click for menu").grid(
            row=6, column=2, pady=5, sticky="e"
        )
        entry = tkinter.ttk.Entry(master=root)
        entry.grid(row=0, column=1, columnspan=2, sticky="ew", pady=5)
        directory = tkinter.StringVar(root, "")
        entry["textvariable"] = directory
        values = [500, 512, 1000, 1024, 2000, 2048, 4000, 4096, 8000, 8192]
        if _ttk_spinbox_available:
            spinbox = tkinter.ttk.Spinbox(master=root, values=values)
        else:
            spinbox = tkinter.Spinbox(master=root, values=values)
        spinbox.grid(row=1, column=1, sticky="ew", pady=5)
        segmentsizebytes = tkinter.StringVar(root, "")
        spinbox["textvariable"] = segmentsizebytes
        database = tkinter.StringVar(root, "")
        self.engine_list = [
            eng
            for eng in (
                dptapi,
                berkeleydb,
                bsddb3,
                vedis,
                unqlite,
                apsw,
                sqlite3,
                gnu,
                ndbm,
            )
            if eng is not None
        ]
        if engines:
            self.engines = engines
        else:
            self.engines = {}
        for item, engine in enumerate(self.engine_list):
            if engine in self.engines:
                radiobutton = tkinter.ttk.Radiobutton(
                    master=root,
                    text=engine.__name__,
                    variable=database,
                    value=str(item),
                )
                row, column = divmod(item, 3)
                radiobutton.grid(row=row + 2, column=column + 1, pady=2)
        frame = tkinter.ttk.Frame(master=root)
        frame.grid(row=7, column=0, columnspan=4, sticky="ew")
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
        self.root = root
        self.text = text
        self.entry = entry
        self.directory = directory
        self.segmentsizebytes = segmentsizebytes
        self.database = database
        self.set_menu_and_entry_events_for_create_database(True)
        entry.bind("<ButtonPress-3>", self.show_menu)
        text.bind("<ButtonPress-3>", self.show_menu)
        if dptapi is not None:
            self.insert_text(
                "".join(
                    (
                        "\nThe dpt engine ignores segment size, ",
                        "and uses a value defined internally.",
                    )
                )
            )
        entry.focus_set()
        self.__help = None

    def insert_text(self, text):
        """Wrap Text widget insert with Enable and Disable state configure."""
        self.text.insert(tkinter.END, text)

    def report_action_or_error(self, msg, error=True):
        """Report outcome of action by adding msg to widget and by dialogue."""
        self.insert_text("\n\n")
        self.insert_text("".join(msg))
        if error:
            tkinter.messagebox.showerror(
                master=self.root, message="\n".join(msg)
            )
        else:
            tkinter.messagebox.showinfo(
                master=self.root, message="\n".join(msg)
            )

    def show_menu(self, event=None):
        """Show the popup menu for widget."""
        self.__menu.tk_popup(*event.widget.winfo_pointerxy())
        self.__xy = event.x, event.y
        self.__menu = self.menu

    def select_database_file(self, event=None):
        """Select a directory."""
        localfilename = tkinter.filedialog.askdirectory(
            parent=self.text,
            title="Select directory where database is to be created",
            initialdir="~",
        )
        if localfilename:
            self.directory.set(localfilename)

    def create_database(self):
        """Create database.

        This method assumes the application has named it's subclass of Database
        with the same name; if not this method must be overridden to provide
        the correct class.

        """
        engine = self.engine_list[int(self.database.get())]
        engine_database_class = self.engines[engine]
        ssb = int(self.segmentsizebytes.get())
        path = self.directory.get()
        if dptapi is engine:
            database = engine_database_class(path, allowcreate=True)
        else:
            database = engine_database_class(
                path, segment_size_bytes=ssb, allowcreate=True
            )
        database.open_database()
        database.close_database()

    def create_folder_and_database(self, event=None):
        """Create database after creating folder if it does not exist."""
        if self.directory.get() == "":
            tkinter.messagebox.showerror(
                master=self.root, message="Please select a directory"
            )
            return
        if self.database.get() == "":
            tkinter.messagebox.showerror(
                master=self.root, message="Please select a database engine"
            )
            return
        if dptapi is not self.engine_list[int(self.database.get())]:
            if self.segmentsizebytes.get() == "":
                tkinter.messagebox.showerror(
                    master=self.root, message="Please select a segment size"
                )
                return
            if not self.segmentsizebytes.get().isdigit():
                tkinter.messagebox.showerror(
                    master=self.root, message="Segment size must be an integer"
                )
                return
            ssb = int(self.segmentsizebytes.get())
            if ssb > 8192 or ssb < 500 and ssb != 16:
                msg = " ".join(
                    (
                        "Segment size must be between 500 and 8192,",
                        "or 16 (intended for testing).",
                    )
                )
                self.report_action_or_error((msg,))
                return
        path = self.directory.get()
        if path != os.path.abspath(path):
            path = os.path.expanduser(os.path.join("~", path))
        if not os.path.exists(os.path.dirname(path)):
            msg = (
                "Cannot create\n",
                self.directory.get(),
                "\nbecause\n",
                os.path.dirname(path),
                "\ndoes not exist.",
            )
            self.report_action_or_error(msg)
            return
        if not os.path.exists(path):
            try:
                os.mkdir(path)
            except:
                msg = (
                    "Cannot create directory\n",
                    os.path.basename(self.directory.get()),
                    "\nin\n",
                    os.path.dirname(self.directory.get()),
                )
                self.report_action_or_error(msg)
                return
        if not os.path.isdir(path):
            msg = (
                "Cannot create database in\n",
                self.directory.get(),
                "\nbecause it is not a directory.",
            )
            self.report_action_or_error(msg)
            return
        if os.listdir(path):
            msg = (
                "Cannot create database in\n",
                self.directory.get(),
                "\nbecause it is not empty.",
            )
            self.report_action_or_error(msg)
            return
        if not tkinter.messagebox.askokcancel(
            master=self.root,
            message="Please confirm request to create database.",
        ):
            return
        self.create_database()
        engine = self.engine_list[int(self.database.get())]
        if dptapi is engine:
            msg = (
                "Database created in directory\n",
                path,
                "".join(
                    (
                        "\nusing ",
                        engine.__name__,
                        ".",
                    )
                ),
            )
        else:
            msg = (
                "Database created in directory\n",
                path,
                "".join(
                    (
                        "\nusing ",
                        engine.__name__,
                        " with segment size (bytes) ",
                        str(ssb),
                        ".",
                    )
                ),
            )
        self.directory.set("")
        self.segmentsizebytes.set("")
        self.database.set("")
        self.report_action_or_error(msg, error=False)

    def set_menu_and_entry_events_for_create_database(self, active):
        """Turn events for opening a URL on if active is True otherwise off."""
        menu = self.menu
        if active:
            menu.add_separator()
            menu.add_command(
                label="Create Database",
                command=self.create_folder_and_database,
                accelerator="Alt F4",
            )
            menu.add_separator()
            menu.add_command(
                label="Select Database Directory",
                command=self.select_database_file,
                accelerator="Alt F5",
            )
            menu.add_separator()
        else:
            menu.delete(0, tkinter.END)
        for entry in (self.text,):
            self._bind_for_scrolling_only(entry)
        for entry in self.entry, self.text:
            entry.bind(
                "<Alt-KeyPress-F5>",
                "" if not active else self.select_database_file,
            )
            entry.bind(
                "<Alt-KeyPress-F4>",
                "" if not active else self.create_folder_and_database,
            )
            entry.bind(
                "<KeyPress-Return>",
                "" if not active else self.create_folder_and_database,
            )

    @staticmethod
    def _bind_for_scrolling_only(widget):
        widget.bind("<KeyPress>", "break")
        widget.bind("<Home>", "return")
        widget.bind("<Left>", "return")
        widget.bind("<Up>", "return")
        widget.bind("<Right>", "return")
        widget.bind("<Down>", "return")
        widget.bind("<Prior>", "return")
        widget.bind("<Next>", "return")
        widget.bind("<End>", "return")


if __name__ == "__main__":

    if unqlite:
        try:
            from .. import unqlite_database
        except ImportError:
            unqlite_database = None
    else:
        unqlite_database = None
    if vedis:
        try:
            from .. import vedis_database
        except ImportError:
            vedis_database = None
    else:
        vedis_database = None
    if _deny_sqlite3:
        sqlite3_database = None
    elif sqlite3:
        try:
            from .. import sqlite3_database
        except ImportError:
            sqlite3_database = None
    else:
        sqlite3_database = None
    if apsw:
        try:
            from .. import apsw_database
        except ImportError:
            apsw_database = None
    else:
        apsw_database = None
    if berkeleydb:
        try:
            from .. import berkeleydb_database
        except ImportError:
            berkeleydb_database = None
    else:
        berkeleydb_database = None
    if bsddb3:
        try:
            from .. import bsddb3_database
        except ImportError:
            bsddb3_database = None
    else:
        bsddb3_database = None
    if dptapi:
        try:
            from .. import dpt_database
        except ImportError:
            dpt_database = None
    else:
        dpt_database = None
    if ndbm:
        try:
            from .. import ndbm_database
        except ImportError:
            ndbm_database = None
    else:
        ndbm_database = None
    if gnu:
        try:
            from .. import gnu_database
        except ImportError:
            gnu_database = None
    else:
        gnu_database = None

    # Allow the create_database tool to be exercised without specifying a
    # useful database.
    class _CreateDatabase(CreateDatabase):
        def __init__(self):
            # Extend.
            engines = {}
            if dptapi:
                engines[dptapi] = dpt_database.Database
            if berkeleydb:
                engines[berkeleydb] = berkeleydb_database.Database
            if bsddb3:
                engines[bsddb3] = bsddb3_database.Database
            if vedis:
                engines[vedis] = vedis_database.Database
            if unqlite:
                engines[unqlite] = unqlite_database.Database
            if apsw:
                engines[apsw] = apsw_database.Database
            if sqlite3:
                engines[sqlite3] = sqlite3_database.Database
            if gnu:
                engines[gnu] = gnu_database.Database
            if ndbm:
                engines[ndbm] = ndbm_database.Database
            super().__init__(engines=engines)

        # The class structure of an application database is expected to be:
        # import solentware_base.<engine>
        # class Database(<engine>.Database):
        #     def __init__(folder, **k):
        #         spec = {...}
        #         super().__init__(spec, folder=folder, **k)
        #
        # This create_database() method emulates the structure by adding '{}'
        # into the engine_database_class() calls.
        def create_database(self):
            # Override.
            engine = self.engine_list[int(self.database.get())]
            engine_database_class = self.engines[engine]
            ssb = int(self.segmentsizebytes.get())
            path = self.directory.get()
            if dptapi is engine:
                database = engine_database_class({}, path, allowcreate=True)
            else:
                database = engine_database_class(
                    {}, path, segment_size_bytes=ssb, allowcreate=True
                )
            database.open_database()
            database.close_database()

    _CreateDatabase().root.mainloop()
