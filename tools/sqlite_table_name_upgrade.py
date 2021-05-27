# sqlite_table_name_upgrade.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Convert SQLite3 table names from basesup to solentware_base conventions.

SQLite3 tables names in basesup2 follow the File and Field names used in DPT.

The equivalent Berkeley DB primary database names follow the file names used
in DPT and the secondary database name are constructed by concatenating the
equivalents of File and Field names used in DPT.

In solentware_base3 the names are constrained to be unique across a database
because they have to co-exist in a single SQLite3 database.

Solentware_base2 is basesup2 renamed: no changes other than those forced by
renaming.

This module provides a class to convert SQLite3 table names to the style used
for Berkeley DB databases, removing a uniqueness constraint on the names of
DPT Fields and the equivalent Berkeley DB secondary databases and SQLite3
tables.

"""

try:
    import apsw as sqlite3_interface
except ModuleNotFoundError:
    try:
        import sqlite3 as sqlite3_interface
    except ModuleNotFoundError:
        sqlite3_interface = False
import tkinter, tkinter.messagebox, tkinter.filedialog, tkinter.ttk
import os

from ..api.constants import (SUBFILE_DELIMITER,
                             INDEXPREFIX,
                             SQLITE_SEGMENT_COLUMN,
                             )
from ..api import filespec


class SqliteTableNameUpgrade:

    def __init__(self, database_specification):
        self.root = None
        title = 'Upgrade SQLite3 Table Names to solentware_base 3.0 Format'
        root = tkinter.Tk()
        root.wm_title(title)
        if sqlite3_interface:
            results_folder = tkinter.filedialog.askdirectory(
                parent=root,
                title='Select Database Folder to Upgrade',
                initialdir='~')
            if results_folder:
                database_file = os.path.join(results_folder,
                                             os.path.basename(results_folder))
                if os.path.exists(database_file):
                    widget = tkinter.Text(master=root, wrap=tkinter.WORD)
                    scrollbar = tkinter.Scrollbar(
                        master=root,
                        orient=tkinter.VERTICAL,
                        command=widget.yview)
                    widget.configure(yscrollcommand=scrollbar.set)
                    scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
                    widget.pack(fill=tkinter.BOTH, expand=tkinter.TRUE)
                    frame = tkinter.Frame(master=root)
                    frame.pack()
                    tkinter.ttk.Button(
                        master=frame,
                        text='Upgrade',
                        command=self.upgrade).pack(side=tkinter.LEFT)
                    tkinter.ttk.Button(
                        master=frame,
                        text='Quit',
                        command=self.quit_).pack(side=tkinter.RIGHT)
                    self.conversions = []
                    self.database_specification = database_specification
                    self.database_file = database_file
                    self.widget = widget
                    self.fill_widget()
                    self.root = root
                else:
                    tkinter.messagebox.showinfo(
                        title=title,
                        message='No database in folder.')
                    root.destroy()
            else:
                tkinter.messagebox.showinfo(
                    title=title,
                    message='No database selected.')
                root.destroy()
        else:
            tkinter.messagebox.showinfo(
                title=title,
                message='Cannot find software to drive SQLite3 database.')
            root.destroy()


    def fill_widget(self):
        widget = self.widget
        database_specification = self.database_specification
        conversions = self.conversions
        widget.insert(
            tkinter.END,
            ''.join(('The items named in the first column will be renamed to ',
                     'the name in the second column.\n\nThese are table names ',
                     'and the associated index name which is the table name ',
                     "prefixed by 'ix'.  The first table in each group has ",
                     'no name in the second column and is not renamed.\n\n')))
        for s in database_specification:
            pname = database_specification[s][filespec.PRIMARY]
            widget.insert(tkinter.END, pname + '\n')
            for sname in database_specification[s][filespec.FIELDS]:
                if pname != sname:
                    nsname = SUBFILE_DELIMITER.join((pname, sname))
                    conversions.append((pname,
                                        sname,
                                        nsname,
                                        INDEXPREFIX + sname,
                                        INDEXPREFIX + nsname,
                                        ))
                    widget.insert(
                        tkinter.END,
                        '\t\t\t\t'.join((sname, nsname)) + '\n')
                    widget.insert(
                        tkinter.END,
                        '\t\t\t\t'.join(
                            (INDEXPREFIX + sname, INDEXPREFIX + nsname)) + '\n')
            widget.insert(tkinter.END, '\n')


    def quit_(self):
        if tkinter.messagebox.askyesno(
            title='Quit Upgrade',
            message='Yes to quit,\n\nNo to just dismiss this dialogue.'):
            self.root.destroy()

        
    def upgrade(self):
        nothing_to_do = True
        widget = self.widget
        conversions = self.conversions
        conn = sqlite3_interface.Connection(self.database_file)
        cursor = conn.cursor()
        cursor.execute("select name from sqlite_master where type='table'")
        names = [row[0] for row in cursor.fetchall()]
        cursor.execute('begin')
        try:
            for conv in self.conversions:
                widget.insert(tkinter.END, 'Converting ' + conv[1] + ' : ')
                widget.see(tkinter.END)
                if conv[2] in names:
                    widget.insert(tkinter.END, conv[2] + ' already exists.\n')
                    continue
                if conv[1] not in names:
                    widget.insert(tkinter.END, 'does not exist.\n')
                    continue
                cursor.execute(
                    'alter table ' + conv[1] + ' rename to ' + conv[2])
                cursor.execute('drop index if exists ' + conv[3])
                cursor.execute(' '.join(('create unique index if not exists',
                                         conv[4],
                                         'on',
                                         conv[2],
                                         '(',
                                         conv[1],
                                         ',',
                                         SQLITE_SEGMENT_COLUMN,
                                         ')')))
                nothing_to_do = False
                widget.insert(tkinter.END, 'done.\n')
            cursor.execute('commit')
            if nothing_to_do:
                widget.insert(tkinter.END, '\nNothing to do.\n')
            else:
                widget.insert(tkinter.END, '\nUpgrade completed.\n')
        except:
            cursor.execute('rollback')
            widget.insert(tkinter.END,
                          '\n\nUpgrade not done because an error occured.\n')
            if not nothing_to_do:
                widget.insert(tkinter.END,
                              'Those marked as done have been undone.\n')
        cursor.close()
        conn.close()
        widget.see(tkinter.END)


if __name__ == '__main__':


    app = SqliteTableNameUpgrade(filespec.FileSpec())
    if app.root:
        app.root.mainloop()
