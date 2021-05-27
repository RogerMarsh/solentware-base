# testdptfilesize.py
# Copyright 2010 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test dialogues for initial size, or increase in size, of DPT file.

List of classes:

None

List of functions:

None

"""

if __name__=='__main__':

    import os
    import tkinter
    import tkinter.messagebox

    import dptdb.dptapi

    from rmappsup.gui import panel, frame
    
    from basesup.api.constants import PRIMARY, SECONDARY, DEFER, BTOD_FACTOR
    from basesup.api.constants import DDNAME, FILE, FOLDER, FIELDS, FILEDESC
    from basesup.api.constants import FLT, INV, UAE, ORD, ONM, SPT, EO, RRN
    from basesup.api.constants import BRESERVE, BREUSE
    from basesup.api.constants import DRESERVE, DPGSRES, FILEORG
    from basesup.api.constants import BRECPPG, DEFAULT_RECORDS
    import basesup.api.filespec
    from basesup.dptapi import DPTapi
    from basesup.gui.dptfilesize import get_sizes_for_new_files
    import basesup.tools.dialogues

    _games_brecppg = 10
    _partial_brecppg = 40
    _default_number_games = 1500000
    _default_number_partial = 8000
    _b_to_d_factor_games = 6
    _b_to_d_factor_partial = 1 # a guess


    class FileSpec(rmappsup.api.filespec.FileSpec):

        def __init__(self, **kargs):
            
            super(FileSpec, self).__init__(
                default_=kargs,
                games={
                    DDNAME: 'GAMES',
                    FILE: 'games.dpt',
                    FILEDESC: {
                        BRECPPG: _games_brecppg,
                        FILEORG: RRN,
                        },
                    BTOD_FACTOR: _b_to_d_factor_games,
                    DEFAULT_RECORDS: _default_number_games,
                    PRIMARY: 'Game',
                    SECONDARY: {
                        'source': None,
                        'Event': None,
                        'Site': None,
                        'Date': None,
                        'Round': None,
                        'White': None,
                        'Black': None,
                        'Result': None,
                        'positions': None,
                        'piecesquaremove': 'PieceSquareMove',
                        },
                    FIELDS: {
                        'Game': None,
                        'Source': {INV:True, ORD:True},
                        'White': {INV:True, ORD:True},
                        'Black': {INV:True, ORD:True},
                        'Event': {INV:True, ORD:True},
                        'Round': {INV:True, ORD:True},
                        'Date': {INV:True, ORD:True},
                        'Result': {INV:True, ORD:True},
                        'Site': {INV:True, ORD:True},
                        'Positions': {INV:True, ORD:True},
                        'PieceSquareMove': {INV:True, ORD:True},
                        },
                    DEFER: {
                        },
                    },
                partial={
                    DDNAME: 'PARTIAL',
                    FILE: 'partial.dpt',
                    FILEDESC: {
                        BRECPPG: _partial_brecppg,
                        FILEORG: RRN,
                        },
                    BTOD_FACTOR: _b_to_d_factor_partial,
                    DEFAULT_RECORDS: _default_number_partial,
                    PRIMARY: 'Partial',
                    SECONDARY: {
                        },
                    FIELDS: {
                        'Partial': None,
                        },
                    DEFER: {
                        },
                    },
                )


    class mAppSysPanel(panel.AppSysPanel):

        _btn_open = 102
        _btn_close = 103

        def __init__(self, parent, cnf=dict(), **kargs):
            super(mAppSysPanel, self).__init__(
                parent=parent,
                cnf=cnf,
                **kargs)
            self.create_buttons()

        def describe_buttons(self):
            self.define_button(
                self._btn_open,
                text='File Open',
                tooltip='File Open button Tooltip.',
                command=self.open_file)
            self.define_button(
                self._btn_close,
                text='File Close',
                tooltip='File Close button Tooltip.',
                command=self.close_file)

        def open_file(self, event=None):
            if self.get_appsys().dbspec.get_database('games', None):
                for k, v in self.get_appsys().dbspec.get_dptfiles().items():
                    extents = dptdb.dptapi.IntVector()
                    v.get_database().ShowTableExtents(extents)
                    extents = [x for x in extents]
                    print(k, extents)
                    dlg = basesup.tools.dialogues.askquestion(
                        'Extend file',
                        ' '.join(('Confirm extend', k, 'file index')))
                    if dlg == tkinter.messagebox.YES:
                        v.get_database().Increase(extents[1], True)
                    dlg = basesup.tools.dialogues.askquestion(
                        'Extend file',
                        ' '.join(('Confirm extend', k, 'file data')))
                    if dlg == tkinter.messagebox.YES:
                        v.get_database().Increase(extents[0], False)
                basesup.tools.dialogues.showinfo(
                    'Test File Size Dialogue',
                    'File is already open')
            else:
                if not get_sizes_for_new_files(self.get_appsys().dbspec):
                    basesup.tools.dialogues.showinfo(
                        'Test File Size Dialogue',
                        'File not opened')
                    return
                self.get_appsys().dbspec.open_context()
                basesup.tools.dialogues.showinfo(
                    'Test File Size Dialogue',
                    'File opened')

        def close_file(self, event=None):
            if self.get_appsys().dbspec.get_database('games', None):
                self.get_appsys().dbspec.close_context()
                basesup.tools.dialogues.showinfo(
                    'Test File Size Dialogue',
                    'File closed')
            else:
                basesup.tools.dialogues.showinfo(
                    'Test File Size Dialogue',
                    'File is not open')


    class mFrame(frame.AppSysFrame):

        _tab_name = 10
        _state_start = 1


    class mApp(object):
        
        def __init__(self):
            self.root = tkinter.Tk()
            self.root.wm_title('Test File Size Dialogue')
            self.mf = mFrame(
                master=self.root,
                background='cyan',
                width=300,
                height=100,
                )
            self.mf.define_tab(
                mFrame._tab_name,
                text='Sample Tab',
                tooltip='Sample Tab tooltip text',
                underline=1,
                tabclass=mAppSysPanel)
            self.mf.create_tabs()
            self.mf.define_state_transitions(
                tab_state={
                    mFrame._state_start: (mFrame._tab_name,),
                    },
                switch_state={
                    (None, None): [
                        mFrame._state_start,
                        mFrame._tab_name],
                    },
                )
            self.mf.get_widget().pack(fill=tkinter.BOTH, expand=True)
            self.mf.get_widget().pack_propagate(False)
            self.mf.show_state()
            self.mf.dbspec = DPTapi(FileSpec(), os.getenv('HOME'))

    app = mApp()
    app.root.mainloop()
