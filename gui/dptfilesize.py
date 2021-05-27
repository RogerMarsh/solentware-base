# dptfilesize.py
# Copyright 2010 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Dialogues for initial size, or increase in size, of DPT file.

List of classes:

FileSize

List of functions:

get_sizes_for_new_files

"""

import os
import tkinter

from ..tools.dialogues import FOCUS_ERROR
from ..api.constants import BSIZE, DSIZE, BRECPPG, DEFAULT_RECORDS


class FileSize(object):

    """Dialogue to give initial file size or accept defaults.
    
    Methods added:

    __del__
    create_buttons
    get_button_definitions
    _get_custom_file_sizes
    is_ok
    is_size_valid
    on_cancel
    on_custom
    on_default
    _report_bytesize
    
    Methods overridden:

    None

    Methods extended:

    __init__
    
    """

    def __init__(self, filespec, cnf=dict(), **kargs):
        """Show dialogue with default file size and standard options.

        The cnf and **kargs arguments are ignored at present.

        """
        super(FileSize, self).__init__()
        self.ok = False
        self.use_custom = None
        self.sizes = dict()
        self.bytesize = dict()
        self.confirm = tkinter.Toplevel()
        self.confirm.wm_title('File sizes')
        self.confirm.grid_rowconfigure(0)
        self.confirm.grid_rowconfigure(1, weight=1)
        self.confirm.grid_rowconfigure(2)
        self.confirm.grid_rowconfigure(3)
        self.confirm.grid_columnconfigure(0, weight=1)
        self.confirm.grid_columnconfigure(1)
        self.buttons_frame = tkinter.Frame(master=self.confirm)
        self.buttons_frame.grid(
            column=0, row=3, sticky=tkinter.NSEW, columnspan=2)
        self.create_buttons(self.get_button_definitions())
        self.reports = tkinter.Text(
                master=self.confirm,
                wrap=tkinter.WORD,
                takefocus=0,
                height=12)
        self.reports.insert(
            tkinter.END,
            ''.join((
                'The files below will be created with sufficient space ',
                'to hold the specified number of records assuming a ',
                'typical size for records.\n\n',
                'Files which already exist are not replaced but are listed ',
                'without the option of changing the initial number of ',
                'records.\n\n',
                'Give a new number in the entry box alongside the default ',
                'to change the initial size.  The file size is reported.\n\n',
                'Each time a file is opened the amount of unused space is ',
                'checked, and if this is less than 90% of the initial size ',
                'then the file is increased in size by initial size.',
                )))
        self.reports.grid(column=0, row=0, sticky=tkinter.NSEW, columnspan=2)
        self.sizes_canvas = tkinter.Canvas(master=self.confirm)
        self.sizes_canvas.grid(column=0, row=1, sticky=tkinter.NSEW)
        self.sizes_frame = tkinter.Frame(self.sizes_canvas)
        self.sizes_frame.grid_columnconfigure(0, weight=1)
        self.sizes_frame.grid_columnconfigure(1, weight=1, uniform='s')
        self.sizes_frame.grid_columnconfigure(2, weight=1, uniform='s')
        self.sizes_frame.grid_columnconfigure(3, weight=1, uniform='s')
        self.sizes_canvas.create_window(
            0, 0, window=self.sizes_frame, anchor=tkinter.NW)
        self.vsbar = tkinter.Scrollbar(self.confirm, orient=tkinter.VERTICAL)
        self.vsbar.grid(column=1, row=1, sticky=tkinter.NSEW)
        self.hsbar = tkinter.Scrollbar(self.confirm, orient=tkinter.HORIZONTAL)
        self.hsbar.grid(column=0, row=2, sticky=tkinter.NSEW)
        self.sizes_canvas.configure(xscrollcommand=self.hsbar.set)
        self.sizes_canvas.configure(yscrollcommand=self.vsbar.set)
        self.hsbar.configure(command=self.sizes_canvas.xview)
        self.vsbar.configure(command=self.sizes_canvas.yview)
        for e, i in enumerate(iter(filespec.get_dptfiles().items())):
            k, v = i
            w = tkinter.Label(master=self.sizes_frame, text=k)
            w.grid(column=0, row=e, sticky=tkinter.NSEW)
            if not os.path.exists(v._file):
                if v._filedesc[BSIZE] is None:
                    records = v._default_records
                else:
                    records = v._filedesc[BSIZE] * v._filedesc[BRECPPG]
                w = tkinter.Label(
                    master=self.sizes_frame,
                    text=str(records))
                w.grid(column=1, row=e, sticky=tkinter.NSEW)
                w = tkinter.Entry(master=self.sizes_frame)
                w.configure(
                    validate='key',
                    validatecommand=(
                        w.register(self.is_size_valid),
                        '%P',
                        '%W'))
                w.grid(column=2, row=e, sticky=tkinter.NSEW)
                self.sizes[k] = w.get
                s = self.bytesize[w.winfo_pathname(w.winfo_id())] = [v]
                w = tkinter.Label(master=self.sizes_frame)
                w.grid(column=3, row=e, sticky=tkinter.NSEW)
                s.append(w)
                self._report_bytesize('', s)
            else:
                w = tkinter.Label(master=self.sizes_frame, text='Exists')
                w.grid(column=2, row=e, sticky=tkinter.NSEW)
                self.sizes[k] = lambda : ''
        self.reports.configure(
            background=self.sizes_frame.cget('background'),
            state=tkinter.DISABLED)
        self.restore_focus = self.confirm.focus_get()
        self.confirm.wait_visibility()
        self.confirm.grab_set()
        slaves = self.sizes_frame.grid_slaves()
        if len(slaves):
            slaves[0].wait_visibility()
        self.sizes_canvas.configure(
            scrollregion=' '.join((
                '0',
                '0',
                str(self.sizes_frame.winfo_reqwidth()),
                str(self.sizes_frame.winfo_reqheight()))))
        self.confirm.wait_window()

    def __del__(self):
        """Restore focus to widget with focus before modal interaction."""
        self.ok = False
        try:
            #restore focus on dismissing dialogue
            if self.restore_focus is not None:
                self.restore_focus.focus_set()
        except tkinter._tkinter.TclError as error:
            #application destroyed while confirm dialogue exists
            if str(error) != FOCUS_ERROR:
                raise

    def create_buttons(self, buttons):
        """Create the buttons in the button definition."""
        for i, b in enumerate(buttons):
            button = tkinter.Button(
                master=self.buttons_frame,
                text=buttons[i][0],
                underline=buttons[i][3],
                command=buttons[i][4])
            self.buttons_frame.grid_columnconfigure(i*2, weight=1)
            button.grid_configure(column=i*2 + 1, row=0)
        self.buttons_frame.grid_columnconfigure(
            len(buttons*2), weight=1)

    def get_button_definitions(self):
        """Return modal confirmation dialogue button definitions"""
        return (
            ('Default Size',
             'Default Size button Tooltip.',
             True,
             -1,
             self.on_default),
            ('Custom Size',
             'Custom Size button Tooltip.',
             True,
             -1,
             self.on_custom),
            ('Cancel',
             'Cancel button Tooltip.',
             True,
             2,
             self.on_cancel),
            )

    def _get_custom_file_sizes(self):
        """Get file sizes from Entry widgets before dialogue destroyed."""
        for k, v in self.sizes.items():
            t = v()  # v is Entry's get method
            if len(t):
                self.sizes[k] = int(t)
            else:
                self.sizes[k] = None

    def is_ok(self):
        """Return True if dialogue dismissed with a file size button"""
        return self.ok

    def is_size_valid(self, new, widget):
        """Return True if new is all digits or ''."""
        if new == '':
            self._report_bytesize(new, self.bytesize[widget])
            return True
        else:
            n = new.isdigit()
            if n:
                self._report_bytesize(new, self.bytesize[widget])
            return n

    def on_cancel(self, event=None):
        """Dismiss dialogue and indicate action to be abandonned."""
        self.ok = False
        self.use_custom = None
        self._get_custom_file_sizes()
        self.confirm.destroy()

    def on_custom(self, event=None):
        """Dismiss dialogue and use values entered to do action."""
        self.ok = True
        self.use_custom = True
        self._get_custom_file_sizes()
        self.confirm.destroy()

    def on_default(self, event=None):
        """Dismiss dialogue and use default values to do action."""
        self.ok = True
        self.use_custom = False
        self._get_custom_file_sizes()
        self.confirm.destroy()

    def _report_bytesize(self, new, report):
        """Report new file size in bytes scaled to Mega or Giga."""
        v, w = report
        f = v._filedesc
        if len(new):
            bsize = int(round(int(new) / f[BRECPPG]))
            if bsize * f[BRECPPG] < int(new):
                bsize += 1
        elif f[BSIZE] is None:
            bsize = int(round(v._default_records / f[BRECPPG]))
            if bsize * f[BRECPPG] < v._default_records:
                bsize += 1
        else:
            bsize = f[BSIZE]
        size = bsize * (1 + v._btod_factor) * 8192
        if size > 999999999:
            scale = 1000000000
            units = 'Gbytes'
        elif size > 999999:
            scale = 1000000
            units = 'Mbytes'
        else:
            scale = 1000
            units = 'Kbytes'
        w.configure(text=' '.join((str(int(round(size / scale))), units)))


def get_sizes_for_new_files(filespec, cnf=dict(), **kargs):
    """Invoke file size dialogue if any files in filespec do not exist"""
    for k, v in filespec.get_dptfiles().items():
        if not os.path.exists(v._file):
            break
    else:
        return True
    d = FileSize(filespec, cnf=cnf, **kargs)
    if not d.is_ok():
        return False
    for k, v in d.sizes.items():
        f = filespec.get_dptfiles()[k]._filedesc
        if v and d.use_custom:
            records = v
        elif f[BSIZE] is None:
            records = filespec.get_dptfiles()[k]._default_records
        else:
            continue
        bsize = int(round(records / f[BRECPPG]))
        if bsize * f[BRECPPG] < records:
            bsize += 1
        dsize = int(round(bsize * filespec.get_dptfiles()[k]._btod_factor))
        f[BSIZE] = bsize
        f[DSIZE] = dsize
    return True
