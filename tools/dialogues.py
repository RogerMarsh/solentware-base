# dialogues.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Wrappers for the Tkinter dialogue functions.

If root window is closed while dialogue (askdirectory and so on) is open
a "can't invoke grab command" error is raised.

These functions wrap the Tkinter functions in try ... except ... clauses.

Note these wrappers may turn out a good idea anyway for portability.
(Sooner than I thought: Python2.5 and Python2.6 with an eye on Python3.0)

List of functions:

showinfo
showwarning
showerror
askquestion
askokcancel
askyesno
askyesnocancel
askretrycancel
askopenfilename
asksaveasfilename
askopenfilenames
askopenfile
askopenfiles
asksaveasfile
askdirectory

"""

# At 2009-08-01 calling tkMessageBox.askyesno and so on does not work
# on Python2.6: s == YES compares booleanString with str
# but calling _show works (as it does in tkMessageBox.py test stuff)
# tkFileDialog functions seem ok

import tkinter, tkinter.messagebox, tkinter.filedialog
import re

GRAB_ERROR = ''.join((
    'can',
    "'",
    't invoke "grab" command:  application has been destroyed'))
FOCUS_ERROR = ''.join((
    'can',
    "'",
    't invoke "focus" command:  application has been destroyed'))


def showinfo(title=None, message=None, **options):
    """Show an info message"""
    try:
        return str(tkinter.messagebox._show(
            title, message, tkinter.messagebox.INFO, tkinter.messagebox.OK, **options))
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def showwarning(title=None, message=None, **options):
    """Show a warning message"""
    try:
        return str(tkinter.messagebox._show(
            title, message, tkinter.messagebox.WARNING, tkinter.messagebox.OK, **options))
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def showerror(title=None, message=None, **options):
    """Show an error message"""
    try:
        return str(tkinter.messagebox._show(
            title, message, tkinter.messagebox.ERROR, tkinter.messagebox.OK, **options))
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askquestion(title=None, message=None, **options):
    """Ask a question"""
    try:
        return str(tkinter.messagebox._show(
            title, message, tkinter.messagebox.QUESTION, tkinter.messagebox.YESNO,
            **options))
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askokcancel(title=None, message=None, **options):
    """Ask if operation should proceed; return true if the answer is ok"""
    try:
        s = tkinter.messagebox._show(
            title, message, tkinter.messagebox.QUESTION, tkinter.messagebox.OKCANCEL,
            **options)
        return str(s) == tkinter.messagebox.OK
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askyesno(title=None, message=None, **options):
    """Ask a question; return true if the answer is yes"""
    try:
        s = tkinter.messagebox._show(
            title, message, tkinter.messagebox.QUESTION, tkinter.messagebox.YESNO,
            **options)
        return str(s) == tkinter.messagebox.YES
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askyesnocancel(title=None, message=None, **options):
    """Ask a question; return true if the answer is yes, None if cancelled."""
    try:
        s = tkinter.messagebox._show(
            title, message, tkinter.messagebox.QUESTION, tkinter.messagebox.YESNOCANCEL,
            **options)
        s = str(s)
        if s == tkinter.messagebox.CANCEL:
            return None
        return s == tkinter.messagebox.YES
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askretrycancel(title=None, message=None, **options):
    """Ask if operation should be retried; return true if the answer is yes"""
    try:
        s = tkinter.messagebox._show(
            title, message, tkinter.messagebox.WARNING, tkinter.messagebox.RETRYCANCEL,
            **options)
        return str(s) == tkinter.messagebox.RETRY
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askopenfilename(**options):
    """Ask for a filename to open

    Use askopenfilenames to get the multiple=Tkinter.TRUE option to avoid
    the problem addressed in that function.
    
    """
    try:
        return tkinter.filedialog.askopenfilename(**options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def asksaveasfilename(**options):
    """Ask for a filename to save as"""
    try:
        return tkinter.filedialog.asksaveasfilename(**options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askopenfilenames(**options):
    """Ask for multiple filenames to open

    Returns a list of filenames or empty list if
    cancel button selected
    
    """
    # tkFileDialog.askopenfilenames always returns a tuple in the FreeBSD
    # port but always returns a string with path names separated by spaces
    # in some versions of the Microsoft Windows port.  Path names containing
    # spaces are surrounded by curly brackets (a TCL list).
    #
    # Under Wine multiple=Tkinter.TRUE has no effect at Python 2.6.2 so the
    # dialogue supports selection of a single file only.  Nothing can be done
    # about this here.  If it works in other versions - excellent.
    try:
        fn = tkinter.filedialog.askopenfilenames(**options)
        if not isinstance(fn, str):
            return fn
        if not fn:
            return fn
        fnl = [s[1:-1] for s in re.findall('{.*}', fn)]
        fnl.extend(re.sub('{.*}', '', fn).split())
        fnl.sort()
        return tuple(fnl)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askopenfile(mode = "r", **options):
    """Ask for a filename to open, and returned the opened file"""
    try:
        return tkinter.filedialog.askopenfile(mode = mode, **options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askopenfiles(mode = "r", **options):
    """Ask for multiple filenames and return the open file objects

    returns a list of open file objects or an empty list if
    cancel selected
    
    """
    try:
        return tkinter.filedialog.askopenfiles(mode = mode, **options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def asksaveasfile(mode = "w", **options):
    """Ask for a filename to save as, and returned the opened file"""
    try:
        return tkinter.filedialog.asksaveasfile(mode = mode, **options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise


def askdirectory(**options):
    """Ask for a directory, and return the file name"""
    try:
        return tkinter.filedialog.askdirectory (**options)
    except tkinter.TclError as error:
        if str(error) != GRAB_ERROR:
            raise
