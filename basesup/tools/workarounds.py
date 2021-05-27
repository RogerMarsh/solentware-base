# workarounds.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Workarounds for Tkinter functions at Tcl/Tk 8.5

It is assumed the genuine functions work at Python 2.5 with Tcl/Tk 8.4, if the
feature is supported, and changes in Tcl/Tk 8.5 raise the problem.

List of functions:

grid_configure_query
text_count   - No wrapper for count command in Text widget provided by Tkinter

"""

def grid_configure_query(widget, command, index, option=None):
    """Hack of Tkinter.py Misc method _grid_configure for option queries

    widget.grid_rowconfigure(1, 'pad') and similar do not work at Python 2.6
    because the self.tk.call(...) for the query, in _grid_configure, returns
    an int (maybe a double for width?) rather than a str; at least when
    Tk/Tcl 8.5 is used.

    This function deals with queries on weight pad minsize uniform and 'all'
    options for columnconfigure and rowconfigure commands.

    Queries on uniform work in the genuine function.

    """
    if option is None:
        res = widget.tk.call('grid', command, widget._w, index)
        words = widget.tk.splitlist(res)
        dict = {}
        for i in range(0, len(words), 2):
            key = words[i][1:]
            value = words[i+1]
            # perhaps just testing value == '' is enough
            if key == 'uniform':
                if not value:
                    value = None
            elif value == '':
                value = None
            dict[key] = value
        return dict
    else:
        # should precisely one leading '-' and one trailing '_' be adjusted
        res = widget.tk.call(
            ('grid',
             command,
             widget._w,
             index,
             ''.join(('-', option.rstrip('_').lstrip('-'))),
             ))
        # perhaps just testing res == '' is enough
        if option == 'uniform':
            if not res:
                res = None
        elif res == '':
            res = None
        return res


def text_count(widget, index1, index2, *options):
    """Hack Text count command. Return integer, or tuple if len(options) > 1.

    Tkinter does not provide a wrapper for the Tk Text widget count command
    at Python 2.7.1

    widget is a Tkinter Text widget.
    index1 and index2 are Indicies as specified in TkCmd documentation.
    options must be a tuple of zero or more of option values.  If no options
    are given the Tk default option is used.  If less than two options are
    given an integer is returned.  Otherwise a tuple of integers is returned
    (in the order specified in TkCmd documentation).

    See text manual page in TkCmd documentation for valid option values and
    index specification.

    Example:
    charcount, linecount = text_count(widget, start, end, '-chars', '-lines')

    """
    return widget.tk.call((widget._w, 'count') + options + (index1, index2))
