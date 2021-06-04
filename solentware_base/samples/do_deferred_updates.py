# do_deferred_updates.py
# Copyright 2011, 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide a simple user interface to the do_deferred_updates function."""


if __name__ == "__main__":

    import tkinter
    import tkinter.ttk
    import tkinter.filedialog
    import tkinter.messagebox

    from .. import do_deferred_updates

    root = tkinter.Tk()
    root.wm_title("Sample do_deferred_updates")
    frame = tkinter.ttk.Frame(master=root)
    text = tkinter.Text(master=frame, wrap=tkinter.WORD)
    frame.pack(fill=tkinter.Y, expand=tkinter.TRUE)
    text.pack(fill=tkinter.BOTH, expand=tkinter.TRUE)
    pyscript = tkinter.filedialog.askopenfilename(
        parent=root, title="Select Python script", initialdir="~"
    )
    if pyscript:
        text.insert(tkinter.END, "Python script: " + pyscript + "\n")
    else:
        text.insert(tkinter.END, "No Python script selected\n")
    if pyscript:
        databasepath = tkinter.filedialog.askdirectory(
            parent=root, title="Select database directory", initialdir="~"
        )
        if databasepath:
            text.insert(
                tkinter.END, "Database directory: " + databasepath + "\n"
            )
        else:
            text.insert(tkinter.END, "No database selected\n")

    def quit_(*a):
        """Quit application."""
        root.destroy()

    def show_menu(event=None):
        """Show popup menu."""
        menu.tk_popup(*event.widget.winfo_pointerxy())

    menu = tkinter.Menu(master=root, tearoff=False)
    if pyscript and databasepath:
        filepath = None

        def proceed(*a):
            """Set up dialogue for next action."""
            global filepath
            if filepath:
                process = do_deferred_updates.do_deferred_updates(
                    pyscript, databasepath, filepath
                )
                process.wait()
                text.insert(
                    tkinter.END, "\nProcess finished\nProceed or Quit?\n\n"
                )
                filepath = None
            elif pyscript and databasepath:
                filepath = tkinter.filedialog.askopenfilename(
                    parent=root, title="Select data file", initialdir="~"
                )
                if filepath:
                    text.insert(tkinter.END, "Data file: " + filepath + "\n")
                else:
                    text.insert(tkinter.END, "No data file selected\n")

        menu.add_separator()
        menu.add_command(
            label="Proceed", command=proceed, accelerator="Alt F2"
        )
        menu.add_command(label="Quit", command=quit_, accelerator="Alt F11")
        menu.add_separator()
        text.insert(
            tkinter.END, "\nRight-click to proceed after each step\n\n"
        )
        text.bind("<Alt-KeyPress-F2>", proceed)
        text.bind("<Alt-KeyPress-F11>", quit_)
        text.bind("<ButtonPress-3>", show_menu)
    else:
        menu.add_separator()
        menu.add_command(label="Quit", command=quit_, accelerator="Alt F11")
        menu.add_separator()
        text.insert(tkinter.END, "\nRight-click to quit\n")
        text.bind("<Alt-KeyPress-F11>", quit_)
        text.bind("<ButtonPress-3>", show_menu)
    root.mainloop()
