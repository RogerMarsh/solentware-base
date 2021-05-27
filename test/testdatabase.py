# testdatabase.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test define the database interface."""


if __name__ == '__main__':

    from basesup.api.database import Database, Cursor

    d = Database()
    c = Cursor()
    print d
    print c
    d.close_context()
