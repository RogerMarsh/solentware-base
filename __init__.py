# __init__.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
This package implements a key:value database of Python objects using a variety
of database engines.

DPT's notion of segments and bitmapped record numbers is applied to Berkeley
DB by suitable wrapping of bsddb3.

Berkeley DB's notion of primary and secondary databases links related key:value
databases in a database.

Key:value databases are implemented in DPT by using a subset of it's database
engine's data definition capabilities.

The wrapping of bsddb3 is emulated using the apsw and sqlite3 interfaces to
SQLite.

Cursors follow the navigation rules in Berkeley DB.

repr() and ast.literal_eval() are used to encode and decode Python objects.
"""
