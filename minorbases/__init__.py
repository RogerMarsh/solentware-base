# __init__.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide read access to various file formats using the database interface
defined in the api.database.Database and api.cursor.Cursor classes.

Access is provided for:

dBaseIII databases
CSV files
Text files
zip compressed text files
bz2 compressed text files

The text file formats are treated as "one line" is "one record".

"""
