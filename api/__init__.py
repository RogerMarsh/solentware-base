# __init__.py
# Copyright 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
This package provides classes used by many, or all, supported database engines
and are not, in general, specific to particular database engines.

Some sre used as base classes of classes supporting a database engine and some
methods will be overridden or extended.

The record, recordset, segmentsize, and whervalues, modules are not used with
DPT's database engine because the engine provides those features itself.

Two modules, find_dpt and where_dpt, are specific to DPT's database engine, and
the find and where modules cannot be used with DPT's database engine.  DPT
provides features which would make find_dpt and where_dpt redundant, but not in
it's database engine.
"""
