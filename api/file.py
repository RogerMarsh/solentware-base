# file.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""File definition elements common to all supported database engines, and
validation of field attributes.

The supported database engines are:

SQLite3, using the apsw or sqlite3 modules.
Berkeley DB, using the bsddb3 module.
DPT, using the dptdb module.

Terminology is different in each database engine.

In SQLite3 this module deals with definition of tables and fields.
In Berkeley DB, this module deals with definition of databases of key-value
pairs, following the example of primary and secondary databases.
In DPT, this module deals with definition of files and fields.

Each database engine is used to emulate the Berkeley DB view of data, combined
with DPT's bitmapped record numbers when a key refers to large numbers of
records.
"""

from .constants import PRIMARY_FIELDATTS, SECONDARY_FIELDATTS, SPT

            
class FileError(Exception):
    pass


class File:
    
    """Set field and file names, and validate field attributes in description.

    dataname - approximately field name used by database engine.
    description - the field attributes, mostly for DPT database engine.
    keyvalueset_name - approximately file name used by database engine.
    engine_fieldatts - attributes relevant to the current database engine.
    error_field - field name from file specification.
    error_file - file name from file specification.

    file name and field name are best thought of as the names of Berkeley DB
    primary and secondary databases respectively.

    error_field and error_file are used only in exception reports when errors
    are found in description during initialization.

    dataname and keyvalueset_name values are chosen to suit each database
    engine, and the details of their use depend on the database engine.

    Using Berkeley DB terminology, an instance describes a primary database if
    dataname equals keyvalueset_name or a secondary database otherwise.  The
    method is_primary will say which it is.

    engine_fieldatts is mostly for the DPT database engine.  One DPT field
    attribute, EO, is borrowed to indicate autoincrement or not in SQlite3
    because it is assumed the equivalent action should happen in both database
    engines.  An attribute is provided to indicate if a secondary database
    should be hash or btree in Berkeley DB.

    The default FileSpec class provides legal defaults for the fieldatts
    expected by the DPT database engine.  However a significant database sizing
    task will be needed to provide values to create a working database using
    the DPT database engine.
    """

    def __init__(self,
                 dataname,
                 description,
                 keyvalueset_name,
                 engine_fieldatts,
                 error_field,
                 error_file,
                 **k):
    
        """Set field and file names, and validate field attributes in
        description.
        """

        self._dataname = dataname
        self._keyvalueset_name = keyvalueset_name
        self._primary = dataname == keyvalueset_name
        self._fieldatts = dict()
        
        if self._primary:
            fieldatts = PRIMARY_FIELDATTS
        else:
            fieldatts = SECONDARY_FIELDATTS
        for attr in engine_fieldatts:
            if attr in fieldatts:
                self._fieldatts[attr] = fieldatts[attr]
        
        if description == None:
            description = dict()
        if not isinstance(description, dict):
            msg = ' '.join(['Attributes for', error_field,
                            'in', error_file,
                            'must be a dictionary or "None"'])
            raise FileError(msg)
        
        for attr in description:
            if attr not in fieldatts:
                msg = ' '.join(['Attribute', repr(attr),
                                'for', error_field,
                                'in', error_file,
                                'is not allowed'])
                raise FileError(msg)
            
            if not isinstance(description[attr], type(fieldatts[attr])):
                msg = ' '.join([repr(attr),
                                'for', error_field,
                                'in', error_file,
                                'is wrong type'])
                raise FileError(msg)
            
            if attr == SPT:
                if (description[attr] < 0 or
                    description[attr] > 100):
                    msg = ' '.join(['Split percentage for', error_field,
                                    'in', error_file,
                                    'is invalid'])
                    raise FileError(msg)

            if attr in engine_fieldatts:
                self._fieldatts[attr] = description[attr]

        # It is known self._dataname is referenced in the super().__init__()
        # chain after moving the ExistenceBitMap() call to PrimaryFile, so the
        # super() call was moved to end of __init__ (without checking if the
        # other attributes are needed too).
        super().__init__(**k)

        # Moved from dbapi.File and _sqlite.File to same position
        # relative to super().__init__() chain.
        self._table_link = None

    @property
    def table_link(self):
        """Return database file."""
        try:
            return self._table_link[0]
        except TypeError:
            if self._table_link is not None:
                raise
            return None

    @property
    def table_connection_list(self):
        """Return list of database files."""
        return self._table_link

    def is_primary(self):
        """Return self._primary which is True or False."""
        return self._primary
