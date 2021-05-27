# primary.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add record and recordset processing to PrimaryFile.
"""


class PrimaryError(Exception):
    pass


class Primary:
    
    """Add record and recordset processing to PrimaryFile.

    Add, delete, and modify, records on the primary database are very database
    engine specific, so these actions are left to subclasses of Primary.

    The methods identical for all supported database engines are __init__()
    and close().

    A number of methods, relevant on secondary databases but not on primary
    databases, are defined to raise exceptions.
    """

    def __init__(self, *args, **kargs):
        """Add record update and recordset processing to PrimaryFile."""
        super().__init__()#*args, **kargs)
        self._clientcursors = dict()
        self._recordsets = dict()
    
    def close(self):
        """Close primary file and any cursors or recordsets."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        for rs in list(self._recordsets.keys()):
            rs.close()
        self._recordsets.clear()
        super().close()

    def get_first_primary_key_for_index_key(self, key):
        """Return first primary key for secondary key in dbname for dbname.

        This operation makes no sense, unless implemented as 'return key',
        because this is Primary class.  Choose to raise an exception.
        
        """
        raise PrimaryError(
            ''.join(
                ('get_first_primary_key_for_index_key not available ',
                 'on primary database')))

    def populate_recordset_key_like(self, recordset, key):
        """Return recordset containing database records with keys like key.

        This operation makes no sense, unless implemented as return recordset
        containing record with key, because this is Primary class.  Choose to
        raise an exception.

        """
        raise PrimaryError(
            ''.join(
                ('populate_recordset_key_like not available ',
                 'on primary database')))

    def populate_recordset_key_startswith(self, recordset, key):
        """Return recordset on database containing records for keys starting
        key.

        This operation makes no sense, unless implemented as return recordset
        containing record with key, because this is Primary class.  Choose to
        raise an exception.

        """
        raise PrimaryError(
            ''.join(
                ('populate_recordset_key_startswith not available ',
                 'on primary database')))
    
    def file_records_under(self, recordset, key):
        """File records in recordset under key.

        This operation makes no sense because this is Primary class.  Raise an
        exception.

        """
        raise PrimaryError(
            'file_records_under not available for primary database')
    
    def unfile_records_under(self, key):
        """Delete the reference to records in file under key.

        This operation makes no sense because this is Primary class.  Raise an
        exception.

        """
        raise PrimaryError(
            'unfile_records_under not available for primary database')

    def find_values(self, valuespec):
        """Yield values meeting valuespec specification.

        This operation is not implemented in Primary class because the values
        are repr(...) intended for interpretation by ast.literal_eval(), but
        the operation is sensible.  Raise an exception.

        """
        raise PrimaryError(
            'find_values not implemented on primary database')
