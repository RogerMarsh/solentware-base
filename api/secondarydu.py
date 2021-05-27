# secondarydu.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add record and recordset processing to SecondaryFile.
"""
from .segmentsize import SegmentSize


class SecondaryduError(Exception):
    pass


class Secondary:
    
    """Add record and recordset processing to SecondaryFile.

    Add, delete, and modify, records on the secondary database are very
    database engine specific, so these actions are left to subclasses of
    Secondary.

    The methods identical for all supported database engines are __init__()
    and close().
    """

    def __init__(self, *args):
        """Delegate arguments to superclass and prepare to cache values for
        deferred update."""
        super().__init__(*args)
        self.values = dict()
    
    def defer_put(self, key, segment, record_number):
        """Add record_number to cached segment for key."""
        values = self.values.get(key)
        if values is None:
            self.values[key] = record_number
        elif isinstance(values, int):
            self.values[key] = [values]
            self.values[key].append(record_number)
        elif isinstance(values, list):
            values.append(record_number)
            if len(values) > SegmentSize.db_upper_conversion_limit:
                v = self.values[key] = SegmentSize.empty_bitarray.copy()
                for rn in values:
                    v[rn] = True
                v[record_number] = True
        else:
            values[record_number] = True

    def delete(self, key, value):
        raise SecondaryduError('delete not implemented')

    def replace(self, key, oldvalue, newvalue):
        raise SecondaryduError('replace not implemented')
