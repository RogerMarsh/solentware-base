# primarydu.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add record and recordset processing to PrimaryFile.
"""
from .segmentsize import SegmentSize
from .bytebit import Bitarray


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
        """Delegate arguments to superclass and prepare to cache existence
        bitmap segments for deferred update."""
        super().__init__(*args, **kargs)
        self.existence_bit_maps = dict()

        # The safe settings unless processing knows better.
        self.high_segment = None
        self.first_chunk = None

    def defer_put(self, segment, record_number):
        """Add bit to existence bit map for new record and defer update."""
        try:
            # Assume cached segment existence bit map exists
            self.existence_bit_maps[segment][record_number] = True
        except KeyError:
            # Get the segment existence bit map from database
            ebmb = self._get_existence_bits(segment)
            if ebmb is None:
                # It does not exist so create a new empty one
                ebm = SegmentSize.empty_bitarray.copy()
            else:
                # It does exist so convert database representation to bitarray
                ebm = Bitarray()
                ebm.frombytes(ebmb)
            # Set bit for record number and add segment to cache
            ebm[record_number] = True
            self.existence_bit_maps[segment] = ebm
