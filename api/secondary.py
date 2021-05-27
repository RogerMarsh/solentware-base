# secondary.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Add record and recordset processing to SecondaryFile.
"""


class Secondary:
    
    """Add record and recordset processing to SecondaryFile.

    Add, delete, and modify, records on the secondary database are very
    database engine specific, so these actions are left to subclasses of
    Secondary.

    The methods identical for all supported database engines are __init__()
    and close().
    """

    def __init__(self, *args):
        """Add update, cursor, and recordset methods, to SecondaryFile."""
        super().__init__()#*args)
        self._clientcursors = dict()
    
    def close(self):
        """Close secondary database and any cursors."""
        for c in list(self._clientcursors.keys()):
            c.close()
        self._clientcursors.clear()
        super().close()
