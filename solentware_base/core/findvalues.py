# findvalues.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Index value selection statement evaluator.

Approximately equivalent to the SQL Select distinct statement and the DPT
Find All Values statement.

The statement syntax is defined in wherevalues.py module docstring.

"""


class FindValues:
    """Selection statement evaluator for a Database instance secondary table.

    The methods of the Database instance db are used to evaluate the request on
    the primary or secondary table named in dbset.

    """

    def __init__(self, db, dbset):
        """Initialise for dbset (table) in db (database)."""
        self._db = db
        self._dbset = dbset

    def find_values(self, valuesclause):
        """Put values meeting valuesclause specification in valuesclause.result."""
        valuesclause.result = list(
            self._db.find_values(valuesclause, self._dbset)
        )
