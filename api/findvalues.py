# findvalues.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""An index value selection statement evaluator approximately equivalent to SQL
Select distinct statement and DPT Find All Values statement.

The statement syntax is defined in wherevalues.py module docstring.

"""


class FindValues():

    """Selection statement evaluator for a Database instance secondary table.

    The methods of the Database instance db are used to evaluate the request on
    the primary or secondary table named in dbset.
    
    """
    
    def __init__(self, db, dbset):
        """Initialise for dbset (table) in db (database)."""
        self._db = db
        self._dbset = dbset

    def find_values(self, valuesclause):
        """Put values meeting valuesclause specification in valuesclause.result.

        """
        valuesclause.result = [
            v for v in self._db.get_database_instance(
                self._dbset, valuesclause.field).find_values(valuesclause)]
