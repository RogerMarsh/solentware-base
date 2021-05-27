# controlfile.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""
"""


class ControlFileError(Exception):
    pass

            
class ControlFile:
    
    """Define a database to hold the keys of existence bitmap segments which
    contain unset bits corresponding to deleted records in a primary database.

    The keys in this database are the names of the primary databases specified
    in the FileSpec instance for the application.

    This database is used by FileControl instances to find record numbers which
    can be re-used rather than add a new record number at the end of a database
    without need.
    """

    def __del__(self):
        self.close()

    def open_root(self, *a):
        """Create file control database."""
        raise ControlFileError('Method open_root not implemented in subclass')

    def close(self):
        """Close file control database."""
        raise ControlFileError('Method close not implemented in subclass')

    def get_control_database(self):
        """Return the database containing file control records."""
        raise ControlFileError(
            'Method get_control_database not implemented in subclass')

    @property
    def control_file(self):
        """Return name of the database containing file control records."""
        raise ControlFileError(
            'Read-only property control_file not implemented in subclass')
