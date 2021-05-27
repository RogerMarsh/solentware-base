# definition.py
# Copyright 2018 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Define the database structure from a description of files and fields.

The terms file and field are used approximately as in DPT (see solentware_base,
the earlier version of basesup2).  Table for file, and field or index for field,
are Sqlite approximations.  Similarely key and value in Berkeley DB primary
and secondary databases, where file fits primary database and field fits
secondary database approximately.

"""
from .constants import FIELDS, PRIMARY, SECONDARY


class DefinitionError(Exception):
    pass


class Definition:
    """Define file from specification using primary and secondary classes.

    dbset: name of file.
    specification: specification for file. The fields in file.
    primary_class: file's data field behaviour.
    secondary_class: file's index field's behaviour.
    field_name_converter: method to customise field names for database engine.

    """

    def __init__(self,
                 dbset=None,
                 specification=None,
                 primary_class=None,
                 secondary_class=None,
                 field_name_converter=None,
                 **kw):
        """Create file definition and primary and secondary field objects."""
        super().__init__()
        for a in (dbset,
                  specification,
                  primary_class,
                  secondary_class,
                  field_name_converter):
            if a is None:
                msg = ' '.join(['Cannot construct definition from',
                                'specification',
                                'because a mandatory argument is missing'])
                raise DefinitionError(msg)
        self._dbset = dbset
        primary = specification[PRIMARY]
        self.primary = primary_class(
            primary,
            specification,
            primary,
            **kw)
        self.secondary = {}
        self.dbname_to_secondary_key = {}
        if SECONDARY in specification:
            for sname, secondary in specification[SECONDARY].items():
                if not isinstance(sname, str):
                    msg = ' '.join(['Secondary name',
                                    'for', dbset,
                                    'must be a string'])
                    raise DefinitionError(msg)

                if secondary is None:
                    secondary = field_name_converter(sname)
                
                if secondary == primary:
                    msg = ' '.join(['Secondary name', secondary,
                                    'for', dbset,
                                    'cannot be same as primary'])
                    raise DefinitionError(msg)

                if secondary not in specification[FIELDS]:
                    msg = ' '.join(['Secondary name',
                                    secondary,
                                    'for', dbset, 'does not have',
                                    'a description'])
                    raise DefinitionError(msg)

                self.secondary[secondary] = self.make_secondary_class(
                    secondary_class,
                    dbset,
                    primary,
                    secondary,
                    specification,
                    **kw)
                self.dbname_to_secondary_key[sname] = secondary
        for v in self.secondary.values():
            v.set_primary_database(self.primary)

    def associate(self, dbname):
        """Return primary definition for dbname."""
        if dbname == self._dbset:
            return self.primary
        return self.secondary[self.dbname_to_secondary_key[dbname]]
