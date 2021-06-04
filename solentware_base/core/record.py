# record.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Classes for key, value, and record definitions.

Key can be string or integer.  Value must be string.

Originally written for use with Berkeley DB.  Beware if using with some
other database system.

Simple (default) use case is
r = Record(keyclass=Key, valueclass=Value)
r = Record() is equivalent
Subclasses of Record will have different defaults.

"""

from pickle import dumps, loads
from ast import literal_eval

# import collections


class _Comparison:
    """Define comparison methods for inclusion in Key and Value classes."""

    # The attributes which contribute to comparision.
    # Empty by default, meaning use the instance dictionary.
    # Override in sublasses if needed.
    comparison_attributes = ()

    def __eq__(self, other):
        """Return (self == other).

        Attributes common to both objects are compared but existence of any
        attributes in just one of the objects evaluates to False.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        if len(self_attrs) != len(other_attrs):
            return False
        for i in other_attrs:
            if i not in self_attrs:
                return False
            if self.__dict__[i] != other.__dict__[i]:
                return False
        return True

    def __ge__(self, other):
        """Return (self >= other)..

        Attributes common to both objects are compared but attributes in just
        one of the objects are ignored.  Evaluates True when there are no
        attributes in common.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        for i in other_attrs:
            if i in self_attrs:
                try:
                    if self.__dict__[i] < other.__dict__[i]:
                        return False
                except TypeError:
                    return False
        return True

    def __gt__(self, other):
        """Return (self > other)..

        Attributes common to both objects are compared but attributes in just
        one of the objects are ignored.  Evaluates True when there are no
        attributes in common.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        for i in other_attrs:
            if i in self_attrs:
                try:
                    if self.__dict__[i] <= other.__dict__[i]:
                        return False
                except TypeError:
                    return False
        return True

    def __le__(self, other):
        """Return (self <= other).

        Attributes common to both objects are compared but attributes in just
        one of the objects are ignored.  Evaluates True when there are no
        attributes in common.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        for i in other_attrs:
            if i in self_attrs:
                try:
                    if self.__dict__[i] > other.__dict__[i]:
                        return False
                except TypeError:
                    return False
        return True

    def __lt__(self, other):
        """Return (self < other).

        Attributes common to both objects are compared but attributes in just
        one of the objects are ignored.  Evaluates True when there are no
        attributes in common.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        for i in other_attrs:
            if i in self_attrs:
                try:
                    if self.__dict__[i] >= other.__dict__[i]:
                        return False
                except TypeError:
                    return False
        return True

    def __ne__(self, other):
        """Return (self != other).

        Attributes common to both objects are compared but existence of any
        attributes in just one of the objects evaluates to True.

        """
        self_attrs = self.__class__.comparison_attributes or self.__dict__
        other_attrs = other.__class__.comparison_attributes or other.__dict__
        if len(self_attrs) != len(other_attrs):
            return True
        for i in other_attrs:
            if i not in self_attrs:
                return True
            if self.__dict__[i] != other.__dict__[i]:
                return True
        return False


class Key(_Comparison):
    """Define key and methods for conversion to database format."""

    def load(self, key):
        """Set self.__dict__ to ast.literal_eval(key).

        Method Record.load_key does not call load if key is a subclass of Key
        and key is pickled.  This is appropriate if Key.pack was used to
        generate key.

        If a subclass pack method does not return a subclass of Key then
        load must be overridden such that it reverses the effect of pickling
        self.pack()

        """
        self.__dict__ = literal_eval(key)

    def pack(self):
        """Return repr(self.__dict__).

        Subclasses must override this method if the return value
        is not pickled before use as record key.

        If a subclass pack method does not return a subclass of Key then
        method Record.load_key must call self.load to reconstruct the key.

        """
        return repr(self.__dict__)


class KeyData(Key):
    """Define key and methods for a string or integer key."""

    def __init__(self):
        """Extend then set self.recno to None."""
        super().__init__()
        self.recno = None

    def load(self, key):
        """Bind self.recno to key."""
        self.recno = key

    def pack(self):
        """Return self.recno."""
        return self.recno


class KeydBaseIII(KeyData):
    """Define key and methods for a dBaseIII record key."""


class KeyText(KeyData):
    """Define key and methods for a text file line number key."""


class Value(_Comparison):
    """Define value and conversion to database format methods.

    Subclasses must extend pack method to populate indexes.  Subclasses should
    override the pack_value method to change the way values are stored on
    database records.

    """

    def empty(self):
        """Set all existing attributes to None.

        Subclasses must override for different behaviour.

        """
        self.__dict__.clear()

    def load(self, value):
        """Set self.__dict__ to ast.literal_eval(value).

        Method Record.load_value does not call load if value is a subclass of
        Value and value is pickled.  This is appropriate if Value.pack_value
        was used to generate value.

        If a subclass pack_value method does not return a subclass of Value
        then load must be overridden such that it reverses the effect of
        pickling self.pack_value()

        """
        self.__dict__ = literal_eval(value)

    def pack(self):
        """Return packed value and empty index dictionary.

        Subclasses must extend pack method to populate indexes.

        """
        return (self.pack_value(), dict())

    def pack_value(self):
        """Return repr(self.__dict__).

        Subclasses must override this method if the return value
        is not pickled before use as record value.

        If a subclass pack method does not return a subclass of Value then
        method Record.load_value must call self.load to reconstruct the value.

        """
        return repr(self.__dict__)

    def get_field_value(self, fieldname, occurrence=0):
        """Return the value of a field, or None if field is not in __dict__.

        Added to support Find and Where classes.

        """
        return self.__dict__.get(fieldname, None)

    def get_field_values(self, fieldname):
        """Return tuple of field values for fieldname.

        Added to support Find and Where classes.

        """
        values = self.get_field_value(fieldname)
        if values is not None:
            return (values,)
        return None


class ValueData(Value):
    """Define value and methods for string or integer data.

    Subclasses must extend inherited pack method to populate indexes.

    """

    def __init__(self):
        """Extend then set self.data to None."""
        super().__init__()
        self.data = None

    def empty(self):
        """Delete all attributes and set data attribute to None.

        Subclasses must override for different behaviour.

        """
        super().empty()
        self.data = None

    def load(self, value):
        """Bind self.data to ast.literal_eval(value)."""
        self.data = literal_eval(value)

    def pack_value(self):
        """Return repr(self.data)."""
        return repr(self.data)


class ValueDict(Value):
    """Define methods to manipulate a dict of self.__dict__ attributes.

    Subclasses must extend inherited pack method to populate indexes.

    """


class ValueList(Value):
    """Define methods to manipulate a list of self.__dict__ attributes.

    This class should not be used directly.  Rather define a subclass and
    and set it's class attributes 'attributes' and '_attribute_order' to
    appropriate values.

    Subclasses must extend inherited pack method to populate indexes.

    """

    attributes = dict()
    _attribute_order = tuple()

    def __init__(self):
        """Extend then call self._empty to initialize self.__dict__."""
        super().__init__()
        self._empty()

    def empty(self):
        """Delete all attributes and set initial attributes to default values.

        Subclasses must override for different behaviour.

        """
        self.__dict__.clear()
        self._empty()

    def load(self, value):
        """Bind attributes in self._attributes_order to items in value.

        self.__dict__ is populated with keys from self._attribute_order
        mapped by position to values in ast.literal_eval(value).
        """
        try:
            for attr, data in zip(self._attribute_order, literal_eval(value)):
                self.__dict__[attr] = data
        except:
            self.__dict__ = dict()

    def pack_value(self):
        """Return repr(list(<attributes in self._attribute_order>))."""
        return repr([self.__dict__.get(a) for a in self._attribute_order])

    def _empty(self):
        """Set initial attributes to default values."""
        attributes = self.attributes
        if isinstance(attributes, dict):
            for name in attributes:
                # if isinstance(attributes[a], collections.Callable):
                if callable(attributes[name]):
                    setattr(self, name, attributes[name]())
                else:
                    setattr(self, name, attributes[name])

    def get_field_value(self, fieldname, occurrence=0):
        """Return value of a field occurrence, the first by default.

        Added to support Find and Where classes.

        """
        occurrences = self.__dict__.get(fieldname, None)
        if not occurrences:
            return None
        try:
            return occurrences[occurrence]
        except IndexError:
            return None

    def get_field_values(self, fieldname):
        """Return tuple of field values for fieldname.

        Added to support Find and Where classes.

        """
        return tuple(self.__dict__.get(fieldname, ()))


class ValueText(Value):
    """Define value and methods for a line from a text file value.

    Subclasses must extend inherited pack method to populate indexes.

    """

    def load(self, value):
        """Bind self.text to value."""
        self.text = value

    def pack_value(self):
        """Return self.text."""
        return self.text


class Record:
    """Define record and database interface.

    Subclasses of Record manage the storage and retrieval of data using
    values managed by subclasses of Value and keys managed by subclasses
    of Key.

    The class attributes deletecallbacks and putcallbacks control the
    application of index updates where the update is not the simple case:
    one index value per index per record.  putcallbacks allows a record
    to be referenced from records on subsidiary files using the record key
    as the link.  deletecallbacks allows the records on subsidiary files
    to be deleted when the main record is deleted.

    The pack method of the Key and Value classes, or subclasses, is used
    to generate the values for Record attributes srkey and srvalue.  These
    attributes are used by the Database subclass methods put_instance
    edit_instance and delete_instance to update the database.  put_record
    calls the Database subclass method put_instance and so on.

    The load_instance method populates a Record instance with data from the
    database record using the load methods of the Key and Value classes, or
    subclasses.  srvalue is set in this process but srkey is not.

    Note that srkey and srvalue determine order on database and that
    comparison of key and value in Record instances is not guaranteed to
    give the same answer as comparison of srkey and srvalue.  This includes
    equality tests involving pickled dictionaries.

    """

    def __init__(self, keyclass=None, valueclass=None):
        """Initialize Record instance.

        keyclass - a subclass of Key
        valueclass - a subclass of Value

        """
        super().__init__()
        if keyclass is None:
            self.key = KeyData()
        elif issubclass(keyclass, Key):
            self.key = keyclass()
        else:
            self.key = Key()
        if valueclass is None:
            self.value = Value()
        elif issubclass(valueclass, Value):
            self.value = valueclass()
        else:
            self.value = Value()
        self.record = None
        self.database = None
        self.dbname = None
        self.srkey = None
        self.srvalue = None
        self.srindex = None

    def __eq__(self, other):
        """Return bool(self.value, self.key) == (other.value, other.key)."""
        # Test keys first because keys are always record numbers, even though
        # keys not equal is just the tie breaker when values are equal.
        return self.key == other.key and self.value == other.value

    def __ge__(self, other):
        """Return bool(self.value, self.key) >= (other.value, other.key)."""
        if self.value > other.value:
            return True
        if self.value == other.value:
            if self.key >= other.key:
                return True
        return False

    def __gt__(self, other):
        """Return bool(self.value, self.key) > (other.value, other.key)."""
        if self.value > other.value:
            return True
        if self.value == other.value:
            if self.key > other.key:
                return True
        return False

    def __le__(self, other):
        """Return bool(self.value, self.key) <= (other.value, other.key)."""
        if self.value < other.value:
            return True
        if self.value == other.value:
            if self.key <= other.key:
                return True
        return False

    def __lt__(self, other):
        """Return bool(self.value, self.key) < (other.value, other.key)."""
        if self.value < other.value:
            return True
        if self.value == other.value:
            if self.key < other.key:
                return True
        return False

    def __ne__(self, other):
        """Return True if values are not equal or keys are not equal."""
        # Test keys first because keys are always record numbers, even though
        # keys not equal is just the tie breaker when values are equal.
        return self.key != other.key or self.value != other.value

    def clone(self):
        """Return a copy of self.

        Copy instance using cPickle.  self.database is dealt
        with separately as it cannot be pickled.  Assume that
        self.key and self.value can be pickled because these
        attributes are stored on the database.

        """
        database = self.database
        self.database = None
        clone = loads(dumps(self))
        self.database = database
        clone.database = database
        return clone

    def delete_record(self, database, dbset):
        """Delete a record."""
        database.delete_instance(dbset, self)

    def edit_record(self, database, dbset, dbname, newrecord):
        """Change database record for self to values in newrecord."""
        if self.srkey == newrecord.srkey:
            self.newrecord = newrecord
            database.edit_instance(dbset, self)
            # Needing self.newrecord = None makes the technique suspect
            # Changing the 'newobject' conditionals in DataGrid.on_data_change
            # allows this statement to be removed leaving the new record data
            # available in post-commit callbacks.
            # self.newrecord = None
        else:
            database.delete_instance(dbset, self)
            # KEYCHANGE
            # can newrecord.key.data be used instead, or even the
            # decode_record_number function directly because this is only use
            # of decode_as_primary_key which calls decode_record_number anyway.
            record = database.get_primary_record(
                dbset,
                # database.decode_as_primary_key(dbset, newrecord.srkey))
                database.decode_record_number(newrecord.srkey)
                if isinstance(newrecord.srkey, int)
                else newrecord.srkey,
            )
            if record is None:
                database.put_instance(dbset, newrecord)
            else:
                i = self.__class__()
                i.load_instance(database, dbset, dbname, record)
                i.newrecord = newrecord
                database.edit_instance(dbset, i)

    def empty(self):
        """Delete all self.value attributes and set to initial values."""
        self.value.empty()

    def get_primary_key_from_index_record(self):
        """Return self.record[1].  Assumes self.record is from an index.

        Subclasses must override this method if primary key is something
        else when record is from an index.  Format of these records is
        (<index value>, <primary key>) by default.

        """
        return self.record[1]

    def get_keys(self, datasource=None, partial=None):
        """Return a list of (key, value) tuples for datasource.dsname.

        An empty list is returned if a partial key is defined.  Subclasses
        must override this method to deal with indexes handled using
        deletecallbacks and putcallbacks.

        Important uses of the return value are in ...Delete ...Edit and
        ...Put methods of subclasses and in various on_data_change methods.
        This method assumes the existence of attributes in the instances
        referenced by self.key and self.value with the same name as
        databases defined by the subclass of the Database class.

        """
        try:
            if partial is not None:
                return []
            if datasource.primary:
                return [(self.key.recno, self.srvalue)]
            return [(self.value.__dict__[datasource.dbname], self.srkey)]
        except:
            return []

    def load_instance(self, database, dbset, dbname, record):
        """Load a class instance from database record."""
        self.record = record
        self.database = database
        self.dbset = dbset
        self.dbname = dbname
        if database.is_primary(dbset, dbname):
            self.load_record(record)
        else:
            self.load_record(
                database.get_primary_record(
                    dbset, self.get_primary_key_from_index_record()
                )
            )

    def load_key(self, key):
        """Load self.key from key."""
        # Huh?
        # Looking for an utf8 encoded repr() so key should be bytes but
        # database engine may have returned an iso-8859-1 str.
        # if isinstance(key, str):
        #    key = key.encode('iso-8859-1')
        # end Huh?
        self.key.load(key)

    def load_record(self, record):
        """Load self.key and self.value from record."""
        self.load_key(record[0])
        self.load_value(record[1])

    def load_value(self, value):
        """Load self.value from value which is repr(<data>).

        literal_eval(value) is delegated to self.value.load() method.

        """
        self.srvalue = value
        self.value.load(value)

    def set_packed_value_and_indexes(self):
        """Set self.srvalue and self.srindex for a database update."""
        self.srvalue, self.srindex = self.packed_value()

    def put_record(self, database, dbset):
        """Add a record to the database."""
        database.put_instance(dbset, self)

    def set_database(self, database):
        """Set database with which record is associated.

        Typical uses are when inserting a record or after closing and
        re-opening database from which record was read.

        """
        self.database = database

    deletecallbacks = dict()
    putcallbacks = dict()

    def packed_key(self):
        """Return self.key converted to string representation.

        Call from the database get_packed_key method only as this may deal with
        some cases first.

        """
        # Database engine interface will decode to iso-8859-1 str if necessary.
        return self.key.pack().encode("utf8")

    def packed_value(self):
        """Return (value, indexes)."""
        value, i = self.value.pack()
        # Database engine interface will decode to iso-8859-1 str if necessary.
        return (value, i)

    def get_srvalue(self):
        """Apply ast.literal_eval to self.srvalue and return created object."""
        return literal_eval(self.srvalue)

    def get_field_value(self, fieldname, occurrence=0):
        """Return value of a field occurrence, the first by default.

        Added to support Find and Where classes.

        """
        return self.value.get_field_value(fieldname, occurrence=occurrence)

    def get_field_values(self, fieldname):
        """Return tuple of field values for fieldname.

        Added to support Find and Where classes.

        """
        return self.value.get_field_values(fieldname)


class RecorddBaseIII(Record):
    """Define a dBaseIII record.

    .ndx files are not supported. Files are read-only.

    dBaseIII files are a simple way of exchanging tables.  This class allows
    import of data from these files.

    """

    def __init__(self, keyclass=None, valueclass=None, **k):
        """Initialize dBaseIII record instance."""
        if keyclass is None:
            keyclass = KeydBaseIII
        elif not issubclass(keyclass, KeydBaseIII):
            keyclass = KeydBaseIII
        if valueclass is None:
            valueclass = Value
        elif not issubclass(valueclass, Value):
            valueclass = Value

        super().__init__(keyclass=keyclass, valueclass=valueclass)

    def packed_value(self):
        """Return (value, indexes)."""
        value, i = self.value.pack()
        # Database engine interface will decode to iso-8859-1 str if necessary.
        return (value.encode("utf8"), i)

    def get_srvalue(self):
        """Return self.srvalue, assumed to be a bytes object."""
        # Wrong, but the is_engine_uses_bytes() test caused this to happen
        # given Database.engine_uses_bytes_or_str was not overridden.
        return self.srvalue


class RecordText(Record):
    """Define a text record.

    Records are newline delimited on text file. Files are read-only.

    Text files are a simple way of exchanging data using a <key>=<value>
    convention for lines of text.  This class allows processing of data from
    these files using the methods designed for database access.

    """

    def __init__(self, keyclass=None, valueclass=None, **k):
        """Initialize dBaseIII record instance."""
        if keyclass is None:
            keyclass = KeyText
        elif not issubclass(keyclass, KeyText):
            keyclass = KeyText
        if valueclass is None:
            valueclass = ValueText
        elif not issubclass(valueclass, ValueText):
            valueclass = ValueText

        super().__init__(keyclass=keyclass, valueclass=valueclass)

    def packed_value(self):
        """Return (value, indexes)."""
        return self.value.pack()

    def get_srvalue(self):
        """Return self.srvalue, assumed to be a bytes object."""
        # Wrong, but the is_engine_uses_bytes() test caused this to happen
        # given Database.engine_uses_bytes_or_str was not overridden.
        return self.srvalue
