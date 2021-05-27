# record.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Base classes for record definitions.

A record consists of key and value.  Key can be string or integer.  Value
must be string.

Originally written for use with Berkeley DB.  Beware if using with some
other database system.

List of classes

Key - key is pickled instance
KeyData - key is string or integer (attribute name is data)
KeydBaseIII - key is integer (attribute name is recno)
KeyDict - key is pickled instance.__dict__
KeyList - key is pickled sorted list of instance attribute values
KeyText - key is integer (attribute name is line)

Record - contains a Key() and a Value() by default.
RecorddBaseIII - contains a KeydBaseIII() and a ValueDict() by default.
RecordText - contains a KeyText() and a ValueText() by default.

Value - value is pickled instance
ValueData - value is string (attribute name is data)
ValueDict - value is pickled instance.__dict__
ValueList - value is pickled sorted list of instance attribute values
ValueText - value is string (attribute name is text)

Simple (default) use case is
r = Record(keyclass=Key, valueclass=Value, picklekey=False, picklevalue=True)
r = Record() is equivalent
Subclasses of Record will have different defaults.

"""

from cPickle import dumps, loads


class Key(object):

    """Define key and methods for comparison and conversion to database format.

    Methods added:

    load
    pack

    Methods overridden:

    __eq__
    __ge__
    __gt__
    __le__
    __lt__
    __ne__

    Methods extended:

    __init__
    
    """

    def __init__(self):

        super(Key, self).__init__()

    def load(self, key):
        """Do nothing.  Assume self created by unpickle so key already set.

        Method Record.load_key does not call load if key is a subclass of Key
        and key is pickled.  This is appropriate if Key.pack was used to
        generate key.

        If a subclass pack method does not return a subclass of Key then
        load must be overridden such that it reverses the effect of pickling
        self.pack()

        """
        pass

    def pack(self):
        """Return self.
        
        Subclasses must override this method if the return value
        is not pickled before use as record key.

        If a subclass pack method does not return a subclass of Key then
        method Record.load_key must call self.load to reconstruct the key.

        """
        return self

    def __eq__(self, other):
        """Return (self == other).  Attributes are compared explicitly."""
        s = self.__dict__
        o = other.__dict__
        if len(s) != len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True

    def __ge__(self, other):
        """Return (self >= other).  Attributes are compared explicitly.
        
        True can be returned if some attributes of other are missing from
        self.  __lt__ and __ge__ may return False for the same pair of
        objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(o) > len(s):
            return False
        for i in o:
            if i not in s:
                return False
            if s[i] != o[i]:
                return False
        return True

    def __gt__(self, other):
        """Return (self > other).  Attributes are compared explicitly.
        
        True cannot be returned unless at least one attribute of other is
        missing from self.  __gt__ and __le__ may return False for the
        same pair of objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(o) >= len(s):
            return False
        for i in o:
            if i not in s:
                return False
            if s[i] != o[i]:
                return False
        return True

    def __le__(self, other):
        """Return (self <= other).  Attributes are compared explicitly.
        
        True can be returned if some attributes of other are missing from
        self.  __gt__ and __le__ may return False for the same pair of
        objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(s) > len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True

    def __lt__(self, other):
        """Return (self < other).  Attributes are compared explicitly.
        
        True cannot be returned unless at least one attribute of other is
        missing from self.  __lt__ and __ge__ may return False for the
        same pair of objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(s) >= len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True

    def __ne__(self, other):
        """Return (self != other).  Attributes are compared explicitly."""
        s = self.__dict__
        o = other.__dict__
        if len(s) != len(o):
            return True
        for i in s:
            if i not in o:
                return True
            if s[i] != o[i]:
                return True
        return False
    

class KeyData(Key):

    """Define key and methods for a string or integer key.

    Methods added:

    None

    Methods overridden:

    load
    pack

    Methods extended:

    __init__
    
    """

    def __init__(self):

        super(KeyData, self).__init__()
        self.data = None

    def load(self, key):
        
        self.data = key
        
    def pack(self):

        return self.data
        

class KeydBaseIII(Key):

    """Define key and methods for a dBaseIII record key.

    Methods added:

    None

    Methods overridden:

    load
    pack

    Methods extended:

    __init__
    
    """

    def __init__(self):

        super(KeydBaseIII, self).__init__()
        self.recno = None

    def load(self, key):
        
        self.recno = key
        
    def pack(self):

        return self.recno
        

class KeyDict(Key):

    """Define key and methods for a pickled instance __dict__ key.

    Methods added:

    None

    Methods overridden:

    load
    pack

    Methods extended:

    None
    
    """

    def load(self, key):
        
        try:
            self.__dict__ = key
        except:
            self.__dict__ = dict()

    def pack(self):

        return self.__dict__


class KeyList(Key):

    """Define key and methods for a pickled ordered list of attributes key.

    Methods added:

    None

    Methods overridden:

    load
    pack

    Methods extended:

    __init__
    
    Example use
    class K(KeyList):
        attributes = dict(a1=list, a2=None)
        _attribute_order = tuple(sorted(attributes.keys()))

    """
    
    attributes = dict()
    _attribute_order = tuple()
    
    def __init__(self, attributes=None):

        super(KeyList, self).__init__()
        
        attributes = self.attributes
        if isinstance(attributes, dict):
            for a in attributes:
                if callable(attributes[a]):
                    setattr(self, a, attributes[a]())
            else:
                setattr(self, a, attributes[a])

    def load(self, key):
        
        try:
            for a, v in zip(self._attribute_order, key):
                self.__dict__[a] = v
        except:
            self.__dict__ = dict()

    def pack(self):

        return [self.__dict__.get(a) for a in self._attribute_order]


class KeyText(Key):

    """Define key and methods for a text file line number key.

    Methods added:

    None

    Methods overridden:

    load
    pack

    Methods extended:

    __init__
    
    """

    def __init__(self):

        super(KeyText, self).__init__()
        self.line = None

    def load(self, key):
        
        self.line = key
        
    def pack(self):

        return self.line
        

class Value(object):

    """Define value and comparison and conversion to database format methods.

    Methods added:

    load
    pack
    pack_value

    Methods overridden:

    __eq__
    __ge__
    __gt__
    __le__
    __lt__
    __ne__

    Methods extended:

    __init__
    
    Notes

    Subclasses must extend pack method to populate indexes.  Subclasses should
    override the pack_value method to change the way values are stored on
    database records.

    """

    def __init__(self):

        super(Value, self).__init__()

    def empty(self):
        """Set all existing attributes to None.

        Subclasses must override for different behaviour.

        """
        self.__dict__.clear()
        
    def load(self, value):
        """Do nothing.  Assume self created by unpickle so value already set.

        Method Record.load_value does not call load if value is a subclass of
        Value and value is pickled.  This is appropriate if Value.pack_value
        was used to generate value.

        If a subclass pack_value method does not return a subclass of Value
        then load must be overridden such that it reverses the effect of
        pickling self.pack_value()

        """
        pass

    def pack(self):
        """Return packed value and empty index dictionary.
        
        Subclasses must extend pack method to populate indexes.

        """
        return (self.pack_value(), dict())
        
    def pack_value(self):
        """Return self.
        
        Subclasses must override this method if the return value
        is not pickled before use as record value.

        If a subclass pack method does not return a subclass of Value then
        method Record.load_value must call self.load to reconstruct the value.

        """
        return self
        
    def __eq__(self, other):
        """Return (self == other).  Attributes are compared explicitly."""
        s = self.__dict__
        o = other.__dict__
        if len(s) != len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True
    
    def __ge__(self, other):
        """Return (self >= other).  Attributes are compared explicitly.
        
        True can be returned if some attributes of other are missing from
        self.  __lt__ and __ge__ may return False for the same pair of
        objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(o) > len(s):
            return False
        for i in o:
            if i not in s:
                return False
            if s[i] != o[i]:
                return False
        return True
    
    def __gt__(self, other):
        """Return (self > other).  Attributes are compared explicitly.
        
        True cannot be returned unless at least one attribute of other is
        missing from self.  __gt__ and __le__ may return False for the
        same pair of objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(o) >= len(s):
            return False
        for i in o:
            if i not in s:
                return False
            if s[i] != o[i]:
                return False
        return True
    
    def __le__(self, other):
        """Return (self <= other).  Attributes are compared explicitly.
        
        True can be returned if some attributes of other are missing from
        self.  __gt__ and __le__ may return False for the same pair of
        objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(s) > len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True
    
    def __lt__(self, other):
        """Return (self < other).  Attributes are compared explicitly.
        
        True cannot be returned unless at least one attribute of other is
        missing from self.  __lt__ and __ge__ may return False for the
        same pair of objects.
        
        """
        s = self.__dict__
        o = other.__dict__
        if len(s) >= len(o):
            return False
        for i in s:
            if i not in o:
                return False
            if s[i] != o[i]:
                return False
        return True
    
    def __ne__(self, other):
        """Return (self != other).  Attributes are compared explicitly."""
        s = self.__dict__
        o = other.__dict__
        if len(s) != len(o):
            return True
        for i in s:
            if i not in o:
                return True
            if s[i] != o[i]:
                return True
        return False
    

class ValueData(Value):

    """Define value and methods for string or integer data.

    Methods added:

    None

    Methods overridden:

    load
    pack_value

    Methods extended:

    __init__
    
    Notes

    Subclasses must extend inherited pack method to populate indexes.

    """

    def __init__(self):

        super(ValueData, self).__init__()
        self.data = None

    def empty(self):
        """Delete all attributes and set data attribute to None.

        Subclasses must override for different behaviour.

        """
        super(ValueData, self).empty()
        self.data = None
        
    def load(self, value):
        
        self.data = value
        
    def pack_value(self):

        return self.data
        

class ValueDict(Value):

    """Define value and methods for a pickled instance __dict__ value.

    Methods added:

    None

    Methods overridden:

    load
    pack_value

    Methods extended:

    None
    
    Notes

    Subclasses must extend inherited pack method to populate indexes.

    """

    def load(self, value):
        
        try:
            self.__dict__ = value
        except:
            self.__dict__ = dict()

    def pack_value(self):

        return self.__dict__
        

class ValueList(Value):

    """Define value and methods for a pickled ordered list of attributes value.

    Methods added:

    None

    Methods overridden:

    load
    pack_value

    Methods extended:

    __init__
    
    Notes

    This class should not be used directly.  Rather define a subclass and
    and set it's class attributes 'attributes' and '_attribute_order' to
    appropriate values.

    Subclasses must extend inherited pack method to populate indexes.

    """
    
    attributes = dict()
    _attribute_order = tuple()
    
    def __init__(self):

        super(ValueList, self).__init__()
        self._empty()

    def empty(self):
        """Delete all attributes and set initial attributes to default values.

        Subclasses must override for different behaviour.

        """
        self.__dict__.clear()
        self._empty()
        
    def load(self, value):
        
        try:
            for a, v in zip(self._attribute_order, value):
                self.__dict__[a] = v
        except:
            self.__dict__ = dict()

    def pack_value(self):

        return [self.__dict__.get(a) for a in self._attribute_order]

    def _empty(self):
        """Set initial attributes to default values."""
        attributes = self.attributes
        if isinstance(attributes, dict):
            for a in attributes:
                if callable(attributes[a]):
                    setattr(self, a, attributes[a]())
                else:
                    setattr(self, a, attributes[a])
        

class ValueText(Value):

    """Define value and methods for a line from a text file value.

    Methods added:

    None

    Methods overridden:

    load
    pack_value

    Methods extended:

    None
    
    Notes

    Subclasses must extend inherited pack method to populate indexes.

    """

    def load(self, value):
        
        self.text = value

    def pack_value(self):

        return self.text
        

class Record(object):
    
    """Define record and database interface.
    
    Methods added:

    clone
    delete_record
    edit_record
    empty
    get_primary_key_from_index_record
    get_keys
    load_instance
    load_key
    load_record
    load_value
    set_packed_value_and_indexes
    put_record
    set_database
    packed_key
    packed_value

    Methods overridden:

    __init__
    __eq__
    __ge__
    __gt__
    __le__
    __lt__
    __ne__

    Methods extended:

    None
    
    Class Attributes

    _deletecallbacks - callbacks to delete records on subsidiary files
    _putcallbacks - callbacks to add records on subsidiary files

    Notes

    Subclasses of Record manage the storage and retrieval of data using
    values managed by subclasses of Value and keys managed by subclasses
    of Key.

    The class attributes _deletecallbacks and _putcallbacks control the
    application of index updates where the update is not the simple case:
    one index value per index per record.  _putcallbacks allows a record
    to be referenced from records on subsidiary files using the record key
    as the link.  _deletecallbacks allows the records on subsidiary files
    to be deleted when the main record is deleted.
    
    The pack method of the Key and Value classes, or subclasses, is used
    to generate the values for Record attributes srkey and srvalue.  These
    attributes are used by the Database subclass methods put_instance
    edit_instance and delete_instance to update the database.  put_record
    calls the Database subclass method put_instance and so on.

    The load_instance method populates a Record instance with data from the
    database record using the load methods of the Key and Value classes, or
    subclasses.  srvalue is set in this process but srkey is not.  A
    record instance created by cPickle.loads() will have srkey equal None.

    Note that srkey and srvalue determine order on database and that
    comparison of key and value in Record instances is not guaranteed to
    give the same answer as comparison of srkey and srvalue.  This includes
    equality tests involving pickled dictionaries.

    """
    
    def __init__(
        self,
        keyclass=None,
        valueclass=None,
        picklekey=False,
        picklevalue=True):
        """Initialize Record instance.

        keyclass - a subclass of Key
        valueclass - a subclass of Value
        picklekey==True - pickle key before storing on database record
        picklevalue==True - pickle value before storing on database record
        
        """
        if issubclass(keyclass, Key):
            self.key = keyclass()
        else:
            self.key = Key()
        if issubclass(valueclass, Value):
            self.value = valueclass()
        else:
            self.value = Value()
        self.record = None
        self.database = None
        self.dbname = None
        self.picklekey = picklekey
        self.picklevalue = picklevalue
        self.srkey = None
        self.srvalue = None
        self.srindex = None

    def __eq__(self, other):
        """Return (self.key == other.key and self.value == other.value)"""
        return self.key == other.key and self.value == other.value
    
    def __ge__(self, other):
        """Return (s.key > o.key or (s.key == s.key and s.value >= o.value))"""
        if self.key > other.key:
            return True
        if self.key == other.key:
            if self.value >= other.value:
                return True
        return False
    
    def __gt__(self, other):
        """Return (s.key > o.key or (s.key == s.key and s.value > o.value))"""
        if self.key > other.key:
            return True
        if self.key == other.key:
            if self.value > other.value:
                return True
        return False
    
    def __le__(self, other):
        """Return (s.key < o.key or (s.key == s.key and s.value <= o.value))"""
        if self.key < other.key:
            return True
        if self.key == other.key:
            if self.value <= other.value:
                return True
        return False
    
    def __lt__(self, other):
        """Return (s.key < o.key or (s.key == s.key and s.value < o.value))"""
        if self.key < other.key:
            return True
        if self.key == other.key:
            if self.value < other.value:
                return True
        return False
    
    def __ne__(self, other):
        """Return (self.key != other.key or self.value != other.value)"""
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
        database.delete_instance(
            dbset,
            self)

    def edit_record(self, database, dbset, dbname, newrecord):
        """Change database record for self to values in newrecord."""
        if self.srkey == newrecord.srkey:
            self.newrecord = newrecord
            database.edit_instance(
                dbset,
                self)
            # Needing self.newrecord = None makes the technique suspect
            self.newrecord = None
        else:
            database.delete_instance(
                dbset,
                self)
            # KEYCHANGE
            # can newrecord.key.data be used instead,
            # or even the decode_record_number function
            r = database.get_primary_record(
                dbset,
                database.decode_as_primary_key(dbset, newrecord.srkey))
            if r == None:
                database.put_instance(
                    dbset,
                    newrecord)
            else:
                i = self.__class__()
                i.load_instance(database, dbset, dbname, r)
                i.newrecord = newrecord
                database.edit_instance(
                    dbset,
                    i)
                
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
        _deletecallbacks and _putcallbacks.
        
        Important uses of the return value are in ...Delete ...Edit and
        ...Put methods of subclasses and in various on_data_change methods.
        This method assumes the existence of attributes in the instances
        referenced by self.key and self.value with the same name as
        databases defined by the subclass of the Database class.

        """
        try:
            if partial != None:
                return []
            elif datasource.primary:
                return [(self.key.__dict__[datasource.dbname],
                         self.srvalue)]
            else:
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
                    dbset,
                    self.get_primary_key_from_index_record()))

    def load_key(self, key):
        """Load self.key from key.

        If self.picklekey is false or the unpickled key is not an instance
        or a subclass of class Key then method self.key.load will load the
        data.  Otherwise bind to self.key.
        
        """
        if self.picklekey:
            k = loads(key)
            if isinstance(k, Key):
                self.key = k
            else:
                self.key.load(k)
        else:
            self.key.load(key)

    def load_record(self, record):
        """Load self.key and self.value from record."""
        self.load_key(record[0])
        self.load_value(record[1])

    def load_value(self, value):
        """Load self.value from value.

        If self.picklevalue is false or the unpickled value is not an instance
        or a subclass of class Value then method self.value.load will load the
        data.  Otherwise bind to self.value.
        
        """
        self.srvalue = value
        if self.picklevalue:
            v = loads(value)
            if isinstance(v, Value):
                self.value = v
            else:
                self.value.load(v)
        else:
            self.value.load(value)

    def set_packed_value_and_indexes(self):
        """Set self.srvalue and self.srindex for a database update."""
        self.srvalue, self.srindex = self.packed_value()

    def put_record(self, database, dbset):
        """Add a record to the database."""
        database.put_instance(
            dbset,
            self)

    def set_database(self, database):
        """Set database with which record is associated.

        Typical uses are when inserting a record or after closing and
        re-opening database from which record was read.
        
        """
        self.database = database

    _deletecallbacks = dict()
    _putcallbacks = dict()

    def packed_key(self):
        """Return self.key converted to string representation.

        If self.picklekey is true self.key.pack() is pickled before return.
        
        Call from the database get_packed_key method only as this may deal
        some cases first.
        
        """
        if self.picklekey:
            return dumps(self.key.pack())
        else:
            return self.key.pack()

    def packed_value(self):
        """Return (value, indexes).
        
        If self.picklevalue is true value is pickled before return.

        """
        if self.picklevalue:
            v, i = self.value.pack()
            return (dumps(v), i)
        else:
            return self.value.pack()


class RecorddBaseIII(Record):

    """Define a dBaseIII record

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    Notes

    .ndx files are not supported. Files are read-only.

    dBaseIII files are a simple way of exchanging tables.  This class allows
    import of data from these files.

    """
    
    def __init__(self, keyclass=None, valueclass=None):
        """Initialize dBaseIII record instance."""
        if not issubclass(keyclass, KeydBaseIII):
            keyclass = KeydBaseIII
        if not issubclass(valueclass, ValueDict):
            valueclass = ValueDict
        picklekey = False
        picklevalue = True

        super(RecorddBaseIII, self).__init__(
            keyclass=keyclass,
            valueclass=valueclass,
            picklekey=picklekey,
            picklevalue=picklevalue)


class RecordText(Record):

    """Define a text record

    Methods added:

    None

    Methods overridden:

    None

    Methods extended:

    __init__
    
    Notes

    Records are newline delimited on text file. Files are read-only.

    Text files are a simple way of exchanging data using a <key>=<value>
    convention for lines of text.  This class allows processing of data from
    these files using the methods designed for database access.

    """

    def __init__(self, keyclass=None, valueclass=None):
        """Initialize dBaseIII record instance."""
        if not issubclass(keyclass, KeyText):
            keyclass = KeyText
        if not issubclass(valueclass, ValueText):
            valueclass = ValueText
        picklekey = False
        picklevalue = False

        super(RecordText, self).__init__(
            keyclass=keyclass,
            valueclass=valueclass,
            picklekey=picklekey,
            picklevalue=picklevalue)

