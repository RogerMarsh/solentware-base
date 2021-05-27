# testrecord.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test base classes for record definitions."""


if __name__=='__main__':

    from basesup.api.record import (
        Key, Value, KeyText, ValueText, Record, RecorddBaseIII, RecordText,
        )

    r = Record(Key, Value)
    print(r)
    r = Record(Record, Record)
    print(r)
    print(r.__dict__)
    r = RecorddBaseIII(Record, Record)
    print(r)
    r = RecordText(KeyText, ValueText)
    print(r)
    print(r.__dict__)
