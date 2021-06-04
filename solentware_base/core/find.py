# find.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Record selection statement evaluator.

Approximately equivalent to SQL Select statement where clause, and DPT Find
statement.

The statement syntax is defined in where.py module docstring.

"""

import re

from . import where


class FindError(Exception):
    """Exception for Find class."""


class Find:
    """Selection statement evaluator for a Database instance primary table.

    The methods of the Database instance db are used to evaluate the request on
    the primary table named in dbset.

    """

    def __init__(self, db, dbset, recordclass=None):
        """Initialiase for dbset (table) in db (database) using recordclass."""
        self._db = db
        self._dbset = dbset
        self._recordclass = recordclass
        # True means Alpha test, False means Num, None means neither.
        # In particular f eq v defaults to f alpha eq v but f is v can be
        # seen as f eq v without the defaulting.
        # Alpha: b>a b>ab 2>1 2>12
        # Num:   b>a ab>b 2>1 12>2
        # In this test Num works for integer numbers only.
        self.compare_field_value = {
            (where.IS, None): self._is,
            (where.LIKE, None): self._like_by_index,
            (where.STARTS, None): self._starts_by_index,
            (where.PRESENT, None): self._present,
            (where.EQ, True): self._eq,
            (where.NE, True): self._ne,
            (where.GT, True): self._gt,
            (where.LT, True): self._lt,
            (where.LE, True): self._le,
            (where.GE, True): self._ge,
            (where.BEFORE, True): self._before,
            (where.AFTER, True): self._after,
            ((where.FROM, where.TO), True): self._from_to,
            ((where.FROM, where.BELOW), True): self._from_below,
            ((where.ABOVE, where.TO), True): self._above_to,
            ((where.ABOVE, where.BELOW), True): self._above_below,
            (where.EQ, False): self._eq,
            (where.NE, False): self._ne,
            (where.GT, False): self._gt,
            (where.LT, False): self._lt,
            (where.LE, False): self._le,
            (where.GE, False): self._ge,
            (where.BEFORE, False): self._before,
            (where.AFTER, False): self._after,
            ((where.FROM, where.TO), False): self._from_to,
            ((where.FROM, where.BELOW), False): self._from_below,
            ((where.ABOVE, where.TO), False): self._above_to,
            ((where.ABOVE, where.BELOW), False): self._above_below,
        }
        self.boolean_operation = {
            where.AND: self._and,
            where.NOR: self._nor,
            where.OR: self._or,
        }

    @property
    def db(self):
        """Retrun database to search."""
        return self._db

    @property
    def dbset(self):
        """Return table to search."""
        return self._dbset

    def condition(self, obj):
        """Set obj.result.answer depending on obj.condition value."""
        if not self._db.exists(self._dbset, obj.field):
            return
        if obj.condition in {
            where.IS,
            where.LIKE,
            where.STARTS,
            where.PRESENT,
        }:
            case = (obj.condition, None)
        elif obj.num is True:
            case = (obj.condition, False)
        else:
            case = (obj.condition, True)

        # Let parser stop 'field not is not value' 'field not is value'
        # 'field is not value' is only case of 'field <condition> not value'
        obj.result.answer = self.compare_field_value[case](obj)
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            obj.result.answer = self.get_existence() ^ obj.result.answer

    def not_condition(self, obj):
        """Invert node's answer if not condition or not phrase specified.

        Both not conditions may be present and then both are applied.

        """
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            obj.result.answer = self.get_existence() ^ obj.result.answer

    def operator(self, obj):
        """Apply 'and', 'or', or 'nor', for obj to node and left node.

        Answer is put in left node and node is set to refer to same answer.

        """
        obj.left.result.answer = self.boolean_operation[obj.operator](obj)
        obj.result = obj.left.result

    def answer(self, obj):
        """Set 'up node's answer to node's answer using node's 'not phrase'."""
        obj.up.result = obj.result
        self.not_condition(obj.up)

    def initialize_answer(self, obj):
        """Initialise node's answer to an empty RecordList."""
        obj.result.answer = self._db.recordlist_nil(self._dbset)

    def get_existence(self):
        """Return RecordList of all existing records."""
        return self._db.recordlist_ebm(self._dbset)

    def get_record(self, recordset):
        """Yield each record from recordet."""
        # Support a single pass through recordset for index evaluation on data.
        instance = self._recordclass()
        for segment in recordset.rs_segments.values():
            j = segment.first()
            while j:
                instance.load_record(
                    self._db.get_primary_record(self._dbset, j[1])
                )
                yield j[1], instance.value
                j = segment.next()

    def non_index_condition(self, obj, record_number, record):
        """Evaluate a condition which cannot be done with indexes."""
        if obj.condition == where.IS:
            if obj.not_value:
                self._is_not(obj, record_number, record)
        elif obj.condition == where.NE:
            self._ne(obj, record_number, record)
        elif obj.condition == where.PRESENT:
            self._present(obj, record_number, record)
        elif obj.condition == where.LIKE:
            self._like(obj, record_number, record)
        elif obj.condition == where.STARTS:
            self._starts(obj, record_number, record)

    def _is(self, obj):
        """Return RecordList for 'field is value' condition."""
        # 'field is value' and 'field is not value' are allowed
        if obj.not_value:
            raise FindError("Attempt 'is' where 'is not' requested")
        return self._db.recordlist_key(
            self._dbset,
            obj.field,
            key=self._db.encode_record_selector(obj.value),
        )

    def _is_not(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field is not value'."""
        # 'field is value' and 'field is not value' are allowed
        if obj.not_value:
            field_values = record.get_field_values(obj.field)
            if field_values:
                for value in field_values:
                    if value != obj.value:
                        obj.result.answer.place_record_number(record_number)
                        break
        else:
            raise FindError("Attempt 'is not' where 'is' requested")

    def _like_by_index(self, obj):
        """Return RecordList for 'field like value' condition."""
        return self._db.recordlist_key_like(
            self._dbset,
            obj.field,
            keylike=self._db.encode_record_selector(obj.value),
        )

    def _starts_by_index(self, obj):
        """Return RecordList for 'field starts value' condition."""
        return self._db.recordlist_key_startswith(
            self._dbset,
            obj.field,
            keystart=self._db.encode_record_selector(obj.value),
        )

    def _like(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field like value'."""
        field_values = record.get_field_values(obj.field)
        if field_values:
            for value in field_values:
                try:
                    if re.search(obj.value, value):
                        obj.result.answer.place_record_number(record_number)
                        break
                except:
                    pass

    def _starts(self, obj, record_number, record):
        """Add record_number to obj answer if 'field starts value'."""
        field_values = record.get_field_values(obj.field)
        if field_values:
            for value in field_values:
                if value.startswith(obj.value):
                    obj.result.answer.place_record_number(record_number)
                    break

    def _present(self, obj, record_number, record):
        """Add record_number to obj answer if 'field' exists in record."""
        if record.get_field_values(obj.field):
            obj.result.answer.place_record_number(record_number)

    def _eq(self, obj):
        """Return RecordList for 'field eq value' condition."""
        return self._db.recordlist_key(
            self._dbset,
            obj.field,
            key=self._db.encode_record_selector(obj.value),
        )

    def _ne(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field ne value'."""
        field_values = record.get_field_values(obj.field)
        if field_values:
            for value in field_values:
                if value != obj.value:
                    obj.result.answer.place_record_number(record_number)
                    break

    def _gt(self, obj):
        """Return RecordList for 'field gt value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            gt=self._db.encode_record_selector(obj.value),
        )

    def _lt(self, obj):
        """Return RecordList for 'field lt value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            lt=self._db.encode_record_selector(obj.value),
        )

    def _le(self, obj):
        """Return RecordList for 'field le value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            le=self._db.encode_record_selector(obj.value),
        )

    def _ge(self, obj):
        """Return RecordList for 'field ge value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            ge=self._db.encode_record_selector(obj.value),
        )

    def _before(self, obj):
        """Return RecordList for 'field before value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            lt=self._db.encode_record_selector(obj.value),
        )

    def _after(self, obj):
        """Return RecordList for 'field after value' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            gt=self._db.encode_record_selector(obj.value),
        )

    def _from_to(self, obj):
        """Return RecordList for 'field from value1 to value2' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            ge=self._db.encode_record_selector(obj.value[0]),
            le=self._db.encode_record_selector(obj.value[1]),
        )

    def _from_below(self, obj):
        """Return RecordList for 'field from value1 below value2' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            ge=self._db.encode_record_selector(obj.value[0]),
            lt=self._db.encode_record_selector(obj.value[1]),
        )

    def _above_to(self, obj):
        """Return RecordList for 'field above value1 to value2' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            gt=self._db.encode_record_selector(obj.value[0]),
            le=self._db.encode_record_selector(obj.value[1]),
        )

    def _above_below(self, obj):
        """Return RecordList for 'field above value1 below value2' condition."""
        return self._db.recordlist_key_range(
            self._dbset,
            obj.field,
            gt=self._db.encode_record_selector(obj.value[0]),
            lt=self._db.encode_record_selector(obj.value[1]),
        )

    # This is wrong not to mention 'self', surely?
    def _and(self, obj):
        """Return this node's answer 'and'ed with left node's answer."""
        return obj.left.result.answer & obj.result.answer

    def _nor(self, obj):
        """Return 'not' this node's answer 'and'ed with left node's answer."""
        return obj.left.result.answer & (
            self.get_existence() ^ obj.result.answer
        )

    # This is wrong not to mention 'self', surely?
    def _or(self, obj):
        """Return this node's answer 'or'ed with left node's answer."""
        return obj.left.result.answer | obj.result.answer
