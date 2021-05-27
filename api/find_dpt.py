# find_dpt.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A record selection statement evaluator approximately equivalent to SQL
Select statement where clause and DPT Find statement.

The statement syntax is defined in where.py module docstring.

"""

import re

from dptdb import dptapi

from . import where
from .find import Find


class Find(Find):

    """Selection statement evaluator for a Database instance primary table.

    The methods of the Database instance db are used to evaluate the request on
    the primary table named in dbset.
    
    """

    def __init__(self, db, dbset, recordclass=None):
        """Initialiase for dbset (table) in db (database) using recordclass.

        DPT encapsulates this as an APIDatabaseFileContext instance which
        provides all the methods needed to evaluate a selection statement.

        """
        super().__init__(db, dbset, recordclass=recordclass)
        self._context = db.get_database(dbset, dbset)

    def __del__(self):
        """Destroy the DPT objects supporting the Find instance on deletion."""
        self.close()

    def close(self):
        """Destroy the DPT objects supporting the Find instance."""
        # This may destroy too much: if so the Where instance using this Find
        # instance will have to walk it's node tree to destroy them.
        #self._context.DestroyAllRecordSets()
        #self._context.DestroyAllValueSets()
        self._context = None

    @property
    def context(self):
        """Return the APIDatabaseFileContext instance supporting the Find."""
        return self._context
        
    def condition(self, obj):
        """Set node's answer depending on condition (eg 'field eq value')."""
        if not self._db.exists(self._dbset, obj.field):
            return None
        if obj.condition in {where.IS, where.LIKE, where.STARTS, where.PRESENT}:
            case = (obj.condition, None)
        elif obj.num is True:
            case = (obj.condition, False)
        else:
            case = (obj.condition, True)

        # Let parser stop 'field not is not value' 'field not is value'
        # 'field is not value' is only case of 'field <condition> not value'
        recordset = self.compare_field_value[case](obj)
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            allrecs = self.get_existence()
            c = self._context
            answer = c.CreateRecordList()
            answer.Place(allrecs)
            c.DestroyRecordSet(allrecs)
            answer.Remove(recordset)
            recordset.Remove(answer)
            c.DestroyRecordSet(answer)
        obj.result.answer = recordset
        
    def not_condition(self, obj):
        """Invert node's answer if not condition or not phrase specified.

        Both not conditions may be present and then both are applied.

        """
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            c = self._context
            ora = obj.result.answer
            allrecs = self.get_existence()
            answer = c.CreateRecordList()
            answer.Place(ora)
            ora.Place(allrecs)
            ora.Remove(answer)
            c.DestroyRecordSet(allrecs)
            c.DestroyRecordSet(answer)
    
    def operator(self, obj):
        """Apply 'and', 'or', or 'nor', for obj to node and left node.

        Answer is put in left node and node is set to refer to same answer.

        """
        obj.left.result.answer = self.boolean_operation[obj.operator](obj)
        obj.result = obj.left.result
    
    def answer(self, obj):
        """Set 'up node's answer to node's answer using node's 'not phrase'"""
        obj.up.result = obj.result
        self.not_condition(obj.up)
    
    def initialize_answer(self, obj):
        """Initialise node's answer to an empty Recordset."""
        obj.result.answer = self._context.CreateRecordList()

    def get_existence(self):
        """Return Recordset of all existing records."""
        c = self._context
        allrecs = c.FindRecords(
            dptapi.APIFindSpecification(
                self._dbset,
                dptapi.FD_ALLRECS,
                dptapi.APIFieldValue('')))
        return allrecs
    
    def get_record(self, recordset):
        """Yield each record from recordet."""
        # Support a single pass through recordset for Table B index evaluation.
        jpfo = self._db.database_definition[self._dbset
                                            ].join_primary_field_occurrences
        instance = self._recordclass()
        c = recordset.OpenCursor()
        try:
            while c.Accessible():
                r = c.AccessCurrentRecordForRead()
                instance.load_record((r.RecNum(), jpfo(r)))
                yield r.RecNum(), instance.value
                c.Advance(1)
        finally:
            recordset.CloseCursor(c)
        
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
        """Return Recordset for 'field is value' condition."""
        # 'field is value' and 'field is not value' are allowed
        if obj.not_value:
            raise RuntimeError('_is')
        else:
            c = self._context
            foundset = c.FindRecords(
                dptapi.APIFindSpecification(
                    self._db.get_database_instance(
                        self._dbset, self._dbset).secondary[obj.field],
                    dptapi.FD_EQ,
                    dptapi.APIFieldValue(
                        self._db.encode_record_selector(obj.value)),
                    ))
            answer = c.CreateRecordList()
            answer.Place(foundset)
            c.DestroyRecordSet(foundset)
            return answer

    def _is_not(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field is not value'.
        """
        # 'field is value' and 'field is not value' are allowed
        if obj.not_value:
            f = record.get_field_values(obj.field)
            if f:
                for v in f:
                    if v != obj.value:
                        c = self._context
                        foundset = c.FindRecords(
                            dptapi.APIFindSpecification(
                                dptapi.FD_SINGLEREC,
                                record_number,
                                ))
                        obj.result.answer.Place(foundset)
                        c.DestroyRecordSet(foundset)
                        break
        else:
            raise RuntimeError('_is_not')

    def _like_by_index(self, obj):
        """Add record_number to obj answer if record has 'field like value'."""
        pattern = '.*?' + obj.value
        c = self._context
        answer = c.CreateRecordList()
        dvc = c.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(obj.field))
        dvc.SetDirection(dptapi.CURSOR_ASCENDING)
        dvc.GotoFirst()
        while dvc.Accessible():
            if re.match(pattern,
                        dvc.GetCurrentValue().ExtractString(),
                        flags=re.IGNORECASE|re.DOTALL):
                foundset = c.FindRecords(
                    dptapi.APIFindSpecification(
                        self._db.get_database_instance(
                            self._dbset, self._dbset).secondary[obj.field],
                        dptapi.FD_EQ,
                        dvc.GetCurrentValue(),
                        ))
                answer.Place(foundset)
                c.DestroyRecordSet(foundset)
                del foundset
            dvc.Advance(1)
        c.CloseDirectValueCursor(dvc)
        return answer

    def _starts_by_index(self, obj):
        """Return Recordset for 'field starts value' condition."""
        c = self._context
        answer = c.CreateRecordList()
        dvc = c.OpenDirectValueCursor(
            dptapi.APIFindValuesSpecification(obj.field))
        dvc.SetDirection(dptapi.CURSOR_ASCENDING)
        dvc.SetRestriction_LoLimit(dptapi.APIFieldValue(obj.value), True)
        dvc.GotoFirst()
        while dvc.Accessible():
            if not dvc.GetCurrentValue().ExtractString().startswith(obj.value):
                break
            foundset = c.FindRecords(
                dptapi.APIFindSpecification(
                    self._db.get_database_instance(
                        self._dbset, self._dbset).secondary[obj.field],
                    dptapi.FD_EQ,
                    dvc.GetCurrentValue(),
                    ))
            answer.Place(foundset)
            c.DestroyRecordSet(foundset)
            del foundset
            dvc.Advance(1)
        c.CloseDirectValueCursor(dvc)
        return answer

    def _like(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field like value'."""
        f = record.get_field_values(obj.field)
        if f:
            for v in f:
                try:
                    if re.search(obj.value, v):
                        c = self._context
                        foundset = c.FindRecords(
                            dptapi.APIFindSpecification(
                                dptapi.FD_SINGLEREC,
                                record_number,
                                ))
                        obj.result.answer.Place(foundset)
                        c.DestroyRecordSet(foundset)
                        break
                except:
                    pass

    def _starts(self, obj, record_number, record):
        """Add record_number to obj answer if 'field starts value'."""
        f = record.get_field_values(obj.field)
        if f:
            for v in f:
                if v.startswith(obj.value):
                    c = self._context
                    foundset = c.FindRecords(
                        dptapi.APIFindSpecification(
                            dptapi.FD_SINGLEREC,
                            record_number,
                            ))
                    obj.result.answer.Place(foundset)
                    c.DestroyRecordSet(foundset)
                    break

    def _present(self, obj, record_number, record):
        """Add record_number to obj answer if 'field' exists in record."""
        if record.get_field_values(obj.field):
            c = self._context
            foundset = c.FindRecords(
                dptapi.APIFindSpecification(
                    dptapi.FD_SINGLEREC,
                    record_number,
                    ))
            obj.result.answer.Place(foundset)
            c.DestroyRecordSet(foundset)

    def _eq(self, obj):
        """Return Recordset for 'field eq value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_EQ,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _ne(self, obj, record_number, record):
        """Add record_number to obj answer if record has 'field ne value'."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_NOT_EQ,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        obj.result.answer.Place(foundset)
        c.DestroyRecordSet(foundset)

    def _gt(self, obj):
        """Return Recordset for 'field gt value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_GT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _lt(self, obj):
        """Return Recordset for 'field lt value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_LT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _le(self, obj):
        """Return Recordset for 'field le value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_LE,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _ge(self, obj):
        """Return Recordset for 'field ge value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_GE,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _before(self, obj):
        """Return Recordset for 'field before value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_LT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _after(self, obj):
        """Return Recordset for 'field after value' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_GT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value)),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _from_to(self, obj):
        """Return Recordset for 'field from value1 to value2' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_RANGE_GE_LE,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[0])),
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[1])),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _from_below(self, obj):
        """Return Recordset for 'field from value1 below value2' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_RANGE_GE_LT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[0])),
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[1])),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _above_to(self, obj):
        """Return Recordset for 'field above value1 to value2' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_RANGE_GT_LE,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[0])),
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[1])),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _above_below(self, obj):
        """Return Recordset for 'field above value1 below value2' condition."""
        c = self._context
        foundset = c.FindRecords(
            dptapi.APIFindSpecification(
                self._db.get_database_instance(
                    self._dbset, self._dbset).secondary[obj.field],
                dptapi.FD_RANGE_GT_LT,
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[0])),
                dptapi.APIFieldValue(
                    self._db.encode_record_selector(obj.value[1])),
                ))
        answer = c.CreateRecordList()
        answer.Place(foundset)
        c.DestroyRecordSet(foundset)
        return answer

    def _and(self, obj):
        """Return this node's answer 'and'ed with left node's answer."""
        c = self._context
        answer = c.CreateRecordList()
        leftonly = c.CreateRecordList()
        leftonly.Place(obj.left.result.answer)
        leftonly.Remove(obj.result.answer)
        answer.Place(obj.left.result.answer)
        answer.Remove(leftonly)
        c.DestroyRecordSet(leftonly)
        return answer

    def _nor(self, obj):
        """Return 'not' this node's answer 'and'ed with left node's answer."""
        c = self._context
        allrecs = self.get_existence()
        alllist = c.CreateRecordList()
        alllist.Place(allrecs)
        alllist.Remove(obj.result.answer)
        alllist.Remove(obj.left.result.answer)
        answer = c.CreateRecordList()
        answer.Place(obj.left.result.answer)
        answer.Remove(alllist)
        c.DestroyRecordSet(alllist)
        c.DestroyRecordSet(allrecs)
        return answer

    def _or(self, obj):
        """Return this node's answer 'or'ed with left node's answer."""
        c = self._context
        answer = c.CreateRecordList()
        answer.Place(obj.left.result.answer)
        answer.Place(obj.result.answer)
        return answer
