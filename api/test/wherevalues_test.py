# wherevalues_test.py
# Copyright 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""wherevalues tests"""

import unittest
from copy import copy, deepcopy
import re

from .. import wherevalues


class WhereValues___init__TC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test____module_constants(self):
        """"""
        constants = ((wherevalues.DOUBLE_QUOTE_STRING, '".*?"'),
                     (wherevalues.SINGLE_QUOTE_STRING, "'.*?'"),
                     (wherevalues.TO, 'to'),
                     (wherevalues.IN, 'in'),
                     (wherevalues.NOT, 'not'),
                     (wherevalues.LIKE, 'like'),
                     (wherevalues.FROM, 'from'),
                     (wherevalues.ABOVE, 'above'),
                     (wherevalues.BELOW, 'below'),
                     (wherevalues.LEADING_SPACE, '(?<=\s)'),
                     (wherevalues.TRAILING_SPACE, '(?=\s)'),
                     (wherevalues.STRING, '[^\s]+'),
                     )
        for a, v in constants:
            self.assertEqual(a, v)
        self.assertEqual(
            wherevalues.WHEREVALUES_RE.__class__, re.compile('').__class__)
        self.assertEqual(
            wherevalues.WHEREVALUES_RE.flags & (re.IGNORECASE|re.DOTALL),
            re.IGNORECASE|re.DOTALL)
        self.assertEqual(
            wherevalues.WHEREVALUES_RE.pattern,
            '|'.join(
                ('".*?"',
                 "'.*?'",
                 'not'.join(('(?<=\s)', '(?=\s)')),
                 'like'.join(('(?<=\s)', '(?=\s)')),
                 'from'.join(('(?<=\s)', '(?=\s)')),
                 'above'.join(('(?<=\s)', '(?=\s)')),
                 'below'.join(('(?<=\s)', '(?=\s)')),
                 'to'.join(('(?<=\s)', '(?=\s)')),
                 'in'.join(('(?<=\s)', '(?=\s)')),
                 '[^\s]+',
                 )).join(('(', ')')))
        self.assertEqual(wherevalues.KEYWORDS,
                         frozenset(('to',
                                    'in',
                                    'not',
                                    'like',
                                    'from',
                                    'above',
                                    'below',
                                    )))

    def test___init__(self):
        """"""
        w = wherevalues.WhereValues('')
        self.assertEqual(len(w.__dict__), 6)
        self.assertEqual(w.statement, '')
        self.assertEqual(w.tokens, None)
        self.assertEqual(w.node, None)
        self.assertEqual(w._error_token_offset, None)
        self.assertEqual(w._not, False)
        self.assertEqual(w._processors, None)


class WhereValues_lexTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__no_keywords(self):
        w = wherevalues.WhereValues('this statement has no keywords')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['this statement has no keywords'])

    def test__no_keywords_mixed(self):
        w = wherevalues.WhereValues('this Statement has no keYWOrds')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['this Statement has no keYWOrds'])

    def test_is_upper(self):
        w = wherevalues.WhereValues('fieldname IN value')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'in', 'value'])

    def test_lex_01(self):
        w = wherevalues.WhereValues('fieldname not from v')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'not', 'from', 'v'])

    def test_lex_02(self):
        w = wherevalues.WhereValues(
            'fieldname from v to w like *as in set')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'from', 'v', 'to', 'w', 'like', '*as', 'in', 'set'])

    def test_lex_03(self):
        w = wherevalues.WhereValues(
            'fieldname above in side below not side')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'above', 'in', 'side', 'below', 'not', 'side'])

    def test_lex_04(self):
        w = wherevalues.WhereValues(
            'fieldname above "in side" below "not side"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'above', 'in side', 'below', 'not side'])

    def test_lex_05(self):
        w = wherevalues.WhereValues(
            ' fieldname  above  in  side  below  not  side ')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'above', 'in', 'side', 'below', 'not', 'side'])

    def test_lex_06(self):
        w = wherevalues.WhereValues(
            ' fieldname  above  in  side  out  below  not  side  ways')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'above', 'in', 'side out', 'below',
             'not', 'side ways'])


class WhereValues_parseTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _vc(self, s, **kw):
        w = wherevalues.WhereValues(s)
        w.lex()
        w.parse()
        wvc = wherevalues.ValuesClause()
        for k in kw:
            self.assertIn(k, wvc.__dict__)
        for k, v in wvc.__dict__.items():
            if k not in kw:
                self.assertEqual(v, w.node.__dict__[k])
            else:
                self.assertEqual(w.node.__dict__[k], kw[k])

    def test__no_keywords(self):
        self._vc('this statement has no keywords',
                 field='this statement has no keywords',
                 valid_phrase=True)

    def test__duplicate_keywords(self):
        pattern = re.compile('v')
        self._vc('field from from', field='field')
        self._vc('field from above', field='field')
        self._vc('field above from', field='field')
        self._vc('field above above', field='field')
        self._vc('field from v from', field='field', from_value='v')
        self._vc('field from v above', field='field', from_value='v')
        self._vc('field above v from', field='field', above_value='v')
        self._vc('field above v above', field='field', above_value='v')
        self._vc('field to to', field='field')
        self._vc('field to below', field='field')
        self._vc('field below to', field='field')
        self._vc('field below below', field='field')
        self._vc('field to v to', field='field', to_value='v')
        self._vc('field to v below', field='field', to_value='v')
        self._vc('field below v to', field='field', below_value='v')
        self._vc('field below v below', field='field', below_value='v')
        self._vc('field like v like',field='field', like_pattern=pattern)
        self._vc('field not like v like',
                 field='field', like_pattern=pattern, like=False)
        self._vc('field like v not like', field='field', like_pattern=pattern)
        self._vc('field not like v not like',
                 field='field', like_pattern=pattern, like=False)
        self._vc('field in v in', field='field', in__set='v')
        self._vc('field not in v in', field='field', in__set='v', in_=False)
        self._vc('field in v not in', field='field', in__set='v')
        self._vc('field not in v not in', field='field', in__set='v', in_=False)

    def test__out_of_order_keywords(self):
        pattern = re.compile('v')
        self._vc('field to from', field='field')
        self._vc('field like to', field='field')
        self._vc('field in like', field='field')
        self._vc('field like from', field='field')
        self._vc('field in from', field='field')
        self._vc('field in to', field='field')
        self._vc('field to v from', field='field', to_value='v')
        self._vc('field like v to', field='field', like_pattern=pattern)
        self._vc('field in v like', field='field', in__set='v')
        self._vc('field like v from', field='field', like_pattern=pattern)
        self._vc('field in v from', field='field', in__set='v')
        self._vc('field in v to', field='field', in__set='v')
        self._vc('field to from v', field='field')
        self._vc('field like to v', field='field')
        self._vc('field in like v', field='field')
        self._vc('field like from v', field='field')
        self._vc('field in from v', field='field')
        self._vc('field in to v', field='field')

    def test_from_keyword(self):
        self._vc('field from v',
                 field='field', from_value='v', valid_phrase=True)
        self._vc('field from v w',
                 field='field', from_value='v w', valid_phrase=True)
        self._vc('field from v     w',
                 field='field', from_value='v w', valid_phrase=True)
        self._vc('field from "v     w"',
                 field='field', from_value='v     w', valid_phrase=True)

    def test_above_keyword(self):
        self._vc('field above v',
                 field='field', above_value='v', valid_phrase=True)
        self._vc('field above v w',
                 field='field', above_value='v w', valid_phrase=True)
        self._vc('field above v     w',
                 field='field', above_value='v w', valid_phrase=True)
        self._vc('field above "v     w"',
                 field='field', above_value='v     w', valid_phrase=True)

    def test_to_keyword(self):
        self._vc('field to v',
                 field='field', to_value='v', valid_phrase=True)
        self._vc('field to v w',
                 field='field', to_value='v w', valid_phrase=True)
        self._vc('field to v     w',
                 field='field', to_value='v w', valid_phrase=True)
        self._vc('field to "v     w"',
                 field='field', to_value='v     w', valid_phrase=True)

    def test_below_keyword(self):
        self._vc('field below v',
                 field='field', below_value='v', valid_phrase=True)
        self._vc('field below v w',
                 field='field', below_value='v w', valid_phrase=True)
        self._vc('field below v     w',
                 field='field', below_value='v w', valid_phrase=True)
        self._vc('field below "v     w"',
                 field='field', below_value='v     w', valid_phrase=True)

    def test_like_keyword(self):
        self._vc('field like v',
                 field='field', like_pattern=re.compile('v'), valid_phrase=True)
        self._vc('field like v w',
                 field='field',
                 like_pattern=re.compile('v w'),
                 valid_phrase=True)
        self._vc('field like v     w',
                 field='field',
                 like_pattern=re.compile('v w'),
                 valid_phrase=True)
        self._vc('field like "v     w"',
                 field='field',
                 like_pattern=re.compile('v     w'),
                 valid_phrase=True)

    def test_not_like_keyword(self):
        self._vc('field not like v',
                 field='field',
                 like_pattern=re.compile('v'),
                 valid_phrase=True,
                 like=False)
        self._vc('field not like v w',
                 field='field',
                 like_pattern=re.compile('v w'),
                 valid_phrase=True,
                 like=False)
        self._vc('field not like v     w',
                 field='field',
                 like_pattern=re.compile('v w'),
                 valid_phrase=True,
                 like=False)
        self._vc('field not like "v     w"',
                 field='field',
                 like_pattern=re.compile('v     w'),
                 valid_phrase=True,
                 like=False)

    def test_in_keyword(self):
        self._vc('field in v',
                 field='field', in__set='v', valid_phrase=True)
        self._vc('field in v w',
                 field='field', in__set='v w', valid_phrase=True)
        self._vc('field in v     w',
                 field='field', in__set='v w', valid_phrase=True)
        self._vc('field in "v     w"',
                 field='field', in__set='v     w', valid_phrase=True)

    def test_not_in_keyword(self):
        self._vc('field not in v',
                 field='field', in__set='v', valid_phrase=True, in_=False)
        self._vc('field not in v w',
                 field='field', in__set='v w', valid_phrase=True, in_=False)
        self._vc('field not in v     w',
                 field='field', in__set='v w', valid_phrase=True, in_=False)
        self._vc('field not in "v     w"',
                 field='field',
                 in__set='v     w',
                 valid_phrase=True,
                 in_=False)

    def test_like_bad_pattern(self):
        self._vc('field like ((',
                 field='field', like_pattern='((', valid_phrase=True)


class WhereValues_validateTC(unittest.TestCase):


    def setUp(self):
        self.w = wherevalues.WhereValues('')
        self.wvc = wherevalues.ValuesClause()
        self.db = Database()

    def tearDown(self):
        pass

    def test_validate_01(self):
        w = self.w
        w.tokens = ['a', 'b', 'c', 'd']
        w._error_token_offset = 2
        self.assertEqual(w.validate(None, None), ['a', 'b'])

    def test_validate_02(self):
        w = self.w
        self.assertEqual(w.node, None)
        self.assertEqual(w.validate(None, None), None)

    def test_validate_03(self):
        w = self.w
        w.node = self.wvc
        w.node.like_pattern = 's'
        self.assertEqual(w.validate(None, None), False)

    def test_validate_04(self):
        w = self.w
        w.node = self.wvc
        self.assertEqual(w.validate(None, None), False)

    def test_validate_05(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        self.assertEqual(w.validate(None, None), False)

    def test_validate_06(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.below_value = 'b'
        w.node.from_value = 'c'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_07(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.below_value = 'b'
        w.node.from_value = 'c'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_08(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.from_value = 'c'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_09(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.below_value = 'b'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_10(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.below_value = 'b'
        w.node.from_value = 'c'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_11(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.from_value = 'c'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), True)

    def test_validate_12(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.below_value = 'b'
        w.node.from_value = 'c'
        self.assertEqual(w.validate(self.db, 'set'), True)

    def test_validate_13(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.to_value = 'd'
        self.assertEqual(w.validate(self.db, 'set'), True)

    def test_validate_14(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        w.node.above_value = 'a'
        w.node.below_value = 'b'
        self.assertEqual(w.validate(self.db, 'set'), True)

    def test_validate_15(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        self.assertEqual(w.validate(self.db, 'set'), True)

    def test_validate_16(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'new name'
        self.assertEqual(w.validate(self.db, 'set'), False)

    def test_validate_17(self):
        w = self.w
        w.node = self.wvc
        w.node.valid_phrase = True
        w.node.field = 'name'
        self.assertEqual(w.validate(self.db, 'new set'), False)


class WhereValues_evaluateTC(unittest.TestCase):

    def setUp(self):
        self.w = wherevalues.WhereValues('')
        self.wvc = wherevalues.ValuesClause()

    def tearDown(self):
        pass

    def test_evaluate_01(self):
        self.assertEqual(self.w.evaluate(None), None)
        self.assertEqual(self.w._processors, None)

    def test_evaluate_02(self):
        self.w.node = self.wvc
        self.assertEqual(self.w.evaluate(None), None)
        self.assertEqual(self.w._processors, None)

    def test_evaluate_03(self):
        self.w.node = self.wvc
        self.assertEqual(self.w.evaluate(object()), None)
        self.assertEqual(self.w._processors, None)


class WhereValuesTC(unittest.TestCase):

    def setUp(self):
        self.fv = FindValues()

    def tearDown(self):
        pass

    def _wv(self, s, r):
        wv = wherevalues.WhereValues(s)
        wv.lex()
        wv.parse()
        wv.validate(self.fv._db, 'set')
        wv.evaluate(self.fv)
        self.assertEqual(wv.node.result, r) 

    def test_wherevalues_01(self):
        self._wv('name above d', ['p', 'q', 'r', 'z', 'zz'])
            


class ValuesClause___init__TC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___init__(self):
        """"""
        vc = wherevalues.ValuesClause()
        self.assertEqual(len(vc.__dict__), 11)
        self.assertEqual(vc.valid_phrase, False)
        self.assertEqual(vc.field, None)
        self.assertEqual(vc.above_value, None)
        self.assertEqual(vc.below_value, None)
        self.assertEqual(vc.from_value, None)
        self.assertEqual(vc.to_value, None)
        self.assertEqual(vc.like, True)
        self.assertEqual(vc.like_pattern, None)
        self.assertEqual(vc.in_, True)
        self.assertEqual(vc.in__set, None)
        self.assertEqual(vc.result, None)


class ValuesClause_apply_pattern_and_set_filters_to_valueTC(unittest.TestCase):

    def setUp(self):
        self.vc = wherevalues.ValuesClause()
        self.like_value = 'a catapult'

    def tearDown(self):
        pass

    def test_no_filters(self):
        vc = self.vc
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), True)

    def test_empty_set_filter_01(self):
        vc = self.vc
        vc.in__set = set()
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), False)

    def test_empty_set_filter_02(self):
        vc = self.vc
        vc.in__set = set()
        vc.in_ = False
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), True)
        self.assertEqual(bool(vc.like_pattern), False, msg='Possibly pattern?')

    def test_value_in_set_filter_01(self):
        vc = self.vc
        vc.in__set = set('v')
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), True)
        self.assertEqual(bool(vc.like_pattern), False, msg='Possibly pattern?')

    def test_value_in_set_filter_02(self):
        vc = self.vc
        vc.in__set = set('v')
        vc.in_ = False
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), False)

    def test_value_not_in_set_filter_01(self):
        vc = self.vc
        vc.in__set = set('w')
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), False)

    def test_value_not_in_set_filter_02(self):
        vc = self.vc
        vc.in__set = set('w')
        vc.in_ = False
        self.assertEqual(vc.apply_pattern_and_set_filters_to_value('v'), True)
        self.assertEqual(bool(vc.like_pattern), False, msg='Possibly pattern?')

    def test_value_like_pattern_filter_01(self):
        vc = self.vc
        vc.like_pattern = re.compile('cat')
        self.assertEqual(
            vc.apply_pattern_and_set_filters_to_value(self.like_value),
            True)

    def test_value_like_pattern_filter_02(self):
        vc = self.vc
        vc.like_pattern = re.compile('\Acat')
        self.assertEqual(
            vc.apply_pattern_and_set_filters_to_value(self.like_value),
            False)

    def test_value_like_pattern_filter_03(self):
        vc = self.vc
        vc.like_pattern = re.compile('cat')
        vc.like = False
        self.assertEqual(
            vc.apply_pattern_and_set_filters_to_value(self.like_value),
            False)

    def test_value_like_pattern_filter_04(self):
        vc = self.vc
        vc.like_pattern = re.compile('\Acat')
        vc.like = False
        self.assertEqual(
            vc.apply_pattern_and_set_filters_to_value(self.like_value),
            True)

    def test_value_like_pattern_filter_05(self):
        vc = self.vc
        vc.like_pattern = re.compile('ult\Z')
        self.assertEqual(
            vc.apply_pattern_and_set_filters_to_value(self.like_value),
            True)


# Emulate necessary parts of solentware_base.api.Database
class Database():

    # Emulate ordered index values in index 'name' in file 'set' in _db.
    _db = {
        'set': {'name': [
            'aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz',
            'p', 'q', 'r', 'z', 'zz']},
        }

    def exists(self, dbset, dbname):
        if dbset not in self._db:
            return False
        return dbname in self._db[dbset]

    # Emulate Database().find_values() method traversing all index values.
    def find_values(self, dbset, valuespec):
        if valuespec.above_value and valuespec.below_value:
            for v in self._db[dbset][valuespec.field]:
                if (v > valuespec.above_value and v < valuespec.below_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.above_value and valuespec.to_value:
            for v in self._db[dbset][valuespec.field]:
                if (v > valuespec.above_value and v <= valuespec.to_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.from_value and valuespec.to_value:
            for v in self._db[dbset][valuespec.field]:
                if (v >= valuespec.from_value and v <= valuespec.to_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.from_value and valuespec.below_value:
            for v in self._db[dbset][valuespec.field]:
                if (v >= valuespec.from_value and v < valuespec.below_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.above_value:
            for v in self._db[dbset][valuespec.field]:
                if (v > valuespec.above_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.from_value:
            for v in self._db[dbset][valuespec.field]:
                if (v >= valuespec.from_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.to_value:
            for v in self._db[dbset][valuespec.field]:
                if (v <= valuespec.to_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        elif valuespec.below_value:
            for v in self._db[dbset][valuespec.field]:
                if (v < valuespec.below_value and
                    valuespec.apply_pattern_and_set_filters_to_value(v)):
                    yield v
        else:
            for v in self._db[dbset][valuespec.field]:
                if valuespec.apply_pattern_and_set_filters_to_value(v):
                    yield v


# Emulate necessary parts of findvalues.FindValues
class FindValues():

    # A file in database _db.
    _dbset = 'set'
    _db = Database()

    def find_values(self, obj):
        obj.result = [v for v in self._db.find_values(self._dbset, obj)]


class ValuesClause_evaluate_node_resultTC(unittest.TestCase):


    def setUp(self):
        self.vc = wherevalues.ValuesClause()
        self.vc.field = 'name'
        self.vc.valid_phrase = True
        self.p = FindValues()

    def tearDown(self):
        pass

    def test_above_and_below_01(self):
        vc = self.vc
        vc.above_value = 'a'
        vc.below_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_above_and_below_02(self):
        vc = self.vc
        vc.above_value = 'd'
        vc.below_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_above_and_below_03(self):
        vc = self.vc
        vc.above_value = 'cz'
        vc.below_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_above_and_to_01(self):
        vc = self.vc
        vc.above_value = 'a'
        vc.to_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_above_and_to_02(self):
        vc = self.vc
        vc.above_value = 'd'
        vc.to_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_above_and_to_03(self):
        vc = self.vc
        vc.above_value = 'cz'
        vc.to_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r', 'z'])

    def test_from_and_to_01(self):
        vc = self.vc
        vc.from_value = 'a'
        vc.to_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_from_and_to_02(self):
        vc = self.vc
        vc.from_value = 'd'
        vc.to_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_from_and_to_03(self):
        vc = self.vc
        vc.from_value = 'cz'
        vc.to_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['cz', 'p', 'q', 'r', 'z'])

    def test_from_and_below_01(self):
        vc = self.vc
        vc.from_value = 'a'
        vc.below_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_from_and_below_02(self):
        vc = self.vc
        vc.from_value = 'd'
        vc.below_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_from_and_below_03(self):
        vc = self.vc
        vc.from_value = 'cz'
        vc.below_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['cz', 'p', 'q', 'r'])

    def test_above_01(self):
        vc = self.vc
        vc.above_value = 'a'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_above_02(self):
        vc = self.vc
        vc.above_value = 'd'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r', 'z', 'zz'])

    def test_above_03(self):
        vc = self.vc
        vc.above_value = 'cz'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r', 'z', 'zz'])

    def test_below_01(self):
        vc = self.vc
        vc.below_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_below_02(self):
        vc = self.vc
        vc.below_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r'])

    def test_below_03(self):
        vc = self.vc
        vc.below_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r'])

    def test_from_01(self):
        vc = self.vc
        vc.from_value = 'a'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_from_02(self):
        vc = self.vc
        vc.from_value = 'd'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r', 'z', 'zz'])

    def test_from_03(self):
        vc = self.vc
        vc.from_value = 'cz'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_to_01(self):
        vc = self.vc
        vc.to_value = 'zza'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_to_02(self):
        vc = self.vc
        vc.to_value = 's'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r'])

    def test_to_03(self):
        vc = self.vc
        vc.to_value = 'z'
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz',  'p', 'q', 'r', 'z'])

    def test_all_(self):
        vc = self.vc
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_valid_phrase(self):
        vc = self.vc
        vc.valid_phrase = False
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            None)

    def test_all_in_set(self):
        vc = self.vc
        vc.in__set = set(('p', 'q', 'r'))
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['p', 'q', 'r'])

    def test_all_not_in_set(self):
        vc = self.vc
        vc.in_ = False
        vc.in__set = set(('p', 'q', 'r'))
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'z', 'zz'])

    def test_all_in_set_empty(self):
        vc = self.vc
        vc.in__set = set()
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            [])

    def test_all_not_in_set_empty(self):
        vc = self.vc
        vc.in_ = False
        vc.in__set = set()
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_all_like_01(self):
        vc = self.vc
        vc.like_pattern = re.compile('z')
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['cz', 'z', 'zz'])

    def test_all_like_02(self):
        vc = self.vc
        vc.like_pattern = re.compile('z')
        vc.like = False
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'p', 'q', 'r'])

    def test_all_like_03(self):
        vc = self.vc
        vc.like_pattern = re.compile('e')
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            [])

    def test_all_like_04(self):
        vc = self.vc
        vc.like_pattern = re.compile('e')
        vc.like = False
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'cz', 'p', 'q', 'r', 'z', 'zz'])

    def test_all_like_in_set_01(self):
        vc = self.vc
        vc.like_pattern = re.compile('z')
        vc.in__set = set('z')
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['z'])

    def test_all_like_in_set_02(self):
        vc = self.vc
        vc.like_pattern = re.compile('z')
        vc.like = False
        vc.in__set = set('z')
        vc.in_ = False
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'p', 'q', 'r'])

    def test_all_like_in_set_03(self):
        vc = self.vc
        vc.like_pattern = re.compile('z')
        vc.like = False
        vc.in__set = set('r')
        vc.in_ = False
        vc.evaluate_node_result(self.p)
        self.assertEqual(
            vc.result,
            ['aa', 'ab', 'ad', 'b', 'cx', 'cy', 'p', 'q'])
        

if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(WhereValues___init__TC))
    runner().run(loader(WhereValues_lexTC))
    runner().run(loader(WhereValues_parseTC))
    runner().run(loader(WhereValues_validateTC))
    runner().run(loader(WhereValues_evaluateTC))
    runner().run(loader(WhereValuesTC))
    runner().run(loader(ValuesClause___init__TC))
    runner().run(loader(ValuesClause_apply_pattern_and_set_filters_to_valueTC))
    runner().run(loader(ValuesClause_evaluate_node_resultTC))
