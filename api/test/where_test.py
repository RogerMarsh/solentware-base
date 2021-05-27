# where_test.py
# Copyright 2015 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""where tests"""

import unittest
from copy import copy, deepcopy
import re

from .. import where
from ..find import Find

# Record numbers are based at 0 in database emulation in Processors class.
RECNUMBASE = 0


def adjust(expected_answer):
    """Adjust record numbers in expected answer to fit database engine."""
    if RECNUMBASE:
        return {n + RECNUMBASE for n in expected_answer}
    else:
        return expected_answer


class WhereTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test___raises(self):
        """"""
        pass

    def test___copy(self):
        """"""
        pass

    def test____assumptions(self):
        """"""
        msg = 'Failure of this test invalidates all other tests'

    def test____module_constants(self):
        """"""
        constants = ((where.DOUBLE_QUOTE_STRING, '".*?"'),
                     (where.SINGLE_QUOTE_STRING, "'.*?'"),
                     (where.LEFT_PARENTHESIS, '('),
                     (where.RIGHT_PARENTHESIS, ')'),
                     (where.OR, 'or'),
                     (where.TO, 'to'),
                     (where.IS, 'is'),
                     (where.EQ, 'eq'),
                     (where.NE, 'ne'),
                     (where.LT, 'lt'),
                     (where.LE, 'le'),
                     (where.GT, 'gt'),
                     (where.GE, 'ge'),
                     (where.NOT, 'not'),
                     (where.NOR, 'nor'),
                     (where.AND, 'and'),
                     (where.NUM, 'num'),
                     (where.LIKE, 'like'),
                     (where.FROM, 'from'),
                     (where.ALPHA, 'alpha'),
                     (where.ABOVE, 'above'),
                     (where.AFTER, 'after'),
                     (where.BELOW, 'below'),
                     (where.BEFORE, 'before'),
                     (where.STARTS, 'starts'),
                     (where.PRESENT, 'present'),
                     (where.STRING, '.+?'),
                     (where.LEADING_SPACE, '(?<=\s)'),
                     (where.TRAILING_SPACE, '(?=\s)'),
                     )
        for a, v in constants:
            self.assertEqual(a, v)
        self.assertEqual(where.KEYWORDS,
                         frozenset(('(',
                                    ')',
                                    'or',
                                    'to',
                                    'is',
                                    'eq',
                                    'ne',
                                    'lt',
                                    'le',
                                    'gt',
                                    'ge',
                                    'not',
                                    'nor',
                                    'and',
                                    'num',
                                    'like',
                                    'from',
                                    'alpha',
                                    'above',
                                    'after',
                                    'below',
                                    'starts',
                                    'before',
                                    'present',
                                    )))
        self.assertEqual(
            where.SINGLE_CONDITIONS,
            frozenset(('eq', 'ne', 'lt', 'le', 'gt', 'ge', 'after', 'before')))
        self.assertEqual(where.FIRST_CONDITIONS,
                         frozenset(('above', 'from')))
        self.assertEqual(where.SECOND_CONDITIONS,
                         frozenset(('to', 'below')))
        self.assertEqual(where.ALPHANUMERIC,
                         frozenset(('alpha', 'num')))
        self.assertEqual(where.BOOLEAN,
                         frozenset(('and', 'or', 'nor')))
        self.assertEqual(
            {where.LEADING_SPACE,
             where.TRAILING_SPACE,
             where.STRING,
             where.DOUBLE_QUOTE_STRING,
             where.SINGLE_QUOTE_STRING,}.union(where.KEYWORDS),
            {a for a, b in constants})
        self.assertEqual(where.WHERE_RE.__class__, re.compile('').__class__)
        self.assertEqual(
            where.WHERE_RE.flags & (re.IGNORECASE|re.DOTALL),
            re.IGNORECASE|re.DOTALL)
        self.assertEqual(
            where.WHERE_RE.pattern,
            '|'.join(
                (where.DOUBLE_QUOTE_STRING,
                 where.SINGLE_QUOTE_STRING,
                 ''.join(('\\', where.LEFT_PARENTHESIS)),
                 ''.join(('\\', where.RIGHT_PARENTHESIS)),
                 where.OR.join(('(?<=\s|\))', '(?=\s|\()')),
                 where.TO.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.IS.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.EQ.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.NE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.LT.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.LE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.GT.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.GE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.NOT.join(('\A', '(?=\s|\()')),
                 where.NOT.join(('(?<=\s|\()', '(?=\s|\()')),
                 where.NOR.join(('(?<=\s|\()', '(?=\s|\()')),
                 where.AND.join(('(?<=\s|\))', '(?=\s|\()')),
                 where.NUM.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.LIKE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.FROM.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.ALPHA.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.ABOVE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.AFTER.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.BELOW.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.BEFORE.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.STARTS.join((where.LEADING_SPACE, where.TRAILING_SPACE)),
                 where.PRESENT.join((where.LEADING_SPACE, '(?=\s|\)|\Z)')),
                 where.STRING,
                 )).join(('(', ')')))

    def test___init__(self):
        """"""
        w = where.Where('')
        self.assertEqual(len(w.__dict__), 7)
        self.assertEqual(w.statement, '')
        self.assertEqual(w.node, None)
        self.assertEqual(w.tokens, None)
        self.assertEqual(w._processors, None)
        self.assertEqual(w._f_or_v, None)
        self.assertEqual(w._not, None)
        self.assertIsInstance(w._error_information, where.WhereStatementError)
        self.assertEqual(w._error_information._statement, '')
        self.assertEqual(w._error_information._tokens, None)
        self.assertEqual(w._error_information._fields, None)


class Where_MethodsTC(unittest.TestCase):

    phrase_attr = frozenset(('field', 'condition', 'value', 'not_phrase',
                             'not_condition', 'not_value', 'num', 'alpha'))

    def setUp(self):
        self.w = where.Where('')
        self.wc = where.WhereClause()
        self.w.node = self.wc
        self.w.node.left = where.WhereClause()
        self.w._lexical_left = where.WhereClause()

    def tearDown(self):
        del self.w
        pass

    def _w(self, n, **kw):
        wv = where.WhereClause()
        for k in kw:
            self.assertIn(k, wv.__dict__)
        for k, v in wv.__dict__.items():
            if k not in kw:
                self.assertEqual(v, n.__dict__[k])
            elif k in self.phrase_attr:
                self.assertEqual(n.__dict__[k], kw[k])

    def test__first_token_field(self):
        """"""
        w = self.w
        w._first_token_field('')
        self._w(w.node, field='', up=w.node.up)

    def test__first_token_left_parenthesis(self):
        """"""
        w = self.w
        w._first_token_left_parenthesis('')
        self._w(w.node, up=w.node.up)
        self.assertIs(w.node.up.up, self.wc)
        self.assertIs(w.node, self.wc.down.down)

    def test__first_token_invert(self):
        """"""
        w = self.w
        w._first_token_invert('')
        self._w(w.node, not_phrase=True, up=self.wc)
        self.assertIs(w.node, self.wc.down)

    def test__right_parenthesis_boolean(self):
        """"""
        w = self.w
        w._right_parenthesis_boolean('and')
        self._w(w.node, operator='and', left=w.node)
        self._w(w.node.left, left=w.node, right=w.node)

    def test__value_boolean(self):
        """"""
        w = self.w
        w._value_boolean('nor')
        self._w(w.node, operator='nor', left=w.node)
        self._w(w.node.left, left=w.node, right=w.node)

    def test__field_condition(self):
        """"""
        w = self.w
        w._f_or_v = 'name'
        w._field_condition('after')
        self._w(w.node, condition='after', field='name', left=w.node)
        self.assertEqual(w._f_or_v, None)

    def test__set_not_num_alpha_condition(self):
        """"""
        # Other tests in test__set_num_alpha_condition.
        w = self.w
        self.assertEqual(w._set_not_num_alpha_condition('not'),
                         w._set_num_alpha_condition)
        self.assertEqual(w._not, True)
        self._w(w.node, left=w.node.left)

    def test__set_num_alpha_condition_is_01(self):
        """"""
        w = self.w
        w._not = True
        self.assertEqual(w._set_num_alpha_condition('is'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_num_alpha_condition_is_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_num_alpha_condition('is'),
                         w._set_not_value)
        self._w(w.node, condition='is', left=w.node.left)

    def test__set_num_alpha_condition_like(self):
        """"""
        w = self.w
        self.assertEqual(w._set_num_alpha_condition('like'),
                         w._set_value_like)
        self._w(w.node, condition='like', left=w.node.left)

    def test__set_num_alpha_condition_present(self):
        """"""
        w = self.w
        self.assertEqual(w._set_num_alpha_condition('present'),
                         w._set_and_or_nor_rightp__double_condition_or_present)
        self._w(w.node, condition='present', left=w.node.left)

    def test__set_num_alpha_condition_alphanum_01(self):
        """"""
        w = self.w
        self.assertEqual(w._set_num_alpha_condition('alpha'),
                         w._set_condition)
        self._w(w.node, alpha=True, num=False, left=w.node.left)

    def test__set_num_alpha_condition_alphanum_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_num_alpha_condition('num'),
                         w._set_condition)
        self._w(w.node, alpha=False, num=True, left=w.node.left)

    def test__set_condition_single(self):
        """"""
        w = self.w
        self.assertEqual(w._set_condition('lt'),
                         w._set_value)
        self._w(w.node, condition='lt', left=w.node.left)

    def test__set_condition_first(self):
        """"""
        w = self.w
        self.assertEqual(w._set_condition('from'),
                         w._set_first_value)
        self._w(w.node, condition='from', left=w.node.left)

    def test__set_condition_keyword(self):
        """"""
        w = self.w
        self.assertEqual(w._set_condition('or'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_condition_anything_else(self):
        """"""
        w = self.w
        self.assertEqual(w._set_condition('some value'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_second_condition(self):
        """"""
        w = self.w
        w.node.condition = 'above'
        self.assertEqual(w._set_second_condition('below'),
                         w._set_second_value)
        self._w(w.node, condition=('above', 'below'), left=w.node.left)

    def test__set_second_condition_keyword(self):
        """"""
        w = self.w
        self.assertEqual(w._set_second_condition('or'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_second_condition_anything_else(self):
        """"""
        w = self.w
        self.assertEqual(w._set_second_condition('some value'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_not_value(self):
        """"""
        # Other tests in test__set_value.
        w = self.w
        self.assertEqual(w._set_not_value('not'),
                         w._set_value)
        self._w(w.node, not_value=True, left=w.node.left)

    def test__set_value_01(self):
        """"""
        w = self.w
        self.assertEqual(w._set_value('and'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_value_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_value('value'),
                         w._set_and_or_nor_rightp__single_condition)
        self._w(w.node, value='value', left=w.node.left)

    def test__set_first_value_01(self):
        """"""
        w = self.w
        self.assertEqual(w._set_first_value('and'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_first_value_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_first_value('value'),
                         w._set_second_condition)
        self._w(w.node, value='value', left=w.node.left)

    def test__set_second_value_01(self):
        """"""
        w = self.w
        self.assertEqual(w._set_second_value('and'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_second_value_02(self):
        """"""
        w = self.w
        w.node.value = 'First value'
        self.assertEqual(w._set_second_value('value 2'),
                         w._set_and_or_nor_rightp__single_condition)
        self._w(w.node, value=('First value', 'value 2'), left=w.node.left)

    def test__copy_pre_field(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.not_phrase = True
        w._copy_pre_field()
        self._w(w.node, not_phrase=True, left=w.node.left)

    def test__copy_pre_is(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.field = 'name'
        w._copy_pre_is()
        self._w(w.node, field='name', left=w.node.left)

    def test__copy_pre_not_condition(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.field = 'NewField'
        w._copy_pre_not_condition()
        self._w(w.node, field='NewField', left=w.node.left)

    def test__copy_pre_like_starts_present(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.not_condition = True
        w._copy_pre_like_starts_present()
        self._w(w.node, not_condition=True, left=w.node.left)

    def test__copy_pre_alphanumeric(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.not_condition = True
        w._copy_pre_alphanumeric()
        self._w(w.node, not_condition=True, left=w.node.left)

    def test__copy_pre_condition(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.num = True
        w.node.left.alpha = False
        w._copy_pre_condition()
        self._w(w.node, num=True, alpha=False, left=w.node.left)

    def test__copy_pre_value_01(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'present'
        self.assertRaisesRegex(where.WhereError,
                               'PRESENT phrase followed by value phrase',
                               w._copy_pre_value)

    def test__copy_pre_value_02(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'from', 'to'
        self.assertRaisesRegex(where.WhereError,
                               'FROM-TO phrase followed by value phrase',
                               w._copy_pre_value)

    def test__copy_pre_value_03(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'from', 'below'
        self.assertRaisesRegex(where.WhereError,
                               'FROM-BELOW phrase followed by value phrase',
                               w._copy_pre_value)

    def test__copy_pre_value_04(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'above', 'to'
        self.assertRaisesRegex(where.WhereError,
                               'ABOVE-TO phrase followed by value phrase',
                               w._copy_pre_value)

    def test__copy_pre_value_05(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'above', 'below'
        self.assertRaisesRegex(where.WhereError,
                               'ABOVE-BELOW phrase followed by value phrase',
                               w._copy_pre_value)

    def test__copy_pre_value__like(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'like'
        w._copy_pre_value()
        self._w(w.node, condition='like', left=w.node.left)

    def test__copy_pre_value__is(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'is'
        w._copy_pre_value()
        self._w(w.node, condition='is', left=w.node.left)

    def test__copy_pre_value__value(self):
        """"""
        w = self.w
        self._w(w.node, left=w.node.left)
        w.node.left.condition = 'or'
        w._copy_pre_value()
        self._w(w.node, condition='or', left=w.node.left)

    def test__deferred_not_condition_01(self):
        """"""
        w = self.w
        w._deferred_not_condition()
        self._w(w.node, left=w.node.left)

    def test__deferred_not_condition_02(self):
        """"""
        w = self.w
        w._not = True
        w._deferred_not_condition()
        self.assertEqual(w._not, None)
        self._w(w.node, not_condition=True, left=w.node.left)

    def test__deferred_not_phrase_01(self):
        """"""
        w = self.w
        w._deferred_not_phrase()
        self._w(w.node, left=w.node.left)

    def test__deferred_not_phrase_02(self):
        """"""
        w = self.w
        w._not = True
        w._deferred_not_phrase()
        self.assertEqual(w._not, None)
        self._w(w.node, not_phrase=True, left=w.node.left)

    def test__set_rightp__rightp_01(self):
        """"""
        w = self.w
        self.assertRaisesRegex(where.WhereError,
                               'No unmatched left-parentheses',
                               w._set_rightp,
                               *(')',))

    def test__set_rightp__rightp_02(self):
        """"""

        # This is not how the links occur in real cases, where it would be wc
        # which has the 'up' link with a corresponding 'down' link in w.
        w = self.w
        wc = where.WhereClause()
        w.node.up = wc
        self._w(w.node, left=w.node.left, up=wc)
        self._w(wc)
        self._w(w.node.left)

    def test__set_rightp__rightp_03(self):
        """"""

        # This is more like a real case than test__set_rightp__rightp_02 case.
        w = self.w
        w.node.up = where.WhereClause()
        self.assertEqual(w._set_rightp(')'),
                         w._set_and_or_nor_rightp)

    def test__set_rightp__keyword(self):
        """"""
        w = self.w
        self.assertEqual(w._set_rightp('below'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_rightp__value(self):
        """"""
        w = self.w
        self.assertEqual(w._set_rightp('value'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_and_or_nor_rightp__boolean(self):
        """"""
        w = self.w
        self.assertEqual(w._set_and_or_nor_rightp('nor'),
                         w._set_field_not_leftp)

    def test__set_and_or_nor_rightp__double_condition_or_present__boolean(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_and_or_nor_rightp__double_condition_or_present('or'),
            w._set_field_leftp_not_condition)

    def test__set_and_or_nor_rightp__single_condition__boolean(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_and_or_nor_rightp__single_condition('and'),
            w._set_field_leftp_not_condition_value)

    def test__set_field_leftp_not_condition(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_leftp_not_condition('not'),
            w._set_field_leftp_condition)
        self.assertEqual(w._not, True)

    def test__set_field_leftp_not_condition_value(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_leftp_not_condition_value('not'),
            w._set_field_leftp_condition_value)
        self.assertEqual(w._not, True)

    def test__set_field_leftp_condition(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_leftp_condition('fieldname'),
            w._set_not_num_alpha_condition)
        self._w(w.node, field='fieldname', left=w.node.left)

    def test__set_leftp_condition__leftp(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('('),
            w._set_field_not_leftp)

    def test__set_leftp_condition__is_01(self):
        """"""
        w = self.w
        w._not = True
        self.assertEqual(w._set_leftp_condition('is'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_leftp_condition__is_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_leftp_condition('is'),
                         w._set_not_value)
        self._w(w.node, condition='is', left=w.node.left)

    def test__set_leftp_condition__single_conditions(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('ge'),
            w._set_value)
        self._w(w.node, condition='ge', left=w.node.left)

    def test__set_leftp_condition__first_conditions(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('above'),
            w._set_first_value)
        self._w(w.node, condition='above', left=w.node.left)

    def test__set_leftp_condition__like(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('like'),
            w._set_value_like)
        self._w(w.node, condition='like', left=w.node.left)

    def test__set_leftp_condition__present(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('present'),
            w._set_and_or_nor_rightp__double_condition_or_present)
        self._w(w.node, condition='present', left=w.node.left)

    def test__set_leftp_condition__alphanumeric_01(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('alpha'),
            w._set_condition)
        self._w(w.node, alpha=True, num=False, left=w.node.left)

    def test__set_leftp_condition__alphanumeric_02(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_leftp_condition('num'),
            w._set_condition)
        self._w(w.node, alpha=False, num=True, left=w.node.left)

    def test__set_leftp_condition__keywords(self):
        """"""
        w = self.w
        self.assertEqual(w._set_leftp_condition('not'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_leftp_condition__value(self):
        """"""
        w = self.w
        self.assertEqual(w._set_leftp_condition('value'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_field_or_value__boolean(self):
        """"""
        w = self.w
        self.assertEqual(w._set_field_or_value('and'),
                         w._set_field_leftp_condition_value)

    def test__set_field_or_value__is_01(self):
        """"""
        w = self.w
        w._not = True
        self.assertEqual(w._set_field_or_value('is'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_field_or_value__is_02(self):
        """"""
        w = self.w
        self.assertEqual(w._set_field_or_value('is'),
                         w._set_not_value)

    def test__set_field_or_value__single_conditions(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('ge'),
            w._set_value)

    def test__set_field_or_value__first_conditions(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('above'),
            w._set_first_value)

    def test__set_field_or_value__like(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('like'),
            w._set_value_like)

    def test__set_field_or_value__present(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('present'),
            w._set_and_or_nor_rightp__double_condition_or_present)

    def test__set_field_or_value__alphanumeric_01(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('alpha'),
            w._set_condition)

    def test__set_field_or_value__alphanumeric_02(self):
        """"""
        w = self.w
        self.assertEqual(
            w._set_field_or_value('num'),
            w._set_condition)

    def test__set_field_or_value__keyword(self):
        """"""
        w = self.w
        self.assertEqual(w._set_field_or_value('not'),
                         False)
        self._w(w.node, left=w.node.left)

    def test__set_field_or_value__value(self):
        """"""
        w = self.w
        self.assertEqual(w._set_field_or_value('value'),
                         False)
        self._w(w.node, left=w.node.left)


class Where_errorTC(unittest.TestCase):

    def setUp(self):
        self.w = where.Where('')

    def tearDown(self):
        pass

    def test____assumptions(self):
        w = self.w
        self.assertEqual(set((w.__dict__)),
                         set(('statement',
                              'node',
                              '_error_information',
                              '_processors',
                              'tokens',
                              '_f_or_v',
                              '_not',
                              )))

    def test_error_keyword(self):
        w = self.w
        self.assertEqual(w.error('above'), False)
        self.assertIsInstance(w._error_information, where.WhereStatementError)
        self.assertEqual(w._error_information._statement, '')
        self.assertEqual(w._error_information._tokens, None)
        self.assertEqual(w._error_information._fields, None)

    def test_error_value(self):
        w = self.w
        self.assertEqual(w.error('bad data'), False)
        self.assertIsInstance(w._error_information, where.WhereStatementError)
        self.assertEqual(w._error_information._statement, '')
        self.assertEqual(w._error_information._tokens, None)
        self.assertEqual(w._error_information._fields, None)


class Where_lex_phraseTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__no_keywords(self):
        w = where.Where('"this statement has no keywords"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['this statement has no keywords'])

    def test__no_keywords_mixed(self):
        w = where.Where('this" "Statement" "has" "no" "keYWOrds')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['this Statement has no keYWOrds'])

    def test_is_upper(self):
        w = where.Where('fieldname IS value')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'is', 'value'])

    def test_non_keywords_mixed(self):
        w = where.Where('fieLdnAme is VALUE')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieLdnAme', 'is', 'VALUE'])

    def test_like_mixed(self):
        w = where.Where('fieldname lIKe pattern')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'like', 'pattern'])

    def test_parentheses(self):
        w = where.Where("('some text')")
        w.lex()
        self.assertEqual(
            w.tokens,
            ['(', 'some text', ')'])

    def test_is(self):
        w = where.Where('fieldname is value')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'is', 'value'])

    def test_like(self):
        w = where.Where('fieldname like pattern')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'like', 'pattern'])

    def test_present(self):
        w = where.Where('fieldname present')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'present'])

    def test_alpha_eq(self):
        w = where.Where('floor alpha eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'alpha', 'eq', 'basement'])

    def test_alpha_ne(self):
        w = where.Where('floor alpha ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'alpha', 'ne', 'first'])

    def test_alpha_gt(self):
        w = where.Where('floor alpha gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'alpha', 'gt', 'ground'])

    def test_alpha_lt(self):
        w = where.Where('"houses in street" alpha lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'alpha', 'lt', 'thirty three'])

    def test_alpha_le(self):
        w = where.Where('floor alpha le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'alpha', 'le', 'third'])

    def test_alpha_ge(self):
        w = where.Where('floor alpha ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'alpha', 'ge', 'second'])

    def test_alpha_before(self):
        w = where.Where('"house room" alpha before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'before', 'attic'])

    def test_alpha_after(self):
        w = where.Where('"house room" alpha after "the ground floor"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'after', 'the ground floor'])

    def test_alpha_from_to(self):
        w = where.Where('"house room" alpha from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'from', 'the floor', 'to',
             'the roof'])

    def test_alpha_from_below(self):
        w = where.Where('"house room" alpha from "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'from', 'the floor', 'below',
             'the roof'])

    def test_alpha_above_to(self):
        w = where.Where('"house room" alpha above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'above', 'the floor', 'to',
             'the roof'])

    def test_alpha_above_below(self):
        w = where.Where('"house room" alpha above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'alpha', 'above', 'the floor', 'below',
             'the roof'])

    def test_num_eq(self):
        w = where.Where('floor num eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'num', 'eq', 'basement'])

    def test_num_ne(self):
        w = where.Where('floor num ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'num', 'ne', 'first'])

    def test_num_gt(self):
        w = where.Where('floor num gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'num', 'gt', 'ground'])

    def test_num_lt(self):
        w = where.Where('houses" in "street num lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'num', 'lt', 'thirty three'])

    def test_num_le(self):
        w = where.Where('floor num le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'num', 'le', 'third'])

    def test_num_ge(self):
        w = where.Where('floor num ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'num', 'ge', 'second'])

    def test_num_before(self):
        w = where.Where('"house room" num before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'before', 'attic'])

    def test_num_after(self):
        w = where.Where('"house "room num after the" "ground" "floor')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'after', 'the ground floor'])

    def test_num_from_to(self):
        w = where.Where('house" room" num from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'from', 'the floor', 'to',
             'the roof'])

    def test_num_from_below(self):
        w = where.Where('"house room" num from "the "floor below the" roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'from', 'the floor', 'below',
             'the roof'])

    def test_num_above_to(self):
        w = where.Where('"house room" num above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'above', 'the floor', 'to', 'the roof'])

    def test_num_above_below(self):
        w = where.Where('"house room" num above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'num', 'above', 'the floor', 'below',
             'the roof'])

    def test_eq(self):
        w = where.Where('floor eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'eq', 'basement'])

    def test_ne(self):
        w = where.Where('floor ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'ne', 'first'])

    def test_gt(self):
        w = where.Where('floor gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'gt', 'ground'])

    def test_lt(self):
        w = where.Where('"houses in street" lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'lt', 'thirty three'])

    def test_le(self):
        w = where.Where('floor le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'le', 'third'])

    def test_ge(self):
        w = where.Where('floor ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'ge', 'second'])

    def test_before(self):
        w = where.Where('"house room" before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'before', 'attic'])

    def test_after(self):
        w = where.Where('"house room" after "the ground floor"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'after', 'the ground floor'])

    def test_from_to(self):
        w = where.Where('"house room" from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'from', 'the floor', 'to', 'the roof'])

    def test_from_below(self):
        w = where.Where('"house room" from "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'from', 'the floor', 'below', 'the roof'])

    def test_above_to(self):
        w = where.Where('"house room" above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'above', 'the floor', 'to', 'the roof'])

    def test_above_below(self):
        w = where.Where('"house room" above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'above', 'the floor', 'below', 'the roof'])

    def test_is_not(self):
        w = where.Where('fieldname is not value')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'is', 'not', 'value'])

    def test_not_like(self):
        w = where.Where('fieldname not like pattern')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'not', 'like', 'pattern'])

    def test_not_present(self):
        w = where.Where('fieldname not present')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['fieldname', 'not', 'present'])

    def test_not_alpha_eq(self):
        w = where.Where('floor not alpha eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'alpha', 'eq', 'basement'])

    def test_not_alpha_ne(self):
        w = where.Where('floor not alpha ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'alpha', 'ne', 'first'])

    def test_not_alpha_gt(self):
        w = where.Where('floor not alpha gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'alpha', 'gt', 'ground'])

    def test_not_alpha_lt(self):
        w = where.Where('"houses in street" not alpha lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'not', 'alpha', 'lt', 'thirty three'])

    def test_not_alpha_le(self):
        w = where.Where('floor not alpha le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'alpha', 'le', 'third'])

    def test_not_alpha_ge(self):
        w = where.Where('floor not alpha ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'alpha', 'ge', 'second'])

    def test_not_alpha_before(self):
        w = where.Where('"house room" not alpha before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha', 'before', 'attic'])

    def test_not_alpha_after(self):
        w = where.Where('"house room" not alpha after "the ground floor"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha', 'after', 'the ground floor'])

    def test_not_alpha_from_to(self):
        w = where.Where('"house room" not alpha from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha',
             'from', 'the floor', 'to', 'the roof'])

    def test_not_alpha_from_below(self):
        w = where.Where(
            '"house room" not alpha from "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha',
             'from', 'the floor', 'below', 'the roof'])

    def test_not_alpha_above_to(self):
        w = where.Where(
            '"house room" not alpha above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha',
             'above', 'the floor', 'to', 'the roof'])

    def test_not_alpha_above_below(self):
        w = where.Where(
            '"house room" not alpha above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'alpha',
             'above', 'the floor', 'below', 'the roof'])

    def test_not_num_eq(self):
        w = where.Where('floor not num eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'num', 'eq', 'basement'])

    def test_not_num_ne(self):
        w = where.Where('floor not num ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'num', 'ne', 'first'])

    def test_not_num_gt(self):
        w = where.Where('floor not num gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'num', 'gt', 'ground'])

    def test_not_num_lt(self):
        w = where.Where('"houses in street" not num lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'not', 'num', 'lt', 'thirty three'])

    def test_not_num_le(self):
        w = where.Where('floor not num le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'num', 'le', 'third'])

    def test_not_num_ge(self):
        w = where.Where('floor not num ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'num', 'ge', 'second'])

    def test_not_num_before(self):
        w = where.Where('"house room" not num before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num', 'before', 'attic'])

    def test_not_num_after(self):
        w = where.Where('"house room" not num after "the ground floor"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num', 'after', 'the ground floor'])

    def test_not_num_from_to(self):
        w = where.Where('"house room" not num from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num',
             'from', 'the floor', 'to', 'the roof'])

    def test_not_num_from_below(self):
        w = where.Where(
            '"house room" not num from "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num',
             'from', 'the floor', 'below', 'the roof'])

    def test_not_num_above_to(self):
        w = where.Where('"house room" not num above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num',
             'above', 'the floor', 'to', 'the roof'])

    def test_not_num_above_below(self):
        w = where.Where('"house room" not num above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'num',
             'above', 'the floor', 'below', 'the roof'])

    def test_not_eq(self):
        w = where.Where('floor not eq basement')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'eq', 'basement'])

    def test_not_ne(self):
        w = where.Where('floor not ne first')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'ne', 'first'])

    def test_not_gt(self):
        w = where.Where('floor not gt ground')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'gt', 'ground'])

    def test_not_lt(self):
        w = where.Where('"houses in street" not lt "thirty three"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['houses in street', 'not', 'lt', 'thirty three'])

    def test_not_le(self):
        w = where.Where('floor not le third')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'le', 'third'])

    def test_not_ge(self):
        w = where.Where('floor not ge second')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['floor', 'not', 'ge', 'second'])

    def test_not_before(self):
        w = where.Where('"house room" not before attic')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'before', 'attic'])

    def test_not_after(self):
        w = where.Where('"house room" not after "the ground floor"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'after', 'the ground floor'])

    def test_not_from_to(self):
        w = where.Where('"house room" not from "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'from', 'the floor', 'to', 'the roof'])

    def test_not_from_below(self):
        w = where.Where('"house room" not from "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'from', 'the floor', 'below',
             'the roof'])

    def test_not_above_to(self):
        w = where.Where('"house room" not above "the floor" to "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'above', 'the floor', 'to', 'the roof'])

    def test_not_above_below(self):
        w = where.Where('"house room" not above "the floor" below "the roof"')
        w.lex()
        self.assertEqual(
            w.tokens,
            ['house room', 'not', 'above', 'the floor', 'below',
             'the roof'])


class Where_parse_phraseTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__no_keywords(self):
        self._cwc(
            '"this statement has no keywords"',
            field='this statement has no keywords')

    def test__no_keywords_mixed(self):
        self._cwc(
            '"this Statement has no keYWOrds"',
            field='this Statement has no keYWOrds')

    def test_is_upper(self):
        self._cwc(
            'fieldname IS value',
            field='fieldname',
            condition='is',
            value='value')

    def test_non_keywords_mixed(self):
        self._cwc(
            'fieLdnAme is VALUE',
            field='fieLdnAme',
            condition='is',
            value='VALUE')

    def test_like_mixed(self):
        self._cwc(
            'fieldname lIKe pattern',
            field='fieldname',
            condition='like',
            value='pattern')

    def test_is(self):
        self._cwc(
            'fieldname is value',
            field='fieldname',
            condition='is',
            value='value')

    def test_like(self):
        self._cwc(
            'fieldname like pattern',
            field='fieldname',
            condition='like',
            value='pattern')

    def test_present(self):
        self._cwc(
            'fieldname present',
            field='fieldname',
            condition='present')

    def test_alpha_eq(self):
        self._cwc(
            'floor alpha eq basement',
            field='floor',
            condition='eq',
            value='basement',
            num=False,
            alpha=True)

    def test_alpha_ne(self):
        self._cwc(
            'floor alpha ne first',
            field='floor',
            condition='ne',
            value='first',
            num=False,
            alpha=True)

    def test_alpha_gt(self):
        self._cwc(
            'floor alpha gt ground',
            field='floor',
            condition='gt',
            value='ground',
            num=False,
            alpha=True)

    def test_alpha_lt(self):
        self._cwc(
            '"houses in street" alpha lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three',
            num=False,
            alpha=True)

    def test_alpha_le(self):
        self._cwc(
            'floor alpha le third',
            field='floor',
            condition='le',
            value='third',
            num=False,
            alpha=True)

    def test_alpha_ge(self):
        self._cwc(
            'floor alpha ge second',
            field='floor',
            condition='ge',
            value='second',
            num=False,
            alpha=True)

    def test_alpha_before(self):
        self._cwc(
            '"house room" alpha before attic',
            field='house room',
            condition='before',
            value='attic',
            num=False,
            alpha=True)

    def test_alpha_after(self):
        self._cwc(
            '"house room" alpha after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor',
            num=False,
            alpha=True)

    def test_alpha_from_to(self):
        self._cwc(
            '"house room" alpha from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'),
            num=False,
            alpha=True)

    def test_alpha_from_below(self):
        self._cwc(
            '"house room" alpha from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'),
            num=False,
            alpha=True)

    def test_alpha_above_to(self):
        self._cwc(
            '"house room" alpha above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'),
            num=False,
            alpha=True)

    def test_alpha_above_below(self):
        self._cwc(
            '"house room" alpha above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'),
            num=False,
            alpha=True)

    def test_num_eq(self):
        self._cwc(
            'floor num eq basement',
            field='floor',
            condition='eq',
            value='basement',
            num=True,
            alpha=False)

    def test_num_ne(self):
        self._cwc(
            'floor num ne first',
            field='floor',
            condition='ne',
            value='first',
            num=True,
            alpha=False)

    def test_num_gt(self):
        self._cwc(
            'floor num gt ground',
            field='floor',
            condition='gt',
            value='ground',
            num=True,
            alpha=False)

    def test_num_lt(self):
        self._cwc(
            '"houses in street" num lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three',
            num=True,
            alpha=False)

    def test_num_le(self):
        self._cwc(
            'floor num le third',
            field='floor',
            condition='le',
            value='third',
            num=True,
            alpha=False)

    def test_num_ge(self):
        self._cwc(
            'floor num ge second',
            field='floor',
            condition='ge',
            value='second',
            num=True,
            alpha=False)

    def test_num_before(self):
        self._cwc(
            '"house room" num before attic',
            field='house room',
            condition='before',
            value='attic',
            num=True,
            alpha=False)

    def test_num_after(self):
        self._cwc(
            '"house room" num after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor',
            num=True,
            alpha=False)

    def test_num_from_to(self):
        self._cwc(
            '"house room" num from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'),
            num=True,
            alpha=False)

    def test_num_from_below(self):
        self._cwc(
            '"house room" num from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'),
            num=True,
            alpha=False)

    def test_num_above_to(self):
        self._cwc(
            '"house room" num above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'),
            num=True,
            alpha=False)

    def test_num_above_below(self):
        self._cwc(
            '"house room" num above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'),
            num=True,
            alpha=False)

    def test_eq(self):
        self._cwc(
            'floor eq basement',
            field='floor',
            condition='eq',
            value='basement')

    def test_ne(self):
        self._cwc(
            'floor ne first',
            field='floor',
            condition='ne',
            value='first')

    def test_gt(self):
        self._cwc(
            'floor gt ground',
            field='floor',
            condition='gt',
            value='ground')

    def test_lt(self):
        self._cwc(
            '"houses in street" lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three')

    def test_le(self):
        self._cwc(
            'floor le third',
            field='floor',
            condition='le',
            value='third')

    def test_ge(self):
        self._cwc(
            'floor ge second',
            field='floor',
            condition='ge',
            value='second')

    def test_before(self):
        self._cwc(
            '"house room" before attic',
            field='house room',
            condition='before',
            value='attic')

    def test_after(self):
        self._cwc(
            '"house room" after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor')

    def test_from_to(self):
        self._cwc(
            '"house room" from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'))

    def test_from_below(self):
        self._cwc(
            '"house room" from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'))

    def test_above_to(self):
        self._cwc(
            '"house room" above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'))

    def test_above_below(self):
        self._cwc(
            '"house room" above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'))

    def test_is_not(self):
        self._cwc(
            'fieldname is not value',
            field='fieldname',
            condition='is',
            value='value',
            not_value=True)

    def test_not_like(self):
        self._cwc(
            'fieldname not like pattern',
            field='fieldname',
            condition='like',
            value='pattern',
            not_condition=True)

    def test_not_present(self):
        self._cwc(
            'fieldname not present',
            field='fieldname',
            condition='present',
            not_condition=True)

    def test_not_alpha_eq(self):
        self._cwc(
            'floor not alpha eq basement',
            field='floor',
            condition='eq',
            value='basement',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_ne(self):
        self._cwc(
            'floor not alpha ne first',
            field='floor',
            condition='ne',
            value='first',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_gt(self):
        self._cwc(
            'floor not alpha gt ground',
            field='floor',
            condition='gt',
            value='ground',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_lt(self):
        self._cwc(
            '"houses in street" not alpha lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_le(self):
        self._cwc(
            'floor not alpha le third',
            field='floor',
            condition='le',
            value='third',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_ge(self):
        self._cwc(
            'floor not alpha ge second',
            field='floor',
            condition='ge',
            value='second',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_before(self):
        self._cwc(
            '"house room" not alpha before attic',
            field='house room',
            condition='before',
            value='attic',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_after(self):
        self._cwc(
            '"house room" not alpha after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor',
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_from_to(self):
        self._cwc(
            '"house room" not alpha from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_from_below(self):
        self._cwc(
            '"house room" not alpha from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_above_to(self):
        self._cwc(
            '"house room" not alpha above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_alpha_above_below(self):
        self._cwc(
            '"house room" not alpha above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=False,
            alpha=True)

    def test_not_num_eq(self):
        self._cwc(
            'floor not num eq basement',
            field='floor',
            condition='eq',
            value='basement',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_ne(self):
        self._cwc(
            'floor not num ne first',
            field='floor',
            condition='ne',
            value='first',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_gt(self):
        self._cwc(
            'floor not num gt ground',
            field='floor',
            condition='gt',
            value='ground',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_lt(self):
        self._cwc(
            '"houses in street" not num lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_le(self):
        self._cwc(
            'floor not num le third',
            field='floor',
            condition='le',
            value='third',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_ge(self):
        self._cwc(
            'floor not num ge second',
            field='floor',
            condition='ge',
            value='second',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_before(self):
        self._cwc(
            '"house room" not num before attic',
            field='house room',
            condition='before',
            value='attic',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_after(self):
        self._cwc(
            '"house room" not num after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor',
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_from_to(self):
        self._cwc(
            '"house room" not num from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_from_below(self):
        self._cwc(
            '"house room" not num from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_above_to(self):
        self._cwc(
            '"house room" not num above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_num_above_below(self):
        self._cwc(
            '"house room" not num above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True,
            num=True,
            alpha=False)

    def test_not_eq(self):
        self._cwc(
            'floor not eq basement',
            field='floor',
            condition='eq',
            value='basement',
            not_condition=True)

    def test_not_ne(self):
        self._cwc(
            'floor not ne first',
            field='floor',
            condition='ne',
            value='first',
            not_condition=True)

    def test_not_gt(self):
        self._cwc(
            'floor not gt ground',
            field='floor',
            condition='gt',
            value='ground',
            not_condition=True)

    def test_not_lt(self):
        self._cwc(
            '"houses in street" not lt "thirty three"',
            field='houses in street',
            condition='lt',
            value='thirty three',
            not_condition=True)

    def test_not_le(self):
        self._cwc(
            'floor not le third',
            field='floor',
            condition='le',
            value='third',
            not_condition=True)

    def test_not_ge(self):
        self._cwc(
            'floor not ge second',
            field='floor',
            condition='ge',
            value='second',
            not_condition=True)

    def test_not_before(self):
        self._cwc(
            '"house room" not before attic',
            field='house room',
            condition='before',
            value='attic',
            not_condition=True)

    def test_not_after(self):
        self._cwc(
            '"house room" not after "the ground floor"',
            field='house room',
            condition='after',
            value='the ground floor',
            not_condition=True)

    def test_not_from_to(self):
        self._cwc(
            '"house room" not from "the floor" to "the roof"',
            field='house room',
            condition=('from', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True)

    def test_not_from_below(self):
        self._cwc(
            '"house room" not from "the floor" below "the roof"',
            field='house room',
            condition=('from', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True)

    def test_not_above_to(self):
        self._cwc(
            '"house room" not above "the floor" to "the roof"',
            field='house room',
            condition=('above', 'to'),
            value=('the floor', 'the roof'),
            not_condition=True)

    def test_not_above_below(self):
        self._cwc(
            '"house room" not above "the floor" below "the roof"',
            field='house room',
            condition=('above', 'below'),
            value=('the floor', 'the roof'),
            not_condition=True)

    def _cwc(
        self,
        query,
        left=None,
        right=None,
        up=None,
        down=None,
        operator=None,
        field=None,
        condition=None,
        value=None,
        not_phrase=None,
        not_condition=None,
        not_value=None,
        num=None,
        alpha=None,
        error_token_offset=None):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        wc = where_clause.node.down
        ae(wc.left, left)
        ae(wc.right, right)
        ae(wc.up, wc.get_root())
        ae(wc.down, down)
        ae(wc.operator, operator)
        ae(wc.field, field)
        ae(wc.condition, condition)
        ae(wc.value, value)
        ae(wc.not_phrase, not_phrase)
        ae(wc.not_condition, not_condition)
        ae(wc.not_value, not_value)
        ae(wc.num, num)
        ae(wc.alpha, alpha)
        self.assertIsInstance(
            where_clause._error_information, where.WhereStatementError)
        self.assertNotEqual(where_clause._error_information._statement, '')
        ae(where_clause._error_information._tokens, None)
        ae(where_clause._error_information._fields, None)


class Where_parse_and_nor_or_phraseTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_and_and_and_phrases_01(self):
        self._cwc(
            'f is p and f is q and f is r and f is s',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='and',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='and',
                  field='f', condition='is', value='r'),
             dict(left=3, up=0, operator='and',
                  field='f', condition='is', value='s'),
             ])

    def test_and_and_and_phrases_02(self):
        self._cwc(
            'f is p and q and r and s',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='and',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='and',
                  field='f', condition='is', value='r'),
             dict(left=3, up=0, operator='and',
                  field='f', condition='is', value='s'),
             ])

    def test_or_or_or_phrases_01(self):
        self._cwc(
            'f is p or f is q or f is r or f is s',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='or',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='or',
                  field='f', condition='is', value='r'),
             dict(left=3, up=0, operator='or',
                  field='f', condition='is', value='s'),
             ])

    def test_or_or_or_phrases_02(self):
        self._cwc(
            'f is p or q or r or s',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='or',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='or',
                  field='f', condition='is', value='r'),
             dict(left=3, up=0, operator='or',
                  field='f', condition='is', value='s'),
             ])

    def test_and_or_and_phrases_01(self):
        self._cwc(
            
                'f is p and f is q or f is r and f is s and f is t or f is u',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='and',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='or',
                  field='f', condition='is', value='r'),
             dict(left=3, right=5, up=0, operator='and',
                  field='f', condition='is', value='s'),
             dict(left=4, right=6, up=0, operator='and',
                  field='f', condition='is', value='t'),
             dict(left=5, up=0, operator='or',
                  field='f', condition='is', value='u'),
             ])

    def test_and_or_and_phrases_02(self):
        self._cwc(
            'f is p and q or r and s and t',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='and',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='or',
                  field='f', condition='is', value='r'),
             dict(left=3, right=5, up=0, operator='and',
                  field='f', condition='is', value='s'),
             dict(left=4, up=0, operator='and',
                  field='f', condition='is', value='t'),
             ])

    def test_and_or_and_phrases_03(self):
        self._cwc(
            'f is p and f is q or f is r and f is s',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='and',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='or',
                  field='f', condition='is', value='r'),
             dict(left=3, up=0, operator='and',
                  field='f', condition='is', value='s'),
             ])

    def test_and_or_and_parentheses_phrases(self):
        self._cwc(
            '(f is p and f is q) or (f is r and f is s)',
            [dict(down=1),
             dict(down=2, right=4, up=0, parentheses=False),
             dict(right=3, up=1,
                  field='f', condition='is', value='p'),
             dict(left=2, up=1, operator='and',
                  field='f', condition='is', value='q'),
             dict(down=5, left=1, up=0, operator='or', parentheses=False),
             dict(right=6, up=4,
                  field='f', condition='is', value='r'),
             dict(left=5, up=4, operator='and',
                  field='f', condition='is', value='s'),
             ])

    def test_or_and_or_phrases_01(self):
        self._cwc(
            'f is p or f is q and f is r or f is s or f is t',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='or',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='and',
                  field='f', condition='is', value='r'),
             dict(left=3, right=5, up=0, operator='or',
                  field='f', condition='is', value='s'),
             dict(left=4, up=0, operator='or',
                  field='f', condition='is', value='t'),
             ])

    def test_or_and_or_phrases_02(self):
        self._cwc(
            'f is p or q and r or s or t',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(left=1, right=3, up=0, operator='or',
                  field='f', condition='is', value='q'),
             dict(left=2, right=4, up=0, operator='and',
                  field='f', condition='is', value='r'),
             dict(left=3, right=5, up=0, operator='or',
                  field='f', condition='is', value='s'),
             dict(left=4, up=0, operator='or',
                  field='f', condition='is', value='t'),
             ])

    def test_or_and_or_parentheses_phrases(self):
        self._cwc(
            'f is p or ( f is q and f is r ) or f is s or f is t',
            [dict(down=1),
             dict(right=2, up=0,
                  field='f', condition='is', value='p'),
             dict(down=3, left=1, right=5, up=0, operator='or',
                  parentheses=False),
             dict(right=4, up=2,
                  field='f', condition='is', value='q'),
             dict(left=3, up=2, operator='and',
                  field='f', condition='is', value='r'),
             dict(left=2, right=6, up=0, operator='or',
                  field='f', condition='is', value='s'),
             dict(left=5, up=0, operator='or',
                  field='f', condition='is', value='t'),
             ])

    def _cwc(self, query, expected_clauses):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        wcw = where_clause.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}
        for e, c in enumerate(wcw):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            ae(wc.left, wci.get(c.left))
            ae(wc.right, wci.get(c.right))
            ae(wc.up, wci.get(c.up))
            ae(wc.down, wci.get(c.down))
            ae(wc.operator, c.operator)
            ae(wc.field, c.field)
            ae(wc.condition, c.condition)
            ae(wc.value, c.value)
            ae(wc.not_phrase, c.not_phrase)
            ae(wc.not_condition, c.not_condition)
            ae(wc.not_value, c.not_value)
            ae(wc.num, c.num)
            ae(wc.alpha, c.alpha)
            ae(len(wcw), len(expected_clauses))


class Where_parse_multi_phraseTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_two_phrases(self):
        self._cwc(
            'f is a or f is b',
            [dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='is', value='b', operator='or'),
             ])

    def test_all_five_simple_positive_phrases(self):
        self._cwc(
            ' '.join(('f is a or f like b or f present or',
                      'f eq c or f from d to e or f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='like', value='b', operator='or'),
             dict(field='f', condition='present', operator='or'),
             dict(field='f', condition='eq', value='c', operator='or'),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'), operator='or'),
             dict(field='f', condition='is', value='f', operator='or'),
             ])

    def test_all_five_simple_negative_phrases(self):
        self._cwc(
            ' '.join(('f is not a or f not like b or f not present or',
                      'f not eq c or f not from d to e or f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a', not_value=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True),
             ])

    def test_all_five_simple_positive_num_phrases(self):
        self._cwc(
            ' '.join(('f is a or f like b or f present or',
                      'f num eq c or f num from d to e or f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='like', value='b', operator='or'),
             dict(field='f', condition='present', operator='or'),
             dict(field='f', condition='eq', value='c', operator='or',
                  num=True, alpha=False),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', num=True, alpha=False),
             dict(field='f', condition='is', value='f', operator='or'),
             ])

    def test_all_five_simple_negative_num_phrases(self):
        self._cwc(
            ' '.join(('f is not a or f not like b or f not present or',
                      'f not num eq c or f not num from d to e or',
                      'f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a', not_value=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True, num=True, alpha=False),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True, num=True, alpha=False),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True),
             ])

    def test_all_five_simple_positive_alpha_phrases(self):
        self._cwc(
            ' '.join(('f is a or f like b or f present or',
                      'f alpha eq c or f alpha from d to e or f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='like', value='b', operator='or'),
             dict(field='f', condition='present', operator='or'),
             dict(field='f', condition='eq', value='c', operator='or',
                  num=False, alpha=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', num=False, alpha=True),
             dict(field='f', condition='is', value='f', operator='or'),
             ])

    def test_all_five_simple_negative_alpha_phrases(self):
        self._cwc(
            ' '.join(('f is not a or f not like b or f not present or',
                      'f not alpha eq c or f not alpha from d to e or',
                      'f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a', not_value=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True, num=False, alpha=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True, num=False, alpha=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True),
             ])

    def test_all_three_simple_boolean_phrases(self):
        self._cwc(
            ' '.join(('f is a or f like b or f present and',
                      'f eq c and f from d to e nor f is f nor f le g',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='like', value='b', operator='or'),
             dict(field='f', condition='present', operator='or'),
             dict(field='f', condition='eq', value='c', operator='and'),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'), operator='and'),
             dict(field='f', condition='is', value='f', operator='nor'),
             dict(field='f', condition='le', value='g', operator='nor'),
             ])

    def _cwc(self, query, expected_clauses):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        for e, c in enumerate(
            where_clause.node.get_clauses_from_root_in_walk_order()):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            ae(wc.operator, c.operator)
            ae(wc.field, c.field)
            ae(wc.condition, c.condition)
            ae(wc.value, c.value)
            ae(wc.not_phrase, c.not_phrase)
            ae(wc.not_condition, c.not_condition)
            ae(wc.not_value, c.not_value)
            ae(wc.num, c.num)
            ae(wc.alpha, c.alpha)


class Where_parse_not_phrasesTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_not_phrase_is(self):
        self._cwc(
            'not f is a',
            [dict(),
             dict(field='f', condition='is', value='a', not_phrase=True),
             ])

    def test_not_phrase_is_not(self):
        self._cwc(
            'not f is not a',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True, not_value=True),
             ])

    def test_not_phrase_like(self):
        self._cwc(
            'not f like b',
            [dict(),
             dict(field='f', condition='like', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_like(self):
        self._cwc(
            'not f not like b',
            [dict(),
             dict(field='f', condition='like', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_present(self):
        self._cwc(
            'not f present',
            [dict(),
             dict(field='f', condition='present',
                  not_phrase=True),
             ])

    def test_not_phrase_not_present(self):
        self._cwc(
            'not f not present',
            [dict(),
             dict(field='f', condition='present',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_eq(self):
        self._cwc(
            'not f eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_eq(self):
        self._cwc(
            'not f not eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_eq(self):
        self._cwc(
            'not f num eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_eq(self):
        self._cwc(
            'not f not num eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_eq(self):
        self._cwc(
            'not f alpha eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_eq(self):
        self._cwc(
            'not f not alpha eq b',
            [dict(),
             dict(field='f', condition='eq', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_ne(self):
        self._cwc(
            'not f ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_ne(self):
        self._cwc(
            'not f not ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_ne(self):
        self._cwc(
            'not f num ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_ne(self):
        self._cwc(
            'not f not num ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_ne(self):
        self._cwc(
            'not f alpha ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_ne(self):
        self._cwc(
            'not f not alpha ne b',
            [dict(),
             dict(field='f', condition='ne', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_gt(self):
        self._cwc(
            'not f gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_gt(self):
        self._cwc(
            'not f not gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_gt(self):
        self._cwc(
            'not f num gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_gt(self):
        self._cwc(
            'not f not num gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_gt(self):
        self._cwc(
            'not f alpha gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_gt(self):
        self._cwc(
            'not f not alpha gt b',
            [dict(),
             dict(field='f', condition='gt', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_lt(self):
        self._cwc(
            'not f lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_lt(self):
        self._cwc(
            'not f not lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_lt(self):
        self._cwc(
            'not f num lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_lt(self):
        self._cwc(
            'not f not num lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_lt(self):
        self._cwc(
            'not f alpha lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_lt(self):
        self._cwc(
            'not f not alpha lt b',
            [dict(),
             dict(field='f', condition='lt', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_le(self):
        self._cwc(
            'not f le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_le(self):
        self._cwc(
            'not f not le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_le(self):
        self._cwc(
            'not f num le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_le(self):
        self._cwc(
            'not f not num le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_le(self):
        self._cwc(
            'not f alpha le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_le(self):
        self._cwc(
            'not f not alpha le b',
            [dict(),
             dict(field='f', condition='le', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_ge(self):
        self._cwc(
            'not f ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_ge(self):
        self._cwc(
            'not f not ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_ge(self):
        self._cwc(
            'not f num ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_ge(self):
        self._cwc(
            'not f not num ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_ge(self):
        self._cwc(
            'not f alpha ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_ge(self):
        self._cwc(
            'not f not alpha ge b',
            [dict(),
             dict(field='f', condition='ge', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_before(self):
        self._cwc(
            'not f before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_before(self):
        self._cwc(
            'not f not before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_before(self):
        self._cwc(
            'not f num before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_before(self):
        self._cwc(
            'not f not num before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_before(self):
        self._cwc(
            'not f alpha before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_before(self):
        self._cwc(
            'not f not alpha before b',
            [dict(),
             dict(field='f', condition='before', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_after(self):
        self._cwc(
            'not f after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True),
             ])

    def test_not_phrase_not_after(self):
        self._cwc(
            'not f not after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_after(self):
        self._cwc(
            'not f num after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_after(self):
        self._cwc(
            'not f not num after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_after(self):
        self._cwc(
            'not f alpha after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_after(self):
        self._cwc(
            'not f not alpha after b',
            [dict(),
             dict(field='f', condition='after', value='b',
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_above_below(self):
        self._cwc(
            'not f above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True),
             ])

    def test_not_phrase_not_above_below(self):
        self._cwc(
            'not f not above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_above_below(self):
        self._cwc(
            'not f num above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_above_below(self):
        self._cwc(
            'not f not num above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_above_below(self):
        self._cwc(
            'not f alpha above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_above_below(self):
        self._cwc(
            'not f not alpha above c below d',
            [dict(),
             dict(field='f', condition=('above', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_above_to(self):
        self._cwc(
            'not f above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True),
             ])

    def test_not_phrase_not_above_to(self):
        self._cwc(
            'not f not above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_above_to(self):
        self._cwc(
            'not f num above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_above_to(self):
        self._cwc(
            'not f not num above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_above_to(self):
        self._cwc(
            'not f alpha above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_above_to(self):
        self._cwc(
            'not f not alpha above c to d',
            [dict(),
             dict(field='f', condition=('above', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_from_below(self):
        self._cwc(
            'not f from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True),
             ])

    def test_not_phrase_not_from_below(self):
        self._cwc(
            'not f not from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_from_below(self):
        self._cwc(
            'not f num from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_from_below(self):
        self._cwc(
            'not f not num from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_from_below(self):
        self._cwc(
            'not f alpha from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_from_below(self):
        self._cwc(
            'not f not alpha from c below d',
            [dict(),
             dict(field='f', condition=('from', 'below'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def test_not_phrase_from_to(self):
        self._cwc(
            'not f from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True),
             ])

    def test_not_phrase_not_from_to(self):
        self._cwc(
            'not f not from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True),
             ])

    def test_not_phrase_num_from_to(self):
        self._cwc(
            'not f num from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True, num=True, alpha=False),
             ])

    def test_not_phrase_not_num_from_to(self):
        self._cwc(
            'not f not num from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=True, alpha=False),
             ])

    def test_not_phrase_alpha_from_to(self):
        self._cwc(
            'not f alpha from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True, num=False, alpha=True),
             ])

    def test_not_phrase_not_alpha_from_to(self):
        self._cwc(
            'not f not alpha from c to d',
            [dict(),
             dict(field='f', condition=('from', 'to'), value=('c', 'd'),
                  not_phrase=True, not_condition=True, num=False, alpha=True),
             ])

    def _cwc(self, query, expected_clauses):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        for e, c in enumerate(
            where_clause.node.get_clauses_from_root_in_walk_order()):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            ae(wc.operator, c.operator)
            ae(wc.field, c.field)
            ae(wc.condition, c.condition)
            ae(wc.value, c.value)
            ae(wc.not_phrase, c.not_phrase)
            ae(wc.not_condition, c.not_condition)
            ae(wc.not_value, c.not_value)
            ae(wc.num, c.num)
            ae(wc.alpha, c.alpha)


class Where_parse_multi_not_phrasesTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_not_first_of_two_phrases(self):
        self._cwc(
            'not f is a and f is b',
            [dict(),
             dict(field='f', condition='is', value='a', not_phrase=True),
             dict(field='f', condition='is', value='b', operator='and'),
             ])

    def test_multiuse_not_first_of_two_phrases(self):
        self._cwc(
            'not f is not a and f is b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True, not_value=True),
             dict(field='f', condition='is', value='b', operator='and'),
             ])
        self._cwc(
            'not f is not a and f is not b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True, not_value=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_value=True),
             ])
        self._cwc(
            'not f is a and f is not b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_value=True),
             ])

    def test_not_both_of_two_phrases(self):
        self._cwc(
            'not f is a and not f is b',
            [dict(),
             dict(field='f', condition='is', value='a', not_phrase=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_phrase=True),
             ])

    def test_multiuse_not_both_of_two_phrases(self):
        self._cwc(
            'not f is not a and not f is b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True, not_value=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_phrase=True),
             ])
        self._cwc(
            'not f is not a and not f is not b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True, not_value=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_phrase=True, not_value=True),
             ])
        self._cwc(
            'not f is a and not f is not b',
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='is', value='b', operator='and',
                  not_phrase=True, not_value=True),
             ])

    def test_all_five_simple_positive_not_phrases(self):
        self._cwc(
            ' '.join(('not f is a or not f like b or not f present or',
                      'not f eq c or not f from d to e or not f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_phrase=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_phrase=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_phrase=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_phrase=True),
             ])

    def test_all_five_simple_negative_not_phrases(self):
        self._cwc(
            ' '.join(('not f is not a or not f not like b or',
                      'not f not present or not f not eq c or',
                      'not f not from d to e or not f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_value=True, not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True, not_phrase=True),
             ])

    def test_all_five_simple_positive_num_not_phrases(self):
        self._cwc(
            ' '.join(('not f is a or not f like b or',
                      'not f present or not f num eq c or',
                      'not f num from d to e or not f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_phrase=True),
             dict(field='f', condition='eq', value='c', operator='or',
                  num=True, alpha=False, not_phrase=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', num=True, alpha=False, not_phrase=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_phrase=True),
             ])

    def test_all_five_simple_negative_num_not_phrases(self):
        self._cwc(
            ' '.join(('not f is not a or not f not like b or',
                      'not f not present or not f not num eq c or',
                      'not f not num from d to e or not f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_value=True, not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True, not_phrase=True,
                  num=True, alpha=False),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True, not_phrase=True,
                  num=True, alpha=False),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True, not_phrase=True),
             ])

    def test_all_five_simple_positive_alpha_not_phrases(self):
        self._cwc(
            ' '.join(('not f is a or not f like b or not f present or',
                      'not f alpha eq c or not f alpha from d to e or',
                      'not f is f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_phrase=True),
             dict(field='f', condition='eq', value='c', operator='or',
                  num=False, alpha=True, not_phrase=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', num=False, alpha=True, not_phrase=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_phrase=True),
             ])

    def test_all_five_simple_negative_alpha_not_phrases(self):
        self._cwc(
            ' '.join(('not f is not a or not f not like b or',
                      'not f not present or not f not alpha eq c or',
                      'not f not alpha from d to e or not f is not f',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_value=True, not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_condition=True, not_phrase=True),
             dict(field='f', condition='eq', value='c',
                  operator='or', not_condition=True, not_phrase=True,
                  num=False, alpha=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='or', not_condition=True, not_phrase=True,
                  num=False, alpha=True),
             dict(field='f', condition='is', value='f',
                  operator='or', not_value=True, not_phrase=True),
             ])

    def test_all_three_simple_boolean_not_phrases(self):
        self._cwc(
            ' '.join(('not f is a or not f like b or not f present and',
                      'not f eq c and not f from d to e nor',
                      'not f is f nor not f le g',
                      )),
            [dict(),
             dict(field='f', condition='is', value='a',
                  not_phrase=True),
             dict(field='f', condition='like', value='b',
                  operator='or', not_phrase=True),
             dict(field='f', condition='present',
                  operator='or', not_phrase=True),
             dict(field='f', condition='eq', value='c',
                  operator='and', not_phrase=True),
             dict(field='f',
                  condition=('from', 'to'), value=('d', 'e'),
                  operator='and', not_phrase=True),
             dict(field='f', condition='is', value='f',
                  operator='nor', not_phrase=True),
             dict(field='f', condition='le', value='g',
                  operator='nor', not_phrase=True),
             ])

    def _cwc(self, query, expected_clauses):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        for e, c in enumerate(
            where_clause.node.get_clauses_from_root_in_walk_order()):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
            except AssertionError:
                #print(e)
                #print(c.__dict__)
                raise


class Where_parse_parenthesis_phrasesTC(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parenthesis_is(self):
        self._cwc(
            '(f is a)',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])
        self._cwc(
            '( f is a)',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])
        self._cwc(
            '( f is a )',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])
        self._cwc(
            '(f is a )',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])
        self._cwc(
            ' ( f is a ) ',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])

    def test_parenthesis_is_two(self):
        node = self._cwc(
            '(f is a or f is b) and (g is c or g is d)',
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='is', value='b', operator='or'),
             dict(operator='and'),
             dict(field='g', condition='is', value='c'),
             dict(field='g', condition='is', value='d', operator='or'),
             ])
        for e, n in enumerate(
            [dict(down=node[1]),
             dict(right=node[4], down=node[2], up=node[0]),
             dict(right=node[3], up=node[1]),
             dict(left=node[2], up=node[1]),
             dict(left=node[1], down=node[5], up=node[0]),
             dict(right=node[6], up=node[4]),
             dict(left=node[5], up=node[4]),
             ]):
            self._cws(node[e], n)

    def test_parenthesis_is_three(self):
        node = self._cwc(
            ' '.join(
                ('(f is a or f is b) and',
                 '(g is c or g is d) and',
                 '(h is e or h is i)')),
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             dict(field='f', condition='is', value='b', operator='or'),
             dict(operator='and'),
             dict(field='g', condition='is', value='c'),
             dict(field='g', condition='is', value='d', operator='or'),
             dict(operator='and'),
             dict(field='h', condition='is', value='e'),
             dict(field='h', condition='is', value='i', operator='or'),
             ])
        for e, n in enumerate(
            [dict(down=node[1]),
             dict(right=node[4], down=node[2], up=node[0]),
             dict(right=node[3], up=node[1]),
             dict(left=node[2], up=node[1]),
             dict(left=node[1], right=node[7], down=node[5], up=node[0]),
             dict(right=node[6], up=node[4]),
             dict(left=node[5], up=node[4]),
             dict(left=node[4], down=node[8], up=node[0]),
             dict(right=node[9], up=node[7]),
             dict(left=node[8], up=node[7]),
             ]):
            self._cws(node[e], n)

    def test_parenthesis_is_three_nested(self):
        node = self._cwc(
            ' '.join(
                ('(f is a nor (w gt y or x lt z) or f is b) and',
                 '(g is c nor (not w gt y nor x lt z) or g is d) and',
                 '(h is e nor (w not gt y or not x lt z) or p is q)')),
            [dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             dict(operator='nor'),
             dict(field='w', condition='gt', value='y'),
             dict(field='x', condition='lt', value='z', operator='or'),
             dict(field='f', condition='is', value='b', operator='or'),
             dict(operator='and'),
             dict(field='g', condition='is', value='c'),
             dict(operator='nor'),
             dict(field='w', condition='gt', value='y', not_phrase=True),
             dict(field='x', condition='lt', value='z', operator='nor'),
             dict(field='g', condition='is', value='d', operator='or'),
             dict(operator='and'),
             dict(field='h', condition='is', value='e'),
             dict(operator='nor'),
             dict(field='w', condition='gt', value='y', not_condition=True),
             dict(field='x', condition='lt', value='z', operator='or',
                  not_phrase=True),
             dict(field='p', condition='is', value='q', operator='or'),
             ])
        for e, n in enumerate(
            [dict(down=node[1]),
             dict(right=node[7], down=node[2], up=node[0]),
             dict(right=node[3], up=node[1]),
             dict(left=node[2], right=node[6], up=node[1], down=node[4]),
             dict(right=node[5], up=node[3]),
             dict(left=node[4], up=node[3]),
             dict(left=node[3], up=node[1]),
             dict(left=node[1], right=node[13], down=node[8], up=node[0]),
             dict(right=node[9], up=node[7]),
             dict(left=node[8], right=node[12], up=node[7], down=node[10]),
             dict(right=node[11], up=node[9]),
             dict(left=node[10], up=node[9]),
             dict(left=node[9], up=node[7]),
             dict(left=node[7], down=node[14], up=node[0]),
             dict(right=node[15], up=node[13]),
             dict(left=node[14], right=node[18], up=node[13],
                  down=node[16]),
             dict(right=node[17], up=node[15]),
             dict(left=node[16], up=node[15]),
             dict(left=node[15], up=node[13]),
             ]):
            self._cws(node[e], n)

    def test_parenthesis_like(self):
        self._cwc(
            '(f like a)',
            [dict(),
             dict(),
             dict(field='f', condition='like', value='a'),
             ])

    def test_parenthesis_present(self):
        self._cwc(
            '(f present)',
            [dict(),
             dict(),
             dict(field='f', condition='present'),
             ])

    def test_parenthesis_after(self):
        self._cwc(
            '(f after a)',
            [dict(),
             dict(),
             dict(field='f', condition='after', value='a'),
             ])

    def test_parenthesis_below(self):
        self._cwc(
            '(f from a below b)',
            [dict(),
             dict(),
             dict(field='f', condition=('from', 'below'), value=('a', 'b')),
             ])

    def test_nested_parenthesis(self):
        self._cwc(
            '((f is a))',
            [dict(),
             dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])
        self._cwc(
            '(((f is a)))',
            [dict(),
             dict(),
             dict(),
             dict(),
             dict(field='f', condition='is', value='a'),
             ])

    def _cwc(self, query, expected_clauses):
        where_clause = where.Where(query)
        where_clause.lex()
        where_clause.parse()
        ae = self.assertEqual
        node = where_clause.node.get_clauses_from_root_in_walk_order()
        for e, c in enumerate(node):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            ae(wc.operator, c.operator)
            ae(wc.field, c.field)
            ae(wc.condition, c.condition)
            ae(wc.value, c.value)
            ae(wc.not_phrase, c.not_phrase)
            ae(wc.not_condition, c.not_condition)
            ae(wc.not_value, c.not_value)
            ae(wc.num, c.num)
            ae(wc.alpha, c.alpha)
        return node

    def _cws(self, where_phrase, expected_navigation):
        wc = where.WhereClause()
        wc.__dict__.update(expected_navigation)
        ae = self.assertEqual
        ae(where_phrase.left, wc.left)
        ae(where_phrase.right, wc.right)
        ae(where_phrase.up, wc.up)
        ae(where_phrase.down, wc.down)


class Where_validateTC(unittest.TestCase):

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [])

    def tearDown(self):
        pass

    def _validate(self, query, error_token_offset=None):
        w = where.Where(query)
        w.lex()
        w.parse()
        #self.assertEqual(w._error_information, error_token_offset)
        return w.validate(self.processors.database, 'file')

    def _ocn(self, query, expected_clauses, expected_result):
        w = where.Where(query)
        ae = self.assertEqual
        w.lex()
        w.parse()
        ae(w.validate(self.processors.database, self.processors.filename), None)
        w._processors = self.processors
        #print()
        #for e, c in enumerate(w.node.get_clauses_from_root_in_walk_order()):
        #    print(e, c)
        #    #print(c.__dict__)
        #print()
        wcw = w.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}
        for e, c in enumerate(wcw):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.left, wci.get(c.left))
                ae(wc.right, wci.get(c.right))
                ae(wc.up, wci.get(c.up))
                ae(wc.down, wci.get(c.down))
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
                ae(len(wcw), len(expected_clauses))
            except AssertionError:
                print(e)
                print(c.__dict__)
                raise
        ae(expected_result, w.node.result)

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [])
        self.assertEqual(self.processors.existence, set(range(0)))
        self.assertEqual(self.processors.get_existence(), set(range(0)))

    def test_00_validate_01(self):
        self.assertEqual(
            self._validate('f5 eq as and f5 like at or f2 is d'), None)
        self.assertIsInstance(
            self._validate('f5 ex d'), where.WhereStatementError)#['f5 ex d'])
        self.assertIsInstance(
            self._validate('f5 ex as and f5 like at or f2 is d', 1),
            where.WhereStatementError)#['f5 ex as'])
        self.assertIsInstance(
            self._validate('f5 eq as ant f5 like at or f2 is d', 3),
            where.WhereStatementError)#['f5', 'eq', 'as ant f5'])
        self.assertEqual(
            self._validate('f5 eq as and (f2 is d or f5 like at)'), None)
        self.assertEqual(
            self._validate('f5 eq as if and (f2 is d or f5 like at)'), None)
        self.assertEqual(
            self._validate(''), None)
        self.assertEqual(
            self._validate(' '), None)
        self.assertIsInstance(
            self._validate('a'), where.WhereStatementError)#['a'])
        self.assertEqual(
            self._validate('f5 eq b or a'), None)
        self.assertEqual(
            self._validate('f5 from a to z'), None)
        self.assertEqual(
            self._validate('f5 not from a to z'), None)

    def test_00_validate_02(self):
        self.assertIsInstance(
            self._validate('undefined field eq b or a'),
            where.WhereStatementError)#['undefined field'])

    def test_01_field_not_in_file(self):
        #self.assertRaisesRegex(
        #    AssertionError,
        #    "Lists differ: \['g'\] != \[\]",
        #    self._ocn,
        #    *(('g eq v', None, None)))
        #self._ocn('g eq v', None, None)
        w = where.Where('g eq v')
        ae = self.assertEqual
        w.lex()
        w.parse()
        self.assertIsInstance(
            w.validate(self.processors.database, self.processors.filename),
            where.WhereStatementError)

    def test_02_index_field_in_file(self):
        self._ocn('f1 eq v',
                  [dict(down=1),
                   dict(up=0,
                        field='f1', condition='eq', value='v'),
                   ],
                  None)

    def test_03_non_index_field_in_file(self):
        self._ocn('f eq v',
                  [dict(down=1),
                   dict(up=0,
                        field='f', condition='eq', value='v'),
                   ],
                  None)

    def test_04_and_index_condition_order(self):
        self._ocn('f1 eq v and f2 eq w and f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_05_and_index_condition_order_abbrev(self):
        self._ocn('f4 eq v and f4 eq w and f4 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v and w and gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)

    def test_06_and_non_index_condition_order(self):
        self._ocn('f1 like v and f2 like w and f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f3', condition='like', value='x'),
                   ],
                  None)

    def test_07_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 like v and f4 like w and f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 like v and w and x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='like', value='x'),
                   ],
                  None)

    def test_08_and_index_and_non_index_condition_order(self):
        self._ocn('f1 eq v and f2 eq w and f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f3', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f1 eq v and f2 like w and f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f1 like v and f2 eq w and f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_09_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v and f4 eq w and f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v and w and like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v and like w and eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v and eq w and x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v and w and eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)

    def test_10_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v and f4 like w and f4 eq x and f4 eq y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='and',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='and',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)
        self._ocn('f4 eq v and like w and eq x and y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='and',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='and',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)

    def test_11_or_index_condition_order(self):
        self._ocn('f1 eq v or f2 eq w or f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_12_or_index_condition_order_abbrev(self):
        self._ocn('f4 eq v or f4 eq w or f4 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v or w or gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)

    def test_13_or_non_index_condition_order(self):
        self._ocn('f1 like v or f2 like w or f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f3', condition='like', value='x'),
                   ],
                  None)

    def test_14_or_non_index_condition_order_abbrev(self):
        self._ocn('f4 like v or f4 like w or f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 like v or w or x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='like', value='x'),
                   ],
                  None)

    def test_15_or_index_and_non_index_condition_order(self):
        self._ocn('f1 eq v or f2 eq w or f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f3', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f1 eq v or f2 like w or f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f1 like v or f2 eq w or f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_16_or_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v or f4 eq w or f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v or w or like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v or like w or eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v or eq w or x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v or w or eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='or',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)

    def test_17_or_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v or f4 like w or f4 eq x or f4 eq y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='or',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='or',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)
        self._ocn('f4 eq v or like w or eq x or y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='or',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='or',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='or',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)

    def test_18_nor_index_condition_order(self):
        self._ocn('f1 eq v nor f2 eq w nor f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_19_nor_index_condition_order_abbrev(self):
        self._ocn('f4 eq v nor f4 eq w nor f4 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor w nor gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='gt', value='x'),
                   ],
                  None)

    def test_20_nor_non_index_condition_order(self):
        self._ocn('f1 like v nor f2 like w nor f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f3', condition='like', value='x'),
                   ],
                  None)

    def test_21_nor_non_index_condition_order_abbrev(self):
        self._ocn('f4 like v nor f4 like w nor f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 like v nor w nor x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='like', value='x'),
                   ],
                  None)

    def test_22_nor_index_and_non_index_condition_order(self):
        self._ocn('f1 eq v nor f2 eq w nor f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f3', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f1 eq v nor f2 like w nor f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f2', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f1 like v nor f2 eq w nor f3 gt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f1', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f2', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f3', condition='gt', value='x'),
                   ],
                  None)

    def test_23_nor_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v nor f4 eq w nor f4 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor w nor like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='like', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor like w nor eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v nor eq w nor x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='eq', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)
        self._ocn('f4 like v nor w nor eq x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='like', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, left=2, operator='nor',
                        field='f4', condition='eq', value='x'),
                   ],
                  None)

    def test_24_nor_index_and_non_index_condition_order_abbrev(self):
        self._ocn('f4 eq v nor f4 like w nor f4 eq x nor f4 eq y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='nor',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='nor',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)
        self._ocn('f4 eq v nor like w nor eq x nor y',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='like', value='w'),
                   dict(up=0, right=4, left=2, operator='nor',
                        field='f4', condition='eq', value='x'),
                   dict(up=0, left=3, operator='nor',
                        field='f4', condition='eq', value='y'),
                   ],
                  None)

    def test_25_not_nor_and_index_condition_order_abbrev(self):
        self._ocn('not f4 eq v nor f4 gt w and f4 lt x',
                  [dict(down=1),
                   dict(up=0, right=2, not_phrase=True,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='lt', value='x'),
                   ],
                  None)

        # not is propagated right, but perhaps nor by itself should stop this.
        self._ocn('not f4 eq v nor gt w and lt x',
                  [dict(down=1),
                   dict(up=0, right=2, not_phrase=True,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor', not_phrase=True,
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_phrase=True,
                        field='f4', condition='lt', value='x'),
                   ],
                  None)
        self._ocn('not f4 eq v nor f4 gt w and lt x',
                  [dict(down=1),
                   dict(up=0, right=2, not_phrase=True,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and',
                        field='f4', condition='lt', value='x'),
                   ],
                  None)

        self._ocn('f4 eq v nor gt w and f4 not lt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_condition=True,
                        field='f4', condition='lt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor gt w and not f4 lt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_phrase=True,
                        field='f4', condition='lt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor gt w and not x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_condition=True,
                        field='f4', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v and gt w and not x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_condition=True,
                        field='f4', condition='gt', value='x'),
                   ],
                  None)
        self._ocn('f4 eq v nor gt w and not lt x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f4', condition='eq', value='v'),
                   dict(up=0, right=3, left=1, operator='nor',
                        field='f4', condition='gt', value='w'),
                   dict(up=0, left=2, operator='and', not_condition=True,
                        field='f4', condition='lt', value='x'),
                   ],
                  None)

    def test_26_down_or_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn(
            ''.join(('f3 le a or ( ',
                     'f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     ') or f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, left=1, right=7, operator='or'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='or',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('f3 le a or ( ',
                     'f4 eq v and like w and eq x and y',
                     ') or f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, left=1, right=7, operator='or'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='or',
                  field='f2', condition='gt', value='b'),
             ],
            None)

    def test_27_split_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn(
            ''.join(('f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     'or f2 gt b and ',
                     'f3 eq f and f3 like g and f3 eq h')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f4', condition='eq', value='v'),
             dict(up=0, right=3, left=1, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=0, right=4, left=2, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=0, right=5, left=3, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, right=6, left=4, operator='or',
                  field='f2', condition='gt', value='b'),
             dict(up=0, right=7, left=5, operator='and',
                  field='f3', condition='eq', value='f'),
             dict(up=0, right=8, left=6, operator='and',
                  field='f3', condition='like', value='g'),
             dict(up=0, left=7, operator='and',
                  field='f3', condition='eq', value='h'),
             ],
            None)
        self._ocn(
            ''.join(('f4 eq v and like w and eq x and y ',
                     'or f2 gt b and ',
                     'f3 eq f and like g and eq h')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f4', condition='eq', value='v'),
             dict(up=0, right=3, left=1, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=0, right=4, left=2, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=0, right=5, left=3, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, right=6, left=4, operator='or',
                  field='f2', condition='gt', value='b'),
             dict(up=0, right=7, left=5, operator='and',
                  field='f3', condition='eq', value='f'),
             dict(up=0, right=8, left=6, operator='and',
                  field='f3', condition='like', value='g'),
             dict(up=0, left=7, operator='and',
                  field='f3', condition='eq', value='h'),
             ],
            None)

    def test_28_down_and_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn(
            ''.join(('f3 le a and ( ',
                     'f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     ') and f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, right=7, left=1, operator='and'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='and',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('f3 le a and ( ',
                     'f4 eq v and like w and eq x and y',
                     ') and f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, right=7, left=1, operator='and'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='and',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('( ',
                     'f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     ') and f2 gt b')),
            [dict(down=1),
             dict(up=0, down=2, right=6),
             dict(up=1, right=3,
                  field='f4', condition='eq', value='v'),
             dict(up=1, right=4, left=2, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=1, right=5, left=3, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=1, left=4, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=1, operator='and',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('f3 le a and ( ',
                     'f4 eq v and like w and eq x and y',
                     ')')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, left=1, operator='and'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             ],
            None)

    def test_29_down_or_and_index_and_non_index_condition_order_abbrev(self):
        self._ocn(
            ''.join(('f3 le a or ( ',
                     'f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     ') or f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, right=7, left=1, operator='or'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='or',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('f3 le a or ( ',
                     'f4 eq v and like w and eq x and y',
                     ') or f2 gt b')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, right=7, left=1, operator='or'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=2, operator='or',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('( ',
                     'f4 eq v and f4 like w and f4 eq x and f4 eq y ',
                     ') or f2 gt b')),
            [dict(down=1),
             dict(up=0, down=2, right=6),
             dict(up=1, right=3,
                  field='f4', condition='eq', value='v'),
             dict(up=1, right=4, left=2, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=1, right=5, left=3, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=1, left=4, operator='and',
                  field='f4', condition='eq', value='y'),
             dict(up=0, left=1, operator='or',
                  field='f2', condition='gt', value='b'),
             ],
            None)
        self._ocn(
            ''.join(('f3 le a or ( ',
                     'f4 eq v and like w and eq x and y',
                     ')')),
            [dict(down=1),
             dict(up=0, right=2,
                  field='f3', condition='le', value='a'),
             dict(up=0, down=3, left=1, operator='or'),
             dict(up=2, right=4,
                  field='f4', condition='eq', value='v'),
             dict(up=2, right=5, left=3, operator='and',
                  field='f4', condition='like', value='w'),
             dict(up=2, right=6, left=4, operator='and',
                  field='f4', condition='eq', value='x'),
             dict(up=2, left=5, operator='and',
                  field='f4', condition='eq', value='y'),
             ],
            None)


class WhereClause_evaluate_index_condition_nodeTC(unittest.TestCase):
    # Mixing conditions which can be evaluated using indexes, or not, is tested
    # in WhereClause_set_non_index_node_constraintTC, 'f like a or f eq b'
    # for example, except to demonstrate evaluate_index_condition_node does
    # not give an answer and does not set any constraints.  The span of each
    # constraint is set by this method.

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])
        self.assertEqual(self.processors.existence, set(range(4)))
        self.assertEqual(self.processors.get_existence(), set(range(4)))

    # Call WhereClause.evaluate_index_condition_node() at point where
    # Where.evaluate() would be called, after doing the preceding tasks in the
    # real method.
    def _eicn(self,
              query,
              expected_clauses,
              expected_result,
              expected_constraintmap,
              expected_validation_response=type(None)):
        w = where.Where(query)

        # Local references to unittest methods.
        ae = self.assertEqual
        ai = self.assertIsInstance

        w.lex()
        w.parse()

        # validate() is called before evaluate()
        ai(w.validate(self.processors.database, self.processors.filename),
           expected_validation_response)

        w._processors = self.processors
        w.node.get_root().evaluate_index_condition_node(w._index_rules_engine)

        # References to nodes for test evaluation.
        wcw = w.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}

        #print()
        #for e, c in enumerate(wcw):
        #    print(e, c)
        #    #print(c.__dict__)
        #    #if c.result:
        #    #    print(c.result.__dict__)
        #    #    print(c.constraint.__dict__)
        #print()
        #print()
        #for e, c in enumerate(wcw):
        #    print(e, c.result)
        #    #print(c.__dict__)
        #    if c.result:
        #        print(c.result.__dict__)
        #        #print(c.constraint.__dict__)
        #    print()
        
        # Non-index node result.answers and all constraint results are None.
        #print(wci)
        #print()
        #print('node map')
        for n in wcw:
            #print(wci[n], n)
            ae(n.constraint.result, None)

        #print()
        #print('node.result')
        #for n in wcw:
        #    if n.result:
        #        print(wci[n], n.result.answer, n.result)
        #    else:
        #        print(wci[n], n.result)
        #print()
        #print('node.constraint')
        #for n in wcw:
        #    print(wci[n], n.constraint)
        #print()
        #print('node.constraint.result')
        #for n in wcw:
        #    if n.constraint:
        #        if n.constraint.result is not None:
        #            print(wci[n], n.constraint.result)

        constraintmap = {}
        for e, c in enumerate(wcw):
            constraintmap.setdefault(c.constraint, set()).add(e)
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.left, wci.get(c.left))
                ae(wc.right, wci.get(c.right))
                ae(wc.up, wci.get(c.up))
                ae(wc.down, wci.get(c.down))
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
                ae(len(wcw), len(expected_clauses))
                #if wc.result is not None:
                #    ae(wc.result.__dict__, c.result.__dict__)
                #else:
                #    ae(wc.result, c.result)
                #ae(wc.constraint.__dict__, c.constraint.__dict__)
            except AssertionError:
                print(e)
                print(c.result.__dict__, wc.result.__dict__)
                raise
        ae(len(constraintmap), len(expected_constraintmap))
        for k, v in constraintmap.items():
            ae(k.result, None)
            ae(v in expected_constraintmap, True)
        if expected_result is not None:
            ae(adjust(expected_result), w.node.result.answer)
        elif expected_validation_response is where.WhereStatementError:
            ae(None, w.node.result.answer)
        else:
            ae(None, w.node.result)

    def test_01_field_not_in_file(self):
        #self.assertRaisesRegex(
        #    AssertionError,
        #    "Lists differ: \['g'\] != \[\]",
        #    self._eicn,
        #    *('g eq v', None, None, None))
        result = where.WhereResult()
        result.answer = set()
        self._eicn('g eq v',
                   [dict(down=1,
                         result=result),
                    dict(up=0,
                         result=result,
                         field='g', condition='eq', value='v'),
                    ],
                   None,
                   ({0}, {1}),
                   where.WhereStatementError)

    def test_02_index_field_in_file_01(self):
        result = where.WhereResult()
        result.answer = set()
        self._eicn('f1 eq "no value"',
                   [dict(down=1,
                         result=result),
                    dict(up=0,
                         result=result,
                         field='f1', condition='eq', value='no value'),
                    ],
                   set(),
                   ({0}, {1}))

    def test_02_index_field_in_file_02(self):
        result = where.WhereResult()
        result.answer = {0}
        self._eicn('f1 eq a',
                   [dict(down=1,
                         result=result),
                    dict(up=0,
                         result=result,
                         field='f1', condition='eq', value='a'),
                    ],
                   {0},
                   ({0}, {1}))

    def test_03_non_index_field_in_file(self):
        result = where.WhereResult()
        result.answer = set()
        self._eicn('f eq v',
                   [dict(down=1,
                         result=result),
                    dict(up=0,
                         result=result,
                         field='f', condition='eq', value='v'),
                    ],
                   set(),
                   ({0}, {1}))

    def test_04_index_field_condition_not_indexed_01(self):
        self._eicn('f1 like a',
                   [dict(down=1),
                    dict(up=0,
                         field='f1', condition='like', value='a'),
                    ],
                   {0},
                   ({0}, {1}))

    def test_04_index_field_condition_not_indexed_02(self):
        result = where.WhereResult()
        result.answer = set()
        self._eicn('f1 like a or f1 eq "no value"',
                   [dict(down=1),
                    dict(up=0, right=2,
                         field='f1', condition='like', value='a'),
                    dict(up=0, left=1, operator='or',
                         result=result,
                         field='f1', condition='eq', value='no value'),
                    ],
                   {0},
                   ({0}, {1}, {2}))

    def test_05_index_field_condition_nor(self):
        result = where.WhereResult()
        result.answer = set((0,))
        self._eicn('f1 eq a nor f1 ge c',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f1', condition='eq', value='a'),
                    dict(up=0, left=1, operator='nor',
                         result=result,
                         field='f1', condition='ge', value='c'),
                    ],
                   {0},
                   ({0}, {1, 2}))

    def test_06_index_field_condition_and_or_and_01(self):
        result = where.WhereResult()
        result.answer = set((2, 3))
        gash_result = where.WhereResult()
        gash_result.answer = set((3,))
        self._eicn('f1 gt b and lt d or f2 gt o and lt q',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f1', condition='gt', value='b'),
                    dict(up=0, left=1, right=3, operator='and',
                         result=result,
                         field='f1', condition='lt', value='d'),
                    dict(up=0, left=2, right=4, operator='or',
                         result=result,
                         field='f2', condition='gt', value='o'),
                    dict(up=0, left=3, operator='and',
                         result=gash_result,
                         field='f2', condition='lt', value='q'),
                    ],
                   {2, 3},
                   ({0}, {1, 2}, {3, 4}))

    def test_06_index_field_condition_and_or_and_02(self):
        result = where.WhereResult()
        result.answer = set((2, 3))
        gash_result = where.WhereResult()
        gash_result.answer = set((3,))
        self._eicn('f1 gt b and lt d or ( f2 gt o and lt q )',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f1', condition='gt', value='b'),
                    dict(up=0, left=1, right=3, operator='and',
                         result=result,
                         field='f1', condition='lt', value='d'),
                    dict(up=0, down=4, left=2, operator='or',
                         result=result),
                    dict(up=3, right=5,
                         result=gash_result,
                         field='f2', condition='gt', value='o'),
                    dict(up=3, left=4, operator='and',
                         result=gash_result,
                         field='f2', condition='lt', value='q'),
                    ],
                   {2, 3},
                   ({0}, {1, 2}, {3}, {4, 5}))

    # Added after query failed test_01_index_field_condition_not_indexed_02 in
    # WhereClause_evaluate_node_resultTC where 'f3 like x' replaces 'f3 ge x'.
    def test_06_index_field_condition_and_or_and_03(self):
        result = where.WhereResult()
        result.answer = set((1, 2, 3))
        gash_result = where.WhereResult()
        gash_result.answer = set((3,))
        self._eicn('f2 gt o and lt q or f1 gt a and lt c and f3 ge x',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f2', condition='gt', value='o'),
                    dict(up=0, left=1, right=3, operator='and',
                         result=result,
                         field='f2', condition='lt', value='q'),
                    dict(up=0, left=2, right=4, operator='or',
                         result=result,
                         field='f1', condition='gt', value='a'),
                    dict(up=0, left=3, right=5, operator='and',
                         result=gash_result,
                         field='f1', condition='lt', value='c'),
                    dict(up=0, left=4, operator='and',
                         result=gash_result,
                         field='f3', condition='ge', value='x'),
                    ],
                   {1, 3},
                   ({0}, {1, 2}, {3, 4, 5}))

    def test_07_index_field_condition_and_or_and_01(self):
        result = where.WhereResult()
        result.answer = set()
        gash_result = where.WhereResult()
        gash_result.answer = set()
        self._eicn('f1 present and like d or f2 like o and like q',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f1', condition='present',),
                    dict(up=0, left=1, right=3, operator='and',
                         result=result,
                         field='f1', condition='like', value='d'),
                    dict(up=0, left=2, right=4, operator='or',
                         result=result,
                         field='f2', condition='like', value='o'),
                    dict(up=0, left=3, operator='and',
                         result=gash_result,
                         field='f2', condition='like', value='q'),
                    ],
                   None,
                   ({0}, {1, 2}, {3, 4}))

    def test_07_index_field_condition_and_or_and_02(self):
        result = where.WhereResult()
        result.answer = set()
        gash_result = where.WhereResult()
        gash_result.answer = set()
        self._eicn('f1 present and like d or ( f2 like o and like q )',
                   [dict(down=1,
                         result=result),
                    dict(up=0, right=2,
                         result=result,
                         field='f1', condition='present'),
                    dict(up=0, left=1, right=3, operator='and',
                         result=result,
                         field='f1', condition='like', value='d'),
                    dict(up=0, down=4, left=2, operator='or',
                         result=result),
                    dict(up=3, right=5,
                         result=gash_result,
                         field='f2', condition='like', value='o'),
                    dict(up=3, left=4, operator='and',
                         result=gash_result,
                         field='f2', condition='like', value='q'),
                    ],
                   None,
                   ({0}, {1, 2}, {3}, {4, 5}))


class WhereClause_set_non_index_node_constraintTC(unittest.TestCase):

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])
        self.assertEqual(self.processors.existence, set(range(4)))
        self.assertEqual(self.processors.get_existence(), set(range(4)))

    # Call WhereClause.set_non_index_node_constraint() at point where
    # Where.evaluate() would be called, after doing the preceding tasks in the
    # real method.
    def _sninc(self,
               query,
               expected_clauses,
               expected_constraintmap,
               expected_constraints,
               expected_nonindex):
        w = where.Where(query)

        # Local references to unittest methods.
        ae = self.assertEqual
        
        w.lex()
        w.parse()

        # validate() is called before evaluate()
        ae(w.validate(self.processors.database, self.processors.filename), None)
        
        w._processors = self.processors
        rn = w.node.get_root()
        rn.evaluate_index_condition_node(w._index_rules_engine)

        # evaluate() exits if result is None.
        ae(rn.result, None, msg='Real code would not get here for given query')

        # References to nodes for test evaluation.
        wcw = w.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}
        
        non_index_nodes = []
        rn.get_non_index_condition_node(non_index_nodes)
        
        # Non-index node result.answers and all constraint results are None.
        #print(wci)
        #print(non_index_nodes)
        ae({wci[n] for n in non_index_nodes}, expected_nonindex)
        for n in non_index_nodes:
            ae(n.result.answer, None)
        for n in wcw:
            #print(wci[n], n)
            ae(n.constraint.result, None)

        rn.constraint.result = where.WhereResult()
        rn.constraint.result.answer = self.processors.get_existence()
        rn.set_non_index_node_constraint(self.processors.initialize_answer)
        #print()
        #for n in non_index_nodes:
        #    print(n.result.__dict__)
        #    print(n.constraint.result)
        #    print()
        
        # Constraints have been set using conditions which can be evaluated
        # using indexes.
        # Nested parentheses may have merged some constraint spans.  The span
        # introduced by parentheses in '( f like v )' is removed because the
        # constraint on 'f like v' is the same.
        constraintmap = {}
        for e, c in enumerate(wcw):
            constraintmap.setdefault(c.constraint, set()).add(e)
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.left, wci.get(c.left))
                ae(wc.right, wci.get(c.right))
                ae(wc.up, wci.get(c.up))
                ae(wc.down, wci.get(c.down))
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
                ae(len(wcw), len(expected_clauses))
            except AssertionError:
                print(e)
                print(c.result.__dict__, wc.result.__dict__)
                raise
        ae(len(constraintmap), len(expected_constraintmap))
        #print()
        #print(expected_constraintmap)
        #for k, v in constraintmap.items():
        #    print(k, v)
        for k, v in constraintmap.items():
            #print('?', v)
            ae(v in expected_constraintmap, True)
            for e, c in enumerate(expected_constraintmap):
                if c == v:
                    ece = expected_constraints[e]
                    #if k.result:
                    #    print(k.result.answer, ece, e)
                    #else:
                    #    print(k.result, ece, e)
                    if ece is None:
                        ae(k.result, ece)
                    else:
                        ae(k.result.answer, adjust(ece))
        ae(None, w.node.result)


    def test_01_index_field_condition_not_indexed_01(self):
        self._sninc('f1 like a',
                    [dict(down=1),
                     dict(up=0,
                          field='f1', condition='like', value='a'),
                     ],
                    ({0}, {1},),
                    ({0, 1, 2, 3}, None),
                    {1})
        self._sninc('( f1 like a )',
                    [dict(down=1),
                     dict(up=0, down=2),
                     dict(up=1,
                          field='f1', condition='like', value='a'),
                     ],
                    ({0}, {1}, {2}),
                    ({0, 1, 2, 3}, None, None),
                    {2})


    def test_02_index_field_condition_not_indexed_or_01(self):
        self._sninc('f1 like a or f2 eq p',
                    [dict(down=1),
                     dict(up=0, right=2,
                         field='f1', condition='like', value='a'),
                     dict(up=0, left=1, operator='or',
                          field='f2', condition='eq', value='p'),
                     ],
                    ({0}, {1}, {2}),
                    ({0, 1, 2, 3}, None, {3}),
                    {1})
        self._sninc('( f1 like a or f2 eq p )',
                    [dict(down=1),
                     dict(up=0, down=2),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='or',
                          field='f2', condition='eq', value='p'),
                     ],
                    ({0}, {1}, {2}, {3}),
                    ({0, 1, 2, 3}, None, None, {3}),
                    {2})
        self._sninc('( f1 like a or f2 eq p ) and f3 eq x',
                    [dict(down=1),
                     dict(up=0, down=2, right=4),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='or',
                          field='f2', condition='eq', value='p'),
                     dict(up=0, left=1, operator='and',
                          field='f3', condition='eq', value='x'),
                     ],
                    ({0}, {1, 4}, {2}, {3}),
                    ({0, 1, 2, 3}, {1}, None, {3}),
                    {2})
        self._sninc('( f1 like a or f2 eq p ) or f3 eq x',
                    [dict(down=1),
                     dict(up=0, down=2, right=4),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='or',
                          field='f2', condition='eq', value='p'),
                     dict(up=0, left=1, operator='or',
                          field='f3', condition='eq', value='x'),
                     ],
                    ({0}, {1}, {2}, {3}, {4}),
                    ({0, 1, 2, 3}, None, None, {3}, {1}),
                    {2})

    def test_03_index_field_condition_not_indexed_and_01(self):
        self._sninc('f1 like a and f2 eq p',
                    [dict(down=1),
                     dict(up=0, right=2,
                          field='f1', condition='like', value='a'),
                     dict(up=0, left=1, operator='and',
                          field='f2', condition='eq', value='p'),
                     ],
                    ({0}, {1, 2}),
                    ({0, 1, 2, 3}, {3}),
                    {1})
        self._sninc('( f1 like a and f2 eq p )',
                    [dict(down=1),
                     dict(up=0, down=2),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='and',
                          field='f2', condition='eq', value='p'),
                     ],
                    ({0}, {1}, {2, 3}),
                    ({0, 1, 2, 3}, None, {3}),
                    {2})
        self._sninc('( f1 like a and f2 eq p ) and f3 eq x',
                    [dict(down=1),
                     dict(up=0, down=2, right=4),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='and',
                          field='f2', condition='eq', value='p'),
                     dict(up=0, left=1, operator='and',
                          field='f3', condition='eq', value='x'),
                     ],
                    ({0}, {1, 4}, {2, 3}),
                    ({0, 1, 2, 3}, {1}, {3}),
                    {2})
        self._sninc('( f1 like a and f2 eq p ) or f3 eq x',
                    [dict(down=1),
                     dict(up=0, down=2, right=4),
                     dict(up=1, right=3,
                          field='f1', condition='like', value='a'),
                     dict(up=1, left=2, operator='and',
                          field='f2', condition='eq', value='p'),
                     dict(up=0, left=1, operator='or',
                          field='f3', condition='eq', value='x'),
                     ],
                    ({0}, {1}, {2, 3}, {4}),
                    ({0, 1, 2, 3}, None, {3}, {1}),
                    {2})


class Where_evaluate_non_index_conditionsTC(unittest.TestCase):

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])
        self.assertEqual(self.processors.existence, set(range(4)))
        self.assertEqual(self.processors.get_existence(), set(range(4)))

    # Call Where._evaluate_non_index_conditions() at point where
    # Where.evaluate() would be called, after doing the preceding tasks in the
    # real method.
    def _enic(self,
              query,
              expected_clauses,
              expected_nonindex,
              expected_nonindex_answer):
        w = where.Where(query)

        # Local references to unittest methods.
        ae = self.assertEqual
        
        w.lex()
        w.parse()

        # validate() is called before evaluate()
        ae(w.validate(self.processors.database, self.processors.filename), None)
        
        w._processors = self.processors
        rn = w.node.get_root()
        rn.evaluate_index_condition_node(w._index_rules_engine)

        # evaluate() exits if result is None.
        ae(rn.result, None, msg='Real code would not get here for given query')

        # References to nodes for test evaluation.
        wcw = w.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}
        
        non_index_nodes = []
        rn.get_non_index_condition_node(non_index_nodes)
        
        # Non-index node result.answers and all constraint results are None.
        #print(wci)
        #print(non_index_nodes)
        ae({wci[n] for n in non_index_nodes}, expected_nonindex)
        for n in non_index_nodes:
            ae(n.result.answer, None)
        for n in wcw:
            #print(wci[n], n)
            ae(n.constraint.result, None)

        rn.constraint.result = where.WhereResult()
        rn.constraint.result.answer = self.processors.get_existence()
        rn.set_non_index_node_constraint(self.processors.initialize_answer)
        w._evaluate_non_index_conditions(non_index_nodes)
        #print()
        #for n in non_index_nodes:
        #    print(n.result.__dict__)
        #    print(n.constraint.result)
        #    print()
        
        # Constraints tests in WhereClause_set_non_index_node_constraintTC are
        # not repeated here.
        for e, c in enumerate(wcw):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.left, wci.get(c.left))
                ae(wc.right, wci.get(c.right))
                ae(wc.up, wci.get(c.up))
                ae(wc.down, wci.get(c.down))
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
                ae(len(wcw), len(expected_clauses))
            except AssertionError:
                print(e)
                print(c.result.__dict__, wc.result.__dict__)
                raise
        ae(None, w.node.result)
        for k, v in expected_nonindex_answer.items():
            ae(wcw[k].result.answer, adjust(v))

    def test_01_index_field_condition_not_indexed_01(self):
        self._enic('f1 like a',
                   [dict(down=1),
                    dict(up=0,
                         field='f1', condition='like', value='a'),
                    ],
                   {1},
                   {1:{0}})
        self._enic('( f1 like a )',
                   [dict(down=1),
                    dict(up=0, down=2),
                    dict(up=1,
                         field='f1', condition='like', value='a'),
                    ],
                   {2},
                   {2:{0}})

    def test_02_index_field_condition_not_indexed_01(self):
        self._enic('f1 not like a',
                   [dict(down=1),
                    dict(up=0,
                         field='f1', condition='like', value='a',
                         not_condition=True),
                    ],
                   {1},
                   {1:{1, 2, 3}})

    def test_03_index_field_condition_not_indexed_01(self):
        self._enic('not f1 like a',
                   [dict(down=1),
                    dict(up=0,
                         field='f1', condition='like', value='a',
                         not_phrase=True),
                    ],
                   {1},
                   {1:{1, 2, 3}})

    def test_04_index_field_condition_not_indexed_01(self):
        self._enic('not f1 not like a',
                   [dict(down=1),
                    dict(up=0,
                         field='f1', condition='like', value='a',
                         not_condition=True, not_phrase=True),
                    ],
                   {1},
                   {1:{0}})


class WhereClause_evaluate_node_resultTC(unittest.TestCase):

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [{'f1':['a']}, # record 0
             {'f1':['b'], 'f3':['x']}, # record 1
             {'f1':['c']}, # record 2
             {'f2':['p']}, # record 3
             ])
        self.assertEqual(self.processors.existence, set(range(4)))
        self.assertEqual(self.processors.get_existence(), set(range(4)))

    # Call WhereClause.evaluate_node_result() at point where Where.evaluate()
    # would be called, after doing the preceding tasks in the real method.
    def _enr(self,
             query,
             expected_clauses,
             expected_answer):
        w = where.Where(query)

        # Local references to unittest methods.
        ae = self.assertEqual
        
        w.lex()
        w.parse()

        # validate() is called before evaluate()
        ae(w.validate(self.processors.database, self.processors.filename), None)
        
        w._processors = self.processors
        rn = w.node.get_root()
        rn.evaluate_index_condition_node(w._index_rules_engine)

        # evaluate() exits if result is None.
        ae(rn.result, None, msg='Real code would not get here for given query')

        # References to nodes for test evaluation.
        wcw = w.node.get_clauses_from_root_in_walk_order()
        wci = {c:e for e, c in enumerate(wcw)}
        
        non_index_nodes = []
        rn.get_non_index_condition_node(non_index_nodes)
        
        # Non-index node result.answers and all constraint results are None.
        #print(wci)
        #print()
        #print('node map')
        #print(non_index_nodes)
        for n in non_index_nodes:
            ae(n.result.answer, None)
        for n in wcw:
            #print(wci[n], n)
            ae(n.constraint.result, None)

        #print()
        #print('node.result')
        #for n in wcw:
        #    if n.result:
        #        print(wci[n], n.result.answer, n.result)
        #    else:
        #        print(wci[n], n.result)
        #print()
        #print('node.constraint')
        #for n in wcw:
        #    print(wci[n], n.constraint)
        #print()
        #print('node.constraint.result')
        #for n in wcw:
        #    if n.constraint:
        #        if n.constraint.result is not None:
        #            print(wci[n], n.constraint.result.answer, n.constraint.result)

        rn.constraint.result = where.WhereResult()
        rn.constraint.result.answer = self.processors.get_existence()
        rn.set_non_index_node_constraint(self.processors.initialize_answer)

        #print()
        #print('after set non-index constraints')
        #print()
        #print('node.result')
        #for n in wcw:
        #    if n.result:
        #        print(wci[n], n.result.answer, n.result)
        #    else:
        #        print(wci[n], n.result)
        #print()
        #print('node.constraint')
        #for n in wcw:
        #    print(wci[n], n.constraint)
        #print()
        #print('node.constraint.result')
        #for n in wcw:
        #    if n.constraint:
        #        if n.constraint.result is not None:
        #            print(wci[n], n.constraint.result.answer, n.constraint.result)

        w._evaluate_non_index_conditions(non_index_nodes)
        rn.evaluate_node_result(w._result_rules_engine)
        #print()
        #for n in non_index_nodes:
        #    print(n.result.__dict__)
        #    print(n.constraint.result)
        #    print()
        
        # Constraints tests in WhereClause_set_non_index_node_constraintTC are
        # not repeated here.
        for e, c in enumerate(wcw):
            wc = where.WhereClause()
            wc.__dict__.update(expected_clauses[e])
            try:
                ae(wc.left, wci.get(c.left))
                ae(wc.right, wci.get(c.right))
                ae(wc.up, wci.get(c.up))
                ae(wc.down, wci.get(c.down))
                ae(wc.operator, c.operator)
                ae(wc.field, c.field)
                ae(wc.condition, c.condition)
                ae(wc.value, c.value)
                ae(wc.not_phrase, c.not_phrase)
                ae(wc.not_condition, c.not_condition)
                ae(wc.not_value, c.not_value)
                ae(wc.num, c.num)
                ae(wc.alpha, c.alpha)
                ae(len(wcw), len(expected_clauses))
            except AssertionError:
                print(e)
                print(c.result.__dict__, wc.result.__dict__)
                raise
        ae(adjust(expected_answer), w.node.result.answer)


    def test_01_index_field_condition_not_indexed_01(self):
        self._enr('f1 like a',
                  [dict(down=1),
                   dict(up=0,
                        field='f1', condition='like', value='a'),
                   ],
                  {0})
        self._enr('( f1 like a )',
                  [dict(down=1),
                   dict(up=0, down=2),
                   dict(up=1,
                        field='f1', condition='like', value='a'),
                   ],
                  {0})

    def test_01_index_field_condition_not_indexed_02(self):

        # A query of this structure is known to not work, but the similar query
        # with 'f3 ge x' instead of 'f3 like x' does work.
        # See test_06_index_field_condition_and_or_and_03()
        # in WhereClause_evaluate_node_resultTC.
        self._enr('f2 gt o and lt q or f1 gt a and lt c and f3 like x',
                  [dict(down=1),
                   dict(up=0, right=2,
                        field='f2', condition='gt', value='o'),
                   dict(up=0, right=3, left=1, operator='and',
                        field='f2', condition='lt', value='q'),
                   dict(up=0, right=4, left=2, operator='or',
                        field='f1', condition='gt', value='a'),
                   dict(up=0, right=5, left=3, operator='and',
                        field='f1', condition='lt', value='c'),
                   dict(up=0, left=4, operator='and',
                        field='f3', condition='like', value='x'),
                   ],
                  {1, 3})


class Where_evaluateTC(unittest.TestCase):

    def setUp(self):
            
        self.processors = Processors(
            {'f'},
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'},
            [{'f':['0'], 'f1':['b'], 'f2':['m', 'n'], 'f3':['a'],
              'f4':['a'], 'f5':['de']},
             {'f':['1'], 'f1':['c'], 'f2':['n', 'o'], 'f3':['ab'],
              'f4':['xa']},
             {'f':['2'], 'f1':['d'], 'f2':['o', 'p'], 'f3':['b'],
              'f4':['xax'], 'f5':['de']},
             {'f':['3'], 'f1':['e'], 'f2':['p', 'q'], 'f3':['ba'],
              'f4':['axa']},
             {'f':['4'], 'f1':['f'], 'f2':['q', 'r'], 'f3':['bc'],
              'f4':['ax'], 'f5':['df']},
             {'f':['5'], 'f1':['g'], 'f2':['s', 't'], 'f3':['1'],
              'f4':['b']},
             {'f':['6'], 'f1':['h'], 'f2':['u', 'v'], 'f3':['12'],
              'f4':['xb'], 'f5':['df']},
             {'f':['7'], 'f1':['i'], 'f2':['w', 'x'], 'f3':['2'],
              'f4':['xbx']},
             {'f':['8'], 'f1':['j'], 'f2':['y', 'z'], 'f3':['21'],
              'f4':['xbax'], 'f5':['dg']},
             {'f':['9'], 'f1':['k'], 'f2':['l', 'l'], 'f3':['23'],
              'f4':['x']},
             ])

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(
            self.processors.non_indexed_fields,
            {'f'})
        self.assertEqual(
            self.processors.indexed_fields,
            {'f1', 'f2', 'f3', 'f4', 'f5', 'f6'})
        self.assertEqual(
            self.processors.records,
            [{'f':['0'], 'f1':['b'], 'f2':['m', 'n'], 'f3':['a'],
              'f4':['a'], 'f5':['de']},
             {'f':['1'], 'f1':['c'], 'f2':['n', 'o'], 'f3':['ab'],
              'f4':['xa']},
             {'f':['2'], 'f1':['d'], 'f2':['o', 'p'], 'f3':['b'],
              'f4':['xax'], 'f5':['de']},
             {'f':['3'], 'f1':['e'], 'f2':['p', 'q'], 'f3':['ba'],
              'f4':['axa']},
             {'f':['4'], 'f1':['f'], 'f2':['q', 'r'], 'f3':['bc'],
              'f4':['ax'], 'f5':['df']},
             {'f':['5'], 'f1':['g'], 'f2':['s', 't'], 'f3':['1'],
              'f4':['b']},
             {'f':['6'], 'f1':['h'], 'f2':['u', 'v'], 'f3':['12'],
              'f4':['xb'], 'f5':['df']},
             {'f':['7'], 'f1':['i'], 'f2':['w', 'x'], 'f3':['2'],
              'f4':['xbx']},
             {'f':['8'], 'f1':['j'], 'f2':['y', 'z'], 'f3':['21'],
              'f4':['xbax'], 'f5':['dg']},
             {'f':['9'], 'f1':['k'], 'f2':['l', 'l'], 'f3':['23'],
              'f4':['x']},
             ])
        self.assertEqual(self.processors.existence, set(range(10)))
        self.assertEqual(self.processors.get_existence(), set(range(10)))

    def _ev(self, query, expected_answer):
        ae = self.assertEqual
        w = where.Where(query)
        w.lex()
        w.parse()
        ae(w.validate(self.processors.database, self.processors.filename), None)
        ae(w.evaluate(self.processors), None)
        ae(w._processors, None)
        ae(adjust(expected_answer), w.node.result.answer)

    def test_01_like_01(self):
        self._ev('f1 like a', set())
        self._ev('f1 like b', {0})
        self._ev('f2 like p', {2, 3})
        self._ev('f3 like a', {0, 1, 3})
        self._ev('f3 like a\Z', {0, 3})
        self._ev('f4 like a', {0, 1, 2, 3, 4, 8})
        self._ev('f4 like a\Z', {0, 1, 3})
        self._ev('f4 like \Aa', {0, 3, 4})
        self._ev('f4 like \Aa\Z', {0})
        self._ev('f4 like b', {5, 6, 7, 8})
        self._ev('f4 like b\Z', {5, 6})
        self._ev('f4 like \Ab', {5})
        self._ev('f4 like \Ab\Z', {5})

    def test_01_like_02_not(self):
        self._ev('not f1 like a', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f1 like b', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 like p', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('not f3 like a', {2, 4, 5, 6, 7, 8, 9})
        self._ev('not f3 like a\Z', {1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('not f4 like a', {5, 6, 7, 9})
        self._ev('not f4 like a\Z', {2, 4, 5, 6, 7, 8, 9})
        self._ev('not f4 like \Aa', {1, 2, 5, 6, 7, 8, 9})
        self._ev('not f4 like \Aa\Z', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f4 like b', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 like b\Z', {0, 1, 2, 3, 4, 7, 8, 9})
        self._ev('not f4 like \Ab', {0, 1, 2, 3, 4, 6, 7, 8, 9})
        self._ev('not f4 like \Ab\Z', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_01_like_03_not(self):
        self._ev('f1 not like a', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f1 not like b', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 not like p', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('f3 not like a', {2, 4, 5, 6, 7, 8, 9})
        self._ev('f3 not like a\Z', {1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('f4 not like a', {5, 6, 7, 9})
        self._ev('f4 not like a\Z', {2, 4, 5, 6, 7, 8, 9})
        self._ev('f4 not like \Aa', {1, 2, 5, 6, 7, 8, 9})
        self._ev('f4 not like \Aa\Z', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f4 not like b', {0, 1, 2, 3, 4, 9})
        self._ev('f4 not like b\Z', {0, 1, 2, 3, 4, 7, 8, 9})
        self._ev('f4 not like \Ab', {0, 1, 2, 3, 4, 6, 7, 8, 9})
        self._ev('f4 not like \Ab\Z', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_01_like_04_not_not(self):
        self._ev('not f1 not like a', set())
        self._ev('not f1 not like b', {0})
        self._ev('not f2 not like p', {2, 3})
        self._ev('not f3 not like a', {0, 1, 3})
        self._ev('not f3 not like a\Z', {0, 3})
        self._ev('not f4 not like a', {0, 1, 2, 3, 4, 8})
        self._ev('not f4 not like a\Z', {0, 1, 3})
        self._ev('not f4 not like \Aa', {0, 3, 4})
        self._ev('not f4 not like \Aa\Z', {0})
        self._ev('not f4 not like b', {5, 6, 7, 8})
        self._ev('not f4 not like b\Z', {5, 6})
        self._ev('not f4 not like \Ab', {5})
        self._ev('not f4 not like \Ab\Z', {5})

    def test_01_like_05_parentheses(self):
        self._ev('( f1 like a )', set())
        self._ev('( f1 like b )', {0})
        self._ev('( f2 like p )', {2, 3})
        self._ev('( f3 like a )', {0, 1, 3})
        self._ev('( f3 like a\Z )', {0, 3})
        self._ev('( f4 like a )', {0, 1, 2, 3, 4, 8})
        self._ev('( f4 like a\Z )', {0, 1, 3})
        self._ev('( f4 like \Aa )', {0, 3, 4})
        self._ev('( f4 like \Aa\Z )', {0})
        self._ev('( f4 like b )', {5, 6, 7, 8})
        self._ev('( f4 like b\Z )', {5, 6})
        self._ev('( f4 like \Ab )', {5})
        self._ev('( f4 like \Ab\Z )', {5})

    def test_01_like_06_not_parentheses_01(self):
        self._ev('not ( f1 like a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f1 like b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 like p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f3 like a )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f3 like a\Z )', {1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f4 like a )', {5, 6, 7, 9})
        self._ev('not ( f4 like a\Z )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f4 like \Aa )', {1, 2, 5, 6, 7, 8, 9})
        self._ev('not ( f4 like \Aa\Z )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f4 like b )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 like b\Z )', {0, 1, 2, 3, 4, 7, 8, 9})
        self._ev('not ( f4 like \Ab )', {0, 1, 2, 3, 4, 6, 7, 8, 9})
        self._ev('not ( f4 like \Ab\Z )', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_01_like_06_not_parentheses_02(self):
        self._ev('( not f1 like a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f1 like b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f2 like p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('( not f3 like a )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('( not f3 like a\Z )', {1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('( not f4 like a )', {5, 6, 7, 9})
        self._ev('( not f4 like a\Z )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('( not f4 like \Aa )', {1, 2, 5, 6, 7, 8, 9})
        self._ev('( not f4 like \Aa\Z )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f4 like b )', {0, 1, 2, 3, 4, 9})
        self._ev('( not f4 like b\Z )', {0, 1, 2, 3, 4, 7, 8, 9})
        self._ev('( not f4 like \Ab )', {0, 1, 2, 3, 4, 6, 7, 8, 9})
        self._ev('( not f4 like \Ab\Z )', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_01_like_07_parentheses_not(self):
        self._ev('( f1 not like a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f1 not like b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 not like p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('( f3 not like a )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('( f3 not like a\Z )', {1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('( f4 not like a )', {5, 6, 7, 9})
        self._ev('( f4 not like a\Z )', {2, 4, 5, 6, 7, 8, 9})
        self._ev('( f4 not like \Aa )', {1, 2, 5, 6, 7, 8, 9})
        self._ev('( f4 not like \Aa\Z )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f4 not like b )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 not like b\Z )', {0, 1, 2, 3, 4, 7, 8, 9})
        self._ev('( f4 not like \Ab )', {0, 1, 2, 3, 4, 6, 7, 8, 9})
        self._ev('( f4 not like \Ab\Z )', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_01_like_08_not_parentheses_not(self):
        self._ev('not ( f1 not like a )', set())
        self._ev('not ( f1 not like b )', {0})
        self._ev('not ( f2 not like p )', {2, 3})
        self._ev('not ( f3 not like a )', {0, 1, 3})
        self._ev('not ( f3 not like a\Z )', {0, 3})
        self._ev('not ( f4 not like a )', {0, 1, 2, 3, 4, 8})
        self._ev('not ( f4 not like a\Z )', {0, 1, 3})
        self._ev('not ( f4 not like \Aa )', {0, 3, 4})
        self._ev('not ( f4 not like \Aa\Z )', {0})
        self._ev('not ( f4 not like b )', {5, 6, 7, 8})
        self._ev('not ( f4 not like b\Z )', {5, 6})
        self._ev('not ( f4 not like \Ab )', {5})
        self._ev('not ( f4 not like \Ab\Z )', {5})

    def test_02_eq_01(self):
        self._ev('f1 eq a', set())
        self._ev('f1 eq b', {0})
        self._ev('f2 eq p', {2, 3})
        self._ev('f3 eq a', {0})

    def test_02_eq_02_not(self):
        self._ev('not f1 eq a', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f1 eq b', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 eq p', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('not f3 eq a', {1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_02_eq_03_not(self):
        self._ev('f1 not eq a', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f1 not eq b', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 not eq p', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('f3 not eq a', {1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_02_eq_04_not_not(self):
        self._ev('not f1 not eq a', set())
        self._ev('not f1 not eq b', {0})
        self._ev('not f2 not eq p', {2, 3})
        self._ev('not f3 not eq a', {0})

    def test_02_eq_05_parentheses(self):
        self._ev('( f1 eq a )', set())
        self._ev('( f1 eq b )', {0})
        self._ev('( f2 eq p )', {2, 3})
        self._ev('( f3 eq a )', {0})

    def test_02_eq_06_not_parentheses_01(self):
        self._ev('not ( f1 eq a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f1 eq b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 eq p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f3 eq a )', {1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_02_eq_06_not_parentheses_02(self):
        self._ev('( not f1 eq a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f1 eq b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f2 eq p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('( not f3 eq a )', {1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_02_eq_07_not_parentheses(self):
        self._ev('( f1 not eq a )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f1 not eq b )', {1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 not eq p )', {0, 1, 4, 5, 6, 7, 8, 9})
        self._ev('( f3 not eq a )', {1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_02_eq_08_not_parentheses_not(self):
        self._ev('not ( f1 not eq a )', set())
        self._ev('not ( f1 not eq b )', {0})
        self._ev('not ( f2 not eq p )', {2, 3})
        self._ev('not ( f3 not eq a )', {0})

    def test_03_present_01(self):
        self._ev('f4 present', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f5 present', {0, 2, 4, 6, 8})
        self._ev('f6 present', set())

    def test_03_present_02_not(self):
        self._ev('not f4 present', set())
        self._ev('not f5 present', {1, 3, 5, 7, 9})
        self._ev('not f6 present', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_03_present_03_not(self):
        self._ev('f4 not present', set())
        self._ev('f5 not present', {1, 3, 5, 7, 9})
        self._ev('f6 not present', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_03_present_04_not_not(self):
        self._ev('not f4 not present', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f5 not present', {0, 2, 4, 6, 8})
        self._ev('not f6 not present', set())

    def test_03_present_05_parentheses(self):
        self._ev('( f4 present )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f5 present )', {0, 2, 4, 6, 8})
        self._ev('( f6 present )', set())

    def test_03_present_06_not_parentheses_01(self):
        self._ev('not ( f4 present )', set())
        self._ev('not ( f5 present )', {1, 3, 5, 7, 9})
        self._ev('not ( f6 present )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_03_present_06_not_parentheses_02(self):
        self._ev('( not f4 present )', set())
        self._ev('( not f5 present )', {1, 3, 5, 7, 9})
        self._ev('( not f6 present )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_03_present_07_not_parentheses(self):
        self._ev('( f4 not present )', set())
        self._ev('( f5 not present )', {1, 3, 5, 7, 9})
        self._ev('( f6 not present )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_03_present_08_not_parentheses_not(self):
        self._ev('not ( f4 not present )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f5 not present )', {0, 2, 4, 6, 8})
        self._ev('not ( f6 not present )', set())

    def test_04_is_01(self):
        self._ev('f4 is xb', {6})
        self._ev('f5 is df', {4, 6})
        self._ev('f6 is v', set())

    def test_04_is_02_not(self):
        self._ev('not f4 is xb', {0, 1, 2, 3, 4, 5, 7, 8, 9})
        self._ev('not f5 is df', {0, 1, 2, 3, 5, 7, 8, 9})
        self._ev('not f6 is v', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_04_is_03_not(self):
        self._ev('f4 is not xb', {0, 1, 2, 3, 4, 5, 7, 8, 9})
        self._ev('f5 is not df', {0, 2, 8})
        self._ev('f6 is not v', set())

    def test_04_is_04_not_not(self):
        self._ev('not f4 is not xb', {6})
        self._ev('not f5 is not df', {1, 3, 4, 5, 6, 7, 9})
        self._ev('not f6 is not v', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_04_is_05_parentheses(self):
        self._ev('( f4 is xb )', {6})
        self._ev('( f5 is df )', {4, 6})
        self._ev('( f6 is v )', set())

    def test_04_is_06_not_parentheses_01(self):
        self._ev('not ( f4 is xb )', {0, 1, 2, 3, 4, 5, 7, 8, 9})
        self._ev('not ( f5 is df )', {0, 1, 2, 3, 5, 7, 8, 9})
        self._ev('not ( f6 is v )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_04_is_06_not_parentheses_02(self):
        self._ev('( not f4 is xb )', {0, 1, 2, 3, 4, 5, 7, 8, 9})
        self._ev('( not f5 is df )', {0, 1, 2, 3, 5, 7, 8, 9})
        self._ev('( not f6 is v )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_04_is_07_not_parentheses(self):
        self._ev('( f4 is not xb )', {0, 1, 2, 3, 4, 5, 7, 8, 9})
        self._ev('( f5 is not df )', {0, 2, 8})
        self._ev('( f6 is not v )', set())

    def test_04_is_08_not_parentheses_not(self):
        self._ev('not ( f4 is not xb )', {6})
        self._ev('not ( f5 is not df )', {1, 3, 4, 5, 6, 7, 9})
        self._ev('not ( f6 is not v )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_05_from_to_01(self):
        self._ev('f3 from 22 to b', {0, 1, 2, 9})
        self._ev('f4 from axb to xba', {1, 2, 5, 6, 9})
        self._ev('f5 from df to dg', {4, 6, 8})

    def test_05_from_to_02_not(self):
        self._ev('not f3 from 22 to b', {3, 4, 5, 6, 7, 8})
        self._ev('not f4 from axb to xba', {0, 3, 4, 7, 8})
        self._ev('not f5 from df to dg', {0, 1, 2, 3, 5, 7, 9})

    def test_05_from_to_03_not(self):
        self._ev('f3 not from 22 to b', {3, 4, 5, 6, 7, 8})
        self._ev('f4 not from axb to xba', {0, 3, 4, 7, 8})
        self._ev('f5 not from df to dg', {0, 1, 2, 3, 5, 7, 9})

    def test_05_from_to_04_not_not(self):
        self._ev('not f3 not from 22 to b', {0, 1, 2, 9})
        self._ev('not f4 not from axb to xba', {1, 2, 5, 6, 9})
        self._ev('not f5 not from df to dg', {4, 6, 8})

    def test_05_from_to_05_parentheses(self):
        self._ev('( f3 from 22 to b )', {0, 1, 2, 9})
        self._ev('( f4 from axb to xba )', {1, 2, 5, 6, 9})
        self._ev('( f5 from df to dg )', {4, 6, 8})

    def test_05_from_to_06_not_parentheses_01(self):
        self._ev('not ( f3 from 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('not ( f4 from axb to xba )', {0, 3, 4, 7, 8})
        self._ev('not ( f5 from df to dg )', {0, 1, 2, 3, 5, 7, 9})

    def test_05_from_to_06_not_parentheses_02(self):
        self._ev('( not f3 from 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('( not f4 from axb to xba )', {0, 3, 4, 7, 8})
        self._ev('( not f5 from df to dg )', {0, 1, 2, 3, 5, 7, 9})

    def test_05_from_to_07_not_parentheses(self):
        self._ev('( f3 not from 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('( f4 not from axb to xba )', {0, 3, 4, 7, 8})
        self._ev('( f5 not from df to dg )', {0, 1, 2, 3, 5, 7, 9})

    def test_05_from_to_08_not_parentheses_not(self):
        self._ev('not ( f3 not from 22 to b )', {0, 1, 2, 9})
        self._ev('not ( f4 not from axb to xba )', {1, 2, 5, 6, 9})
        self._ev('not ( f5 not from df to dg )', {4, 6, 8})

    def test_06_from_below_01(self):
        self._ev('f3 from 22 below b', {0, 1, 9})
        self._ev('f4 from axb below xba', {1, 2, 5, 6, 9})
        self._ev('f5 from df below dg', {4, 6})

    def test_06_from_below_02_not(self):
        self._ev('not f3 from 22 below b', {2, 3, 4, 5, 6, 7, 8})
        self._ev('not f4 from axb below xba', {0, 3, 4, 7, 8})
        self._ev('not f5 from df below dg', {0, 1, 2, 3, 5, 7, 8, 9})

    def test_06_from_below_03_not(self):
        self._ev('f3 not from 22 below b', {2, 3, 4, 5, 6, 7, 8})
        self._ev('f4 not from axb below xba', {0, 3, 4, 7, 8})
        self._ev('f5 not from df below dg', {0, 1, 2, 3, 5, 7, 8, 9})

    def test_06_from_below_04_not_not(self):
        self._ev('not f3 not from 22 below b', {0, 1, 9})
        self._ev('not f4 not from axb below xba', {1, 2, 5, 6, 9})
        self._ev('not f5 not from df below dg', {4, 6})

    def test_06_from_below_05_parentheses(self):
        self._ev('( f3 from 22 below b )', {0, 1, 9})
        self._ev('( f4 from axb below xba )', {1, 2, 5, 6, 9})
        self._ev('( f5 from df below dg )', {4, 6})

    def test_06_from_below_06_not_parentheses_01(self):
        self._ev('not ( f3 from 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('not ( f4 from axb below xba )', {0, 3, 4, 7, 8})
        self._ev('not ( f5 from df below dg )', {0, 1, 2, 3, 5, 7, 8, 9})

    def test_06_from_below_06_not_parentheses_02(self):
        self._ev('( not f3 from 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('( not f4 from axb below xba )', {0, 3, 4, 7, 8})
        self._ev('( not f5 from df below dg )', {0, 1, 2, 3, 5, 7, 8, 9})

    def test_06_from_below_07_not_parentheses(self):
        self._ev('( f3 not from 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('( f4 not from axb below xba )', {0, 3, 4, 7, 8})
        self._ev('( f5 not from df below dg )', {0, 1, 2, 3, 5, 7, 8, 9})

    def test_06_from_below_08_not_parentheses_not(self):
        self._ev('not ( f3 not from 22 below b )', {0, 1, 9})
        self._ev('not ( f4 not from axb below xba )', {1, 2, 5, 6, 9})
        self._ev('not ( f5 not from df below dg )', {4, 6})

    def test_07_above_below_01(self):
        self._ev('f3 above 22 below b', {0, 1, 9})
        self._ev('f4 above axb below xba', {1, 2, 5, 6, 9})
        self._ev('f5 above df below dg', set())

    def test_07_above_below_02_not(self):
        self._ev('not f3 above 22 below b', {2, 3, 4, 5, 6, 7, 8})
        self._ev('not f4 above axb below xba', {0, 3, 4, 7, 8})
        self._ev('not f5 above df below dg', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_07_above_below_03_not(self):
        self._ev('f3 not above 22 below b', {2, 3, 4, 5, 6, 7, 8})
        self._ev('f4 not above axb below xba', {0, 3, 4, 7, 8})
        self._ev('f5 not above df below dg', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_07_above_below_04_not_not(self):
        self._ev('not f3 not above 22 below b', {0, 1, 9})
        self._ev('not f4 not above axb below xba', {1, 2, 5, 6, 9})
        self._ev('not f5 not above df below dg', set())

    def test_07_above_below_05_parentheses(self):
        self._ev('( f3 above 22 below b )', {0, 1, 9})
        self._ev('( f4 above axb below xba )', {1, 2, 5, 6, 9})
        self._ev('( f5 above df below dg )', set())

    def test_07_above_below_06_not_parentheses_01(self):
        self._ev('not ( f3 above 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('not ( f4 above axb below xba )', {0, 3, 4, 7, 8})
        self._ev('not ( f5 above df below dg )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_07_above_below_06_not_parentheses_02(self):
        self._ev('( not f3 above 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('( not f4 above axb below xba )', {0, 3, 4, 7, 8})
        self._ev('( not f5 above df below dg )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_07_above_below_07_not_parentheses(self):
        self._ev('( f3 not above 22 below b )', {2, 3, 4, 5, 6, 7, 8})
        self._ev('( f4 not above axb below xba )', {0, 3, 4, 7, 8})
        self._ev('( f5 not above df below dg )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

    def test_07_above_below_08_not_parentheses_not(self):
        self._ev('not ( f3 not above 22 below b )', {0, 1, 9})
        self._ev('not ( f4 not above axb below xba )', {1, 2, 5, 6, 9})
        self._ev('not ( f5 not above df below dg )', set())

    def test_08_above_to_01(self):
        self._ev('f3 above 22 to b', {0, 1, 2, 9})
        self._ev('f4 above axb to xba', {1, 2, 5, 6, 9})
        self._ev('f5 above df to dg', {8})

    def test_08_above_to_02_not(self):
        self._ev('not f3 above 22 to b', {3, 4, 5, 6, 7, 8})
        self._ev('not f4 above axb to xba', {0, 3, 4, 7, 8})
        self._ev('not f5 above df to dg', {0, 1, 2, 3, 4, 5, 6, 7, 9})

    def test_08_above_to_03_not(self):
        self._ev('f3 not above 22 to b', {3, 4, 5, 6, 7, 8})
        self._ev('f4 not above axb to xba', {0, 3, 4, 7, 8})
        self._ev('f5 not above df to dg', {0, 1, 2, 3, 4, 5, 6, 7, 9})

    def test_08_above_to_04_not_not(self):
        self._ev('not f3 not above 22 to b', {0, 1, 2, 9})
        self._ev('not f4 not above axb to xba', {1, 2, 5, 6, 9})
        self._ev('not f5 not above df to dg', {8})

    def test_08_above_to_05_parentheses(self):
        self._ev('( f3 above 22 to b )', {0, 1, 2, 9})
        self._ev('( f4 above axb to xba )', {1, 2, 5, 6, 9})
        self._ev('( f5 above df to dg )', {8})

    def test_08_above_to_06_not_parentheses_01(self):
        self._ev('not ( f3 above 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('not ( f4 above axb to xba )', {0, 3, 4, 7, 8})
        self._ev('not ( f5 above df to dg )', {0, 1, 2, 3, 4, 5, 6, 7, 9})

    def test_08_above_to_06_not_parentheses_02(self):
        self._ev('( not f3 above 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('( not f4 above axb to xba )', {0, 3, 4, 7, 8})
        self._ev('( not f5 above df to dg )', {0, 1, 2, 3, 4, 5, 6, 7, 9})

    def test_08_above_to_07_not_parentheses(self):
        self._ev('( f3 not above 22 to b )', {3, 4, 5, 6, 7, 8})
        self._ev('( f4 not above axb to xba )', {0, 3, 4, 7, 8})
        self._ev('( f5 not above df to dg )', {0, 1, 2, 3, 4, 5, 6, 7, 9})

    def test_08_above_to_08_not_parentheses_not(self):
        self._ev('not ( f3 not above 22 to b )', {0, 1, 2, 9})
        self._ev('not ( f4 not above axb to xba )', {1, 2, 5, 6, 9})
        self._ev('not ( f5 not above df to dg )', {8})

    def test_09_ne_01(self):
        self._ev('f1 ne e', {0, 1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('f2 ne q', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 ne r', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('f4 ne b', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_09_ne_02_not(self):
        self._ev('f1 not ne e', {3})
        self._ev('f2 not ne q', set())
        self._ev('f2 not ne r', set())
        self._ev('f4 not ne b', {5})

    def test_09_ne_03_not(self):
        self._ev('not f1 ne e', {3})
        self._ev('not f2 ne q', set())
        self._ev('not f2 ne r', set())
        self._ev('not f4 ne b', {5})

    def test_09_ne_04_not_not(self):
        self._ev('not f1 not ne e', {0, 1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 not ne q', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 not ne r', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not f4 not ne b', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_09_ne_05_parentheses(self):
        self._ev('( f1 ne e )', {0, 1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 ne q )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 ne r )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('( f4 ne b )', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_09_ne_06_not_parentheses_01(self):
        self._ev('not ( f1 ne e )', {3})
        self._ev('not ( f2 ne q )', set())
        self._ev('not ( f2 ne r )', set())
        self._ev('not ( f4 ne b )', {5})

    def test_09_ne_06_not_parentheses_02(self):
        self._ev('( not f1 ne e )', {3})
        self._ev('( not f2 ne q )', set())
        self._ev('( not f2 ne r )', set())
        self._ev('( not f4 ne b )', {5})

    def test_09_ne_07_not_parentheses(self):
        self._ev('( f1 not ne e )', {3})
        self._ev('( f2 not ne q )', set())
        self._ev('( f2 not ne r )', set())
        self._ev('( f4 not ne b )', {5})

    def test_09_ne_08_not_not_parentheses(self):
        self._ev('not ( f1 not ne e )', {0, 1, 2, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 not ne q )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 not ne r )', {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f4 not ne b )', {0, 1, 2, 3, 4, 6, 7, 8, 9})

    def test_10_gt_01(self):
        self._ev('f1 gt e', {4, 5, 6, 7, 8, 9})
        self._ev('f2 gt q', {4, 5, 6, 7, 8})
        self._ev('f2 gt r', {5, 6, 7, 8})
        self._ev('f4 gt b', {1, 2, 6, 7, 8, 9})

    def test_10_gt_02_not(self):
        self._ev('f1 not gt e', {0, 1, 2, 3})
        self._ev('f2 not gt q', {0, 1, 2, 3, 9})
        self._ev('f2 not gt r', {0, 1, 2, 3, 4, 9})
        self._ev('f4 not gt b', {0, 3, 4, 5})

    def test_10_gt_03_not(self):
        self._ev('not f1 gt e', {0, 1, 2, 3})
        self._ev('not f2 gt q', {0, 1, 2, 3, 9})
        self._ev('not f2 gt r', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 gt b', {0, 3, 4, 5})

    def test_10_gt_04_not_not(self):
        self._ev('not f1 not gt e', {4, 5, 6, 7, 8, 9})
        self._ev('not f2 not gt q', {4, 5, 6, 7, 8})
        self._ev('not f2 not gt r', {5, 6, 7, 8})
        self._ev('not f4 not gt b', {1, 2, 6, 7, 8, 9})

    def test_10_gt_05_parentheses(self):
        self._ev('( f1 gt e )', {4, 5, 6, 7, 8, 9})
        self._ev('( f2 gt q )', {4, 5, 6, 7, 8})
        self._ev('( f2 gt r )', {5, 6, 7, 8})
        self._ev('( f4 gt b )', {1, 2, 6, 7, 8, 9})

    def test_10_gt_06_not_parentheses_01(self):
        self._ev('not ( f1 gt e )', {0, 1, 2, 3})
        self._ev('not ( f2 gt q )', {0, 1, 2, 3, 9})
        self._ev('not ( f2 gt r )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 gt b )', {0, 3, 4, 5})

    def test_10_gt_06_not_parentheses_02(self):
        self._ev('( not f1 gt e )', {0, 1, 2, 3})
        self._ev('( not f2 gt q )', {0, 1, 2, 3, 9})
        self._ev('( not f2 gt r )', {0, 1, 2, 3, 4, 9})
        self._ev('( not f4 gt b )', {0, 3, 4, 5})

    def test_10_gt_07_not_parentheses(self):
        self._ev('( f1 not gt e )', {0, 1, 2, 3})
        self._ev('( f2 not gt q )', {0, 1, 2, 3, 9})
        self._ev('( f2 not gt r )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 not gt b )', {0, 3, 4, 5})

    def test_10_gt_08_not_not_parentheses(self):
        self._ev('not ( f1 not gt e )', {4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 not gt q )', {4, 5, 6, 7, 8})
        self._ev('not ( f2 not gt r )', {5, 6, 7, 8})
        self._ev('not ( f4 not gt b )', {1, 2, 6, 7, 8, 9})

    def test_11_lt_01(self):
        self._ev('f1 lt e', {0, 1, 2})
        self._ev('f2 lt q', {0, 1, 2, 3, 9})
        self._ev('f2 lt r', {0, 1, 2, 3, 4, 9})
        self._ev('f4 lt b', {0, 3, 4})

    def test_11_lt_02_not(self):
        self._ev('f1 not lt e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 not lt q', {4, 5, 6, 7, 8})
        self._ev('f2 not lt r', {5, 6, 7, 8})
        self._ev('f4 not lt b', {1, 2, 5, 6, 7, 8, 9})

    def test_11_lt_03_not(self):
        self._ev('not f1 lt e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 lt q', {4, 5, 6, 7, 8})
        self._ev('not f2 lt r', {5, 6, 7, 8})
        self._ev('not f4 lt b', {1, 2, 5, 6, 7, 8, 9})

    def test_11_lt_04_not_not(self):
        self._ev('not f1 not lt e', {0, 1, 2})
        self._ev('not f2 not lt q', {0, 1, 2, 3, 9})
        self._ev('not f2 not lt r', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 not lt b', {0, 3, 4})

    def test_11_lt_05_parentheses(self):
        self._ev('( f1 lt e )', {0, 1, 2})
        self._ev('( f2 lt q )', {0, 1, 2, 3, 9})
        self._ev('( f2 lt r )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 lt b )', {0, 3, 4})

    def test_11_lt_06_not_parentheses_01(self):
        self._ev('not ( f1 lt e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 lt q )', {4, 5, 6, 7, 8})
        self._ev('not ( f2 lt r )', {5, 6, 7, 8})
        self._ev('not ( f4 lt b )', {1, 2, 5, 6, 7, 8, 9})

    def test_11_lt_06_not_parentheses_02(self):
        self._ev('( not f1 lt e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f2 lt q )', {4, 5, 6, 7, 8})
        self._ev('( not f2 lt r )', {5, 6, 7, 8})
        self._ev('( not f4 lt b )', {1, 2, 5, 6, 7, 8, 9})

    def test_11_lt_07_not_parentheses(self):
        self._ev('( f1 not lt e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 not lt q )', {4, 5, 6, 7, 8})
        self._ev('( f2 not lt r )', {5, 6, 7, 8})
        self._ev('( f4 not lt b )', {1, 2, 5, 6, 7, 8, 9})

    def test_11_lt_08_not_not_parentheses(self):
        self._ev('not ( f1 not lt e )', {0, 1, 2})
        self._ev('not ( f2 not lt q )', {0, 1, 2, 3, 9})
        self._ev('not ( f2 not lt r )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 not lt b )', {0, 3, 4})

    def test_12_le_01(self):
        self._ev('f1 le e', {0, 1, 2, 3})
        self._ev('f2 le q', {0, 1, 2, 3, 4, 9})
        self._ev('f2 le r', {0, 1, 2, 3, 4, 9})
        self._ev('f4 le b', {0, 3, 4, 5})

    def test_12_le_02_not(self):
        self._ev('f1 not le e', {4, 5, 6, 7, 8, 9})
        self._ev('f2 not le q', {5, 6, 7, 8})
        self._ev('f2 not le r', {5, 6, 7, 8})
        self._ev('f4 not le b', {1, 2, 6, 7, 8, 9})

    def test_12_le_03_not(self):
        self._ev('not f1 le e', {4, 5, 6, 7, 8, 9})
        self._ev('not f2 le q', {5, 6, 7, 8})
        self._ev('not f2 le r', {5, 6, 7, 8})
        self._ev('not f4 le b', {1, 2, 6, 7, 8, 9})

    def test_12_le_04_not_not(self):
        self._ev('not f1 not le e', {0, 1, 2, 3})
        self._ev('not f2 not le q', {0, 1, 2, 3, 4, 9})
        self._ev('not f2 not le r', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 not le b', {0, 3, 4, 5})

    def test_12_le_05_parentheses(self):
        self._ev('( f1 le e )', {0, 1, 2, 3})
        self._ev('( f2 le q )', {0, 1, 2, 3, 4, 9})
        self._ev('( f2 le r )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 le b )', {0, 3, 4, 5})

    def test_12_le_06_not_parentheses_01(self):
        self._ev('not ( f1 le e )', {4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 le q )', {5, 6, 7, 8})
        self._ev('not ( f2 le r )', {5, 6, 7, 8})
        self._ev('not ( f4 le b )', {1, 2, 6, 7, 8, 9})

    def test_12_le_06_not_parentheses_02(self):
        self._ev('( not f1 le e )', {4, 5, 6, 7, 8, 9})
        self._ev('( not f2 le q )', {5, 6, 7, 8})
        self._ev('( not f2 le r )', {5, 6, 7, 8})
        self._ev('( not f4 le b )', {1, 2, 6, 7, 8, 9})

    def test_12_le_07_not_parentheses(self):
        self._ev('( f1 not le e )', {4, 5, 6, 7, 8, 9})
        self._ev('( f2 not le q )', {5, 6, 7, 8})
        self._ev('( f2 not le r )', {5, 6, 7, 8})
        self._ev('( f4 not le b )', {1, 2, 6, 7, 8, 9})

    def test_12_le_08_not_not_parentheses(self):
        self._ev('not ( f1 not le e )', {0, 1, 2, 3})
        self._ev('not ( f2 not le q )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f2 not le r )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 not le b )', {0, 3, 4, 5})

    def test_13_ge_01(self):
        self._ev('f1 ge e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 ge q', {3, 4, 5, 6, 7, 8})
        self._ev('f2 ge r', {4, 5, 6, 7, 8})
        self._ev('f4 ge b', {1, 2, 5, 6, 7, 8, 9})

    def test_13_ge_02_not(self):
        self._ev('f1 not ge e', {0, 1, 2})
        self._ev('f2 not ge q', {0, 1, 2, 9})
        self._ev('f2 not ge r', {0, 1, 2, 3, 9})
        self._ev('f4 not ge b', {0, 3, 4})

    def test_13_ge_03_not(self):
        self._ev('not f1 ge e', {0, 1, 2})
        self._ev('not f2 ge q', {0, 1, 2, 9})
        self._ev('not f2 ge r', {0, 1, 2, 3, 9})
        self._ev('not f4 ge b', {0, 3, 4})

    def test_13_ge_04_not_not(self):
        self._ev('not f1 not ge e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 not ge q', {3, 4, 5, 6, 7, 8})
        self._ev('not f2 not ge r', {4, 5, 6, 7, 8})
        self._ev('not f4 not ge b', {1, 2, 5, 6, 7, 8, 9})

    def test_13_ge_05_parentheses(self):
        self._ev('( f1 ge e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 ge q )', {3, 4, 5, 6, 7, 8})
        self._ev('( f2 ge r )', {4, 5, 6, 7, 8})
        self._ev('( f4 ge b )', {1, 2, 5, 6, 7, 8, 9})

    def test_13_ge_06_not_parentheses_01(self):
        self._ev('not ( f1 ge e )', {0, 1, 2})
        self._ev('not ( f2 ge q )', {0, 1, 2, 9})
        self._ev('not ( f2 ge r )', {0, 1, 2, 3, 9})
        self._ev('not ( f4 ge b )', {0, 3, 4})

    def test_13_ge_06_not_parentheses_02(self):
        self._ev('( not f1 ge e )', {0, 1, 2})
        self._ev('( not f2 ge q )', {0, 1, 2, 9})
        self._ev('( not f2 ge r )', {0, 1, 2, 3, 9})
        self._ev('( not f4 ge b )', {0, 3, 4})

    def test_13_ge_07_not_parentheses(self):
        self._ev('( f1 not ge e )', {0, 1, 2})
        self._ev('( f2 not ge q )', {0, 1, 2, 9})
        self._ev('( f2 not ge r )', {0, 1, 2, 3, 9})
        self._ev('( f4 not ge b )', {0, 3, 4})

    def test_13_ge_08_not_not_parentheses(self):
        self._ev('not ( f1 not ge e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 not ge q )', {3, 4, 5, 6, 7, 8})
        self._ev('not ( f2 not ge r )', {4, 5, 6, 7, 8})
        self._ev('not ( f4 not ge b )', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_01(self):
        self._ev('f1 before e', {0, 1, 2})
        self._ev('f2 before q', {0, 1, 2, 3, 9})
        self._ev('f2 before r', {0, 1, 2, 3, 4, 9})
        self._ev('f4 before b', {0, 3, 4})

    def test_14_before_02_not(self):
        self._ev('f1 not before e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('f2 not before q', {4, 5, 6, 7, 8})
        self._ev('f2 not before r', {5, 6, 7, 8})
        self._ev('f4 not before b', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_03_not(self):
        self._ev('not f1 before e', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not f2 before q', {4, 5, 6, 7, 8})
        self._ev('not f2 before r', {5, 6, 7, 8})
        self._ev('not f4 before b', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_04_not_not(self):
        self._ev('not f1 not before e', {0, 1, 2})
        self._ev('not f2 not before q', {0, 1, 2, 3, 9})
        self._ev('not f2 not before r', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 not before b', {0, 3, 4})

    def test_14_before_05_parentheses(self):
        self._ev('( f1 before e )', {0, 1, 2})
        self._ev('( f2 before q )', {0, 1, 2, 3, 9})
        self._ev('( f2 before r )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 before b )', {0, 3, 4})

    def test_14_before_06_not_parentheses_01(self):
        self._ev('not ( f1 before e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 before q )', {4, 5, 6, 7, 8})
        self._ev('not ( f2 before r )', {5, 6, 7, 8})
        self._ev('not ( f4 before b )', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_06_not_parentheses_02(self):
        self._ev('( not f1 before e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('( not f2 before q )', {4, 5, 6, 7, 8})
        self._ev('( not f2 before r )', {5, 6, 7, 8})
        self._ev('( not f4 before b )', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_07_not_parentheses(self):
        self._ev('( f1 not before e )', {3, 4, 5, 6, 7, 8, 9})
        self._ev('( f2 not before q )', {4, 5, 6, 7, 8})
        self._ev('( f2 not before r )', {5, 6, 7, 8})
        self._ev('( f4 not before b )', {1, 2, 5, 6, 7, 8, 9})

    def test_14_before_08_not_not_parentheses(self):
        self._ev('not ( f1 not before e )', {0, 1, 2})
        self._ev('not ( f2 not before q )', {0, 1, 2, 3, 9})
        self._ev('not ( f2 not before r )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 not before b )', {0, 3, 4})

    def test_15_after_01(self):
        self._ev('f1 after e', {4, 5, 6, 7, 8, 9})
        self._ev('f2 after q', {4, 5, 6, 7, 8})
        self._ev('f2 after r', {5, 6, 7, 8})
        self._ev('f4 after b', {1, 2, 6, 7, 8, 9})

    def test_15_after_02_not(self):
        self._ev('f1 not after e', {0, 1, 2, 3})
        self._ev('f2 not after q', {0, 1, 2, 3, 9})
        self._ev('f2 not after r', {0, 1, 2, 3, 4, 9})
        self._ev('f4 not after b', {0, 3, 4, 5})

    def test_15_after_03_not(self):
        self._ev('not f1 after e', {0, 1, 2, 3})
        self._ev('not f2 after q', {0, 1, 2, 3, 9})
        self._ev('not f2 after r', {0, 1, 2, 3, 4, 9})
        self._ev('not f4 after b', {0, 3, 4, 5})

    def test_15_after_04_not_not(self):
        self._ev('not f1 not after e', {4, 5, 6, 7, 8, 9})
        self._ev('not f2 not after q', {4, 5, 6, 7, 8})
        self._ev('not f2 not after r', {5, 6, 7, 8})
        self._ev('not f4 not after b', {1, 2, 6, 7, 8, 9})

    def test_15_after_05_parentheses(self):
        self._ev('( f1 after e )', {4, 5, 6, 7, 8, 9})
        self._ev('( f2 after q )', {4, 5, 6, 7, 8})
        self._ev('( f2 after r )', {5, 6, 7, 8})
        self._ev('( f4 after b )', {1, 2, 6, 7, 8, 9})

    def test_15_after_06_not_parentheses_01(self):
        self._ev('not ( f1 after e )', {0, 1, 2, 3})
        self._ev('not ( f2 after q )', {0, 1, 2, 3, 9})
        self._ev('not ( f2 after r )', {0, 1, 2, 3, 4, 9})
        self._ev('not ( f4 after b )', {0, 3, 4, 5})

    def test_15_after_06_not_parentheses_02(self):
        self._ev('( not f1 after e )', {0, 1, 2, 3})
        self._ev('( not f2 after q )', {0, 1, 2, 3, 9})
        self._ev('( not f2 after r )', {0, 1, 2, 3, 4, 9})
        self._ev('( not f4 after b )', {0, 3, 4, 5})

    def test_15_after_07_not_parentheses(self):
        self._ev('( f1 not after e )', {0, 1, 2, 3})
        self._ev('( f2 not after q )', {0, 1, 2, 3, 9})
        self._ev('( f2 not after r )', {0, 1, 2, 3, 4, 9})
        self._ev('( f4 not after b )', {0, 3, 4, 5})

    def test_15_after_08_not_not_parentheses(self):
        self._ev('not ( f1 not after e )', {4, 5, 6, 7, 8, 9})
        self._ev('not ( f2 not after q )', {4, 5, 6, 7, 8})
        self._ev('not ( f2 not after r )', {5, 6, 7, 8})
        self._ev('not ( f4 not after b )', {1, 2, 6, 7, 8, 9})


class Processors:

    def __init__(self, non_indexed_fields, indexed_fields, records):

        # Emulate the database interface available in real cases to do field
        # defined for file check in condition() method.  The Database.exists()
        # method is called.
        self.filename = 'file'
        self.database = Database(
            {self.filename: non_indexed_fields.union(indexed_fields)})

        # Real cases currently have exactly one non_indexed_field and zero or
        # more indexed fields where ifn = Fn(non_if) for each indexed field n
        # for each value of the non-indexed field.
        self.non_indexed_fields = non_indexed_fields
        self.indexed_fields = indexed_fields
        self.records = records
        self.existence = {e for e, r in enumerate(records)}
        
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
        
    def condition(self, obj):
        if not self.database.exists(self.filename, obj.field):
            return None
        if obj.condition in {where.IS, where.LIKE, where.PRESENT}:
            case = (obj.condition, None)
        elif obj.num is True:
            case = (obj.condition, False)
        else:
            case = (obj.condition, True)

        # Let parser stop 'field not is not value' 'field not is value'
        # 'field is not value' is only case of 'field <condition> not value'
        obj.result.answer = self.compare_field_value[case](obj)
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            obj.result.answer = self.existence - obj.result.answer
        
    def not_condition(self, obj):
        if bool(obj.not_condition) ^ bool(obj.not_phrase):
            obj.result.answer = self.existence - obj.result.answer
    
    def operator(self, obj):
        obj.left.result.answer = self.boolean_operation[obj.operator](obj)
        obj.result = obj.left.result
    
    def answer(self, obj):

        # Currently I think the 'not's must be applied to the 'up' node only.
        obj.up.result = obj.result
        self.not_condition(obj.up)
    
    def initialize_answer(self, obj):
        obj.result.answer = set()
    
    def get_existence(self):
        return self.existence.copy()
    
    def get_record(self, recordset):
        for record_number in recordset:
            yield record_number, self.records[record_number]
        
    def non_index_condition(self, obj, record_number, record):
        if obj.condition == where.IS:
            if obj.not_value:
                self._is_not(obj, record_number, record)
        elif obj.condition == where.NE:
            self._ne(obj, record_number, record)
        elif obj.condition == where.PRESENT:
            self._present(obj, record_number, record)
        elif obj.condition == where.LIKE:
            self._like(obj, record_number, record)

    def _is(self, obj):
        # 'field is value' and 'field is not value' are allowed
        recordset = set()
        if obj.not_value:
            raise RuntimeError('_is')
        else:
            for e, r in enumerate(self.records):
                f = r.get(obj.field)
                if f:
                    if obj.value in f:
                        recordset.add(e)
        return recordset

    def _is_not(self, obj, record_number, record):
        # 'field is value' and 'field is not value' are allowed
        if obj.not_value:
            f = record.get(obj.field)
            if f:
                for v in f:
                    if v != obj.value:
                        obj.result.answer.add(record_number)
                        break
        else:
            raise RuntimeError('_is_not')

    def _like_by_index(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if re.match('.*?' + obj.value,
                                v,
                                flags=re.IGNORECASE|re.DOTALL):
                        recordset.add(e)
                        break
        return recordset

    def _starts_by_index(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v.startswith(obj.value):
                        recordset.add(e)
                        break
        return recordset

    def _like(self, obj, record_number, record):
        f = record.get(obj.field)
        if f:
            for v in f:
                if re.search(obj.value, v):
                    obj.result.answer.add(record_number)
                    break

    def _starts(self, obj, record_number, record):
        pass
    
    def _present(self, obj, record_number, record):
        if record.get(obj.field):
            obj.result.answer.add(record_number)

    def _eq(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                if obj.value in f:
                    recordset.add(e)
        return recordset

    def _ne(self, obj, record_number, record):
        f = record.get(obj.field)
        if f:
            for v in f:
                if v != obj.value:
                    obj.result.answer.add(record_number)
                    break

    def _gt(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v > obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _lt(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v < obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _le(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v <= obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _ge(self, obj):
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v >= obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _before(self, obj):
        # Can use same logic as _lt in this test.
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v < obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _after(self, obj):
        # Can use same logic as _gt in this test.
        recordset = set()
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v > obj.value:
                        recordset.add(e)
                        break
        return recordset

    def _from_to(self, obj):
        recordset = set()
        from_, to = obj.value
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v >= from_ and v <= to:
                        recordset.add(e)
                        break
        return recordset

    def _from_below(self, obj):
        recordset = set()
        from_, below = obj.value
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v >= from_ and v < below:
                        recordset.add(e)
                        break
        return recordset

    def _above_to(self, obj):
        recordset = set()
        above, to = obj.value
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v > above and v <= to:
                        recordset.add(e)
                        break
        return recordset

    def _above_below(self, obj):
        recordset = set()
        above, below = obj.value
        for e, r in enumerate(self.records):
            f = r.get(obj.field)
            if f:
                for v in f:
                    if v > above and v < below:
                        recordset.add(e)
                        break
        return recordset

    def _and(self, obj):
        return obj.left.result.answer.intersection(obj.result.answer)

    def _nor(self, obj):
        return obj.left.result.answer.intersection(
            self.existence - obj.result.answer)

    def _or(self, obj):
        return obj.left.result.answer.union(obj.result.answer)


class Constraints(Processors):
    # Override the actions to evaluate non-index conditions.
    # This allows the constraints on record scans to be preserved for
    # comparison with expected values.
    # The constraints refer to objects which are modified in real actions.
        
    def non_index_condition(self, obj, record_number, record):
        pass
        
    def not_condition(self, obj):
        pass
        
    def operator(self, obj):
        pass
        
    def answer(self, obj):
        pass


class Database():

    def __init__(self, dbset):

        # Set a dictionary of field names in file named dbset.
        self.dbset = dbset

    # Where.validate() calls object.exists() to see if field <dbname> is in file
    # <dbset>.
    def exists(self, dbset, dbname):
        return dbname in self.dbset[dbset]


class ProcessorsTC(unittest.TestCase):

    def setUp(self):
        self.f = Find('a', 'b')
        self.p = Processors(set(), set(), [])

    def tearDown(self):
        self.f = None
        self.p = None

    def test____assumptions(self):
        """Check Processors has the same interface as Find.

        Are the same methods available with the same signatures?

        """
        msg = 'Failure of this test invalidates all other tests'

        # The attributes missing from Processors are methods used within Find
        # as helpers, within other methods, which are meaningless in Processors.
        # Those other methods in Processors do not need the help.
        self.assertEqual(set(Find.__dict__) - set(Processors.__dict__),
                         {'_lt_key', '_gt_key', 'dbset', 'db'},
                         msg=msg)

        f = self.f
        fas = set(dir(f))
        p = self.p

        # Compare the signatures of the methods.
        for a in dir(p):
            if not a.startswith('__'):
                at = getattr(p, a)
                if not isinstance(at,(dict, list, set, str)):
                    if hasattr(at, '__name__'):
                        self.assertIn(a, fas, msg=msg)
                        fac = getattr(f, a).__code__
                        pac = getattr(p, a).__code__
                        argcount = pac.co_argcount
                        self.assertEqual(argcount, fac.co_argcount, msg=msg)
                        self.assertEqual(pac.co_varnames[:argcount],
                                         fac.co_varnames[:argcount],
                                         msg=msg)

    def test_compare_method_lookup(self):
        f = self.f
        p = self.p
        self.assertEqual(
            p.compare_field_value,
            {(where.IS, None): p._is,
             (where.LIKE, None): p._like_by_index,
             (where.STARTS, None): p._starts_by_index,
             (where.PRESENT, None): p._present,
             (where.EQ, True): p._eq,
             (where.NE, True): p._ne,
             (where.GT, True): p._gt,
             (where.LT, True): p._lt,
             (where.LE, True): p._le,
             (where.GE, True): p._ge,
             (where.BEFORE, True): p._before,
             (where.AFTER, True): p._after,
             ((where.FROM, where.TO), True): p._from_to,
             ((where.FROM, where.BELOW), True): p._from_below,
             ((where.ABOVE, where.TO), True): p._above_to,
             ((where.ABOVE, where.BELOW), True): p._above_below,
             (where.EQ, False): p._eq,
             (where.NE, False): p._ne,
             (where.GT, False): p._gt,
             (where.LT, False): p._lt,
             (where.LE, False): p._le,
             (where.GE, False): p._ge,
             (where.BEFORE, False): p._before,
             (where.AFTER, False): p._after,
             ((where.FROM, where.TO), False): p._from_to,
             ((where.FROM, where.BELOW), False): p._from_below,
             ((where.ABOVE, where.TO), False): p._above_to,
             ((where.ABOVE, where.BELOW), False): p._above_below,
             })
        self.assertEqual(set(p.compare_field_value), set(f.compare_field_value))
        for k, v in p.compare_field_value.items():
            self.assertEqual(v.__name__, f.compare_field_value[k].__name__)

    def test_operator_method_lookup(self):
        f = self.f
        p = self.p
        self.assertEqual(
            p.boolean_operation,
            {where.AND: p._and,
             where.NOR: p._nor,
             where.OR: p._or,
             })
        self.assertEqual(set(p.boolean_operation), set(f.boolean_operation))
        for k, v in p.boolean_operation.items():
            self.assertEqual(v.__name__, f.boolean_operation[k].__name__)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(WhereTC))
    runner().run(loader(Where_MethodsTC))
    runner().run(loader(Where_errorTC))
    runner().run(loader(Where_lex_phraseTC))
    runner().run(loader(Where_parse_phraseTC))
    runner().run(loader(Where_parse_and_nor_or_phraseTC))
    runner().run(loader(Where_parse_multi_phraseTC))
    runner().run(loader(Where_parse_not_phrasesTC))
    runner().run(loader(Where_parse_multi_not_phrasesTC))
    runner().run(loader(Where_parse_parenthesis_phrasesTC))
    runner().run(loader(Where_validateTC))

    # These tests replicate Where.evaluate() processing up to point where the
    # named method has been called to allow tests on Where instance state.
    runner().run(loader(WhereClause_evaluate_index_condition_nodeTC))
    ##runner().run(loader(WhereClause_set_non_index_node_constraintTC))
    ##runner().run(loader(Where_evaluate_non_index_conditionsTC))
    ##runner().run(loader(WhereClause_evaluate_node_resultTC))

    runner().run(loader(Where_evaluateTC))
    runner().run(loader(ProcessorsTC))
