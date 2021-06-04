# where.py
# Copyright (c) 2015 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Record selection statement parser.

Approximately equivalent to SQL Select statement where clause, and DPT Find
statement, retrieval conditions.

The syntax is:

[NOT] phrase [<AND|OR|NOR> [NOT] phrase] ...

where phrase is one of:

fieldname IS [NOT] value
fieldname [NOT] LIKE pattern
fieldname [NOT] STARTS value
fieldname [NOT] PRESENT
fieldname [NOT] [NUM|ALPHA] <EQ|NE|GT|LT|LE|GE|BEFORE|AFTER> value
fieldname [NOT] [NUM|ALPHA] <FROM|ABOVE> value <TO|BELOW> value

| indicates choices.
[] indicates optional items and <> indicates choice in non-optional items.

Priority of operators is, in decreasing order: NOT NOR AND OR.

Parentheses can be placed around a sequence of phrases to override the normal
priority order.  The phrases within parentheses are evaluated before phrases
outside parentheses.

It is possible for "fieldname IS NOT value" to give a different answer than
"fieldname NOT EQ value" or "fieldname NE value" or "NOT fieldmae EQ value",
particularly the last of these.

"""

import re
import sre_constants
from tkinter import simpledialog

from .constants import SECONDARY

DOUBLE_QUOTE_STRING = '".*?"'
SINGLE_QUOTE_STRING = "'.*?'"
LEFT_PARENTHESIS = "("
RIGHT_PARENTHESIS = ")"
OR = "or"
TO = "to"
IS = "is"
EQ = "eq"
NE = "ne"
LT = "lt"
LE = "le"
GT = "gt"
GE = "ge"
NOT = "not"
NOR = "nor"
AND = "and"
NUM = "num"
LIKE = "like"
FROM = "from"
ALPHA = "alpha"
ABOVE = "above"
AFTER = "after"
BELOW = "below"
BEFORE = "before"
STARTS = "starts"
PRESENT = "present"
STRING = ".+?"

LEADING_SPACE = r"(?<=\s)"
TRAILING_SPACE = r"(?=\s)"

WHERE_RE = re.compile(
    "|".join(
        (
            DOUBLE_QUOTE_STRING,
            SINGLE_QUOTE_STRING,
            "".join(("\\", LEFT_PARENTHESIS)),
            "".join(("\\", RIGHT_PARENTHESIS)),
            OR.join((r"(?<=\s|\))", r"(?=\s|\()")),
            TO.join((LEADING_SPACE, TRAILING_SPACE)),
            IS.join((LEADING_SPACE, TRAILING_SPACE)),
            EQ.join((LEADING_SPACE, TRAILING_SPACE)),
            NE.join((LEADING_SPACE, TRAILING_SPACE)),
            LT.join((LEADING_SPACE, TRAILING_SPACE)),
            LE.join((LEADING_SPACE, TRAILING_SPACE)),
            GT.join((LEADING_SPACE, TRAILING_SPACE)),
            GE.join((LEADING_SPACE, TRAILING_SPACE)),
            NOT.join((r"\A", r"(?=\s|\()")),
            NOT.join((r"(?<=\s|\()", r"(?=\s|\()")),
            NOR.join((r"(?<=\s|\()", r"(?=\s|\()")),
            AND.join((r"(?<=\s|\))", r"(?=\s|\()")),
            NUM.join((LEADING_SPACE, TRAILING_SPACE)),
            LIKE.join((LEADING_SPACE, TRAILING_SPACE)),
            FROM.join((LEADING_SPACE, TRAILING_SPACE)),
            ALPHA.join((LEADING_SPACE, TRAILING_SPACE)),
            ABOVE.join((LEADING_SPACE, TRAILING_SPACE)),
            AFTER.join((LEADING_SPACE, TRAILING_SPACE)),
            BELOW.join((LEADING_SPACE, TRAILING_SPACE)),
            BEFORE.join((LEADING_SPACE, TRAILING_SPACE)),
            STARTS.join((LEADING_SPACE, TRAILING_SPACE)),
            PRESENT.join((LEADING_SPACE, r"(?=\s|\)|\Z)")),
            STRING,
        )
    ).join(("(", ")")),
    flags=re.IGNORECASE | re.DOTALL,
)

KEYWORDS = frozenset(
    (
        LEFT_PARENTHESIS,
        RIGHT_PARENTHESIS,
        OR,
        TO,
        IS,
        EQ,
        NE,
        LT,
        LE,
        GT,
        GE,
        NOT,
        NOR,
        AND,
        NUM,
        LIKE,
        FROM,
        ALPHA,
        ABOVE,
        AFTER,
        BELOW,
        BEFORE,
        STARTS,
        PRESENT,
    )
)
SINGLE_CONDITIONS = frozenset((EQ, NE, LT, LE, GT, GE, AFTER, BEFORE))
FIRST_CONDITIONS = frozenset((ABOVE, FROM))
SECOND_CONDITIONS = frozenset((TO, BELOW))
ALPHANUMERIC = frozenset((ALPHA, NUM))
BOOLEAN = frozenset((AND, OR, NOR))
STRUCTURE_TOKENS = frozenset(
    (
        LEFT_PARENTHESIS,
        RIGHT_PARENTHESIS,
        None,
    )
)


class WhereError(Exception):
    """Exception for Where class."""


class Where:
    """Find records matching the query in statement."""

    def __init__(self, statement):
        """Create Where instance for statement."""
        self.statement = statement
        self.node = None
        self.tokens = None
        self._error_information = WhereStatementError(statement)
        self._processors = None
        self._f_or_v = None
        self._not = None

    @property
    def error_information(self):
        """Return WhereStatementError object for the Where object."""
        return self._error_information

    def lex(self):
        """Split instance's statement into tokens."""
        tokens = []
        strings = []
        for word in WHERE_RE.split(self.statement):
            if word.lower() in KEYWORDS:
                if strings:
                    tokens.append("".join([_trim(s) for s in strings if s]))
                    strings.clear()
                tokens.append(word.lower())
            elif word.strip():
                strings.append(word.strip())
        if strings:
            tokens.append("".join([_trim(s) for s in strings if s]))
            strings.clear()
        self.tokens = tokens

    def parse(self):
        """Parse instance's tokens to create node structure to do query."""
        self.node = WhereClause()
        state = self._set_field_not_leftp_start
        for i, token in enumerate(self.tokens):
            state = state(token)
            if not state:
                self._error_information.tokens = self.tokens[: i + 1]
                break
        else:
            if self._f_or_v is not None:
                self._deferred_value()
            if self.node.up is not None:
                self.node = self.node.up

    def validate(self, db, dbset):
        """Return None if query is valid, or a WhereStatementError instance."""
        if self._error_information.tokens:
            return self._error_information
        if self.node is None:
            return None
        clauses = self.node.get_root().get_clauses_from_root_in_walk_order()

        # Valid nodes, considering (field, condition, value), are like
        # (str, str, str) or (None, None, None).  Mis-spelling a condition
        # will produce the field name ' '.join((field, condition, value, ...))
        # but mis-spelling an operator will produce the value ' '.join((value,
        # operator, field)) and ignore some tokens.
        fields = set()
        for node in clauses:
            if node.field is not None:
                if not db.exists(dbset, node.field):
                    fields.add(node.field)

        # field attribute of each clause must be None or exist as a field.
        if fields:
            self._error_information.fields = fields
            return self._error_information

        return None

    # Probably a good thing to provide, but introduced to cope with inability
    # to put '(' or ')' directly in values because it is picked as a reserved
    # word.
    def fill_placeholders(self, replacements=None):
        """Substitute replacement values or prompt for value if none supplied."""
        if self.node is None:
            return
        if replacements is None:
            replacements = {}
        for node in self.node.get_clauses_from_root_in_walk_order():
            value = node.value
            if value is not None:
                if value.startswith("?") and value.endswith("?"):
                    if value in replacements:
                        node.value = replacements.pop(value)
                    else:
                        node.value = simpledialog.askstring(
                            "Supply replacement value",
                            "".join(("Placeholder is ", value)),
                        )
                        # raise WhereError(
                        #    ''.join(('Expected replacement value for ',
                        #             value,
                        #             ' is missing.')))

    def evaluate(self, processors):
        """Evaluate the query.

        The answer to the query defined in instance's statement is put in the
        self.node.result attribute.

        """
        if self.node is None:
            return
        self._processors = processors
        try:
            node = self.node.get_root()
            node.evaluate_index_condition_node(self._index_rules_engine)
            if node.result is not None:
                return
            non_index_nodes = []
            node.get_non_index_condition_node(non_index_nodes)
            node.constraint.result = WhereResult()
            node.constraint.result.answer = processors.get_existence()
            node.set_non_index_node_constraint(processors.initialize_answer)
            self._evaluate_non_index_conditions(non_index_nodes)
            node.evaluate_node_result(self._result_rules_engine)
        finally:
            self._processors = None

    def close_all_nodes_except_answer(self):
        """Destroy the intermediate recordsets held in the node tree.

        Do nothing.

        where.WhereClause instances in the where.Where.node tree are destroyed
        completely as a consequence of destroying self.

        """

    def get_node_result_answer(self):
        """Return the recordset answer for the query.

        The processors argument is ignored: it is present for compatibility
        with the version of this method in the where_dpt module.

        """
        return self.node.result.answer

    def _evaluate_non_index_conditions(self, non_index_nodes):
        """Evaluate the conditions in the non-index nodes.

        Create a temporary node containing all records in at least one
        non-index node and process these records using the condition in
        each non-index node.

        """
        processors = self._processors
        constraint = WhereConstraint()
        constraint.result = WhereResult()
        processors.initialize_answer(constraint)
        for condition in {n.constraint for n in non_index_nodes}:
            if condition.result:
                constraint.result.answer |= condition.result.answer
            else:
                constraint.result.answer = processors.get_existence()
                break
        for record in processors.get_record(constraint.result.answer):
            for node in non_index_nodes:
                processors.non_index_condition(node, *record)
        for node in non_index_nodes:
            processors.not_condition(node)

    def _index_rules_engine(self, operator, obj):
        """Evaluate the rule in each node where an index is available."""
        if operator in {
            IS,
            LIKE,
            STARTS,
            PRESENT,
            EQ,
            NE,
            GT,
            LT,
            LE,
            GE,
            BEFORE,
            AFTER,
            (FROM, TO),
            (FROM, BELOW),
            (ABOVE, TO),
            (ABOVE, BELOW),
        }:
            obj.result = WhereResult()
            self._processors.condition(obj)
        elif operator in {NOR, AND, OR}:
            self._processors.operator(obj)
        else:
            self._processors.answer(obj)

    def _result_rules_engine(self, operator, obj):
        """Combine the results evaluated for each node."""
        if operator in {
            EQ,
            GT,
            LT,
            LE,
            GE,
            BEFORE,
            AFTER,
            (FROM, TO),
            (FROM, BELOW),
            (ABOVE, TO),
            (ABOVE, BELOW),
        }:
            pass
        elif operator in {
            LIKE,
            STARTS,
            PRESENT,
            NE,
        }:
            self._processors.not_condition(obj)
        elif operator == IS:
            if obj.not_value:
                self._processors.not_condition(obj)
        elif operator in {NOR, AND, OR}:
            if obj.result is not obj.left.result:
                self._processors.operator(obj)
        else:
            if obj.result is not obj.up.result:
                self._processors.answer(obj)

    def _set_field_not_leftp_start(self, token):
        """Expect fieldname, 'not', or '(' at start."""
        token_lower = token.lower()
        if token_lower == NOT:
            self._first_token_invert(token)
            return self._set_field_leftp
        if token_lower == LEFT_PARENTHESIS:
            self._first_token_left_parenthesis(token)
            return self._set_field_not_leftp
        if token_lower not in KEYWORDS:
            self._first_token_field(token)
            return self._set_not_num_alpha_condition
        return self.error(token)

    def _set_field_leftp(self, token):
        """Expect fieldname, or '(', after 'not'."""
        token_lower = token.lower()
        if token_lower == LEFT_PARENTHESIS:
            self._boolean_left_parenthesis(token)
            return self._set_field_not_leftp
        if token_lower not in KEYWORDS:
            self._deferred_not_phrase()
            self.node.field = token
            return self._set_not_num_alpha_condition
        return self.error(token)

    def _set_field_not_leftp(self, token):
        """Expect fieldname, 'not', or '(', after '(' or boolean."""
        token_lower = token.lower()
        if token_lower == NOT:
            self._not = True
            return self._set_field_leftp
        if token_lower == LEFT_PARENTHESIS:
            self._boolean_left_parenthesis(token)
            return self._set_field_not_leftp
        if token_lower not in KEYWORDS:
            self._deferred_not_phrase()
            self.node.field = token
            return self._set_not_num_alpha_condition
        return self.error(token)

    def _set_not_num_alpha_condition(self, token):
        """Expect 'not', 'num', 'alpha', or a condition, after fieldname."""
        token_lower = token.lower()
        if token_lower == NOT:
            self._not = True
            return self._set_num_alpha_condition
        return self._set_num_alpha_condition(token)

    def _set_num_alpha_condition(self, token):
        """Expect 'num', 'alpha', or condition, after fieldname or 'not'."""
        token_lower = token.lower()
        if token_lower == IS:

            # Rather than a separate state in _set_not_num_alpha_condition
            # for the token_lower == NOT case because 'is' is never preceded by 'not'.
            if self._not:
                return self.error(token)
            self.node.condition = token_lower
            return self._set_not_value

        if token_lower == LIKE:
            self._deferred_not_condition()
            self.node.condition = token_lower
            return self._set_value_like
        if token_lower == STARTS:
            self._deferred_not_condition()
            self.node.condition = token_lower
            return self._set_value
        if token_lower == PRESENT:
            self._deferred_not_condition()
            self.node.condition = token_lower
            return self._set_and_or_nor_rightp__double_condition_or_present
        if token_lower in ALPHANUMERIC:
            self._deferred_not_condition()
            self._alphanum_condition(token_lower)
            return self._set_condition
        return self._set_condition(token)

    def _set_condition(self, token):
        """Expect condition after 'alpha' or 'num'."""
        token_lower = token.lower()
        if token_lower in SINGLE_CONDITIONS:
            self._deferred_not_condition()
            self.node.condition = token_lower
            return self._set_value
        if token_lower in FIRST_CONDITIONS:
            self._deferred_not_condition()
            self.node.condition = token_lower
            return self._set_first_value
        return self.error(token)

    def _set_second_condition(self, token):
        """Expect second condition after first value of double condition."""
        token_lower = token.lower()
        if token_lower in SECOND_CONDITIONS:
            self.node.condition = self.node.condition, token_lower
            return self._set_second_value
        return self.error(token)

    def _set_not_value(self, token):
        """Expect 'not', or a value, after a condition."""
        token_lower = token.lower()
        if token_lower == NOT:
            self.node.not_value = True
            return self._set_value
        return self._set_value(token)

    def _set_value(self, token):
        """Expect value after single condition."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            self.node.value = token
            return self._set_and_or_nor_rightp__single_condition
        return self.error(token)

    def _set_value_like(self, token):
        """Expect value, a regular expression, after like."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            try:
                re.compile(token, flags=re.IGNORECASE | re.DOTALL)
            except sre_constants.error:
                return self.error(token)
            self.node.value = token
            return self._set_and_or_nor_rightp__single_condition
        return self.error(token)

    def _set_first_value(self, token):
        """Expect first value after first condition in double condition."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            self.node.value = token
            return self._set_second_condition
        return self.error(token)

    def _set_second_value(self, token):
        """Expect second value after second condition in double condition."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            self.node.value = self.node.value, token
            return self._set_and_or_nor_rightp__single_condition
        return self.error(token)

    def _set_and_or_nor_rightp__single_condition(self, token):
        """Expect boolean or rightp after value in single condition phrase.

        The construct 'field eq value1 or value2 or ...' makes sense because a
        condition, 'eq', has been specified.

        The construct 'field eq value1 or gt value2 or ...' makes sense but may
        express redundant or contradictory conditions.

        """
        token_lower = token.lower()
        if token_lower in BOOLEAN:
            self._right_parenthesis_boolean(token_lower)
            return self._set_field_leftp_not_condition_value
        return self._set_rightp(token)

    def _set_and_or_nor_rightp__double_condition_or_present(self, token):
        """Expect boolean or rightp after present or double condition phrase.

        The construct 'field present or value or ...' makes no sense because a
        condition has not been specified.

        The construct 'field present or gt value or ...' does make sense, but
        is likely to express redundant or contradictory conditions.

        The construct 'field from value1 to value2 or ...' is similar because
        there is no way of indicating two values except by use of the 'from',
        'above', 'to', and 'below' keywords.  The condition has to be given as
        in 'field from value1 to value2 or eq value3 or value4 or ...'.

        """
        token_lower = token.lower()
        if token_lower in BOOLEAN:
            self._right_parenthesis_boolean(token_lower)
            return self._set_field_leftp_not_condition
        return self._set_rightp(token)

    def _set_and_or_nor_rightp(self, token):
        """Expect boolean or rightp after rightp.

        A fieldname must be given at start of next phrase.

        """
        token_lower = token.lower()
        if token_lower in BOOLEAN:
            self._right_parenthesis_boolean(token_lower)
            return self._set_field_not_leftp
        return self._set_rightp(token)

    # This is never set as state, but called when ')' is remaining valid token.
    def _set_rightp(self, token):
        """Expect rightp after rightp.

        A fieldname must be given at start of next phrase.

        """
        token_lower = token.lower()
        if token_lower == RIGHT_PARENTHESIS:
            if self.node.up is None:
                raise WhereError("No unmatched left-parentheses")
            self.node = self.node.up
            return self._set_and_or_nor_rightp
        return self.error(token)

    def _set_field_leftp_not_condition(self, token):
        """Expect fieldname, '(', 'not', or condition after rightp."""
        token_lower = token.lower()
        if token_lower == NOT:
            self._not = True
            return self._set_field_leftp_condition
        return self._set_field_leftp_condition(token)

    def _set_field_leftp_condition(self, token):
        """Expect fieldname, '(', or condition after rightp 'not'."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            self._deferred_not_phrase()
            self.node.field = token
            return self._set_not_num_alpha_condition
        return self._set_leftp_condition(token)

    def _set_field_leftp_not_condition_value(self, token):
        """Expect fieldname, '(', 'not', condition, or value, after rightp."""
        token_lower = token.lower()
        if token_lower == NOT:
            self._not = True
            return self._set_field_leftp_condition_value
        return self._set_field_leftp_condition_value(token)

    def _set_field_leftp_condition_value(self, token):
        """Expect fieldname, '(', condition, or value, after rightp 'not'."""
        token_lower = token.lower()
        if token_lower not in KEYWORDS:
            self._f_or_v = token
            return self._set_field_or_value__not
        return self._set_leftp_condition(token)

    def _set_field_or_value__not(self, token):
        """Expect keyword to interpret previous token as field or value."""
        token_lower = token.lower()
        if token_lower == NOT:

            # '... or not f not like b and ...' or similar might be happening
            # so treat existing 'not' as phrase not.
            self._deferred_not_phrase()

            self._not = True
            return self._set_field_or_value
        if token_lower == RIGHT_PARENTHESIS:
            if self._f_or_v is not None:
                self._deferred_value()
            else:
                raise WhereError("No token to use as value")
            if self.node.up is None:
                raise WhereError("No unmatched left-parentheses")
            self.node = self.node.up
            return self._set_and_or_nor_rightp
        self._deferred_not_phrase()
        return self._set_field_or_value(token)

    def _set_field_or_value(self, token):
        """Expect keyword to interpret previous token as field or value."""
        token_lower = token.lower()
        if token_lower in BOOLEAN:
            self._value_boolean(token_lower)
            return self._set_field_leftp_condition_value
        if token_lower == IS:

            # Rather than a separate state in _set_not_num_alpha_condition
            # for the token_lower == NOT case because 'is' is never preceded by 'not'.
            if self._not:
                return self.error(token)
            self._field_condition(token_lower)
            return self._set_not_value

        if token_lower in SINGLE_CONDITIONS:
            self._field_condition(token_lower)
            return self._set_value
        if token_lower in FIRST_CONDITIONS:
            self._field_condition(token_lower)
            return self._set_first_value
        if token_lower == LIKE:
            self._field_condition(token_lower)
            return self._set_value_like
        if token_lower == STARTS:
            self._field_condition(token_lower)
            return self._set_value
        if token_lower == PRESENT:
            self._field_condition(token_lower)
            return self._set_and_or_nor_rightp__double_condition_or_present
        if token_lower in ALPHANUMERIC:
            self._field_condition(token_lower)
            self._alphanum_condition(token_lower)
            return self._set_condition
        return self.error(token)

    # Why is this not the same as _set_num_alpha_condition?
    # Perhaps the question should be the other way round!
    def _set_leftp_condition(self, token):
        """Expect '(' or condition after rightp 'not'.

        Called by methods which deal with fieldnames and values.

        """
        token_lower = token.lower()
        if token_lower == LEFT_PARENTHESIS:
            self._boolean_left_parenthesis(token)
            return self._set_field_not_leftp
        if token_lower == IS:

            # Rather than a separate state in _set_not_num_alpha_condition
            # for the token_lower == NOT case because 'is' is never preceded by 'not'.
            if self._not:
                return self.error(token)
            self.node.condition = token_lower
            return self._set_not_value

        if token_lower in SINGLE_CONDITIONS:
            self._copy_pre_condition()
            self.node.condition = token_lower
            return self._set_value
        if token_lower in FIRST_CONDITIONS:
            self._copy_pre_condition()
            self.node.condition = token_lower
            return self._set_first_value
        if token_lower == LIKE:
            self._copy_pre_like_starts_present()
            self.node.condition = token_lower
            return self._set_value_like
        if token_lower == STARTS:
            self._copy_pre_like_starts_present()
            self.node.condition = token_lower
            return self._set_value
        if token_lower == PRESENT:
            self._copy_pre_like_starts_present()
            self.node.condition = token_lower
            return self._set_and_or_nor_rightp__double_condition_or_present
        if token_lower in ALPHANUMERIC:
            self._copy_pre_alphanumeric()
            self._alphanum_condition(token_lower)
            return self._set_condition
        return self.error(token)

    # Returning False always cannot possibly be correct, surely?
    def error(self, token):
        """Return False."""
        if token.lower() in KEYWORDS:
            return False
        return False

    def _deferred_not_condition(self):
        """Nearest 'not' to left inverts a condition such as 'eq'."""
        if self._not is not None:
            self.node.not_condition = self._not
            self._not = None

    def _deferred_not_phrase(self):
        """Nearest 'not' to left inverts a phrase such as 'field eq value'."""
        if self._not is not None:
            self.node.not_phrase = self._not
            self._not = None

    def _deferred_value(self):
        """Nearest value to left is 'value' in 'field eq value'."""
        if self._f_or_v:
            self._copy_pre_value()
            self.node.value = self._f_or_v
            self._f_or_v = None

    def _copy_pre_field(self):
        """Copy pre-field attributes from nearest node to left."""
        self.node.not_phrase = self.node.left.not_phrase

    def _copy_pre_is(self):
        """Copy pre-is attributes from nearest node to left."""
        self._copy_pre_field()
        self.node.field = self.node.left.field

    def _copy_pre_not_condition(self):
        """Copy pre-'not condition' attributes from nearest node to left."""
        self._copy_pre_field()
        self.node.field = self.node.left.field

    def _copy_pre_like_starts_present(self):
        """Copy pre- like, starts, or present, attributes from node to left."""
        self._copy_pre_not_condition()
        self.node.not_condition = self.node.left.not_condition
        self._deferred_not_condition()

    def _copy_pre_alphanumeric(self):
        """Copy pre-alpha or pre-num attributes from nearest node to left."""
        self._copy_pre_not_condition()
        self.node.not_condition = self.node.left.not_condition
        self._deferred_not_condition()

    def _copy_pre_condition(self):
        """Copy pre-condition attributes from nearest node to left."""
        self._copy_pre_alphanumeric()
        self.node.num = self.node.left.num
        self.node.alpha = self.node.left.alpha

    def _copy_pre_value(self):
        """Copy pre-value attributes from nearest node to left."""
        snl = self.node.left
        if snl.condition == PRESENT:
            raise WhereError("PRESENT phrase followed by value phrase")
        if snl.condition == (FROM, TO):
            raise WhereError("FROM-TO phrase followed by value phrase")
        if snl.condition == (FROM, BELOW):
            raise WhereError("FROM-BELOW phrase followed by value phrase")
        if snl.condition == (ABOVE, TO):
            raise WhereError("ABOVE-TO phrase followed by value phrase")
        if snl.condition == (ABOVE, BELOW):
            raise WhereError("ABOVE-BELOW phrase followed by value phrase")
        if snl.condition == LIKE:
            self._copy_pre_like_starts_present()
        elif snl.condition == STARTS:
            self._copy_pre_like_starts_present()
        elif snl.condition == IS:
            self._copy_pre_is()
        else:
            self._copy_pre_condition()
        self.node.condition = snl.condition

    def _first_token_field(self, token):
        """Set nodes for first token is a field name."""
        node = WhereClause()
        node.field = token
        node.up = self.node
        self.node.down = node
        self.node = node

    def _first_token_left_parenthesis(self, token):
        """Set nodes for first token is '('."""
        node = WhereClause()
        node.down = WhereClause()
        node.down.up = node
        node.up = self.node
        self.node.down = node
        self.node = node.down

    def _boolean_left_parenthesis(self, token):
        """Set nodes for '(' token after 'and', 'or', or 'nor'."""
        self._deferred_not_phrase()
        node = self.node
        node.down = WhereClause()
        node.down.up = node
        self.node = node.down

    def _first_token_invert(self, token):
        """Set nodes for first token is 'not'."""
        node = WhereClause()
        node.not_phrase = True
        node.up = self.node
        self.node.down = node
        self.node = node

    def _right_parenthesis_boolean(self, token):
        """Set nodes for 'and', 'or', or 'nor', after ')' or 'f <cond> v'."""
        node = WhereClause()
        self.node.right = node
        node.up = self.node.up
        node.left = self.node
        self.node = node
        node.operator = token

    def _value_boolean(self, token):
        """Set nodes for 'and', 'or', or 'nor', after 'f <cond> v1 <token> v2'.

        Fill in the assumed field, condition, and invert operations, then
        proceed as if all tokens are present (having been repeated).

        """
        self._copy_pre_value()
        self.node.value = self._f_or_v
        self._f_or_v = None
        self._right_parenthesis_boolean(token)

    def _field_condition(self, token):
        """Set nodes for a condition: nearest value to left is a field name."""
        self.node.field = self._f_or_v
        self._f_or_v = None
        self.node.condition = token
        self._deferred_not_condition()

    def _alphanum_condition(self, token):
        """Set nodes for 'alpha' or 'num'."""
        self.node.alpha = token == ALPHA
        self.node.num = token == NUM


class WhereClause:
    """Phrase in Where specification.

    The Where parser binds WhereClause attributes to the field name, condition,
    and values, found in a phrase of a 'find where' statement.

    The attributes are:

    left - Adjacent node to left, or None.
    right - Adjacent node to right, or None.
    up - Parent node, or None.  Only one node in tree with up == None.
    down - Leftmost child node, or None.
    operator - 'and', 'nor', 'or', or None.  Rule to combine result with
               adjacent node to right.
    field - Name of field on database whose value is compared.
    condition - Comparison rule for field's value with value.
    value - Value to compare with field's value on database.
    not_phrase - True if 'not' applies to phrase.  For example '.. and not ..'.
    not_condition - True if 'not' applies to condition.
                    For example '.. f not eq v ..'.
    not_value - True if 'not' applies to value.  Only '.. f is not v ..'.
    num - Numeric comparison.  For example '.. f num eq v ..'.  Ignored at
        present, all comparisons are alpha.  All values are str: if this is
        implemented it will mean str length counts first in comparisons.
    alpha - Alphabetic comparison.  For example '.. f alpha eq v ..'.  Ignored
        at present, all comparisons are alpha by default.
    result - The answer generated by evaluating the node.
    constraint - Restrictions on result implied by results generated for other
                nodes.

    """

    def __init__(self):
        """Initialiase a node."""
        # Navigation
        self.left = None
        self.right = None
        self.up = None
        self.down = None
        self.operator = None
        # Phrase
        self.field = None
        self.condition = None
        self.value = None
        self.not_phrase = None
        self.not_condition = None
        self.not_value = None
        self.num = None
        self.alpha = None
        # Evaluation
        self.result = None
        self.constraint = None

    def get_root(self):
        """Return root node of tree containing self."""
        node = self
        while True:
            if node.left is None and node.up is None:
                return node
            if node.up is not None:
                node = node.up
            else:
                node = node.left

    def get_clauses_from_current_in_walk_order(self, clauses=None):
        """Add nodes to clauses in walk, down then right, order."""
        if clauses is None:
            clauses = []
        clauses.append(self)
        if self.down is not None:
            self.down.get_clauses_from_current_in_walk_order(clauses=clauses)
        if self.right is not None:
            self.right.get_clauses_from_current_in_walk_order(clauses=clauses)

    def get_clauses_from_root_in_walk_order(self):
        """Return all nodes in walk, down then right, order."""
        clauses = []
        self.get_root().get_clauses_from_current_in_walk_order(clauses=clauses)
        return clauses

    def get_condition(self):
        """Return identity of self."""
        return id(self)

    def evaluate_node_result(self, rules):
        """Evaluate node result assuming non-index conditions are resolved.

        Processing order is current node, right node, down node, to keep open
        possibility of not doing down steps if other nodes linked by left and
        right produce an empty set of records.

        This method is called recursively for right nodes until there are no
        more in the current chain.  The recursive calls for down nodes are
        done on reaching end of right chain.

        """
        if self.right is not None:
            self.right.evaluate_node_result(rules)
        if self.down is not None:
            self.down.evaluate_node_result(rules)
            for operator in (
                NOR,
                AND,
                OR,
            ):
                node = self.down
                while node:
                    if node.operator == operator:
                        if node.constraint.pending:
                            rules(operator, node)
                    node = node.right
            rules(None, self.down)

    def evaluate_index_condition_node(self, rules):
        """Evaluate node when index is available.

        Processing order is current node, right node, down node, to keep open
        possibility of not doing down steps if other nodes linked by left and
        right produce an empty set of records.

        This method is called recursively for right nodes until there are no
        more in the current chain.  The recursive calls for down nodes are
        done on reaching end of right chain.

        """
        if self.condition == IS:
            if not self.not_value:
                rules(self.condition, self)
        # LIKE not in this test because in can be applied to index fields as
        # well as non-index fields.  Add test to pass non-index LIKE on to
        # get_non_index_condition_node() method.
        # STARTS, meaning '<value>.*', will be added to reduce index scans.
        elif self.condition not in (
            None,
            PRESENT,
            NE,
        ):
            rules(self.condition, self)
        if self.operator in {NOR, AND}:
            self.constraint = self.left.constraint
        else:
            self.constraint = WhereConstraint()
        if self.result is None and self.condition:
            self.constraint.pending = True
        if self.right is not None:
            self.right.evaluate_index_condition_node(rules)
        if self.down is not None:
            self.down.evaluate_index_condition_node(rules)
            for operator in (
                NOR,
                AND,
                OR,
            ):
                node = self.down
                while node:
                    if node.operator == operator:
                        if node.result and node.left.result:
                            if not node.constraint.pending:
                                rules(operator, node)
                    node = node.right
            node = self.down
            while True:
                if node.result is None:
                    self.constraint.pending = True
                    break
                if node.right is None:
                    rules(None, node.up.down)
                    break
                node = node.right

    def get_non_index_condition_node(self, non_index_nodes):
        """Add nodes which cannot be evaluated by index to non_index_nodes."""
        if self.down is not None:
            self.down.get_non_index_condition_node(non_index_nodes)
        if self.condition == IS:
            if self.not_value:
                self.result = WhereResult()
                non_index_nodes.append(self)
        elif self.condition in (
            LIKE,
            STARTS,
            PRESENT,
            NE,
        ):
            self.result = WhereResult()
            non_index_nodes.append(self)
        if self.right is not None:
            self.right.get_non_index_condition_node(non_index_nodes)

    def set_non_index_node_constraint(self, initialize_answer):
        """Set constraint when index cannot be used to evaluate.

        initialize_answer - find.Find object's initialize_answer method.

        Nodes are processed left to right then down.  It is possible down
        operations may be avoided depending on the outcome of processing at a
        given level left to right.  Avoidance not implemented yet.

        Down operations occur in response to explicit parentheses in a query.

        """
        if self.result:
            if self.result.answer is not None:
                if self.constraint.result:
                    self.constraint.result.answer &= self.result.answer
                else:
                    self.constraint.result = self.result
            else:
                initialize_answer(self)
        if self.right is not None:
            self.right.set_non_index_node_constraint(initialize_answer)
        if self.down is not None:
            self.down.set_non_index_node_constraint(initialize_answer)


class WhereResult:
    """A node's answer."""

    def __init__(self):
        """Set result's answer to None meaning not yet evaluated."""
        self.answer = None


class WhereConstraint:
    """A node's answer must be subset of constraint's result when not None."""

    def __init__(self):
        """Set constraint's result to None meaning no constraint by default."""
        self.result = None
        self.pending = False


def _trim(string):
    """Remove one leading and trailing ' or " used in values with whitespace."""
    if string[0] in "'\"":
        return string[1:-1]
    return string


class WhereStatementError:
    """Error information about a where query and report fomatters.

    This class assumes processing a query stops when the first error is met.

    The parse() and validate() methods of Where return a WhereStatementError
    instance if any attribute other than _statement is bound to an object other
    than None.

    """

    def __init__(self, statement):
        """Initialize statement and set no token or field errors."""
        self._statement = statement
        self._tokens = None
        self._fields = None

    @property
    def statement(self):
        """Return statement."""
        return self._statement

    @property
    def tokens(self):
        """Return tokens."""
        return self._tokens

    @tokens.setter
    def tokens(self, value):
        if self._tokens is not None:
            raise WhereError("A token error already exists.")
        self._tokens = value

    @property
    def fields(self):
        """Return frozenset of fields."""
        return frozenset(self._fields)

    @fields.setter
    def fields(self, value):
        if self._fields is not None:
            raise WhereError("A field error already exists.")
        self._fields = value

    def get_error_report(self, datasource):
        """Return a str for error dialogue using database's field names."""
        if not self._tokens and not self._fields:
            return " ".join(
                (
                    "No error information available for query ",
                    repr(self._statement),
                )
            )

        # The program's name for a field is used in query statements because
        # database engines may have different restrictions on the characters,
        # and their case, in field names.
        # (Remove comment when 'k if v else k' is discarded?)
        report = [
            "".join(
                (
                    "Fields in file are:\n\n",
                    "\n".join(
                        sorted(
                            [
                                k if v else k
                                for k, v in datasource.dbhome.specification[
                                    datasource.dbset
                                ][SECONDARY].items()
                            ]
                        )
                    ),
                )
            ),
            "".join(
                (
                    "Keywords are:\n\n",
                    "  ".join(k for k in sorted(KEYWORDS)),
                )
            ),
        ]

        if not self._fields:
            report.insert(
                -2,
                "".join(
                    (
                        "Error found in query, probably near end ",
                        "of:\n\n",
                        " ".join(self._tokens),
                        "\n\nelements.",
                    )
                ),
            )
            return "\n\n".join(report)
        probf = []
        probt = []
        for fieldname in self._fields:
            if len(fieldname.split()) > 1:
                probt.append(fieldname)
            else:
                probf.append(fieldname)
        if probt:
            report.insert(
                -2,
                "".join(
                    (
                        "Probably keywords are missing or have spelling mistakes:\n\n",
                        "\n".join(probt),
                        "\n\nalthough these could be field names if the list of ",
                        "allowed field names has names with spaces.",
                    )
                ),
            )
        if probf:
            report.insert(
                -2,
                "".join(
                    (
                        "Probably field names with spelling mistakes:\n\n",
                        "\n".join(sorted(probf)),
                    )
                ),
            )
        return "\n\n".join(report)
