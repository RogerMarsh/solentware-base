# wherevalues.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A index value selection statement parser approximately equivalent to SQL
Select statement where clause and DPT Find All Values statement retrieval
conditions.

The syntax is:

fieldname [<FROM|ABOVE> value] [<TO|BELOW> value] [[NOT] LIKE pattern]
          [[NOT] IN set]

| indicates choices.
[] indicates optional items and <> indicates choice in non-optional items.

"""

import re

DOUBLE_QUOTE_STRING = '".*?"'
SINGLE_QUOTE_STRING = "'.*?'"
IN = 'in'
TO = 'to'
NOT = 'not'
LIKE = 'like'
FROM = 'from'
ABOVE = 'above'
BELOW = 'below'
STRING = '[^\s]+'

LEADING_SPACE = '(?<=\s)'
TRAILING_SPACE = '(?=\s)'

WHEREVALUES_RE = re.compile(
    '|'.join((DOUBLE_QUOTE_STRING,
              SINGLE_QUOTE_STRING,
              NOT.join((LEADING_SPACE, TRAILING_SPACE)),
              LIKE.join((LEADING_SPACE, TRAILING_SPACE)),
              FROM.join((LEADING_SPACE, TRAILING_SPACE)),
              ABOVE.join((LEADING_SPACE, TRAILING_SPACE)),
              BELOW.join((LEADING_SPACE, TRAILING_SPACE)),
              TO.join((LEADING_SPACE, TRAILING_SPACE)),
              IN.join((LEADING_SPACE, TRAILING_SPACE)),
              STRING,
              )).join(('(', ')')),
    flags=re.IGNORECASE|re.DOTALL)

KEYWORDS = frozenset((TO,
                      IN,
                      NOT,
                      LIKE,
                      FROM,
                      ABOVE,
                      BELOW,
                      ))


class WhereValuesError(Exception):
    pass


class WhereValues(object):

    """Find index values matching the query in statement."""

    def __init__(self, statement):
        """Create WhereValues instance for statement."""
        self.statement = statement
        self.tokens = None
        self.node = None
        self._error_token_offset = None
        self._not = False
        self._processors = None
        
    def lex(self):
        """Split instance's statement into tokens."""
        tokens = []
        strings = []
        for w in WHEREVALUES_RE.split(self.statement):
            if w.lower() in KEYWORDS:
                if strings:
                    tokens.append(' '.join([_trim(s) for s in strings if s]))
                    strings.clear()
                tokens.append(w.lower())
            elif w.strip():
                strings.append(w.strip())
        if strings:
            tokens.append(' '.join([_trim(s) for s in strings if s]))
            strings.clear()
        self.tokens = tokens

    def parse(self):
        """Parse instance's tokens to create node structure to do query.

        The structure is simple, consisting of a single node, a ValuesClause
        object.

        """
        self.node = ValuesClause()
        state = self._set_fieldname
        for e, t in enumerate(self.tokens):
            state = state(t)
            if not state:
                self._error_token_offset = e
                break
        else:
            self.node.valid_phrase = True

    def validate(self, db, dbset):
        """Check the node derived from statement contains a valid search
        specification for db and dbset.

        db - the database.
        dbset - the table in the database.

        The field must exist in table dbset of database db.
        One only of above_value and from_value can be siven.
        One only of below_value and to_value can be siven.

        """
        if self._error_token_offset is not None:
            return self.tokens[:self._error_token_offset]
        if self.node is None:
            return None
        n = self.node

        # Valid values are None or a compiled regular expression.
        # The attribute is bound to the string which failed to compile if the
        # compilation failed.
        if isinstance(n.like_pattern, str):
            return False
        
        if not n.valid_phrase:
            return n.valid_phrase
        if n.field is None:
            return False
        elif not db.exists(dbset, n.field):
            return False
        if n.above_value is not None and n.from_value is not None:
            return False
        if n.below_value is not None and n.to_value is not None:
            return False
        return True

    def evaluate(self, processors):
        """Evaluate the query using the processor.

        processors - A FindValues object.

        The processor will know how to access the field in the statement.

        The answer to the query defined in instance's statement is put in the
        self.node.result attribute.

        """
        if self.node is None:
            return None
        self._processors = processors
        try:
            self.node.evaluate_node_result(processors)
        finally:
            self._processors = None

    def error(self, t):
        """Return False, t is an unexpected keyword or value."""
        return False

    def _set_fieldname(self, t):
        """Set field name and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.field = t
        return self._set_not_from_to_like_in_

    def _set_from_value(self, t):
        """Set from value and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.from_value = t
        return self._set_not_to_like_in_

    def _set_above_value(self, t):
        """Set above value and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.above_value = t
        return self._set_not_to_like_in_

    def _set_to_value(self, t):
        """Set to value and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.to_value = t
        return self._set_not_like_in_

    def _set_below_value(self, t):
        """Set to value and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.below_value = t
        return self._set_not_like_in_

    def _set_like_value(self, t):
        """Set like value and return method to process next token."""

        # If 't' really must be one of the keywords the construct
        # "fieldname from 't' to 't'" must be used to achieve the same result.
        # Normally "fieldname like \At\Z" will do.
        if t.lower() in KEYWORDS:
            return self.error(t)
        try:
            self.node.like_pattern = re.compile(t)
        except:
            self.node.like_pattern = t
        if self._not:
            self.node.like = False
            self._not = False
        return self._set_not_in_

    def _set_in__value(self, t):
        """Set 'in set' value and return method to process next token."""
        if t.lower() in KEYWORDS:
            return self.error(t)
        self.node.in__set = t
        if self._not:
            self.node.in_ = False
            self._not = False
        return self._finish

    def _set_not_from_to_like_in_(self, t):
        """Set not or condition and return method to process next token.

        'from', 'above', 'to', 'below', 'like', and 'in', are accepted
        conditions.

        """
        if t.lower() == NOT:
            self._not = True
            return self._set_like_in_
        elif t.lower() == FROM:
            return self._set_from_value
        elif t.lower() == ABOVE:
            return self._set_above_value
        elif t.lower() == TO:
            return self._set_to_value
        elif t.lower() == BELOW:
            return self._set_below_value
        elif t.lower() == LIKE:
            return self._set_like_value
        elif t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _set_not_to_like_in_(self, t):
        """Set not or condition and return method to process next token.

        'to', 'below', 'like', and 'in', are accepted conditions.

        """
        if t.lower() == NOT:
            self._not = True
            return self._set_like_in_
        elif t.lower() == TO:
            return self._set_to_value
        elif t.lower() == BELOW:
            return self._set_below_value
        elif t.lower() == LIKE:
            return self._set_like_value
        elif t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _set_not_like_in_(self, t):
        """Set not or condition and return method to process next token.

        'like' and 'in' are accepted conditions.

        """
        if t.lower() == NOT:
            self._not = True
            return self._set_like_in_
        elif t.lower() == LIKE:
            return self._set_like_value
        elif t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _set_not_in_(self, t):
        """Set not or condition and return method to process next token.

        'in' is accepted condition.

        """
        if t.lower() == NOT:
            self._not = True
            return self._set_in_
        elif t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _set_like_in_(self, t):
        """Set condition and return method to process next token.

        'like' and 'in' are accepted conditions.

        """
        if t.lower() == LIKE:
            return self._set_like_value
        elif t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _set_in_(self, t):
        """Set condition and return method to process next token.

        'in' is accepted condition.

        """
        if t.lower() == IN:
            return self._set_in__value
        else:
            return self.error(t)

    def _finish(self, t):
        """Set error if any token found after final valid token."""
        return self.error(t)


class ValuesClause(object):

    """Phrase in WhereValues specification.

    The WhereValues parser binds ValuesClause attributes to the field name,
    condition, and values, found in a phrase of a 'find values' statement;
    and states whether the attributes describe a valid phrase.

    The attributes are:

    valid_phrase - True if the phrase can be evaluated.
    field - Name of field on database whose value is compared.
    above_value - field value matches if greater than above_value.
    below_value - field value matches if less than below_value.
    from_value - field value matches if greater than or equal from_value.
    to_value - field value matches if less than or equal to_value.
    like - True if field value matches if it matches like_pattern.
    like_pattern - Regular expression to evaluate 'like'.
    in_ - True if field value matches if it is in the in__set set of values.
    in__set - Iterable of values to evaluate 'in'.
    result - List of values found when node is evaluated.

    The syntax of the value selection statement leads to these possibilities:

    Range is defined by one of the valuesclause attribute sets:
    above_value and below_value are not None
    above_value and to_value are not None
    from_value and to_value are not None
    from_value and below_value are not None
    above_value is not None
    to_value is not None
    from_value is not None
    below_value is not None
    above_value, to_value, from_value, and below_value, are None,

    Filters are defined by one of the valuesclause attribute sets:
    like is False and like_pattern is None
    like is True and like_pattern is not None
    in_ is False and in__set is None
    in_ is True and in__set is an iterable
    Any pairing of the 'like' and 'in_' attribute sets above.

    A range and a filter may appear in the same phrase.
    
    """

    def __init__(self):
        """Initialiase a node.

        valid_phrase is set False, like and in_ are set True, and the rest are
        set None.

        """
        self.valid_phrase = False
        # Phrase
        self.field = None
        self.above_value = None
        self.below_value = None
        self.from_value = None
        self.to_value = None
        self.like = True
        self.like_pattern = None
        self.in_ = True
        self.in__set = None
        # Evaluation
        self.result = None

    def evaluate_node_result(self, processors):
        """Call processor's find_values() method to evaluate node's phrase and
        bind node's result attribute to the answer.

        processors - FindValues object which does the evaluation.

        """
        if self.valid_phrase:
            processors.find_values(self)

    def apply_pattern_and_set_filters_to_value(self, value):
        """Apply 'like' and 'value set' constraints to value.

        This method is intended for use as a callback by a FindValues object.

        The underlying database engine may, or may not, have internal methods
        able to do either or both these functions.

        This method assumes the use of Python regular expressions to do 'like'
        constraints and Python set operations to do 'value set' constraints.

        """
        if self.like_pattern:
            if not self.like_pattern.search(value):
                if self.like:
                    return False
            elif not self.like:
                return False
        if self.in__set is not None:
            if self.in_:
                return value in self.in__set
            else:
                return value not in self.in__set
        return True


def _trim(s):
    """Remove one leading and trailing ' or " used in values with whitespace."""
    if s[0] in '\'"':
        return s[1:-1]
    return s
