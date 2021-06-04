# wherevalues.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Index value selection statement parser.

Approximately equivalent to the SQL Select statement where clause, or the
DPT Find All Values statement, retrieval conditions.

The syntax is:

fieldname [<FROM|ABOVE> value] [<TO|BELOW> value] [[NOT] LIKE pattern]
          [[NOT] IN set]

| indicates choices.
[] indicates optional items and <> indicates choice in non-optional items.

"""

import re

DOUBLE_QUOTE_STRING = r'".*?"'
SINGLE_QUOTE_STRING = r"'.*?'"
IN = r"in"
TO = r"to"
NOT = r"not"
LIKE = r"like"
FROM = r"from"
ABOVE = r"above"
BELOW = r"below"
STRING = r"[^\s]+"

LEADING_SPACE = r"(?<=\s)"
TRAILING_SPACE = r"(?=\s)"

WHEREVALUES_RE = re.compile(
    r"|".join(
        (
            DOUBLE_QUOTE_STRING,
            SINGLE_QUOTE_STRING,
            NOT.join((LEADING_SPACE, TRAILING_SPACE)),
            LIKE.join((LEADING_SPACE, TRAILING_SPACE)),
            FROM.join((LEADING_SPACE, TRAILING_SPACE)),
            ABOVE.join((LEADING_SPACE, TRAILING_SPACE)),
            BELOW.join((LEADING_SPACE, TRAILING_SPACE)),
            TO.join((LEADING_SPACE, TRAILING_SPACE)),
            IN.join((LEADING_SPACE, TRAILING_SPACE)),
            STRING,
        )
    ).join((r"(", r")")),
    flags=re.IGNORECASE | re.DOTALL,
)

KEYWORDS = frozenset(
    (
        TO,
        IN,
        NOT,
        LIKE,
        FROM,
        ABOVE,
        BELOW,
    )
)


class WhereValuesError(Exception):
    """Exception for WhereValues class."""


class WhereValues:
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
        for word in WHEREVALUES_RE.split(self.statement):
            if word.lower() in KEYWORDS:
                if strings:
                    tokens.append(" ".join([_trim(s) for s in strings if s]))
                    strings.clear()
                tokens.append(word.lower())
            elif word.strip():
                strings.append(word.strip())
        if strings:
            tokens.append(" ".join([_trim(s) for s in strings if s]))
            strings.clear()
        self.tokens = tokens

    def parse(self):
        """Parse instance's tokens to create node structure to do query.

        The structure is simple, consisting of a single node, a ValuesClause
        object.

        """
        self.node = ValuesClause()
        state = self._set_fieldname
        for item, token in enumerate(self.tokens):
            state = state(token)
            if not state:
                self._error_token_offset = item
                break
        else:
            self.node.valid_phrase = True

    def validate(self, db, dbset):
        """Verify self's statement has a valid search for db and dbset.

        db - the database.
        dbset - the table in the database.

        The field must exist in table dbset of database db.
        One only of above_value and from_value can be siven.
        One only of below_value and to_value can be siven.

        """
        if self._error_token_offset is not None:
            return self.tokens[: self._error_token_offset]
        if self.node is None:
            return None
        node = self.node

        # Valid values are None or a compiled regular expression.
        # The attribute is bound to the string which failed to compile if the
        # compilation failed.
        if isinstance(node.like_pattern, str):
            return False

        if not node.valid_phrase:
            return node.valid_phrase
        if node.field is None:
            return False
        if not db.exists(dbset, node.field):
            return False
        if node.above_value is not None and node.from_value is not None:
            return False
        if node.below_value is not None and node.to_value is not None:
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
            return
        self._processors = processors
        try:
            self.node.evaluate_node_result(processors)
        finally:
            self._processors = None

    def error(self, token):
        """Return False, token is an unexpected keyword or value."""
        return False

    def _set_fieldname(self, token):
        """Set field name and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.field = token
        return self._set_not_from_to_like_in_

    def _set_from_value(self, token):
        """Set from value and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.from_value = token
        return self._set_not_to_like_in_

    def _set_above_value(self, token):
        """Set above value and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.above_value = token
        return self._set_not_to_like_in_

    def _set_to_value(self, token):
        """Set to value and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.to_value = token
        return self._set_not_like_in_

    def _set_below_value(self, token):
        """Set to value and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.below_value = token
        return self._set_not_like_in_

    def _set_like_value(self, token):
        """Set like value and return method to process next token."""
        # If 'token' really must be one of the keywords the construct
        # "fieldname from 'token' to 'token'" must be used to achieve the
        # same result.
        # Normally "fieldname like \At\Z" will do.
        if token.lower() in KEYWORDS:
            return self.error(token)
        try:
            self.node.like_pattern = re.compile(token)
        except:
            self.node.like_pattern = token
        if self._not:
            self.node.like = False
            self._not = False
        return self._set_not_in_

    def _set_in__value(self, token):
        """Set 'in set' value and return method to process next token."""
        if token.lower() in KEYWORDS:
            return self.error(token)
        self.node.in__set = token
        if self._not:
            self.node.in_ = False
            self._not = False
        return self._finish

    def _set_not_from_to_like_in_(self, token):
        """Set not or condition and return method to process next token.

        'from', 'above', 'to', 'below', 'like', and 'in', are accepted
        conditions.

        """
        if token.lower() == NOT:
            self._not = True
            return self._set_like_in_
        if token.lower() == FROM:
            return self._set_from_value
        if token.lower() == ABOVE:
            return self._set_above_value
        if token.lower() == TO:
            return self._set_to_value
        if token.lower() == BELOW:
            return self._set_below_value
        if token.lower() == LIKE:
            return self._set_like_value
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _set_not_to_like_in_(self, token):
        """Set not or condition and return method to process next token.

        'to', 'below', 'like', and 'in', are accepted conditions.

        """
        if token.lower() == NOT:
            self._not = True
            return self._set_like_in_
        if token.lower() == TO:
            return self._set_to_value
        if token.lower() == BELOW:
            return self._set_below_value
        if token.lower() == LIKE:
            return self._set_like_value
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _set_not_like_in_(self, token):
        """Set not or condition and return method to process next token.

        'like' and 'in' are accepted conditions.

        """
        if token.lower() == NOT:
            self._not = True
            return self._set_like_in_
        if token.lower() == LIKE:
            return self._set_like_value
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _set_not_in_(self, token):
        """Set not or condition and return method to process next token.

        'in' is accepted condition.

        """
        if token.lower() == NOT:
            self._not = True
            return self._set_in_
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _set_like_in_(self, token):
        """Set condition and return method to process next token.

        'like' and 'in' are accepted conditions.

        """
        if token.lower() == LIKE:
            return self._set_like_value
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _set_in_(self, token):
        """Set condition and return method to process next token.

        'in' is accepted condition.

        """
        if token.lower() == IN:
            return self._set_in__value
        return self.error(token)

    def _finish(self, token):
        """Set error if any token found after final valid token."""
        return self.error(token)


class ValuesClause:
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
        """Evaluate self's phrase with the processors FindValues object.

        Call processor's find_values() method to evaluate node's phrase and
        bind node's result attribute to the answer.

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
            return value not in self.in__set
        return True


def _trim(string):
    """Remove one leading and trailing ' or " used in values with whitespace."""
    if string[0] in "'\"":
        return string[1:-1]
    return string
