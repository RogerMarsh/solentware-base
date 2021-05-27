# where_dpt.py
# Copyright (c) 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""A record selection statement parser approximately equivalent to SQL Select
statement where clause and DPT Find statement retrieval conditions.

"""

from .where import Where, WhereClause, WhereConstraint, WhereResult


class Where(Where):

    """Override where.Where to use operators available in DPT.
    """

    def evaluate(self, processors):
        """Evaluate the query.

        processors - the find_dpt.Find object which refers to the database
                    against which the query is to be evaluated.

        The answer to the query defined in instance's statement is put in the
        self.node.result attribute.

        """
        if self.node is None:
            return None
        self._processors = processors
        try:
            rn = self.node.get_root()
            rn.evaluate_index_condition_node(self._index_rules_engine)
            if rn.result is not None:
                return
            non_index_nodes = []
            rn.get_non_index_condition_node(non_index_nodes)
            for n in non_index_nodes:
                n.result = WhereResult()
            rn.constraint = WhereConstraint()
            rn.constraint.result = WhereResult()
            rn.constraint.result.answer = processors.get_existence()
            rn.set_non_index_node_constraint(processors.initialize_answer)
            cn = WhereConstraint()
            cn.result = WhereResult()
            processors.initialize_answer(cn)
            for c in {n.constraint for n in non_index_nodes}:
                if c.result:
                    cn.result.answer.Place(c.result.answer)
                else:
                    cn.result.answer = processors.get_existence()
                    break
            for record in processors.get_record(cn.result.answer):
                for n in non_index_nodes:
                    processors.non_index_condition(n, *record)
            self.node.get_root().evaluate_node_result(self._result_rules_engine)
        finally:
            self._processors = None

    def close_all_nodes(self, processors):
        """Destroy the recordsets held in the node tree.

        processors - the find_dpt.Find object which refers to the database
                    owning the recordsets.

        The dptapi.APIRecordList and dptapi.APIRecordSet instances held in
        where_dpt.WhereClause instances in the where_dpt.Where.node tree must
        be destroyed by calling <context>.DestroyRecordSet(<set>) before
        destroying self.

        """
        if self.node is not None:
            recordsets = set()
            for c in self.node.get_clauses_from_root_in_walk_order():
                if c.result:
                    recordsets.add(c.result.answer)
                if c.constraint:
                    if c.constraint.result:
                        if c.constraint.result.answer:
                            recordsets.add(c.constraint.result.answer)
            recordsets.discard(None)
            while len(recordsets):
                rs = recordsets.pop()
                processors.context.DestroyRecordSet(rs)


class WhereClause(WhereClause):

    """Override where.WhereClause to use operators available in DPT.
    """

    def set_non_index_node_constraint(self, initialize_answer):
        """Set constraint when index cannot be used to evaluate.

        initialize_answer - find_dpt.Find object's initialize_answer method.

        Nodes are processed left to right then down.  It is possible down
        operations may be avoided depending on the outcome of processing at a
        given level left to right.  Avoidance not implemented yet.

        Down operations occur in response to explicit parentheses in a query.

        """
        if self.left:
            if self.result:
                if self.result.answer is not None:
                    if self.constraint is self.left.constraint:
                        if self.constraint.result:
                            rl = self._processors.context.CreateRecordList()
                            rl.Place(self.constraint.result.answer)
                            rl.Remove(self.result.answer)
                            self.constraint.result.answer.Remove(rl)
                            self._processors.context.DestroyRecordSet(rl)
                        else:
                            self.constraint.result = self.result
                    else:
                        self.constraint.result = self.result
                else:
                    initialize_answer(self)
        elif self.result:
            if self.result.answer is not None:
                self.constraint.result = self.result
            else:
                initialize_answer(self)
        if self.down is not None:
            self.down.set_non_index_node_constraint(initialize_answer)
        if self.right is not None:
            self.right.set_non_index_node_constraint(initialize_answer)
        if self.constraint.result is None:
            if self.up:
                self.constraint = self.up.constraint
