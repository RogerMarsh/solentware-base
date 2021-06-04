# test__tree.py
# Copyright 2019 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""tree tests."""
# The _nosql and test__nosql  modules are written by copying _sqlite and
# test__sqlite, then change test__nosql to do unqlite or vedis things one test
# at a time and replace the SQLite things in _nosql as they get hit.

import unittest
from ast import literal_eval
import sys
import random

try:
    import unqlite
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    unqlite = None
try:
    import vedis
except ImportError:  # Not ModuleNotFoundError for Pythons earlier than 3.6
    vedis = None

from .. import tree
from .. import _nosql


class Tree(unittest.TestCase):
    def setUp(self):

        # UnQLite and Vedis are sufficiently different that the open_database()
        # call arguments have to be set diferrently for these engines.
        if dbe_module is unqlite:
            self._oda = dbe_module, dbe_module.UnQLite, dbe_module.UnQLiteError
        elif dbe_module is vedis:
            self._oda = dbe_module, dbe_module.Vedis, None

        class _D(_nosql.Database):
            pass

        self._D = _D
        self.database = self._D({"file1": {"field1"}}, segment_size_bytes=None)
        self.database.open_database(*self._oda)

    def tearDown(self):
        self.database.close_database()
        self.database = None
        self._D = None


class Tree___init__(Tree):
    def test_01___init__(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 4 positional arguments ",
                    "but 5 were given",
                )
            ),
            tree.Tree,
            *(None, None, None, None),
        )

    def test_02___init__(self):
        self.assertRaisesRegex(
            AttributeError,
            "'NoneType' object has no attribute 'table'",
            tree.Tree,
            *(None, None, None),
        )

    def test_03___init__(self):
        self.assertRaisesRegex(
            KeyError,
            "_",
            tree.Tree,
            *("", "", self.database),
        )

    def test_04___init__(self):
        t = tree.Tree("file1", "field1", self.database)
        self.assertEqual(
            sorted(t.__dict__.keys()),
            [
                "_next_node",
                "branching_factor",
                "database",
                "field",
                "file",
                "high_node",
                "key_node",
                "key_root",
                "key_segment",
            ],
        )
        self.assertEqual(t.key_node, "1_1_2")
        self.assertEqual(t.key_root, "1_1")
        self.assertEqual(t.key_segment, "1_1_0")
        self.assertEqual(t.high_node, "1_1__high_tree_node")
        self.assertEqual(t._next_node, None)
        self.assertEqual(t.branching_factor, 5)
        self.assertIs(t.database, self.database)


class Tree_file1_field1(Tree):
    def setUp(self):
        super().setUp()
        self.tree = tree.Tree("file1", "field1", self.database)

    def tearDown(self):
        del self.tree
        super().tearDown()

    def print_nodes(self, key_root="1_1"):
        db = self.database.dbenv
        tree = self.tree
        key_high = "_".join((key_root, "_high_tree_node"))
        if not db.exists(key_high):
            sys.stdout.write("No nodes printed: high node not defined.\n")
            return
        high_node = literal_eval(db[key_high].decode())
        root_printed = False
        sys.stdout.write(
            " ".join(("High node is", "".join((str(high_node), ".\n"))))
        )
        for i in range(high_node + 1):
            key = "_".join((key_root, str(2), str(i)))
            if db.exists(key):
                sys.stdout.write("".join((db[key].decode(), "\n")))
            elif not root_printed and db.exists(key_root):
                node = literal_eval(db[key_root].decode())
                if node[0] == i:
                    sys.stdout.write("".join((repr(node), "\n")))
                    root_printed = True
                del node
        sys.stdout.write(
            " ".join(
                (
                    "Existing nodes in",
                    "".join(("range(", str(0), ", ", str(high_node + 1), ")")),
                    "printed.\n",
                )
            )
        )

    # Does this method imply unittest needs testing?
    def check_nodes(self, key_root="1_1"):
        db = self.database.dbenv
        tree = self.tree
        key_high = "_".join((key_root, "_high_tree_node"))
        if not db.exists(key_high):
            return
        high_node = literal_eval(db[key_high].decode())
        if db.exists(key_root):
            root_node = literal_eval(db[key_root].decode())
        else:
            root_node = None
        nodes = {}
        for i in range(high_node + 1):
            key = "_".join((key_root, str(2), str(i)))
            if db.exists(key):
                nodes[i] = literal_eval(db[key].decode())
            elif root_node:
                if root_node[0] == i:
                    nodes[i] = root_node
                    root_node = None
        if len(nodes):
            self.assertEqual(root_node, None)
        leftmost_key = None
        leftmost_node = None
        rightmost_node = None
        low_leaf_keys = set()
        splitter_keys = set()
        all_keys = set()
        for k, v in nodes.items():
            self.assertEqual(k, v[0])  # expected node number.
            self.assertEqual(v[4], sorted(v[4]))
            self.assertEqual(len(v[4]) < tree.branching_factor, True)
            if v[1] == 4:  # leaf.
                self.assertEqual(
                    len(v[4]) + 1 >= tree.branching_factor // 2, True
                )  # no entries so test against <keys + 1>.
                self.assertEqual(v[5], None)  # entries.
                self.assertNotEqual(v[2], v[3])  # left and right nodes.
                if v[2] == None:  # no leaf to left.
                    self.assertEqual(leftmost_node, None)
                    leftmost_node = v
                    leftmost_key = v[4][0]
                else:
                    self.assertNotEqual(v[2], v[0])  # points to another node.
                    self.assertEqual(v[0], nodes[v[2]][3])  # pointers fit.
                if v[3] == None:  # no leaf to right.
                    self.assertEqual(rightmost_node, None)
                    rightmost_node = v
                else:
                    self.assertNotEqual(v[3], v[0])  # points to another node.
                    self.assertEqual(v[0], nodes[v[3]][2])  # pointers fit.
                self.assertEqual(v[4][0] not in low_leaf_keys, True)
                low_leaf_keys.add(v[4][0])  # keys in branch or root nodes.
                all_keys.update(v[4])  # all keys in leaf nodes.
            elif v[1] == 2:  # solo root.
                self.assertEqual(v[5], None)  # entries.
                self.assertEqual(v[2], v[3])  # left and right nodes.
                self.assertEqual(v[2], None)  # left node.
                leftmost_node = v
                rightmost_node = v
            elif v[1] == 1:  # root.
                self.assertEqual(v[2], v[3])  # left and right nodes.
                self.assertEqual(v[2], None)  # left node.
                self.assertEqual(
                    len(v[4]) + 1, len(v[5])
                )  # key and entry count.
                self.assertEqual(v[4][0] not in splitter_keys, True)
                splitter_keys.update(v[4])  # low keys in leaf nodes.
                self.assertEqual(len(v[5]) > 1, True)  # solo root if 1 entry.
            elif v[1] == 3:  # branch.
                self.assertEqual(
                    len(v[5]) >= tree.branching_factor // 2, True
                )  # lower limit on number of entries.
                self.assertEqual(v[2], v[3])  # left and right nodes.
                self.assertEqual(v[2], None)  # left node.
                self.assertEqual(
                    len(v[4]) + 1, len(v[5])
                )  # key and entry count.
                self.assertEqual(
                    v[4][0] not in splitter_keys,
                    True,
                    msg="".join(
                        (
                            "Node ",
                            str(v[0]),
                            ", key '",
                            str(v[4][0]),
                            "'",
                        )
                    ),
                )
                splitter_keys.update(v[4])  # low keys in leaf nodes.
            else:
                self.assertEqual(
                    v[1] in {1, 2, 3, 4}, True
                )  # catch node type.
        if len(nodes) > 1:
            self.assertEqual(len(splitter_keys) + 1, len(low_leaf_keys))
            self.assertEqual(
                splitter_keys.union({leftmost_key}), low_leaf_keys
            )
        else:
            for (
                k
            ) in nodes:  # solo root is usually 0, but not in an emptied db.
                self.assertEqual(nodes[k][1], 2)
        self.assertNotEqual(leftmost_node, None)  # nodes must exist.
        self.assertNotEqual(rightmost_node, None)  # nodes must exist.
        leaf_keys = []
        n = leftmost_node
        while True:
            leaf_keys.extend(n[4])
            if n[3] is None:
                break
            n = nodes[n[3]]
        if len(nodes) > 1:
            self.assertEqual(leaf_keys, sorted(all_keys))


class Tree_insert_branching_5(Tree_file1_field1):
    def test_01_insert__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "insert\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree.insert,
            *(None, None),
        )

    def test_02_insert__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "insert\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.tree.insert,
        )

    def test_03_insert__key_exists(self):
        # Does key exist on database? It does, do nothing even if no tree.
        self.database.dbenv["1_1_0_key"] = True
        self.assertEqual("1_1_0_key" in self.database.dbenv, True)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual(self.tree.insert("key"), None)
        # Reversed to fit code commented at start of Tree.insert().  Reason is
        # how _nosqldu.Database.sort_and_write() interacts with Tree.
        # self.assertEqual('' in self.database.dbenv.exists('1_1'), False)
        self.assertEqual("1_1" in self.database.dbenv, True)

    def test_04_insert__first_key_into_tree(self):
        self.assertEqual("1_1_0_key" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual(self.tree.insert("key"), None)
        self.assertEqual(
            self.database.dbenv["1_1"],
            repr([0, 2, None, None, ["key"], None]).encode(),
        )

    def test_05_insert__populate_tree_branching_factor_2n_plus_1(self):
        self.assertEqual(self.tree.branching_factor, 5)
        # self.print_nodes()
        self.tree.insert("k3")
        # self.print_nodes()
        self.tree.insert("k2")
        # self.print_nodes()
        self.tree.insert("k5")
        # self.print_nodes()
        self.tree.insert("k1")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [0, 2, None, None, ["k1", "k2", "k3", "k5"], None],
        )
        self.tree.insert("k4")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2", "k3"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, None, ["k4", "k5"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [2, 1, None, None, ["k4"], [0, 1]],
        )
        self.tree.insert("k59")
        # self.print_nodes()
        self.tree.insert("k58")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2", "k3"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, None, ["k4", "k5", "k58", "k59"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [2, 1, None, None, ["k4"], [0, 1]],
        )
        self.tree.insert("k57")
        # self.print_nodes()
        self.tree.insert("k56")
        # self.print_nodes()
        self.tree.insert("k55")
        # self.print_nodes()
        self.tree.insert("k54")
        # self.print_nodes()
        self.tree.insert("k53")
        # self.print_nodes()
        self.tree.insert("k31")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2", "k3", "k31"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, 5, ["k4", "k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [2, 1, None, None, ["k4", "k54", "k56", "k58"], [0, 1, 5, 4, 3]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 4, None, ["k58", "k59"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, 3, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 1, 4, ["k54", "k55"], None],
        )
        self.tree.insert("k32")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 6, ["k1", "k2", "k3"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 6, 5, ["k4", "k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k31", "k4"], [0, 6, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 4, None, ["k58", "k59"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, 3, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 1, 4, ["k54", "k55"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 4, 0, 1, ["k31", "k32"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_7"].decode()),
            [7, 3, None, None, ["k56", "k58"], [5, 4, 3]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [8, 1, None, None, ["k54"], [2, 7]],
        )
        self.tree.insert("k84")
        # self.print_nodes()
        self.tree.insert("k83")
        # self.print_nodes()
        self.tree.insert("k85")
        # self.print_nodes()
        self.tree.insert("k87")
        # self.print_nodes()
        self.tree.insert("k86")
        # self.print_nodes()
        self.tree.insert("k88")
        # self.print_nodes()
        self.tree.insert("k80")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 6, ["k1", "k2", "k3"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 6, 5, ["k4", "k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k31", "k4"], [0, 6, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 4, 9, ["k58", "k59", "k80", "k83"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, 3, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 1, 4, ["k54", "k55"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 4, 0, 1, ["k31", "k32"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_7"].decode()),
            [7, 3, None, None, ["k56", "k58", "k84", "k87"], [5, 4, 3, 9, 10]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [8, 1, None, None, ["k54"], [2, 7]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_9"].decode()),
            [9, 4, 3, 10, ["k84", "k85", "k86"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_10"].decode()),
            [10, 4, 9, None, ["k87", "k88"], None],
        )
        self.tree.insert("k81")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 6, ["k1", "k2", "k3"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 6, 5, ["k4", "k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k31", "k4"], [0, 6, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 4, 11, ["k58", "k59", "k80"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, 3, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 1, 4, ["k54", "k55"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 4, 0, 1, ["k31", "k32"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_7"].decode()),
            [7, 3, None, None, ["k56", "k58"], [5, 4, 3]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [8, 1, None, None, ["k54", "k81"], [2, 7, 12]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_9"].decode()),
            [9, 4, 11, 10, ["k84", "k85", "k86"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_10"].decode()),
            [10, 4, 9, None, ["k87", "k88"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_11"].decode()),
            [11, 4, 3, 9, ["k81", "k83"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_12"].decode()),
            [12, 3, None, None, ["k84", "k87"], [11, 9, 10]],
        )


class Tree_insert_branching_4(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.tree.branching_factor = 4

    def test_01_insert__populate_tree_branching_factor_2n(self):
        self.assertEqual(self.tree.branching_factor, 4)
        # self.print_nodes()
        self.tree.insert("k3")
        # self.print_nodes()
        self.tree.insert("k2")
        # self.print_nodes()
        self.tree.insert("k5")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [0, 2, None, None, ["k2", "k3", "k5"], None],
        )
        self.tree.insert("k1")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, None, ["k3", "k5"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [2, 1, None, None, ["k3"], [0, 1]],
        )
        self.tree.insert("k4")
        # self.print_nodes()
        self.tree.insert("k59")
        # self.print_nodes()
        self.tree.insert("k58")
        # self.print_nodes()
        self.tree.insert("k57")
        # self.print_nodes()
        self.tree.insert("k56")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, 3, ["k3", "k4"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [2, 1, None, None, ["k3", "k5", "k58"], [0, 1, 3, 4]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 1, 4, ["k5", "k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 3, None, ["k58", "k59"], None],
        )
        self.tree.insert("k55")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, 3, ["k3", "k4"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k3"], [0, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 1, 5, ["k5", "k55"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, None, ["k58", "k59"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 3, 4, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 3, None, None, ["k56", "k58"], [3, 5, 4]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [7, 1, None, None, ["k5"], [2, 6]],
        )
        self.tree.insert("k54")
        # self.print_nodes()
        self.tree.insert("k53")
        # self.print_nodes()
        self.tree.insert("k31")
        # self.print_nodes()
        self.tree.insert("k84")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, 3, ["k3", "k31", "k4"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k3"], [0, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 1, 8, ["k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, None, ["k58", "k59", "k84"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 8, 4, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 3, None, None, ["k54", "k56", "k58"], [3, 8, 5, 4]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [7, 1, None, None, ["k5"], [2, 6]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_8"].decode()),
            [8, 4, 3, 5, ["k54", "k55"], None],
        )
        self.tree.insert("k83")
        # self.print_nodes()
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_0"].decode()),
            [0, 4, None, 1, ["k1", "k2"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_1"].decode()),
            [1, 4, 0, 3, ["k3", "k31", "k4"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_2"].decode()),
            [2, 3, None, None, ["k3"], [0, 1]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_3"].decode()),
            [3, 4, 1, 8, ["k5", "k53"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_4"].decode()),
            [4, 4, 5, 9, ["k58", "k59"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_5"].decode()),
            [5, 4, 8, 4, ["k56", "k57"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_6"].decode()),
            [6, 3, None, None, ["k54"], [3, 8]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1"].decode()),
            [7, 1, None, None, ["k5", "k56"], [2, 6, 10]],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_8"].decode()),
            [8, 4, 3, 5, ["k54", "k55"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_9"].decode()),
            [9, 4, 4, None, ["k83", "k84"], None],
        )
        self.assertEqual(
            literal_eval(self.database.dbenv["1_1_2_10"].decode()),
            [10, 3, None, None, ["k58", "k83"], [5, 4, 9]],
        )


class Tree_insert_branching_6(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.tree.branching_factor = 6

    def test_01_insert__keys_in_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        for i in range(100):
            self.assertEqual(
                self.tree.insert("k" + str(i)), None, msg="k" + str(i)
            )
            # self.print_nodes()
            self.check_nodes()

    def test_02_insert__keys_in_reverse_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        for i in range(99, -1, -1):
            self.assertEqual(
                self.tree.insert("k" + str(i)), None, msg="k" + str(i)
            )
            # self.print_nodes()
            self.check_nodes()

    def test_03_insert__keys_in_random_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        keys = set()
        ordered_keys = []
        while len(keys) < 100:
            i = random.randint(0, 500)
            if i in keys:
                continue
            keys.add(i)
            ordered_keys.append(i)
            self.assertEqual(
                self.tree.insert("k" + str(i)), None, msg=repr(ordered_keys)
            )
            # self.print_nodes()
            self.check_nodes()


class Tree_delete_branching_5(Tree_file1_field1):
    def test_01_delete__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree.delete,
            *(None, None),
        )

    def test_02_delete__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "delete\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.tree.delete,
        )

    def test_03_delete__key_exists(self):
        self.database.dbenv["1_1_0_key"] = repr(None)
        self.assertRaisesRegex(
            tree.TreeError,
            "Cannot delete 'key' because it refers to records",
            self.tree.delete,
            *("key",),
        )

    def test_04_delete__key_not_in_empty_tree(self):
        # self.print_nodes()
        self.check_nodes()
        self.assertEqual(self.tree.delete("key"), None)
        # self.print_nodes()
        self.check_nodes()

    def test_05_delete__key_not_in_occupied_tree(self):
        self.tree.insert("k84")
        # self.print_nodes()
        self.check_nodes()
        self.assertEqual(self.tree.delete("key"), None)
        # self.print_nodes()
        self.check_nodes()

    def test_06_delete__only_key_in_tree(self):
        self.tree.insert("key")
        # self.print_nodes()
        self.check_nodes()
        self.assertEqual(self.tree.delete("key"), None)
        # self.print_nodes()
        self.check_nodes()

    def test_07_delete__keys_in_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1__high_tree_node" in self.database.dbenv, False)

    def test_08_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()

    def test_09_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()

    def test_10_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()

    def test_11_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()

    def test_12_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()

    def test_13_delete__keys_in_leaf_nodes_off_root(self):
        # Fill four leaf nodes then delete keys from one of middle node until
        # empty.  This will exercise delete only, take from left, take from
        # right, merge with left. merge with right, and collapse one level.
        self.assertEqual(self.tree.branching_factor, 5)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        self.tree.insert("k59")
        self.tree.insert("k58")
        self.tree.insert("k57")
        self.tree.insert("k56")
        self.tree.insert("k55")
        self.tree.insert("k54")
        self.tree.insert("k53")
        self.tree.insert("k31")
        self.tree.insert("k42")
        self.tree.insert("k542")
        self.tree.insert("k548")
        self.tree.insert("k563")
        self.tree.insert("k567")
        self.tree.insert("k584")
        self.tree.insert("k586")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k54")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k548")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k55")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k542")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k563")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k56")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k57")
        # self.print_nodes()
        self.tree.delete("k58")
        # self.print_nodes()
        self.tree.delete("k584")
        # self.print_nodes()
        self.tree.delete("k567")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k59")
        # self.print_nodes()
        self.tree.delete("k586")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k53")
        # self.print_nodes()
        self.tree.delete("k42")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k31")
        # self.print_nodes()
        self.check_nodes()

    def test_14_delete__keys_in_leaf_nodes_off_branch(self):
        # Populate a tree with root, branch, and leaf, nodes.  Then delete keys
        # until a branch is too empty then figure what to do from example.
        # The insertions for test_05_insert__populate_tree_... will do setup.
        self.assertEqual(self.tree.branching_factor, 5)
        # self.print_nodes()
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        self.tree.insert("k59")
        self.tree.insert("k58")
        self.tree.insert("k57")
        self.tree.insert("k56")
        self.tree.insert("k55")
        self.tree.insert("k54")
        self.tree.insert("k53")
        self.tree.insert("k31")
        self.tree.insert("k32")
        self.tree.insert("k84")
        self.tree.insert("k83")
        self.tree.insert("k85")
        self.tree.insert("k87")
        self.tree.insert("k86")
        self.tree.insert("k88")
        self.tree.insert("k80")
        self.tree.insert("k81")
        self.tree.insert("k69")
        self.tree.insert("k68")
        self.tree.insert("k67")
        self.tree.insert("k66")
        self.tree.insert("k65")
        self.tree.insert("k64")
        self.tree.insert("k63")
        self.tree.insert("k62")
        self.tree.insert("k61")
        self.tree.insert("k60")
        # self.print_nodes()
        self.check_nodes()
        # Delete from node 4 because this leaf has 4 siblings in it's parent.
        self.tree.delete("k56")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k57")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k59")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k87")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k84")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k85")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k86")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k80")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k81")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k54")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k56")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k83")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k32")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k59")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k31")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k58")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k60")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k88")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k67")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k61")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k63")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k55")
        # self.print_nodes()
        self.check_nodes()


class Tree_delete_branching_4(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.tree.branching_factor = 4

    def test_01_delete__keys_in_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1__high_tree_node" in self.database.dbenv, False)

    def test_02_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()

    def test_03_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()

    def test_04_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()

    def test_05_delete__key_in_root_to_solo_root(self):
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()

    def test_06_delete__keys_in_leaf_nodes_off_root(self):
        # Fill four leaf nodes then delete keys from one of middle node until
        # about to reduce to solo root.  This will exercise delete only, take
        # from left, take from right, merge with left. merge with right, and
        # collapse one level.
        self.assertEqual(self.tree.branching_factor, 4)
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        self.tree.insert("k59")
        self.tree.insert("k58")
        self.tree.insert("k57")
        self.tree.insert("k56")
        self.tree.insert("k60")
        self.tree.insert("k25")
        self.tree.insert("k35")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k35")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k57")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k56")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k58")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k60")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.check_nodes()
        # Rest would copy tests 01, 02, 03, 04, or 05.

    def test_07_delete__keys_in_leaf_nodes_off_branch(self):
        # Populate a tree with root, branch, and leaf, nodes.  Then delete keys
        # until a branch is too empty then figure what to do from example.
        # The insertions for test_05_insert__populate_tree_... will do setup.
        self.assertEqual(self.tree.branching_factor, 4)
        # self.print_nodes()
        self.check_nodes()
        self.tree.insert("k3")
        self.tree.insert("k2")
        self.tree.insert("k5")
        self.tree.insert("k1")
        self.tree.insert("k4")
        self.tree.insert("k59")
        self.tree.insert("k58")
        self.tree.insert("k57")
        self.tree.insert("k56")
        self.tree.insert("k55")
        self.tree.insert("k54")
        self.tree.insert("k53")
        self.tree.insert("k31")
        self.tree.insert("k32")
        self.tree.insert("k84")
        self.tree.insert("k83")
        self.tree.insert("k85")
        self.tree.insert("k87")
        self.tree.insert("k86")
        self.tree.insert("k88")
        self.tree.insert("k80")
        self.tree.insert("k81")
        self.tree.insert("k20")
        self.tree.insert("k22")
        self.tree.insert("k24")
        # self.print_nodes()
        self.check_nodes()
        self.tree.insert("k26")
        # self.print_nodes()
        self.tree.insert("k28")
        self.tree.insert("k30")
        ##self.print_nodes()
        self.tree.insert("k32")
        ##self.print_nodes()
        self.tree.insert("k34")
        # self.print_nodes()
        self.check_nodes()
        # Delete from node 4 because this leaf is in smallest branch (node 7).
        self.tree.delete("k57")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k58")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k88")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k87")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k84")
        ##self.print_nodes()
        self.check_nodes()
        self.tree.delete("k85")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k86")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k80")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k81")
        # self.print_nodes()
        self.tree.delete("k54")
        # self.print_nodes()
        self.tree.delete("k56")
        # self.print_nodes()
        self.tree.delete("k5")
        # self.print_nodes()
        self.tree.delete("k83")
        # self.print_nodes()
        self.tree.delete("k3")
        # self.print_nodes()
        self.tree.delete("k2")
        # self.print_nodes()
        self.tree.delete("k32")
        # self.print_nodes()
        self.tree.delete("k59")
        # self.print_nodes()
        self.tree.delete("k31")
        # self.print_nodes()
        self.tree.delete("k1")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k20")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k55")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k22")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k53")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k24")
        # self.print_nodes()
        self.check_nodes()
        self.tree.delete("k4")
        # self.print_nodes()
        self.check_nodes()
        # Rest would copy tests 01, 02, 03, 04, or 05.


class Tree_delete_branching_6(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.tree.branching_factor = 6
        for i in range(50):
            self.tree.insert("k" + str(i))

    def test_01_delete__keys_in_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        # self.print_nodes()
        self.check_nodes()
        for i in range(50):
            self.assertEqual(
                self.tree.delete("k" + str(i)), None, msg="k" + str(i)
            )
            # self.print_nodes()
            self.check_nodes()

    def test_02_delete__keys_in_reverse_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        # self.print_nodes()
        self.check_nodes()
        for i in range(49, -1, -1):
            self.assertEqual(
                self.tree.delete("k" + str(i)), None, msg="k" + str(i)
            )
            # self.print_nodes()
            self.check_nodes()

    def test_03_delete__keys_in_random_order(self):
        self.assertEqual(self.tree.branching_factor, 6)
        keys = set()
        ordered_keys = []
        while len(keys) < 100:
            i = random.randint(50, 150)
            if i in keys:
                continue
            keys.add(i)
            ordered_keys.append(i)
            self.tree.insert("k" + str(i))
        # self.print_nodes()
        self.check_nodes()
        while len(keys) < 100:
            i = random.randint(0, 150)
            if i in keys:
                continue
            keys.add(i)
            ordered_keys.append(i)
            self.assertEqual(
                self.tree.delete("k" + str(i)), None, msg=repr(ordered_keys)
            )
            # self.print_nodes()
            self.check_nodes()


class Tree_locate(Tree_file1_field1):
    def test_01_locate__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "locate\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree.locate,
            *(None, None),
        )

    def test_02_locate__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "locate\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.tree.locate,
        )

    def test_03_locate__key_does_not_exist(self):
        # Does key exist on database? It does not, tree is not searched.
        self.assertEqual("1_1_2_key" in self.database.dbenv, False)
        self.assertEqual(self.tree.locate("key"), ("key", None))

    def test_04_locate__key_exists(self):
        # Does key exist on database? It does, accept whatever search() says.
        self.database.dbenv["1_1_2_key"] = True
        self.assertEqual("1_1_2_key" in self.database.dbenv, True)
        l = self.tree.locate("key")
        self.assertEqual(len(l), 2)
        self.assertEqual(l[0], "key")
        self.assertEqual(l[-1], None)


class Tree_search(Tree_file1_field1):
    def test_01_search__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "search\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree.search,
            *(None, None),
        )

    def test_02_search__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "search\(\) missing 1 required positional argument: ",
                    "'key'",
                )
            ),
            self.tree.search,
        )

    def test_02_search__empty_tree(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        n = self.tree.search("key")
        self.assertEqual(n, None)


class Tree__splitters(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.database.dbenv[self.tree.high_node] = repr(30)


class Tree__split_solo_root(Tree__splitters):
    def test_01__split_solo_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_solo_root\(\) takes 4 positional arguments ",
                    "but 5 were given",
                )
            ),
            self.tree._split_solo_root,
            *(None, None, None, None),
        )

    def test_02__split_solo_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_solo_root\(\) missing 3 required positional arguments: ",
                    "'key', 'nodepath', and 'insertion_point'",
                )
            ),
            self.tree._split_solo_root,
        )

    def test_03__split_solo_root__insert_low(self):
        nodepath = [
            tree._Node(1, 2, None, None, ["k0", "k2", "k4", "k6"], None)
        ]
        self.tree._split_solo_root("k1", nodepath, 1)
        self.assertEqual(len(nodepath), 2)
        self.assertEqual(
            nodepath[0].node, [32, 1, None, None, ["k4"], [1, 31]]
        )
        self.assertEqual(
            nodepath[1].node, [1, 4, None, 31, ["k0", "k1", "k2"], None]
        )

    def test_04__split_solo_root__insert_high(self):
        nodepath = [
            tree._Node(1, 2, None, None, ["k0", "k2", "k4", "k6"], None)
        ]
        self.tree._split_solo_root("k5", nodepath, 3)
        self.assertEqual(len(nodepath), 2)
        self.assertEqual(
            nodepath[0].node, [32, 1, None, None, ["k5"], [1, 31]]
        )
        self.assertEqual(
            nodepath[1].node, [31, 4, 1, None, ["k5", "k6"], None]
        )


class Tree__split_leaf(Tree__splitters):
    def test_01__split_leaf__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_leaf\(\) takes 4 positional arguments ",
                    "but 5 were given",
                )
            ),
            self.tree._split_leaf,
            *(None, None, None, None),
        )

    def test_02__split_leaf__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_leaf\(\) missing 3 required positional arguments: ",
                    "'key', 'nodepath', and 'insertion_point'",
                )
            ),
            self.tree._split_leaf,
        )

    def test_03__split_leaf__insert_low(self):
        nodepath = [
            tree._Node(1, 4, None, None, ["k0", "k2", "k4", "k6"], None)
        ]
        self.assertEqual(self.tree._split_leaf("k1", nodepath, 1), ("k4", 31))
        self.assertEqual(len(nodepath), 1)
        self.assertEqual(
            nodepath[0].node, [1, 4, None, 31, ["k0", "k1", "k2"], None]
        )

    def test_04__split_leaf__insert_high(self):
        nodepath = [
            tree._Node(1, 4, None, None, ["k0", "k2", "k4", "k6"], None)
        ]
        self.assertEqual(self.tree._split_leaf("k5", nodepath, 3), ("k5", 31))
        self.assertEqual(len(nodepath), 1)
        self.assertEqual(
            nodepath[0].node, [31, 4, 1, None, ["k5", "k6"], None]
        )


class Tree__split_root(Tree__splitters):
    def test_01__split_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_root\(\) takes 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.tree._split_root,
            *(None, None, None),
        )

    def test_02__split_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_root\(\) missing 2 required positional arguments: ",
                    "'nodepath' and 'splitter'",
                )
            ),
            self.tree._split_root,
        )

    def test_03__split_root__insert_low(self):
        nodepath = [
            tree._Node(
                1, 1, None, None, ["k0", "k2", "k4", "k6"], [5, 6, 7, 8, 9]
            )
        ]
        self.tree._split_root(nodepath, ("k1", 20))
        self.assertEqual(len(nodepath), 2)
        self.assertEqual(
            nodepath[0].node, [32, 1, None, None, ["k2"], [1, 31]]
        )
        self.assertEqual(
            nodepath[1].node, [1, 3, None, None, ["k0", "k1"], [5, 6, 20]]
        )

    def test_04__split_root__insert_high(self):
        nodepath = [
            tree._Node(
                1, 1, None, None, ["k0", "k2", "k4", "k6"], [5, 6, 7, 8, 9]
            )
        ]
        self.tree._split_root(nodepath, ("k5", 20))
        self.assertEqual(len(nodepath), 2)
        self.assertEqual(
            nodepath[0].node, [32, 1, None, None, ["k4"], [1, 31]]
        )
        self.assertEqual(
            nodepath[1].node, [31, 3, None, None, ["k5", "k6"], [8, 20, 9]]
        )


class Tree__split_branch(Tree__splitters):
    def test_01__split_branch__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_branch\(\) takes 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.tree._split_branch,
            *(None, None, None),
        )

    def test_02__split_branch__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_split_branch\(\) missing 2 required positional arguments: ",
                    "'nodepath' and 'splitter'",
                )
            ),
            self.tree._split_branch,
        )

    def test_03__split_branch__insert_low(self):
        nodepath = [
            tree._Node(
                1, 3, None, None, ["k0", "k2", "k4", "k6"], [5, 6, 7, 8, 9]
            )
        ]
        self.tree._split_branch(nodepath, ("k1", 20))
        self.assertEqual(len(nodepath), 1)
        self.assertEqual(
            nodepath[0].node, [1, 3, None, None, ["k0", "k1"], [5, 6, 20]]
        )

    def test_04__split_branch__insert_high(self):
        nodepath = [
            tree._Node(
                1, 3, None, None, ["k0", "k2", "k4", "k6"], [5, 6, 7, 8, 9]
            )
        ]
        self.tree._split_branch(nodepath, ("k5", 20))
        self.assertEqual(len(nodepath), 1)
        self.assertEqual(
            nodepath[0].node, [31, 3, None, None, ["k5", "k6"], [8, 20, 9]]
        )


class Tree__read_root(Tree__splitters):
    def test_01__read_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "read_root\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.tree.read_root,
            *(None,),
        )

    def test_02__read_root(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual(self.tree.read_root(), b"None")
        self.database.dbenv["1_1"] = repr(True)
        self.assertEqual(self.tree.read_root(), b"True")


class Tree__write_root(Tree__splitters):
    def test_01__write_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_root\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree._write_root,
            *(None, None),
        )

    def test_02__write_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_root\(\) missing 1 required positional ",
                    "argument: 'nodedata'",
                )
            ),
            self.tree._write_root,
        )

    def test_03__write_root(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.tree._write_root(True)
        self.assertEqual(self.database.dbenv["1_1"], b"True")


class Tree__read_node(Tree__splitters):
    def test_01__read_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "read_node\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree.read_node,
            *(None, None),
        )

    def test_02__read_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "read_node\(\) missing 1 required positional ",
                    "argument: 'node_number'",
                )
            ),
            self.tree.read_node,
        )

    def test_03__read_node(self):
        self.assertRaisesRegex(
            KeyError,
            "'key not found|",  # unqlite and vedis exception text.
            self.tree.read_node,
            *(0,),
        )
        self.database.dbenv["1_1_2_0"] = repr("None")
        self.assertEqual(self.tree.read_node(0), b"'None'")


class Tree__write_node(Tree__splitters):
    def test_01__write_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_node\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree._write_node,
            *(None, None),
        )

    def test_02__write_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_node\(\) missing 1 required positional ",
                    "argument: 'nodedata'",
                )
            ),
            self.tree._write_node,
        )

    def test_03__write_node(self):
        self.assertEqual("1_1_2_1" in self.database.dbenv, False)
        self.tree._write_node([1, None])
        self.assertEqual(self.database.dbenv["1_1_2_1"], b"[1, None]")


class Tree__delete_root(Tree__splitters):
    def test_01__delete_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_delete_root\(\) takes 1 positional argument ",
                    "but 2 were given",
                )
            ),
            self.tree._delete_root,
            *(None,),
        )

    def test_02__delete_root(self):
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.tree._delete_root()
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.database.dbenv["1_1"] = repr(True)
        self.assertEqual(self.database.dbenv["1_1"], b"True")
        self.tree._delete_root()
        self.assertEqual("1_1" in self.database.dbenv, False)


class Tree__delete_node(Tree__splitters):
    def test_01__delete_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_delete_node\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree._delete_node,
            *(None, None),
        )

    def test_02___delete_node__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_delete_node\(\) missing 1 required positional ",
                    "argument: 'node_number'",
                )
            ),
            self.tree._delete_node,
        )

    def test_03__delete_node(self):
        self.assertEqual("1_1_2_2" in self.database.dbenv, False)
        self.database.dbenv["1_1_2_2"] = repr("None")
        self.assertEqual("1_1_2_2" in self.database.dbenv, True)
        self.tree._delete_node(2)
        self.assertEqual("1_1_2_2" in self.database.dbenv, False)


class Tree__write_modified_nodes(Tree__splitters):
    def setUp(self):
        super().setUp()
        self.node10 = tree._Node(10, 0)
        self.node11 = tree._Node(11, tree._Node.ROOT)
        self.node12 = tree._Node(12, tree._Node.SOLO_ROOT)
        self.node13 = tree._Node(13, tree._Node.BRANCH)
        self.node14 = tree._Node(14, tree._Node.LEAF)
        # del self.database.dbenv['1_1']

    def test_01__write_modified_nodes__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_modified_nodes\(\) takes 2 positional arguments ",
                    "but 3 were given",
                )
            ),
            self.tree._write_modified_nodes,
            *(None, None),
        )

    def test_02__write_modified_nodes__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_write_modified_nodes\(\) missing 1 required positional ",
                    "argument: 'nodes'",
                )
            ),
            self.tree._write_modified_nodes,
        )

    def test_03__write_modified_nodes__modified_is_False(self):
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)
        for n in (
            self.node10,
            self.node11,
            self.node12,
            self.node13,
            self.node14,
        ):
            self.assertEqual(n.modified, False)
            self.assertEqual(self.tree._write_modified_nodes([n]), None)
            self.assertEqual(n.modified, False)
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)

    def test_04__write_modified_nodes__modified_is_True(self):
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)
        for n in (
            self.node10,
            self.node11,
            self.node12,
            self.node13,
            self.node14,
        ):
            self.assertEqual(n.modified, False)
            n.modified = True
            self.assertEqual(self.tree._write_modified_nodes([n]), None)
            self.assertEqual(n.modified, False)
        self.assertEqual("1_1_2_10" in self.database.dbenv, True)
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, True)
        self.assertEqual("1_1_2_14" in self.database.dbenv, True)

    def test_05__write_modified_nodes__modified_is_None(self):
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)
        for n in (
            self.node10,
            self.node11,
            self.node12,
            self.node13,
            self.node14,
        ):
            self.assertEqual(n.modified, False)
            n.modified = None
            self.assertEqual(self.tree._write_modified_nodes([n]), None)
            self.assertEqual(n.modified, False)
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)

    def test_06__write_modified_nodes__modified_is_None__key_exists(self):
        self.assertEqual("1_1_2_10" in self.database.dbenv, False)
        self.assertEqual("1_1" in self.database.dbenv, False)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, False)
        self.assertEqual("1_1_2_14" in self.database.dbenv, False)
        for n in (
            self.node10,
            self.node11,
            self.node12,
            self.node13,
            self.node14,
        ):
            n.modified = True
            self.assertEqual(self.tree._write_modified_nodes([n]), None)
        self.assertEqual("1_1_2_10" in self.database.dbenv, True)
        self.assertEqual("1_1" in self.database.dbenv, True)
        self.assertEqual("1_1_2_11" in self.database.dbenv, False)
        self.assertEqual("1_1_2_12" in self.database.dbenv, False)
        self.assertEqual("1_1_2_13" in self.database.dbenv, True)
        self.assertEqual("1_1_2_14" in self.database.dbenv, True)
        for n in (
            self.node10,
            self.node13,
            self.node14,
        ):
            self.assertEqual(n.modified, False)
            n.modified = None
            self.assertEqual(self.tree._write_modified_nodes([n]), None)
            self.assertEqual(n.modified, False)
        # Exchanging node11 and node12 in following four tests works too.
        self.node11.modified = None
        self.node12.modified = None
        self.assertEqual(self.tree._write_modified_nodes([self.node11]), None)
        self.assertEqual(self.node11.modified, False)
        self.assertEqual(self.tree._write_modified_nodes([self.node12]), None)
        self.assertEqual(self.node12.modified, False)


class Tree__splitter_node_for_key(Tree__splitters):
    def setUp(self):
        super().setUp()
        self.nodepath = [
            tree._Node(5, tree._Node.ROOT, keys=["k2"]),
            tree._Node(6, tree._Node.BRANCH, keys=["k4"]),
            tree._Node(7, tree._Node.LEAF),
        ]

    def test_01__splitter_node_for_key__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_splitter_node_for_key\(\) takes 3 positional arguments ",
                    "but 4 were given",
                )
            ),
            self.tree._splitter_node_for_key,
            *(None, None, None),
        )

    def test_02__splitter_node_for_key__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_splitter_node_for_key\(\) missing 2 required positional ",
                    "arguments: 'nodepath' and 'key'",
                )
            ),
            self.tree._splitter_node_for_key,
        )

    def test_03__splitter_node_for_key__empty_nodepath(self):
        self.assertRaisesRegex(
            tree.TreeError,
            "Key expected but not found in root or branch node",
            self.tree._splitter_node_for_key,
            *([], None),
        )

    def test_04__splitter_node_for_key__key_not_in_nodepath(self):
        self.assertRaisesRegex(
            tree.TreeError,
            "Key expected but not found in root or branch node",
            self.tree._splitter_node_for_key,
            *(self.nodepath, ""),
        )

    def test_05__splitter_node_for_key__key_in_nodepath(self):
        node, poffset = self.tree._splitter_node_for_key(self.nodepath, "k4")
        self.assertIs(node, self.nodepath[1])
        self.assertEqual(poffset, 0)
        node, poffset = self.tree._splitter_node_for_key(self.nodepath, "k2")
        self.assertIs(node, self.nodepath[0])
        self.assertEqual(poffset, 0)

    # 'node[tree._Node.KEYS]' should never be empty except temporarely when
    # merging nodes in trees with branching factors less than 6.
    # This method does not check for the condition.
    def test_06__splitter_node_for_key__nodepath_keys_empty(self):
        self.nodepath[0].node[tree._Node.KEYS] = []
        self.assertRaisesRegex(
            tree.TreeError,
            "Key expected but not found in root or branch node",
            self.tree._splitter_node_for_key,
            *(self.nodepath, "k2"),
        )

    # 'node[tree._Node.KEYS]' should never be empty except temporarely when
    # merging nodes in trees with branching factors less than 6.
    # This method does not check for the condition.
    def test_07__splitter_node_for_key__nodepath_keys_empty(self):
        self.nodepath[1].node[tree._Node.KEYS] = []
        node, poffset = self.tree._splitter_node_for_key(self.nodepath, "k2")
        self.assertIs(node, self.nodepath[0])
        self.assertEqual(poffset, 0)


class Tree__replace_splitter_in_branch_or_root(Tree__splitters):
    def test_01__replace_splitter_in_branch_or_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_replace_splitter_in_branch_or_root\(\) takes 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.tree._replace_splitter_in_branch_or_root,
            *(None, None),
        )

    def test_02__replace_splitter_in_branch_or_root__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_replace_splitter_in_branch_or_root\(\) missing 1 required ",
                    "positional argument: 'nodepath'",
                )
            ),
            self.tree._replace_splitter_in_branch_or_root,
        )

    def test_03__replace_splitter_in_branch_or_root(self):
        nodepath = [
            tree._Node(5, tree._Node.ROOT, keys=["k2"]),
            tree._Node(6, tree._Node.BRANCH, keys=["k4"]),
            tree._Node(7, tree._Node.LEAF, keys=["k2", "k3"]),
        ]
        self.assertEqual(
            self.tree._replace_splitter_in_branch_or_root(nodepath), None
        )
        node = nodepath[0]
        self.assertEqual(node.modified, True)
        self.assertEqual(node.node[tree._Node.KEYS][0], "k3")


# _merge_leaf is called in one place, in delete(), and exists to park the code
# in a more convenient place.  For now rely on delete's unittests apart from
# arguments tests.
class Tree__merge_leaf(Tree__splitters):
    def test_01__merge_leaf__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_merge_leaf\(\) takes 4 positional ",
                    "arguments but 5 were given",
                )
            ),
            self.tree._merge_leaf,
            *(None, None, None, None),
        )

    def test_02__merge_leaf__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_merge_leaf\(\) missing 3 required positional arguments: ",
                    "'key', 'nodepath', and 'deletion_point'",
                )
            ),
            self.tree._merge_leaf,
        )


# _merge_branch is called in one place, in delete(), and exists to park the code
# in a more convenient place.  For now rely on delete's unittests apart from
# arguments tests.
class Tree__merge_branch(Tree__splitters):
    def test_01__merge_branch__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_merge_branch\(\) takes 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.tree._merge_branch,
            *(None, None),
        )

    def test_02__merge_branch__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "_merge_branch\(\) missing 1 required positional argument: ",
                    "'nodepath'",
                )
            ),
            self.tree._merge_branch,
        )


class Cursor___init__(Tree_file1_field1):
    def test_01___init____arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            tree.Cursor,
            *(None, None),
        )

    def test_02___init____arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 1 required positional argument: ",
                    "'tree'",
                )
            ),
            tree.Cursor,
        )

    def test_03___init__(self):
        c = tree.Cursor(self.tree)
        self.assertEqual(
            sorted(c.__dict__.keys()),
            [
                "_partial",
                "current_key",
                "current_key_node_number",
                "tree",
            ],
        )
        self.assertEqual(c._partial, None)
        self.assertEqual(c.current_key, None)
        self.assertEqual(c.current_key_node_number, None)
        self.assertIs(c.tree, self.tree)


class Cursor_methods_empty_database(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.cursor = tree.Cursor(self.tree)

    def test_01_first__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "first\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.cursor.first,
            *(None,),
        )

    def test_02_get_position_of_key__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_position_of_key\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_position_of_key,
            *(None, None),
        )

    def test_03_get_key_at_position__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "get_key_at_position\(\) takes from 1 to 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.get_key_at_position,
            *(None, None),
        )

    def test_04_last__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "last\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.cursor.last,
            *(None,),
        )

    def test_05_nearest__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "nearest\(\) takes 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.nearest,
            *(None, None),
        )

    def test_06_next__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "next\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.cursor.next,
            *(None,),
        )

    def test_07_prev__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "prev\(\) takes 1 positional ",
                    "argument but 2 were given",
                )
            ),
            self.cursor.prev,
            *(None,),
        )

    def test_08_setat__arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "setat\(\) takes 2 positional ",
                    "arguments but 3 were given",
                )
            ),
            self.cursor.setat,
            *(None, None),
        )

    def test_09__methods(self):
        self.assertEqual(self.cursor.first(), None)
        self.assertEqual(self.cursor.last(), None)
        self.assertEqual(self.cursor.nearest(""), None)
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor.setat(""), None)


class Cursor_methods_populated_database(Tree_file1_field1):
    def setUp(self):
        super().setUp()
        self.cursor = tree.Cursor(self.tree)
        for i in range(10):
            self.tree.insert(str(i + 10))

    def test_01__first(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.first(), "10")
        self.tree.delete(str(13))
        self.tree.delete(str(15))
        self.tree.delete(str(17))
        self.tree.delete(str(18))
        self.tree.delete(str(19))
        self.tree.delete(str(12))
        self.tree.delete(str(10))
        # self.print_nodes()
        self.assertEqual(self.cursor.first(), "11")
        self.tree.delete(str(11))
        self.tree.delete(str(14))
        self.tree.delete(str(16))
        # self.print_nodes()
        self.assertEqual(self.cursor.first(), None)

    def test_02__last(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.last(), "19")
        self.tree.delete(str(13))
        self.tree.delete(str(15))
        self.tree.delete(str(17))
        self.tree.delete(str(18))
        self.tree.delete(str(19))
        self.tree.delete(str(12))
        self.tree.delete(str(10))
        # self.print_nodes()
        self.assertEqual(self.cursor.last(), "16")
        self.tree.delete(str(11))
        self.tree.delete(str(14))
        self.tree.delete(str(16))
        # self.print_nodes()
        self.assertEqual(self.cursor.last(), None)

    def test_03__nearest(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.nearest("20"), None)
        self.assertEqual(self.cursor.nearest("185"), "19")
        self.assertEqual(self.cursor.nearest("155"), "16")
        self.assertEqual(self.cursor.nearest("15"), "15")
        self.tree.delete(str(13))
        self.tree.delete(str(15))
        self.tree.delete(str(17))
        self.tree.delete(str(18))
        self.tree.delete(str(19))
        self.tree.delete(str(12))
        self.tree.delete(str(10))
        self.tree.delete(str(11))
        self.tree.delete(str(14))
        self.tree.delete(str(16))
        # self.print_nodes()
        self.assertEqual(self.cursor.nearest("30"), None)

    def test_04__next(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.next(), "10")
        self.assertEqual(self.cursor.next(), "11")
        self.assertEqual(self.cursor.next(), "12")
        self.assertEqual(self.cursor.next(), "13")
        self.assertEqual(self.cursor.next(), "14")
        self.assertEqual(self.cursor.next(), "15")
        self.assertEqual(self.cursor.next(), "16")
        self.assertEqual(self.cursor.next(), "17")
        self.assertEqual(self.cursor.next(), "18")
        self.assertEqual(self.cursor.next(), "19")
        self.assertEqual(self.cursor.next(), None)
        self.assertEqual(self.cursor.first(), "10")
        self.assertEqual(self.cursor.next(), "11")
        self.assertEqual(self.cursor.next(), "12")
        self.assertEqual(self.cursor.next(), "13")
        self.assertEqual(self.cursor.next(), "14")
        # self.print_nodes()
        self.tree.delete(str(10))
        self.tree.delete(str(11))
        self.tree.delete(str(12))
        # self.print_nodes()
        self.assertEqual(self.cursor.next(), "15")
        self.assertEqual(self.cursor.first(), "13")
        self.assertEqual(self.cursor.next(), "14")
        self.tree.delete(str(14))
        # self.print_nodes()
        self.assertEqual(self.cursor.next(), "15")
        self.assertEqual(self.cursor.next(), "16")
        self.tree.delete(str(16))
        # self.print_nodes()
        self.assertEqual(self.cursor.next(), "17")
        # self.print_nodes()
        self.tree.delete(str(13))
        self.tree.delete(str(15))
        self.tree.delete(str(17))
        self.tree.delete(str(18))
        self.tree.delete(str(19))
        # self.print_nodes()
        self.assertEqual(self.cursor.next(), None)

    def test_05__next__force_search(self):
        # self.print_nodes()
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "10")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "11")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "12")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "13")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "14")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "15")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "16")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "17")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "18")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "19")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), None)
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.first(), "10")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "11")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "12")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "13")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "14")
        # self.print_nodes()
        self.tree.delete(str(10))
        self.tree.delete(str(11))
        self.tree.delete(str(12))
        # self.print_nodes()
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "15")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.first(), "13")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "14")
        self.tree.delete(str(14))
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.next(), "15")
        # self.print_nodes()

    def test_06__prev(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.prev(), "19")
        self.assertEqual(self.cursor.prev(), "18")
        self.assertEqual(self.cursor.prev(), "17")
        self.assertEqual(self.cursor.prev(), "16")
        self.assertEqual(self.cursor.prev(), "15")
        self.assertEqual(self.cursor.prev(), "14")
        self.assertEqual(self.cursor.prev(), "13")
        self.assertEqual(self.cursor.prev(), "12")
        self.assertEqual(self.cursor.prev(), "11")
        self.assertEqual(self.cursor.prev(), "10")
        self.assertEqual(self.cursor.prev(), None)
        self.assertEqual(self.cursor.last(), "19")
        self.assertEqual(self.cursor.prev(), "18")
        self.assertEqual(self.cursor.prev(), "17")
        self.assertEqual(self.cursor.prev(), "16")
        self.assertEqual(self.cursor.prev(), "15")
        # self.print_nodes()
        self.tree.delete(str(19))
        self.tree.delete(str(18))
        self.tree.delete(str(17))
        # self.print_nodes()
        self.assertEqual(self.cursor.prev(), "14")
        self.assertEqual(self.cursor.last(), "16")
        self.assertEqual(self.cursor.prev(), "15")
        self.tree.delete(str(15))
        # self.print_nodes()
        self.assertEqual(self.cursor.prev(), "14")
        self.assertEqual(self.cursor.prev(), "13")
        self.tree.delete(str(13))
        # self.print_nodes()
        self.assertEqual(self.cursor.prev(), "12")
        # self.print_nodes()
        self.tree.delete(str(10))
        self.tree.delete(str(11))
        self.tree.delete(str(12))
        self.tree.delete(str(14))
        self.tree.delete(str(16))
        # self.print_nodes()
        self.assertEqual(self.cursor.prev(), None)

    def test_07__prev__force_search(self):
        # self.print_nodes()
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "19")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "18")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "17")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "16")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "15")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "14")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "13")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "12")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "11")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "10")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), None)
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.last(), "19")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "18")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "17")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "16")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "15")
        # self.print_nodes()
        self.tree.delete(str(19))
        self.tree.delete(str(18))
        self.tree.delete(str(17))
        # self.print_nodes()
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "14")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.last(), "16")
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "15")
        self.tree.delete(str(14))
        self.cursor.current_key_node_number = 200
        self.assertEqual(self.cursor.prev(), "13")
        # self.print_nodes()

    def test_08__setat(self):
        # self.print_nodes()
        self.assertEqual(self.cursor.setat("20"), None)
        self.assertEqual(self.cursor.setat("145"), None)
        self.assertEqual(self.cursor.setat("12"), "12")
        self.tree.delete(str(13))
        self.tree.delete(str(15))
        self.tree.delete(str(17))
        self.tree.delete(str(18))
        self.tree.delete(str(19))
        self.tree.delete(str(12))
        self.tree.delete(str(10))
        self.tree.delete(str(11))
        self.tree.delete(str(14))
        self.tree.delete(str(16))
        self.assertEqual(self.cursor.setat("12"), None)
        self.assertEqual(self.cursor.setat("11"), None)


class _Node_class_constants(unittest.TestCase):
    def test_01_class_constants(self):
        self.assertEqual(
            sorted([c for c in dir(tree._Node) if c.isupper()]),
            [
                "BRANCH",
                "ENTRIES",
                "KEYS",
                "LEAF",
                "LEAF_NODES",
                "LEFT_SIBLING_NODE_NUMBER",
                "NODE_NUMBER",
                "NODE_TYPE",
                "RIGHT_SIBLING_NODE_NUMBER",
                "ROOT",
                "ROOT_NODES",
                "SOLO_ROOT",
            ],
        )
        self.assertEqual(tree._Node.ROOT, 1)
        self.assertEqual(tree._Node.SOLO_ROOT, 2)
        self.assertEqual(tree._Node.BRANCH, 3)
        self.assertEqual(tree._Node.LEAF, 4)
        self.assertEqual(tree._Node.ROOT_NODES, {1, 2})
        self.assertEqual(tree._Node.LEAF_NODES, {2, 4})
        self.assertEqual(tree._Node.NODE_NUMBER, 0)
        self.assertEqual(tree._Node.NODE_TYPE, 1)
        self.assertEqual(tree._Node.LEFT_SIBLING_NODE_NUMBER, 2)
        self.assertEqual(tree._Node.RIGHT_SIBLING_NODE_NUMBER, 3)
        self.assertEqual(tree._Node.KEYS, 4)
        self.assertEqual(tree._Node.ENTRIES, 5)


class _Node___init__(unittest.TestCase):
    def test_01___init___too_many_arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) takes from 3 to 7 positional arguments ",
                    "but 8 were given",
                )
            ),
            tree._Node,
            *(None, None, None, None, None, None, None),
        )

    def test_02___init___no_arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "__init__\(\) missing 2 required positional arguments: ",
                    "'number' and 'type_'",
                )
            ),
            tree._Node,
        )

    def test_03___init___minimum_arguments(self):
        self.assertEqual(tree._Node(1, 2).node, [1, 2, None, None, None, None])

    def test_04___init___attributes(self):
        n = tree._Node(1, 2)
        self.assertEqual(sorted(n.__slots__), ["modified", "node"])
        self.assertEqual(hasattr(n, "__dict__"), False)


class _Node___len__(unittest.TestCase):
    def test_01___len___default_arguments(self):
        self.assertRaisesRegex(
            TypeError,
            "object of type 'NoneType' has no len()",
            len,
            tree._Node(None, None),
        )

    def test_02___len___keys_present(self):
        self.assertEqual(len(tree._Node(None, None, keys=[])), 0)


class _Node_insert_into_branch_or_root(unittest.TestCase):
    def test_01_insert_into_branch_or_root__no_arguments(self):
        n = tree._Node(None, None, None, keys=[])
        self.assertRaisesRegex(
            TypeError,
            "".join(
                (
                    "insert_into_branch_or_root\(\) missing 2 required ",
                    "positional arguments: 'key' and 'node_number'",
                )
            ),
            n.insert_into_branch_or_root,
        )

    def test_02_insert_into_branch_or_root__single_path(self):
        n = tree._Node(None, None, None, keys=[], entries=[])
        self.assertEqual(n.insert_into_branch_or_root("key", 2), 0)
        self.assertEqual(n.node, [None, None, None, None, ["key"], [2]])


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    for dbe_module in unqlite, vedis:
        if dbe_module is None:
            continue
        runner().run(loader(Tree___init__))
        runner().run(loader(Tree_insert_branching_5))
        runner().run(loader(Tree_insert_branching_4))
        runner().run(loader(Tree_insert_branching_6))
        runner().run(loader(Tree_delete_branching_5))
        runner().run(loader(Tree_delete_branching_4))
        runner().run(loader(Tree_delete_branching_6))
        runner().run(loader(Tree_locate))
        runner().run(loader(Tree_search))
        runner().run(loader(Tree__split_solo_root))
        runner().run(loader(Tree__split_leaf))
        runner().run(loader(Tree__split_root))
        runner().run(loader(Tree__split_branch))
        runner().run(loader(Tree__read_root))
        runner().run(loader(Tree__write_root))
        runner().run(loader(Tree__read_node))
        runner().run(loader(Tree__write_node))
        runner().run(loader(Tree__delete_root))
        runner().run(loader(Tree__delete_node))
        runner().run(loader(Tree__write_modified_nodes))
        runner().run(loader(Tree__splitter_node_for_key))
        runner().run(loader(Tree__replace_splitter_in_branch_or_root))
        runner().run(loader(Tree__merge_leaf))
        runner().run(loader(Tree__merge_branch))
        runner().run(loader(Cursor___init__))
        runner().run(loader(Cursor_methods_empty_database))
        runner().run(loader(Cursor_methods_populated_database))
        runner().run(loader(_Node_class_constants))
        runner().run(loader(_Node___init__))
        runner().run(loader(_Node___len__))
        runner().run(loader(_Node_insert_into_branch_or_root))
