# tree.py
# Copyright 2018 Roger Marsh
# License: See LICENSE.TXT (BSD licence)

"""A Python tree implementation based on btree.h and btree.cpp from DPT.

This module provides ordered access to indicies stored in databases such as
UnQLite and Vedis.

"""
from bisect import bisect_right, bisect_left
from ast import literal_eval

from .constants import (
    SUBFILE_DELIMITER,
    FIELDS,
    BRANCHING_FACTOR,
    HIGH_TREE_NODE,
    SEGMENT_KEY_SUFFIX,
    TREE_NODE_SUFFIX,
)


class TreeError(Exception):
    """Exception for Tree class."""


class Tree:
    """Define a tree associated with a (file, field) in a _nosql database.

    The keys in the tree will be <field value> and the values in leaf nodes,
    including the solo root node if there is one, will be a reference to the
    node containing the key.  The records referenced by the key are accessed
    via database[key] where database is an UnQLite() or Vedis() object.

    The value found by tree search may become wrong if other keys are inserted
    or deleted.  If tke key is not on the node referenced by the value, at a
    later time, attempt to relocate the key in the tree before assuming it has
    been deleted.

    The tree provides ordered access to the values of a field.  It can, and
    should, be ignored when accessing data once the relevant key is known.
    """

    def __init__(self, file, field, database):
        """Note file, field, and database."""
        self.file = file
        self.field = field
        self.database = database
        self.key_root = database.table[SUBFILE_DELIMITER.join((file, field))][
            0
        ]
        self.key_segment = SUBFILE_DELIMITER.join(
            (self.key_root, SEGMENT_KEY_SUFFIX)
        )
        self.key_node = SUBFILE_DELIMITER.join(
            (self.key_root, TREE_NODE_SUFFIX)
        )
        self.high_node = SUBFILE_DELIMITER.join(
            (self.key_root, HIGH_TREE_NODE)
        )
        self.branching_factor = {
            k.lower(): v[BRANCHING_FACTOR]
            for k, v in database.specification[file][FIELDS].items()
            if isinstance(v, dict)
        }[field.lower()]
        self._next_node = None

    @property
    def next_node(self):
        """Increment node count and return it."""
        # Be sure to initialize self._next_node to current high node before
        # accessing this property: either 0 or value from database.
        self._next_node += 1
        return self._next_node

    def insert(self, key):
        """Insert key into the tree if not already present.

        The insert() method must be called before the (file, field, key) record
        is created because if the record exists it is assumed key is already in
        the tree.

        The (file, field) record is the root node of the tree.
        The (file, field, key) record is the list of segments containing record
        numbers of data records indexed by the key.

        """
        # Assume key is in tree if this record exists.
        # Cooperation with _nosql.Database.add_record_to_field_value() method.
        # if SUBFILE_DELIMITER.join((self.key_segment, key)
        #                          ) in self.database.dbenv:
        #    return
        nodepath = self.search(key)
        try:
            node = nodepath[-1]
        except TypeError:
            if nodepath is None:
                # Inserting into an empty tree.
                node = _Node(0, _Node.SOLO_ROOT, keys=[])
                nodepath = [node]
            else:
                raise
        keys = node.node[_Node.KEYS]
        node_type = node.node[_Node.NODE_TYPE]
        assert node_type in (_Node.LEAF, _Node.SOLO_ROOT)
        insertion_point = bisect_left(keys, key)
        if insertion_point != len(keys) and keys[insertion_point] == key:
            return
        if node_type == _Node.LEAF:
            if len(node) < self.branching_factor - 1:
                keys.insert(insertion_point, key)
                self._write_node(node.node)
                return
            splitter = self._split_leaf(key, nodepath, insertion_point)
            if splitter is None:
                return
            tailpath = []
            while True:
                tailpath.insert(0, nodepath.pop())
                node = nodepath[-1]
                if len(node) < self.branching_factor - 1:
                    node.insert_into_branch_or_root(*splitter)
                    if node.node[_Node.NODE_TYPE] == _Node.ROOT:
                        self._write_root(node.node)
                    else:
                        self._write_node(node.node)
                    nodepath.extend(
                        tailpath
                    )  # Assume worth preserve nodepath.
                    return
                if node.node[_Node.NODE_TYPE] == _Node.ROOT:
                    self._split_root(nodepath, splitter)
                    nodepath.extend(
                        tailpath
                    )  # Assume worth preserve nodepath.
                    return
                splitter = self._split_branch(nodepath, splitter)
        if len(node) < self.branching_factor - 1:
            if len(node) == 0:
                self.database.dbenv[self.high_node] = repr(
                    node.node[_Node.NODE_NUMBER]
                )
            keys.insert(insertion_point, key)
            self._write_root(node.node)
            return
        self._split_solo_root(key, nodepath, insertion_point)
        return

    def delete(self, key):
        """Delete key from the tree if no record references exist."""
        # Cooperation with _nosql.Database.remove_record_from_field_value()
        # method.
        if (
            SUBFILE_DELIMITER.join((self.key_segment, key))
            in self.database.dbenv
        ):
            raise TreeError(
                "".join(
                    ("Cannot delete '", key, "' because it refers to records")
                )
            )
        nodepath = self.search(key)
        try:
            node = nodepath[-1]
        except TypeError:
            if nodepath is None:
                # Deleting from an empty tree.
                return
            raise
        node_type = node.node[_Node.NODE_TYPE]
        assert node_type in (_Node.LEAF, _Node.SOLO_ROOT)
        keys = node.node[_Node.KEYS]
        deletion_point = bisect_left(keys, key)
        if deletion_point == len(keys) or keys[deletion_point] != key:
            return
        if node_type == _Node.LEAF:
            # len(node) equals number of entries.
            if len(node) > self.branching_factor // 2:
                if deletion_point == 0:
                    if node.node[_Node.LEFT_SIBLING_NODE_NUMBER] is not None:
                        self._replace_splitter_in_branch_or_root(nodepath)
                del keys[deletion_point]
                node.modified = True
                self._write_modified_nodes(nodepath)
                return
            if self._merge_leaf(key, nodepath, deletion_point) is None:
                return
            # merge_leaf resolves all cases where there are no branch nodes.
            tailpath = []
            while True:
                tailpath.insert(0, nodepath.pop())
                if (
                    len(nodepath) < 2
                ):  # Need a parent node too in _merge_branch.
                    break
                # len(nodepath[-1]) is one less than number of entries.
                if len(nodepath[-1]) + 1 >= self.branching_factor // 2:
                    break
                if self._merge_branch(nodepath) is None:
                    break
            nodepath.extend(tailpath)  # Assume worth preserve nodepath.
            return
        del keys[deletion_point]
        if len(keys):
            self._write_root(node.node)
        else:
            self._delete_root()
            del self.database.dbenv[self.high_node]
        return

    def locate(self, key):
        """Return key and location in the tree if present."""
        if (
            SUBFILE_DELIMITER.join((self.key_segment, key))
            not in self.database.dbenv
        ):
            return key, None
        return key, self.search(key)

    def search(self, key):
        """Return location in tree of key or where key would be put."""
        # bisect_right because if keys[n+1] > key >= keys[n] then the entry
        # where key will be found is entries[n+1] (all keys >= keys[-1] are
        # found in entries[-1] and all keys < keys[0] are found in entries[0]).
        # root = literal_eval(self.database.dbenv[self.key_root].decode())
        root = literal_eval(self.read_root().decode())
        if root is None:
            return None
        nodepath = [_Node(*root)]
        while True:
            tip = nodepath[-1].node
            type_ = tip[_Node.NODE_TYPE]
            if type_ == _Node.LEAF:
                return nodepath
            if type_ == _Node.BRANCH:
                nodepath.append(
                    _Node(
                        *literal_eval(
                            self.read_node(
                                tip[_Node.ENTRIES][
                                    bisect_right(tip[_Node.KEYS], key)
                                ]
                            ).decode()
                        )
                    )
                )
            elif type_ == _Node.ROOT:
                nodepath.append(
                    _Node(
                        *literal_eval(
                            self.read_node(
                                tip[_Node.ENTRIES][
                                    bisect_right(tip[_Node.KEYS], key)
                                ]
                            ).decode()
                        )
                    )
                )
            elif type_ == _Node.SOLO_ROOT:
                return nodepath
            else:
                raise TreeError(
                    "".join(
                        (
                            "Node type value",
                            str(type_),
                            " of node number ",
                            str(tip[_Node.NODE_NUMBER]),
                            " is not a recognised node type",
                        )
                    )
                )

    def _split_solo_root(self, key, nodepath, insertion_point):
        node = nodepath[-1]
        node_node = node.node
        nkeys = node_node[_Node.KEYS]
        nnumber = _Node.NODE_NUMBER
        assert len(nkeys) == self.branching_factor - 1
        split = self.branching_factor // 2
        nkeys.insert(insertion_point, key)
        self._next_node = literal_eval(
            self.database.dbenv[self.high_node].decode()
        )
        right_node_number = self.next_node
        rln = _Node(
            right_node_number,
            _Node.LEAF,
            left=node_node[nnumber],
            right=node_node[_Node.RIGHT_SIBLING_NODE_NUMBER],
            keys=nkeys[-split:],
        )
        lln = _Node(
            node_node[nnumber],
            _Node.LEAF,
            left=node_node[_Node.LEFT_SIBLING_NODE_NUMBER],
            right=right_node_number,
            keys=nkeys[:-split],
        )
        root_node_number = self.next_node
        root_node = _Node(
            root_node_number,
            _Node.ROOT,
            keys=[nkeys[-split]],
            entries=[lln.node[nnumber], rln.node[nnumber]],
        )
        self.database.dbenv[self.high_node] = repr(self._next_node)
        self._write_root(root_node.node)
        self._write_node(rln.node)
        self._write_node(lln.node)
        if insertion_point > split:
            nodepath[-1] = rln  # Assume it is worth preserving nodepath.
        else:
            nodepath[-1] = lln  # Assume it is worth preserving nodepath.
        nodepath.insert(
            0, root_node
        )  # Assume it is worth preserving nodepath.

    def _split_leaf(self, key, nodepath, insertion_point):
        node = nodepath[-1]
        node_node = node.node
        nkeys = node_node[_Node.KEYS]
        nnumber = _Node.NODE_NUMBER
        nright = node_node[_Node.RIGHT_SIBLING_NODE_NUMBER]
        assert len(nkeys) == self.branching_factor - 1
        split = self.branching_factor // 2
        nkeys.insert(insertion_point, key)
        self._next_node = literal_eval(
            self.database.dbenv[self.high_node].decode()
        )
        right_node_number = self.next_node
        rln = _Node(
            right_node_number,
            _Node.LEAF,
            left=node_node[nnumber],
            right=nright,
            keys=nkeys[-split:],
        )
        lln = _Node(
            node_node[nnumber],
            _Node.LEAF,
            left=node_node[_Node.LEFT_SIBLING_NODE_NUMBER],
            right=right_node_number,
            keys=nkeys[:-split],
        )
        if nright is not None:
            rsrln = literal_eval(self.read_node(nright).decode())
            rsrln[_Node.LEFT_SIBLING_NODE_NUMBER] = right_node_number
            self._write_node(rsrln)
        self.database.dbenv[self.high_node] = repr(self._next_node)
        self._write_node(rln.node)
        self._write_node(lln.node)
        if insertion_point > split:
            nodepath[-1] = rln  # Assume it is worth preserving nodepath.
        else:
            nodepath[-1] = lln  # Assume it is worth preserving nodepath.
        return rln.node[_Node.KEYS][0], right_node_number

    def _split_root(self, nodepath, splitter):
        node = nodepath[-1]
        node_node = node.node
        nkeys = node_node[_Node.KEYS]
        nnumber = _Node.NODE_NUMBER
        assert len(nkeys) == self.branching_factor - 1
        split = self.branching_factor // 2
        insertion_point = node.insert_into_branch_or_root(*splitter)
        self._next_node = literal_eval(
            self.database.dbenv[self.high_node].decode()
        )
        right_node_number = self.next_node
        rbn = _Node(
            right_node_number,
            _Node.BRANCH,
            keys=nkeys[-split:],
            entries=node_node[_Node.ENTRIES][-split - 1 :],
        )
        lbn = _Node(
            node_node[nnumber],
            _Node.BRANCH,
            keys=nkeys[: -split - 1],
            entries=node_node[_Node.ENTRIES][: -split - 1],
        )
        root_node_number = self.next_node
        root_node = _Node(
            root_node_number,
            _Node.ROOT,
            keys=[nkeys[-split - 1]],
            entries=[lbn.node[nnumber], rbn.node[nnumber]],
        )
        self.database.dbenv[self.high_node] = repr(self._next_node)
        self._write_root(root_node.node)
        self._write_node(rbn.node)
        self._write_node(lbn.node)
        if insertion_point > split:
            nodepath[-1] = rbn  # Assume it is worth preserving nodepath.
        else:
            nodepath[-1] = lbn  # Assume it is worth preserving nodepath.
        nodepath.insert(
            0, root_node
        )  # Assume it is worth preserving nodepath.

    def _split_branch(self, nodepath, splitter):
        node = nodepath[-1]
        node_node = node.node
        nkeys = node_node[_Node.KEYS]
        nnumber = _Node.NODE_NUMBER
        assert len(nkeys) == self.branching_factor - 1
        split = self.branching_factor // 2
        insertion_point = node.insert_into_branch_or_root(*splitter)
        self._next_node = literal_eval(
            self.database.dbenv[self.high_node].decode()
        )
        right_node_number = self.next_node
        rbn = _Node(
            right_node_number,
            _Node.BRANCH,
            keys=nkeys[-split:],
            entries=node_node[_Node.ENTRIES][-split - 1 :],
        )
        lbn = _Node(
            node_node[nnumber],
            _Node.BRANCH,
            keys=nkeys[: -split - 1],
            entries=node_node[_Node.ENTRIES][: -split - 1],
        )
        self.database.dbenv[self.high_node] = repr(self._next_node)
        self._write_node(rbn.node)
        self._write_node(lbn.node)
        if insertion_point > split:
            nodepath[-1] = rbn  # Assume it is worth preserving nodepath.
        else:
            nodepath[-1] = lbn  # Assume it is worth preserving nodepath.
        return nkeys[-split - 1], right_node_number

    # Anticipate maintaining a cache of _Node.node objects.
    def read_root(self):
        """Return root node, or b"None" if root node does not exist."""
        try:
            return self.database.dbenv[self.key_root]
        except KeyError:
            return b"None"

    # Anticipate maintaining a cache of _Node.node objects.
    def _write_root(self, nodedata):
        self.database.dbenv[self.key_root] = repr(nodedata)

    # Anticipate maintaining a cache of _Node.node objects.
    def read_node(self, node_number):
        """Return node referenced by node_number."""
        return self.database.dbenv[
            SUBFILE_DELIMITER.join((self.key_node, str(node_number)))
        ]

    # Anticipate maintaining a cache of _Node.node objects.
    def _write_node(self, nodedata):
        self.database.dbenv[
            SUBFILE_DELIMITER.join(
                (self.key_node, str(nodedata[_Node.NODE_NUMBER]))
            )
        ] = repr(nodedata)

    # Anticipate maintaining a cache of _Node.node objects.
    def _delete_root(self):
        try:
            del self.database.dbenv[self.key_root]
        except KeyError:
            pass

    # Anticipate maintaining a cache of _Node.node objects.
    def _delete_node(self, node_number):
        try:
            del self.database.dbenv[
                SUBFILE_DELIMITER.join((self.key_node, str(node_number)))
            ]
        except KeyError:
            pass

    def _write_modified_nodes(self, nodes):
        for node in nodes:
            if node.modified:
                node_node = node.node
                if node_node[_Node.NODE_TYPE] in _Node.ROOT_NODES:
                    self._write_root(node.node)
                else:
                    self._write_node(node.node)
                node.modified = False
            elif node.modified is None:
                node_node = node.node
                if node_node[_Node.NODE_TYPE] in _Node.ROOT_NODES:
                    self._delete_root()
                else:
                    self._delete_node(node.node[_Node.NODE_NUMBER])
                node.modified = False

    def _splitter_node_for_key(self, nodepath, key):
        # Caller can assume the returned node is in nodepath, but this method
        # does not assume the caller will modify the returned node.
        # It is assumed nodepath[-1] is the leaf node containing key, and is
        # not the left-most leaf node in the tree.
        for node in reversed(nodepath[:-1]):
            pkeys = node.node[_Node.KEYS]
            poffset = bisect_left(pkeys, key)
            if poffset == len(pkeys) or pkeys[poffset] != key:
                continue
            return node, poffset
        raise TreeError("Key expected but not found in root or branch node")

    def _replace_splitter_in_branch_or_root(self, nodepath):
        # Somewhere in nodepath above leaf <keys[1]> must replace <keys[0]>.
        # It is assumed, without test, that oldkey is being removed from tree,
        # and that oldkey is current value at node.node[_Node.KEYS][offset].
        # Caller is assumed to have checked that oldkey will not be in the
        # left-most node in the tree, hence not expected to be present in a
        # branch or root node.
        oldkey, newkey = nodepath[-1].node[_Node.KEYS][:2]
        node, offset = self._splitter_node_for_key(nodepath, oldkey)
        node.node[_Node.KEYS][offset] = newkey
        node.modified = True

    def _merge_leaf(self, key, nodepath, deletion_point):
        node = nodepath[-1]
        parent = nodepath[-2]
        entries = parent.node[_Node.ENTRIES]
        offset = entries.index(node.node[_Node.NODE_NUMBER])
        if offset < len(entries) - 1:
            # Take key from right sibling if possible.
            right_sibling = _Node(
                *literal_eval(self.read_node(entries[offset + 1]).decode())
            )
            if len(right_sibling) > self.branching_factor // 2:
                keys = node.node[_Node.KEYS]
                sibling_keys = right_sibling.node[_Node.KEYS]
                splitter, split_offset = self._splitter_node_for_key(
                    nodepath, sibling_keys[0]
                )
                if deletion_point == 0:
                    if node.node[_Node.LEFT_SIBLING_NODE_NUMBER] is not None:
                        self._replace_splitter_in_branch_or_root(nodepath)
                del keys[deletion_point]
                keys.append(sibling_keys.pop(0))
                splitter.node[_Node.KEYS][split_offset] = sibling_keys[0]
                node.modified = True
                splitter.modified = True
                right_sibling.modified = True
                self._write_modified_nodes(nodepath)
                self._write_modified_nodes([right_sibling])
                return None
        if offset > 0:
            # Take key from left sibling if possible.
            left_sibling = _Node(
                *literal_eval(self.read_node(entries[offset - 1]).decode())
            )
            if len(left_sibling) > self.branching_factor // 2:
                keys = node.node[_Node.KEYS]
                splitter, split_offset = self._splitter_node_for_key(
                    nodepath, keys[0]
                )
                if deletion_point == 0:
                    if node.node[_Node.LEFT_SIBLING_NODE_NUMBER] is not None:
                        self._replace_splitter_in_branch_or_root(nodepath)
                del keys[deletion_point]
                keys.insert(0, left_sibling.node[_Node.KEYS].pop())
                splitter.node[_Node.KEYS][split_offset] = keys[0]
                node.modified = True
                splitter.modified = True
                left_sibling.modified = True
                self._write_modified_nodes(nodepath)
                self._write_modified_nodes([left_sibling])
                return None
        if offset < len(entries) - 1:
            # Merge right sibling into nodepath[-1]
            keys = node.node[_Node.KEYS]
            sibling_keys = right_sibling.node[_Node.KEYS]
            splitter, split_offset = self._splitter_node_for_key(
                nodepath, sibling_keys[0]
            )
            if deletion_point == 0:
                if node.node[_Node.LEFT_SIBLING_NODE_NUMBER] is not None:
                    self._replace_splitter_in_branch_or_root(nodepath)
            del keys[deletion_point]
            keys.extend(sibling_keys)
            rsrsnn = right_sibling.node[_Node.RIGHT_SIBLING_NODE_NUMBER]
            node.node[_Node.RIGHT_SIBLING_NODE_NUMBER] = rsrsnn
            if rsrsnn is not None:
                rsrsnode = _Node(
                    *literal_eval(self.read_node(rsrsnn).decode())
                )
                rsrsnode.node[_Node.LEFT_SIBLING_NODE_NUMBER] = node.node[
                    _Node.NODE_NUMBER
                ]
                rsrsnode.modified = True
                self._write_modified_nodes([rsrsnode])
            del splitter.node[_Node.KEYS][split_offset]
            # In case parent is also the splitter, which is likely.
            db_offset = entries[offset + 1]
            del splitter.node[_Node.ENTRIES][split_offset + 1]
            node.modified = True
            splitter.modified = True
            self._delete_node(db_offset)
        else:
            # Merge left sibling into nodepath[-1]
            keys = node.node[_Node.KEYS]
            splitter, split_offset = self._splitter_node_for_key(
                nodepath, keys[0]
            )
            if deletion_point == 0:
                if node.node[_Node.LEFT_SIBLING_NODE_NUMBER] is not None:
                    self._replace_splitter_in_branch_or_root(nodepath)
            del keys[deletion_point]
            node.node[_Node.KEYS] = left_sibling.node[_Node.KEYS] + keys
            lslsnn = left_sibling.node[_Node.LEFT_SIBLING_NODE_NUMBER]
            node.node[_Node.LEFT_SIBLING_NODE_NUMBER] = lslsnn
            if lslsnn is not None:
                lslsnode = _Node(
                    *literal_eval(self.read_node(lslsnn).decode())
                )
                lslsnode.node[_Node.RIGHT_SIBLING_NODE_NUMBER] = node.node[
                    _Node.NODE_NUMBER
                ]
                lslsnode.modified = True
                self._write_modified_nodes([lslsnode])
            del splitter.node[_Node.KEYS][split_offset]
            # In case parent is also the splitter, which is likely.
            db_offset = entries[offset - 1]
            del splitter.node[_Node.ENTRIES][split_offset]
            node.modified = True
            splitter.modified = True
            self._delete_node(db_offset)
        if parent.node[_Node.NODE_TYPE] == _Node.ROOT:
            # Complete merge actions here if no branch nodes involved.
            if len(parent) == 0:
                node.node[_Node.NODE_TYPE] = _Node.SOLO_ROOT
                self._delete_node(node.node[_Node.NODE_NUMBER])
                self._write_root(node.node)
                del nodepath[-2]
            self._write_modified_nodes(nodepath)
            return None
        self._write_modified_nodes(nodepath)
        return True  # The leaf may need one or more branch merges to fit.

    def _merge_branch(self, nodepath):
        node = nodepath[-1]
        parent = nodepath[-2]
        entries = parent.node[_Node.ENTRIES]
        offset = entries.index(node.node[_Node.NODE_NUMBER])
        if offset < len(entries) - 1:
            # Take entry from right sibling if possible.
            right_sibling = _Node(
                *literal_eval(self.read_node(entries[offset + 1]).decode())
            )
            if len(right_sibling) + 1 > self.branching_factor // 2:
                node.node[_Node.KEYS].append(parent.node[_Node.KEYS][offset])
                parent.node[_Node.KEYS][offset] = right_sibling.node[
                    _Node.KEYS
                ].pop(0)
                node.node[_Node.ENTRIES].append(
                    right_sibling.node[_Node.ENTRIES].pop(0)
                )
                node.modified = True
                parent.modified = True
                right_sibling.modified = True
                self._write_modified_nodes([right_sibling])
                self._write_modified_nodes(nodepath)
                return None
        if offset > 0:
            # Take entry from left sibling if possible.
            left_sibling = _Node(
                *literal_eval(self.read_node(entries[offset - 1]).decode())
            )
            if len(left_sibling) + 1 > self.branching_factor // 2:
                node.node[_Node.KEYS].insert(
                    0, parent.node[_Node.KEYS][offset - 1]
                )
                parent.node[_Node.KEYS][offset - 1] = left_sibling.node[
                    _Node.KEYS
                ].pop()
                node.node[_Node.ENTRIES].insert(
                    0, left_sibling.node[_Node.ENTRIES].pop()
                )
                node.modified = True
                parent.modified = True
                left_sibling.modified = True
                self._write_modified_nodes([left_sibling])
                self._write_modified_nodes(nodepath)
                return None
        if offset < len(entries) - 1:
            # Merge right sibling into nodepath[-1].
            node.node[_Node.KEYS].append(parent.node[_Node.KEYS].pop(offset))
            del entries[entries.index(right_sibling.node[_Node.NODE_NUMBER])]
            node.node[_Node.ENTRIES].extend(right_sibling.node[_Node.ENTRIES])
            node.node[_Node.KEYS].extend(right_sibling.node[_Node.KEYS])
            node.modified = True
            parent.modified = True
            right_sibling.modified = None
            self._write_modified_nodes([right_sibling])
            self._write_modified_nodes(nodepath)
        else:
            # Merge left sibling into nodepath[-1].
            node.node[_Node.KEYS].insert(
                0, parent.node[_Node.KEYS].pop(offset - 1)
            )
            del entries[entries.index(left_sibling.node[_Node.NODE_NUMBER])]
            node.node[_Node.ENTRIES] = (
                left_sibling.node[_Node.ENTRIES] + node.node[_Node.ENTRIES]
            )
            node.node[_Node.KEYS] = (
                left_sibling.node[_Node.KEYS] + node.node[_Node.KEYS]
            )
            node.modified = True
            parent.modified = True
            left_sibling.modified = None
            self._write_modified_nodes([left_sibling])
            self._write_modified_nodes(nodepath)
        if parent.node[_Node.NODE_TYPE] == _Node.ROOT:
            # Complete merge actions here.
            if len(parent) == 0:
                # This turns the single child branch node into the root node.
                node.modified = None
                parent.modified = None
                self._write_modified_nodes(nodepath)
                node.node[_Node.NODE_TYPE] = parent.node[_Node.NODE_TYPE]
                node.modified = True
                self._write_modified_nodes(nodepath)
                return None
            # Keep branch under root.
            return None
        # Return value must indicate continue 'merge branch loop'.
        return True


class Cursor:
    """Define a cursor associated with a (file, field) in a _nosql database."""

    def __init__(self, tree):
        """Create a cursor on tree with an undefined position."""
        self.tree = tree
        self.current_key_node_number = None
        self.current_key = None
        self._partial = None

    @property
    def partial(self):
        """Set partial, defining range of values for cursor operations."""
        return self._partial

    @partial.setter
    def partial(self, value):
        self._partial = value

    def close(self):
        """Close cursor."""
        self.tree = None
        self.current_key_node_number = None
        self.current_key = None
        self._partial = None

    def first(self):
        """Move cursor to first record."""
        node = literal_eval(self.tree.read_root().decode())
        if node is None:
            return None
        while node[_Node.NODE_TYPE] not in _Node.LEAF_NODES:
            node = literal_eval(
                self.tree.read_node(node[_Node.ENTRIES][0]).decode()
            )
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        self.current_key = node[_Node.KEYS][0]
        return self.current_key

    def get_position_of_key(self, key=None):
        """Not implemented."""
        raise TreeError("Cursor.get_position_of_key() not implemented yet")

    def get_key_at_position(self, position=None):
        """Not implemented."""
        raise TreeError("Cursor.get_key_at_position() not implemented yet")

    def last(self):
        """Move cursor to last record."""
        node = literal_eval(self.tree.read_root().decode())
        if node is None:
            return None
        while node[_Node.NODE_TYPE] not in _Node.LEAF_NODES:
            node = literal_eval(
                self.tree.read_node(node[_Node.ENTRIES][-1]).decode()
            )
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        self.current_key = node[_Node.KEYS][-1]
        return self.current_key

    def nearest(self, key):
        """Move cursor to first record for value not less than key."""
        try:
            node = self.tree.search(key)[-1].node
        except TypeError:
            self.current_key = None
            self.current_key_node_number = None
            return self.current_key
        i = bisect_left(node[_Node.KEYS], key)
        if i == len(node[_Node.KEYS]):
            if node[_Node.RIGHT_SIBLING_NODE_NUMBER] is None:
                return None
            node = literal_eval(
                self.tree.read_node(
                    node[_Node.RIGHT_SIBLING_NODE_NUMBER]
                ).decode()
            )
            self.current_key_node_number = node[_Node.NODE_NUMBER]
            self.current_key = node[_Node.KEYS][0]
            return self.current_key
        self.current_key = node[_Node.KEYS][i]
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        return self.current_key

    def next(self):
        """Move cursor to next record, or first record if never moved."""
        if self.current_key is None:
            return self.first()
        try:
            node = literal_eval(
                self.tree.read_node(self.current_key_node_number).decode()
            )
        except KeyError:
            try:
                node = self.tree.search(self.current_key)[-1].node
            except TypeError:
                return None
        i = bisect_left(node[_Node.KEYS], self.current_key)
        if node[_Node.KEYS][i] == self.current_key:
            if i == len(node[_Node.KEYS]) - 1:
                if node[_Node.RIGHT_SIBLING_NODE_NUMBER] is None:
                    return None
                node = literal_eval(
                    self.tree.read_node(
                        node[_Node.RIGHT_SIBLING_NODE_NUMBER]
                    ).decode()
                )
                self.current_key_node_number = node[_Node.NODE_NUMBER]
                self.current_key = node[_Node.KEYS][0]
                return self.current_key
            self.current_key_node_number = node[_Node.NODE_NUMBER]
            self.current_key = node[_Node.KEYS][i + 1]
            return self.current_key
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        self.current_key = node[_Node.KEYS][i]
        return self.current_key

    def prev(self):
        """Move cursor to previous record, or last record if never moved."""
        if self.current_key is None:
            return self.last()
        try:
            node = literal_eval(
                self.tree.read_node(self.current_key_node_number).decode()
            )
        except KeyError:
            try:
                node = self.tree.search(self.current_key)[-1].node
            except TypeError:
                return None
        i = bisect_left(node[_Node.KEYS], self.current_key)
        if i == 0:
            if node[_Node.LEFT_SIBLING_NODE_NUMBER] is None:
                return None
            node = literal_eval(
                self.tree.read_node(
                    node[_Node.LEFT_SIBLING_NODE_NUMBER]
                ).decode()
            )
            self.current_key_node_number = node[_Node.NODE_NUMBER]
            self.current_key = node[_Node.KEYS][-1]
            return self.current_key
        if (
            i == len(node[_Node.KEYS])
            or node[_Node.KEYS][i] == self.current_key
        ):
            self.current_key_node_number = node[_Node.NODE_NUMBER]
            self.current_key = node[_Node.KEYS][i - 1]
            return self.current_key
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        self.current_key = node[_Node.KEYS][i - 1]
        return self.current_key

    def setat(self, key):
        """Move cursor to first record for value of key."""
        try:
            node = self.tree.search(key)[-1].node
        except TypeError:
            return None
        i = bisect_left(node[_Node.KEYS], key)
        if i == len(node[_Node.KEYS]):
            return None
        if key != node[_Node.KEYS][i]:
            return None
        self.current_key = node[_Node.KEYS][i]
        self.current_key_node_number = node[_Node.NODE_NUMBER]
        return self.current_key


class _Node:

    # The valid values of node[NODE_TYPE]
    ROOT = 1
    SOLO_ROOT = 2
    BRANCH = 3
    LEAF = 4

    ROOT_NODES = {ROOT, SOLO_ROOT}
    LEAF_NODES = {SOLO_ROOT, LEAF}

    # Index numbers into node[]
    # LEFT_SIBLING_NODE_NUMBER and RIGHT_SIBLING_NODE_NUMBER might not be used
    # in non-LEAF node types.
    # A new node is given node number <root node>[HIGH_USED_NODE_NUMBER] + 1.
    NODE_NUMBER = 0
    NODE_TYPE = 1
    LEFT_SIBLING_NODE_NUMBER = 2
    RIGHT_SIBLING_NODE_NUMBER = 3
    KEYS = 4
    ENTRIES = 5

    # Often it is easier to access the list read from database using the index
    # constants defined in _Node class, without creating a _Node object.
    # However the 'node' slot may be more convenient if modifications are done
    # but not immediately written to database.  Then the 'modified' slot says
    # if the _Node object's 'node' slot has to be written to database.
    __slots__ = "node", "modified"

    def __init__(
        self, number, type_, left=None, right=None, keys=None, entries=None
    ):
        self.node = [number, type_, left, right, keys, entries]
        self.modified = False

    def __len__(self):
        # A node has up to b entries and b-1 keys, where b is the branching
        # factor of the tree.  Root and branch nodes have one more entry than
        # keys, while solo root and leaf nodes have the same number of entries
        # and keys.  The number of keys in a full node is the same for all
        # types of node, hence the definition of __len__().
        return len(self.node[_Node.KEYS])

    def insert_into_branch_or_root(self, key, node_number):
        keys = self.node[_Node.KEYS]
        insertion_point = bisect_left(keys, key)
        assert insertion_point == len(keys) or key != keys[insertion_point]
        keys.insert(insertion_point, key)
        self.node[_Node.ENTRIES].insert(insertion_point + 1, node_number)
        return insertion_point
