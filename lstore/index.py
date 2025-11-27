import math

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] * table.num_columns

    def locate(self, column, value):
        """
        Returns the RID of the first record with the given value on column "column".
        Returns None if not found.
        """
        if self.indices[column] is None:
            return None
        
        tree = self.indices[column]
        result = tree.locate(value)
        return result[0] if result else None

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end".
        """
        if self.indices[column] is None:
            return []
        
        tree = self.indices[column]
        return tree.locate_range(begin, end)

    def create_index(self, column_number):
        """Create index on specific column"""
        if self.indices[column_number] is None:
            self.indices[column_number] = BPlusTree(order=4)
            # page_directory maps: rid -> list of (page_idx, slot_idx) for each column
            for rid, positions in self.table.page_directory.items():
                # Get the position for this specific column
                page_idx, slot_idx = positions[column_number]
                value = self.table.read_column(column_number, page_idx, slot_idx)
                self.indices[column_number].insert(value, rid)
                
    def insert(self, column_number, value, rid):
        """Insert a (value, rid) pair into the column's index (if it exists)."""
        if self.indices[column_number] is not None:
            tree = self.indices[column_number]
            tree.insert(value, rid)

    def drop_index(self, column_number):
        """Drop index of specific column"""
        self.indices[column_number] = None


class BPlusTree:
    """B+ Tree implementation for indexing."""
    def __init__(self, order=4):
        self.root = Node(order)
        self.order = order

    def insert(self, key, rid):
        """Insert a key-rid pair into the tree."""
        root = self.root
        
        if len(root.keys) == self.order - 1:
            new_root = Node(self.order)
            new_root.is_leaf = False
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root

        self.insert_non_full(self.root, key, rid)

    def insert_non_full(self, node, key, rid):
        """Insert into a node that is not full."""
        if node.is_leaf:
            idx = 0
            while idx < len(node.keys) and node.keys[idx][0] < key:
                idx += 1
            node.keys.insert(idx, (key, rid))
        else:
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            
            if len(node.children[i].keys) == self.order - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1

            self.insert_non_full(node.children[i], key, rid)
        
    def _split_child(self, parent, index):
        """Split a full child node."""
        node = parent.children[index]
        mid = self.order // 2
        new_node = Node(self.order)
        new_node.is_leaf = node.is_leaf

        split_key = node.keys[mid][0] if node.is_leaf else node.keys[mid]
        parent.keys.insert(index, split_key)
        new_node.keys = node.keys[mid:] if node.is_leaf else node.keys[mid + 1:]
        node.keys = node.keys[:mid]

        if not node.is_leaf:
            new_node.children = node.children[mid + 1:]
            node.children = node.children[:mid + 1]
        else:
            new_node.next = node.next
            node.next = new_node

        parent.children.insert(index + 1, new_node)
        
    def locate(self, key):
        """Find all RIDs with the given key."""
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]

        return [rid for k, rid in node.keys if k == key]
    
    def locate_range(self, start, end):
        """Find all RIDs with keys in the range [start, end]."""
        node = self.root
        
        # Navigate to the leftmost leaf that could contain start
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and start > node.keys[i]:
                i += 1
            node = node.children[i]

        results = []
        # Traverse leaf nodes to collect all matching keys
        while node:
            for k, rid in node.keys:
                if start <= k <= end:
                    results.append(rid)
                elif k > end:
                    return results
            node = node.next
        return results


class Node:
    """Node in a B+ Tree."""
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.children = []
        self.is_leaf = True
        self.next = None  # For leaf nodes to form a linked list