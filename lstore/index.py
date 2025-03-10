import bisect
from collections import deque

class Index:
    def __init__(self, table):
        self.table_name = table.name
        self.num_columns = table.num_columns
        self.indices = [None] * self.num_columns
        # Use deque for faster append/pop operations
        self.insert_cache = {col: deque() for col in range(self.num_columns)}
        self.max_keys = [None] * self.num_columns
        self.insert_cache_size = 50000
        # Increase threshold to reduce sorting frequency
        self.unsorted_cache = {col: deque() for col in range(self.num_columns)} 
        self.unsorted_threshold = 2000
        self.primary_key_cache = {}
        self.sorted_records = []
        for col in range(self.num_columns):
            self.create_index(col)


    """
    Create the index
    """
    def create_index(self, column_number):
        if column_number < 0 or column_number >= self.num_columns:
            return False
        self.indices[column_number] = BPlusTree()


    def refresh_indexes(self, table):
        """
        Refresh all indexes based on the current state of the table.
        """
        # Clear existing indexes
        self.indices = [None] * self.num_columns
        self.primary_key_cache = {}
        self.sorted_records = []

        # Recreate indexes
        for col in range(self.num_columns):
            self.create_index(col)

        # Re-index all records in the table
        for _, locations in table.page_directory.items():
            base_path, base_offset = locations[0]
            print("base path: " + base_path)
            base_record = table.bufferpool.get_page(base_path).read_index(base_offset)
            table.bufferpool.unpin_page(base_path)
            self.add_record(base_record)


    """
    Flush the cache to the index
    """
    def flush_cache(self):
        for col in range(self.num_columns):
            self._flush_cache_for_column(col)


    """
    Add a record to the index more efficiently
    """
    def add_record(self, record):
        rid_to_add = record.rid
        encoded_rid = rid_to_add.encode('utf-8')
        # For primary key (column 0), update primary key cache and sorted list
        primary_key = record.columns[0]
        if primary_key is not None:
            self.primary_key_cache[primary_key] = encoded_rid
            # Insert into sorted_records using bisect for O(log n) insertion
            bisect.insort(self.sorted_records, (primary_key, encoded_rid))

        for col, key in enumerate(record.columns):
            if key is None:
                continue
            # Instead of sorting per insert, simply append to unsorted cache
            self.unsorted_cache[col].append((key, encoded_rid))
            # Remove per-insert threshold check: we now defer sorting to flush_cache
            if len(self.insert_cache[col]) >= self.insert_cache_size:
                self._flush_cache_for_column(col)


    """
    Efficiently merge two sorted lists
    """
    def _merge_sorted_lists(self, list1, list2):
        result = []
        i, j = 0, 0
        len1, len2 = len(list1), len(list2)
        
        # Merge in O(n+m) time
        while i < len1 and j < len2:
            if list1[i][0] <= list2[j][0]:
                result.append(list1[i])
                i += 1
            else:
                result.append(list2[j])
                j += 1
                
        # Add remaining elements
        result.extend(list1[i:])
        result.extend(list2[j:])
        return result


    """
    Flush cache for a column with improved batch handling
    """
    def _flush_cache_for_column(self, col):
        # In flush, if unsorted_cache exists, sort it once
        if self.unsorted_cache[col]:
            sorted_unsorted = sorted(self.unsorted_cache[col], key=lambda x: x[0])
            # Merge with any existing sorted insert_cache
            if self.insert_cache[col]:
                cache = self._merge_sorted_lists(self.insert_cache[col], sorted_unsorted)
            else:
                cache = sorted_unsorted
            self.insert_cache[col] = cache
            self.unsorted_cache[col] = []
            
        if not self.insert_cache[col]:
            return
            
        try:
            batch_size = 5000
            for i in range(0, len(self.insert_cache[col]), batch_size):
                batch = self.insert_cache[col][i:i+batch_size]
                try:
                    self.indices[col].batch_insert(batch)
                except ValueError:
                    for (k, v) in batch:
                        self.indices[col][k] = v
        except Exception as e:
            print(f"Error in batch insert: {e}, falling back to individual inserts")
            for (k, v) in self.insert_cache[col]:
                self.indices[col][k] = v
                
        if self.insert_cache[col]:
            if self.max_keys[col] is None or self.insert_cache[col][-1][0] > self.max_keys[col]:
                self.max_keys[col] = self.insert_cache[col][-1][0]
        self.insert_cache[col] = []


    """
    Locate a record in the index
    """
    def locate(self, column, value):
        # For primary key lookups, use cache
        if column == 0 and value in self.primary_key_cache:
            return self.primary_key_cache[value].decode('utf-8')
        # Instead of flushing all columns, flush only the target column
        self._flush_cache_for_column(column)
        if value is None:
            return False
        try:
            val = self.indices[column][value]
            if val is not None:
                return val.decode('utf-8')
        except KeyError:
            return False
        return False


    """
    Locate a range of records in the index
    """
    def locate_range(self, begin, end, column):
        """Optimized range query implementation"""
        # Fast path for primary key column using sorted_records
        if column == 0:
            # Pre-allocate result with appropriate size estimate
            result = {}
            left = bisect.bisect_left(self.sorted_records, (begin, b""))
            right = bisect.bisect_right(self.sorted_records, (end, b"\xff"))
            
            # Use dictionary comprehension for better performance
            result = {key: rid.decode('utf-8') for key, rid in self.sorted_records[left:right]}
            return result if result else False
        
        # For other columns
        self._flush_cache_for_column(column)
        
        # Start at the first leaf containing values >= begin
        node = self.indices[column].search(begin)
        if not node:
            return False
        
        result = {}
        # Traverse leaf nodes without repeated searches
        while node:
            for i, k in enumerate(node.keys):
                if k < begin:
                    continue
                if end is not None and k > end:
                    return result if result else False
                result[k] = node.children[i].decode('utf-8')
            
            # Move to next leaf
            node = node.next
        
        return result if result else False
    
    def exists(self, column, value):
        """Fast existence check with early returns"""
        # Check primary key cache first (fastest path)
        if column == 0 and value in self.primary_key_cache:
            return True
        
        # Check unsorted and insert caches
        for cache_key, _ in self.unsorted_cache[column]:
            if cache_key == value:
                return True
                
        for cache_key, _ in self.insert_cache[column]:
            if cache_key == value:
                return True
                
        # Only flush if necessary
        if not self.unsorted_cache[column] and not self.insert_cache[column]:
            # Direct B+ tree check without flushing caches
            try:
                return self.indices[column].has_key(value)
            except Exception:
                return False
        
        # Full check with cache flush
        self._flush_cache_for_column(column)
        return self.indices[column].has_key(value)
    
    def __getstate__(self):
        """
        Control what gets pickled - exclude the table reference
        """
        state = self.__dict__.copy()
        # Remove the table reference as it contains unpickleable locks
        if 'table' in state:
            state['table'] = None
        return state

    def __setstate__(self, state):
        """
        Control what happens during unpickling
        """
        self.__dict__.update(state)


"""
B+ Tree implementation from scratch
"""
class BPlusTree:
    def __init__(self, order=75):
        self.order = order
        self.max_keys = order - 1
        self.root = BPlusTreeNode(is_leaf=True)
        self._size = 0

    def search(self, key):
        node = self.root
        while not node.is_leaf:
            i = bisect.bisect_right(node.keys, key)
            node = node.children[i]
        return node

    def __getitem__(self, key):
        if isinstance(key, slice):
            start, stop = key.start, key.stop
            result = {}
            node = self.search(start)
            while node:
                for k, v in zip(node.keys, node.children):
                    if k < start:
                        continue
                    if stop is not None and k >= stop:
                        return result
                    result[k] = v
                node = node.next
            return result
        else:
            leaf = self.search(key)
            i = bisect.bisect_left(leaf.keys, key)
            if i < len(leaf.keys) and leaf.keys[i] == key:
                return leaf.children[i]
            raise KeyError(f"Key {key} not found")

    def __setitem__(self, key, value):
        root = self.root
        if len(root.keys) == self.max_keys:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.children.append(root)
            self.split_child(new_root, 0)
            self.root = new_root
        self.insert_non_full(self.root, key, value)
        self._size += 1  

    def insert_non_full(self, node, key, value):
        if node.is_leaf:
            i = bisect.bisect_left(node.keys, key)
            node.keys.insert(i, key)
            node.children.insert(i, value)
        else:
            i = bisect.bisect_right(node.keys, key)
            child = node.children[i]
            if len(child.keys) == self.max_keys:
                self.split_child(node, i)
                if key >= node.keys[i]:
                    i += 1
            self.insert_non_full(node.children[i], key, value)

    def split_child(self, parent, index):
        node = parent.children[index]
        new_node = BPlusTreeNode(is_leaf=node.is_leaf)
        mid = len(node.keys) // 2
        if node.is_leaf:
            new_node.keys = node.keys[mid:]
            new_node.children = node.children[mid:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid]
            new_node.next = node.next
            node.next = new_node
            split_key = new_node.keys[0]
        else:
            split_key = node.keys[mid]
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
        parent.keys.insert(index, split_key)
        parent.children.insert(index + 1, new_node)

    def batch_insert(self, pairs):
        if len(self) > 0:
            current_max = self.max_key()
            if pairs[0][0] <= current_max:
                raise ValueError("Keys to batch insert must be sorted and greater than existing keys")
        for key, value in pairs:
            self.__setitem__(key, value)

    def max_key(self):
        node = self.root
        while not node.is_leaf:
            node = node.children[-1]
        return node.keys[-1] if node.keys else None

    def __len__(self):
        return self._size

    # Add a method to get all key-value pairs
    def items(self):
        __slots__ = ('is_leaf', 'keys', 'children', 'next')
        """Get all key-value pairs in the tree"""
        result = []
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
            
        while node:
            for k, v in zip(node.keys, node.children):
                result.append((k, v))
            node = node.next
        return result
    
    def has_key(self, key):
        """
        Check if key exists in the tree without retrieving value
        Much faster than __getitem__ for existence checks
        """
        try:
            leaf = self.search(key)
            i = bisect.bisect_left(leaf.keys, key)
            return i < len(leaf.keys) and leaf.keys[i] == key
        except:
            return False


class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = None
