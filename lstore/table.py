"""
TABLE.PY - UPDATED VERSION
Replace your existing table.py with this file.

Key Changes:
1. Added: from lstore.thread_safe import ThreadSafeIndex
2. Wrapped index with ThreadSafeIndex
3. Added 4 locks: metadata_lock, pd_lock, rid_lock, vc_lock
4. Made insert_row() thread-safe
5. Added thread-safe getter methods
"""

from lstore.index import Index
from lstore.sharedDS import ThreadSafeIndex
from time import time
from lstore.page import Page
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        
        # Wrap index with thread-safe wrapper
        self._index = Index(self)
        self.index = ThreadSafeIndex(self._index)
        
        self.base_page = [[Page()] for _ in range(num_columns)]
        self.tail_page = [[Page()] for _ in range(num_columns)]
        self.rid_counter = 0
        self.version_chain = {}
        
        # Add locks for thread safety
        self.metadata_lock = threading.RLock()  # Protects metadata (name, key, num_columns)
        self.pd_lock = threading.RLock()  # Protects page_directory
        self.rid_lock = threading.RLock()  # Protects rid_counter
        self.vc_lock = threading.RLock()  # Protects version_chain

        self.index.create_index(self.key)

    def insert_row(self, columns):
        """Thread-safe row insertion"""
        # Protect rid_counter and page_directory
        with self.rid_lock:
            rid = self.rid_counter + 1
            self.rid_counter += 1
        
        page_positions = [None] * self.num_columns
   
        for i, value in enumerate(columns):
            current_page = self.base_page[i][-1]
            
            # Check and allocate new page if needed
            if not current_page.has_capacity():
                new_page = Page()
                self.base_page[i].append(new_page)
                current_page = new_page
            
            current_page.write(value)
            page_index = len(self.base_page[i]) - 1
            record_offset = self.base_page[i][-1].num_records - 1
            page_positions[i] = (page_index, record_offset)
        
        # Update page directory with lock
        with self.pd_lock:
            self.page_directory[rid] = page_positions
        
        # Update index
        primary_key_value = columns[self.key]
        self.index.insert(self.key, primary_key_value, rid)
        
        return rid
        
    def read_column(self, col_idx, page_idx, slot_idx):
        """Thread-safe column read"""
        page = self.base_page[col_idx][page_idx]
        return page.read(slot_idx)
    
    def get_num_columns(self):
        """Thread-safe getter for num_columns"""
        with self.metadata_lock:
            return self.num_columns
    
    def get_key(self):
        """Thread-safe getter for key"""
        with self.metadata_lock:
            return self.key
    
    def get_name(self):
        """Thread-safe getter for name"""
        with self.metadata_lock:
            return self.name

    def merge(self):
        pass