import threading

class ThreadSafeIndex:
    """
    Thread-safe wrapper for Index operations.
    Protects index operations with locks to prevent race conditions.
    """
    def __init__(self, index):
        self.index = index
        self.lock = threading.RLock()  # Reentrant lock for nested calls
    
    def create_index(self, column_number):
        """Create an index on a column."""
        with self.lock:
            return self.index.create_index(column_number)
    
    def locate(self, column_number, value):
        """Locate records with a specific value in a column."""
        with self.lock:
            return self.index.locate(column_number, value)
    
    def locate_range(self, begin, end, column_number):
        """Locate records within a range."""
        with self.lock:
            return self.index.locate_range(begin, end, column_number)
    
    def drop_index(self, column_number):
        """Drop an index on a column."""
        with self.lock:
            return self.index.drop_index(column_number)
    
    def insert(self, column_number, value, rid):
        """Insert a (value, rid) pair into the column's index."""
        with self.lock:
            return self.index.insert(column_number, value, rid)

class ThreadSafeBufferpool:
    """
    Thread-safe wrapper for Bufferpool operations.
    Protects buffer pool with fine-grained locking.
    """
    def __init__(self, bufferpool):
        self.bufferpool = bufferpool
        # Use separate locks for different operations to reduce contention
        self.page_locks = {}  # One lock per page
        self.pool_lock = threading.Lock()  # For pool-level operations
    
    def _get_page_lock(self, page_id):
        """Get or create a lock for a specific page."""
        with self.pool_lock:
            if page_id not in self.page_locks:
                self.page_locks[page_id] = threading.RLock()
            return self.page_locks[page_id]
    
    def get_page(self, page_id):
        """Get a page from the buffer pool."""
        page_lock = self._get_page_lock(page_id)
        with page_lock:
            return self.bufferpool.get_page(page_id)
    
    def pin_page(self, page_id):
        """Pin a page in memory."""
        page_lock = self._get_page_lock(page_id)
        with page_lock:
            return self.bufferpool.pin_page(page_id)
    
    def unpin_page(self, page_id):
        """Unpin a page."""
        page_lock = self._get_page_lock(page_id)
        with page_lock:
            return self.bufferpool.unpin_page(page_id)
    
    def flush_page(self, page_id):
        """Flush a page to disk."""
        page_lock = self._get_page_lock(page_id)
        with page_lock:
            return self.bufferpool.flush_page(page_id)
    
    def evict_page(self):
        """Evict a page from the buffer pool."""
        # This is a pool-level operation that may affect multiple pages
        with self.pool_lock:
            return self.bufferpool.evict_page()

class ThreadSafeTable:
    """
    Thread-safe wrapper for Table operations.
    Coordinates with lock manager for record-level locking.
    """
    def __init__(self, table):
        self.table = table
        self.metadata_lock = threading.RLock()  # For table metadata
        # Note: Record-level locking is handled by LockManager
    
    def get_num_columns(self):
        """Thread-safe access to table metadata."""
        with self.metadata_lock:
            return self.table.num_columns
    
    def get_key_column(self):
        """Thread-safe access to key column."""
        with self.metadata_lock:
            return self.table.key
    
    def get_name(self):
        """Thread-safe access to table name."""
        with self.metadata_lock:
            return self.table.name

# Alternative: Lightweight locking approach
class PageLatch:
    """
    Lightweight latch for short-term page protection.
    Use this for buffer pool pages instead of heavy locks.
    """
    def __init__(self):
        self.latch = threading.Lock()
        self.readers = 0
        self.writer = False
        self.cv = threading.Condition(self.latch)
    
    def acquire_read(self):
        """Acquire read latch (multiple readers allowed)."""
        with self.cv:
            while self.writer:
                self.cv.wait()
            self.readers += 1
    
    def release_read(self):
        """Release read latch."""
        with self.cv:
            self.readers -= 1
            if self.readers == 0:
                self.cv.notify_all()
    
    def acquire_write(self):
        """Acquire write latch (exclusive)."""
        with self.cv:
            while self.readers > 0 or self.writer:
                self.cv.wait()
            self.writer = True
    
    def release_write(self):
        """Release write latch."""
        with self.cv:
            self.writer = False
            self.cv.notify_all()

# Context managers for cleaner code
class ReadLatch:
    """Context manager for read latches."""
    def __init__(self, latch):
        self.latch = latch
    
    def __enter__(self):
        self.latch.acquire_read()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.latch.release_read()
        return False

class WriteLatch:
    """Context manager for write latches."""
    def __init__(self, latch):
        self.latch = latch
    
    def __enter__(self):
        self.latch.acquire_write()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.latch.release_write()
        return False