import threading
from enum import Enum

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2

class Lock:
    """Represents a lock on a single record."""
    def __init__(self):
        self.lock = threading.Lock()
        self.shared_holders = set()
        self.exclusive_holder = None
    
    def can_grant_shared(self, transaction_id):
        """Check if a shared lock can be granted."""
        return self.exclusive_holder is None or self.exclusive_holder == transaction_id
    
    def can_grant_exclusive(self, transaction_id):
        """Check if an exclusive lock can be granted."""
        if self.exclusive_holder == transaction_id:
            return True
        if self.exclusive_holder is not None:
            return False
        if len(self.shared_holders) == 0:
            return True
        if len(self.shared_holders) == 1 and transaction_id in self.shared_holders:
            return True
        return False
    
    def acquire_shared(self, transaction_id):
        """Try to acquire a shared lock. Returns True if successful."""
        with self.lock:
            if self.can_grant_shared(transaction_id):
                self.shared_holders.add(transaction_id)
                return True
            return False
    
    def acquire_exclusive(self, transaction_id):
        """Try to acquire an exclusive lock. Returns True if successful."""
        with self.lock:
            if self.can_grant_exclusive(transaction_id):
                self.shared_holders.discard(transaction_id)
                self.exclusive_holder = transaction_id
                return True
            return False
    
    def release(self, transaction_id):
        """Release all locks held by this transaction."""
        with self.lock:
            self.shared_holders.discard(transaction_id)
            if self.exclusive_holder == transaction_id:
                self.exclusive_holder = None

class LockManager:
    """
    Manages locks for all records in the database.
    Implements Strict 2PL with no-wait policy.
    """
    def __init__(self):
        self.locks = {}
        self.manager_lock = threading.Lock()
        self.transaction_locks = {}
    
    def _get_lock(self, record_id):
        """Get or create a lock for a record."""
        with self.manager_lock:
            if record_id not in self.locks:
                self.locks[record_id] = Lock()
            return self.locks[record_id]
    
    def acquire_shared(self, transaction_id, record_id):
        """
        Try to acquire a shared (read) lock.
        Returns True if successful, False if lock cannot be granted (transaction should abort).
        """
        lock = self._get_lock(record_id)
        success = lock.acquire_shared(transaction_id)
        
        if success:
            with self.manager_lock:
                if transaction_id not in self.transaction_locks:
                    self.transaction_locks[transaction_id] = []
                lock_entry = (record_id, LockType.SHARED)
                if lock_entry not in self.transaction_locks[transaction_id]:
                    self.transaction_locks[transaction_id].append(lock_entry)
        
        return success
    
    def acquire_exclusive(self, transaction_id, record_id):
        """
        Try to acquire an exclusive (write) lock.
        Returns True if successful, False if lock cannot be granted (transaction should abort).
        """
        lock = self._get_lock(record_id)
        success = lock.acquire_exclusive(transaction_id)
        
        if success:
            with self.manager_lock:
                if transaction_id not in self.transaction_locks:
                    self.transaction_locks[transaction_id] = []
                self.transaction_locks[transaction_id] = [
                    (rid, lt) for rid, lt in self.transaction_locks[transaction_id]
                    if rid != record_id
                ]
                self.transaction_locks[transaction_id].append((record_id, LockType.EXCLUSIVE))
        
        return success
    
    def release_all(self, transaction_id):
        """
        Release all locks held by a transaction.
        Called when transaction commits or aborts.
        """
        with self.manager_lock:
            if transaction_id not in self.transaction_locks:
                return

            for record_id, lock_type in self.transaction_locks[transaction_id]:
                if record_id in self.locks:
                    self.locks[record_id].release(transaction_id)
            
            del self.transaction_locks[transaction_id]

_lock_manager = None
_lock_manager_lock = threading.Lock()

def get_lock_manager():
    """Get the global lock manager instance (thread-safe singleton)."""
    global _lock_manager
    if _lock_manager is None:
        with _lock_manager_lock:
            if _lock_manager is None:
                _lock_manager = LockManager()
    return _lock_manager