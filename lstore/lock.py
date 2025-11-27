import threading
from enum import Enum

class LockType(Enum):
    SHARED = 1      # Read lock (multiple transactions can hold)
    EXCLUSIVE = 2   # Write lock (only one transaction can hold)

class Lock:
    """Represents a lock on a single record."""
    def __init__(self):
        self.lock = threading.Lock()  # Protects the lock state itself
        self.shared_holders = set()   # Set of transaction IDs holding shared locks
        self.exclusive_holder = None  # Transaction ID holding exclusive lock
    
    def can_grant_shared(self, transaction_id):
        """Check if a shared lock can be granted."""
        # Can grant if: no exclusive holder OR we already hold exclusive
        return self.exclusive_holder is None or self.exclusive_holder == transaction_id
    
    def can_grant_exclusive(self, transaction_id):
        """Check if an exclusive lock can be granted."""
        # Can grant if: no locks held OR only we hold a shared lock OR we already hold exclusive
        if self.exclusive_holder == transaction_id:
            return True
        if self.exclusive_holder is not None:
            return False
        # Check shared locks
        if len(self.shared_holders) == 0:
            return True
        if len(self.shared_holders) == 1 and transaction_id in self.shared_holders:
            return True  # Lock upgrade
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
                # Remove from shared holders if upgrading
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
        self.locks = {}  # Maps record_id -> Lock
        self.manager_lock = threading.Lock()  # Protects the locks dictionary
        self.transaction_locks = {}  # Maps transaction_id -> list of (record_id, lock_type)
    
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
            # Track this lock for the transaction
            with self.manager_lock:
                if transaction_id not in self.transaction_locks:
                    self.transaction_locks[transaction_id] = []
                self.transaction_locks[transaction_id].append((record_id, LockType.SHARED))
        
        return success
    
    def acquire_exclusive(self, transaction_id, record_id):
        """
        Try to acquire an exclusive (write) lock.
        Returns True if successful, False if lock cannot be granted (transaction should abort).
        """
        lock = self._get_lock(record_id)
        success = lock.acquire_exclusive(transaction_id)
        
        if success:
            # Track this lock for the transaction
            with self.manager_lock:
                if transaction_id not in self.transaction_locks:
                    self.transaction_locks[transaction_id] = []
                # Remove any existing shared lock entry for this record
                if transaction_id in self.transaction_locks:
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
            
            # Release each lock
            for record_id, lock_type in self.transaction_locks[transaction_id]:
                if record_id in self.locks:
                    self.locks[record_id].release(transaction_id)
            
            # Clear transaction's lock tracking
            del self.transaction_locks[transaction_id]


# Global lock manager instance
_lock_manager = None

def get_lock_manager():
    """Get the global lock manager instance."""
    global _lock_manager
    if _lock_manager is None:
        _lock_manager = LockManager()
    return _lock_manager