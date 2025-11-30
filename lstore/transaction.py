from lstore.table import Table, Record
from lstore.index import Index
import threading

_transaction_id_lock = threading.Lock()
_next_transaction_id = 0

def get_next_transaction_id():
    """Generate a unique transaction ID."""
    global _next_transaction_id
    with _transaction_id_lock:
        tid = _next_transaction_id
        _next_transaction_id += 1
        return tid

class Transaction:
    """
    Basic transaction - executes queries sequentially.
    Thread safety comes from table-level locks, not 2PL.
    """
    def __init__(self):
        self.transaction_id = get_next_transaction_id()
        self.queries = []
        self.executed_operations = [] 
        
    def add_query(self, query, table, *args):
        """Add a query to this transaction."""
        self.queries.append((query, table, args))
        
    def run(self):
        """
        Execute all queries in the transaction.
        Returns True if all succeed, False otherwise.
        """
        try:
            for query, table, args in self.queries:
                result = query(*args)

                if result is False:
                    return self.abort()

                self.executed_operations.append({
                    'query': query,
                    'table': table,
                    'args': args,
                    'result': result
                })

            return self.commit()
            
        except Exception as e:
            return self.abort()
    
    def abort(self):
        """Abort the transaction."""
        self.executed_operations.clear()
        return False
    
    def commit(self):
        """Commit the transaction."""
        self.executed_operations.clear()
        return True