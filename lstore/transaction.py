from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import get_lock_manager, LockType
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
    Transaction with Strict 2PL and no-wait policy.
    """
    def __init__(self):
        self.transaction_id = get_next_transaction_id()
        self.queries = []
        self.executed_operations = [] 
        self.lock_manager = get_lock_manager()
        
    def add_query(self, query, table, *args):
        """
        Add a query to this transaction.
        """
        self.queries.append((query, table, args))
        
    def run(self):
        """
        Execute all queries in the transaction using Strict 2PL.
        Returns True if transaction commits, False if it aborts.
        """
        try:
            for i, (query, table, args) in enumerate(self.queries):
                # Prepare operation metadata for rollback
                operation_info = self._prepare_operation(query, table, args)
                operation_info['index'] = i
                
                # Acquire necessary locks based on operation type
                lock_acquired = self._acquire_locks_for_operation(operation_info)
                
                if not lock_acquired:
                    # Cannot acquire lock - abort immediately (no-wait policy)
                    return self.abort()
                
                # Execute the query
                result = query(*args)
                
                # If the query failed, abort the transaction
                if result == False:
                    return self.abort()
                
                # Track successful operation
                operation_info['result'] = result
                self.executed_operations.append(operation_info)
            
            # All queries succeeded, commit the transaction
            return self.commit()
            
        except Exception as e:
            # Any exception should trigger abort
            return self.abort()
    
    def _acquire_locks_for_operation(self, operation_info):
        """
        Acquire necessary locks for an operation.
        Returns True if all locks acquired, False otherwise.
        """
        query_name = operation_info['query_name']
        args = operation_info['args']
        table = operation_info['table']
        
        # Determine which records need locking and lock type
        if 'select' in query_name:
            # SELECT needs shared lock on the record
            if len(args) >= 1:
                key = args[0]  # Primary key
                record_id = self._get_record_id(table, key)
                if record_id is not None:
                    if not self.lock_manager.acquire_shared(self.transaction_id, record_id):
                        return False
        
        elif 'insert' in query_name:
            # INSERT needs exclusive lock on the new record
            if len(args) >= 1:
                key = args[0]  # Primary key is typically first column
                record_id = self._get_record_id(table, key)
                # For insert, we create a synthetic record_id based on table and key
                record_id = f"{table.name}:{key}"
                if not self.lock_manager.acquire_exclusive(self.transaction_id, record_id):
                    return False
        
        elif 'update' in query_name:
            # UPDATE needs exclusive lock on the record
            if len(args) >= 1:
                key = args[0]  # Primary key
                record_id = self._get_record_id(table, key)
                if record_id is not None:
                    if not self.lock_manager.acquire_exclusive(self.transaction_id, record_id):
                        return False
        
        elif 'delete' in query_name:
            # DELETE needs exclusive lock on the record
            if len(args) >= 1:
                key = args[0]  # Primary key
                record_id = self._get_record_id(table, key)
                if record_id is not None:
                    if not self.lock_manager.acquire_exclusive(self.transaction_id, record_id):
                        return False
        
        elif 'sum' in query_name:
            # SUM needs shared locks on range of records
            if len(args) >= 2:
                start_key = args[0]
                end_key = args[1]
                # Lock all records in range
                record_ids = self._get_record_ids_in_range(table, start_key, end_key)
                for record_id in record_ids:
                    if not self.lock_manager.acquire_shared(self.transaction_id, record_id):
                        return False
        
        return True
    
    def _get_record_id(self, table, key):
        """
        Get a unique identifier for a record.
        This should be based on your table's internal structure.
        """
        # Simple implementation: table name + key
        return f"{table.name}:{key}"
    
    def _get_record_ids_in_range(self, table, start_key, end_key):
        """
        Get record IDs for all records in a key range.
        """
        # This depends on your table implementation
        # For now, return a list of record IDs in the range
        record_ids = []
        try:
            # You might need to access table's internal structure
            # This is a simplified version
            for key in range(start_key, end_key + 1):
                record_ids.append(f"{table.name}:{key}")
        except:
            pass
        return record_ids
    
    def _prepare_operation(self, query, table, args):
        """
        Prepare operation metadata for potential rollback.
        """
        operation_info = {
            'query': query,
            'table': table,
            'args': args,
            'query_name': query.__name__ if hasattr(query, '__name__') else str(query)
        }
        
        # For updates and deletes, capture the old state
        if 'update' in operation_info['query_name']:
            if len(args) > 0:
                primary_key = args[0]
                old_record = self._get_record_snapshot(table, primary_key)
                operation_info['old_record'] = old_record
                operation_info['primary_key'] = primary_key
                
        elif 'delete' in operation_info['query_name']:
            if len(args) > 0:
                primary_key = args[0]
                old_record = self._get_record_snapshot(table, primary_key)
                operation_info['old_record'] = old_record
                operation_info['primary_key'] = primary_key
                
        elif 'insert' in operation_info['query_name']:
            if len(args) > 0:
                operation_info['inserted_key'] = args[0]
        
        return operation_info
    
    def _get_record_snapshot(self, table, primary_key):
        """
        Capture the current state of a record before modification.
        """
        try:
            # Attempt to select the record
            # Note: We should already have a lock on this record
            from lstore.query import Query
            query = Query(table)
            records = query.select(primary_key, 0, [1] * table.num_columns)
            
            if records and len(records) > 0:
                record = records[0]
                snapshot = {
                    'columns': [record.columns[i] for i in range(len(record.columns))],
                    'rid': record.rid if hasattr(record, 'rid') else None
                }
                return snapshot
        except:
            pass
        return None
    
    def abort(self):
        """
        Abort the transaction and rollback all changes.
        """
        # Rollback operations in reverse order (LIFO)
        for operation_info in reversed(self.executed_operations):
            try:
                self._rollback_operation(operation_info)
            except Exception as e:
                pass  # Continue rolling back even if one fails
        
        # Release all locks (Strict 2PL: release at end)
        self.lock_manager.release_all(self.transaction_id)
        
        # Clear executed operations
        self.executed_operations.clear()
        
        return False
    
    def _rollback_operation(self, operation_info):
        """
        Rollback a single operation.
        """
        query_name = operation_info['query_name']
        table = operation_info['table']
        
        if 'insert' in query_name:
            # For insert rollback, delete the inserted record
            inserted_key = operation_info.get('inserted_key')
            if inserted_key is not None:
                try:
                    from lstore.query import Query
                    query = Query(table)
                    query.delete(inserted_key)
                except:
                    pass
                
        elif 'update' in query_name:
            # For update rollback, restore old values
            primary_key = operation_info.get('primary_key')
            old_record = operation_info.get('old_record')
            if primary_key is not None and old_record is not None:
                try:
                    from lstore.query import Query
                    query = Query(table)
                    # Restore all columns
                    old_columns = old_record['columns']
                    query.update(primary_key, *old_columns)
                except:
                    pass
                
        elif 'delete' in query_name:
            # For delete rollback, re-insert the record
            primary_key = operation_info.get('primary_key')
            old_record = operation_info.get('old_record')
            if primary_key is not None and old_record is not None:
                try:
                    from lstore.query import Query
                    query = Query(table)
                    old_columns = old_record['columns']
                    query.insert(*old_columns)
                except:
                    pass
    
    def commit(self):
        """
        Commit the transaction.
        In Strict 2PL, all locks are held until commit.
        """
        # Release all locks (Strict 2PL: release at end)
        self.lock_manager.release_all(self.transaction_id)
        
        # Clear executed operations
        self.executed_operations.clear()
        
        return True