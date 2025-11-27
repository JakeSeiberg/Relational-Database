from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    """
    DEADLOCK-FREE VERSION
    Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions[:] if transactions else []  # Copy the list
        self.result = 0
        self.thread = None
    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
    
    """
    Runs all transaction as a thread
    """
    def run(self):
        # Create and start a thread that calls __run
        self.thread = threading.Thread(target=self.__run)
        self.thread.daemon = False  # Ensure thread completes
        self.thread.start()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread is not None:
            self.thread.join(timeout=30)  # Add timeout to prevent infinite waiting
            if self.thread.is_alive():
                print(f"WARNING: Worker thread did not finish within timeout")
    
    def __run(self):
        """
        Execute all transactions assigned to this worker.
        Retry aborted transactions until they commit.
        """
        for transaction in self.transactions:
            # Keep retrying until the transaction commits
            committed = False
            retry_count = 0
            max_retries = 100  # Prevent infinite retry loops
            
            while not committed and retry_count < max_retries:
                try:
                    # Each transaction returns True if committed or False if aborted
                    result = transaction.run()
                    
                    if result:
                        # Transaction committed successfully
                        committed = True
                        self.stats.append(True)
                    else:
                        # Transaction aborted due to lock conflict or other failure
                        # Reset transaction state for retry
                        transaction.executed_operations.clear()
                        retry_count += 1
                        
                        # Small delay before retry to reduce contention
                        if retry_count < max_retries:
                            import time
                            time.sleep(0.001)  # 1ms delay
                
                except Exception as e:
                    print(f"ERROR in transaction worker: {type(e).__name__}: {e}")
                    import traceback
                    traceback.print_exc()
                    break
            
            if retry_count >= max_retries:
                print(f"WARNING: Transaction exceeded max retries ({max_retries})")
                self.stats.append(False)
        
        # Store the number of transactions that committed
        self.result = len([x for x in self.stats if x])