from lstore.table import Table, Record
from lstore.index import Index
import threading
import time

class TransactionWorker:
    """
    Transaction worker that executes transactions with limited retry.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions[:] if transactions else []
        self.result = 0
        self.thread = None
    
    def add_transaction(self, t):
        """Appends t to transactions"""
        self.transactions.append(t)
    
    def run(self):
        """Runs all transaction as a thread"""
        self.thread = threading.Thread(target=self.__run)
        self.thread.daemon = False
        self.thread.start()
    
    def join(self):
        """Waits for the worker to finish"""
        if self.thread is not None:
            self.thread.join(timeout=30)
            if self.thread.is_alive():
                print(f"WARNING: Worker thread did not finish within timeout")
    
    def __run(self):
        """
        Execute all transactions assigned to this worker.
        With simplified locking, most should succeed on first try.
        """
        for transaction in self.transactions:
            committed = False
            retry_count = 0
            max_retries = 10
            
            while not committed and retry_count < max_retries:
                try:
                    result = transaction.run()
                    
                    if result:
                        committed = True
                        self.stats.append(True)
                    else:
                        transaction.executed_operations.clear()
                        retry_count += 1

                        if retry_count < max_retries:
                            time.sleep(0.001)
                
                except Exception as e:
                    transaction.executed_operations.clear()
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(0.001)
            
            if not committed:
                print(f"WARNING: Transaction failed after {max_retries} retries")
                self.stats.append(False)
        
        self.result = len([x for x in self.stats if x])