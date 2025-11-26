from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:
    """
    # Creates a transaction worker object.
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
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread is not None:
            self.thread.join()


    def __run(self):
        """
        Execute all transactions assigned to this worker.
        Retry aborted transactions until they commit.
        """
        for transaction in self.transactions:
            # Keep retrying until the transaction commits
            committed = False
            while not committed:
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
                    # Note: locks are already released in abort()
        
        # Store the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))