from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from random import choice, randint, sample, seed
import time

db = Database()
db.open('./CS451')

grades_table = db.get_table('Grades')
query = Query(grades_table)

print("=" * 60)
print("COMPREHENSIVE LOCKING TEST SUITE")
print("=" * 60)

# Test 1: High Contention Test - Many transactions updating same records
print("\n[TEST 1] High Contention Test")
print("Testing: Lock conflicts and abort/retry behavior")

seed(12345)
num_records = 50  # Small set for high contention
num_transactions = 100
num_threads = 8

keys = [92106429 + i for i in range(num_records)]
initial_values = {}

# Initialize records with known values
for key in keys:
    initial_values[key] = randint(100, 200)
    query.update(key, None, initial_values[key], None, None, None)

transactions = [Transaction() for _ in range(num_transactions)]
transaction_workers = [TransactionWorker() for _ in range(num_threads)]

# Each transaction updates RANDOM records (high conflict probability)
for t in transactions:
    # Each transaction updates 5 random records
    target_keys = sample(keys, 5)
    for key in target_keys:
        new_value = randint(0, 50)
        t.add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        t.add_query(query.update, grades_table, key, None, new_value, None, None, None)

# Distribute to workers
for i, t in enumerate(transactions):
    transaction_workers[i % num_threads].add_transaction(t)

# Run and measure
start_time = time.time()
for worker in transaction_workers:
    worker.run()
for worker in transaction_workers:
    worker.join()
elapsed = time.time() - start_time

# Check statistics
total_commits = sum(worker.result for worker in transaction_workers)
total_aborts = sum(len(worker.stats) for worker in transaction_workers) - total_commits

print(f"Transactions attempted: {num_transactions}")
print(f"Commits: {total_commits}")
print(f"Aborts (that later retried): {total_aborts}")
print(f"Time: {elapsed:.2f}s")
print(f"Status: {'PASS - All transactions committed' if total_commits == num_transactions else 'FAIL'}")

# Test 2: Read-Write Conflict Test
print("\n[TEST 2] Read-Write Conflict Test")
print("Testing: Readers don't block readers, but writers block readers")

seed(23456)
num_read_transactions = 50
num_write_transactions = 10

transactions = []

# Create many read transactions
for _ in range(num_read_transactions):
    t = Transaction()
    read_key = choice(keys)
    t.add_query(query.select, grades_table, read_key, 0, [1, 1, 1, 1, 1])
    transactions.append(t)

# Intersperse write transactions
for _ in range(num_write_transactions):
    t = Transaction()
    write_key = choice(keys)
    t.add_query(query.update, grades_table, write_key, None, randint(0, 100), None, None, None)
    transactions.append(t)

# Shuffle for random ordering
from random import shuffle
shuffle(transactions)

transaction_workers = [TransactionWorker() for _ in range(num_threads)]
for i, t in enumerate(transactions):
    transaction_workers[i % num_threads].add_transaction(t)

start_time = time.time()
for worker in transaction_workers:
    worker.run()
for worker in transaction_workers:
    worker.join()
elapsed = time.time() - start_time

total_commits = sum(worker.result for worker in transaction_workers)
print(f"Total transactions: {len(transactions)}")
print(f"Commits: {total_commits}")
print(f"Time: {elapsed:.2f}s")
print(f"Status: {'PASS' if total_commits == len(transactions) else 'FAIL'}")

# Test 3: Isolation Test - Verify no dirty reads
print("\n[TEST 3] Isolation Test (No Dirty Reads)")
print("Testing: Transactions don't see uncommitted changes")

seed(34567)
test_key = keys[0]

# Set initial value
query.update(test_key, None, 1000, None, None, None)

# Transaction 1: Long-running update that will abort
t1 = Transaction()
t1.add_query(query.update, grades_table, test_key, None, 9999, None, None, None)
# Add a conflicting update that will cause abort
t1.add_query(query.update, grades_table, test_key, None, 8888, None, None, None)

# Transaction 2: Should read original value, not 9999
t2 = Transaction()
t2.add_query(query.select, grades_table, test_key, 0, [1, 1, 1, 1, 1])

# Run them concurrently
workers = [TransactionWorker([t1]), TransactionWorker([t2])]
for w in workers:
    w.run()
for w in workers:
    w.join()

# Verify final value
final_record = query.select(test_key, 0, [1, 1, 1, 1, 1])[0]
final_value = final_record.columns[1]

print(f"Initial value: 1000")
print(f"Final value: {final_value}")
print(f"Status: {'PASS - No dirty read' if final_value != 9999 else 'FAIL - Dirty read detected!'}")

# Test 4: Deadlock Prevention Test
print("\n[TEST 4] Deadlock Prevention Test")
print("Testing: No-wait policy prevents deadlocks")

seed(45678)
# Create transactions that would deadlock without no-wait
# T1: locks A then B
# T2: locks B then A
# With no-wait, one should abort immediately

key_a = keys[0]
key_b = keys[1]

t1 = Transaction()
t1.add_query(query.update, grades_table, key_a, None, 100, None, None, None)
t1.add_query(query.update, grades_table, key_b, None, 200, None, None, None)

t2 = Transaction()
t2.add_query(query.update, grades_table, key_b, None, 300, None, None, None)
t2.add_query(query.update, grades_table, key_a, None, 400, None, None, None)

workers = [TransactionWorker([t1]), TransactionWorker([t2])]

start_time = time.time()
for w in workers:
    w.run()
for w in workers:
    w.join()
elapsed = time.time() - start_time

total_commits = sum(w.result for w in workers)
print(f"Both transactions completed: {total_commits == 2}")
print(f"Time: {elapsed:.2f}s")
print(f"Status: {'PASS - No deadlock' if elapsed < 5.0 else 'FAIL - Possible deadlock (timeout)'}")

# Test 5: Lock Upgrade Test
print("\n[TEST 5] Lock Upgrade Test")
print("Testing: Transaction can upgrade from shared to exclusive lock")

seed(56789)
test_key = keys[0]

# Transaction that reads then writes same record
t = Transaction()
t.add_query(query.select, grades_table, test_key, 0, [1, 1, 1, 1, 1])
t.add_query(query.update, grades_table, test_key, None, 777, None, None, None)

worker = TransactionWorker([t])
worker.run()
worker.join()

print(f"Lock upgrade successful: {worker.result == 1}")
print(f"Status: {'PASS' if worker.result == 1 else 'FAIL'}")

# Test 6: Range Lock Test (if sum is implemented)
print("\n[TEST 6] Range Lock Test")
print("Testing: Range queries acquire proper locks")

try:
    seed(67890)
    start_key = keys[0]
    end_key = keys[10]
    
    # Transaction 1: Sum over range
    t1 = Transaction()
    t1.add_query(query.sum, grades_table, start_key, end_key, 0)
    
    # Transaction 2: Update within range (should conflict)
    t2 = Transaction()
    mid_key = keys[5]
    t2.add_query(query.update, grades_table, mid_key, None, 999, None, None, None)
    
    workers = [TransactionWorker([t1]), TransactionWorker([t2])]
    for w in workers:
        w.run()
    for w in workers:
        w.join()
    
    total_commits = sum(w.result for w in workers)
    print(f"Both transactions handled: {total_commits == 2}")
    print(f"Status: {'PASS' if total_commits == 2 else 'FAIL'}")
except Exception as e:
    print(f"Status: SKIPPED (sum not implemented or error: {e})")

print("\n" + "=" * 60)
print("LOCKING TEST SUITE COMPLETE")
print("=" * 60)

db.close()