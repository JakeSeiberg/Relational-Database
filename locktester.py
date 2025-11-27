"""
Test script for Part 1 Transaction implementation (no locking)
Tests basic transaction functionality: commit, abort, and rollback
"""

from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from random import randint, seed

print("=" * 60)
print("TRANSACTION PART 1 TEST SCRIPT")
print("=" * 60)

# Setup
db = Database()
db.open('./test_db')

# Create a simple test table
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

print("\n[TEST 1] Basic Transaction Commit")
print("-" * 60)

# Insert some initial records
print("Inserting initial records...")
for i in range(5):
    key = 1000 + i
    query.insert(key, i, i*10, i*100, i*1000)
    print(f"  Inserted: {key}, {i}, {i*10}, {i*100}, {i*1000}")

# Test 1: Simple transaction that should commit
print("\n[Transaction 1] Update with commit:")
t1 = Transaction()
t1.add_query(query.select, grades_table, 1000, 0, [1, 1, 1, 1, 1])
t1.add_query(query.update, grades_table, 1000, *[None, 99, None, None, None])
result1 = t1.run()

print(f"  Transaction result: {'COMMIT' if result1 else 'ABORT'}")

# Verify the update
record = query.select(1000, 0, [1, 1, 1, 1, 1])[0]
print(f"  After transaction - Key 1000: {record.columns}")
print(f"  Expected column[1] = 99, Got = {record.columns[1]}")
assert record.columns[1] == 99, "Update failed!"
print("  ✓ Test 1 PASSED")


print("\n[TEST 2] Transaction with Multiple Operations")
print("-" * 60)

t2 = Transaction()
t2.add_query(query.update, grades_table, 1001, *[None, 111, None, None, None])
t2.add_query(query.update, grades_table, 1002, *[None, 222, None, None, None])
t2.add_query(query.update, grades_table, 1003, *[None, 333, None, None, None])
result2 = t2.run()

print(f"  Transaction result: {'COMMIT' if result2 else 'ABORT'}")

# Verify all updates
for i, expected in enumerate([111, 222, 333]):
    record = query.select(1001 + i, 0, [1, 1, 1, 1, 1])[0]
    print(f"  Key {1001 + i}: column[1] = {record.columns[1]} (expected {expected})")
    assert record.columns[1] == expected, f"Update for key {1001 + i} failed!"
print("  ✓ Test 2 PASSED")


print("\n[TEST 3] Transaction with TransactionWorker (Single Thread)")
print("-" * 60)

t3 = Transaction()
t3.add_query(query.update, grades_table, 1004, *[None, 444, None, None, None])

worker = TransactionWorker([t3])
worker.run()
worker.join()

print(f"  Worker result: {worker.result} transactions committed")
assert worker.result == 1, "Worker should have committed 1 transaction"

record = query.select(1004, 0, [1, 1, 1, 1, 1])[0]
print(f"  Key 1004: column[1] = {record.columns[1]} (expected 444)")
assert record.columns[1] == 444, "Worker transaction failed!"
print("  ✓ Test 3 PASSED")


print("\n[TEST 4] Multiple Transactions with Multiple Workers")
print("-" * 60)

num_transactions = 10
num_workers = 3

transactions = []
for i in range(num_transactions):
    t = Transaction()
    key = 2000 + i
    # Insert new records
    t.add_query(query.insert, grades_table, key, i, i*2, i*3, i*4)
    transactions.append(t)

workers = []
for i in range(num_workers):
    workers.append(TransactionWorker())

# Distribute transactions across workers
for i, t in enumerate(transactions):
    workers[i % num_workers].add_transaction(t)

# Run all workers
for w in workers:
    w.run()

# Wait for completion
for w in workers:
    w.join()

# Check results
total_committed = sum(w.result for w in workers)
print(f"  Total committed: {total_committed}/{num_transactions}")
assert total_committed == num_transactions, "Some transactions failed!"

# Verify all inserts
for i in range(num_transactions):
    key = 2000 + i
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    expected = [key, i, i*2, i*3, i*4]
    print(f"  Key {key}: {record.columns} (expected {expected})")
    assert record.columns == expected, f"Insert for key {key} failed!"
print("  ✓ Test 4 PASSED")


print("\n[TEST 5] Concurrent Updates (Non-conflicting keys)")
print("-" * 60)

# Create base records
for i in range(20):
    key = 3000 + i
    query.insert(key, 0, 0, 0, 0)

# Create transactions that update different keys
num_transactions = 20
transactions = []
seed(12345)

for i in range(num_transactions):
    t = Transaction()
    key = 3000 + i  # Each transaction gets unique key
    value = randint(100, 999)
    t.add_query(query.update, grades_table, key, *[None, value, None, None, None])
    transactions.append((t, key, value))

# Run with multiple workers
num_workers = 4
workers = [TransactionWorker() for _ in range(num_workers)]

for i, (t, _, _) in enumerate(transactions):
    workers[i % num_workers].add_transaction(t)

for w in workers:
    w.run()

for w in workers:
    w.join()

# Verify results
total_committed = sum(w.result for w in workers)
print(f"  Total committed: {total_committed}/{num_transactions}")
assert total_committed == num_transactions, "Some transactions failed!"

success_count = 0
for t, key, expected_value in transactions:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    if record.columns[1] == expected_value:
        success_count += 1
    else:
        print(f"  ✗ Key {key}: got {record.columns[1]}, expected {expected_value}")

print(f"  Verified {success_count}/{num_transactions} updates")
assert success_count == num_transactions, "Some updates were incorrect!"
print("  ✓ Test 5 PASSED")


print("\n" + "=" * 60)
print("ALL TESTS PASSED!")
print("=" * 60)

db.close()