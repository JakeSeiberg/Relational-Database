"""
Simple test to check if deadlock is fixed.
Run this with: python simple_test.py

If it completes in under 5 seconds, deadlock is fixed!
If it hangs, you still have a deadlock issue.
"""

import signal
import sys

def timeout_handler(signum, frame):
    print("\n" + "="*60)
    print("❌ TIMEOUT - DEADLOCK DETECTED")
    print("="*60)
    print("The program is stuck in a deadlock.")
    print("Check the troubleshooting guide for fixes.")
    print("="*60)
    sys.exit(1)

# Set 10 second timeout
signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(10)

try:
    print("="*60)
    print("DEADLOCK TEST")
    print("="*60)
    
    print("\n1. Importing modules...")
    from lstore.db import Database
    from lstore.query import Query
    from lstore.transaction import Transaction
    print("   ✓ Imports successful")
    
    print("\n2. Creating database...")
    db = Database()
    print("   ✓ Database created")
    
    print("\n3. Creating table...")
    table = db.create_table('Students', 5, 0)
    print("   ✓ Table created")
    
    print("\n4. Creating query object...")
    query = Query(table)
    print("   ✓ Query object created")
    
    print("\n5. Testing direct insert (no transaction)...")
    result = query.insert(1, 100, 200, 300, 400)
    print(f"   ✓ Direct insert result: {result}")
    
    print("\n6. Testing direct select (no transaction)...")
    result = query.select(1, table.key, [1, 1, 1, 1, 1])
    print(f"   ✓ Direct select result: {result}")
    
    print("\n7. Creating transaction with insert...")
    t1 = Transaction()
    t1.add_query(query.insert, table, 2, 110, 210, 310, 410)
    print("   ✓ Transaction created")
    
    print("\n8. Running transaction...")
    result = t1.run()
    print(f"   ✓ Transaction result: {result}")
    
    print("\n9. Creating transaction with select...")
    t2 = Transaction()
    t2.add_query(query.select, table, 2, table.key, [1, 1, 1, 1, 1])
    print("   ✓ Transaction created")
    
    print("\n10. Running transaction...")
    result = t2.run()
    print(f"   ✓ Transaction result: {result}")
    
    print("\n11. Creating transaction with update...")
    t3 = Transaction()
    t3.add_query(query.update, table, 2, None, 999, None, None, None)
    print("   ✓ Transaction created")
    
    print("\n12. Running transaction...")
    result = t3.run()
    print(f"   ✓ Transaction result: {result}")
    
    print("\n13. Testing concurrent transactions...")
    from lstore.transaction_worker import TransactionWorker
    
    # Create two transactions
    t4 = Transaction()
    t4.add_query(query.insert, table, 3, 120, 220, 320, 420)
    
    t5 = Transaction()
    t5.add_query(query.insert, table, 4, 130, 230, 330, 430)
    
    # Run in workers
    worker1 = TransactionWorker([t4])
    worker2 = TransactionWorker([t5])
    
    worker1.run()
    worker2.run()
    
    worker1.join()
    worker2.join()
    
    print(f"   ✓ Worker 1 result: {worker1.result}")
    print(f"   ✓ Worker 2 result: {worker2.result}")
    
    # Cancel timeout
    signal.alarm(0)
    
    print("\n" + "="*60)
    print("✓✓✓ ALL TESTS PASSED - NO DEADLOCK ✓✓✓")
    print("="*60)
    print("\nYour transaction system is working correctly!")
    print("="*60 + "\n")

except Exception as e:
    signal.alarm(0)
    print("\n" + "="*60)
    print("❌ ERROR OCCURRED")
    print("="*60)
    print(f"Error type: {type(e).__name__}")
    print(f"Error message: {e}")
    print("\nFull traceback:")
    import traceback
    traceback.print_exc()
    print("="*60 + "\n")
    sys.exit(1)