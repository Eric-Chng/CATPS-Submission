# CATPS-Submission

## How To Run
Open folder in IntelliJ. 
Go to each file and click run on the main methods. Optionally, add DYLD_LIBRARY_PATH=/usr/local/lib to environment variables (wasn't needed when testing).

## Report
### Task 3
FDB server worked great!

### Task 4
Created template class that other classes rely on to abstract operations with FDB server.

### Task 5
Generated 10k key value pairs and measured getrange time in miliseconds. Made generation and deletion of keys in parallel to speed up process. Getranges took between multiple to hundreds of miliseconds as expected for a singlethreaded operation. See SingleGetRangeOutput.txt

### Task 6
Generated 10k key value pairs and measured thread creation and parallel getrange time in miliseconds. Made generation and deletion of keys in parallel to speed up process. Getranges in parallel were much faster, taking between several to dozens of miliseconds. See MultiRangesOutput.txt

### Task 7
Simultaneously run two transactions, one which reads some keys and another which modifies the same keys. No transaction conflict since read operations are marked as read-only and operate on a snapshot of the database. 
**1.** T1 commits successfully. Read transactions take place in an instantaneous snapshot when they are committed, so it will not encounter conflicts. Due to starting first, it will also not likely see T2's modified values.
**2.** Similarly, T2 will commit successfully. Read only transactions are not a concern when checking for conflicts, so T2 can run freely without concern. Thus, it will succeed in updating K2 and K4.

### Task 8
Simultaneously run two readwrite transactions. They write on the keys the other is reading. Conflict is expected
**1.** I expect T2 to succeed. This is because while the range it is reading from conflicts with the keys added by T1, it is started first so it has precedence in the conflict resolution.
**2.** I expect T1 to fail. This is because the range it is reading conflicts with the keys added by T2. This is a read-write conflict so the transaction aborts. If they are not interleaved, trivially the transaction will succeed.

## Reflection
After setting up and running on a different machine, everything went smoothly. Finished in less than an hour (code was already written and functioned with almost no modifications. Only major change was multithreading adding/deleting 10k keys to speed up process)
