
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

public class TransactionConflict {
    /*
    OUTPUT
    Transaction T2 committed successfully.
    Transaction T1 aborted: com.apple.foundationdb.FDBException: Transaction not committed due to conflict with another transaction
     */

    public static void main(String[] args) {
        FDB fdb = FDB.selectAPIVersion(620);
        Database db = fdb.open();

        // Store 2 key-value pairs
        BasicFDBOps.setKeyValue(db, "K1", "V1");
        BasicFDBOps.setKeyValue(db, "K2", "V2");

        // Transaction T1 and read several keys in a separate thread
        Thread t1Thread = new Thread(() -> {
            Transaction t1 = db.createTransaction();
            t1.get("K1".getBytes()).join();
            t1.set("K2".getBytes(), "V2_updated".getBytes());
            try {
                t1.commit().join();
                System.out.println("Transaction T1 committed successfully.");
            } catch (Exception e) {
                System.out.println("Transaction T1 aborted: " + e.getMessage());
            }
        });

        // Transaction T2 that updates the values of K2, K4 in another thread
        Thread t2Thread = new Thread(() -> {
            Transaction t2 = db.createTransaction();
            t2.get("K2".getBytes()).join();
            t2.set("K1".getBytes(), "V1_updated".getBytes());
            try {
                t2.commit().join();
                System.out.println("Transaction T2 committed successfully.");
            } catch (Exception e) {
                System.out.println("Transaction T2 aborted: " + e.getMessage());
            }
        });

        // Start both threads at once
        // T2 first because of spec
        t2Thread.start();
        t1Thread.start();
        try {
            t1Thread.join();
            t2Thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        /*
            ANSWERING EXPLANATIONS
            1. I expect T2 to succeed. This is because while the range it is reading from conflicts with the keys
            added by T1, it is started first so it has precedence in the conflict resolution.
            2. I expect T1 to fail. This is because the range it is reading conflicts with the keys added by T2.
            This is a read-write conflict so the transaction aborts. If they are not interleaved, trivially the
            transaction will succeed.
         */

        // Clear db
        BasicFDBOps.deleteKeyValue(db, "K1");
        BasicFDBOps.deleteKeyValue(db, "K2");

        // Close db
        db.close();
    }
}