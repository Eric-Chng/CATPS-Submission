
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;

public class ReadSnapshot {
    /*
    OUTPUT

    Transaction T1 committed successfully.
    Transaction T2 committed successfully.


     */

    private static final FDB fdb = FDB.selectAPIVersion(620);

    public static void main(String[] args) {
        Database db = fdb.open();

        // Store 4 key-value pairs
        BasicFDBOps.setKeyValue(db, "K1", "V1");
        BasicFDBOps.setKeyValue(db, "K2", "V2");
        BasicFDBOps.setKeyValue(db, "K3", "V3");
        BasicFDBOps.setKeyValue(db, "K4", "V4");

        // Transaction T1 will read several keys in a thread
        Thread t1Thread = new Thread(() -> {
            Transaction t1 = db.createTransaction();
            t1.get("K1".getBytes()).join();
            t1.get("K2".getBytes()).join();
            t1.get("K3".getBytes()).join();
            try {
                t1.commit().join();
                System.out.println("Transaction T1 committed successfully.");
            } catch (Exception e) {
                System.out.println("Transaction T1 aborted: " + e.getMessage());
            }
        });

        // Transaction T2 will update the values of K2, K4 in a thread
        Thread t2Thread = new Thread(() -> {
            Transaction t2 = db.createTransaction();
            t2.set("K2".getBytes(), "V2_updated".getBytes());
            t2.set("K4".getBytes(), "V4_updated".getBytes());
            try {
                t2.commit().join();
                System.out.println("Transaction T2 committed successfully.");
            } catch (Exception e) {
                System.out.println("Transaction T2 aborted: " + e.getMessage());
            }
        });

        //start both threads at once
        t1Thread.start();
        t2Thread.start();
        try {
            t1Thread.join();
            t2Thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        /*
            ANSWERING EXPLANATIONS
            I cannot be certain due to not being able to run the code but here is my expectation for the code
            1. T1 commits successfully. Read transactions take place in an instantaneous snapshot when they are
            committed, so it will not encounter conflicts. Due to starting first, it will also not likely see
            T2's modified values.
            2. Similarly, T2 will commit successfully. Read only transactions are not a concern when checking
            for conflicts, so T2 can run freely without concern. Thus, it will succeed in updating K2 and K4.
         */

        // Clear db
        BasicFDBOps.deleteKeyValue(db, "K1");
        BasicFDBOps.deleteKeyValue(db, "K2");
        BasicFDBOps.deleteKeyValue(db, "K3");
        BasicFDBOps.deleteKeyValue(db, "K4");

        // Close db
        db.close();
    }
}