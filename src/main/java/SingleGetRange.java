import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.StreamingMode;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.io.FileWriter;
import java.io.IOException;

public class SingleGetRange {

    private static final int TOTAL_KEYS = 10000;

    public static void main(String[] args) {

        System.out.println("Opening connection");
        FDB fdb = FDB.selectAPIVersion(620);
        Database db = fdb.open();
        System.out.println("Connection Opened");

        // Store 10k key-value pairs
        System.out.println("Adding Keys");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            int rangeStart = i * 1000;
            int rangeEnd = rangeStart + 1000;
            executorService.submit(() -> {
                for (int j = rangeStart; j < rangeEnd; j++) {
                    BasicFDBOps.setKeyValue(db, "key_" + j, "val_"+j);
                }
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Keys added");

        FileWriter out = null;
        try {
            out = new FileWriter("SingleGetRangeOutput.txt");
            // Retrieve all 10k key-value pairs using different streaming modes
            for (StreamingMode x : BasicFDBOps.allModes) {
                measureGetRangePerformance(db, x, out);
            }
            out.close();
        } catch (IOException e) {
            System.out.println("File Exception: " + e.getMessage());
        }


        // Delete the key-value pairs to avoid DB bloat
        System.out.println("Deleting Keys");
        ExecutorService executorServiceDelete = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            int rangeStart = i * 1000;
            int rangeEnd = rangeStart + 1000;
            executorServiceDelete.submit(() -> {
                for (int j = rangeStart; j < rangeEnd; j++) {
                    BasicFDBOps.setKeyValue(db, "key_" + j, "val_"+j);
                }
            });
        }
        executorServiceDelete.shutdown();
        try {
            executorServiceDelete.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Deleting Done");
        // Close the database connection
        db.close();
    }

    /**
     * Measures getrange performance on one thread
     * @param db db reference
     * @param streamingMode streaming mode
     * @param out output file
     */
    private static void measureGetRangePerformance(Database db, StreamingMode streamingMode, FileWriter out) throws IOException{
        long startTime = System.currentTimeMillis();
        BasicFDBOps.getRange(db, TOTAL_KEYS, false, streamingMode);
        long endTime = System.currentTimeMillis();

        out.write("Streaming Mode: " + streamingMode + ", Time Taken: " + (endTime - startTime) + " ms\n");
        System.out.println("Streaming Mode: " + streamingMode + ", Time Taken: " + (endTime - startTime) + " ms");
    }
}