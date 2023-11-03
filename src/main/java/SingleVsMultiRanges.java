
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.StreamingMode;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.io.FileWriter;
import java.io.IOException;

public class SingleVsMultiRanges {

    private static final int TOTAL_KEYS = 10000;
    private static final int KEYS_PER_RANGE = 1000;
    private static final int NUM_RANGES = TOTAL_KEYS / KEYS_PER_RANGE;

    public static void main(String[] args) {
        System.out.println("Opening connection");
        FDB fdb = FDB.selectAPIVersion(620);
        Database db = fdb.open();
        System.out.println("Connection Opened");

        // Store 10k key-value pairs in parallel
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

        // Retrieve all 10k key-value pairs using different streaming modes
        FileWriter out = null;
        try {
            out = new FileWriter("MultiRangesOutput.txt");
            // Retrieve all 10k key-value pairs using different streaming modes
            for (StreamingMode x : BasicFDBOps.allModes) {
                measureParallelGetRangePerformance(db, x, out);
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
     * Measures getrange performance on multiple thread
     * @param db db reference
     * @param streamingMode streaming mode
     */
    private static void measureParallelGetRangePerformance(Database db, StreamingMode streamingMode, FileWriter out) throws IOException {
        // Measure multiple getRanges in parallel
        long start = System.currentTimeMillis(); //before threadpool to measure parallelization overhead
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_RANGES);
        for (int i = 0; i < NUM_RANGES; i++) {
            int rangeStart = i * KEYS_PER_RANGE;
            int rangeEnd = rangeStart + KEYS_PER_RANGE;
            executorService.submit(() -> {
                BasicFDBOps.getRange(db, rangeStart, rangeEnd, false, streamingMode);
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();

        out.write("Streaming Mode: " + streamingMode + " | Parallel getRange time: " + (end - start) + " ms\n");
        System.out.println("Streaming Mode: " + streamingMode + " | Parallel getRange time: " + (end - start) + " ms");
    }
}