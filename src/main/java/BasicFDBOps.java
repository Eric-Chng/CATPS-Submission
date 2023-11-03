import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;
import java.util.Optional;

public class BasicFDBOps {

    public static final StreamingMode[] allModes = new StreamingMode[]{
            StreamingMode.WANT_ALL,
            StreamingMode.EXACT,
            StreamingMode.ITERATOR,
            StreamingMode.LARGE,
            StreamingMode.MEDIUM,
            StreamingMode.SMALL,
            StreamingMode.SERIAL
    };


    /**
     * set key value pair. will override
     * @param db db reference
     * @param key key to set
     * @param value value to set
     */
    public static void setKeyValue(Database db, String key, String value) {
        db.run(tr -> {
            tr.set(Tuple.from(key).pack(), Tuple.from(value).pack());
            return null;
        });
    }

    /**
     * get key value pair
     * @param db db reference
     * @param key key to get
     * @return optional string if key exists in db. print example: value.ifPresent(System.out::println)
     */
    public static Optional<String> getKeyValue(Database db, String key) {
        return db.run(tr -> {
            byte[] valueBytes = tr.get(Tuple.from(key).pack()).join();
            if (valueBytes != null) {
                return Optional.of(Tuple.fromBytes(valueBytes).getString(0));
            }
            return Optional.empty();
        });
    }

    /**
     * Delete key pair. For cleaning up db
     * @param db db reference
     * @param key key to delete
     */
    public static void deleteKeyValue(Database db, String key) {
        db.run(tr -> {
            tr.clear(Tuple.from(key).pack());
            return null;
        });
    }

    /**
     * getrange operation with various options
     * @param db db reference
     * @param rowLimit max num of rows
     * @param reverse if true, reverse order. Limit will also be starting from end
     * @param streamingMode streaming mode
     */
    public static void getRange(Database db, int rowLimit, boolean reverse, StreamingMode streamingMode) {
        db.run(tr -> {
            Range range = new Range(new byte[]{0x00}, new byte[]{(byte)0xff});
            List<KeyValue> keyValues = tr.getRange(range, rowLimit, reverse, streamingMode).asList().join();
            return keyValues.size(); // Just to use the result
        });
    }

    /**
     * Overloaded getRange operation with various options and specific range start and end.
     * @param db db reference
     * @param rangeStart start of the range (inclusive)
     * @param rangeEnd end of the range (exclusive)
     * @param reverse if true, reverse order. Limit will also be starting from end
     * @param streamingMode streaming mode
     */
    public static void getRange(Database db, int rangeStart, int rangeEnd, boolean reverse, StreamingMode streamingMode) {
        db.run(tr -> {
            byte[] startKey = Tuple.from(rangeStart).pack();
            byte[] endKey = Tuple.from(rangeEnd).pack();
            Range range = new Range(startKey, endKey);
            List<KeyValue> keyValues = tr.getRange(range, (rangeEnd-rangeStart+1), reverse, streamingMode).asList().join();
            return keyValues.size(); // Just to use the result
        });
    }
}