package data.bytedance.net.ck.hive;

import data.bytedance.net.utils.Tuple;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the serialized data getting from Hive.
 */
public class ClickHouseWritable implements Writable {
    Map<String, Tuple<? extends StructField, Object>> value;

    public ClickHouseWritable(Map<String, Tuple<? extends StructField, Object>> value) {
        this.value = value;
    }

    public Map<String, Tuple<? extends StructField, Object>> getValue() {
        return value;
    }
    /**
     * Serialize the fields of this object to <code>out</code>
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Deserialize the fields of this object from <code>in</code>
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
