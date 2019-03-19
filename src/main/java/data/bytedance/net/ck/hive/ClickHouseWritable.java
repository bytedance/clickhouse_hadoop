package data.bytedance.net.ck.hive;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Represents the serialized data getting from Hive.
 */
public class ClickHouseWritable implements Writable {
    Map<String, Object> value;

    public ClickHouseWritable(Map<String, Object> value) {
        this.value = value;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    /**
     * Serialize the fields of this object to <code>out</code>
     * 
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Deserialize the fields of this object from <code>in</code>
     * 
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
