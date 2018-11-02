package data.bytedance.net.ck.hive;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A serializable object which implements a serialization protocol
 * based on DataInput and dataOutput
 */
public class ClickHouseWritable implements Writable {
    /**
     * Serialize the fields of this object to <code>out</code>
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

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
