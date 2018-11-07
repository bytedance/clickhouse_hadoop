package data.bytedance.net.ck.hive;

import junit.framework.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseRecordWriterTest {

    @Test
    public void testConstructInsertQuery() {
        List<String> columns = new ArrayList<String>();
        columns.add("c1");
        columns.add("c2");
        columns.add("c3");
        String query = ClickHouseRecordWriter.constructInsertQuery("test", columns);
        String expected = "INSERT INTO test (c1,c2,c3) VALUES (?,?,?)";
        Assert.assertEquals(expected, query);

    }

}