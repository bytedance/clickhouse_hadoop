package data.bytedance.net.ck.hive;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class ClickHouseRecordWriterTest {

    private static final String TABLE_NAME = "testWriter";
    private static final String CLICKHOUSE_CREATE_TABLE_STMT = "CREATE TABLE IF NOT EXISTS testWriter\n" +
            "(\n" +
            "    time Datetime,\n" +
            "    c0 String,\n" +
            "    c1 String,\n" +
            "    c2 String,\n" +
            "    c3 Float64,\n" +
            "    c4 Float32,\n" +
            "    c5 Int64,\n" +
            "    c6 Int32,\n" +
            "    c7 Int16,\n" +
            "    c8 Int8\n" +
            ")\n" +
            "ENGINE = MergeTree\n" +
            "PARTITION BY time\n" +
            "ORDER BY c0\n" +
            "SETTINGS index_granularity = 8192";

    private static final String COLUMN_HIVE_TYPES = "timestamp,string,char(6),varchar(8),double,float,bigint,int,smallint,tinyint";
    private static ClickHouseHelper helper;

    @BeforeEach
    public void beforeTest() throws SQLException, ClassNotFoundException {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection c = DriverManager.getConnection(TestHelper.ckConnStr);
        try {
            Statement stmt = c.createStatement();
            stmt.executeQuery("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.executeQuery(CLICKHOUSE_CREATE_TABLE_STMT);
        } finally {
            c.close();
        }
        helper = ClickHouseHelper.getClickHouseHelper(TestHelper.ckConnStr, TABLE_NAME);
    }

    @AfterEach
    public void afterTest() throws SQLException {
        Connection c = helper.getClickHouseConnection();
        try {
            Statement stmt = c.createStatement();
            stmt.executeQuery("DROP TABLE IF EXISTS " + TABLE_NAME);
        } finally {
            c.close();
        }

    }

    /**
     * Test that the insert query can be correctly constructed from column names and table name
     */
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

    /**
     * Test that the writer can correctly write data to ClickHouse
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws SerDeException
     * @throws IOException
     */
    @Test
    public void testWriteSingleRow() throws SQLException, ClassNotFoundException, SerDeException, IOException {
        List<String> columnNames = helper.getColumnNames();
        ClickHouseRecordWriter recordWriter = new ClickHouseRecordWriter(
                helper, 5, TABLE_NAME, columnNames, helper.getColumnTypes());

        ClickHouseSerDe serDe = new ClickHouseSerDe();
        Configuration conf = new Configuration();
        Properties tblProps = TestHelper.createPropertiesSource(TABLE_NAME, String.join(",", columnNames));
        SerDeUtils.initializeSerDe(serDe, conf, tblProps, null);

        Object[] row_object1 = new Object[]{
                new TimestampWritable(new Timestamp(1377907200000L)),
                new Text("dim1_val"),
                new HiveCharWritable(new HiveChar("dim2_v", 6)),
                new HiveVarcharWritable(new HiveVarchar("dim3_val", 8)),
                new DoubleWritable(10669.3D),
                new FloatWritable(10669.45F),
                new LongWritable(1113939),
                new IntWritable(1112123),
                new ShortWritable((short) 12),
                new ByteWritable((byte) 0),
        };
        ClickHouseWritable writable = TestHelper.serializeObject(serDe, row_object1, COLUMN_HIVE_TYPES);
        recordWriter.write(writable);
        // force flush
        recordWriter.close(true);
        Connection conn = helper.getClickHouseConnection();
        Statement stmt = conn.createStatement();
        try {
            ResultSet rs = stmt.executeQuery("SELECT " + String.join(",", columnNames) + " FROM " + TABLE_NAME);
            int count = 0;
            while (rs.next()) {
                ++count;
                if (count > 1) {
                    Assert.assertTrue("There can only be one row", count <= 1);
                }
                Assert.assertEquals(new Timestamp(1377907200000L), rs.getTimestamp(1));
                Assert.assertEquals("dim1_val", rs.getString(2));
                Assert.assertEquals("dim2_v", rs.getString(3));
                Assert.assertEquals("dim3_val", rs.getString(4));
                Assert.assertEquals(10669.3D, rs.getDouble(5));
                Assert.assertEquals(10669.45F, rs.getFloat(6));
                Assert.assertEquals(1113939, rs.getLong(7));
                Assert.assertEquals(1112123, rs.getInt(8));
                Assert.assertEquals((short) 12, rs.getShort(9));
                Assert.assertEquals((byte) 0, rs.getByte(10));
            }
        } finally {
            conn.close();
            stmt.close();
        }
    }

    /**
     * Make sure that the RecordWriter will correctly flush the in memory rows as long as the number reaches the
     * batch size
     *
     * @throws SerDeException
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testWriteAutoBatch() throws SerDeException, IOException, SQLException {
        int batchSize = 5;
        List<String> columnNames = helper.getColumnNames();
        ClickHouseRecordWriter recordWriter = new ClickHouseRecordWriter(
                helper, batchSize, TABLE_NAME, columnNames, helper.getColumnTypes());

        ClickHouseSerDe serDe = new ClickHouseSerDe();
        Configuration conf = new Configuration();
        Properties tblProps = TestHelper.createPropertiesSource(TABLE_NAME, String.join(",", columnNames));
        SerDeUtils.initializeSerDe(serDe, conf, tblProps, null);

        Object[] row_object1 = new Object[]{
                new TimestampWritable(new Timestamp(1377907200000L)),
                new Text("dim1_val"),
                new HiveCharWritable(new HiveChar("dim2_v", 6)),
                new HiveVarcharWritable(new HiveVarchar("dim3_val", 8)),
                new DoubleWritable(10669.3D),
                new FloatWritable(10669.45F),
                new LongWritable(1113939),
                new IntWritable(1112123),
                new ShortWritable((short) 12),
                new ByteWritable((byte) 0),
        };
        ClickHouseWritable writable = TestHelper.serializeObject(serDe, row_object1, COLUMN_HIVE_TYPES);
        for (int i = 0; i < batchSize; ++i) {
            recordWriter.write(writable);
        }
        Connection conn = helper.getClickHouseConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME);
        rs.next();
        Assert.assertEquals(5, rs.getInt(1));

    }
}