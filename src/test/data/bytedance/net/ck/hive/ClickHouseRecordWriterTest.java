package data.bytedance.net.ck.hive;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
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

    @BeforeAll
    public static void beforeTest() throws SQLException, ClassNotFoundException {
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

    @AfterAll
    public static void afterTest() throws SQLException {
        Connection c = helper.getClickHouseConnection();
        try {
            Statement stmt = c.createStatement();
            stmt.executeQuery("DROP TABLE IF EXISTS " + TABLE_NAME);
        } finally {
            c.close();
        }

    }

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

    @Test
    public void testWrite() throws SQLException, ClassNotFoundException, SerDeException, IOException {
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
        recordWriter.close(true);
        Connection conn = helper.getClickHouseConnection();
    }
}