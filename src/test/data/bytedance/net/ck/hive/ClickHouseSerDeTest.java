package data.bytedance.net.ck.hive;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

class ClickHouseSerDeTest {

    private static final String COLUMN_NAMES = "time,c0,c1,c2,c3,c4,c5,c6,c7,c8";
    private static final Object[] ROW_OBJECT = new Object[]{
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
    private static final String TABLE_NAME = "testSerDe";
    private static final String CLICKHOUSE_CREATE_TABLE_STMT = "CREATE TABLE IF NOT EXISTS testSerDe\n" +
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

    @BeforeAll
    public static void beforeTest() throws SQLException {
        ClickHouseHelper helper = ClickHouseHelper.getClickHouseHelper(TestHelper.ckConnStr, TABLE_NAME);
        Connection c = helper.getClickHouseConnection();
        try {
            Statement stmt = c.createStatement();
            stmt.executeQuery("DROP TABLE IF EXISTS " + TABLE_NAME);
            stmt.executeQuery(CLICKHOUSE_CREATE_TABLE_STMT);
        } finally {
            c.close();
        }
    }

    @AfterAll
    public static void afterTest() throws SQLException {
        ClickHouseHelper helper = ClickHouseHelper.getClickHouseHelper(TestHelper.ckConnStr, TABLE_NAME);
        Connection c = helper.getClickHouseConnection();
        try {
            Statement stmt = c.createStatement();
            stmt.executeQuery("DROP TABLE IF EXISTS" + TABLE_NAME);
        } finally {
            c.close();
        }

    }

    @Test
    public void testClickHouseObjectSerializer() throws SerDeException {
        ClickHouseSerDe serDe = new ClickHouseSerDe();
        Configuration conf = new Configuration();
        Properties tblProps;
        tblProps = createPropertiesSource(TABLE_NAME, COLUMN_NAMES);
        SerDeUtils.initializeSerDe(serDe, conf, tblProps, null);
        serializeObject(tblProps, serDe, ROW_OBJECT, COLUMN_HIVE_TYPES, null);
    }

    private static Properties createPropertiesSource(String tableName, String columnNames) {
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, columnNames);
        tbl.setProperty(Constants.CK_CONN_STRS, TestHelper.ckConnStr);
        tbl.setProperty(Constants.CK_TBL_NAME, tableName);
        return tbl;
    }

    public static void serializeObject(Properties properties, ClickHouseSerDe serDe,
                                       Object[] rowObject, String types, ClickHouseWritable clickHouseWritable) throws SerDeException {
        List<String> columnNames = serDe.getColumnNames();
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(types);
        List<ObjectInspector> inspectors = new ArrayList<>();
        inspectors.addAll(Lists.transform(colTypes,
                (Function<TypeInfo, ObjectInspector>) type -> PrimitiveObjectInspectorFactory
                        .getPrimitiveWritableObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(type.getTypeName()))));
        ObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

        ClickHouseWritable writable = (ClickHouseWritable) serDe.serialize(rowObject, inspector);
        Assert.assertEquals(10, writable.getValue().size());

    }
}