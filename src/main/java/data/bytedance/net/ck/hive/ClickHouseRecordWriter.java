package data.bytedance.net.ck.hive;

import data.bytedance.net.utils.Tuple;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Preconditions;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ClickHouseRecordWriter implements RecordWriter {
    private static Logger logger = LoggerFactory.getLogger(ClickHouseRecordWriter.class);
    private final int batchSize;
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final String insertQuery;
    private final ClickHouseHelper clickHouseHelper;

    private ArrayList<Map<String, Tuple<? extends StructField, Object>>> data = new ArrayList<>();

    public ClickHouseRecordWriter(ClickHouseHelper helper, int batchSize, String tableName,
                                  List<String> columnNames, List<String> columnTypes) {
        this.batchSize = batchSize;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.insertQuery = constructInsertQuery(tableName, columnNames);
        this.clickHouseHelper = helper;

    }

    public static String constructInsertQuery(String tableName, List<String> columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" ");

        String fields = String.join(",", columnNames);
        String[] valueSlice = new String[columnNames.size()];
        Arrays.fill(valueSlice, "?");
        String values = String.join(",", valueSlice);
        sql.append("(").append(fields).append(") VALUES (")
                .append(values).append(")");
        return sql.toString();
    }

    private static void addValuesToBatch(
            Map<String, Tuple<? extends StructField, Object>> value,
            PreparedStatement stmt, List<String> columnNames, List<String> columnTypes) throws SQLException {
        Preconditions.checkArgument(columnNames.size() == columnTypes.size(), "Column types and column names must be matched");
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            String columnType = columnTypes.get(i);
            Tuple<? extends StructField, Object> v = value.containsKey(columnName) ? value.get(columnName) : null;
            if (v != null && v.y != null) {
                StructField structField = v.x;
                Object obj = v.y;
                switch (columnType) {
                    case "Int8":
                    case "UInt8":
                        stmt.setByte(i + 1, ((ByteObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "Int16":
                    case "UInt16":
                        stmt.setShort(i + 1, ((ShortObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "Int32":
                    case "UInt32":
                        stmt.setInt(i + 1, ((IntObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "Int64":
                    case "UInt64":
                        stmt.setLong(i + 1, ((LongObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "Float32":
                        stmt.setFloat(i + 1, ((FloatObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "Float64":
                        stmt.setDouble(i + 1, ((DoubleObjectInspector) structField.getFieldObjectInspector()).get(obj));
                        break;
                    case "String":
                        ObjectInspector inspector = structField.getFieldObjectInspector();
                        if (inspector instanceof HiveCharObjectInspector) {
                            stmt.setString(i + 1,
                                    ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(obj).getValue());
                        } else if (inspector instanceof HiveVarcharObjectInspector) {
                            stmt.setString(i + 1,
                                    ((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(obj).getValue());
                        } else if (inspector instanceof StringObjectInspector) {
                            stmt.setString(i + 1,
                                    ((StringObjectInspector) inspector).getPrimitiveJavaObject(obj));
                        }
                        break;
                    case "DateTime":
                        stmt.setTimestamp(i + 1,
                                ((TimestampObjectInspector) structField.getFieldObjectInspector()).getPrimitiveJavaObject(obj));
                        break;
                    case "Date":
                        stmt.setDate(i + 1, ((DateObjectInspector) structField.getFieldObjectInspector()).getPrimitiveJavaObject(obj));
                        break;
                    default:
                        throw new SQLException(String.format("Un-supported type %s", columnType));
                }
            } else {
                switch (columnType) {
                    case "Int8":
                    case "UInt8":
                        stmt.setByte(i + 1, (byte) 0);
                        break;
                    case "Int16":
                    case "UInt16":
                        stmt.setShort(i + 1, (byte) 0);
                        break;
                    case "Int32":
                    case "UInt32":
                        stmt.setInt(i + 1, 0);
                        break;
                    case "Int64":
                    case "UInt64":
                        stmt.setLong(i + 1, 0L);
                        break;
                    case "Float32":
                        stmt.setFloat(i + 1, 0.0f);
                        break;
                    case "Float64":
                        stmt.setDouble(i + 1, 0.0);
                        break;
                    case "String":
                        stmt.setString(i + 1, "");
                        break;
                    case "DateTime":
                        stmt.setTimestamp(i + 1, new Timestamp(DateTime.now().getMillis()));
                        break;
                    case "Date":
                        stmt.setDate(i + 1, new Date(DateTime.now().getMillis()));
                        break;
                    default:
                        throw new SQLException(String.format("Un-supported type %s", columnType));
                }
            }

        }
        stmt.addBatch();
    }

    public void flush(int retry, Exception exception) throws IOException {
        if (data.isEmpty()) {
            return;
        }
        if (retry == 0) {
            logger.error("No more retry, failed!");
            if (exception != null) {
                throw new IOException(exception);
            } else {
                throw new IOException("Flush error!");
            }
        }
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = clickHouseHelper.getClickHouseConnection();
            statement = connection.prepareStatement(this.insertQuery);

            for(Map value : data) {
                addValuesToBatch(value, statement, columnNames, columnTypes);
            }
            statement.executeBatch();
            data.clear();
        } catch (SQLException e) {
            logger.error("Write error", e);
            flush(retry - 1, e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("Error closing resource", e);
            }
        }
    }

    @Override
    public void write(Writable w) throws IOException {

        ClickHouseWritable ckWritable = (ClickHouseWritable) w;
        data.add(ckWritable.getValue());

        if (data.size() >= batchSize) {
            flush(3, null);
        }

    }

    @Override
    public void close(boolean abort) throws IOException {
        flush(3, null);
    }
}
