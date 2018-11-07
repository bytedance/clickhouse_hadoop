package data.bytedance.net.ck.hive;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ClickHouseRecordWriter implements RecordWriter {
    private final int batchSize;
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final String insertQuery;

    public ClickHouseRecordWriter(ClickHouseHelper helper, int batchSize, String tableName,
                                  List<String> columnNames, List<String> columnTypes) {
        this.batchSize = batchSize;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.insertQuery = constructInsertQuery(tableName, columnNames);
    }

    public static String constructInsertQuery(String tableName, List<String> columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(tableName);

        String fields = String.join(",", columnNames);
        String[] valueSlice = new String[columnNames.size()];
        Arrays.fill(valueSlice, "?");
        String values = String.join(",", valueSlice);
        sql.append("(").append(fields).append(")").append("VALUES (")
                .append(values).append(values).append(")");
        return sql.toString();
    }

    @Override
    public void write(Writable w) throws IOException {

    }

    @Override
    public void close(boolean abort) throws IOException {

    }
}
