package data.bytedance.net.ck.hive;

public class Constants {
    // config key for the column names
    public static final String LIST_COLUMNS = "columns";
    // the type of each column
    public static final String CK_CONN_STRS = "clickhouse.conn.urls";
    public static final String CK_TBL_NAME = "clickhouse.table.name";
    public static final String CK_BATCH_SIZE = "clickhouse.insert.batch.size";
    public static final int DEFAULT_BATCH_SIZE = 500;
}
