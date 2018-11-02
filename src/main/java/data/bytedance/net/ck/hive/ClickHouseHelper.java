package data.bytedance.net.ck.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseHelper {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseHelper.class);
    private final String connStr;
    private final String tableName;
    private List<String> columnNames;
    private List<String> columnTypes;

    static {
        try {
            Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            logger.error("Can't find suitable driver", e);
        }
    }


    public ClickHouseHelper(String connStr, String tableName) throws SQLException {
        this.connStr = connStr;
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        initColumnNamesAndTypesFromSystemQuery();
    }

    public ClickHouseHelper(String connStr, String tableName, List<String> columnNames, List<String> columnTypes) {
        this.connStr = connStr;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    public Connection getClickHouseConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(getConnStr());
        return connection;
    }


    public void initColumnNamesAndTypesFromSystemQuery() throws SQLException {
        Connection conn = getClickHouseConnection();
        try {
            Statement stmt = conn.createStatement();
            String query = "SELECT name, type from system.columns where table = '" + getTableName() + "';";
            logger.info("Initializing columns and types with " + query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                this.columnNames.add(rs.getString(1));
                this.columnTypes.add(rs.getString(2));
            }
        } finally {
            conn.close();
        }
    }

    public String getConnStr() {
        return connStr;
    }

    public String getTableName() {
        return tableName;
    }


    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }
}
