package data.bytedance.net.ck.hive;

import junit.framework.Assert;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

class CkHelperTest {
    @Test
    void testInitColumnsFromCK() throws SQLException, ClassNotFoundException {
        Class.forName("com.github.housepower.jdbc.ClickHouseDriver");
        Connection conn = DriverManager.getConnection(TestHelper.getTestCkConnStr());
        try {
            Statement stmt = conn.createStatement();
            stmt.executeQuery("drop table if exists test_ck_helper;");
            stmt.executeQuery("create table test_ck_helper(day default toDate( toDateTime(timestamp) ), timestamp UInt32, name String, impressions UInt32) Engine=MergeTree(day, (timestamp, name), 8192)");
            CkHelper helper = new CkHelper(TestHelper.getTestCkConnStr(), "test_ck_helper");
            List<String> columnNames = helper.getColumnNames();
            List<String> columnTypes = helper.getColumnTypes();
            Assert.assertEquals(4, columnNames.size());
            Assert.assertEquals(4, columnTypes.size());
            Assert.assertEquals("day", columnNames.get(0));
            Assert.assertEquals("Date", columnTypes.get(0));
            Assert.assertEquals("timestamp", columnNames.get(1));
            Assert.assertEquals("UInt32", columnTypes.get(1));
            Assert.assertEquals("name", columnNames.get(2));
            Assert.assertEquals("String", columnTypes.get(2));
            Assert.assertEquals("impressions", columnNames.get(3));
            Assert.assertEquals("UInt32", columnTypes.get(3));
            stmt.executeQuery("drop table if exists test_ck_helper;");

        } finally {
            conn.close();
        }

    }
}