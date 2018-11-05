package data.bytedance.net.ck.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ClickHouseSerDe implements SerDe {
    private int columnCount;
    private StructObjectInspector objectInspector;
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSerDe.class);
    private List<String> columnNames;
    private List<String> columnTypes;


    /**
     * Set up the tlbProps of the destination
     * - columns
     * - column.types
     *
     * @param configuration
     * @param tblProps      the properties defined in the TBLPROPERTIES clause of the CREATE TABLE statement
     * @throws SerDeException
     */
    @Override
    public void initialize(Configuration configuration, Properties tblProps) throws SerDeException {
        if (logger.isDebugEnabled()) {
            logger.debug("tblProps" + tblProps);
        }
        String connStr = tblProps.getProperty(Constants.CK_CONN_STR);
        String tblName = tblProps.getProperty(Constants.CK_TBL_NAME);

        // Table name and connection string are required
        if (connStr == null || connStr == "") {
            throw new SerDeException(Constants.CK_CONN_STR + " must be set in TBLPROPERTIES");
        }

        if (tblName == null || tblName == "") {
            throw new SerDeException(Constants.CK_TBL_NAME + " must be set in TBLPROPERTIES");
        }


        String columnNameProperty = tblProps.getProperty(Constants.LIST_COLUMNS);
        ClickHouseHelper helper;
        try {
            helper = ClickHouseHelper.getClickHouseHelper(connStr, tblName);
        } catch (SQLException e) {
            throw new SerDeException(e.getCause());
        }

        // if columns and column types are not explicitly defined, we need to find them out.
        if (columnNameProperty != null || columnNameProperty != "") {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        } else {
            columnNames = helper.getColumnNames();
            columnTypes = helper.getColumnTypes();
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ClickHouseWritable.class;
    }

    /**
     * This method takes an object representing a row of data from Hive, and uses the ObjectInspector
     * to get the data for each data and serialize
     *
     * @param o
     * @param objectInspector
     * @return
     * @throws SerDeException
     */
    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        return null;
    }

    /**
     * This method does the work of deserializing a record into Java objects
     * that hive can work with via the ObjectInspector interface
     * We don't support querying at this moment, thus this method will throw an exception
     *
     * @param writable
     * @return
     * @throws SerDeException
     */
    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        throw new UnsupportedOperationException("Reads are not allowed");
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return null;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
