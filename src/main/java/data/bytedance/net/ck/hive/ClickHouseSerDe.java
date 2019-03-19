package data.bytedance.net.ck.hive;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import data.bytedance.net.utils.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.*;

public class ClickHouseSerDe extends AbstractSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSerDe.class);
    private ObjectInspector inspector;

    // The column and type mapping
    private List<String> columnNames;

    public List<String> getColumnNames() {
        return columnNames;
    }


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
        // a list of connection strings separated by comma, for load balancing
        String ckConnectionStrings = tblProps.getProperty(Constants.CK_CONN_STRS);
        String tblName = tblProps.getProperty(Constants.CK_TBL_NAME);

        // Table name and connection string are required
        if (ckConnectionStrings == null || ckConnectionStrings == "") {
            throw new SerDeException(Constants.CK_CONN_STRS + " must be set in TBLPROPERTIES");
        }

        if (tblName == null || tblName == "") {
            throw new SerDeException(Constants.CK_TBL_NAME + " must be set in TBLPROPERTIES");
        }


        String columnNameProperty = tblProps.getProperty(serdeConstants.LIST_COLUMNS);
        logger.info("Column Names: " + columnNameProperty);
        List<String> columnTypes;
        ClickHouseHelper helper;
        try {
            helper = ClickHouseHelper.getClickHouseHelper(ckConnectionStrings, tblName);
        } catch (SQLException e) {
            throw new SerDeException(e.getCause());
        }

        // if columns and column types are not explicitly defined, we need to find them out from clickhouse schema
        if (columnNameProperty != null || columnNameProperty != "") {
            columnNames = Arrays.asList(columnNameProperty.split(","));
            columnTypes = new ArrayList<>();
            Map<String, String> typeMap = helper.getNameTypeMap();
            for (String n : columnNames) {
                columnTypes.add(typeMap.get(n));
            }
        } else {
            columnNames = helper.getColumnNames();
            columnTypes = helper.getColumnTypes();
        }
        List<ObjectInspector> inspectors = new ArrayList<>();

        inspectors.addAll(Lists.transform(columnTypes, new Function<String, ObjectInspector>() {
            @Nullable
            @Override
            public ObjectInspector apply(@Nullable String s) {
                return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                        TypeInfoFactory.getPrimitiveTypeInfo(ClickHouseHelper.ClickHouseToHiveType(s)));
            }
        }));
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ClickHouseWritable.class;
    }

    /**
     * This method takes an object representing a row of data from Hive, and uses the ObjectInspector
     * to get the data for each data and serialize it into Writable -- a serializable object
     *
     * @param o
     * @param objectInspector
     * @return
     * @throws SerDeException
     */
    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(
                    getClass().toString() + " can only serialize struct types, but we got: " + objectInspector.getTypeName());
        }

        StructObjectInspector soi = (StructObjectInspector) objectInspector;

        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(o);

        HashMap<String, Tuple<? extends StructField, Object>> value = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            StructField ref = fields.get(i);
            Object data = values.get(i);
            value.put(columnNames.get(i), new Tuple<>(ref, data));
        }
        return new ClickHouseWritable(value);
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

    /**
     * Get the object inspector that can be used to navigate through the internal structure
     * of the Object returned from deserialize(...). Not supported for this case
     *
     * @return
     * @throws SerDeException
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }
}
