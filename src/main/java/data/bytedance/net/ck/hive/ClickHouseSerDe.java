package data.bytedance.net.ck.hive;

import com.google.common.collect.Lists;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ClickHouseSerDe extends AbstractSerDe {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSerDe.class);
    private ObjectInspector inspector;

    // The column and type mapping
    private String[] columns;
    private PrimitiveTypeInfo[] types;

    /**
     * Set up the tlbProps of the destination - columns - column.types
     *
     * @param configuration
     * @param tblProps      the properties defined in the TBLPROPERTIES clause of
     *                      the CREATE TABLE statement
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
        String columnTypeProperty = tblProps.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        logger.info("Column Names: " + columnNameProperty);
        logger.info("Column Types: " + columnTypeProperty);

        // if columns and column types are not explicitly defined, we need to find them
        // out from clickhouse schema
        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
        List<String> columnTypeNames = Arrays.asList(columnTypeProperty.split(":"));
        List<PrimitiveTypeInfo> columnTypes = Lists.transform(columnTypeNames,
                (String s) -> TypeInfoFactory.getPrimitiveTypeInfo(s));
        List<ObjectInspector> inspectors = new ArrayList<>();

        columns = columnNames.toArray(new String[columnNames.size()]);
        types = columnTypes.toArray(new PrimitiveTypeInfo[columnTypes.size()]);

        inspectors.addAll(Lists.transform(columnTypes,
                (PrimitiveTypeInfo type) -> PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(type)));
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return ClickHouseWritable.class;
    }

    /**
     * This method takes an object representing a row of data from Hive, and uses
     * the ObjectInspector to get the data for each data and serialize it into
     * Writable -- a serializable object
     *
     * @param o
     * @param objectInspector
     * @return
     * @throws SerDeException
     */
    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        if (objectInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString() + " can only serialize struct types, but we got: "
                    + objectInspector.getTypeName());
        }

        StructObjectInspector soi = (StructObjectInspector) objectInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> values = soi.getStructFieldsDataAsList(o);

        final Map<String, Object> value = new HashMap<>();
        for (int i = 0; i < columns.length; i++) {
            if (values.get(i) == null) {
                // null, we just add it
                value.put(columns[i], null);
                continue;
            }
            final Object res;
            switch (types[i].getPrimitiveCategory()) {
            case TIMESTAMP:
                res = ((TimestampObjectInspector) fields.get(i).getFieldObjectInspector())
                        .getPrimitiveJavaObject(values.get(i));
                break;
            case BYTE:
                res = ((ByteObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case SHORT:
                res = ((ShortObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case INT:
                res = ((IntObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case LONG:
                res = ((LongObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case FLOAT:
                res = ((FloatObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case DOUBLE:
                res = ((DoubleObjectInspector) fields.get(i).getFieldObjectInspector()).get(values.get(i));
                break;
            case CHAR:
                res = ((HiveCharObjectInspector) fields.get(i).getFieldObjectInspector())
                        .getPrimitiveJavaObject(values.get(i)).getValue();
                break;
            case VARCHAR:
                res = ((HiveVarcharObjectInspector) fields.get(i).getFieldObjectInspector())
                        .getPrimitiveJavaObject(values.get(i)).getValue();
                break;
            case STRING:
                res = ((StringObjectInspector) fields.get(i).getFieldObjectInspector())
                        .getPrimitiveJavaObject(values.get(i));
                break;
            case DATE:
                res = ((DateObjectInspector) fields.get(i).getFieldObjectInspector())
                        .getPrimitiveJavaObject(values.get(i));
                break;
            default:
                throw new SerDeException("Unsupported type: " + types[i].getPrimitiveCategory());
            }
            value.put(columns[i], res);
        }
        return new ClickHouseWritable(value);
    }

    /**
     * This method does the work of deserializing a record into Java objects that
     * hive can work with via the ObjectInspector interface We don't support
     * querying at this moment, thus this method will throw an exception
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
     * Get the object inspector that can be used to navigate through the internal
     * structure of the Object returned from deserialize(...). Not supported for
     * this case
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
