package data.bytedance.net.ck.hive;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestHelper {
    public static String ckConnStr = "jdbc:clickhouse://172.31.0.18:9000";

    public static ClickHouseWritable serializeObject(ClickHouseSerDe serDe,
                                                     Object[] rowObject, String types)  throws SerDeException {
        List<String> columnNames = serDe.getColumnNames();
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(types);
        List<ObjectInspector> inspectors = new ArrayList<>();
        inspectors.addAll(Lists.transform(colTypes,
                (Function<TypeInfo, ObjectInspector>) type -> PrimitiveObjectInspectorFactory
                        .getPrimitiveWritableObjectInspector(TypeInfoFactory.getPrimitiveTypeInfo(type.getTypeName()))));
        ObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

        ClickHouseWritable writable = (ClickHouseWritable) serDe.serialize(rowObject, inspector);
        return writable;
    }

    public static Properties createPropertiesSource(String tableName, String columnNames) {
        Properties tbl = new Properties();
        tbl.setProperty(Constants.LIST_COLUMNS, columnNames);
        tbl.setProperty(Constants.CK_CONN_STRS, TestHelper.ckConnStr);
        tbl.setProperty(Constants.CK_TBL_NAME, tableName);
        return tbl;
    }
}
