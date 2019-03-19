package data.bytedance.net.ck.hive;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;

public class ClickHouseOutputFormat<V> implements HiveOutputFormat<ClickHouseWritable, V> {
    private static Logger logger = LoggerFactory.getLogger(ClickHouseOutputFormat.class);

    @Override
    public RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path,
            Class<? extends Writable> aClass,
            boolean b,
            Properties tblProps,
            Progressable progressable
    ) throws IOException {
        logger.info("Table Properties");
        Set<Entry<Object, Object>> entries = tblProps.entrySet();
        for (Entry<Object, Object> entry: entries) {
            logger.info(entry.getKey() + " : " + entry.getValue());
        }

        String ckConnectionStrings = tblProps.getProperty(Constants.CK_CONN_STRS);
        String tblName = tblProps.getProperty(Constants.CK_TBL_NAME);
        // Table name and connection string are required
        if (ckConnectionStrings == null || ckConnectionStrings == "") {
            throw new IOException(Constants.CK_CONN_STRS + " must be set in TBLPROPERTIES");
        }

        if (tblName == null || tblName == "") {
            throw new IOException(Constants.CK_TBL_NAME + " must be set in TBLPROPERTIES");
        }

        String batchSizeStr = tblProps.getProperty(Constants.CK_BATCH_SIZE);
        int batchSize = 0;
        try {
            if (batchSizeStr == null || batchSizeStr == "") {
                batchSize = Constants.DEFAULT_BATCH_SIZE;
            } else {
                batchSize = Integer.parseInt(batchSizeStr);
            }
        } catch (NumberFormatException e) {
            logger.info(String.format("Parsing %s failed, use default", batchSizeStr), e);
            batchSize = Constants.DEFAULT_BATCH_SIZE;
        }

        ClickHouseHelper ckHelper;
        try {
            ckHelper = ClickHouseHelper.getClickHouseHelper(ckConnectionStrings, tblName);
        } catch (SQLException e) {
            logger.error("Can't create ckHelper ", e);
            throw new IOException(e);
        }
        List<String> columnNames = ckHelper.getColumnNames();
        List<String> columnTypes = ckHelper.getColumnTypes();
        return new ClickHouseRecordWriter(ckHelper, batchSize, tblName, columnNames, columnTypes);
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<ClickHouseWritable, V> getRecordWriter(
            FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable
    ) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        // hive should not invoke this method
    }
}
