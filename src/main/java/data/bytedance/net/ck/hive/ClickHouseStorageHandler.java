package data.bytedance.net.ck.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;

import java.util.Map;
import java.util.Properties;

public class ClickHouseStorageHandler implements HiveStorageHandler {
    private Configuration conf;
    @Override
    public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
        return ClickHouseInputFormat.class;
    }

    @Override
    public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass() {
        return ClickHouseOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return ClickHouseSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new CkHook();
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jopProperties) {
        Properties tableProps = tableDesc.getProperties();
        for (String key: tableProps.stringPropertyNames()) {
            if (conf == null || conf.get(key) == null) {
                jopProperties.put(key, tableProps.getProperty(key));
            }
        }
    }

    @Override
    public void configureJobConf(TableDesc tableDesc, org.apache.hadoop.mapred.JobConf jobConf) {
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Dummy implementation, do nothing
     */
    private static class CkHook implements HiveMetaHook {
        @Override
        public void preCreateTable(Table table) throws MetaException {

        }

        @Override
        public void rollbackCreateTable(Table table) throws MetaException {

        }

        @Override
        public void commitCreateTable(Table table) throws MetaException {

        }

        @Override
        public void preDropTable(Table table) throws MetaException {

        }

        @Override
        public void rollbackDropTable(Table table) throws MetaException {

        }

        @Override
        public void commitDropTable(Table table, boolean b) throws MetaException {

        }
    }


}

