package data.bytedance.net.ck.hive;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class ClickHouseOutputFormat<V> implements HiveOutputFormat<ClickHouseWritable, V> {
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Path path,
            Class<? extends Writable> aClass,
            boolean b,
            Properties properties,
            Progressable progressable
    ) throws IOException {
        return null;
    }

    @Override
    public RecordWriter<ClickHouseWritable, V> getRecordWriter(
            FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable
    ) throws IOException {
        return null;
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

    }
}
