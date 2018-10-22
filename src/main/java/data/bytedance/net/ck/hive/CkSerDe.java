package data.bytedance.net.ck.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

public class CkSerDe implements SerDe
{
  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException
  {

  }

  @Override
  public Class<? extends Writable> getSerializedClass()
  {
    return null;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
  {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException
  {
    throw new UnsupportedOperationException("Reads are not allowed");
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException
  {
    return null;
  }

  @Override
  public SerDeStats getSerDeStats()
  {
    return null;
  }
}
