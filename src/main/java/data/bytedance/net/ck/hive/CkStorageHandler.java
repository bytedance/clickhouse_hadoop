package data.bytedance.net.ck.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;

import java.util.Map;

public class CkStorageHandler implements HiveStorageHandler
{
  @Override
  public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass()
  {
    return null;
  }

  @Override
  public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass()
  {
    return null;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass()
  {
    return CkSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook()
  {
    return new CkHook();
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException
  {
    return null;
  }

  @Override
  public void configureInputJobProperties(
      TableDesc tableDesc, Map<String, String> map
  )
  {

  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map)
  {

  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map)
  {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, org.apache.hadoop.mapred.JobConf jobConf)
  {

  }

  @Override
  public void setConf(Configuration configuration)
  {

  }

  @Override
  public Configuration getConf()
  {
    return null;
  }

  /**
   * Dummy implementation, do nothing
   */
  private static class CkHook implements HiveMetaHook
  {
    @Override
    public void preCreateTable(Table table) throws MetaException
    {

    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException
    {

    }

    @Override
    public void commitCreateTable(Table table) throws MetaException
    {

    }

    @Override
    public void preDropTable(Table table) throws MetaException
    {

    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException
    {

    }

    @Override
    public void commitDropTable(Table table, boolean b) throws MetaException
    {

    }
  }


}

