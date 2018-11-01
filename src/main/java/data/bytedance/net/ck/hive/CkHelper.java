package data.bytedance.net.ck.hive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CkHelper
{
  private static final Logger logger = LoggerFactory.getLogger(CkHelper.class);
  private final String connStr;
  private final String tableName;
  private List<String> columnNames;
  private List<String> columnTypes;

  public CkHelper(String connStr, String tableName)
  {
    this.connStr = connStr;
    this.tableName = tableName;
    initColumnNamesAndTypesFromCk();
  }

  public CkHelper(String connStr, String tableName, List<String> columnNames, List<String> columnTypes)
  {
    this.connStr = connStr;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }



  public void initColumnNamesAndTypesFromCk()
  {

  }

  public String getConnStr()
  {
    return connStr;
  }

  public String getTableName()
  {
    return tableName;
  }

  public List<String> getColumnNames()
  {
    return columnNames;
  }

  public List<String> getColumnTypes()
  {
    return columnTypes;
  }
}
