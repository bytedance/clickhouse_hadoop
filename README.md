# ClickHouse Hadoop

Integrate ClickHouse natively with Hive, currently only writing is supported. Connecting Hadoop's massive data storage and deep processing power with the high performance of ClickHouse. 

## Build the Project

```bash
mvn package -Phadoop26 -DskipTests
```

## Run the test cases

It is required that a clickhouse-server is running in the localhost to correctly run the test cases.


## Usage


### Create ClickHouse table

```sql
CREATE TABLE hive_test
(
    c1 String,
    c2 Float64,
    c3 String
)
ENGINE = MergeTree()
PARTITION BY c3
ORDER BY c1
```

### Create Hive External Table

Before starting the hive cli, set the environment variable `HIVE_AUX_JARS_PATH`

```bash
export HIVE_AUX_JARS_PATH=<path-to-your-project>/target/clickhouse-hadoop-<version>.jar
```

Then start the `hive-cli` and create Hive external table:

```sql
CREATE EXTERNAL TABLE default.ck_test(
   c1 string,
   c2 double,
   c3 string
)
STORED BY 'data.bytedance.net.ck.hive.ClickHouseStorageHandler'
TBLPROPERTIES('clickhouse.conn.urls'='jdbc:clickhouse://<host-1>:<port1>,jdbc:clickhouse://<host2>:<port2>',
'clickhouse.table.name'='hive_test');
```

### Data Ingestion

In `hive-cli`

```sql
INSERT INTO default.ck_test
select  c1, c2, c3 FROM default.source_table where part='part_val'
```

