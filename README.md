# ClickHouse Hadoop

Integrate ClickHouse natively with Hive, currently only writing is supported. Connecting Hadoop's massive data storage and deep processing power with the high performance of ClickHouse. 

## Build the Project

```
mvn package -Phadoop -DskipTests
```

## Run the test cases

It is required that a clickhouse-server is running in the localhost to correctly run the test cases.
