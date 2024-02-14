---
title: Scala
tags:
  - Spark
  - Scala
  - API
  - OpenHouse
---

##  Data Definition Language (DDL)
For DDLs such as `CREATE TABLE`, `ALTER TABLE`, `GRANT/REVOKE` etc; use `.sql()` in SparkSession. Native Scala APIs for achieving those outcomes aren't available currently.

```scala
-- Create Table
spark.sql("CREATE TABLE openhouse.db.table (id bigint COMMENT 'unique id', data string)")

-- Alter Table
spark.sql("""ALTER TABLE openhouse.db.table SET TBLPROPERTIES (
  'key1' = 'value1',
  'key2' = 'value2'
)""")

-- Grant Select
spark.sql("GRANT SELECT ON TABLE openhouse.db.table TO <user>;")
```

[See all available DDL commands here.](/docs/APIs/Catalog/SQL#data-definition-language-ddl)

## Reads
To query a table, run the following:

```scala
val df = spark.table("openhouse.db.table")
```

You can also filter data using custom filters as follows:
```scala
val filtered_df = df.filter(col("datepartition") > "2022-05-10")
```

### With Time-Travel
Identify older snapshot you want to read, by [Inspecting Metadata](/docs/APIs/Catalog/SQL#inspecting-metadata). Then run the following: 
```scala
spark.read.option("snapshot-id", 1031031135387001532L).table("openhouse.db.table")
```

## Writes
We highly recommend users adopt Apache Spark’s new DataFrameWriterV2 API in Spark 3 when programming with DataFrame API.

The following are example Scala statements in Spark 3 to write to a partitioned table.

### Create Table
```scala
import org.apache.spark.sql.functions;

// Your code preparing DataFrame
 
df.writeTo("openhouse.db.table").create()
```

### Create Partitioned Table
```scala
import org.apache.spark.sql.functions;
  
// Your code to create a table through existing data frame.
 
df.sortWithinPartitions("datepartition")
.writeTo("openhouse.db.table")
.partitionedBy(functions.col("datepartition"))
.create()
```

### Append Data to Partitioned Table
```scala
append_df.sortWithinPartitions("datepartition")
.writeTo("openhouse.db.table")
.append()
```

### Overwrite Data
```scala
// You can dynamically overwrite partitions, which means
// any partitions with at least one row matched will be overwritten.
// To make overwritePartitions work for table-overwrite:
 
overwrite_df.sortWithinPartitions("datepartition")
.writeTo("openhouse.db.table")
.overwritePartitions()
 
 
// To explicitly overwrite, use the following
overwrite_df.sortWithinPartitions("datepartition")
.writeTo("openhouse.db.table")
.overwrite($"level" === "INFO")
```

:::note
Note that explicit sort is necessary in partition-write because Spark doesn’t allow Iceberg to request a sort before writing as of Spark 3.0. [See more at link](https://iceberg.apache.org/docs/latest/spark-writes/#writing-to-partitioned-tables). 
:::