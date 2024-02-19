---
title: SQL
tags:
  - Spark
  - SQL
  - API
  - OpenHouse
---

## Data Definition Language (DDL)

### CREATE TABLE
To create an OpenHouse table, run following SQL in Spark.

```sql
CREATE TABLE openhouse.db.table (id bigint COMMENT 'unique id', data string);
```

OpenHouse supports following Create clauses:
- `PARTITIONED BY`
- `TBLPROPERTIES (‘key’=’value’,..)`

OpenHouse does not support following Create clauses:
- `LOCATION (*)`
- `CLUSTERED BY (*)`
- `COMMENT ‘table documentation’`

List of supported DataTypes can be seen in [Iceberg Spark Types](https://iceberg.apache.org/docs/latest/spark-writes/#spark-type-to-iceberg-type).

#### PARTITIONED BY
OpenHouse supports single timestamp column to be specified in the partitioning scheme. It also supports upto three string or integer type column based partitioning scheme.

To partition your table, you can use the following SQL syntax
```sql
CREATE TABLE openhouse.db.table(datepartition string, epoch_ts timestamp)
PARTITIONED BY (
    days(epoch_ts),
    datepartition
    )
```
- `days(epoch_ts)`: partitions data by applying day-granularity on the timestamp-type column "epoch_ts". 

Other granularities supported are:
- `years(epoch_ts)`: partition by year
- `months(epoch_ts)`: partition by month
- `hours(epoch_ts)`: partition by hour

You can also partition your data based on string column by using identity partitioning (for example: `datepartition`).

Constraints: 
- Other iceberg transforms such as bucket, truncate are not supported on timestamp column. 
- No transformation is supported  on string or integer type partition column. Only timestamp-type column allows transformation.

#### TBLPROPERTIES
```sql
CREATE TABLE openhouse.db.table(
    data string
)
TBLPROPERTIES (
    'key'='value',
    ...
);
```
:::warning
Keys with the prefix “openhouse.” (for example: “openhouse.tableId”) are preserved and cannot be set/modified. 
Additionally, all Iceberg [TableProperties](https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/TableProperties.java) are also preserved.
:::

#### CREATE TABLE AS SELECT (CTAS)
To create an OpenHouse table with some data, run following SQL in Spark.
```sql
CREATE TABLE openhouse.db.table
AS
SELECT * FROM anyCatalog.srcDb.srcTable WHERE data = 'v1';
```

:::tip
`Create table like tableName` is not supported. You can use `create table A as select * from B limit 0`  to achieve same effect
:::

### REFRESH TABLE
Upon reading a table, its state is cached in your session. In order to read the new state of the table: 

```sql
REFRESH TABLE openhouse.db.table;
```

### DROP TABLE
To delete table’s data and metadata, run: 
```sql
DROP TABLE openhouse.db.table;
```

### ALTER TABLE
OpenHouse supports following ALTER statements
- Setting or removing table properties.
- Schema Evolution:
  - Adding columns in both top and nested level.
  - Widening the type of int, float, and decimal fields.
  - Making required columns optional. 

OpenHouse doesn’t support for now
- Schema evolution: Drop, rename and reordering.
- Renaming a table.
- Adding, removing, and changing partitioning.
- Other iceberg alters such as: `write ordered by` / `write distributed by`

#### ALTER TABLE ... SET TBLPROPERTIES
To set table properties

```sql
ALTER TABLE openhouse.db.table SET TBLPROPERTIES (
  'key1' = 'value1',
  'key2' = 'value2'
)
```
To unset table properties
```sql
ALTER TABLE openhouse.db.table UNSET TBLPROPERTIES ('key1', 'key2')
```
:::warning
Keys with the prefix “openhouse.” (for example: “openhouse.tableId”) are preserved and cannot be set/modified. 
Additionally, all Iceberg [TableProperties](https://github.com/apache/iceberg/blob/master/core/src/main/java/org/apache/iceberg/TableProperties.java) are also preserved.
:::

#### ALTER TABLE ... ADD COLUMN
Adding column is a supported schema evolution, to add a new column:
```sql
ALTER TABLE openhouse.db.table
ADD COLUMNS (
    new_column string comment 'new_column docs'
)
```
Multiple columns can be added separated by comma.
Nested columns can be added as follows:

```sql
-- create a struct column
ALTER TABLE openhouse.db.table
ADD COLUMN point struct<x: double, y: double>;
 
-- add a field to the struct
ALTER TABLE openhouse.db.table
ADD COLUMN point.z double
```

```sql
-- create a nested array column of struct
ALTER TABLE openhouse.db.table
ADD COLUMN points array<struct<x: double, y: double>>;
 
-- add a field to the struct within an array. Using keyword 'element' to access the array's element column.
ALTER TABLE openhouse.db.table
ADD COLUMN points.element.z double
```
```sql
-- create a map column of struct key and struct value
ALTER TABLE openhouse.db.table
ADD COLUMN point_map map<struct<x: int>, struct<a: int>>;
 
-- add a field to the value struct in a map. Using keyword 'value' to access the map's value column.
ALTER TABLE openhouse.db.table
ADD COLUMN point_map.value.b int
 
-- Altering 'key' struct is not allowed. Only map 'value' can be updated.
```

#### ALTER TABLE ... ALTER COLUMN
Alter column is used for:
1) Type widening, OpenHouse supports `int to bigint`, `float to double`
  ```sql
  ALTER TABLE openhouse.db.table ALTER COLUMN measurement TYPE double;
  ```
2) Setting column comments

  ```sql
  ALTER TABLE openhouse.db.table ALTER COLUMN measurement COMMENT 'unit is kilobytes per second';
  ```
3) Make field optional
 
 ```sql
 ALTER TABLE openhouse.db.table ALTER COLUMN id DROP NOT NULL;
 ```

### GRANT/ REVOKE
Tables and databases in OpenHouse are access controlled by its own RBAC system, to perform any operation you need to have the right privilege. 

#### ALTER TABLE SET POLICY (SHARING=)
To make your table sharable, run the following command:
```sql
ALTER TABLE openhouse.db.table SET POLICY (SHARING=TRUE);
```
:::note
Trying to share a table without running this command will throw the error: `db.table` is not a shared table 
:::

#### GRANT/ REVOKE SELECT ON TABLE
As a TABLE_ADMIN you can grant another user TABLE_VIEWER role by running the SQL:
```sql
GRANT SELECT ON TABLE openhouse.db.table TO <user>;
```

To revoke the privilege:
```sql
REVOKE SELECT ON TABLE openhouse.db.table FROM <user>;
```

#### GRANT/ REVOKE MANAGE GRANTS ON TABLE 
As a ACL_EDITOR / TABLE_ADMIN, In order to grant sharing rights on your table to other users, you can make them ACL_EDITOR by running the SQL:
```sql 
GRANT MANAGE GRANTS ON TABLE openhouse.db.table TO <user>;
```

To revoke:
```sql
REVOKE MANAGE GRANTS ON TABLE openhouse.db.table FROM <user>;
```

#### GRANT/REVOKE CREATE TABLE ON DATABASE
As a ACL_EDITOR role of database you can grant TABLE_CREATOR role to whoever wants to create a table in your database.

```sql
GRANT CREATE TABLE ON DATABASE openhouse.db TO <user>;
```

To revoke:
```sql
REVOKE CREATE TABLE ON DATABASE openhouse.db FROM <user>;
```

#### SHOW GRANTS ON TABLE/DATABASE
In order to view granted privileges:

For Table:
```sql
SHOW GRANTS ON TABLE openhouse.db.table;
```

For Database:
```sql
SHOW GRANTS ON DATABASE openhouse.db;
```

## Reads
### SELECT FROM
OpenHouse supports ANSI SQL for SELECT statements. For complete syntax see [Spark SELECT syntax](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select.html).

```sql
SELECT * FROM openhouse.db.table;
```
:::note
Note: response of the table is cached for the duration of the session. In order to refresh the cache, please run
`REFRESH TABLE`
:::

### SELECT FROM w/ Time-Travel
OpenHouse uses Iceberg as the table format. Iceberg generates a version/snapshot for every update to the table. A snapshot captures the state of the table at a specific point in time. We can query iceberg tables using the snapshot ID or timestamp from the past. Unlike Hive, Iceberg guarantees query reproducibility when querying historical data.

Time-travel is supported through following syntax.
```sql
-- suffix with "at_timestamp" string and epoch time in milliseconds.
SELECT * FROM openhouse.db.table.at_timestamp_1686699440000;
 
-- standard sql dialect for this time-travel is NOT supported
SELECT * FROM openhouse.db.table FOR SYSTEM_TIME AS OF 1686699440000;
```

Querying a older version of a table is supported through following syntax.
```sql
-- suffix with "snapshot_id" string followed by the ID of snapshot or version.
SELECT * FROM openhouse.db.table.snapshot_id_821787972099903997;
 
-- standard sql dialect for this version query is NOT supported
SELECT * FROM openhouse.db.table FOR SYSTEM_VERSION AS OF 821787972099903997;
```

## Writes
### INSERT INTO
OpenHouse supports standard ANSI SQL for INSERT statements.
```sql
INSERT INTO openhouse.db.table VALUES (1, 'a'), (2, 'b');
```

### DELETE FROM
Where clause is used to delete rows
```sql
DELETE FROM openhouse.db.table WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00';
```

### INSERT OVERWRITE
Insert overwrite replaces the partitions in the target table that contains rows from the SELECT query. 

```sql
INSERT OVERWRITE openhouse.db.table SELECT uuid, ts
FROM anyCatalog.db.another_table
WHERE cast(ts as date) = '2020-07-01'
```

:::danger
If the table is unpartitioned the whole table data will be replaced.
:::

### UPDATE
You can use `update` clause to update specific rows.
```sql
UPDATE openhouse.db.table SET data = 'updated_data'
WHERE ts >= '2020-05-01 00:00:00' and ts < '2020-06-01 00:00:00'
```

### MERGE INTO
Merge into is used to update target table (ex. db.target) based on source query (SELECT ..) 

```sql
MERGE INTO openhouse.db.table t
USING (SELECT ...) s
ON t.id = s.id
WHEN MATCHED .. THEN..
```

The merge condition `WHEN  MATCHED .. THEN ..` determines whether update/delete/insert would happen
```sql
-- delete
WHEN MATCHED AND s.op = 'delete' THEN DELETE
-- update
WHEN MATCHED AND t.count IS NULL AND s.op = 'increment' THEN UPDATE SET t.count = 0
-- insert
WHEN NOT MATCHED THEN INSERT *
```

`THEN INSERT` also supports additional conditions
```sql
WHEN NOT MATCHED AND s.event_time > still_valid_threshold THEN INSERT (id, count) VALUES (s.id, 1)
```
Complete syntax can be seen at [Databricks Merge Into](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-merge-into.html).

## Inspecting Metadata
### List Partitions
`show partitions db.table` does not work for OpenHouse tables. You can do the same thing with different SQL syntax like below:
```sql
 SELECT * FROM openhouse.db.table.partitions;
 ```

### List Snapshots
 Each update to the table will generate a new snapshot. Browsing snapshots is a critical feature to enable time-travel queries with snapshhot-id and commit-timestamp. 
```sql
SELECT * FROM openhouse.db.table.snapshots
```

## Unsupported Operations
Following SQL commands are unsupported
```sql
CREATE TABLE LIKE

-- Renaming Fields
ALTER TABLE openhouse.db.table RENAME COLUMN data TO payload
ALTER TABLE openhouse.db.table RENAME COLUMN point.x TO x_axis
 
-- Setting comments and type-widening at the same time
ALTER TABLE openhouse.db.table ALTER COLUMN measurement TYPE double COMMENT 'unit is bytes per second'
 
-- Reorder
ALTER TABLE openhouse.db.table ALTER COLUMN col FIRST
ALTER TABLE openhouse.db.sample ALTER COLUMN nested.col AFTER other_col
 
-- Drop
ALTER TABLE openhouse.db.table DROP COLUMN id
ALTER TABLE openhouse.db.table DROP COLUMN point.z
 
-- Other commands that are not supported yet
ALTER TABLE .. RENAME TO
ALTER TABLE ... ADD PARTITION FIELD
ALTER TABLE ... DROP PARTITION FIELD
ALTER TABLE ... REPLACE PARTITION FIELD
ALTER TABLE ... WRITE ORDERED BY
ALTER TABLE ... WRITE DISTRIBUTED BY PARTITION
```