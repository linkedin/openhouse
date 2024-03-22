---
title: Getting Started
tags:
  - Spark
  - Hadoop
  - Docker
  - OpenHouse
  - Iceberg
sidebar_position: 2
---
# OpenHouse on Spark & HDFS

In this guide, we will quickly set up a running environment and experiment with some simple SQL commands. Our
environment will include all the core OpenHouse services such as [Catalog Service](./intro.md#catalog-service),
[House Table service](./intro.md#house-table-service) and [others](./intro.md#control-plane-for-tables),
[a Spark 3.1 engine](https://spark.apache.org/releases/spark-release-3-1-1.html) and
also [HDFS namenode and datanode](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html#NameNode+and+DataNodes).
By the end of this walkthrough, we will have made some tables on OpenHouse, put data in them, retrieved the data
and checked HDFS namenodes to see the data written.

In the consecutive optional section, you can learn more about some simple GRANT REVOKE commands and how
OpenHouse manages access control.

### Prerequisites
- [Docker CLI](https://docs.docker.com/get-docker/)
- [Docker Compose CLI](https://github.com/docker/compose-cli/blob/main/INSTALL.md)

## Create and write to OpenHouse Tables
### Get environment ready
First, clone [OpenHouse github repository](https://github.com/linkedin/openhouse) and
run `./gradlew build` command at the root directory. After the command succeeds you should see `BUILD SUCCESSFUL`
message.

```shell
openhouse$main>  ./gradlew build
```

Then, go to the location `infra/recipes/docker-compose/oh-hadoop-spark` and execute `docker compose up` command.
When you finish, you should see many docker containers running.
```shell
# change directory
openhouse$main>  cd infra/recipes/docker-compose/oh-hadoop-spark

# spin up containers
oh-hadoop-spark$main>  docker compose up –d
[+] Running
✔ Container local.spark-master            Running
✔ Container local.openhouse-housetables   Running
...
```
Voila! Just like that, we have all the core OpenHouse services up alongside Spark and Hadoop.

### Run SQL commands
Let us  execute some basic SQL commands to create table, add data and retrieve the data.

First login to the driver node and start the spark-shell.
```shell
oh-hadoop-spark$main>  docker exec -it local.spark-master /bin/bash

openhouse@0a9ed5853291:/opt/spark$  bin/spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.1_2.12:1.2.0   \
--jars openhouse-spark-runtime_2.12-*-all.jar  \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions   \
--conf spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog   \
--conf spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog     \
--conf spark.sql.catalog.openhouse.metrics-reporter-impl=com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter    \
--conf spark.sql.catalog.openhouse.uri=http://openhouse-tables:8080   \
--conf spark.sql.catalog.openhouse.auth-token=$(cat /var/config/openhouse.token) \
--conf spark.sql.catalog.openhouse.cluster=LocalHadoopCluster
```
:::note
the configuration `spark.sql.catalog.openhouse.uri=http://openhouse-tables:8080` points to the docker container
running the [REST-based catalog service](./intro.md#catalog-service).
:::

Once the spark-shell is up, we run the following command to create a simple table.

```sql
scala>  spark.sql("CREATE TABLE openhouse.db.tb (ts timestamp, data string) PARTITIONED BY (days(ts))")
```

Run a `SHOW TABLES` command to confirm the table that we just created!

```sql

scala> spark.sql("SHOW TABLES IN openhouse.db").show

+---------+---------+
|namespace|tableName|
+---------+---------+
| db      |       tb|
+---------+---------+

```

Great! We have created our first table. Now, let us put some data in it and retrieve it.

```sql

scala>  spark.sql("""
INSERT INTO TABLE openhouse.db.tb VALUES
   (current_timestamp(), 'today'),
   (date_sub(CAST(current_timestamp() as DATE), 30), 'today-30d')
 """)

scala> spark.sql("SELECT * FROM openhouse.db.tb").show

+--------------------+---------+
|                  ts|     data|
+--------------------+---------+
|2024-03-22 19:39:...|    today|
| 2024-02-21 00:00:00|today-30d|
+--------------------+---------+

```

Looks great! We just added some data to OpenHouse and queried the data using Spark SQL.

To find out more about other SQL commands that OH supports, please visit the [SQL User Guide](./User%20Guide/Catalog/SQL.md).

### Inspect data on HDFS
We will now look at the data that was stored on HDFS.
First, we should log out of the container if we are on Spark driver and log in to the HDFS name node.

```
oh-hadoop-spark$main>  docker exec -it local.namenode bash

root@aa91a7bc8575:/# hdfs dfs -ls -R /data/openhouse/
```
This directory has all the OpenHouse tables that were made earlier. You should find the Iceberg metadata
files (`metadata.json`, `/metadata`) and the datafiles (in `/data`) that were created for the data we added
in the previous section.

## (Optional) Control access to Tables
We will continue with the same environment and the table (ie db.table) as before for this section.

You might have seen the parameter `spark.sql.catalog.openhouse.auth-token=$(cat /var/config/openhouse.token)` when you
launched the sparkshell. This parameter sets up the client with your user token.

As you did before, start the spark-shell and run the following SQL command to make it fail.
```SQL
scala> spark.sql("GRANT SELECT ON TABLE openhouse.db.tb TO user_1").show

java.lang.IllegalArgumentException: 400 , {"status":"BAD_REQUEST","error":"Bad Request","message":"db.tb2 is not a shared table","stacktrace":null,"cause":"Not Available"}
```
This error means the table is not sharable. **In OpenHouse, tables are private by default**. You can share them by
running the SQL command:
```SQL
scala> spark.sql("ALTER TABLE openhouse.db.tb SET POLICY ( SHARING=true )")
```

In order to check the ACLs for this table, run:
```SQL

scala> spark.sql("SHOW GRANTS ON TABLE openhouse.db.tb2").show
+---------+---------+
|privilege|principal|
+---------+---------+
|   SELECT|   user_1|
+---------+---------+

```

You can also apply the similar access control for database entity, please refer to the
[User Guide](./User%20Guide/Catalog/SQL.md#grant-revoke) to learn more.