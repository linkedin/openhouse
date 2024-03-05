package com.linkedin.openhouse.spark.statementtest;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SetTableReplicationPolicyStatementTest {
  private static SparkSession spark = null;

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path unittest = new Path(Files.createTempDirectory("unittest_settablepolicy").toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                ("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions"))
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.openhouse.type", "hadoop")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .getOrCreate();
  }

  @Test
  public void testSimpleSetReplicationPolicy() {
    String replicationConfigJson =
        "{\"cluster\":\"a\", \"schedule\":\"b\"}, {\"cluster\": \"aa\", \"schedule\": \"bb\"}";
    Dataset<Row> ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = "
                + "({cluster:'a', schedule:'b'}, {cluster: 'aa', schedule: 'bb'}))");
    assert isPlanValid(ds, replicationConfigJson);

    replicationConfigJson = "{\"cluster\":\"a\", \"schedule\":\"b\"}";
    ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({cluster:'a', schedule:'b'}))");
    assert isPlanValid(ds, replicationConfigJson);
  }

  @Test
  public void testReplicationPolicyWithoutProperSyntax() {
    // missing schedule keyword
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({cluster: 'aa'}))")
                .show());

    // Missing cluster keyword
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({schedule: 'ss'}))")
                .show());

    // Typo in keyword schedule
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({cluster: 'aa', schedul: 'ss'}))")
                .show());

    // Typo in keyword cluster
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({clustr: 'aa', schedule: 'ss'}))")
                .show());

    // Missing quote in cluster value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({cluster: aa', schedule: 'ss}))")
                .show());

    // Type in REPLICATION keyword
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICAT = ({cluster: 'aa', schedule: 'ss}))")
                .show());

    // Missing cluster and schedule value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({}))").show());
  }

  @BeforeEach
  public void setup() {
    spark.sql("CREATE TABLE openhouse.db.table (id bigint, data string) USING iceberg").show();
    spark.sql("CREATE TABLE openhouse.0_.0_ (id bigint, data string) USING iceberg").show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
    spark
        .sql("ALTER TABLE openhouse.0_.0_ SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE openhouse.db.table").show();
    spark.sql("DROP TABLE openhouse.0_.0_").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  @SneakyThrows
  private boolean isPlanValid(Dataset<Row> dataframe, String replicationConfigJson) {
    replicationConfigJson = "[" + replicationConfigJson + "]";
    String queryStr = dataframe.queryExecution().explainString(ExplainMode.fromString("simple"));
    JsonArray jsonArray = new Gson().fromJson(replicationConfigJson, JsonArray.class);
    boolean isValid = false;
    for (JsonElement element : jsonArray) {
      JsonObject entry = element.getAsJsonObject();
      String cluster = entry.get("cluster").getAsString();
      String schedule = entry.get("schedule").getAsString();
      isValid = queryStr.contains(cluster) && queryStr.contains(schedule);
    }
    return isValid;
  }
}
