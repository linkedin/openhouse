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
    String replicationConfigJson = "[{\"destination\":\"a\", \"interval\":\"24H\"}]";
    Dataset<Row> ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = "
                + "({destination:'a', interval:24H}))");
    assert isPlanValid(ds, replicationConfigJson);

    // Test support with multiple clusters
    replicationConfigJson =
        "[{\"destination\":\"a\", \"interval\":\"12H\"}, {\"destination\":\"aa\", \"interval\":\"12H\"}]";
    ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = "
                + "({destination:'a', interval:12h}, {destination:'aa', interval:12H}))");
    assert isPlanValid(ds, replicationConfigJson);
  }

  @Test
  public void testSimpleSetReplicationPolicyOptionalInterval() {
    // Test with optional interval
    String replicationConfigJson = "[{\"destination\":\"a\"}]";
    Dataset<Row> ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = " + "({destination:'a'}))");
    assert isPlanValid(ds, replicationConfigJson);

    // Test with optional interval for multiple clusters
    replicationConfigJson = "[{\"destination\":\"a\"}, {\"destination\":\"b\"}]";
    ds =
        spark.sql(
            "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = "
                + "({destination:'a'}, {destination:'b'}))");
    assert isPlanValid(ds, replicationConfigJson);
  }

  @Test
  public void testReplicationPolicyWithoutProperSyntax() {
    // Empty cluster value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination:}))")
                .show());

    // Empty interval value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interval:}))")
                .show());

    // Empty interval value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interval:}))")
                .show());

    // Missing cluster value but interval present
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination:, interval: '12h'}))")
                .show());

    // Missing interval value but keyword present
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'a', interval:}))")
                .show());

    // Missing cluster value for multiple clusters
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination:, interval:'12H'}, {cluster:, interval: '12H'}))")
                .show());

    // Missing cluster keyword for multiple clusters
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination:'a'}, {interval: '12h'}))")
                .show());

    // Missing cluster keyword
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({interval: '12h'}))")
                .show());

    // Typo in keyword interval
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interv: '12h'}))")
                .show());

    // Typo in keyword cluster
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destina: 'aa', interval: '12h'}))")
                .show());

    // Missing quote in cluster value
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: aa', interval: '12h}))")
                .show());

    // Typo in REPLICATION keyword
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICAT = ({destination: 'aa', interval: '12h'}))")
                .show());

    // Interval input does not follow 'h/H' format
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interval: '12'}))")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interval: '1D'}))")
                .show());

    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql(
                    "ALTER TABLE openhouse.db.table SET POLICY (REPLICATION = ({destination: 'aa', interval: '12d'}))")
                .show());

    // Missing cluster and interval values
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
    String queryStr = dataframe.queryExecution().explainString(ExplainMode.fromString("simple"));
    JsonArray jsonArray = new Gson().fromJson(replicationConfigJson, JsonArray.class);
    boolean isValid = false;
    for (JsonElement element : jsonArray) {
      JsonObject entry = element.getAsJsonObject();
      String destination = entry.get("destination").getAsString();
      isValid = queryStr.contains(destination);
      if (entry.has("interval")) {
        String interval = entry.get("interval").getAsString();
        isValid = queryStr.contains(destination) && queryStr.contains(interval);
      }
    }
    return isValid;
  }
}
