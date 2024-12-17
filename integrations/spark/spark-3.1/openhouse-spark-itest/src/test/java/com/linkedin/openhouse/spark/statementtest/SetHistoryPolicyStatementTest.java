package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import java.util.Optional;
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
public class SetHistoryPolicyStatementTest {
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
  public void testSetHistoryPolicyGood() {
    // Validate setting only time setting
    Dataset<Row> ds = spark.sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY MAX_AGE=24H)");
    assert isPlanValid(ds, "db.table", Optional.of("24"), Optional.of("HOUR"), Optional.empty());

    ds = spark.sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY VERSIONS=10)");
    assert isPlanValid(ds, "db.table", Optional.empty(), Optional.empty(), Optional.of("10"));

    // Validate both time and count setting
    ds = spark.sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY MAX_AGE=2D VERSIONS=20)");
    assert isPlanValid(ds, "db.table", Optional.of("2"), Optional.of("DAY"), Optional.of("20"));
  }

  @Test
  public void testSetHistoryPolicyIncorrectSyntax() {
    // No time granularity
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY MAX_AGE=24)").show());

    // Count before time
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () ->
            spark
                .sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY VERSIONS=10 MAX_AGE=24H)")
                .show());

    // No time or count
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table SET POLICY (HISTORY )").show());
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
  private boolean isPlanValid(
      Dataset<Row> dataframe,
      String dbTable,
      Optional<String> maxAge,
      Optional<String> granularity,
      Optional<String> versions) {
    String queryStr = dataframe.queryExecution().explainString(ExplainMode.fromString("simple"));
    return queryStr.contains(dbTable)
        && (!maxAge.isPresent() || queryStr.contains(maxAge.get()))
        && (!granularity.isPresent() || queryStr.contains(granularity.get()))
        && (!versions.isPresent() || queryStr.contains(versions.get()));
  }
}
