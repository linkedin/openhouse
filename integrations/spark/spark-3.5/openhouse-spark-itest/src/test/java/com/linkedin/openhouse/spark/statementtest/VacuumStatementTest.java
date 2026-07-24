package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class VacuumStatementTest {

  private static SparkSession spark = null;

  private long snapshotCount(String table) {
    return spark.sql("SELECT * FROM " + table + ".snapshots").count();
  }

  private long rowCount(String table) {
    return spark.sql("SELECT * FROM " + table).count();
  }

  @Test
  public void testVacuumExpiresSnapshots() {
    // Three inserts create three snapshots.
    Assertions.assertEquals(3, snapshotCount("openhouse.db.table"));

    // RETAIN 0 HOURS expires everything but the current snapshot; the table stays readable.
    spark.sql("VACUUM openhouse.db.table RETAIN 0 HOURS").collect();

    Assertions.assertEquals(1, snapshotCount("openhouse.db.table"));
    Assertions.assertEquals(3, rowCount("openhouse.db.table"));
  }

  @Test
  public void testVacuumWithDefaultRetentionSucceeds() {
    // No RETAIN: each procedure applies its own default retention. Table remains readable.
    spark.sql("VACUUM openhouse.db.table").collect();
    Assertions.assertEquals(3, rowCount("openhouse.db.table"));
  }

  @Test
  public void testVacuumRemoveOrphanFilesPreservesLiveData() {
    // A 24-hour window is safely above Iceberg's orphan-file removal floor and must not delete any
    // file the table references, so all rows survive.
    spark.sql("VACUUM openhouse.db.table REMOVE ORPHAN FILES RETAIN 24 HOURS").collect();
    Assertions.assertEquals(3, rowCount("openhouse.db.table"));
  }

  @Test
  public void testVacuumLowerCase() {
    spark.sql("vacuum openhouse.db.table retain 0 hours").collect();
    Assertions.assertEquals(1, snapshotCount("openhouse.db.table"));
  }

  @Test
  public void testVacuumNonOpenhouseTableThrows() {
    Assertions.assertThrows(
        Exception.class, () -> spark.sql("VACUUM openhouse.db.not_openhouse").collect());
  }

  @Test
  public void testVacuumInvalidSyntaxThrows() {
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("VACUUM openhouse.db.table RETAIN 5 DAYS").collect());
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path unittest = new Path(Files.createTempDirectory("unittest").toString());
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

  @BeforeEach
  public void setup() {
    spark
        .sql(
            "CREATE TABLE openhouse.db.table (id bigint, data string, `openhouse.tableId` string) USING iceberg")
        .show();
    spark
        .sql("ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('openhouse.tableId' = 'tableid')")
        .show();
    spark.sql("INSERT INTO openhouse.db.table VALUES (1, 'a', 'tableid')").show();
    spark.sql("INSERT INTO openhouse.db.table VALUES (2, 'b', 'tableid')").show();
    spark.sql("INSERT INTO openhouse.db.table VALUES (3, 'c', 'tableid')").show();

    spark
        .sql("CREATE TABLE openhouse.db.not_openhouse (id bigint, data string) USING iceberg")
        .show();
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE IF EXISTS openhouse.db.table").show();
    spark.sql("DROP TABLE IF EXISTS openhouse.db.not_openhouse").show();
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }
}
