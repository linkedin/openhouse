package com.linkedin.openhouse.spark.statementtest;

import com.linkedin.openhouse.javaclient.api.SupportsUnlock;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.UnlockTable;
import java.nio.file.Files;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class UnlockTableStatementTest {

  private static SparkSession spark = null;

  @Test
  public void testUnlockLegacyLock() {
    spark.sql("ALTER TABLE openhouse.db.table UNLOCK");
    assertUnlocked("db.table", null);
  }

  @Test
  public void testUnlockWithReason() {
    spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY");
    assertUnlocked("db.table", "SYSTEM_ONLY");
  }

  @Test
  public void testUnlockLowerCase() {
    spark.sql("alter table openhouse.db.table unlock reason system_only");
    assertUnlocked("db.table", "SYSTEM_ONLY");
  }

  @Test
  public void testUnlockAfterUseCatalog() {
    spark.sql("USE openhouse");
    try {
      spark.sql("ALTER TABLE db.table UNLOCK");
      assertUnlocked("db.table", null);
    } finally {
      spark.sql("USE spark_catalog");
    }
  }

  @Test
  public void testUnlockQuotedAndKeywordIdentifiers() {
    spark.sql("ALTER TABLE openhouse.`my db`.`unlock` UNLOCK REASON `system_only`");
    assertUnlocked("my db.unlock", "SYSTEM_ONLY");
    spark.sql("ALTER TABLE openhouse.reason.unlock /* comment */ UNLOCK");
    assertUnlocked("reason.unlock", null);
  }

  @Test
  public void testUnlockKeepsEscapedBackticks() {
    spark.sql("ALTER TABLE openhouse.db.`ta``ble` UNLOCK REASON `SYSTEM``ONLY`");
    assertUnlocked("db.ta`ble", "SYSTEM`ONLY");
  }

  @Test
  public void testUnlockSyntaxErrors() {
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON"));
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK SYSTEM_ONLY"));
    Assertions.assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCK REASON SYSTEM_ONLY NOW"));
    Assertions.assertThrows(
        ParseException.class, () -> spark.sql("ALTER TABLE openhouse.db.table UNLOCKED"));
    Assertions.assertNull(UnlockHadoopCatalog.tableName);
  }

  @Test
  public void testOtherAlterTableStatementsAreNotUnlock() throws ParseException {
    for (String sql :
        new String[] {
          "ALTER TABLE openhouse.db.table RENAME TO unlock",
          "ALTER TABLE openhouse.db.table SET TBLPROPERTIES ('note' = 'unlock')",
          "ALTER TABLE openhouse.db.table ADD COLUMNS (unlock string)"
        }) {
      Assertions.assertFalse(
          spark.sessionState().sqlParser().parsePlan(sql) instanceof UnlockTable, sql);
    }
  }

  @Test
  public void testUnsupportedCatalog() {
    Exception exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> spark.sql("ALTER TABLE spark_catalog.db.table UNLOCK"));
    Assertions.assertTrue(
        exception.getMessage().contains("Catalog 'spark_catalog' does not support UNLOCK"));
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
            .config(
                "spark.sql.catalog.openhouse.catalog-impl",
                "com.linkedin.openhouse.spark.statementtest.UnlockTableStatementTest$UnlockHadoopCatalog")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", unittest.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    UnlockHadoopCatalog.tableName = null;
    UnlockHadoopCatalog.reason = null;
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  private void assertUnlocked(String tableName, String reason) {
    Assertions.assertEquals(tableName, UnlockHadoopCatalog.tableName, "Table mismatch");
    Assertions.assertEquals(reason, UnlockHadoopCatalog.reason, "Reason mismatch");
  }

  public static class UnlockHadoopCatalog extends HadoopCatalog implements SupportsUnlock {
    public static String tableName;
    public static String reason;

    @Override
    public void unlockTable(TableIdentifier tableIdentifier, String lockReason) {
      tableName = tableIdentifier.toString();
      reason = lockReason;
    }
  }
}
