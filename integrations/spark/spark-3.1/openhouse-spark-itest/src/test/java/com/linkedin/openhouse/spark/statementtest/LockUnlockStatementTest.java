package com.linkedin.openhouse.spark.statementtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.javaclient.api.SupportsTableLocking;
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LockUnlockStatementTest {
  private SparkSession spark;

  @Test
  public void testLegacyLockAndUnlock() {
    spark.sql("LOCK TABLE openhouse.db.table");
    assertInvocation("LOCK", "db.table", Optional.empty(), Optional.empty());

    spark.sql("UNLOCK TABLE openhouse.db.table");
    assertInvocation("UNLOCK", "db.table", Optional.empty(), Optional.empty());
  }

  @Test
  public void testReasonedLockAndUnlockNormalizeCase() {
    spark.sql(
        "lock table openhouse.db.table with reason tier3_auto_cleanup "
            + "message 'Cleanup starts tomorrow'");
    assertInvocation(
        "LOCK",
        "db.table",
        Optional.of("TIER3_AUTO_CLEANUP"),
        Optional.of("Cleanup starts tomorrow"));

    spark.sql("UnLoCk TaBlE openhouse.db.table WiTh ReAsOn TiEr3_AuTo_ClEaNuP");
    assertInvocation("UNLOCK", "db.table", Optional.of("TIER3_AUTO_CLEANUP"), Optional.empty());
  }

  @Test
  public void testLockMessageSupportsSqlEscaping() {
    spark.sql(
        "LOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP " + "MESSAGE 'It''s locked'");
    assertInvocation(
        "LOCK", "db.table", Optional.of("TIER3_AUTO_CLEANUP"), Optional.of("It's locked"));
  }

  @Test
  public void testIdentifierResolution() {
    spark.sql("LOCK TABLE openhouse.`db`.`table`");
    assertInvocation("LOCK", "db.table", Optional.empty(), Optional.empty());

    spark.sql("LOCK TABLE openhouse.0_.0_ WITH REASON TIER3_AUTO_CLEANUP");
    assertInvocation("LOCK", "0_.0_", Optional.of("TIER3_AUTO_CLEANUP"), Optional.empty());

    spark.sql("USE openhouse");
    spark.sql("UNLOCK TABLE db.table");
    assertInvocation("UNLOCK", "db.table", Optional.empty(), Optional.empty());

    spark.sql("USE openhouse.db");
    spark.sql("LOCK TABLE table WITH REASON TIER3_AUTO_CLEANUP");
    assertInvocation("LOCK", "db.table", Optional.of("TIER3_AUTO_CLEANUP"), Optional.empty());
  }

  @Test
  public void testCommandKeywordsRemainTableIdentifiers() {
    Arrays.asList("lock", "unlock", "with", "reason", "message")
        .forEach(
            tableName -> {
              spark.sql("LOCK TABLE openhouse.db." + tableName);
              assertInvocation("LOCK", "db." + tableName, Optional.empty(), Optional.empty());
            });
  }

  @Test
  public void testCommentsAreAccepted() {
    Arrays.asList(
            "/* leading */ LOCK TABLE openhouse.db.table",
            "-- leading\nLOCK TABLE openhouse.db.table",
            "LOCK /* inline */ TABLE openhouse.db.table",
            "/* multi\nline */ UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP")
        .forEach(spark::sql);

    assertInvocation("UNLOCK", "db.table", Optional.of("TIER3_AUTO_CLEANUP"), Optional.empty());
  }

  @Test
  public void testMalformedSyntax() {
    assertThrows(ParseException.class, () -> spark.sql("LOK TABLE openhouse.db.table"));
    assertThrows(ParseException.class, () -> spark.sql("LOCK openhouse.db.table"));
    assertThrows(OpenhouseParseException.class, () -> spark.sql("LOCK TABLE"));
    assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("LOCK TABLE openhouse.db.table WITH REASON"));
    assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("LOCK TABLE openhouse.db.table WITH REASON 'TIER3_AUTO_CLEANUP'"));
    assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("LOCK TABLE openhouse.db.table WITH REASON tier3.auto_cleanup"));
    assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("LOCK TABLE openhouse.db.table MESSAGE 'Cleanup'"));
    assertThrows(
        OpenhouseParseException.class,
        () ->
            spark.sql(
                "LOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP "
                    + "WITH MESSAGE 'Cleanup'"));
    assertThrows(
        OpenhouseParseException.class,
        () ->
            spark.sql(
                "UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP "
                    + "MESSAGE 'Cleanup'"));
    assertThrows(
        OpenhouseParseException.class,
        () -> spark.sql("LOCK TABLE openhouse.db.table WITH CAPABILITY cleanup"));
  }

  @Test
  public void testUnsupportedCatalog() {
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db");
    spark.sql("CREATE TABLE spark_catalog.db.table_lock_test (id bigint) USING iceberg");
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> spark.sql("LOCK TABLE spark_catalog.db.table_lock_test"));
    assertTrue(
        exception
            .getMessage()
            .contains("Catalog 'spark_catalog' does not support Table Lock Statements"));
    spark.sql("DROP TABLE spark_catalog.db.table_lock_test");
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path warehouse = new Path(Files.createTempDirectory("lock-statement-test").toString());
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + "com.linkedin.openhouse.spark.extensions.OpenhouseSparkSessionExtensions")
            .config("spark.sql.catalog.openhouse", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                "spark.sql.catalog.openhouse.catalog-impl", LockingHadoopCatalog.class.getName())
            .config("spark.sql.catalog.openhouse.warehouse", warehouse.toString())
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", warehouse.toString())
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    LockingHadoopCatalog.operation = Optional.empty();
    LockingHadoopCatalog.tableIdentifier = Optional.empty();
    LockingHadoopCatalog.reason = Optional.empty();
    LockingHadoopCatalog.message = Optional.empty();
    spark.sql("CREATE TABLE openhouse.db.table (id bigint) USING iceberg");
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE openhouse.db.table");
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  private void assertInvocation(
      String operation, String tableIdentifier, Optional<String> reason, Optional<String> message) {
    assertEquals(Optional.of(operation), LockingHadoopCatalog.operation);
    assertEquals(
        tableIdentifier,
        LockingHadoopCatalog.tableIdentifier.orElseThrow(AssertionError::new).toString());
    assertEquals(reason, LockingHadoopCatalog.reason);
    assertEquals(message, LockingHadoopCatalog.message);
  }

  public static class LockingHadoopCatalog extends HadoopCatalog implements SupportsTableLocking {
    private static Optional<String> operation = Optional.empty();
    private static Optional<TableIdentifier> tableIdentifier = Optional.empty();
    private static Optional<String> reason = Optional.empty();
    private static Optional<String> message = Optional.empty();

    @Override
    public void lockTable(
        TableIdentifier identifier, Optional<String> lockReason, Optional<String> lockMessage) {
      operation = Optional.of("LOCK");
      tableIdentifier = Optional.of(identifier);
      reason = lockReason;
      message = lockMessage;
    }

    @Override
    public void unlockTable(TableIdentifier identifier, Optional<String> lockReason) {
      operation = Optional.of("UNLOCK");
      tableIdentifier = Optional.of(identifier);
      reason = lockReason;
      message = Optional.empty();
    }
  }
}
