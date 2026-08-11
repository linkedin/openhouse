package com.linkedin.openhouse.spark.statementtest;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.javaclient.api.SupportsColumnEntitlements;
import java.nio.file.Files;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** Verifies that columns the principal is not entitled to read are masked out of query results. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ColumnAclStatementTest {

  private static final String TAGGED_POLICIES =
      "{\"columnTags\":{\"ssn\":{\"tags\":[\"PII\"]},\"salary\":{\"tags\":[\"HC\"]}}}";

  private static SparkSession spark = null;

  @Test
  public void testRestrictedColumnIsMasked() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows = spark.sql("SELECT id, ssn, salary FROM openhouse.db.tagged").collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1L, rows.get(0).getLong(0));
    Assertions.assertTrue(rows.get(0).isNullAt(1), "Restricted column should be masked");
    Assertions.assertEquals(100L, rows.get(0).getLong(2), "Granted column should be readable");
  }

  @Test
  public void testMultipleRestrictedColumnsAreMasked() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn", "salary");

    List<Row> rows = spark.sql("SELECT * FROM openhouse.db.tagged").collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1L, rows.get(0).getLong(0));
    Assertions.assertTrue(rows.get(0).isNullAt(1));
    Assertions.assertTrue(rows.get(0).isNullAt(2));
  }

  @Test
  public void testMaskingIsCaseInsensitiveOnColumnName() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("SSN");

    List<Row> rows = spark.sql("SELECT ssn FROM openhouse.db.tagged").collectAsList();

    Assertions.assertTrue(rows.get(0).isNullAt(0));
  }

  @Test
  public void testNothingRestrictedLeavesResultsIntact() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of();

    List<Row> rows = spark.sql("SELECT id, ssn, salary FROM openhouse.db.tagged").collectAsList();

    Assertions.assertEquals("123-45-6789", rows.get(0).getString(1));
    Assertions.assertEquals(100L, rows.get(0).getLong(2));
  }

  /**
   * A filter on a restricted column must be evaluated against the mask, not against the stored
   * value, otherwise the value could be recovered by probing it one predicate at a time.
   */
  @Test
  public void testFilterOnRestrictedColumnMatchesNothing() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows =
        spark.sql("SELECT id FROM openhouse.db.tagged WHERE ssn = '123-45-6789'").collectAsList();

    Assertions.assertTrue(rows.isEmpty(), "Restricted column must not be usable as a predicate");
  }

  /** Masking must survive being carried through another table, e.g. via CTAS. */
  @Test
  public void testCtasCopiesMaskedValues() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    spark.sql(
        "CREATE TABLE openhouse.db.copied USING iceberg AS SELECT * FROM openhouse.db.tagged");
    List<Row> rows = spark.sql("SELECT id, ssn FROM openhouse.db.copied").collectAsList();

    Assertions.assertEquals(1, rows.size());
    Assertions.assertTrue(rows.get(0).isNullAt(1), "Restricted column must not be copied out");
  }

  /** Writes must land in the table untouched; only the query feeding them is subject to masking. */
  @Test
  public void testInsertIntoRestrictedColumnIsNotMasked() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    spark.sql("INSERT INTO openhouse.db.tagged VALUES (2, '987-65-4321', 200)");

    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of();
    List<Row> rows = spark.sql("SELECT ssn FROM openhouse.db.tagged WHERE id = 2").collectAsList();
    Assertions.assertEquals("987-65-4321", rows.get(0).getString(0));
  }

  /** A table without column tags must not trigger a catalog call at all. */
  @Test
  public void testUntaggedTableIsNotResolved() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");
    ColumnAclHadoopCatalog.resolveCount = 0;

    List<Row> rows = spark.sql("SELECT id, ssn FROM openhouse.db.untagged").collectAsList();

    Assertions.assertEquals("123-45-6789", rows.get(0).getString(1));
    Assertions.assertEquals(0, ColumnAclHadoopCatalog.resolveCount);
  }

  /**
   * A tagged table read while column ACLs are switched off must still be masked once re-enabled.
   */
  @Test
  public void testDisabledConfigSkipsMasking() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");
    spark.conf().set("spark.openhouse.columnAcl.enabled", "false");
    try {
      List<Row> rows = spark.sql("SELECT ssn FROM openhouse.db.tagged").collectAsList();
      Assertions.assertEquals("123-45-6789", rows.get(0).getString(0));
    } finally {
      spark.conf().set("spark.openhouse.columnAcl.enabled", "true");
    }
  }

  /** A tagged table in a catalog that cannot resolve entitlements must fail rather than expose. */
  @Test
  public void testTaggedTableInCatalogWithoutEntitlementsFails() {
    Exception exception =
        Assertions.assertThrows(
            Exception.class, () -> spark.sql("SELECT ssn FROM nonacl.db.tagged").collectAsList());

    Assertions.assertTrue(
        messageChain(exception).contains("cannot resolve column entitlements"),
        "Expected a fail-closed error, got: " + messageChain(exception));
  }

  /** Joining on a restricted column must compare masks, not the underlying values. */
  @Test
  public void testJoinOnRestrictedColumnMatchesNothing() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows =
        spark
            .sql(
                "SELECT l.id FROM openhouse.db.tagged l JOIN openhouse.db.tagged r "
                    + "ON l.ssn = r.ssn")
            .collectAsList();

    Assertions.assertTrue(rows.isEmpty());
  }

  /** Aggregates must see the mask, so a restricted column contributes nothing. */
  @Test
  public void testAggregateOverRestrictedColumn() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows =
        spark
            .sql("SELECT count(ssn) AS c, count(id) AS i FROM openhouse.db.tagged")
            .collectAsList();

    Assertions.assertEquals(0L, rows.get(0).getLong(0));
    Assertions.assertEquals(1L, rows.get(0).getLong(1));
  }

  /** Masking must not be escapable by wrapping the read in a subquery or CTE. */
  @Test
  public void testMaskingSurvivesSubquery() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows =
        spark
            .sql(
                "WITH t AS (SELECT id, ssn FROM openhouse.db.tagged) "
                    + "SELECT ssn FROM t WHERE id = 1")
            .collectAsList();

    Assertions.assertTrue(rows.get(0).isNullAt(0));
  }

  /** The DataFrame API resolves against an already analyzed plan, so masking must be stable. */
  @Test
  public void testMaskingIsStableAcrossDataFrameOperations() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of("ssn");

    List<Row> rows = spark.table("openhouse.db.tagged").select("ssn", "salary").collectAsList();

    Assertions.assertTrue(rows.get(0).isNullAt(0));
    Assertions.assertEquals(100L, rows.get(0).getLong(1));
  }

  private static String messageChain(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      builder.append(current.getMessage()).append('\n');
    }
    return builder.toString();
  }

  @SneakyThrows
  @BeforeAll
  public void setupSpark() {
    Path unittest = new Path(Files.createTempDirectory("unittest").toString());
    Path nonAclWarehouse = new Path(Files.createTempDirectory("nonacl").toString());
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
                "com.linkedin.openhouse.spark.statementtest.ColumnAclStatementTest$ColumnAclHadoopCatalog")
            .config("spark.sql.catalog.openhouse.warehouse", unittest.toString())
            // A catalog with no notion of column entitlements, used to check the fail-closed path.
            .config("spark.sql.catalog.nonacl", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nonacl.type", "hadoop")
            .config("spark.sql.catalog.nonacl.warehouse", nonAclWarehouse.toString())
            // The resolver caches decisions, which would otherwise leak between test cases.
            .config("spark.openhouse.columnAcl.cacheTtlSeconds", "0")
            .getOrCreate();
  }

  @BeforeEach
  public void setup() {
    ColumnAclHadoopCatalog.restrictedColumns = ImmutableList.of();
    ColumnAclHadoopCatalog.resolveCount = 0;
    spark.sql(
        String.format(
            "CREATE TABLE openhouse.db.tagged (id bigint, ssn string, salary bigint) USING iceberg "
                + "TBLPROPERTIES ('openhouse.tableId'='tagged', 'policies'='%s')",
            TAGGED_POLICIES));
    spark.sql("INSERT INTO openhouse.db.tagged VALUES (1, '123-45-6789', 100)");
    spark.sql(
        "CREATE TABLE openhouse.db.untagged (id bigint, ssn string) USING iceberg "
            + "TBLPROPERTIES ('openhouse.tableId'='untagged')");
    spark.sql("INSERT INTO openhouse.db.untagged VALUES (1, '123-45-6789')");
    spark.sql(
        String.format(
            "CREATE TABLE nonacl.db.tagged (id bigint, ssn string) USING iceberg "
                + "TBLPROPERTIES ('openhouse.tableId'='tagged', 'policies'='%s')",
            TAGGED_POLICIES));
  }

  @AfterEach
  public void tearDown() {
    spark.sql("DROP TABLE IF EXISTS openhouse.db.tagged");
    spark.sql("DROP TABLE IF EXISTS openhouse.db.untagged");
    spark.sql("DROP TABLE IF EXISTS openhouse.db.copied");
    spark.sql("DROP TABLE IF EXISTS nonacl.db.tagged");
  }

  @AfterAll
  public void tearDownSpark() {
    spark.close();
  }

  public static class ColumnAclHadoopCatalog extends HadoopCatalog
      implements SupportsColumnEntitlements {
    public static List<String> restrictedColumns = ImmutableList.of();
    public static int resolveCount = 0;

    @Override
    public ColumnEntitlementsDto getColumnEntitlements(TableIdentifier tableIdentifier) {
      resolveCount++;
      return new ColumnEntitlementsDto(ImmutableList.of(), restrictedColumns);
    }
  }
}
