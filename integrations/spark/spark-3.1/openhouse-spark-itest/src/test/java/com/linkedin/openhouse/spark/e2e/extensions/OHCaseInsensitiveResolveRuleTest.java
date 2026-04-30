package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;

import com.linkedin.openhouse.spark.SparkTestBase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link com.linkedin.openhouse.spark.extensions.OHCaseInsensitiveResolveRule}. These
 * tests use a mock OH server so they can simulate a case-duplicate table (one whose column names
 * differ only in casing, e.g. "id" and "ID") without being blocked by the OH server's schema
 * validation, which rejects such schemas on creation.
 */
@ExtendWith(SparkTestBase.class)
public class OHCaseInsensitiveResolveRuleTest {

  /**
   * Verifies that {@code OHCaseInsensitiveResolveRule} excludes case-duplicate tables from
   * normalization and does NOT silently resolve a mixed-case column reference to the wrong column.
   *
   * <p>Setup: a pre-existing OH table with columns "id" (field 1) and "ID" (field 2). With {@code
   * spark.sql.caseSensitive=true}, a reference to {@code Id} (neither exact case) must NOT be
   * silently redirected to "ID" by the rule. Instead the rule must leave the plan unchanged so
   * Spark's own {@code ResolveReferences} reports an unresolved attribute.
   *
   * <p>Without the case-duplicate guard the rule's map would contain {@code "id" -> "ID"} (last
   * write wins), causing {@code Id} to be renamed to {@code "ID"} and resolved silently to the
   * wrong field. The guard prevents this by returning an empty mapping for case-duplicate tables.
   */
  @SneakyThrows
  @Test
  public void testCaseDuplicateTable_mixedCaseRef_doesNotSilentlyNormalize() {
    // Create an Iceberg table with case-duplicate schema directly via the Iceberg Java API,
    // bypassing both Spark SQL and the OH server (neither allows duplicate-cased column names).
    // This simulates a table created before the server-side validation was introduced.
    TableIdentifier tableId = TableIdentifier.of("dbCaseDupRule", "caseDupTbl");
    Schema caseDupSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.optional(2, "ID", Types.StringType.get()));

    String warehouse = spark.conf().get("spark.sql.catalog.testhelper.warehouse");
    Catalog hadoopCatalog =
        CatalogUtil.buildIcebergCatalog(
            "testhelper_caseduptable",
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION, warehouse, ICEBERG_CATALOG_TYPE, "hadoop"),
            new Configuration());
    hadoopCatalog.createTable(tableId, caseDupSchema);

    // Derive a valid metadata.json path the same way MockHelpers.craftMetadataLocation does.
    Table table = hadoopCatalog.loadTable(tableId);
    Path metadataPath =
        Paths.get(((BaseTable) table).operations().current().metadataFileLocation());
    String copiedMetadata =
        Files.copy(
                metadataPath,
                metadataPath.resolveSibling(
                    new Random().nextInt(Integer.MAX_VALUE) + "-.metadata.json"))
            .toString();

    // Mock the OH catalog to return the case-duplicate table.
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbCaseDupRule",
                "caseDupTbl",
                "c1",
                "dbCaseDupRule.caseDupTbl.c1",
                "UUID",
                copiedMetadata,
                "v1",
                SchemaParser.toJson(caseDupSchema),
                null,
                null)));

    // With caseSensitive=true, a mixed-case reference "Id" has no exact match in either "id"
    // or "ID". The rule must skip normalization (empty mappings) so Spark reports an unresolved
    // attribute rather than silently redirecting to the wrong column.
    spark.conf().set("spark.sql.caseSensitive", "true");
    try {
      Assertions.assertThrows(
          Exception.class,
          () -> spark.sql("SELECT Id FROM openhouse.dbCaseDupRule.caseDupTbl").collectAsList(),
          "Rule must not normalize mixed-case ref against a case-duplicate table; query must throw");
    } finally {
      spark.conf().set("spark.sql.caseSensitive", "false");
    }
  }

  /**
   * Verifies that {@code OHCaseInsensitiveResolveRule} does NOT rename column references that
   * belong to a non-OH catalog relation when the OH table in the same plan has a column with the
   * same case-folded name.
   *
   * <p>Setup: an OH table ({@code openhouse} catalog) has column {@code "ID"} (uppercase); a non-OH
   * Iceberg table in the {@code testhelper} Hadoop catalog has column {@code "id"} (lowercase).
   * Both appear in the same plan via a JOIN. Both catalogs resolve their tables as {@code
   * DataSourceV2Relation} leaf nodes in the same analyzer pass, so the non-OH relation IS visible
   * to {@code collectOHColumnMappings} when the rule fires.
   *
   * <p>Without the fix: the mapping {@code {"id" -> "ID"}} is built from the OH table and {@code
   * transformExpressions} renames the non-OH table's {@code id} reference to {@code "ID"}, which
   * fails to resolve under {@code caseSensitive=true}.
   *
   * <p>With the fix, {@code collectOHColumnMappings} detects that {@code "id"} (case-folded) also
   * appears in the non-OH {@code DataSourceV2Relation}, excludes it from the mapping, and leaves
   * the non-OH table's reference unchanged — the analysis succeeds.
   */
  @SneakyThrows
  @Test
  public void testCrossCatalogJoin_nonOHTableColumnNotRenamedToMatchOHCasing() {
    // Create a non-OH Iceberg table in the testhelper (Hadoop) catalog with lowercase column "id".
    // testhelper uses type=hadoop (not catalog-impl), so isOHRelation returns false for it.
    spark.sql(
        "CREATE TABLE IF NOT EXISTS testhelper.dbCrossJoin.nonOhTable (id string) USING iceberg");

    // Create an OH table with uppercase column "ID" directly via Hadoop catalog,
    // bypassing Spark SQL DDL to ensure the column is stored as "ID" (not lowercased).
    TableIdentifier ohTableId = TableIdentifier.of("dbCrossJoin", "ohJoinTable");
    Schema ohSchema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));

    String warehouse = spark.conf().get("spark.sql.catalog.testhelper.warehouse");
    Catalog hadoopCatalog =
        CatalogUtil.buildIcebergCatalog(
            "testhelper_crossjoin",
            ImmutableMap.of(
                CatalogProperties.WAREHOUSE_LOCATION, warehouse, ICEBERG_CATALOG_TYPE, "hadoop"),
            new Configuration());
    hadoopCatalog.createTable(ohTableId, ohSchema);

    Table table = hadoopCatalog.loadTable(ohTableId);
    Path metadataPath =
        Paths.get(((BaseTable) table).operations().current().metadataFileLocation());
    String copiedMetadata =
        Files.copy(
                metadataPath,
                metadataPath.resolveSibling(
                    new Random().nextInt(Integer.MAX_VALUE) + "-.metadata.json"))
            .toString();

    // Mock the OH catalog response for the OH table (column "ID").
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbCrossJoin",
                "ohJoinTable",
                "c1",
                "dbCrossJoin.ohJoinTable.c1",
                "UUID",
                copiedMetadata,
                "v1",
                SchemaParser.toJson(ohSchema),
                null,
                null)));

    // Both tables appear in the same plan via a JOIN. With caseSensitive=true:
    //   - testhelper.dbCrossJoin.nonOhTable has "id" (lowercase, exact case in query)
    //   - openhouse.dbCrossJoin.ohJoinTable has "ID" (uppercase, exact case in query)
    // The rule must exclude "id" from its mapping (shared name with the non-OH relation) and
    // leave the non-OH table's reference unchanged — analysis must succeed.
    spark.conf().set("spark.sql.caseSensitive", "true");
    try {
      Assertions.assertDoesNotThrow(
          () ->
              spark
                  .sql(
                      "SELECT t.id FROM testhelper.dbCrossJoin.nonOhTable t"
                          + " JOIN openhouse.dbCrossJoin.ohJoinTable oh ON t.id = oh.ID")
                  .queryExecution()
                  .analyzed(),
          "Analysis must succeed: the rule must not rename nonOhTable.id to ID");
    } finally {
      spark.conf().set("spark.sql.caseSensitive", "false");
      spark.sql("DROP TABLE IF EXISTS testhelper.dbCrossJoin.nonOhTable");
    }
  }
}
