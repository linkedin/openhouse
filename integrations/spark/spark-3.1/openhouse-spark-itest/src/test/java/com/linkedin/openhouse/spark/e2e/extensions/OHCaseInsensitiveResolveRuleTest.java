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
}
