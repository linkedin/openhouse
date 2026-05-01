package com.linkedin.openhouse.spark.catalogtest;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

public class CatalogOperationTest extends OpenHouseSparkITest {
  @Test
  public void testCasingWithCTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      spark.sql("CREATE TABLE openhouse.d1.`tT1` (name string)");
      // testing writing behavior, note the casing of tt1 is intentionally changed.
      spark.sql("INSERT INTO openhouse.d1.Tt1 VALUES ('foo')");

      // Verifying by querying with all lower-cased name
      Assertions.assertEquals(
          1, spark.sql("SELECT * from openhouse.d1.tt1").collectAsList().size());
      // ctas but referring with lower-cased name
      spark.sql("CREATE TABLE openhouse.d1.t2 AS SELECT * from openhouse.d1.tt1");
      Assertions.assertEquals(1, spark.sql("SELECT * FROM openhouse.d1.t2").collectAsList().size());
    }
  }

  @Test
  public void testCreateTablePartitionedByDate() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      String quotedFqtn = "openhouse.d1.tpartionedbydate";
      spark.sql(
          String.format(
              "CREATE TABLE %s (data string) PARTITIONED BY (datefield DATE)", quotedFqtn));
      spark
          .sql(String.format("INSERT INTO %s SELECT 'a', to_date('2024-06-21')", quotedFqtn))
          .show();

      // Get the schema of the table
      StructType schema = spark.table(quotedFqtn).schema();

      // Assert that the "datefield" column is of DateType
      StructField dateField = schema.fields()[1]; // Assuming "datefield" is the second column
      Assertions.assertEquals("datefield", dateField.name());
      Assertions.assertTrue(
          dateField.dataType() instanceof DateType, "The 'datefield' column should be of DateType");
    }
  }

  @Test
  public void testCatalogWriteAPI() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      // Create a table
      Schema schema = new Schema(Types.NestedField.required(1, "name", Types.StringType.get()));
      TableIdentifier tableIdentifier = TableIdentifier.of("db", "aaa");
      icebergCatalog.createTable(tableIdentifier, schema);

      // Write into data with intentionally changed casing in name
      TableIdentifier tableIdentifierUpperTblName = TableIdentifier.of("db", "AAA");

      DataFile fooDataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath("/path/to/data-a.parquet")
              .withFileSizeInBytes(10)
              .withRecordCount(1)
              .build();
      AtomicReference<Table> tableRef = new AtomicReference<>();
      Assertions.assertDoesNotThrow(
          () -> {
            Table loadedTable = icebergCatalog.loadTable(tableIdentifierUpperTblName);
            tableRef.set(loadedTable);
          });
      Table table = tableRef.get();
      Assertions.assertDoesNotThrow(
          () -> {
            table.newAppend().appendFile(fooDataFile).commit();
          });
    }
  }

  @Test
  public void testCreateReplicaSkipFieldIdReassignmentUnPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));

      // Field ids not reassigned
      TableIdentifier tableIdentifier = TableIdentifier.of("replication_test", "t1");
      Map<String, String> props = new HashMap<>();
      props.put("client.table.schema", SchemaParser.toJson(schema));
      Table table = icebergCatalog.createTable(tableIdentifier, schema, null, props);
      Schema schemaAfterCreation = table.schema();
      Assertions.assertTrue(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(1, schemaAfterCreation.findField("a").fieldId());
      Assertions.assertNotEquals(3, schemaAfterCreation.findField("a.b").fieldId());
      Assertions.assertNotEquals(2, schemaAfterCreation.findField("c").fieldId());
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(4, table.schema().findField("d").fieldId());
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(5, table.schema().findField("a.e").fieldId());
    }
  }

  @Test
  public void testAlterTableUnsetReplicationPolicy() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.`ttt1` (name string)");
      spark.sql("INSERT INTO openhouse.d1.ttt1 VALUES ('foo')");
      spark.sql(
          "ALTER TABLE openhouse.d1.ttt1 SET POLICY (REPLICATION=({destination:'WAR', interval:12h}))");
      spark.sql(
          "ALTER TABLE openhouse.d1.ttt1 SET POLICY (RETENTION= 30d on column name where pattern='yyyy-MM-dd')");
      Policies policies = getPoliciesObj("openhouse.d1.ttt1", spark);
      Assertions.assertNotNull(policies);
      Assertions.assertEquals(
          "'WAR'", policies.getReplication().getConfig().get(0).getDestination());
      Assertions.assertNotNull(policies.getRetention());
      Assertions.assertEquals(
          "'yyyy-MM-dd'", policies.getRetention().getColumnPattern().getPattern());

      // unset replication policy
      spark.sql("ALTER TABLE openhouse.d1.ttt1 UNSET POLICY (REPLICATION)");
      Policies updatedPolicy = getPoliciesObj("openhouse.d1.ttt1", spark);
      Assertions.assertEquals(updatedPolicy.getReplication().getConfig().size(), 0);
      // assert that other policies, retention is not modified after unsetting replication
      Assertions.assertNotNull(updatedPolicy.getRetention());
      Assertions.assertEquals(
          "'yyyy-MM-dd'", updatedPolicy.getRetention().getColumnPattern().getPattern());

      // assert retention can be set after unsetting replication
      spark.sql(
          "ALTER TABLE openhouse.d1.ttt1 SET POLICY (RETENTION = 30D on COLUMN name WHERE pattern = 'yyyy')");
      Policies policyWithRetention = getPoliciesObj("openhouse.d1.ttt1", spark);
      Assertions.assertNotNull(policyWithRetention);
      Assertions.assertEquals(
          "'yyyy'", policyWithRetention.getRetention().getColumnPattern().getPattern());
      Assertions.assertEquals(0, policyWithRetention.getReplication().getConfig().size());

      // assert replication can be set again after retention policy
      spark.sql(
          "ALTER TABLE openhouse.d1.ttt1 SET POLICY (REPLICATION=({destination:'WAR', interval:12h}))");
      Policies policyWithReplication = getPoliciesObj("openhouse.d1.ttt1", spark);
      Assertions.assertNotNull(policyWithReplication);
      Assertions.assertEquals(
          "'WAR'", policyWithReplication.getReplication().getConfig().get(0).getDestination());

      // UNSET policy for table without replication
      spark.sql("CREATE TABLE openhouse.d1.`tttest1` (name string)");
      spark.sql("INSERT INTO openhouse.d1.tttest1 VALUES ('foo')");
      spark.sql("ALTER TABLE openhouse.d1.tttest1 UNSET POLICY (REPLICATION)");
      Policies policytttest1 = getPoliciesObj("openhouse.d1.tttest1", spark);
      Assertions.assertEquals(0, policytttest1.getReplication().getConfig().size());
    }
  }

  @Test
  public void testCreateReplicaSkipFieldIdReassignmentPartitionedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));
      // Partition spec with identity partitioning on c
      PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("c").build();

      // Field ids not reassigned
      TableIdentifier tableIdentifier = TableIdentifier.of("replication_test", "t2");
      Map<String, String> props = new HashMap<>();
      props.put("client.table.schema", SchemaParser.toJson(schema));
      Table table = icebergCatalog.createTable(tableIdentifier, schema, null, props);
      Schema schemaAfterCreation = table.schema();
      Assertions.assertTrue(schemaAfterCreation.sameSchema(schema));
      Assertions.assertEquals(1, schemaAfterCreation.findField("a").fieldId());
      Assertions.assertNotEquals(3, schemaAfterCreation.findField("a.b").fieldId());
      Assertions.assertNotEquals(2, schemaAfterCreation.findField("c").fieldId());
      PartitionSpec pspecAfterCreation = table.spec();
      // pspec on c changes to 2
      Assertions.assertNotEquals(Sets.newHashSet(3), pspecAfterCreation.identitySourceIds());
      // Evolve schema, add top level column d (should work as before)
      table.updateSchema().addColumn("d", Types.StringType.get()).commit();
      Assertions.assertEquals(4, table.schema().findField("d").fieldId());
      // Evolve schema, add child column e to a (should work as before)
      table.updateSchema().addColumn("a", "e", Types.StringType.get()).commit();
      Assertions.assertEquals(5, table.schema().findField("a.e").fieldId());
    }
  }

  /**
   * This is a copy of com.linkedin.openhouse.jobs.spark.Operations#getCatalog() temporarily.
   * Refactoring these pieces require deployment coordination, thus we shall create an artifact
   * module that can be pulled by :apps module.
   */
  private Catalog getOpenHouseCatalog(SparkSession spark) {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = String.format("spark.sql.catalog.openhouse.");
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    // this initializes the catalog based on runtime Catalog class passed in catalog-impl conf.
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        "openhouse",
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }

  private Policies getPoliciesObj(String tableName, SparkSession spark) {
    List<Row> props = spark.sql(String.format("show tblProperties %s", tableName)).collectAsList();
    Map<String, String> collect =
        props.stream().collect(Collectors.toMap(r -> r.getString(0), r -> r.getString(1)));
    String policiesStr = String.valueOf(collect.get("policies"));
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.fromJson(policiesStr, Policies.class);
  }

  @Test
  public void testRenameTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);

      TableIdentifier fromTableIdentifier = TableIdentifier.of("db", "rename_test");
      spark.sql("CREATE TABLE openhouse.db.rename_test (name string)");

      TableIdentifier toTableIdentifier = TableIdentifier.of("db", "rename_test_renamed");
      spark.sql("ALTER TABLE openhouse.db.rename_test RENAME TO openhouse.db.rename_test_renamed");

      Table loadedTable = icebergCatalog.loadTable(toTableIdentifier);
      Assertions.assertNotNull(loadedTable);

      Assertions.assertEquals(
          loadedTable.properties().get("openhouse.tableUri"),
          "local-cluster.db.rename_test_renamed");

      Assertions.assertThrows(
          NoSuchTableException.class, () -> icebergCatalog.loadTable(fromTableIdentifier));

      spark.sql("CREATE TABLE openhouse.db.rename_test (name string)");
    }
  }

  @Test
  public void testRenameTableFailsConflict() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));

      // Field ids not reassigned
      TableIdentifier fromTableIdentifier = TableIdentifier.of("db", "rename_test2");
      TableIdentifier conflictingTableIdentifier = TableIdentifier.of("db", "rename_test_conflict");
      Map<String, String> props = new HashMap<>();
      props.put("client.table.schema", SchemaParser.toJson(schema));
      props.put("user.property", "test_property");
      Map<String, String> conflictingProps = new HashMap<>();
      conflictingProps.put("client.table.schema", SchemaParser.toJson(schema));
      Table createdTable = icebergCatalog.createTable(fromTableIdentifier, schema, null, props);
      Table conflictingTable =
          icebergCatalog.createTable(conflictingTableIdentifier, schema, null, conflictingProps);
      Assertions.assertNull(conflictingTable.properties().get("user.property"));
      TableIdentifier toTableIdentifier = TableIdentifier.of("db", "rename_test_conflict");

      // Should fail with conflict
      Assertions.assertThrows(
          WebClientResponseWithMessageException.class,
          () ->
              spark.sql(
                  "ALTER TABLE openhouse.db.rename_test2 RENAME TO openhouse.db.rename_test_conflict"));

      // Since rename fails, the properties on the user table should not have propagated
      Assertions.assertNull(
          icebergCatalog.loadTable(conflictingTableIdentifier).properties().get("user.property"));

      Assertions.assertNotNull(icebergCatalog.loadTable(fromTableIdentifier));
    }
  }

  @Test
  public void testRenameTableCaseSensitivity() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog icebergCatalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(
                  1,
                  "a",
                  Types.StructType.of(Types.NestedField.required(2, "b", Types.StringType.get()))),
              Types.NestedField.required(3, "c", Types.StringType.get()));

      // Field ids not reassigned
      TableIdentifier fromTableIdentifier = TableIdentifier.of("db", "rename_TEST3");
      icebergCatalog.createTable(fromTableIdentifier, schema, null, new HashMap<>());
      Table createdTable = icebergCatalog.loadTable(fromTableIdentifier);
      Assertions.assertEquals(createdTable.name(), "openhouse.db.rename_TEST3");
      TableIdentifier toTableIdentifier =
          TableIdentifier.of("db", "rename_test_renamed_CASE_SENSITIVE");
      Assertions.assertDoesNotThrow(
          () ->
              icebergCatalog.renameTable(
                  TableIdentifier.of("DB", "RENAME_test3"), toTableIdentifier));
      Table renamedTable =
          icebergCatalog.loadTable(TableIdentifier.of("dB", "rename_test_renamed_case_SENSITIVE"));

      // Ensure that the original db name is preserved
      Assertions.assertEquals(
          renamedTable.name(), "openhouse.dB.rename_test_renamed_case_SENSITIVE");
    }
  }

  @Test
  public void testAlterTableSetSortOrder() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql("CREATE TABLE openhouse.db.test_sort_order (id int, data string)");
      spark.sql("ALTER TABLE openhouse.db.test_sort_order WRITE ORDERED BY (id)");
      Table table = catalog.loadTable(TableIdentifier.of("db", "test_sort_order"));
      Assertions.assertEquals(
          SortOrder.builderFor(table.schema()).asc("id").build(), table.sortOrder());
      String distribution =
          spark
              .sql("show tblproperties openhouse.db.test_sort_order")
              .filter("key='write.distribution-mode'")
              .select("value")
              .first()
              .getString(0);
      Assertions.assertEquals("range", distribution);
    }
  }

  @Test
  public void testAlterTableUnsetSortOrder() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql("CREATE TABLE openhouse.db.test_sort_order_unset (id int, data string)");
      spark.sql("ALTER TABLE openhouse.db.test_sort_order_unset WRITE ORDERED BY (id)");
      spark.sql("ALTER TABLE openhouse.db.test_sort_order_unset WRITE UNORDERED");
      Table table = catalog.loadTable(TableIdentifier.of("db", "test_sort_order_unset"));
      Assertions.assertEquals(SortOrder.unsorted(), table.sortOrder());
    }
  }

  @Test
  public void testAlterTableSortOrderCTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql("CREATE TABLE openhouse.db.t1 (id int, data string)");
      spark.sql("ALTER TABLE openhouse.db.t1 WRITE ORDERED BY (id)");
      Table oldTable = catalog.loadTable(TableIdentifier.of("db", "t1"));
      // CTAS with sort order is only supported through catalog API
      Transaction transaction =
          catalog
              .buildTable(TableIdentifier.of("db", "test_sort_order_ctas"), oldTable.schema())
              .withSortOrder(oldTable.sortOrder())
              .createTransaction();
      transaction.commitTransaction();
      Table newTable = catalog.loadTable(TableIdentifier.of("db", "test_sort_order_ctas"));
      Assertions.assertEquals(
          SortOrder.builderFor(oldTable.schema()).asc("id").build(), newTable.sortOrder());
      // CTAS with sort order is not supported through SQL API
      spark.sql(
          "CREATE TABLE openhouse.db.test_sort_order_ctas_sql AS SELECT * FROM openhouse.db.t1");
      Table newSqlTable = catalog.loadTable(TableIdentifier.of("db", "test_sort_order_ctas_sql"));
      Assertions.assertEquals(SortOrder.unsorted(), newSqlTable.sortOrder());
    }
  }

  // ===== Case-insensitive reads (OHCaseInsensitiveResolveRule) =====

  @Test
  public void testReadWithCaseMismatchSucceeds_andDoesNotChangeCaseSensitiveConfig()
      throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Create a table with uppercase column "ID" — typical for tables originally created
      // by Hive or engines that preserve user-specified casing.
      Catalog catalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "ID", Types.StringType.get()),
              Types.NestedField.optional(2, "value", Types.LongType.get()));
      catalog.createTable(TableIdentifier.of("d1", "case_read_test"), schema);
      spark.sql("INSERT INTO openhouse.d1.case_read_test VALUES ('row1', 42)");

      // With caseSensitive=true, vanilla Spark would reject "id" as unresolved against "ID".
      // OHCaseInsensitiveResolveRule normalizes the attribute before ResolveReferences runs,
      // so the query must succeed — and crucially must NOT change the session setting.
      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        List<Row> rows = spark.sql("SELECT id FROM openhouse.d1.case_read_test").collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("row1", rows.get(0).getString(0));

        // The rule must NOT mutate spark.sql.caseSensitive — that is the whole point of moving
        // away from the session-level override approach.
        Assertions.assertEquals(
            "true",
            spark.conf().get("spark.sql.caseSensitive"),
            "OHCaseInsensitiveResolveRule must not modify spark.sql.caseSensitive");
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE openhouse.d1.case_read_test");
      }
    }
  }

  @Test
  public void testViewWithCaseMismatchResolvesViaRule() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Create a table with uppercase "ID" column.
      Catalog catalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "ID", Types.StringType.get()),
              Types.NestedField.optional(2, "count", Types.LongType.get()));
      catalog.createTable(TableIdentifier.of("d1", "view_case_test"), schema);
      spark.sql("INSERT INTO openhouse.d1.view_case_test VALUES ('a', 1), ('b', 2)");

      // View defined with explicit lowercase column references against a table that stores "ID".
      // With caseSensitive=true and without the rule both the view definition and any outer query
      // referencing a mismatched column name would fail to resolve.
      spark.sql(
          "CREATE OR REPLACE TEMP VIEW v_case AS "
              + "SELECT id, count FROM openhouse.d1.view_case_test");

      // View defined with SELECT * — columns come from star-expansion over the OH table schema.
      spark.sql(
          "CREATE OR REPLACE TEMP VIEW v_case_star AS "
              + "SELECT * FROM openhouse.d1.view_case_test");

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        // SELECT * from explicit-column view: the rule normalises the inlined view SQL and the
        // query must return all rows with correct values.
        List<Row> selectStar = spark.sql("SELECT * FROM v_case ORDER BY id").collectAsList();
        Assertions.assertEquals(2, selectStar.size());
        Assertions.assertEquals("a", selectStar.get(0).getString(0));
        Assertions.assertEquals("b", selectStar.get(1).getString(0));

        // SELECT id from explicit-column view: the outer query uses a case-mismatched column
        // reference against the view output — the rule must normalise it at the outer level too.
        List<Row> selectId = spark.sql("SELECT id FROM v_case ORDER BY id").collectAsList();
        Assertions.assertEquals(2, selectId.size());
        Assertions.assertEquals("a", selectId.get(0).getString(0));
        Assertions.assertEquals("b", selectId.get(1).getString(0));

        // SELECT * from star view: star-expansion over the OH table schema produces "ID"; the
        // rule must make reading it back case-insensitive.
        List<Row> starViewSelectStar =
            spark.sql("SELECT * FROM v_case_star ORDER BY id").collectAsList();
        Assertions.assertEquals(2, starViewSelectStar.size());
        Assertions.assertEquals("a", starViewSelectStar.get(0).getString(0));
        Assertions.assertEquals("b", starViewSelectStar.get(1).getString(0));

        // SELECT id from star view: explicit column reference with case mismatch against
        // star-expanded view output must also resolve correctly.
        List<Row> starViewSelectId =
            spark.sql("SELECT id FROM v_case_star ORDER BY id").collectAsList();
        Assertions.assertEquals(2, starViewSelectId.size());
        Assertions.assertEquals("a", starViewSelectId.get(0).getString(0));
        Assertions.assertEquals("b", starViewSelectId.get(1).getString(0));

        Assertions.assertEquals(
            "true",
            spark.conf().get("spark.sql.caseSensitive"),
            "OHCaseInsensitiveResolveRule must not modify spark.sql.caseSensitive");
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE openhouse.d1.view_case_test");
      }
    }
  }

  @Test
  public void testCaseDuplicateTableIsExcludedFromNormalization() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // The open-source server does not reject case-duplicate column names at CREATE TABLE time
      // (that guard lives in the li-openhouse extension's LiSchemaValidator for schema evolution).
      // Such a table can therefore exist, e.g. created before server-side validation was added.
      // OHCaseInsensitiveResolveRule contains a matching defensive exclusion: it skips
      // normalization for case-duplicate tables rather than silently misdirecting references.
      // The net effect is that Spark's own ResolveReferences handles the ambiguous column, which
      // raises an AnalysisException instead of resolving to the wrong column.
      Catalog catalog = getOpenHouseCatalog(spark);
      Schema schema =
          new Schema(
              Types.NestedField.required(1, "id", Types.StringType.get()),
              Types.NestedField.optional(2, "ID", Types.StringType.get()));

      // CREATE succeeds — no CREATE-time case-duplicate validation in open-source server.
      catalog.createTable(TableIdentifier.of("d1", "case_dup_test"), schema);

      // With caseSensitive=false (default), the ambiguous lowercase reference "id" must throw
      // at analysis time — this is independent of whether the table has data.
      // The rule's empty mapping for this table means no silent rename occurs; Spark detects
      // the ambiguity itself.
      Assertions.assertThrows(
          Exception.class,
          () -> spark.sql("SELECT id FROM openhouse.d1.case_dup_test").collectAsList(),
          "Ambiguous reference against case-duplicate table must throw");

      spark.sql("DROP TABLE openhouse.d1.case_dup_test");
    }
  }

  @Test
  public void testWriteWithCaseMismatch_succeedsWithCaseSensitiveTrue() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Create a table with uppercase column "ID" — the common case for tables originally created
      // by Hive or engines that preserve user-specified casing.
      Catalog catalog = getOpenHouseCatalog(spark);
      Schema schema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
      catalog.createTable(TableIdentifier.of("d1", "write_case_test"), schema);

      // With caseSensitive=true, Spark's ResolveOutputRelation uses a case-sensitive resolver and
      // cannot find source column "id" in the target schema column "ID". Vanilla Spark would throw
      // "Cannot find data for output column 'ID'" at analysis time.
      //
      // OHSparkCatalog advertises ACCEPT_ANY_SCHEMA so outputResolved=true and
      // ResolveOutputRelation skips OH writes. OHWriteSchemaNormalizationRule (post-hoc) then
      // inserts a Project(Alias("id" → "ID")) so Iceberg sees the correct stored casing.
      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () -> spark.sql("SELECT 'row1' AS id").writeTo("openhouse.d1.write_case_test").append(),
            "writeTo().append() must succeed when source has lowercase 'id' and OH table has 'ID'");

        // Verify the row was written with the correct stored casing.
        List<Row> rows = spark.sql("SELECT id FROM openhouse.d1.write_case_test").collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("row1", rows.get(0).getString(0));

        // The rule must NOT mutate spark.sql.caseSensitive.
        Assertions.assertEquals(
            "true",
            spark.conf().get("spark.sql.caseSensitive"),
            "OHWriteSchemaNormalizationRule must not modify spark.sql.caseSensitive");
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE openhouse.d1.write_case_test");
      }
    }
  }
}
