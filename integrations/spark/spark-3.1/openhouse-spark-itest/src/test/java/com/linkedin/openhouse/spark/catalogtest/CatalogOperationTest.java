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
import org.apache.iceberg.NullOrder;
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

  private static final String DATABASE = "d1_catalog";

  @Test
  public void testCasingWithCTAS() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      spark.sql("CREATE TABLE openhouse." + DATABASE + ".`tT1` (name string)");
      // testing writing behavior, note the casing of tt1 is intentionally changed.
      spark.sql("INSERT INTO openhouse." + DATABASE + ".Tt1 VALUES ('foo')");

      // Verifying by querying with all lower-cased name
      Assertions.assertEquals(
          1, spark.sql("SELECT * from openhouse." + DATABASE + ".tt1").collectAsList().size());
      // ctas but referring with lower-cased name
      spark.sql(
          "CREATE TABLE openhouse."
              + DATABASE
              + ".t2 AS SELECT * from openhouse."
              + DATABASE
              + ".tt1");
      Assertions.assertEquals(
          1, spark.sql("SELECT * FROM openhouse." + DATABASE + ".t2").collectAsList().size());
    }
  }

  @Test
  public void testCreateTablePartitionedByDate() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // creating a casing preserving table using backtick
      String quotedFqtn = "openhouse." + DATABASE + ".tpartionedbydate";
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
      spark.sql("CREATE TABLE openhouse." + DATABASE + ".`ttt1` (name string)");
      spark.sql("INSERT INTO openhouse." + DATABASE + ".ttt1 VALUES ('foo')");
      spark.sql(
          "ALTER TABLE openhouse."
              + DATABASE
              + ".ttt1 SET POLICY (REPLICATION=({destination:'WAR', interval:12h}))");
      spark.sql(
          "ALTER TABLE openhouse."
              + DATABASE
              + ".ttt1 SET POLICY (RETENTION= 30d on column name where pattern='yyyy-MM-dd')");
      Policies policies = getPoliciesObj("openhouse." + DATABASE + ".ttt1", spark);
      Assertions.assertNotNull(policies);
      Assertions.assertEquals(
          "'WAR'", policies.getReplication().getConfig().get(0).getDestination());
      Assertions.assertNotNull(policies.getRetention());
      Assertions.assertEquals(
          "'yyyy-MM-dd'", policies.getRetention().getColumnPattern().getPattern());

      // unset replication policy
      spark.sql("ALTER TABLE openhouse." + DATABASE + ".ttt1 UNSET POLICY (REPLICATION)");
      Policies updatedPolicy = getPoliciesObj("openhouse." + DATABASE + ".ttt1", spark);
      Assertions.assertEquals(updatedPolicy.getReplication().getConfig().size(), 0);
      // assert that other policies, retention is not modified after unsetting replication
      Assertions.assertNotNull(updatedPolicy.getRetention());
      Assertions.assertEquals(
          "'yyyy-MM-dd'", updatedPolicy.getRetention().getColumnPattern().getPattern());

      // assert retention can be set after unsetting replication
      spark.sql(
          "ALTER TABLE openhouse."
              + DATABASE
              + ".ttt1 SET POLICY (RETENTION = 30D on COLUMN name WHERE pattern = 'yyyy')");
      Policies policyWithRetention = getPoliciesObj("openhouse." + DATABASE + ".ttt1", spark);
      Assertions.assertNotNull(policyWithRetention);
      Assertions.assertEquals(
          "'yyyy'", policyWithRetention.getRetention().getColumnPattern().getPattern());
      Assertions.assertEquals(0, policyWithRetention.getReplication().getConfig().size());

      // assert replication can be set again after retention policy
      spark.sql(
          "ALTER TABLE openhouse."
              + DATABASE
              + ".ttt1 SET POLICY (REPLICATION=({destination:'WAR', interval:12h}))");
      Policies policyWithReplication = getPoliciesObj("openhouse." + DATABASE + ".ttt1", spark);
      Assertions.assertNotNull(policyWithReplication);
      Assertions.assertEquals(
          "'WAR'", policyWithReplication.getReplication().getConfig().get(0).getDestination());

      // UNSET policy for table without replication
      spark.sql("CREATE TABLE openhouse." + DATABASE + ".`tttest1` (name string)");
      spark.sql("INSERT INTO openhouse." + DATABASE + ".tttest1 VALUES ('foo')");
      spark.sql("ALTER TABLE openhouse." + DATABASE + ".tttest1 UNSET POLICY (REPLICATION)");
      Policies policytttest1 = getPoliciesObj("openhouse." + DATABASE + ".tttest1", spark);
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
      // inserts a Project(Alias("id" -> "ID")) so Iceberg sees the correct stored casing.
      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () -> spark.sql("SELECT 'row1' AS id").writeTo("openhouse.d1.write_case_test").append(),
            "writeTo().append() must succeed when source has lowercase 'id' and OH table has 'ID'");

        // Verify the row was written with the correct stored casing.
        // Use the exact stored column name "ID" (not lowercase "id") for the read since this
        // branch does not include the read-side case-insensitive resolution rule.
        List<Row> rows = spark.sql("SELECT ID FROM openhouse.d1.write_case_test").collectAsList();
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

  /**
   * Verifies that {@code OHWriteSchemaNormalizationRule} fixes writes from a temporary Spark view
   * into an OH table when the view's column casing differs from the stored table casing.
   *
   * <p>Without the fix: {@code INSERT INTO oh_tbl SELECT colA FROM tempView} fails at analysis time
   * because Spark's {@code ResolveOutputRelation} performs a case-sensitive name comparison between
   * the view output column (e.g. {@code "colA"}) and the stored table column (e.g. {@code "COLA"}).
   * This throw happens regardless of {@code spark.sql.caseSensitive}, because the temporary view
   * introduces an intermediate resolved relation whose output attribute names are locked to the
   * casing in the view body.
   *
   * <p>With the fix: {@code OHSparkCatalog} advertises {@code ACCEPT_ANY_SCHEMA}, causing {@code
   * ResolveOutputRelation} to skip OH write commands entirely. {@code
   * OHWriteSchemaNormalizationRule} then fires post-hoc and inserts a {@code Project} that renames
   * the view output column to match the stored OH casing, regardless of whether the source is a
   * temp view, a direct table, or any other resolved query.
   */
  @Test
  public void testWriteFromTempView_caseMismatch_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Create an OH table with uppercase column "ID".
      Catalog catalog = getOpenHouseCatalog(spark);
      Schema schema = new Schema(Types.NestedField.required(1, "ID", Types.StringType.get()));
      catalog.createTable(TableIdentifier.of("d1", "write_view_case_test"), schema);

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        // Create a temp view that produces a lowercase "id" column.
        // This simulates the real-world pattern: a view created over a Hive/external source
        // where the engine lowercases or camelCases the identifier.
        spark.sql("CREATE OR REPLACE TEMP VIEW v_write_src AS SELECT 'row1' AS id");

        // INSERT INTO from the temp view must succeed — the rule must rename "id" → "ID"
        // in the Project inserted between the view output and the Iceberg writer.
        Assertions.assertDoesNotThrow(
            () ->
                spark.sql(
                    "INSERT INTO openhouse.d1.write_view_case_test SELECT id FROM v_write_src"),
            "INSERT INTO from temp view must succeed when view has 'id' and OH table stores 'ID'");

        // Confirm the row landed with the correct stored casing.
        List<Row> rows =
            spark.sql("SELECT ID FROM openhouse.d1.write_view_case_test").collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("row1", rows.get(0).getString(0));
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP VIEW IF EXISTS v_write_src");
        spark.sql("DROP TABLE openhouse.d1.write_view_case_test");
      }
    }
  }

  /**
   * Verifies that {@code OHWriteSchemaNormalizationRule} handles writes between two OH tables whose
   * nested struct field names differ only in case (e.g. {@code firstName} in the source vs {@code
   * firstname} in the target).
   *
   * <p>Without the fix: {@code INSERT INTO t1 SELECT * FROM t2} fails at analysis time because
   * Spark's {@code ResolveOutputRelation} treats {@code STRUCT<firstName:STRING,lastName:STRING>}
   * and {@code STRUCT<firstname:STRING,lastname:STRING>} as incompatible types when {@code
   * caseSensitive=true}, and the analyzer throws before the write reaches the OH server.
   *
   * <p>With the fix: {@code OHSparkCatalog} advertises {@code ACCEPT_ANY_SCHEMA}, bypassing {@code
   * ResolveOutputRelation}. {@code OHWriteSchemaNormalizationRule} then inserts {@code
   * Alias(Cast(info, STRUCT<firstname,lastname>), "info")} into the plan. Spark's struct {@code
   * Cast} maps fields positionally, so the {@code firstName} value at position 0 is mapped to the
   * {@code firstname} slot in the target, and similarly for {@code lastName} → {@code lastname}.
   */
  @Test
  public void testWriteNestedStructCaseMismatch_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Target table: lowercase nested field names.
      spark.sql(
          "CREATE TABLE openhouse.db.nested_struct_tgt"
              + " (id INT, info STRUCT<firstname:STRING, lastname:STRING>)");
      // Source table: camelCase nested field names.
      spark.sql(
          "CREATE TABLE openhouse.db.nested_struct_src"
              + " (id INT, info STRUCT<firstName:STRING, lastName:STRING>)");
      // Populate the source table before enabling case-sensitive mode.
      spark.sql(
          "INSERT INTO openhouse.db.nested_struct_src"
              + " VALUES (1, named_struct('firstName', 'John', 'lastName', 'Doe'))");

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        // Without the fix: Spark's ResolveOutputRelation sees that
        // STRUCT<firstName,lastName> != STRUCT<firstname,lastname> (type inequality when
        // caseSensitive=true) and throws "Cannot write incompatible data to table" before
        // the INSERT reaches the OH server.
        //
        // With the fix: OHSparkCatalog adds ACCEPT_ANY_SCHEMA so ResolveOutputRelation is
        // skipped entirely. OHWriteSchemaNormalizationRule (post-hoc) detects that the
        // info column's struct type differs and inserts Cast(info, STRUCT<firstname,lastname>).
        // Spark's struct Cast maps fields positionally (not by name), so firstName→firstname
        // and lastName→lastname are transferred correctly at execution time.
        Assertions.assertDoesNotThrow(
            () ->
                spark.sql(
                    "INSERT INTO openhouse.db.nested_struct_tgt"
                        + " SELECT * FROM openhouse.db.nested_struct_src"),
            "INSERT INTO must succeed when source has camelCase nested fields "
                + "and target has lowercase nested fields");

        // Verify data landed in the target with the stored lowercase field names.
        List<Row> rows =
            spark
                .sql("SELECT info.firstname, info.lastname FROM openhouse.db.nested_struct_tgt")
                .collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("John", rows.get(0).getString(0));
        Assertions.assertEquals("Doe", rows.get(0).getString(1));
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.nested_struct_tgt");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.nested_struct_src");
      }
    }
  }

  @Test
  public void testWriteOrderedByPersistsMultiColumnSortOrder() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql(
          "CREATE TABLE openhouse.db.write_ordered_multi (id INT, category STRING, data STRING)");
      spark.sql("ALTER TABLE openhouse.db.write_ordered_multi WRITE ORDERED BY category, id");

      Table table = catalog.loadTable(TableIdentifier.of("db", "write_ordered_multi"));
      Assertions.assertEquals(
          SortOrder.builderFor(table.schema()).asc("category").asc("id").build(),
          table.sortOrder());
    }
  }

  @Test
  public void testWriteOrderedByRespectsDirectionAndNullOrder() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql("CREATE TABLE openhouse.db.write_ordered_desc (id INT, category STRING)");
      // DESC defaults to NULLS LAST in Iceberg; override to NULLS FIRST to verify both
      // direction and null-order are propagated end-to-end.
      spark.sql(
          "ALTER TABLE openhouse.db.write_ordered_desc WRITE ORDERED BY category DESC NULLS FIRST");

      Table table = catalog.loadTable(TableIdentifier.of("db", "write_ordered_desc"));
      Assertions.assertEquals(
          SortOrder.builderFor(table.schema()).desc("category", NullOrder.NULLS_FIRST).build(),
          table.sortOrder());
    }
  }

  @Test
  public void testWriteOrderedByRoundTripsThroughInsert() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      spark.sql("CREATE TABLE openhouse.db.write_ordered_insert (id INT, category STRING)");
      spark.sql("ALTER TABLE openhouse.db.write_ordered_insert WRITE ORDERED BY id");

      spark.sql(
          "INSERT INTO openhouse.db.write_ordered_insert VALUES (3, 'C'), (1, 'A'), (2, 'B')");

      Table table = catalog.loadTable(TableIdentifier.of("db", "write_ordered_insert"));
      // Sort order metadata is preserved across an INSERT (no implicit reset).
      Assertions.assertEquals(
          SortOrder.builderFor(table.schema()).asc("id").build(), table.sortOrder());

      List<Row> rows =
          spark.sql("SELECT id FROM openhouse.db.write_ordered_insert ORDER BY id").collectAsList();
      Assertions.assertEquals(3, rows.size());
      Assertions.assertEquals(1, rows.get(0).getInt(0));
      Assertions.assertEquals(2, rows.get(1).getInt(0));
      Assertions.assertEquals(3, rows.get(2).getInt(0));
    }
  }

  /**
   * Partition-column case mismatch via {@code DataFrame.writeTo().append()} (by-name V2 write). The
   * DataFrame has the partition column spelled {@code "DatePartition"} (camelCase) while the OH
   * table stores {@code "datepartition"} (lowercase). With {@code caseSensitive=true} the normal
   * Spark analyzer would fail with "Cannot find data for output column 'datepartition'". The rule
   * must rename the source column so the value lands in the correct partition.
   */
  @Test
  public void testWritePartitionColumnCaseMismatch_writeToAppend_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE openhouse.db.partition_writeto_case (id INT, datepartition STRING)"
              + " PARTITIONED BY (datepartition)");
      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () ->
                spark
                    .sql("SELECT 1 AS id, '2026-05-22' AS DatePartition")
                    .writeTo("openhouse.db.partition_writeto_case")
                    .append(),
            "writeTo append must succeed when DF has 'DatePartition' and table has 'datepartition'");
        // Verify partition routing: a query that filters by the stored partition column name
        // returns the row only if it landed in the correct partition.
        List<Row> rows =
            spark
                .sql(
                    "SELECT id, datepartition FROM openhouse.db.partition_writeto_case"
                        + " WHERE datepartition = '2026-05-22'")
                .collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(1, rows.get(0).getInt(0));
        Assertions.assertEquals("2026-05-22", rows.get(0).getString(1));
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.partition_writeto_case");
      }
    }
  }

  /**
   * Partition-column case mismatch via SQL {@code INSERT INTO … SELECT} (by-position V2 write).
   * Source table column is {@code "DatePartition"}; target table column is {@code "datepartition"}
   * and is the partition key. Rows must land in the correct partitions despite the case difference.
   */
  @Test
  public void testWritePartitionColumnCaseMismatch_sqlInsert_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE openhouse.db.partition_sql_case (id INT, datepartition STRING)"
              + " PARTITIONED BY (datepartition)");
      spark.sql("CREATE TABLE openhouse.db.partition_sql_src (Id INT, DatePartition STRING)");
      spark.sql(
          "INSERT INTO openhouse.db.partition_sql_src"
              + " VALUES (1, '2026-05-22'), (2, '2026-05-23')");

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () ->
                spark.sql(
                    "INSERT INTO openhouse.db.partition_sql_case"
                        + " SELECT * FROM openhouse.db.partition_sql_src"),
            "INSERT INTO SELECT must succeed when source has 'DatePartition' and target has"
                + " 'datepartition'");
        List<Row> rows =
            spark
                .sql("SELECT id, datepartition FROM openhouse.db.partition_sql_case ORDER BY id")
                .collectAsList();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("2026-05-22", rows.get(0).getString(1));
        Assertions.assertEquals("2026-05-23", rows.get(1).getString(1));
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.partition_sql_case");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.partition_sql_src");
      }
    }
  }

  /**
   * Nested struct case mismatch where source fields are in a <em>different order</em> than target.
   * Source struct order is {@code <lastName, firstName>} (lastName at index 0); target is {@code
   * <firstname, lastname>} (firstname at index 0). A positional struct {@code Cast} would put the
   * source's lastName value into the target's firstname slot — silent data corruption. The
   * recursive name-based alignment in {@link
   * com.linkedin.openhouse.spark.extensions.OHWriteSchemaNormalizationRule} maps fields by
   * case-insensitive name so values land in the correct target field.
   */
  @Test
  public void testWriteNestedStructReorderedFields_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE openhouse.db.nested_reordered_tgt"
              + " (id INT, info STRUCT<firstname:STRING, lastname:STRING>)");
      spark.sql(
          "CREATE TABLE openhouse.db.nested_reordered_src"
              + " (id INT, info STRUCT<lastName:STRING, firstName:STRING>)");
      spark.sql(
          "INSERT INTO openhouse.db.nested_reordered_src"
              + " VALUES (1, named_struct('lastName', 'Doe', 'firstName', 'John'))");

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () ->
                spark.sql(
                    "INSERT INTO openhouse.db.nested_reordered_tgt"
                        + " SELECT * FROM openhouse.db.nested_reordered_src"),
            "INSERT must succeed when nested source fields are in different order than target");
        List<Row> rows =
            spark
                .sql("SELECT info.firstname, info.lastname FROM openhouse.db.nested_reordered_tgt")
                .collectAsList();
        Assertions.assertEquals(1, rows.size());
        // Critical: John must land in 'firstname' (not 'lastname') — proves name-based, not
        // positional, struct field matching.
        Assertions.assertEquals(
            "John",
            rows.get(0).getString(0),
            "firstname must contain 'John' — value matched by name, not by source position");
        Assertions.assertEquals(
            "Doe",
            rows.get(0).getString(1),
            "lastname must contain 'Doe' — value matched by name, not by source position");
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.nested_reordered_tgt");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.nested_reordered_src");
      }
    }
  }

  /**
   * Deeply nested struct (struct-in-struct) with case mismatches at every level. Exercises the
   * recursion in {@code alignExpressionToTargetType}: outer struct fields, inner struct fields, and
   * the leaf field all need case-insensitive name resolution.
   */
  @Test
  public void testWriteDeepNestedStructCaseMismatch_succeeds() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql(
          "CREATE TABLE openhouse.db.deep_nested_tgt"
              + " (id INT,"
              + " person STRUCT<name:STRING, address:STRUCT<street:STRING, city:STRING>>)");
      spark.sql(
          "CREATE TABLE openhouse.db.deep_nested_src"
              + " (id INT,"
              + " Person STRUCT<Name:STRING, Address:STRUCT<Street:STRING, CITY:STRING>>)");
      spark.sql(
          "INSERT INTO openhouse.db.deep_nested_src"
              + " VALUES (1, named_struct('Name', 'Alice',"
              + " 'Address', named_struct('Street', '100 Main', 'CITY', 'SF')))");

      spark.conf().set("spark.sql.caseSensitive", "true");
      try {
        Assertions.assertDoesNotThrow(
            () ->
                spark.sql(
                    "INSERT INTO openhouse.db.deep_nested_tgt"
                        + " SELECT * FROM openhouse.db.deep_nested_src"),
            "INSERT must succeed when struct-in-struct fields differ in case at every level");
        List<Row> rows =
            spark
                .sql(
                    "SELECT person.name, person.address.street, person.address.city"
                        + " FROM openhouse.db.deep_nested_tgt")
                .collectAsList();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("Alice", rows.get(0).getString(0));
        Assertions.assertEquals("100 Main", rows.get(0).getString(1));
        Assertions.assertEquals("SF", rows.get(0).getString(2));
      } finally {
        spark.conf().set("spark.sql.caseSensitive", "false");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.deep_nested_tgt");
        spark.sql("DROP TABLE IF EXISTS openhouse.db.deep_nested_src");
      }
    }
  }

  /**
   * Builds the Iceberg {@link Catalog} that the configured Spark session is using to talk to
   * OpenHouse. Tests that need a direct catalog handle (e.g. to call {@code createTable} with a
   * specific schema before exercising the writer) use this rather than going through SQL DDL.
   */
  private Catalog getOpenHouseCatalog(SparkSession spark) {
    final Map<String, String> catalogProperties = new HashMap<>();
    final String catalogPropertyPrefix = "spark.sql.catalog.openhouse.";
    final Map<String, String> sparkProperties = JavaConverters.mapAsJavaMap(spark.conf().getAll());
    for (Map.Entry<String, String> entry : sparkProperties.entrySet()) {
      if (entry.getKey().startsWith(catalogPropertyPrefix)) {
        catalogProperties.put(
            entry.getKey().substring(catalogPropertyPrefix.length()), entry.getValue());
      }
    }
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        "openhouse",
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }
}
