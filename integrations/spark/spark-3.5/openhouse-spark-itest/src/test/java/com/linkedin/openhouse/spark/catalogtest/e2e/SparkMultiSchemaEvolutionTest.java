package com.linkedin.openhouse.spark.catalogtest.e2e;

import com.linkedin.openhouse.javaclient.OpenHouseCatalog;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

public class SparkMultiSchemaEvolutionTest extends OpenHouseSparkITest {

  @Test
  void testMultiSchemaEvolution() throws Exception {
    SparkSession spark = null;
    try {
      spark = getSparkSession();
      spark.sql(
          "CREATE TABLE openhouse.multiSchemaTest.t1 (name string, id int) TBLPROPERTIES ('openhouse.tableType' = 'REPLICA_TABLE');");
      spark.sql("INSERT INTO openhouse.multiSchemaTest.t1 VALUES ('Alice', 1)");
      spark.sql("INSERT INTO openhouse.multiSchemaTest.t1 VALUES ('Bob', 2), ('Charlie', 3)");
      TableIdentifier tableIdentifier = TableIdentifier.of("multiSchemaTest", "t1");
      OpenHouseCatalog ohCatalog = (OpenHouseCatalog) getOpenHouseCatalog(spark);
      TableOperations ops = ohCatalog.newTableOps(tableIdentifier);
      Schema evolvedSchema =
          new Schema(
              Types.NestedField.optional(1, "name", Types.StringType.get()),
              Types.NestedField.optional(2, "id", Types.IntegerType.get()),
              Types.NestedField.optional(3, "newCol", Types.IntegerType.get()));
      Schema finalEvolvedSchema =
          new Schema(
              Types.NestedField.optional(1, "name", Types.StringType.get()),
              Types.NestedField.optional(2, "id", Types.IntegerType.get()),
              Types.NestedField.optional(3, "newCol1", Types.IntegerType.get()),
              Types.NestedField.optional(4, "newCol2", Types.IntegerType.get()));

      TableMetadata metadata = ops.current();
      TableMetadata evolvedMetadata =
          TableMetadata.buildFrom(metadata)
              .addSchema(evolvedSchema, evolvedSchema.highestFieldId())
              .build();
      TableMetadata finalEvolvedMetadata =
          TableMetadata.buildFrom(evolvedMetadata)
              .addSchema(finalEvolvedSchema, finalEvolvedSchema.highestFieldId())
              .setCurrentSchema(2)
              .build();

      Assertions.assertEquals(finalEvolvedMetadata.schemas().size(), 3);
      ops.commit(metadata, finalEvolvedMetadata);
      TableMetadata result = ops.current();
      Assertions.assertEquals(3, result.schemas().size());
      Assertions.assertTrue(result.schema().sameSchema(finalEvolvedSchema));
    } finally {
      if (spark != null) {
        spark.sql("DROP TABLE openhouse.multiSchemaTest.t1");
      }
    }
  }

  @Test
  void testMultiSchemaEvolutionColumnOrderingOnCreate() throws Exception {
    SparkSession spark = null;
    try {
      spark = getSparkSession();
      TableIdentifier tableIdentifier = TableIdentifier.of("multiSchemaTest", "t2");
      OpenHouseCatalog ohCatalog = (OpenHouseCatalog) getOpenHouseCatalog(spark);
      Schema schemaColumnOrdering =
          new Schema(
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(4, "newCol1", Types.IntegerType.get()),
              Types.NestedField.optional(3, "newCol2", Types.IntegerType.get()));
      Map<String, String> tableProperties = new HashMap<>();
      tableProperties.put("openhouse.tableType", "REPLICA_TABLE");
      tableProperties.put("openhouse.isTableReplicated", "true");
      tableProperties.put("client.table.schema", SchemaParser.toJson(schemaColumnOrdering));
      ohCatalog.createTable(tableIdentifier, schemaColumnOrdering, null, tableProperties);
      TableOperations ops = ohCatalog.newTableOps(tableIdentifier);
      TableMetadata metadata = ops.current();
      Assertions.assertEquals(metadata.schema().findColumnName(2), "name");
      Assertions.assertTrue(metadata.schema().sameSchema(schemaColumnOrdering));
      Schema schemaColumnOrdering2 =
          new Schema(
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(4, "newCol1", Types.IntegerType.get()),
              Types.NestedField.optional(3, "newCol2", Types.IntegerType.get()),
              Types.NestedField.optional(5, "newCol3", Types.IntegerType.get()));

      Schema schemaColumnOrdering3 =
          new Schema(
              Types.NestedField.optional(2, "name", Types.StringType.get()),
              Types.NestedField.optional(1, "id", Types.IntegerType.get()),
              Types.NestedField.optional(4, "newCol1", Types.IntegerType.get()),
              Types.NestedField.optional(3, "newCol2", Types.IntegerType.get()),
              Types.NestedField.optional(5, "newCol3", Types.IntegerType.get()),
              Types.NestedField.optional(6, "newCol4", Types.IntegerType.get()),
              Types.NestedField.optional(7, "newCol5", Types.IntegerType.get()));

      TableMetadata evolvedMetadata =
          TableMetadata.buildFrom(metadata)
              .addSchema(schemaColumnOrdering2, schemaColumnOrdering2.highestFieldId())
              .build();
      TableMetadata finalEvolvedMetadata =
          TableMetadata.buildFrom(evolvedMetadata)
              .addSchema(schemaColumnOrdering3, schemaColumnOrdering3.highestFieldId())
              .setCurrentSchema(2)
              .build();

      Assertions.assertEquals(finalEvolvedMetadata.schemas().size(), 3);
      ops.commit(metadata, finalEvolvedMetadata);
      TableMetadata result = ops.current();
      Assertions.assertEquals(3, result.schemas().size());
      // Validate ordering of columns persists on creation
      Assertions.assertEquals(result.schema().findColumnName(2), "name");
      Assertions.assertTrue(result.schema().sameSchema(schemaColumnOrdering3));
    } finally {
      if (spark != null) {
        spark.sql("DROP TABLE openhouse.multiSchemaTest.t2");
      }
    }
  }

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
    // this initializes the catalog based on runtime Catalog class passed in catalog-impl conf.
    return CatalogUtil.loadCatalog(
        sparkProperties.get("spark.sql.catalog.openhouse.catalog-impl"),
        "openhouse",
        catalogProperties,
        spark.sparkContext().hadoopConfiguration());
  }
}
