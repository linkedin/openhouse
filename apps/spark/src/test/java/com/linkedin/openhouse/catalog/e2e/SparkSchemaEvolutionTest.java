package com.linkedin.openhouse.catalog.e2e;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkSchemaEvolutionTest extends OpenHouseSparkITest {

  @Test
  void testEndUserSchemaEvolution() throws Exception {
    SparkSession spark = null;
    try {
      spark = getSparkSession();
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string, id bigint)");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('Alice', 1)");
      spark.sql("INSERT INTO openhouse.d1.t1 VALUES ('Bob', 2), ('Charlie', 3)");

      // Inspect the schema object first, establish the baseline
      Dataset<Row> tableDF = spark.table("openhouse.d1.t1");

      StructType oldSchema = tableDF.schema();

      // Create new dummy dataframe with reordered columns
      StructType schema =
          new StructType(
              new StructField[] {
                new StructField("dt", DataTypes.StringType, false, Metadata.empty()),
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty())
              });

      List<Row> data =
          Arrays.asList(
              RowFactory.create("2025-04-05", 1L, "John Doe"),
              RowFactory.create("2025-04-06", 2L, "Jane Smith"));

      // Remember the column ordering here is dt, id, name,
      // the idea table schema's column ordering will be name, id, dt (as long as dt is at the end,
      // it should be working)
      Dataset<Row> dummyDF = spark.createDataFrame(data, schema);

      // Manually evolve to mimic client-side explicit evolution, before a write can be issued
      Operations operations = Operations.withCatalog(spark, null);
      Table table = operations.getTable("d1.t1");
      int colLen = table.schema().columns().size();
      String lastColName = table.schema().columns().get(colLen - 1).name();
      table
          .updateSchema()
          .unionByNameWith(SparkSchemaUtil.convert(schema))
          .moveAfter("dt", lastColName)
          .commit();

      Schema newSchema = operations.getTable("d1.t1").schema();
      List<Types.NestedField> newSchemaCols = newSchema.columns();
      Assertions.assertEquals(newSchemaCols.size(), 3);
      Assertions.assertEquals(newSchemaCols.get(2).name(), "dt");

      // This is necessary to ensure Spark not caching the previous state.
      spark.sql("REFRESH TABLE openhouse.d1.t1");

      dummyDF.write().mode("append").format("parquet").saveAsTable("openhouse.d1.t1");

      // Validating write go through successfully
      tableDF = spark.table("openhouse.d1.t1");
      Assertions.assertEquals(tableDF.count(), 5);

      // another schema evolution
      StructType schema2 =
          new StructType(
              new StructField[] {
                new StructField("dt", DataTypes.StringType, false, Metadata.empty()),
                new StructField("zipcode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty())
              });
      table = operations.getTable("d1.t1");
      table.updateSchema().unionByNameWith(SparkSchemaUtil.convert(schema2)).commit();
      newSchema = operations.getTable("d1.t1").schema();
      newSchemaCols = newSchema.columns();
      Assertions.assertEquals(newSchemaCols.size(), 4);
    } finally {
      if (spark != null) {
        spark.sql("DROP TABLE openhouse.d1.t1");
      }
    }
  }
}
